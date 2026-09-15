"""
Persistence layer.

Two responsibilities, kept separate on purpose:
  1. Dedup/state store  -> "have I already crawled this URL/hash?"
  2. Object storage      -> where the raw crawled document is written

Both are behind small interfaces so the crawler never talks to
MinIO/S3/Redis/Postgres directly — that keeps the component testable
and lets Ops swap the backend without touching crawl logic.
"""
from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from typing import Optional

from .models import CrawledDocument


class DedupStore(ABC):
    """Tracks which (source_id, content_hash) pairs have already been
    processed, so a scheduled re-crawl doesn't re-emit unchanged pages."""

    @abstractmethod
    def seen(self, source_id: str, content_hash: str) -> bool: ...

    @abstractmethod
    def mark_seen(self, source_id: str, content_hash: str) -> None: ...


class InMemoryDedupStore(DedupStore):
    """Default for local runs / unit tests. NOT for production —
    state is lost on restart. Swap for RedisDedupStore in deploy config."""

    def __init__(self) -> None:
        self._seen: set[str] = set()

    def seen(self, source_id: str, content_hash: str) -> bool:
        return f"{source_id}:{content_hash}" in self._seen

    def mark_seen(self, source_id: str, content_hash: str) -> None:
        self._seen.add(f"{source_id}:{content_hash}")


class RedisDedupStore(DedupStore):
    """Production dedup store. Requires `redis` package + REDIS_URL."""

    def __init__(self, redis_client, ttl_seconds: int = 60 * 60 * 24 * 30):
        self._r = redis_client
        self._ttl = ttl_seconds

    def _key(self, source_id: str, content_hash: str) -> str:
        return f"crawler:dedup:{source_id}:{content_hash}"

    def seen(self, source_id: str, content_hash: str) -> bool:
        return bool(self._r.exists(self._key(source_id, content_hash)))

    def mark_seen(self, source_id: str, content_hash: str) -> None:
        self._r.set(self._key(source_id, content_hash), 1, ex=self._ttl)


class ObjectStore(ABC):
    @abstractmethod
    def put_document(self, doc: CrawledDocument) -> str:
        """Persist the document; return the storage path/URI it was written to."""


class LocalFileObjectStore(ObjectStore):
    """Default for local verification runs. Writes one JSON file per document."""

    def __init__(self, base_dir: str = "./crawled_output"):
        self.base_dir = base_dir
        os.makedirs(base_dir, exist_ok=True)

    def put_document(self, doc: CrawledDocument) -> str:
        path = os.path.join(self.base_dir, f"{doc.source_id}__{doc.id}.json")
        with open(path, "w", encoding="utf-8") as f:
            f.write(doc.model_dump_json(indent=2))
        return path


class S3ObjectStore(ObjectStore):
    """Production object store — MinIO/S3, matching the platform's chosen
    backend. Requires `boto3` + bucket/credentials via env/deploy config."""

    def __init__(self, s3_client, bucket: str, prefix: str = "raw-documents"):
        self._s3 = s3_client
        self._bucket = bucket
        self._prefix = prefix

    def put_document(self, doc: CrawledDocument) -> str:
        key = f"{self._prefix}/{doc.source_id}/{doc.id}.json"
        self._s3.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=doc.model_dump_json().encode("utf-8"),
            ContentType="application/json",
        )
        return f"s3://{self._bucket}/{key}"
