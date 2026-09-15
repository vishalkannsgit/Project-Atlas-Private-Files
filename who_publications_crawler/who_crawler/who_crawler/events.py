"""
Event publishing — this is THE integration point with the rest of
Project Atlas. The crawler never calls the Ingestion service directly;
it only publishes an event. Whoever owns Ingestion & Data Quality just
needs to agree on this JSON shape and the topic name — nothing else
about this crawler is their concern.

Topic: atlas.documents.crawled
"""
from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from typing import Optional

from .models import CrawledDocument

logger = logging.getLogger("who_crawler.events")

TOPIC_DOCUMENT_CRAWLED = "atlas.documents.crawled"


class EventPublisher(ABC):
    @abstractmethod
    def publish_document_crawled(self, doc: CrawledDocument, storage_uri: str) -> None: ...


class KafkaEventPublisher(EventPublisher):
    """Production publisher. Requires `confluent-kafka` or `kafka-python`
    and a configured producer (injected, not constructed here, so tests
    never need a real broker)."""

    def __init__(self, producer, topic: str = TOPIC_DOCUMENT_CRAWLED):
        self._producer = producer
        self._topic = topic

    def publish_document_crawled(self, doc: CrawledDocument, storage_uri: str) -> None:
        payload = {
            "event_type": "document.crawled",
            "document_id": doc.id,
            "source_id": doc.source_id,
            "business_unit": doc.business_unit,
            "url": str(doc.url),
            "title": doc.title,
            "format": doc.format.value,
            "content_hash": doc.content_hash,
            "storage_uri": storage_uri,
            "crawled_at": doc.crawled_at.isoformat(),
            "owner": doc.owner,
            "version": doc.version,
            "access_rights": doc.access_rights,
        }
        # key by document id -> guarantees ordering/idempotent handling
        # per document if the topic is partitioned by key
        self._producer.produce(
            self._topic,
            key=doc.id.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
        )
        self._producer.flush()
        logger.info("published document.crawled id=%s url=%s", doc.id, doc.url)


class NullEventPublisher(EventPublisher):
    """No-op publisher for local verification / unit tests."""

    def publish_document_crawled(self, doc: CrawledDocument, storage_uri: str) -> None:
        logger.info(
            "[dry-run] would publish document.crawled id=%s storage_uri=%s",
            doc.id, storage_uri,
        )
