"""
Data contracts for the Web Crawling workstream.

Every source-specific crawler (WHO, CDC, NIH, ...) must consume a
`CrawlSourceConfig` and produce a stream of `CrawledDocument` objects.
Downstream consumers (Ingestion & Data Quality, Document Processing)
depend ONLY on this shape — never on how a specific crawler works
internally. This is the "contract" referenced in the architecture doc.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, HttpUrl, field_validator


class DocumentFormat(str, Enum):
    HTML = "html"
    PDF = "pdf"
    JSON = "json"


class CrawlSourceConfig(BaseModel):
    """
    Declarative config for one crawlable source. A new source (CDC, NIH,
    a non-health regulator, a company's press-release page...) is added
    by writing a new config, NOT new crawler code. This is what keeps the
    crawler domain-independent.
    """
    source_id: str = Field(..., description="Stable id, e.g. 'who_publications'")
    business_unit: str = Field(..., description="Owning team/unit for audit metadata")
    listing_url: HttpUrl
    link_selector: str = Field(..., description="CSS selector for publication links on the listing page")
    next_page_selector: Optional[str] = Field(None, description="CSS selector for pagination 'next' link")
    title_selector: Optional[str] = "h1"
    body_selector: Optional[str] = "article, main, body"
    date_selector: Optional[str] = None
    max_pages: int = 5
    rate_limit_seconds: float = 1.5
    respect_robots_txt: bool = True
    request_timeout_seconds: int = 20
    user_agent: str = "ProjectAtlasCrawler/1.0 (+internal-use)"


class CrawledDocument(BaseModel):
    """
    The ONLY thing downstream stages are allowed to depend on.
    Matches the metadata fields called out in the Project Atlas spec:
    id, filename/source, type, source, business unit, owner, upload date,
    version, access rights, status.
    """
    id: str
    source_id: str
    business_unit: str
    url: HttpUrl
    title: str
    content: str
    format: DocumentFormat = DocumentFormat.HTML
    published_date: Optional[datetime] = None
    crawled_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    content_hash: str
    owner: str = "web-crawling-workstream"
    version: int = 1
    access_rights: str = "internal"
    status: str = "crawled"  # crawled -> ingested -> processed ... (set by downstream)

    @field_validator("content")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("content must not be empty")
        return v

    @staticmethod
    def make_id(source_id: str, url: str) -> str:
        return hashlib.sha256(f"{source_id}:{url}".encode()).hexdigest()[:24]

    @staticmethod
    def hash_content(content: str) -> str:
        return hashlib.sha256(content.encode("utf-8")).hexdigest()


class CrawlRunResult(BaseModel):
    """Summary emitted at the end of every crawl run — used for logging,
    metrics, and the Kafka 'crawl completed' event."""
    source_id: str
    started_at: datetime
    finished_at: datetime
    pages_visited: int
    documents_found: int
    documents_new: int
    documents_skipped_duplicate: int
    documents_failed: int
    errors: list[str] = Field(default_factory=list)
