"""
PublicationsCrawler — a generic, config-driven crawler for "listing page
-> article pages" style sources (publication feeds, press releases,
advisories, etc).

WHO Publications is the FIRST configured source, not a special case.
Adding CDC or NIH later means writing a new CrawlSourceConfig, not new
crawler logic — this is what keeps the component domain-independent.
"""
from __future__ import annotations

import logging
import time
import urllib.robotparser as robotparser
from datetime import datetime, timezone
from typing import Iterator, Optional
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .events import EventPublisher, NullEventPublisher
from .metrics import CrawlerMetrics
from .models import CrawledDocument, CrawlRunResult, CrawlSourceConfig, DocumentFormat
from .storage import DedupStore, InMemoryDedupStore, ObjectStore, LocalFileObjectStore

logger = logging.getLogger("who_crawler")


class RobotsDisallowed(Exception):
    """Raised when robots.txt forbids fetching a URL."""


class TransientFetchError(Exception):
    """Network/5xx errors worth retrying."""


class PublicationsCrawler:
    """
    Contract
    --------
    Input:  a CrawlSourceConfig (see models.py)
    Output: yields CrawledDocument objects AND, as a side effect,
            (a) persists each document via `object_store`
            (b) publishes a `document.crawled` event via `event_publisher`
            (c) records metrics via `metrics`
    Idempotency:
        Re-running against the same source is safe. Documents are
        identified by a deterministic id (hash of source_id + url) and
        deduped by content hash, so an unchanged page is fetched but
        never re-emitted downstream, and a changed page is emitted again
        with `version` bumped by the caller if desired.
    Failure handling:
        - robots.txt disallow -> page skipped, logged, counted, no raise
        - HTTP 4xx (except 429) -> page skipped, logged, counted
        - HTTP 429/5xx/timeouts -> retried with exponential backoff (3x),
          then skipped and counted as failed if still failing
        - malformed/empty content -> skipped, counted, never sent downstream
    """

    def __init__(
        self,
        config: CrawlSourceConfig,
        dedup_store: Optional[DedupStore] = None,
        object_store: Optional[ObjectStore] = None,
        event_publisher: Optional[EventPublisher] = None,
        metrics: Optional[CrawlerMetrics] = None,
        http_client: Optional[httpx.Client] = None,
    ) -> None:
        self.config = config
        self.dedup_store = dedup_store or InMemoryDedupStore()
        self.object_store = object_store or LocalFileObjectStore()
        self.event_publisher = event_publisher or NullEventPublisher()
        self.metrics = metrics or CrawlerMetrics()
        self._client = http_client or httpx.Client(
            timeout=config.request_timeout_seconds,
            headers={"User-Agent": config.user_agent},
            follow_redirects=True,
        )
        self._robots = self._load_robots() if config.respect_robots_txt else None

    # ---------- robots.txt ----------

    def _load_robots(self) -> robotparser.RobotFileParser:
        parsed = urlparse(str(self.config.listing_url))
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        rp = robotparser.RobotFileParser()
        try:
            resp = self._client.get(robots_url)
            rp.parse(resp.text.splitlines())
            logger.info("loaded robots.txt from %s", robots_url)
        except Exception as exc:  # network issue reading robots.txt itself
            logger.warning("could not fetch robots.txt (%s) - defaulting to disallow-all", exc)
            rp.disallow_all = True
        return rp

    def _check_allowed(self, url: str) -> None:
        if self._robots and not self._robots.can_fetch(self.config.user_agent, url):
            raise RobotsDisallowed(url)

    # ---------- HTTP with retry ----------

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(TransientFetchError),
    )
    def _fetch(self, url: str) -> httpx.Response:
        try:
            resp = self._client.get(url)
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise TransientFetchError(str(exc)) from exc

        if resp.status_code == 429 or resp.status_code >= 500:
            raise TransientFetchError(f"HTTP {resp.status_code} from {url}")
        resp.raise_for_status()  # raises for other 4xx, not retried
        return resp

    # ---------- crawl logic ----------

    def _iter_listing_pages(self) -> Iterator[str]:
        url = str(self.config.listing_url)
        for _ in range(self.config.max_pages):
            yield url
            if not self.config.next_page_selector:
                return
            resp = self._fetch(url)
            soup = BeautifulSoup(resp.text, "html.parser")
            next_link = soup.select_one(self.config.next_page_selector)
            if not next_link or not next_link.get("href"):
                return
            url = urljoin(url, next_link["href"])
            time.sleep(self.config.rate_limit_seconds)

    def _extract_links(self, listing_url: str) -> list[str]:
        resp = self._fetch(listing_url)
        soup = BeautifulSoup(resp.text, "html.parser")
        links = []
        for a in soup.select(self.config.link_selector):
            href = a.get("href")
            if href:
                links.append(urljoin(listing_url, href))
        return links

    def _parse_article(self, url: str) -> Optional[CrawledDocument]:
        resp = self._fetch(url)
        soup = BeautifulSoup(resp.text, "html.parser")

        title_el = soup.select_one(self.config.title_selector) if self.config.title_selector else None
        body_el = soup.select_one(self.config.body_selector) if self.config.body_selector else None

        title = title_el.get_text(strip=True) if title_el else url
        content = body_el.get_text(separator="\n", strip=True) if body_el else ""

        if not content or len(content) < 40:
            logger.warning("skipping %s - extracted content too short/empty", url)
            return None

        published_date = None
        if self.config.date_selector:
            date_el = soup.select_one(self.config.date_selector)
            if date_el and date_el.get_text(strip=True):
                published_date = self._try_parse_date(date_el.get_text(strip=True))

        content_hash = CrawledDocument.hash_content(content)
        return CrawledDocument(
            id=CrawledDocument.make_id(self.config.source_id, url),
            source_id=self.config.source_id,
            business_unit=self.config.business_unit,
            url=url,
            title=title,
            content=content,
            format=DocumentFormat.HTML,
            published_date=published_date,
            content_hash=content_hash,
        )

    @staticmethod
    def _try_parse_date(raw: str) -> Optional[datetime]:
        for fmt in ("%d %B %Y", "%B %d, %Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return None

    def run(self) -> CrawlRunResult:
        started_at = datetime.now(timezone.utc)
        pages_visited = 0
        found = new = dup = failed = 0
        errors: list[str] = []

        try:
            listing_iter = self._iter_listing_pages()
            while True:
                try:
                    listing_url = next(listing_iter)
                except StopIteration:
                    break
                except Exception as exc:
                    # a failure fetching a pagination ("next") page — stop
                    # paginating but keep whatever was already collected
                    errors.append(f"pagination fetch failed: {exc}")
                    self.metrics.pages_failed.inc()
                    logger.error("pagination fetch failed: %s", exc)
                    break

                try:
                    self._check_allowed(listing_url)
                except RobotsDisallowed:
                    logger.info("robots.txt disallows listing page %s - stopping", listing_url)
                    break

                pages_visited += 1
                try:
                    article_urls = self._extract_links(listing_url)
                except Exception as exc:
                    errors.append(f"listing page failed {listing_url}: {exc}")
                    self.metrics.pages_failed.inc()
                    logger.error("listing page failed %s: %s", listing_url, exc)
                    continue

                for article_url in article_urls:
                    found += 1
                    try:
                        self._check_allowed(article_url)
                    except RobotsDisallowed:
                        logger.info("robots.txt disallows %s - skipping", article_url)
                        continue

                    try:
                        doc = self._parse_article(article_url)
                    except Exception as exc:
                        failed += 1
                        errors.append(f"{article_url}: {exc}")
                        self.metrics.documents_failed.inc()
                        logger.error("failed to crawl %s: %s", article_url, exc)
                        continue
                    finally:
                        time.sleep(self.config.rate_limit_seconds)

                    if doc is None:
                        failed += 1
                        continue

                    if self.dedup_store.seen(doc.source_id, doc.content_hash):
                        dup += 1
                        self.metrics.documents_duplicate.inc()
                        continue

                    storage_uri = self.object_store.put_document(doc)
                    self.event_publisher.publish_document_crawled(doc, storage_uri)
                    self.dedup_store.mark_seen(doc.source_id, doc.content_hash)
                    new += 1
                    self.metrics.documents_new.inc()
        except Exception as exc:
            # Last-resort safety net: a scheduled job must never crash the
            # pod with a bare traceback. Record it, exit run() cleanly, let
            # main.py's exit-code logic decide how loud to alert.
            errors.append(f"unexpected crawler error: {exc}")
            logger.exception("unexpected error during crawl run")
        finally:
            finished_at = datetime.now(timezone.utc)

        result = CrawlRunResult(
            source_id=self.config.source_id,
            started_at=started_at,
            finished_at=finished_at,
            pages_visited=pages_visited,
            documents_found=found,
            documents_new=new,
            documents_skipped_duplicate=dup,
            documents_failed=failed,
            errors=errors,
        )
        logger.info("crawl run complete: %s", result.model_dump_json())
        self.metrics.run_duration_seconds.observe(
            (finished_at - started_at).total_seconds()
        )
        return result
