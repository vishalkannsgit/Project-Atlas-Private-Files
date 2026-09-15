"""
ApiListingCrawler — same output contract, storage, events, retries, and
dedup as PublicationsCrawler, but pulls the item list from a JSON API
instead of scraping HTML. Use this for sources whose listing page is
rendered client-side (WHO Publications is the first case).
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .api_models import ApiListingSourceConfig
from .events import EventPublisher, NullEventPublisher
from .metrics import CrawlerMetrics
from .models import CrawledDocument, CrawlRunResult, DocumentFormat
from .storage import DedupStore, InMemoryDedupStore, ObjectStore, LocalFileObjectStore
from .crawler import TransientFetchError

logger = logging.getLogger("who_crawler.api")


class ApiListingCrawler:
    def __init__(
        self,
        config: ApiListingSourceConfig,
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

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(TransientFetchError),
    )
    def _fetch_json(self, url: str) -> dict:
        try:
            resp = self._client.get(url)
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise TransientFetchError(str(exc)) from exc
        if resp.status_code == 429 or resp.status_code >= 500:
            raise TransientFetchError(f"HTTP {resp.status_code} from {url}")
        resp.raise_for_status()
        return resp.json()

    def _page_url(self, skip: int) -> str:
        sep = "&" if "?" in self.config.api_url else "?"
        return f"{self.config.api_url}{sep}{self.config.skip_param}={skip}&{self.config.top_param}={self.config.page_size}"

    def _extract_items(self, payload: dict) -> list[dict]:
        node = payload
        for key in self.config.items_path.split("."):
            if isinstance(node, dict) and key in node:
                node = node[key]
            else:
                return []
        return node if isinstance(node, list) else []

    def _fetch_detail_content(self, url: str) -> str:
        """Best-effort fetch of the item's own detail page for body text.
        Detail pages are typically server-rendered even when the listing
        page is client-rendered. Falls back to empty string on failure —
        the title/metadata alone still makes a valid (if thin) document."""
        try:
            resp = self._client.get(url)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            body = soup.select_one("div.sf-detail-body-wrapper, article, main")
            return body.get_text(separator="\n", strip=True) if body else ""
        except Exception as exc:
            logger.warning("could not fetch detail page %s: %s", url, exc)
            return ""

    def run(self) -> CrawlRunResult:
        started_at = datetime.now(timezone.utc)
        found = new = dup = failed = 0
        pages_visited = 0
        errors: list[str] = []

        try:
            skip = 0
            while skip < self.config.max_items:
                page_url = self._page_url(skip)
                try:
                    payload = self._fetch_json(page_url)
                except Exception as exc:
                    errors.append(f"API page failed (skip={skip}): {exc}")
                    self.metrics.pages_failed.inc()
                    logger.error("API page failed (skip=%s): %s", skip, exc)
                    break

                pages_visited += 1
                items = self._extract_items(payload)
                if not items:
                    break  # no more results

                for item in items:
                    found += 1
                    try:
                        title = item.get(self.config.title_field) or "Untitled"
                        rel_url = item.get(self.config.url_field) or ""
                        url = urljoin(self.config.url_base + "/", rel_url.lstrip("/"))
                        date_str = item.get(self.config.date_field)

                        body = self._fetch_detail_content(url)
                        content = body if len(body) >= 40 else f"{title}\n\n{item.get('Tag', '')}".strip()

                        if len(content) < 10:
                            failed += 1
                            continue

                        content_hash = CrawledDocument.hash_content(content)
                        doc = CrawledDocument(
                            id=CrawledDocument.make_id(self.config.source_id, url),
                            source_id=self.config.source_id,
                            business_unit=self.config.business_unit,
                            url=url,
                            title=title,
                            content=content,
                            format=DocumentFormat.HTML,
                            content_hash=content_hash,
                        )
                    except Exception as exc:
                        failed += 1
                        errors.append(f"item failed: {exc}")
                        self.metrics.documents_failed.inc()
                        continue
                    finally:
                        time.sleep(self.config.rate_limit_seconds)

                    if self.dedup_store.seen(doc.source_id, doc.content_hash):
                        dup += 1
                        self.metrics.documents_duplicate.inc()
                        continue

                    storage_uri = self.object_store.put_document(doc)
                    self.event_publisher.publish_document_crawled(doc, storage_uri)
                    self.dedup_store.mark_seen(doc.source_id, doc.content_hash)
                    new += 1
                    self.metrics.documents_new.inc()

                if len(items) < self.config.page_size:
                    break  # last page
                skip += self.config.page_size
        except Exception as exc:
            errors.append(f"unexpected crawler error: {exc}")
            logger.exception("unexpected error during API crawl run")
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
        logger.info("API crawl run complete: %s", result.model_dump_json())
        self.metrics.run_duration_seconds.observe((finished_at - started_at).total_seconds())
        return result
