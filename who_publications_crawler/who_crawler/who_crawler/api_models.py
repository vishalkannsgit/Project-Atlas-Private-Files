"""
Some sources (like WHO Publications) render their listing page with
JavaScript, so a plain HTTP GET never sees the actual list — only the
empty template. Where the site exposes the underlying JSON API it uses
to fetch that data (visible in browser DevTools > Network tab), we call
that API directly instead of scraping HTML. Much more reliable, and
what most modern sites' "crawlers" actually do in practice.

This is a SEPARATE source type from CrawlSourceConfig (HTML+selectors).
Both produce the same CrawledDocument, so nothing downstream needs to
know which mode a given source uses.
"""
from __future__ import annotations

from pydantic import BaseModel, Field, HttpUrl


class ApiListingSourceConfig(BaseModel):
    source_id: str
    business_unit: str
    api_url: str = Field(..., description="Full URL of the JSON listing API, including query params")
    page_size: int = 50
    max_items: int = 200  # safety cap for a single run; raise once verified
    items_path: str = "value"  # key in the JSON response holding the list of items
    title_field: str = "Title"
    url_field: str = "ItemDefaultUrl"  # usually a partial path, needs a base joined
    url_base: str = "https://www.who.int/publications/i/item"
    date_field: str = "FormatedDate"
    download_field: str = "DownloadUrl"
    skip_param: str = "$skip"
    top_param: str = "$top"
    rate_limit_seconds: float = 1.5
    request_timeout_seconds: int = 20
    user_agent: str = "ProjectAtlasCrawler/1.0 (+internal-use)"
