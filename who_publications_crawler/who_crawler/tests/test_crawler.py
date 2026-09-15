"""
Automated tests for the WHO Publications crawler component.

Run with: pytest -v
Uses httpx's MockTransport so no real network calls happen — this is
exactly what should run in CI and as part of local verification.
"""
import httpx
import pytest

from who_crawler.crawler import PublicationsCrawler
from who_crawler.events import NullEventPublisher
from who_crawler.metrics import CrawlerMetrics
from who_crawler.models import CrawlSourceConfig
from who_crawler.storage import InMemoryDedupStore, LocalFileObjectStore

LISTING_HTML = """
<html><body>
<a class="pub-link" href="/article-1">Article 1</a>
<a class="pub-link" href="/article-2">Article 2</a>
</body></html>
"""

ARTICLE_HTML = """
<html><body>
<h1>{title}</h1>
<article>{body}</article>
</body></html>
"""

ROBOTS_ALLOW_ALL = "User-agent: *\nAllow: /\n"
ROBOTS_DISALLOW_ARTICLE_2 = "User-agent: *\nDisallow: /article-2\n"

ARTICLE_BODIES = {
    "/article-1": "This is enough content to pass the min-length check for article one, used in tests.",
    "/article-2": "This is enough content to pass the min-length check for article two, used in tests.",
}


def make_config(**overrides) -> CrawlSourceConfig:
    base = dict(
        source_id="who_publications",
        business_unit="health-intelligence",
        listing_url="https://example.org/publications",
        link_selector="a.pub-link",
        title_selector="h1",
        body_selector="article",
        rate_limit_seconds=0,  # no sleeping in tests
        respect_robots_txt=True,
    )
    base.update(overrides)
    return CrawlSourceConfig(**base)


def transport_for(robots_txt: str, article_body: str | None = None):
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/robots.txt":
            return httpx.Response(200, text=robots_txt)
        if path == "/publications":
            return httpx.Response(200, text=LISTING_HTML)
        if path in ("/article-1", "/article-2"):
            body = article_body if article_body is not None else ARTICLE_BODIES[path]
            return httpx.Response(200, text=ARTICLE_HTML.format(title=f"Title {path}", body=body))
        return httpx.Response(404)
    return httpx.MockTransport(handler)


def make_crawler(config, transport, **kwargs):
    client = httpx.Client(transport=transport, base_url="https://example.org")
    return PublicationsCrawler(
        config=config,
        dedup_store=kwargs.get("dedup_store") or InMemoryDedupStore(),
        object_store=kwargs.get("object_store") or LocalFileObjectStore(base_dir="/tmp/who_crawler_test_output"),
        event_publisher=kwargs.get("event_publisher") or NullEventPublisher(),
        metrics=CrawlerMetrics(),
        http_client=client,
    )


def test_happy_path_crawls_and_emits_two_documents():
    config = make_config()
    crawler = make_crawler(config, transport_for(ROBOTS_ALLOW_ALL))
    result = crawler.run()

    assert result.documents_found == 2
    assert result.documents_new == 2
    assert result.documents_failed == 0
    assert result.documents_skipped_duplicate == 0


def test_respects_robots_txt_disallow():
    config = make_config()
    crawler = make_crawler(config, transport_for(ROBOTS_DISALLOW_ARTICLE_2))
    result = crawler.run()

    # article-2 is disallowed -> only article-1 counted as new
    assert result.documents_found == 2
    assert result.documents_new == 1


def test_idempotent_rerun_produces_no_new_documents():
    config = make_config()
    dedup = InMemoryDedupStore()
    transport = transport_for(ROBOTS_ALLOW_ALL)

    crawler1 = make_crawler(config, transport, dedup_store=dedup)
    first = crawler1.run()
    assert first.documents_new == 2

    crawler2 = make_crawler(config, transport, dedup_store=dedup)
    second = crawler2.run()
    assert second.documents_new == 0
    assert second.documents_skipped_duplicate == 2


def test_short_content_is_skipped_not_emitted():
    config = make_config()
    crawler = make_crawler(config, transport_for(ROBOTS_ALLOW_ALL, article_body="too short"))
    result = crawler.run()

    assert result.documents_new == 0
    assert result.documents_failed == 2  # parsed but rejected for thin content


def test_malformed_source_config_rejected():
    with pytest.raises(Exception):
        CrawlSourceConfig(source_id="x")  # missing required fields


def test_transient_http_errors_are_retried(monkeypatch):
    calls = {"n": 0}

    def flaky_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text=ROBOTS_ALLOW_ALL)
        if request.url.path == "/publications":
            calls["n"] += 1
            if calls["n"] < 2:
                return httpx.Response(503)
            return httpx.Response(200, text=LISTING_HTML)
        if request.url.path in ("/article-1", "/article-2"):
            return httpx.Response(200, text=ARTICLE_HTML.format(
                title=f"Title {request.url.path}", body=ARTICLE_BODIES[request.url.path]
            ))
        return httpx.Response(404)

    config = make_config()
    crawler = make_crawler(config, httpx.MockTransport(flaky_handler))
    result = crawler.run()

    assert calls["n"] >= 2  # first 503 was retried, not fatal
    assert result.documents_new == 2
