import httpx

from who_crawler.api_crawler import ApiListingCrawler
from who_crawler.api_models import ApiListingSourceConfig
from who_crawler.events import NullEventPublisher
from who_crawler.metrics import CrawlerMetrics
from who_crawler.storage import InMemoryDedupStore, LocalFileObjectStore

PAGE_1 = {
    "value": [
        {"Title": "Item One", "ItemDefaultUrl": "/item-one", "FormatedDate": "1 Sept 2026", "Tag": "health"},
        {"Title": "Item Two", "ItemDefaultUrl": "/item-two", "FormatedDate": "2 Sept 2026", "Tag": "health"},
    ]
}
PAGE_2 = {"value": []}  # no more results

DETAIL_HTML_TEMPLATE = """
<html><body><article>This is enough content to pass the min-length check for {item}, used in tests.</article></body></html>
"""


def make_config(**overrides) -> ApiListingSourceConfig:
    base = dict(
        source_id="who_publications",
        business_unit="health-intelligence",
        api_url="https://example.org/api/hubs/publications",
        page_size=2,
        max_items=10,
        rate_limit_seconds=0,
    )
    base.update(overrides)
    return ApiListingSourceConfig(**base)


def transport_two_pages():
    def handler(request: httpx.Request) -> httpx.Response:
        if "api/hubs/publications" in str(request.url):
            skip = request.url.params.get("$skip", "0")
            return httpx.Response(200, json=PAGE_1 if skip == "0" else PAGE_2)
        # detail page - vary content by path so items aren't deduped as identical
        return httpx.Response(200, text=DETAIL_HTML_TEMPLATE.format(item=request.url.path))
    return httpx.MockTransport(handler)


def make_crawler(config, transport):
    client = httpx.Client(transport=transport, base_url="https://example.org")
    return ApiListingCrawler(
        config=config,
        dedup_store=InMemoryDedupStore(),
        object_store=LocalFileObjectStore(base_dir="/tmp/who_api_test_output"),
        event_publisher=NullEventPublisher(),
        metrics=CrawlerMetrics(),
        http_client=client,
    )


def test_happy_path_two_items():
    crawler = make_crawler(make_config(), transport_two_pages())
    result = crawler.run()
    assert result.documents_found == 2
    assert result.documents_new == 2
    assert result.pages_visited == 2  # page 1 with items, page 2 empty -> stop


def test_idempotent_rerun():
    config = make_config()
    dedup = InMemoryDedupStore()
    transport = transport_two_pages()

    r1 = ApiListingCrawler(
        config=config, dedup_store=dedup,
        object_store=LocalFileObjectStore(base_dir="/tmp/who_api_test_output2"),
        event_publisher=NullEventPublisher(), metrics=CrawlerMetrics(),
        http_client=httpx.Client(transport=transport, base_url="https://example.org"),
    ).run()
    assert r1.documents_new == 2

    r2 = ApiListingCrawler(
        config=config, dedup_store=dedup,
        object_store=LocalFileObjectStore(base_dir="/tmp/who_api_test_output2"),
        event_publisher=NullEventPublisher(), metrics=CrawlerMetrics(),
        http_client=httpx.Client(transport=transport, base_url="https://example.org"),
    ).run()
    assert r2.documents_new == 0
    assert r2.documents_skipped_duplicate == 2


def test_empty_first_page_is_not_an_error():
    def handler(request):
        return httpx.Response(200, json={"value": []})
    crawler = make_crawler(make_config(), httpx.MockTransport(handler))
    result = crawler.run()
    assert result.documents_found == 0
    assert result.errors == []


def test_api_error_recorded_not_crashing():
    def handler(request):
        return httpx.Response(500)
    crawler = make_crawler(make_config(), httpx.MockTransport(handler))
    result = crawler.run()
    assert result.documents_found == 0
    assert len(result.errors) > 0
