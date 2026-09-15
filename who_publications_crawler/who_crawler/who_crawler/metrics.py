"""
Metrics — exposed on /metrics for Prometheus scrape, feeding the
Observability & Operations workstream's dashboards/alerts.
"""
from prometheus_client import CollectorRegistry, Counter, Histogram, REGISTRY


class CrawlerMetrics:
    """
    One instance is created per process in production (main.py), registered
    against the global REGISTRY that /metrics serves. Tests construct many
    instances in-process, so each test gets its own private registry to
    avoid 'duplicate timeseries' errors — pass `registry=REGISTRY` explicitly
    in main.py if you want them scraped process-wide.
    """
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        reg = registry if registry is not None else CollectorRegistry()
        self.registry = reg
        self.documents_new = Counter(
            "crawler_documents_new_total", "New documents successfully crawled and published",
            registry=reg,
        )
        self.documents_duplicate = Counter(
            "crawler_documents_duplicate_total", "Documents skipped because content was already seen",
            registry=reg,
        )
        self.documents_failed = Counter(
            "crawler_documents_failed_total", "Documents that failed to fetch/parse after retries",
            registry=reg,
        )
        self.pages_failed = Counter(
            "crawler_listing_pages_failed_total", "Listing pages that failed to fetch",
            registry=reg,
        )
        self.run_duration_seconds = Histogram(
            "crawler_run_duration_seconds", "Wall-clock duration of a full crawl run",
            registry=reg,
        )
