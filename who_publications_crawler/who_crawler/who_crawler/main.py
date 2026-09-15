"""
Entrypoint. This is what the Kubernetes CronJob (see deploy/cronjob.yaml)
actually invokes: `python -m who_crawler.main --source who_publications`

Wires real backends (Redis dedup, S3 object store, Kafka publisher) from
environment variables. Falls back to local/in-memory/no-op backends when
those env vars are absent, which is exactly what "local verification"
should run against.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

from prometheus_client import REGISTRY, start_http_server

from who_crawler.api_crawler import ApiListingCrawler
from who_crawler.crawler import PublicationsCrawler
from who_crawler.events import EventPublisher, NullEventPublisher
from who_crawler.metrics import CrawlerMetrics
from who_crawler.sources.who_publications import WHO_PUBLICATIONS_API_CONFIG
from who_crawler.storage import (
    DedupStore,
    InMemoryDedupStore,
    LocalFileObjectStore,
    ObjectStore,
)

# Each entry says which crawler engine to use for that source.
# "api" = ApiListingCrawler (JSON API, for JS-rendered listing pages)
# "html" = PublicationsCrawler (CSS-selector scraping, for static HTML)
SOURCES = {
    "who_publications": ("api", WHO_PUBLICATIONS_API_CONFIG),
    # "cdc_advisories": ("html", CDC_ADVISORIES_CONFIG),   <- added later
}


def build_dedup_store() -> DedupStore:
    redis_url = os.getenv("REDIS_URL")
    if redis_url:
        import redis
        from who_crawler.storage import RedisDedupStore
        return RedisDedupStore(redis.from_url(redis_url))
    logging.warning("REDIS_URL not set - using in-memory dedup (NOT safe for production)")
    return InMemoryDedupStore()


def build_object_store() -> ObjectStore:
    bucket = os.getenv("S3_BUCKET")
    if bucket:
        import boto3
        from who_crawler.storage import S3ObjectStore
        client = boto3.client(
            "s3",
            endpoint_url=os.getenv("S3_ENDPOINT_URL"),  # set for MinIO
            aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
        )
        return S3ObjectStore(client, bucket=bucket)
    logging.warning("S3_BUCKET not set - writing documents to ./crawled_output")
    return LocalFileObjectStore()


def build_event_publisher() -> EventPublisher:
    brokers = os.getenv("KAFKA_BROKERS")
    if brokers:
        from confluent_kafka import Producer
        from who_crawler.events import KafkaEventPublisher
        producer = Producer({"bootstrap.servers": brokers})
        return KafkaEventPublisher(producer)
    logging.warning("KAFKA_BROKERS not set - events will be logged, not published")
    return NullEventPublisher()


def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Project Atlas — Web Crawling workstream")
    parser.add_argument("--source", required=True, choices=list(SOURCES.keys()))
    parser.add_argument("--metrics-port", type=int, default=int(os.getenv("METRICS_PORT", "9090")))
    args = parser.parse_args()

    start_http_server(args.metrics_port)
    logging.info("metrics server listening on :%s/metrics", args.metrics_port)

    config_kind, config = SOURCES[args.source]
    crawler_cls = ApiListingCrawler if config_kind == "api" else PublicationsCrawler
    crawler = crawler_cls(
        config=config,
        dedup_store=build_dedup_store(),
        object_store=build_object_store(),
        event_publisher=build_event_publisher(),
        metrics=CrawlerMetrics(registry=REGISTRY),
    )

    result = crawler.run()
    logging.info(
        "DONE source=%s new=%d duplicate=%d failed=%d pages=%d",
        result.source_id, result.documents_new, result.documents_skipped_duplicate,
        result.documents_failed, result.pages_visited,
    )
    # Non-zero exit if the run failed entirely — lets the CronJob/alerting
    # system flag a broken crawler even if it "succeeded" with 0 documents.
    if result.errors:
        logging.error("run completed with %d error(s): %s", len(result.errors), result.errors)

    # Total failure: nothing was found AND something actually went wrong
    # (as opposed to a source that legitimately has zero new publications
    # today, which is a normal, healthy outcome).
    if result.documents_found == 0 and result.errors:
        logging.error("no documents found and errors occurred - treating run as failure")
        return 1
    if result.documents_found > 0 and result.documents_new == 0 and result.documents_skipped_duplicate == 0:
        logging.error("all documents failed - treating run as failure")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
