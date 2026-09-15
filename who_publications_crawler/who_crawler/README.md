# WHO Publications Crawler — Web Crawling Workstream

Production component for Project Atlas, Sprint 02 (Data Acquisition).
WHO Publications is the **first configured source** on a generic,
domain-independent crawling engine — CDC, NIH, or any non-health source
plugs in via a new config file, not new code.

## Component Contract

**Input:** either an `ApiListingSourceConfig` (JSON API listing, e.g. WHO —
see `who_crawler/api_models.py`) or a `CrawlSourceConfig` (HTML + CSS
selectors, for sources without a listing API — see `who_crawler/models.py`).
WHO uses the API mode because its publications listing page is rendered
client-side by JavaScript; a plain HTTP fetch never sees the actual list,
only the empty loading template. WHO's own JSON API (visible in browser
DevTools > Network tab while browsing the page) is called directly instead.

**Output:** a stream of `CrawledDocument` objects, each with:
`id, source_id, business_unit, url, title, content, format, published_date,
crawled_at, content_hash, owner, version, access_rights, status` — matching
the metadata fields defined in the Project Atlas spec. Both crawler engines
(`ApiListingCrawler`, `PublicationsCrawler`) produce exactly this shape, so
downstream consumers never need to know which mode a given source uses.

**Side effects per document:**
1. Written to object storage (MinIO/S3, or local disk for verification)
2. `document.crawled` event published to Kafka topic `atlas.documents.crawled`
   — this is the integration seam with Ingestion & Data Quality. That team
   only needs to agree on this JSON shape (see `who_crawler/events.py`),
   never on this crawler's internals.

## Idempotency & Failure Handling

- **Dedup:** documents are identified by `sha256(source_id + url)`, and
  content is fingerprinted by `sha256(content)`. Re-running the same crawl
  never re-emits an unchanged page downstream.
- **robots.txt:** checked before every fetch. Disallowed URLs are skipped
  and logged, never fetched.
- **Retries:** HTTP 429/5xx and network/timeout errors are retried 3x with
  exponential backoff (`tenacity`). Other 4xx errors are not retried.
- **Thin/empty content:** rejected before it ever reaches storage or Kafka.
- **Run-level failure:** if every document in a run fails, the process
  exits non-zero so the CronJob/alerting layer sees it as a failed run
  even though no exception escaped the loop.

## Local Verification (what you run before marking the ClickUp task done)

```bash
pip install -r requirements.txt
pytest -v                                   # 1. automated tests, no network
python -m who_crawler.main --source who_publications   # 2. real run, local backends
cat crawled_output/*.json | head -50        # 3. spot-check output shape
```

With no `REDIS_URL` / `S3_BUCKET` / `KAFKA_BROKERS` set, step 2 runs fully
locally: in-memory dedup, JSON files in `./crawled_output/`, and events are
just logged instead of published — safe to run repeatedly on your machine.

**Before you check this off as verified, also confirm against the real
WHO site (not just the mocked tests):**
- [ ] The API URL in `who_publications.py` (`sf_site` param, field names)
      still matches what WHO's site actually calls — open DevTools >
      Network on https://www.who.int/publications/i and confirm.
- [ ] `robots.txt` on `who.int` is actually being fetched and honored.
- [ ] A full run completes and produces sane-looking documents in
      `crawled_output/` — check titles and body text are real content,
      not empty or truncated.
- [ ] Once confirmed working, raise `max_items` in `who_publications.py`
      beyond the initial safety cap of 200 to actually cover the full
      12,000+ publication backlog (paginate via `$skip`/`$top`).

## Observability

Metrics exposed on `:9090/metrics` (Prometheus format):
`crawler_documents_new_total`, `crawler_documents_duplicate_total`,
`crawler_documents_failed_total`, `crawler_listing_pages_failed_total`,
`crawler_run_duration_seconds`.

## Deployment

- `Dockerfile` — builds the image, runs as non-root.
- `deploy/cronjob.yaml` — Kubernetes CronJob (daily), plus a
  `ServiceMonitor` so Prometheus scrapes `/metrics` automatically.
  `concurrencyPolicy: Forbid` prevents overlapping runs of the same source.

## Adding a New Source (e.g. CDC)

1. Create `who_crawler/sources/cdc_advisories.py` with a `CrawlSourceConfig`.
2. Register it in `SOURCES` in `who_crawler/main.py`.
3. Add a second CronJob in `deploy/` pointing `--source cdc_advisories`.

No changes to `crawler.py`, `models.py`, `events.py`, or `storage.py` are
needed — that's the domain-independence guarantee.
