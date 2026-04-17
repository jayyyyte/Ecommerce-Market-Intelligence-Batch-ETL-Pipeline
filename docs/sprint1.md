# Sprint 1 — Definition of Done Review

**Sprint:** Foundation & Data Extraction

---

## Acceptance Criteria

| # | Criterion | Status | Evidence / Notes |
|---|-----------|--------|------------------|
| 1 | `docker compose up` brings Airflow UI, PostgreSQL, and Metabase online without errors | Done | Airflow at localhost:8080, Metabase at localhost:3000, PostgreSQL at :5432 |
| 2 | All 3 PostgreSQL tables created with correct schema and constraints | Done | `sql/create_tables.sql` defines `products_market` (UNIQUE on product_id+source+extraction_date), `pipeline_runs`, `rejected_records`. Verify with `\dt` + `\d products_market` in psql |
| 3 | Airflow Connection `postgres_ecommerce` and all Variables configured | Done | See README → Quick Start Step 5. Variables: `DATA_SOURCE_URL`, `STAGING_DIR`, `SLACK_WEBHOOK_URL`, `MAX_RETRIES`, `PIPELINE_ENV` |
| 4 | `extractor.py` fetches real data and saves raw JSON to `staging/` directory | Done | `python -m etl.extractor` → `staging/{date}/raw_products.json` created (confirmed manually) |
| 5 | Retry decorator correctly retries on timeout/5xx with exponential backoff (unit test passes) | Done | `TestRetryWithBackoff::test_retries_on_timeout_succeeds_on_third` + `test_exponential_backoff_delay_sequence` both pass |
| 6 | `tests/test_extractor.py`: all tests pass with 0 failures | Done | 35 passed — `pytest tests/test_extractor.py -v` |
| 7 | No credentials or secrets committed to Git | Done | `.env` in `.gitignore`. `grep -r 'password\|secret' dags/ etl/ utils/` → 0 results |
| 8 | README updated with 'Getting Started' section covering Docker Compose setup | Done | `README.md` — sections: Quick Start, Architecture, Data Sources, Testing, Troubleshooting |

**Result: 8 / 8 criteria met **

---

## Sprint Goal — Verification

| Can the developer… | Status |
|--------------------|--------|
| Run the full Docker Compose stack (Airflow + PostgreSQL + Metabase) locally? | Yes |
| Execute a Python script that fetches real product data from the chosen source? | Yes — `python -m etl.extractor` |
| Verify that raw data is saved to a staging file (JSON/CSV)? | Yes — `staging/{date}/raw_products.json` |
| Confirm PostgreSQL tables are created with correct schema? | Yes — via `\dt` + `\d table_name` in psql |

---

## Bonus Deliverables (Not in original DoD)

| Item | Status |
|------|--------|
| TikiScraper — real market data source with UA rotation & 1.5 s delay | Done — `etl/tiki_scraper.py` |
| 49 unit tests for TikiScraper — all HTTP mocked, 0 failures | Done — `tests/test_tiki_scraper.py` |
| Pagination duplicate bug caught and fixed with regression test | Done — `test_exact_page_limit_does_not_duplicate` |
| `config/schema_spec.yaml` — full field spec for both sources | Done |

---

## Test Summary

```
$ python -m pytest tests/ -v

84 passed in 1.26s

Breakdown:
  tests/test_extractor.py    35 tests   (retry, FakeStore, validate, enrich, save)
  tests/test_tiki_scraper.py 49 tests   (validate, parse, category, stock, fetch, extract)
```

---

## Files Produced in Sprint 1

```
etl/
  extractor.py      BaseExtractor + FakeStoreExtractor (single-fetch, atomic write, retry)
  tiki_scraper.py   TikiScraper (paginated, UA rotation, graceful skip, BS4 ready)

utils/
  retry.py          @retry_with_backoff — 30/60/120 s backoff + typed exceptions

config/
  schema_spec.yaml  Expected columns, types, critical fields for both sources

tests/
  test_extractor.py     35 tests
  test_tiki_scraper.py  49 tests

README_Sprint1.md           Quick Start, Architecture, Data Sources, Testing, Troubleshooting
```

---

## Carry-Forward Notes for Sprint 2

- `schema_spec.yaml` is ready to be consumed by **Pandera** in `transformer.py`
- `stock_status` from FakeStore defaults to `"unknown"` — transformer's `fill_defaults()` will handle this correctly
- `source_url` in FakeStore records points to the product image URL (no real product page in FakeStore) — documented in both code and README
- Tiki prices are in **VND** — transformer must NOT mix with FakeStore USD prices; add a `currency` field or keep sources separate in dashboards
