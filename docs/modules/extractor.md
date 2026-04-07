# Module: `etl/extractor.py`

## Overview

Responsible for fetching raw product data from one or more e-commerce sources (REST API or web scraping). Applies retry-with-backoff on network failures, enforces rate limiting between page requests, and persists raw data atomically to the staging directory before any transformation occurs.

---

## Responsibilities

### ✅ Handles
- Connecting to the configured data source (`DATA_SOURCE_URL`)
- Paginating through all result pages until the source is exhausted
- Enriching every raw row with `extraction_timestamp` and `source` fields
- Retrying on: `requests.Timeout`, `requests.ConnectionError`, HTTP 429, HTTP 5xx
- Applying exponential backoff between retries (30s → 60s → 120s + jitter)
- Respecting rate limits via configurable per-page delay
- Writing raw data to `staging/{execution_date}/raw_products.json` atomically (write to `.tmp`, then rename)
- Logging rows fetched per page, total rows, and total request duration

### ❌ Does NOT handle
- Data cleaning, validation, or type coercion (→ `etl/transformer.py`)
- Loading data into PostgreSQL (→ `etl/loader.py`)
- Sending alerts on failure (→ `etl/notifier.py`)
- Constructing or managing the staging directory (caller must ensure it exists)

---

## Structure

```
etl/
└── extractor.py
    ├── class BaseExtractor          # Abstract base — defines the extraction contract
    │   ├── extract(execution_date)  # Main entrypoint; returns List[dict]
    │   ├── fetch_page(page)         # Fetches a single page; implemented by subclasses
    │   ├── validate_response(data)  # Checks response has expected fields
    │   └── save_raw(data, path)     # Atomic JSON write to staging
    ├── class FakeStoreExtractor     # Concrete impl for fakestoreapi.com
    └── class TikiScraper            # Concrete impl for Tiki (optional, scraping)

utils/
└── retry.py
    └── retry_with_backoff(...)      # Decorator used by fetch_page
```

---

## Usage

Called from the `extract_data` Airflow task. The task receives the logical execution date (`ds`) from Airflow context and passes it to the extractor.

```python
from etl.extractor import FakeStoreExtractor
from etl.config import get_config

def extract_data(**context):
    cfg = get_config()
    execution_date = context["ds"]          # e.g. "2025-06-15"

    extractor = FakeStoreExtractor(cfg)
    rows = extractor.extract(execution_date) # List[dict]

    # Pass run metadata to downstream tasks
    context["ti"].xcom_push(key="row_count", value=len(rows))
    context["ti"].xcom_push(key="staging_path",
                            value=f"{cfg.staging_dir}/{execution_date}/raw_products.json")
    return len(rows)
```

---

## API

### `class BaseExtractor`

Abstract base class. Subclasses must implement `fetch_page()`.

| Method | Signature | Description |
|---|---|---|
| `extract` | `(execution_date: str) → List[dict]` | Orchestrates full extraction: paginate → enrich → save |
| `fetch_page` | `(page: int) → List[dict]` | **Abstract.** Fetches one page from the source |
| `validate_response` | `(data: Any) → bool` | Returns `True` if response has expected structure |
| `save_raw` | `(data: List[dict], path: Path) → None` | Atomic write: `.tmp` file → rename to final path |

---

### `class FakeStoreExtractor(BaseExtractor)`

Concrete extractor for `https://fakestoreapi.com/products`.

| Method | Description |
|---|---|
| `fetch_page(page)` | GETs `/products?limit=20&skip={offset}`. Returns `[]` when exhausted. |

**Output row shape (per product):**

```python
{
    "product_id":         "1",
    "name":               "Fjallraven - Foldsack No. 1 Backpack",
    "category":           "men's clothing",
    "price":              109.95,
    "rating":             3.9,
    "review_count":       120,
    "stock_status":       "in_stock",       # derived — always in_stock for this source
    "source":             "fakestore_api",
    "source_url":         "https://fakestoreapi.com/products/1",
    "extraction_timestamp": "2025-06-15T00:05:32Z"
}
```

---

### `class TikiScraper(BaseExtractor)` *(optional)*

Web scraper for `tiki.vn`. Uses `requests` + `BeautifulSoup`. Respects `rate_limit_delay` from config between page requests.

| Method | Description |
|---|---|
| `fetch_page(page)` | GETs Tiki category listing page, parses HTML for product cards |

---

### `utils/retry.py` — `@retry_with_backoff`

```python
@retry_with_backoff(max_retries=3, base_delay=30)
def fetch_page(self, page: int) -> List[dict]:
    ...
```

| Parameter | Default | Description |
|---|---|---|
| `max_retries` | `3` | Total attempts before raising |
| `base_delay` | `30` | Seconds for first retry; doubles each attempt |

**Retry behaviour by exception type:**

| Exception | Action |
|---|---|
| `requests.Timeout` | Retry with backoff |
| `requests.ConnectionError` | Retry with backoff |
| `HTTP 429` | Raise `RateLimitError` → Airflow retries after 60s |
| `HTTP 5xx` | Retry with backoff; alert after max retries |
| `HTTP 4xx` (not 429) | Raise immediately — not retryable |

---

## Data Flow

```
Airflow task: extract_data
    │
    ▼
FakeStoreExtractor.extract(execution_date)
    │
    ├── loop: fetch_page(page=1, 2, 3 …)
    │       │
    │       ├── @retry_with_backoff wraps HTTP GET
    │       ├── validate_response() — check fields exist
    │       └── append rows to accumulator list
    │
    ├── enrich rows: add source, extraction_timestamp
    │
    └── save_raw(rows, staging/{date}/raw_products.json)
            │  (write to .tmp → os.rename → atomic)
            ▼
        staging/2025-06-15/raw_products.json   ← consumed by transformer
```

---

## Dependencies

### External
| Library | Used for |
|---|---|
| `requests` | HTTP client for API calls |
| `beautifulsoup4` | HTML parsing (TikiScraper only) |
| `loguru` | Structured logging |

### Internal
| Module | Used for |
|---|---|
| `etl.config` | `get_config()` — source URL, timeouts, retries, staging dir |
| `utils.retry` | `@retry_with_backoff` decorator |

---

## Notes / Constraints

- **Atomic writes:** Raw files are written to `{path}.tmp` first, then renamed. This prevents the transformer from reading a partially written file if the process is killed mid-write.
- **Staging path convention:** Always `staging/{execution_date}/raw_{source}.json`. The date comes from Airflow's `ds` variable (ISO format: `YYYY-MM-DD`).
- **`extraction_timestamp` vs `execution_date`:** `extraction_timestamp` is the exact UTC moment the row was fetched. `execution_date` is the Airflow logical date used for partitioning — these may differ during backfills.
- **FakeStore pagination:** The API returns an empty list `[]` when `skip` exceeds the total product count. This is the loop termination condition.
- **TikiScraper** is rate-limited by design. Do not remove the `time.sleep(rate_limit_delay)` — removing it risks IP bans.
- When `validate_response()` returns `False` for a page, that page is skipped and logged as a warning — it does **not** abort the full extraction.