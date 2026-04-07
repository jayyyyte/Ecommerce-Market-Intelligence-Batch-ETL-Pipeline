# Module: `etl/loader.py`

## Overview

Loads the cleaned DataFrame into PostgreSQL using UPSERT semantics, guaranteeing idempotency across DAG re-runs. Manages all database writes within a single transaction — including the main product load, rejected records, and pipeline run status — rolling back everything atomically on any failure.

---

## Responsibilities

### ✅ Handles
- Connecting to PostgreSQL via SQLAlchemy using `cfg.db_url`
- UPSERTing clean rows into `products_market` using `ON CONFLICT DO UPDATE`
- Bulk-inserting rejected rows into `rejected_records`
- Inserting a new `pipeline_runs` row when a run starts (`log_run_start`)
- Updating the `pipeline_runs` row when a run ends (`log_run_end`) — always, even on failure
- Wrapping all writes in a single transaction with full rollback on error
- Returning insert/update counts separately for accurate pipeline metrics
- Archiving the raw staging file to `archive/{YYYY}/{MM}/{DD}/` after a successful load

### ❌ Does NOT handle
- Data cleaning or validation (→ `etl/transformer.py`)
- Extracting data from the source (→ `etl/extractor.py`)
- Sending Slack or email notifications (→ `etl/notifier.py`)
- Schema migrations or DDL changes (→ `sql/create_tables.sql`)
- Query-time reads or analytics queries

---

## Structure

```
etl/
└── loader.py
    ├── class DataLoader              # Main loader class
    │   ├── load_products(df, run_id) # UPSERT clean rows → products_market
    │   ├── load_rejected(df, run_id) # Bulk insert → rejected_records
    │   ├── log_run_start(...)        # INSERT row into pipeline_runs
    │   ├── log_run_end(...)          # UPDATE pipeline_runs row with final status
    │   └── archive_staging(path)     # Move raw file to archive directory
    └── _build_upsert_stmt()          # Internal — constructs SQLAlchemy INSERT ... ON CONFLICT
```

---

## Usage

Called from two Airflow tasks: `load_to_db` (main load) and implicitly from `extract_data` and `notify_status` (run logging).

```python
import pandas as pd
import sqlalchemy as sa
from etl.loader import DataLoader
from etl.config import get_config

def load_to_db(**context):
    cfg    = get_config()
    ti     = context["ti"]
    run_id = ti.xcom_pull(task_ids="extract_data", key="run_id")

    clean_df     = pd.read_json(ti.xcom_pull(task_ids="transform_data"))
    rejected_df  = pd.read_json(ti.xcom_pull(task_ids="transform_data", key="rejected_df_json"))
    quality_score = ti.xcom_pull(task_ids="transform_data", key="quality_score")

    engine = sa.create_engine(cfg.db_url)
    loader = DataLoader(engine, cfg)

    # Single transaction: products + rejected records
    inserted, updated = loader.load_products(clean_df, run_id)
    loader.load_rejected(rejected_df, run_id)

    loader.log_run_end(
        run_id=run_id,
        status="SUCCESS",
        counts={
            "rows_loaded":   inserted + updated,
            "rows_rejected": len(rejected_df),
            "data_quality_score": quality_score,
        },
    )

    ti.xcom_push(key="rows_inserted", value=inserted)
    ti.xcom_push(key="rows_updated",  value=updated)
```

---

## API

### `class DataLoader`

```python
loader = DataLoader(engine: sa.Engine, cfg: PipelineConfig)
```

---

#### `load_products(df, run_id) → tuple[int, int]`

UPSERTs all rows in `df` into `products_market`.

| Argument | Type | Description |
|---|---|---|
| `df` | `pd.DataFrame` | Clean DataFrame from `DataTransformer.transform()` |
| `run_id` | `str` (UUID) | Pipeline run identifier for `pipeline_run_id` column |

**Returns:** `(inserted_count, updated_count)`

**Conflict resolution** — `ON CONFLICT (product_id, source, extraction_date) DO UPDATE SET`:

| Column | Action |
|---|---|
| `price` | Updated to new value |
| `rating` | Updated to new value |
| `review_count` | Updated to new value |
| `stock_status` | Updated to new value |
| `updated_at` | Set to `NOW()` |
| `pipeline_run_id` | Updated to current `run_id` |
| `name`, `category`, `source_url` | **Not updated** — first-write wins |

---

#### `load_rejected(df, run_id) → None`

Bulk-inserts all rows from `rejected_df` into `rejected_records`.

| Argument | Type | Description |
|---|---|---|
| `df` | `pd.DataFrame` | Rejected rows from `DataTransformer.transform()` |
| `run_id` | `str` (UUID) | Foreign key to `pipeline_runs.run_id` |

- Uses `pd.DataFrame.to_sql(..., if_exists="append", method="multi")` for batch efficiency.
- Called in the same transaction as `load_products`. If either fails, both roll back.
- No-op if `df` is empty — does not raise.

---

#### `log_run_start(run_id, dag_id, execution_date) → None`

Inserts a new row into `pipeline_runs` with `status='RUNNING'` at the beginning of the pipeline.

```python
loader.log_run_start(
    run_id="550e8400-e29b-41d4-a716-446655440000",
    dag_id="ecommerce_market_etl",
    execution_date="2025-06-15",
)
```

Called from the `extract_data` task, before any external HTTP calls are made.

---

#### `log_run_end(run_id, status, counts, error_msg=None) → None`

Updates the `pipeline_runs` row to its final state. **Must be called in a `finally` block** to guarantee it always runs.

| Parameter | Type | Description |
|---|---|---|
| `run_id` | `str` | UUID of the run to update |
| `status` | `str` | `"SUCCESS"` / `"FAILED"` / `"PARTIAL"` |
| `counts` | `dict` | Keys: `rows_extracted`, `rows_transformed`, `rows_rejected`, `rows_loaded`, `data_quality_score` |
| `error_msg` | `str \| None` | Exception message if status is `FAILED`; `None` on success |

---

#### `archive_staging(staging_path) → None`

Moves the raw JSON file from `staging/` to `archive/{YYYY}/{MM}/{DD}/` and verifies MD5 integrity before deleting the original.

---

## Data Flow

```
transform_data task (XCom)
    │
    ├── clean_df      ──►  load_products()
    │                          │
    │                          ├── BEGIN TRANSACTION
    │                          ├── INSERT INTO products_market ... ON CONFLICT DO UPDATE
    │                          ├── (count inserted vs updated via pg row status)
    │                          └── COMMIT  ← or ROLLBACK on any exception
    │
    ├── rejected_df   ──►  load_rejected()
    │                          │
    │                          └── INSERT INTO rejected_records (bulk)
    │
    └── run_id        ──►  log_run_end()
                               │
                               └── UPDATE pipeline_runs SET status, counts, finished_at
```

---

## Dependencies

### External
| Library | Used for |
|---|---|
| `sqlalchemy` | Engine, Core INSERT with ON CONFLICT, transaction context |
| `psycopg2-binary` | PostgreSQL adapter (SQLAlchemy backend) |
| `pandas` | DataFrame → SQL via `to_sql()` for bulk rejected inserts |
| `loguru` | Structured logging |

### Internal
| Module | Used for |
|---|---|
| `etl.config` | `cfg.db_url`, `cfg.staging_dir`, `cfg.archive_retention_days` |

---

## Notes / Constraints

- **Idempotency guarantee:** Running `load_products()` twice with identical data produces exactly the same result as running it once. The UNIQUE constraint on `(product_id, source, extraction_date)` is what makes this possible — without it, re-runs create duplicates.
- **Partial load protection:** `load_products()` and `load_rejected()` share a connection and transaction context. If `load_rejected()` fails after `load_products()` succeeds, the entire transaction rolls back.
- **`log_run_end` MUST use `try/finally`** in the calling task — even if the load raises, the pipeline_runs row must be updated to `FAILED` so the Airflow UI and dashboard show correct status.
- **Insert vs update count** is derived from PostgreSQL's `xmax` system column: `xmax = 0` means inserted, `xmax != 0` means updated. This detail matters for accurate pipeline metrics.
- The loader does **not** create tables — `sql/create_tables.sql` must have been executed before the first run.
- For very large DataFrames (>100K rows), consider chunking the UPSERT into batches of 5,000 rows to avoid statement size limits and memory pressure.