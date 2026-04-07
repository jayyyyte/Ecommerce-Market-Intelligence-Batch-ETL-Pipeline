# Module: `etl/transformer.py`

## Overview

Transforms raw extracted data into a clean, validated, analysis-ready Pandas DataFrame. Applies a sequential cleaning pipeline (null removal → type normalization → deduplication → schema validation), tracks every rejected row with a reason code, and emits a data quality score for the run.

---

## Responsibilities

### ✅ Handles
- Loading raw JSON from the staging file into a DataFrame
- Dropping rows with `NULL` / empty `product_id` or `price`
- Normalizing `price` to `float` with 2 decimal places; rejecting `price <= 0`
- Normalizing `category` to lowercase slug format (`"Men's Clothing"` → `"mens_clothing"`)
- Clipping `rating` to `[0.0, 5.0]`; logging rows that were clipped
- Filling missing `review_count` with `0`, missing `stock_status` with `"unknown"`
- Running schema validation (Pandera) after all cleaning steps
- Tracking every dropped row in a `rejected_df` with a `rejection_reason` code
- Adding `pipeline_run_id` UUID to every clean row for traceability
- Calculating and returning a `data_quality_score` (%)

### ❌ Does NOT handle
- Fetching raw data from the source (→ `etl/extractor.py`)
- Writing the rejected records to the database (→ `etl/loader.py`)
- Sending quality alerts (→ `etl/notifier.py`)
- Business-level transformations (aggregations, joins) — those belong in SQL

---

## Structure

```
etl/
└── transformer.py
    ├── class DataTransformer          # Main transformation orchestrator
    │   ├── transform(df, run_id)      # Public entrypoint; returns (clean_df, rejected_df, score)
    │   ├── drop_invalid_rows(df)      # Step 1 — null/empty critical fields
    │   ├── normalize_price(df)        # Step 2 — cast, round, reject negatives
    │   ├── normalize_category(df)     # Step 3 — slug format
    │   ├── validate_rating(df)        # Step 4 — clip to [0, 5]
    │   ├── fill_defaults(df)          # Step 5 — fill optional nulls
    │   ├── add_run_metadata(df)       # Step 6 — append pipeline_run_id column
    │   └── _reject(rows, reason)      # Internal — accumulates rejected_df
    └── _build_schema()                # Returns Pandera DataFrameSchema

config/
└── schema_spec.yaml                   # Declares expected columns and types
```

---

## Usage

Called from the `transform_data` Airflow task. The task reads the staging file path from XCom (set by `extract_data`) and passes the loaded DataFrame to the transformer.

```python
import pandas as pd
from etl.transformer import DataTransformer
from etl.config import get_config

def transform_data(**context):
    cfg = get_config()
    ti  = context["ti"]

    staging_path = ti.xcom_pull(task_ids="extract_data", key="staging_path")
    run_id       = ti.xcom_pull(task_ids="extract_data", key="run_id")

    raw_df = pd.read_json(staging_path)

    transformer = DataTransformer(cfg)
    clean_df, rejected_df, quality_score = transformer.transform(raw_df, run_id)

    # Pass downstream
    ti.xcom_push(key="rows_transformed", value=len(clean_df))
    ti.xcom_push(key="rows_rejected",    value=len(rejected_df))
    ti.xcom_push(key="quality_score",    value=quality_score)
    ti.xcom_push(key="rejected_df_json", value=rejected_df.to_json())

    return clean_df.to_json()
```

---

## API

### `class DataTransformer`

```python
transformer = DataTransformer(cfg: PipelineConfig)
clean_df, rejected_df, score = transformer.transform(df, run_id)
```

#### `transform(df, run_id) → tuple[DataFrame, DataFrame, float]`

Orchestrates the full cleaning pipeline. Always returns all three values even if `clean_df` is empty.

| Return value | Type | Description |
|---|---|---|
| `clean_df` | `pd.DataFrame` | Rows that passed all checks; ready for DB load |
| `rejected_df` | `pd.DataFrame` | Rows dropped; columns: `run_id`, `raw_data` (JSON), `rejection_reason`, `rejected_at` |
| `quality_score` | `float` | `len(clean_df) / len(input_df) * 100`; `0.0` if input was empty |

---

#### Cleaning Steps (executed in order)

| Step | Method | Action | Rejection Reason Code |
|---|---|---|---|
| 1 | `drop_invalid_rows` | Drop rows where `product_id` or `price` is NULL/empty | `null_product_id`, `null_price` |
| 2 | `normalize_price` | Cast to `float`, round 2dp, drop `price <= 0` | `invalid_price` |
| 3 | `normalize_category` | Lowercase, replace spaces/special chars with `_` | — (no rejection, just transform) |
| 4 | `validate_rating` | Clip to `[0.0, 5.0]`; log clipped rows as warnings | — (clip, not reject) |
| 5 | `fill_defaults` | `review_count=0`, `stock_status="unknown"` for NULLs | — (fill, not reject) |
| 6 | `add_run_metadata` | Add `pipeline_run_id` column from `run_id` arg | — |
| 7 | Pandera validation | Schema check on final DataFrame | `schema_violation` |

---

#### Rejection Reason Codes

| Code | Meaning |
|---|---|
| `null_product_id` | `product_id` is `None`, `NaN`, or empty string |
| `null_price` | `price` is `None` or `NaN` |
| `invalid_price` | `price` is not castable to float, or `price <= 0` |
| `schema_violation` | Row fails Pandera column type or constraint check |

---

## Data Flow

```
staging/2025-06-15/raw_products.json
    │
    ▼
pd.read_json()  →  raw_df (N rows)
    │
    ├── Step 1: drop_invalid_rows     →  rejected_df += dropped rows (reason: null_*)
    ├── Step 2: normalize_price       →  rejected_df += dropped rows (reason: invalid_price)
    ├── Step 3: normalize_category    →  in-place transform, no rejection
    ├── Step 4: validate_rating       →  clip values, warn in logs
    ├── Step 5: fill_defaults         →  in-place fill, no rejection
    ├── Step 6: add_run_metadata      →  adds pipeline_run_id column
    └── Step 7: pandera schema check  →  rejected_df += failed rows (reason: schema_violation)
    │
    ▼
clean_df (M rows, M ≤ N)    →  consumed by loader
rejected_df (N-M rows)      →  consumed by loader (bulk insert to rejected_records)
quality_score = M/N * 100   →  logged to pipeline_runs
```

---

## Dependencies

### External
| Library | Used for |
|---|---|
| `pandas` | Core DataFrame operations |
| `pandera` | Schema validation with typed column checks |
| `loguru` | Structured logging with log levels |
| `PyYAML` | Loading `config/schema_spec.yaml` |

### Internal
| Module | Used for |
|---|---|
| `etl.config` | `get_config()` — `min_quality_score` threshold |

---

## Notes / Constraints

- **Steps are ordered intentionally** — `normalize_price` runs before `validate_rating` because an unparseable price must be rejected before any numeric comparison.
- **`rejected_df` accumulates across all steps** — rows rejected in Step 1 are never passed to Step 2. Each rejected row carries the reason from the step that first dropped it.
- **Pandera schema is re-validated after all steps**, not before — we validate the *output*, not the raw input, because cleaning may produce violations the raw data didn't have.
- **`data_quality_score` denominator** is the **input row count** (before any rejection), not the count after Step 1. This gives a true picture of source data quality.
- **Category normalization is lossy** — `"Men's Clothing"` and `"Men s Clothing"` both become `"men_s_clothing"` vs `"mens_clothing"`. Review the regex in `normalize_category()` if source categories change.
- If `clean_df` is empty after all steps, the transformer still returns normally — it does **not** raise. The loader will insert 0 rows, and `pipeline_runs` will record `rows_loaded=0`. It is the notifier's job to alert on quality scores below `min_quality_score`.