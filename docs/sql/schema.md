# SQL: Database Schema

**Files:** `sql/00_init_databases.sql`, `sql/create_tables.sql`

## Overview

Two SQL files define the complete PostgreSQL schema for the pipeline. `00_init_databases.sql` creates the three required databases on first container start. `create_tables.sql` defines the three application tables with all constraints, indexes, and comments.

---

## Responsibilities

### ✅ Handles
- Creating `airflow_db` and `metabaseappdb` alongside the default `ecommerce_db`
- Defining all columns, types, defaults, and constraints for 3 tables
- Creating UNIQUE indexes that enable idempotent UPSERT logic
- Creating performance indexes on commonly filtered columns

### ❌ Does NOT handle
- Data migrations (adding/renaming columns after initial creation)
- Seed data or test fixtures
- Views, stored procedures, or triggers
- User/role creation or permission grants

---

## File Roles

### `sql/00_init_databases.sql`

Runs **automatically** when the `postgres` Docker container starts for the first time (mounted into `/docker-entrypoint-initdb.d/`). Uses conditional `SELECT ... WHERE NOT EXISTS \gexec` pattern — safe on container restarts.

```sql
-- Creates these databases if they don't exist:
airflow_db       -- Airflow metadata (scheduler state, task history)
metabaseappdb    -- Metabase application data (dashboards, questions)
-- (ecommerce_db is created by POSTGRES_DB env var — no SQL needed)
```

> ⚠️ This file only runs once per Docker volume lifetime. To re-run it, you must destroy the volume: `docker compose down -v`.

---

### `sql/create_tables.sql`

Executed by `scripts/init_db.py`. All statements use `IF NOT EXISTS` — safe to re-run.

---

## Tables

### `pipeline_runs`

Audit log for every DAG execution. Created before `products_market` because it is a FK parent.

| Column | Type | Key | Description |
|---|---|---|---|
| `run_id` | `UUID` | PK | Unique per pipeline execution |
| `dag_id` | `VARCHAR(200)` | | Airflow DAG identifier |
| `execution_date` | `DATE` | UNIQUE with dag_id | Logical execution date from Airflow `ds` |
| `status` | `VARCHAR(20)` | CHECK | `RUNNING` / `SUCCESS` / `FAILED` / `PARTIAL` |
| `rows_extracted` | `INTEGER` | | Raw rows from source |
| `rows_transformed` | `INTEGER` | | After cleaning |
| `rows_rejected` | `INTEGER` | | Dropped during validation |
| `rows_loaded` | `INTEGER` | | Successfully upserted into DB |
| `data_quality_score` | `NUMERIC(5,2)` | | `rows_loaded / rows_extracted * 100` |
| `started_at` | `TIMESTAMPTZ` | | Run start time |
| `finished_at` | `TIMESTAMPTZ` | | Run end time (NULL while running) |
| `error_message` | `TEXT` | | Exception detail on failure |

---

### `products_market`

Main fact table. One row per unique `(product_id, source, extraction_date)` combination.

| Column | Type | Key | Description |
|---|---|---|---|
| `id` | `SERIAL` | PK | Internal auto-increment |
| `product_id` | `VARCHAR(100)` | UNIQUE composite | External ID from source |
| `name` | `VARCHAR(500)` | NOT NULL | Product display name |
| `category` | `VARCHAR(100)` | | Normalised slug (`mens_clothing`) |
| `price` | `NUMERIC(12,2)` | NOT NULL, >0 | Price in source currency |
| `rating` | `NUMERIC(3,2)` | 0–5 | Average customer rating |
| `review_count` | `INTEGER` | DEFAULT 0 | Total reviews |
| `stock_status` | `VARCHAR(50)` | DEFAULT 'unknown' | `in_stock` / `out_of_stock` / `unknown` |
| `source` | `VARCHAR(100)` | UNIQUE composite | Source identifier (`fakestore_api`) |
| `source_url` | `TEXT` | | Direct product URL |
| `extraction_date` | `DATE` | UNIQUE composite | Airflow `ds` — logical partition key |
| `pipeline_run_id` | `UUID` | FK → pipeline_runs | Trace row back to its run |
| `created_at` | `TIMESTAMPTZ` | | First insert time |
| `updated_at` | `TIMESTAMPTZ` | | Last UPSERT update time |

---

### `rejected_records`

Audit trail for every row dropped during transformation.

| Column | Type | Description |
|---|---|---|
| `id` | `SERIAL PK` | Auto ID |
| `run_id` | `UUID FK` | Links to `pipeline_runs.run_id` (CASCADE delete) |
| `raw_data` | `JSONB` | Original extracted row verbatim |
| `rejection_reason` | `VARCHAR(200)` | Reason code: `null_price`, `invalid_price`, etc. |
| `rejected_at` | `TIMESTAMPTZ` | Timestamp of rejection |

---

## Indexes

| Index Name | Table | Columns | Purpose |
|---|---|---|---|
| `uq_products_market_key` | `products_market` | `(product_id, source, extraction_date)` | **UPSERT dedup** — conflict target |
| `idx_products_category` | `products_market` | `category` | Filter by category in queries |
| `idx_products_extraction_date` | `products_market` | `extraction_date DESC` | Date range queries and dashboard filters |
| `idx_products_source` | `products_market` | `source` | Filter by data source |
| `uq_pipeline_runs_dag_date` | `pipeline_runs` | `(dag_id, execution_date)` | Prevent duplicate run logs per date |
| `idx_pipeline_runs_date` | `pipeline_runs` | `execution_date DESC` | Dashboard pipeline health queries |
| `idx_pipeline_runs_status` | `pipeline_runs` | `status` | Filter failed runs |
| `idx_rejected_run_id` | `rejected_records` | `run_id` | Join rejected records to a specific run |
| `idx_rejected_reason` | `rejected_records` | `rejection_reason` | Aggregate by rejection type |

---

## Notes / Constraints

- **UPSERT conflict target** is `(product_id, source, extraction_date)` — this is the business key that defines uniqueness per day per source. The `ON CONFLICT` clause in `loader.py` must use exactly these three columns.
- **`pipeline_run_id FK`** in `products_market` is nullable and `ON DELETE SET NULL` — if a `pipeline_runs` row is deleted (e.g. data cleanup), the product rows are retained with a null run reference.
- **`rejected_records.run_id FK`** is `ON DELETE CASCADE` — deleting a pipeline run cleans up its rejected records automatically.
- **`data_quality_score`** column is added to `pipeline_runs` as `NUMERIC(5,2)` — max value `999.99`. In practice it will always be `0.00–100.00`.
- The `CHECK (status IN (...))` constraint on `pipeline_runs` enforces the status enum at the DB level — any typo in Python code will cause an immediate `IntegrityError`.