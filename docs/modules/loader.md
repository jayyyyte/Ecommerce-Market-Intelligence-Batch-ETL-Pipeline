# Module: `etl/loader.py`

## Overview

`etl/loader.py` is the Sprint 2 Phase 4 loading layer. It persists clean product rows into PostgreSQL with idempotent UPSERT logic, writes rejected rows into `rejected_records`, logs run lifecycle state in `pipeline_runs`, and handles raw-file archiving.

The module is intentionally independent of Airflow operators. Sprint 5 can call it from DAG tasks without changing the data contract.

---

## Responsibilities

### Handles
- Inserting `RUNNING` rows into `pipeline_runs`
- UPSERTing clean rows into `products_market`
- Bulk inserting rejected rows into `rejected_records`
- Updating `pipeline_runs` with final counts and status
- Rolling back product and rejected-row writes together on failure
- Returning inserted and updated counts separately
- Archiving raw staging files into `archive/YYYY/MM/DD/`
- Deleting archived files older than `cfg.archive_retention_days`

### Does not handle
- Extraction
- Transformation or schema validation
- Table creation or migrations
- Slack or email notifications
- Airflow DAG orchestration

---

## Public API

### `DataLoader(engine, cfg=None)`

Creates a loader bound to a SQLAlchemy engine and optional `PipelineConfig`.

### `log_run_start(run_id, dag_id, execution_date) -> None`

Inserts a `pipeline_runs` row with:
- `status='RUNNING'`
- `started_at=NOW()`
- the provided logical execution date

### `load_products(df, run_id, connection=None) -> tuple[int, int]`

UPSERTs `df` into `products_market` on:
- `(product_id, source, extraction_date)`

On conflict, only these columns are updated:
- `price`
- `rating`
- `review_count`
- `stock_status`
- `pipeline_run_id`
- `updated_at`

These columns are preserved from the first insert:
- `name`
- `category`
- `source_url`

Returns:
- `(rows_inserted, rows_updated)`

Insert vs update counts are derived from PostgreSQL `xmax`.

### `load_rejected(df, run_id, connection=None) -> int`

Bulk inserts rows into `rejected_records`. The `run_id` argument overwrites any stale run id already present in the DataFrame so rejected rows always point at the current pipeline run.

Returns:
- number of inserted rejected rows

### `log_run_end(run_id, status, counts, error_msg=None) -> None`

Updates the matching `pipeline_runs` row with:
- final `status`
- `rows_extracted`
- `rows_transformed`
- `rows_rejected`
- `rows_loaded`
- `data_quality_score`
- `finished_at`
- optional `error_message`

Expected statuses:
- `SUCCESS`
- `PARTIAL`
- `FAILED`

### `run_load(clean_df, rejected_df, *, run_id, dag_id, execution_date, rows_extracted, rows_transformed, quality_score) -> dict`

High-level wrapper for the Phase 4 load flow:

1. `log_run_start(...)`
2. open one transaction
3. `load_products(...)`
4. `load_rejected(...)`
5. commit or rollback
6. `log_run_end(...)`

Final status rules:
- `SUCCESS` if load succeeds and `rows_rejected == 0`
- `PARTIAL` if load succeeds and `rows_rejected > 0`
- `FAILED` if any database write fails

Returns a summary dict with:
- `status`
- `rows_extracted`
- `rows_transformed`
- `rows_rejected`
- `rows_loaded`
- `rows_inserted`
- `rows_updated`
- `data_quality_score`
- `error_message`

### `archive_staging(staging_path, execution_date) -> Path`

Copies a raw staging file into:
- `archive/YYYY/MM/DD/<original_filename>`

Behavior:
- computes MD5 on source and archive copy
- deletes the source only if checksums match
- raises on missing source or checksum mismatch

### `cleanup_archive(now=None) -> int`

Deletes archived files older than `cfg.archive_retention_days`.

Returns:
- number of deleted files

---

## Transaction Model

`run_load(...)` is the transaction boundary for product and rejected-row writes.

- `load_products(...)` and `load_rejected(...)` both accept an optional open SQLAlchemy connection.
- When `run_load(...)` passes the same connection to both methods, they share a single transaction.
- If the rejected-row insert fails after product UPSERT begins, the whole transaction is rolled back.
- `pipeline_runs` status is updated outside that transaction so failures are still recorded.

---

## Archive Path Rules

The archive root is derived from `cfg.staging_dir`.

Examples:
- `staging/` -> sibling `archive/`
- `/opt/airflow/staging` -> `/opt/airflow/archive`

This keeps local runs and Docker runs aligned without adding another config variable.

---

## Testing

Repo coverage for this module is split into two layers:

- `tests/test_loader.py`
  Covers loader behavior, archive handling, and rollback semantics.
- `tests/test_integration.py`
  Covers transform-to-load flows against PostgreSQL.

Database-backed tests require:
- `TEST_DATABASE_URL`

If that variable is not set, DB-backed tests skip instead of writing into a developer's default database.

---

## Notes

- The loader assumes `sql/create_tables.sql` has already been executed.
- `rows_loaded` means rows written during the run: `rows_inserted + rows_updated`.
- `data_quality_score` is passed through from `DataTransformer`; the loader does not recompute it.
