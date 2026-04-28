# Sprint 2 Review - Transformation and Loading

**Sprint focus:** Phase 3 - Data Transformation + Phase 4 - Data Loading  
**Main deliverable:** Raw product data can be cleaned, validated, loaded into PostgreSQL, audited, and tested for idempotency.

---

## Sprint Goal

By the end of Sprint 2, the project can:

- Take raw extracted product rows and produce a clean, schema-valid Pandas DataFrame.
- Reject invalid rows with a clear `rejection_reason`.
- Calculate a per-run `data_quality_score`.
- Load clean rows into PostgreSQL with idempotent UPSERT behavior.
- Record rejected rows in `rejected_records`.
- Record each pipeline execution in `pipeline_runs`.
- Roll back database writes if a load step fails.
- Archive raw staging files with checksum verification.

**Result:** The Sprint 2 core goal is implemented in the ETL modules and covered by unit/integration tests. DB-backed tests require a configured PostgreSQL test database via `TEST_DATABASE_URL`.

---

## Acceptance Criteria

| # | Criterion | Status | Evidence / Notes |
|---|-----------|--------|------------------|
| 1 | Raw data can be transformed into a clean DataFrame | Done | `DataTransformer.transform()` returns `clean_df`, `rejected_df`, and `quality_score`. |
| 2 | Invalid critical fields are rejected | Done | Null/empty `product_id`, null/empty/invalid `price`, and schema violations are captured with reason codes. |
| 3 | Output schema is validated | Done | Pandera schema is built from `config/schema_spec.yaml`. |
| 4 | Data quality score is calculated | Done | Score is `len(clean_df) / len(input_df) * 100`, with `0.0` for empty input. |
| 5 | Clean rows load into PostgreSQL | Done | `DataLoader.load_products()` inserts into `products_market`. |
| 6 | Re-runs do not create duplicate product rows | Done | UPSERT conflict key is `(product_id, source, extraction_date)`. |
| 7 | Rejected rows are persisted | Done | `DataLoader.load_rejected()` writes to `rejected_records`. |
| 8 | Pipeline runs are audited | Done | `log_run_start()` and `log_run_end()` write lifecycle status and row counts to `pipeline_runs`. |
| 9 | Load is transactional | Done | `run_load()` rolls back product/rejected writes if the DB load fails. |
| 10 | Raw staging files can be archived | Done | `archive_staging()` copies to `archive/YYYY/MM/DD/`, verifies MD5, then removes the source. |
| 11 | Archive retention cleanup exists | Done | `cleanup_archive()` deletes files older than `archive_retention_days`. |
| 12 | Tests cover transform, load, and transform-to-load flow | Done | `python -m pytest tests` -> `115 passed, 13 skipped`. |

---

## What Was Built

### 1. Data Transformation Layer

Implemented in `etl/transformer.py`.

The transformer is responsible for turning raw extracted rows into a stable, database-ready format. It does not write to PostgreSQL. Instead, it returns three outputs:

```python
clean_df, rejected_df, quality_score = transformer.transform(raw_df, run_id)
```

The cleaning pipeline runs in this order:

| Step | Behavior | Output |
|------|----------|--------|
| 1 | Reject missing or empty `product_id` | `rejection_reason = null_product_id` |
| 2 | Reject missing or empty `price` | `rejection_reason = null_price` |
| 3 | Convert price to numeric, round to 2 decimals, reject `price <= 0` | `rejection_reason = invalid_price` |
| 4 | Normalize category into lowercase slug format | Clean category string |
| 5 | Clip rating into `[0.0, 5.0]` | Row stays valid, warning is logged |
| 6 | Fill defaults for optional fields | `review_count = 0`, `stock_status = unknown` |
| 7 | Overwrite `pipeline_run_id` with current run id | Traceability |
| 8 | Validate final rows with Pandera | Invalid rows become `schema_violation` |

The important design choice is that validation happens after cleaning. This lets the transformer fix recoverable dirty values first, then reject only rows that still violate the expected schema.

### 2. Rejected Row Tracking

Rejected rows are not silently dropped. Each rejected row is stored in `rejected_df` with:

| Column | Meaning |
|--------|---------|
| `run_id` | Current pipeline run id |
| `raw_data` | Original row data before cleaning |
| `rejection_reason` | Reason code such as `null_product_id`, `invalid_price`, or `schema_violation` |
| `rejected_at` | UTC timestamp when the row was rejected |

This makes data quality issues auditable. A later dashboard or monitoring task can group by `rejection_reason` to see why rows are failing.

### 3. Data Quality Score

Sprint 2 adds a simple quality metric:

```text
data_quality_score = clean row count / original row count * 100
```

Examples:

| Input rows | Clean rows | Rejected rows | Score |
|------------|------------|---------------|-------|
| 10 | 10 | 0 | 100.00 |
| 10 | 8 | 2 | 80.00 |
| 0 | 0 | 0 | 0.00 |

The score is returned by the transformer and stored by the loader in `pipeline_runs.data_quality_score`.

### 4. PostgreSQL Loading Layer

Implemented in `etl/loader.py`.

`DataLoader` handles the database side of Sprint 2:

```python
loader = DataLoader(engine, cfg)
result = loader.run_load(
    clean_df,
    rejected_df,
    run_id=run_id,
    dag_id="ecommerce_market_etl",
    execution_date="2026-04-21",
    rows_extracted=len(raw_df),
    rows_transformed=len(clean_df),
    quality_score=quality_score,
)
```

The high-level load flow is:

1. Create or confirm a `pipeline_runs` row with `RUNNING` status.
2. UPSERT clean rows into `products_market`.
3. Insert rejected rows into `rejected_records`.
4. Update `pipeline_runs` with final row counts and status.
5. Roll back product/rejected writes if any DB step fails.

Final status rules:

| Condition | Final status |
|-----------|--------------|
| Load succeeds and no rows were rejected | `SUCCESS` |
| Load succeeds but some rows were rejected | `PARTIAL` |
| Product load, rejected insert, or final status update fails | `FAILED` |

### 5. Idempotent UPSERT

The loader uses PostgreSQL `INSERT ... ON CONFLICT DO UPDATE`.

The conflict key is:

```text
(product_id, source, extraction_date)
```

This means:

- Same product + same source + same extraction date = update existing row.
- Same product + same source + new extraction date = create a new historical snapshot.

This is important for daily trend dashboards. Re-running the same day should not create duplicates, but a new day should preserve a new snapshot.

On conflict, the loader updates mutable market fields:

- `price`
- `rating`
- `review_count`
- `stock_status`
- `pipeline_run_id`
- `updated_at`

It intentionally preserves original descriptive fields such as `name`, `category`, and `source_url` after first insert.

### 6. Pipeline Run Logging

The `pipeline_runs` table is the audit trail for each pipeline execution.

It records:

- `run_id`
- `dag_id`
- `execution_date`
- `status`
- `rows_extracted`
- `rows_transformed`
- `rows_rejected`
- `rows_loaded`
- `data_quality_score`
- `started_at`
- `finished_at`
- `error_message`

This gives enough information to answer operational questions:
- Did the run succeed?
- How many rows were extracted?
- How many rows were rejected?
- Did the quality score fall below the expected threshold?
- What error happened if the run failed?

### 7. Archive and Retention

The loader also handles raw staging file preservation:

```python
archived_path = loader.archive_staging("staging/2026-04-21/raw_products.json", "2026-04-21")
```

Archive behavior:

1. Copy staging file to `archive/YYYY/MM/DD/<filename>`.
2. Compute MD5 checksum for source and destination.
3. Delete the source only after checksum matches.
4. Raise an error if the source is missing or checksum validation fails.

Retention cleanup:

```python
deleted_count = loader.cleanup_archive()
```

Files older than `cfg.archive_retention_days` are deleted.

---

## Database Elements

Sprint 2 depends on three PostgreSQL tables.

### `products_market`

Main fact table for clean product snapshots.

Important fields:

- `product_id`
- `name`
- `category`
- `price`
- `rating`
- `review_count`
- `stock_status`
- `source`
- `source_url`
- `extraction_date`
- `pipeline_run_id`

Important constraint:

```sql
UNIQUE (product_id, source, extraction_date)
```

This is what makes UPSERT idempotent.

### `pipeline_runs`

Audit table for pipeline executions.

Sprint 2 added/uses:

```sql
data_quality_score NUMERIC(5,2)
```

The DDL also includes an additive migration guard:

```sql
ALTER TABLE pipeline_runs
    ADD COLUMN IF NOT EXISTS data_quality_score NUMERIC(5,2) DEFAULT NULL;
```

This protects existing local databases that were created before the quality score column was added.

### `rejected_records`

Stores rows that failed validation.

Important fields:

- `run_id`
- `raw_data`
- `rejection_reason`
- `rejected_at`

`raw_data` is JSONB, so the loader normalizes nested Pandas timestamps, missing values, and numpy scalar values before inserting.

---

## Building and Implementation Process

Sprint 2 was implemented in two major phases.

### Phase 3 - Transformation

The first step was to define a clean contract between extraction and loading:

```text
raw_df -> DataTransformer -> clean_df + rejected_df + quality_score
```

The transformer was built around small steps so each rule can be tested independently:

- critical null checks first,
- price normalization before schema validation,
- category normalization as a non-rejecting cleanup,
- rating clipping instead of rejection,
- default filling for optional fields,
- final schema validation through Pandera.

The rejection system was designed to keep the original row available for debugging. This is why rejected records store `raw_data`, not only the cleaned row.

### Phase 4 - Loading

The loader was then built around the database contract:

```text
clean_df -> products_market
rejected_df -> rejected_records
run metadata -> pipeline_runs
raw staging file -> archive/
```

The main engineering focus was correctness:

- UPSERT must prevent duplicate daily product rows.
- Foreign keys must point to valid `pipeline_runs` rows.
- Product and rejected writes must commit or roll back together.
- Failure must still be visible in `pipeline_runs`.
- Integration tests must distinguish same-day reruns from new-day snapshots.

The final loader design keeps database operations explicit and testable. `load_products()` and `load_rejected()` can be tested directly, while `run_load()` tests the full transactional behavior.

---

## Current Sprint 2 flow:

```text
Raw extracted rows
        |
        v
DataTransformer.transform(raw_df, run_id)
        |
        +--> clean_df
        +--> rejected_df
        +--> quality_score
        |
        v
DataLoader.run_load(...)
        |
        +--> pipeline_runs: RUNNING
        +--> products_market: UPSERT clean rows
        +--> rejected_records: insert rejected rows
        +--> pipeline_runs: SUCCESS / PARTIAL / FAILED
```

Example outcomes:

| Scenario | Result |
|----------|--------|
| All rows valid | Clean rows load, `pipeline_runs.status = SUCCESS` |
| Some rows invalid | Clean rows load, rejected rows are stored, `status = PARTIAL` |
| Same data rerun for same date | No duplicate rows; existing rows are updated |
| Same product appears on next date | New row is inserted as historical snapshot |
| DB insert fails during load | Product/rejected writes roll back, run is marked `FAILED` |

---

## Run Guide

### 1. Start the Docker stack

```powershell
docker compose up airflow-init
docker compose up -d
```

### 2. Initialize PostgreSQL schema

From the host:

```powershell
docker exec airflow_scheduler python /opt/airflow/scripts/init_db.py
```

Optional direct SQL check, if the repository is mounted inside the container:

```powershell
docker exec -it ecommerce_postgres psql -U etl_user -d ecommerce_db -f /opt/airflow/sql/create_tables.sql
```

In normal Docker usage, prefer `scripts/init_db.py` because it uses the project path layout.

### 3. Run transformation tests

```powershell
python -m pytest tests\test_transformer.py -v
```

### 4. Run loader tests without PostgreSQL

Some loader tests do not need a real PostgreSQL database:

```powershell
python -m pytest tests\test_loader.py -v
```

DB-backed tests will skip unless `TEST_DATABASE_URL` is set.

### 5. Run DB-backed loader and integration tests

Create or choose a dedicated test database, then set:

```powershell
$env:TEST_DATABASE_URL="postgresql+psycopg2://etl_user:etl_password@localhost:5432/ecommerce_db_test"
python -m pytest tests\test_loader.py tests\test_integration.py -v
```

Do not point `TEST_DATABASE_URL` at a database with important manual data, because tests truncate:

```sql
rejected_records, products_market, pipeline_runs
```

### 6. Run all project tests

Recommended command:

```powershell
python -m pytest tests
```

Current observed result:

```text
115 passed, 13 skipped
```

Note: running plain `python -m pytest` from the repo root may try to collect Airflow runtime logs under `logs/` on Windows. Use `python -m pytest tests` for the project test suite.

### 7. Verify database contents manually

Open psql:

```powershell
docker exec -it ecommerce_postgres psql -U etl_user -d ecommerce_db
```

Check clean products:

```sql
SELECT product_id, source, extraction_date, price, rating, pipeline_run_id
FROM products_market
ORDER BY extraction_date DESC, product_id
LIMIT 20;
```

Check run audit:

```sql
SELECT run_id, dag_id, execution_date, status,
       rows_extracted, rows_transformed, rows_rejected,
       rows_loaded, data_quality_score, error_message
FROM pipeline_runs
ORDER BY started_at DESC
LIMIT 10;
```

Check rejected records:

```sql
SELECT rejection_reason, COUNT(*) AS rejected_count
FROM rejected_records
GROUP BY rejection_reason
ORDER BY rejected_count DESC;
```

Check idempotency:

```sql
SELECT product_id, source, extraction_date, COUNT(*)
FROM products_market
GROUP BY product_id, source, extraction_date
HAVING COUNT(*) > 1;
```

Expected result: zero rows.

---

## Test Summary

Current test command:

```powershell
python -m pytest tests
```

Current result:

```text
115 passed, 13 skipped
```

Test coverage by area:

| Test file | Purpose |
|-----------|---------|
| `tests/test_transformer.py` | Cleaning steps, schema validation, rejection reasons, quality score |
| `tests/test_loader.py` | Pipeline run logging, product UPSERT, rejected insert, rollback behavior, archive handling |
| `tests/test_integration.py` | Transformer-to-loader flow, idempotency, new-date historical snapshot, partial loads, DB failure rollback |
| `tests/test_extractor.py` | Existing Sprint 1 extractor behavior |
| `tests/test_tiki_scraper.py` | Existing Sprint 1 Tiki scraper behavior |

Skipped tests:

- Loader and integration tests that require PostgreSQL are skipped when `TEST_DATABASE_URL` is not configured.
- This is intentional to avoid accidentally truncating a developer's default database.

---

## Bonus Deliverables

| Item | Status | Notes |
|------|--------|-------|
| `data_quality_score` metric | Done | Returned by transformer and stored in `pipeline_runs`. |
| Quality threshold warning | Done | Loader logs warning if score is below `cfg.min_quality_score`. |
| JSON-safe rejected raw data | Done | Nested timestamps, missing values, and numpy scalars are normalized before JSONB insert. |
| Archive checksum verification | Done | MD5 is checked before deleting the staging source file. |
| Historical snapshot integration test | Done | Same product on a new `extraction_date` creates a second row. |
| Rollback tests | Done | Tests cover rejected insert failure and final status update failure. |

---

## Known Caveats and Carry-Forward Notes

- PostgreSQL-backed tests must be run with `TEST_DATABASE_URL` before a final demo or release checkpoint.
- Identical same-day UPSERT reruns currently count as updates because `ON CONFLICT DO UPDATE` always runs. This is acceptable for Sprint 2 because row-count idempotency is satisfied.
- Airflow DAG orchestration is not part of Sprint 2. It belongs to Sprint 3.
- Slack/email notification wiring is not part of Sprint 2. It belongs to Sprint 3.
- Metabase dashboard work is not part of Sprint 2. It belongs to Sprint 4.
- The loader assumes `sql/create_tables.sql` or `scripts/init_db.py` has already created the required tables and indexes.

---

## Sprint 2 Conclusion

Sprint 2 moves the project from extraction-only into a real ETL pipeline core.

The system can now:

- clean raw product data,
- reject bad rows with reasons,
- calculate quality metrics,
- load valid data into PostgreSQL,
- preserve daily product history,
- avoid duplicate rows on rerun,
- audit each pipeline execution,
- roll back failed loads,
- and preserve raw staging files through archive handling.

This provides the foundation needed for Sprint 3, where Airflow will connect extraction, transformation, loading, and notification into one scheduled DAG.
