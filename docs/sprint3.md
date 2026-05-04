# Sprint 3 Review - Airflow Orchestration & Error Handling

**Sprint focus:** Phase 5 - Apache Airflow orchestration, retry behavior, failure handling, and notifications  
**Main deliverable:** The full ETL flow can run as one scheduled Airflow DAG with extraction, validation, transformation, loading, and final status notification.

---

## Sprint Status

Sprint 3 is complete for the implemented pipeline behavior.

The DAG imports cleanly in Airflow, runs end-to-end, writes audit data to PostgreSQL, handles failed upstream tasks correctly, and has tests covering DAG shape, notification behavior, loader idempotency, and database integration.


Operational note: Slack delivery requires a real `SLACK_WEBHOOK_URL`. The notifier code is complete and tested, but a placeholder/example webhook will log an HTTP warning instead of delivering a real Slack message.

---

## Sprint Goal

By the end of Sprint 3, the project can:

- Trigger the full ETL pipeline from the Airflow UI.
- Run the DAG automatically every day at `00:00` UTC.
- Execute the task chain:
  `extract_data -> validate_schema -> transform_data -> load_to_db -> notify_status`.
- Retry transient task failures with Airflow retry settings.
- Stop the pipeline before loading when schema validation fails.
- Persist pipeline run status and row counts in PostgreSQL.
- Send success and failure summaries through the notifier.
- Mark the DAG run as failed when any upstream task fails.
- Support reruns and backfill-style execution using date-aware staging paths and deterministic run ids.

**Result:** Sprint 3 connects the Sprint 1 extractor and Sprint 2 transformer/loader into a production Airflow DAG.

---

## Acceptance Criteria

| # | Criterion | Evidence / Notes |
|---|-----------|--------|------------------|
| 1 | DAG `ecommerce_market_etl` is visible in Airflow UI | `airflow dags list` shows `ecommerce_market_etl`; `airflow dags list-import-errors` returns `No data found`. |
| 2 | DAG has 5 required tasks | `extract_data`, `validate_schema`, `transform_data`, `load_to_db`, `notify_status`. |
| 3 | Tasks run in the required order | `extract -> validate -> transform -> load -> notify`. |
| 4 | DAG runs daily at `00:00` UTC | `schedule_interval="0 0 * * *"`. |
| 5 | `max_active_runs=1` prevents overlapping runs | Done | Configured in `dags/ecommerce_market_dag.py`. |
| 6 | Airflow retries are configured | `retries=3`, `retry_delay=5 minutes`, `retry_exponential_backoff=True`. |
| 7 | Each task has execution timeout | Main tasks use 10-30 minute timeouts; notifier uses 5 minutes. |
| 8 | Schema drift halts before transform/load | `validate_schema` checks unexpected/missing columns before transformation. |
| 9 | Load writes to PostgreSQL and archives raw file | `load_to_db` calls `DataLoader.run_load()`, archives raw staging file, and cleans old archive files. |
| 10 | Notification runs after success or failure | `notify_status` uses `TriggerRule.ALL_DONE`. |
| 11 | Failed upstream task makes DAG run failed | `notify_status` raises `AirflowException` after sending/recording failure. |
| 12 | Immediate task-level failure callback exists | `default_args["on_failure_callback"] = notify_on_failure`. |
| 13 | Slack/webhook notification does not break the DAG | `PipelineNotifier` catches request exceptions and HTTP error responses. |
| 14 | Backfill/rerun uses logical date | Paths use `context["ds"]`; run id is deterministic from `dag_id + execution_date`. |
| 15 | Same-day product reruns are idempotent | Product rows use UPSERT on `(product_id, source, extraction_date)`. |
| 16 | Same-run rejected records do not duplicate | `DataLoader.run_load()` deletes rejected rows for the current `run_id` before reinserting. |
| 17 | No default hardcoded DB password in `dags/`, `etl/`, or setup scripts | Password fallback removed; use `.env` or Airflow Connection. |
| 18 | Real Slack receipt verified | Code is complete; requires a real `SLACK_WEBHOOK_URL`. Empty value disables Slack in dev. |

---

## What Was Built

### 1. Production Airflow DAG

Implemented in `dags/ecommerce_market_dag.py`.

The DAG is named:

```text
ecommerce_market_etl
```

Runtime configuration:

| Setting | Value |
|---------|-------|
| `schedule_interval` | `0 0 * * *` |
| `start_date` | `2026-01-01` |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| `retries` | `3` |
| `retry_delay` | `5 minutes` |
| `retry_exponential_backoff` | `True` |
| `on_failure_callback` | `notify_on_failure` |

Task graph:

```text
extract_data
  -> validate_schema
  -> transform_data
  -> load_to_db
  -> notify_status
```

### 2. Extract Task

Task: `extract_data`

Responsibilities:

- Build a deterministic `run_id` from DAG id and logical execution date.
- Create or reset a `pipeline_runs` row with status `RUNNING`.
- Call `FakeStoreExtractor.extract()`.
- Save raw records to:

```text
/opt/airflow/staging/<execution_date>/raw_products.json
```

- Return metadata through XCom instead of pushing the full raw payload.

Returned metadata includes:

| Field | Meaning |
|-------|---------|
| `run_id` | Stable id for one DAG and logical date |
| `execution_date` | Airflow `ds` value |
| `raw_path` | Staged raw JSON path |
| `rows_extracted` | Number of rows extracted |

### 3. Validate Schema Task

Task: `validate_schema`

Responsibilities:

- Read `raw_products.json`.
- Detect unexpected columns before transformation.
- Use `DataTransformer` schema helpers to verify expected structure.
- Fail early if the source shape drifts.

This prevents bad source structure from reaching transformation or database loading.

### 4. Transform Task

Task: `transform_data`

Responsibilities:

- Read staged raw JSON.
- Call `DataTransformer.transform(raw_df, run_id)`.
- Write clean rows to:

```text
/opt/airflow/staging/<execution_date>/clean_products.json
```

- Write rejected rows to:

```text
/opt/airflow/staging/<execution_date>/rejected_records.json
```

- Write transform metrics to:

```text
/opt/airflow/staging/<execution_date>/transform_metrics.json
```

Metrics include row counts and `data_quality_score`.

### 5. Load Task

Task: `load_to_db`

Responsibilities:

- Read clean and rejected artifacts.
- Call `DataLoader.run_load()`.
- UPSERT clean products into `products_market`.
- Insert rejected records into `rejected_records`.
- Update `pipeline_runs` with final counts and status.
- Archive raw file to:

```text
/opt/airflow/archive/YYYY/MM/DD/raw_products.json
```

- Clean expired archive files based on `archive_retention_days`.

Important fix completed during Sprint 3:

- The loader now accepts `execution_date` values serialized as ISO datetimes, such as `2026-05-02T00:00:00.000`, not only `YYYY-MM-DD`.

### 6. Notification Task

Task: `notify_status`

Responsibilities:

- Always run after upstream tasks finish by using `TriggerRule.ALL_DONE`.
- Collect upstream task states.
- Send success summary if all upstream tasks succeeded.
- Mark the pipeline run failed and send failure summary if any upstream task failed.
- Raise `AirflowException` after failure notification so Airflow marks the DAG run failed.

This fixes the issue where `load_to_db` could fail while `notify_status` succeeded and made the DAG look green.

### 7. Notifier Module

Implemented in `etl/notifier.py`.

Main API:

```python
notifier = PipelineNotifier()
notifier.send_success(context, run_id=run_id, rows_loaded=20)
notifier.send_failure(context, run_id=run_id, failed_tasks=["load_to_db"])
```

Behavior:

- Empty `SLACK_WEBHOOK_URL` disables Slack in dev mode.
- HTTP errors are logged as warnings.
- Request exceptions are caught.
- Notification failure never becomes pipeline-critical.
- `notify_on_failure(context)` is compatible with Airflow task callbacks.

### 8. Docker Runtime Fix

`docker-compose.yml` was updated so Airflow can import shared retry utilities:

```yaml
- ./utils:/opt/airflow/utils
```

Without this mount, Airflow could import `etl`, but `etl.extractor` failed on:

```text
ModuleNotFoundError: No module named 'utils'
```

The obsolete Compose `version` field was also removed.

---

## Working Process

Sprint 3 was implemented in this order:

1. Defined `ecommerce_market_etl` DAG and task graph.
2. Connected extraction, validation, transformation, and loading through XCom metadata.
3. Persisted only file paths and counts through XCom, not full datasets.
4. Added final notification task with `TriggerRule.ALL_DONE`.
5. Added Slack-compatible notifier helper.
6. Fixed Airflow import issue by mounting `utils/` into the Airflow containers.
7. Fixed PowerShell SQL initialization workflow.
8. Fixed DAG failure semantics so upstream failure makes the DAG run fail.
9. Added immediate `on_failure_callback` for task failures.
10. Removed default DB password fallback from source code.
11. Made rejected-record loading idempotent for same `run_id`.
12. Fixed `load_to_db` date parsing for ISO datetime strings from JSON artifacts.
13. Re-ran unit, integration, and Airflow container tests.
14. Ran an end-to-end Airflow DAG test.

---

## Run Guide

Run these commands from PowerShell in the repository root:

```powershell
cd C:\Users\Tlinh\Ecommerce-Market-Batch-ETL-Pipeline
```

### 1. Start or Recreate Airflow Containers

```powershell
docker compose up -d --force-recreate airflow-webserver airflow-scheduler
```

If Postgres is not running:

```powershell
docker compose up -d
```

### 2. Initialize PostgreSQL Tables

Do not use this command from the Postgres container:

```powershell
docker exec -it ecommerce_postgres psql -U etl_user -d ecommerce_db -f /opt/airflow/sql/create_tables.sql
```

That path exists in the Airflow containers, not in the Postgres container.

Use this PowerShell-compatible command instead:

```powershell
Get-Content .\sql\create_tables.sql | docker exec -i ecommerce_postgres psql -U etl_user -d ecommerce_db
```

Expected output includes `CREATE TABLE`, `CREATE INDEX`, and possibly `already exists` notices. The notices are normal because the SQL script is idempotent.

### 3. Check DAG Import

```powershell
docker exec airflow_scheduler airflow dags list-import-errors
```

Expected:

```text
No data found
```

Check the DAG exists:

```powershell
docker exec airflow_scheduler airflow dags list | findstr ecommerce_market_etl
```

### 4. Run Full DAG from CLI

Use a logical date:

```powershell
docker exec airflow_scheduler airflow dags test ecommerce_market_etl 2026-05-03
```

Expected behavior:

- `extract_data` succeeds.
- `validate_schema` succeeds.
- `transform_data` succeeds.
- `load_to_db` loads rows into PostgreSQL and archives raw data.
- `notify_status` succeeds when upstream tasks succeed.
- DAG run state is `success`.

### 5. Run Full DAG from Airflow UI

1. Open:

```text
http://localhost:8080
```

2. Find `ecommerce_market_etl`.
3. Unpause the DAG.
4. Click Trigger DAG.
5. Open Graph view.
6. Confirm all tasks turn green on a clean run.

### 6. Check Database Results

Open `psql`:

```powershell
docker exec -it ecommerce_postgres psql -U etl_user -d ecommerce_db
```

Run:

```sql
SELECT status,
       execution_date,
       rows_extracted,
       rows_transformed,
       rows_loaded,
       rows_rejected,
       data_quality_score,
       error_message
FROM pipeline_runs
ORDER BY started_at DESC
LIMIT 5;
```

Check product and rejected counts:

```sql
SELECT COUNT(*) FROM products_market;
SELECT COUNT(*) FROM rejected_records;
```

### 7. Run Tests Locally

Local tests that do not require Airflow/Postgres:

```powershell
$env:PYTHONPATH=(Get-Location).Path
pytest tests/test_loader.py tests/test_notifier.py -q
```

Expected from latest verification:

```text
11 passed, 11 skipped
```

The skipped tests are PostgreSQL-backed tests when `TEST_DATABASE_URL` is not configured.

### 8. Run Tests Inside Airflow Container

DAG and notifier tests:

```powershell
docker exec -e PYTHONPATH=/opt/airflow airflow_scheduler pytest /opt/airflow/tests/test_ecommerce_market_dag.py /opt/airflow/tests/test_notifier.py -q
```

Expected from latest verification:

```text
11 passed
```

Loader integration tests against the running Postgres container:

```powershell
docker exec -e PYTHONPATH=/opt/airflow airflow_scheduler bash -lc 'TEST_DATABASE_URL="postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}" pytest /opt/airflow/tests/test_loader.py -q'
```

Expected from latest verification:

```text
17 passed
```

---

## Backfill and Rerun Notes

Production DAG config keeps:

```python
catchup=False
```

For a demo backfill, use Airflow CLI rather than permanently changing production config:

```powershell
docker exec airflow_scheduler airflow dags backfill -s 2026-05-01 -e 2026-05-07 ecommerce_market_etl
```

Important behavior:

- Each logical date writes staging files under `/opt/airflow/staging/<YYYY-MM-DD>/`.
- `run_id` is deterministic from `dag_id + execution_date`.
- Re-running the same logical date resets the `pipeline_runs` row to `RUNNING`, then updates it to the final status.
- Product rows are UPSERTed by `(product_id, source, extraction_date)`.
- Rejected rows for the same run are replaced before inserting the new rejected set.

---

## Failure Handling

### Expected Failure Flow

If an upstream task fails:

1. Airflow retries according to `default_args`.
2. After final failure, `notify_on_failure` sends an immediate task-level alert.
3. `notify_status` still runs because it uses `TriggerRule.ALL_DONE`.
4. `notify_status` marks the run failed in PostgreSQL when possible.
5. `notify_status` sends a final failure summary.
6. `notify_status` raises `AirflowException`.
7. Airflow marks `notify_status` failed and the DAG run failed.

This is intentional. The notification task should not hide upstream failure.

### Notification Delivery

Slack/webhook behavior depends on `SLACK_WEBHOOK_URL`:

| Value | Behavior |
|-------|----------|
| Empty string | Slack disabled; notifier logs and returns. |
| Real Slack webhook | Success/failure message is posted. |
| Placeholder or invalid URL | Warning is logged; DAG does not fail because of notification delivery. |

---

## Verification Evidence

Latest verification performed on May 4, 2026:

| Check | Result |
|-------|--------|
| Local loader/notifier tests | `11 passed, 11 skipped` |
| Airflow DAG import errors | `No data found` |
| Airflow container DAG/notifier tests | `11 passed` |
| Airflow container loader integration tests with Postgres | `17 passed` |
| Full DAG CLI test | `airflow dags test ecommerce_market_etl 2026-05-03` succeeded |
| E2E row result | `load_to_db` loaded `20` rows |

Known warning:

- Airflow logs `RemovedInAirflow3Warning` for `schedule_interval`. This is acceptable on Airflow 2.7.3. A future cleanup can migrate to `schedule=`.

---

## Files Changed or Added

| File | Purpose |
|------|---------|
| `dags/ecommerce_market_dag.py` | Production DAG and task implementations |
| `etl/notifier.py` | Slack/webhook notification helper and Airflow failure callback |
| `etl/loader.py` | Idempotent rejected-row replay and ISO datetime execution-date parsing |
| `etl/config.py` | Central config without hardcoded password fallback |
| `docker-compose.yml` | Airflow runtime volume mount for `utils/` |
| `scripts/init_db.py` | DB init script without password fallback |
| `scripts/setup_airflow_connections.py` | Airflow connection setup without password fallback |
| `tests/test_ecommerce_market_dag.py` | DAG structure, XCom, failure-status behavior |
| `tests/test_notifier.py` | Notification success/failure/noop/error behavior |
| `tests/test_loader.py` | Loader idempotency, rejected-row replay, date parsing, DB behavior |

---

## Troubleshooting

### DAG import error: `No module named 'utils'`

Recreate Airflow containers so the updated volume mount is active:

```powershell
docker compose up -d --force-recreate airflow-webserver airflow-scheduler
```

Then check:

```powershell
docker exec airflow_scheduler airflow dags list-import-errors
```

### PowerShell rejects `< sql/create_tables.sql`

PowerShell does not support that shell redirection syntax for this use case.

Use:

```powershell
Get-Content .\sql\create_tables.sql | docker exec -i ecommerce_postgres psql -U etl_user -d ecommerce_db
```

### `notify_status` is red after an upstream task failed

This is correct. It means Sprint 3 failure handling is working: the final notification task is deliberately failing the DAG run after sending/recording the failure.

### Slack returns HTTP 405 or another warning

The webhook URL is invalid or a placeholder. Set `SLACK_WEBHOOK_URL` to a real Slack Incoming Webhook URL, or set it to an empty string in dev mode.

---

## Sprint 3 Conclusion

Sprint 3 completes the orchestration layer.

The project now has a working Airflow-controlled batch ETL pipeline that can:

- extract raw product data,
- validate source schema,
- transform and score data quality,
- load clean and rejected records into PostgreSQL,
- archive raw staging files,
- audit each run,
- retry transient failures,
- notify success/failure,
- and correctly mark DAG runs failed when upstream work fails.