# DAG: `hello_world_etl_check`

**File:** `dags/hello_world_dag.py`

## Overview

A smoke-test DAG that verifies the complete Phase 1 infrastructure is correctly configured. Runs five assertion tasks in a fan-in pattern — each check is independent and fails with an actionable error message if something is misconfigured. Not part of the production pipeline.

---

## Responsibilities

### ✅ Handles
- Verifying `etl.config.get_config()` is importable and returns expected values
- Testing the `postgres_ecommerce` Airflow Connection is live and all 3 schema tables exist
- Asserting all required Airflow Variables are present
- Confirming the staging directory exists and is writable
- Printing a final success banner when all checks pass

### ❌ Does NOT handle
- Any real data extraction, transformation, or loading
- Scheduled automatic execution (manual trigger only: `schedule_interval=None`)
- Creating missing resources — it only checks and fails with a clear message

---

## DAG Configuration

| Property | Value |
|---|---|
| `dag_id` | `hello_world_etl_check` |
| `schedule_interval` | `None` (manual trigger only) |
| `catchup` | `False` |
| `retries` | `0` |
| `tags` | `smoke-test`, `phase-1` |

---

## Task Graph

```
check_config ──────────────────────┐
check_db_connection ───────────────┤──► print_summary
check_airflow_variables ───────────┤
check_staging_directory ───────────┘
```

All four check tasks run in **parallel**. `print_summary` only runs when all four succeed (default `ALL_SUCCESS` trigger rule).

---

## Tasks

| Task ID | What it checks | Fails when |
|---|---|---|
| `check_config` | `get_config()` importable; `data_source_url` and `staging_dir` are non-empty | Import error or empty required field |
| `check_db_connection` | `postgres_ecommerce` connection reachable; all 3 tables exist in `ecommerce_db` | Connection refused or missing tables |
| `check_airflow_variables` | All 5 critical Variables (`DATA_SOURCE_URL`, `STAGING_DIR`, `MAX_RETRIES`, `PIPELINE_ENV`, `POSTGRES_CONN_ID`) are set | Any Variable returns `None` |
| `check_staging_directory` | Staging dir exists and a `.write_probe` file can be written and read | `OSError` / permission denied |
| `print_summary` | *(no assertion)* Prints success banner | Never fails |

---

## Usage

### Trigger from Airflow UI
1. Go to **DAGs** → find `hello_world_etl_check`
2. Toggle the DAG **ON** (unpause)
3. Click ▶ **Trigger DAG**
4. Open the run → **Graph** view → all tasks should turn green

### Trigger from CLI

```bash
docker exec airflow_scheduler \
    airflow dags trigger hello_world_etl_check
```

### Interpret failures

| Task that fails | Likely cause | Fix |
|---|---|---|
| `check_config` | `etl` package not on Python path | Check `volumes` mount in `docker-compose.yml` |
| `check_db_connection` | Tables missing | Run `scripts/init_db.py` |
| `check_airflow_variables` | Variables not created | Run `scripts/setup_airflow_connections.py` |
| `check_staging_directory` | Volume not mounted | Check `staging:/opt/airflow/staging` in compose |

---

## Dependencies

### Internal
| Module | Used for |
|---|---|
| `etl.config` | Imported and tested in `check_config` task |

### Airflow
| Import | Used for |
|---|---|
| `airflow.hooks.base.BaseHook` | Resolving `postgres_ecommerce` connection credentials |
| `airflow.models.Variable` | Checking Variable existence |

---

## Notes / Constraints

- **Delete or disable this DAG in production** — it serves no operational purpose once Phase 1 is validated. Leave it paused if you want to keep it for future re-validation.
- The `check_db_connection` task uses `BaseHook.get_connection()` which reads directly from the Airflow metadata DB — it does not call `get_config().db_url`. This is intentional: it tests the Airflow Connection independently from the ETL config.
- The staging directory probe creates and immediately deletes `.write_probe` — it does not leave any files behind.