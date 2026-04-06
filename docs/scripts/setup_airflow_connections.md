# Script: `scripts/setup_airflow_connections.py`

## Overview

Idempotently creates the Airflow Connection and all Variables required by the ETL pipeline. Must be run inside the Airflow container after `airflow db migrate` completes. Skips any item that already exists unless `--overwrite` is passed.

---

## Responsibilities

### ✅ Handles
- Creating the `postgres_ecommerce` Airflow Connection (type: `postgres`)
- Creating all 10 pipeline Airflow Variables from environment variables or hardcoded defaults
- Skipping existing items by default (safe to re-run)
- Replacing existing items when `--overwrite` is passed
- Printing a clear per-item status (created / replaced / skipped)

### ❌ Does NOT handle
- Setting up the PostgreSQL schema (→ `scripts/init_db.py`)
- Running Airflow DB migrations (`airflow db migrate`)
- Creating Airflow users
- Encrypting secrets (Airflow stores Variables encrypted via the Fernet key in `.env`)

---

## Usage

```bash
# Normal run — skips anything that already exists
docker exec airflow_scheduler \
    python /opt/airflow/scripts/setup_airflow_connections.py

# Force overwrite all existing connections and variables
docker exec airflow_scheduler \
    python /opt/airflow/scripts/setup_airflow_connections.py --overwrite
```

Also runs automatically during `docker compose up` via the `airflow-init` entrypoint command.

---

## What Gets Created

### Connection: `postgres_ecommerce`

| Field | Value source |
|---|---|
| Type | `postgres` (hardcoded) |
| Host | `POSTGRES_HOST` env var |
| Schema (DB) | `POSTGRES_DB` env var |
| Login | `POSTGRES_USER` env var |
| Password | `POSTGRES_PASSWORD` env var |
| Port | `POSTGRES_PORT` env var |

Test this connection in the Airflow UI: `Admin → Connections → postgres_ecommerce → Test`.

### Variables

| Variable Key | Default Value | Description |
|---|---|---|
| `DATA_SOURCE_URL` | `https://fakestoreapi.com/products` | API endpoint |
| `STAGING_DIR` | `/opt/airflow/staging` | Raw file output path |
| `ARCHIVE_RETENTION_DAYS` | `90` | Days to keep archived files |
| `MAX_RETRIES` | `3` | HTTP retry attempts |
| `REQUEST_TIMEOUT` | `30` | HTTP timeout (seconds) |
| `RATE_LIMIT_DELAY` | `1.5` | Delay between scraping page requests |
| `SLACK_WEBHOOK_URL` | `""` | Slack webhook (empty = disabled) |
| `PIPELINE_ENV` | `dev` | Runtime environment |
| `MIN_QUALITY_SCORE` | `70.0` | Min quality % before warning |
| `POSTGRES_CONN_ID` | `postgres_ecommerce` | Airflow connection ID used by pipeline |

---

## API

All functions are internal. Not imported by other modules.

| Function | Description |
|---|---|
| `setup_connections(overwrite)` | Creates/replaces `postgres_ecommerce` Connection |
| `setup_variables(overwrite)` | Creates/replaces all 10 Variables |
| `parse_args()` | Parses `--overwrite` CLI flag |
| `main()` | Entry point |

---

## Dependencies

| Library | Used for |
|---|---|
| `airflow.models.Connection` | Creating/querying Airflow Connections |
| `airflow.models.Variable` | Creating/querying Airflow Variables |
| `airflow.utils.db.provide_session` | SQLAlchemy session for Connection upsert |

---

## Notes / Constraints

- **Must run inside the Airflow container** — `airflow.models` is not available outside it. The script exits with code `1` and a clear error message if run in a non-Airflow environment.
- Variables set here are the **initial defaults**. You can override any of them later in the Airflow UI under `Admin → Variables` without re-running this script.
- **`SLACK_WEBHOOK_URL` defaults to `""`** — this is intentional for `dev` mode. Set it to a real webhook URL in the Airflow UI or `.env` before deploying to production.
- The `--overwrite` flag is useful when rotating secrets or correcting a misconfigured connection, but should not be used in automated deployments without care — it will reset any manually customised variable values.