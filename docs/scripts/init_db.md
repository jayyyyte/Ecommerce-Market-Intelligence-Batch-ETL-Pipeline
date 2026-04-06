# Script: `scripts/init_db.py`

## Overview

One-time setup script that connects to `ecommerce_db` and executes `sql/create_tables.sql` to create all three pipeline tables and their indexes. Includes verification steps to confirm tables and indexes were created correctly. Safe to re-run due to `IF NOT EXISTS` guards in the SQL.

---

## Responsibilities

### ✅ Handles
- Reading DB credentials from environment variables or `.env` file
- Connecting to PostgreSQL via SQLAlchemy
- Executing `sql/create_tables.sql` as a single transaction
- Verifying all 3 tables exist after execution
- Verifying all 9 indexes exist after execution
- Printing a clear pass/fail summary to stdout

### ❌ Does NOT handle
- Creating the `ecommerce_db` database itself (done by `00_init_databases.sql` via Docker init)
- Creating `airflow_db` or `metabaseappdb`
- Running Airflow migrations
- Setting up Airflow Connections or Variables (→ `scripts/setup_airflow_connections.py`)

---

## Usage

### From host machine (requires `.env` to be present and postgres port exposed)

```bash
# Export variables or rely on python-dotenv picking up .env
python scripts/init_db.py
```

### From inside the Airflow container (recommended)

```bash
docker exec airflow_scheduler python /opt/airflow/scripts/init_db.py
```

### Automatic (runs as part of `airflow-init` container startup)

The `docker-compose.yml` `airflow-init` service runs `init_db.py` automatically after `airflow db migrate`. You only need to run it manually if you reset the database volume.

---

## API

All functions are internal helpers. The script is run as `__main__` — not imported by other modules.

| Function | Description |
|---|---|
| `get_engine()` | Builds a SQLAlchemy engine from env vars |
| `init_schema(engine)` | Reads and executes `sql/create_tables.sql` |
| `verify_tables(engine)` | Asserts all 3 tables exist; prints row counts |
| `verify_indexes(engine)` | Asserts all 9 indexes exist; prints missing ones as warnings |
| `main()` | Entry point: calls all four in sequence |

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_USER` | `etl_user` | DB username |
| `POSTGRES_PASSWORD` | `etl_password` | DB password |
| `POSTGRES_HOST` | `localhost` | DB host (`postgres` inside Docker) |
| `POSTGRES_PORT` | `5432` | DB port |
| `POSTGRES_DB` | `ecommerce_db` | Target database |

---

## Expected Output

```
============================================================
  E-Commerce ETL — Database Schema Initialiser
============================================================
🔗  Connected: PostgreSQL 15.x on x86_64-pc-linux-gnu
📂  Executing: /opt/airflow/sql/create_tables.sql
✅  Schema initialised successfully.

   ✔  pipeline_runs                — 0 rows
   ✔  products_market              — 0 rows
   ✔  rejected_records             — 0 rows

✅  All tables verified.
✅  All 9 indexes verified.

🎉  Day 3 complete — PostgreSQL schema is ready!
```

---

## Dependencies

| Library | Used for |
|---|---|
| `sqlalchemy` | Engine + connection |
| `psycopg2-binary` | PostgreSQL adapter |
| `python-dotenv` | Optional `.env` loading (gracefully skipped if not installed) |

---

## Notes / Constraints

- **`POSTGRES_HOST` difference:** Inside Docker, use `postgres` (the service name). Outside Docker (local dev), use `localhost`. The script defaults to `localhost` for the `get_engine()` call — the Docker `airflow-init` service sets `POSTGRES_HOST=postgres` via the compose env block.
- The script exits with code `1` on any failure (connection error, missing SQL file, missing tables) — this causes the `airflow-init` container to fail visibly rather than silently continuing.
- Re-running on an already-initialised database is safe — all DDL uses `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS`.