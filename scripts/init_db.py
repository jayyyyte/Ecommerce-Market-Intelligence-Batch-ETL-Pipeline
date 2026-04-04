#!/usr/bin/env python3
"""
scripts/init_db.py
------------------
Initialises the ecommerce_db schema by executing sql/create_tables.sql.

Usage (from project root):
    # Locally (requires .env to be present)
    python scripts/init_db.py

    # Inside the running Airflow container
    docker exec -it airflow_scheduler python /opt/airflow/scripts/init_db.py

    # One-liner from host (re-runs safely)
    docker exec airflow_scheduler python /opt/airflow/scripts/init_db.py
"""

import os
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Allow the script to be run both from the repo root and from inside a
# container where /opt/airflow is the working directory.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SQL_FILE = PROJECT_ROOT / "sql" / "create_tables.sql"

# Prefer environment variables already set (Docker / .env); fall back to
# sensible defaults that match .env.example.
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env", override=False)
except ImportError:
    pass  # python-dotenv not installed — rely on env vars being set already

import sqlalchemy as sa
from sqlalchemy import text


# ---------------------------------------------------------------------------
# Build the connection URL from environment variables
# ---------------------------------------------------------------------------
def get_engine() -> sa.Engine:
    user     = os.environ.get("POSTGRES_USER",     "etl_user")
    password = os.environ.get("POSTGRES_PASSWORD", "etl_password")
    host     = os.environ.get("POSTGRES_HOST",     "localhost")
    port     = os.environ.get("POSTGRES_PORT",     "5432")
    db       = os.environ.get("POSTGRES_DB",       "ecommerce_db")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return sa.create_engine(url, echo=False)


# ---------------------------------------------------------------------------
# Run the DDL file
# ---------------------------------------------------------------------------
def init_schema(engine: sa.Engine) -> None:
    if not SQL_FILE.exists():
        print(f"❌  SQL file not found: {SQL_FILE}", file=sys.stderr)
        sys.exit(1)

    ddl = SQL_FILE.read_text(encoding="utf-8")

    print(f"📂  Executing: {SQL_FILE}")
    with engine.begin() as conn:
        conn.execute(text(ddl))
    print("✅  Schema initialised successfully.\n")


# ---------------------------------------------------------------------------
# Verify the tables were created
# ---------------------------------------------------------------------------
def verify_tables(engine: sa.Engine) -> None:
    expected = {"pipeline_runs", "products_market", "rejected_records"}

    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
        ))
        found = {row[0] for row in result}

    missing = expected - found
    if missing:
        print(f"❌  Missing tables: {missing}", file=sys.stderr)
        sys.exit(1)

    for table in sorted(expected):
        # Row count for a quick sanity check
        with engine.connect() as conn:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        print(f"   ✔  {table:30s} — {count} rows")

    print("\n✅  All tables verified.\n")


# ---------------------------------------------------------------------------
# Index verification
# ---------------------------------------------------------------------------
def verify_indexes(engine: sa.Engine) -> None:
    expected_indexes = [
        "uq_products_market_key",
        "idx_products_category",
        "idx_products_extraction_date",
        "idx_products_source",
        "uq_pipeline_runs_dag_date",
        "idx_pipeline_runs_date",
        "idx_pipeline_runs_status",
        "idx_rejected_run_id",
        "idx_rejected_reason",
    ]

    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'"
        ))
        found = {row[0] for row in result}

    missing = [i for i in expected_indexes if i not in found]
    if missing:
        print(f"⚠️   Missing indexes: {missing}")
    else:
        print(f"✅  All {len(expected_indexes)} indexes verified.\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    print("=" * 60)
    print("  E-Commerce ETL — Database Schema Initialiser")
    print("=" * 60)

    engine = get_engine()

    # Test connectivity first
    try:
        with engine.connect() as conn:
            db_version = conn.execute(text("SELECT version()")).scalar()
        print(f"🔗  Connected: {db_version.split(',')[0]}\n")
    except Exception as exc:
        print(f"❌  Cannot connect to PostgreSQL: {exc}", file=sys.stderr)
        print(
            "    Ensure the postgres container is running and POSTGRES_* env vars are set.",
            file=sys.stderr,
        )
        sys.exit(1)

    init_schema(engine)
    verify_tables(engine)
    verify_indexes(engine)

    print("🎉  Day 3 complete — PostgreSQL schema is ready!\n")


if __name__ == "__main__":
    main()