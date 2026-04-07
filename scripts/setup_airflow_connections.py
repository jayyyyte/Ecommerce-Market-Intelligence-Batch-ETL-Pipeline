#!/usr/bin/env python3
"""
scripts/setup_airflow_connections.py
-------------------------------------
Idempotently creates all required Airflow Connections and Variables.

Run this ONCE after `airflow-init` completes:

    docker exec -it airflow_scheduler \\
        python /opt/airflow/scripts/setup_airflow_connections.py

Safe to re-run: existing connections/variables are NOT overwritten
unless you pass --overwrite.
"""

import argparse
import os
import sys


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Set up Airflow Connections and Variables.")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing connections and variables (default: skip if exists).",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------
def setup_connections(overwrite: bool) -> None:
    """Create the postgres_ecommerce Connection in the Airflow metadata DB."""
    from airflow.models import Connection  # type: ignore
    from airflow.utils.db import provide_session  # type: ignore

    @provide_session
    def _upsert(session=None):
        conn_id = "postgres_ecommerce"
        existing = session.query(Connection).filter_by(conn_id=conn_id).first()

        if existing and not overwrite:
            print(f"   ⏭   Connection '{conn_id}' already exists — skipping.")
            return

        conn = Connection(
            conn_id=conn_id,
            conn_type="postgres",
            host=os.environ.get("POSTGRES_HOST", "postgres"),
            schema=os.environ.get("POSTGRES_DB", "ecommerce_db"),
            login=os.environ.get("POSTGRES_USER", "etl_user"),
            password=os.environ.get("POSTGRES_PASSWORD", "etl_password"),
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
        )

        if existing:
            session.delete(existing)
            session.commit()
            print(f"   ♻️   Connection '{conn_id}' replaced.")
        else:
            print(f"   ✅  Connection '{conn_id}' created.")

        session.add(conn)
        session.commit()

    _upsert()


# ---------------------------------------------------------------------------
# Variables
# ---------------------------------------------------------------------------
def setup_variables(overwrite: bool) -> None:
    """Create all Airflow Variables required by the pipeline."""
    from airflow.models import Variable  # type: ignore

    variables = {
        # ── Data source ───────────────────────────────────────────────────
        "DATA_SOURCE_URL": os.environ.get(
            "DATA_SOURCE_URL", "https://fakestoreapi.com/products"
        ),
        # ── Filesystem ────────────────────────────────────────────────────
        "STAGING_DIR": os.environ.get("STAGING_DIR", "/opt/airflow/staging"),
        "ARCHIVE_RETENTION_DAYS": os.environ.get("ARCHIVE_RETENTION_DAYS", "90"),
        # ── Retry / resilience ────────────────────────────────────────────
        "MAX_RETRIES": os.environ.get("MAX_RETRIES", "3"),
        "REQUEST_TIMEOUT": "30",
        "RATE_LIMIT_DELAY": "1.5",
        # ── Alerting ──────────────────────────────────────────────────────
        "SLACK_WEBHOOK_URL": os.environ.get("SLACK_WEBHOOK_URL", ""),
        # ── Runtime environment ───────────────────────────────────────────
        "PIPELINE_ENV": os.environ.get("PIPELINE_ENV", "dev"),
        # ── Data quality ──────────────────────────────────────────────────
        "MIN_QUALITY_SCORE": "70.0",
        # ── PostgreSQL connection id ──────────────────────────────────────
        "POSTGRES_CONN_ID": "postgres_ecommerce",
    }

    for key, default_value in variables.items():
        existing = Variable.get(key, default_var=None)

        if existing is not None and not overwrite:
            print(f"   ⏭   Variable '{key}' already exists — skipping.")
            continue

        Variable.set(key, default_value)
        action = "replaced" if existing is not None else "created"
        print(f"   ✅  Variable '{key}' {action}.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    args = parse_args()

    print("=" * 60)
    print("  E-Commerce ETL — Airflow Setup")
    print("=" * 60)

    # Verify we're running inside Airflow's environment
    try:
        import airflow  # noqa: F401
    except ImportError:
        print(
            "❌  This script must be run inside the Airflow container where\n"
            "   the 'airflow' package is installed.\n"
            "\n"
            "   Run with:\n"
            "   docker exec -it airflow_scheduler \\\n"
            "       python /opt/airflow/scripts/setup_airflow_connections.py",
            file=sys.stderr,
        )
        sys.exit(1)

    print("\n📡  Setting up Connections …")
    setup_connections(overwrite=args.overwrite)

    print("\n🔧  Setting up Variables …")
    setup_variables(overwrite=args.overwrite)

    print("\n🎉  Airflow Connections and Variables are ready!")
    print(
        "\n   Verify in the UI:\n"
        "   • Connections: Admin → Connections → search 'postgres_ecommerce'\n"
        "   • Variables:   Admin → Variables\n"
    )


if __name__ == "__main__":
    main()