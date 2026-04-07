"""
dags/hello_world_dag.py
------------------------
A minimal smoke-test DAG to verify the Airflow environment is correctly
configured before the real pipeline DAG is built in Sprint 3.

What it checks:
  ✔ DAG is parsed without import errors
  ✔ etl.config.get_config() is importable and returns the expected values
  ✔ The postgres_ecommerce connection is reachable
  ✔ All required Airflow Variables are present
  ✔ The staging directory is writable

Trigger manually from the UI or CLI:
    airflow dags trigger hello_world_etl_check
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Default arguments applied to every task in this DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "etl-team",
    "depends_on_past": False,
    "retries": 0,  # No retries for a smoke-test DAG
    "retry_delay": timedelta(minutes=1),
}

# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def check_config(**context) -> None:
    """Verify etl.config loads without errors and prints key values."""
    import sys
    sys.path.insert(0, "/opt/airflow")

    from etl.config import get_config  # type: ignore

    cfg = get_config()

    print("=" * 50)
    print("  Pipeline Configuration")
    print("=" * 50)
    print(f"  data_source_url        : {cfg.data_source_url}")
    print(f"  staging_dir            : {cfg.staging_dir}")
    print(f"  postgres_conn_id       : {cfg.postgres_conn_id}")
    print(f"  max_retries            : {cfg.max_retries}")
    print(f"  pipeline_env           : {cfg.pipeline_env}")
    print(f"  min_quality_score      : {cfg.min_quality_score}%")
    print(f"  slack_webhook_url set  : {'Yes' if cfg.slack_webhook_url else 'No (stub)'}")
    print("=" * 50)

    assert cfg.data_source_url, "DATA_SOURCE_URL is not set!"
    assert cfg.staging_dir, "STAGING_DIR is not set!"
    print("✅  Config check passed.")


def check_db_connection(**context) -> None:
    """Verify the postgres_ecommerce connection is live and the schema exists."""
    from airflow.hooks.base import BaseHook  # type: ignore
    import sqlalchemy as sa
    from sqlalchemy import text

    hook = BaseHook.get_connection("postgres_ecommerce")
    url = (
        f"postgresql+psycopg2://{hook.login}:{hook.password}"
        f"@{hook.host}:{hook.port}/{hook.schema}"
    )

    engine = sa.create_engine(url)

    with engine.connect() as conn:
        # Check PostgreSQL version
        version = conn.execute(text("SELECT version()")).scalar()
        print(f"🔗  Connected: {version.split(',')[0]}")

        # Check required tables exist
        result = conn.execute(text(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
        ))
        tables = {row[0] for row in result}

    required = {"products_market", "pipeline_runs", "rejected_records"}
    missing = required - tables

    if missing:
        raise RuntimeError(
            f"❌  Missing tables: {missing}\n"
            "    Run: docker exec airflow_scheduler python /opt/airflow/scripts/init_db.py"
        )

    for t in sorted(required):
        print(f"   ✔  Table '{t}' exists.")

    print("✅  Database connection check passed.")


def check_variables(**context) -> None:
    """Verify all required Airflow Variables are present."""
    from airflow.models import Variable  # type: ignore

    required_vars = [
        "DATA_SOURCE_URL",
        "STAGING_DIR",
        "MAX_RETRIES",
        "PIPELINE_ENV",
        "POSTGRES_CONN_ID",
    ]

    missing = []
    for var in required_vars:
        value = Variable.get(var, default_var=None)
        if value is None:
            missing.append(var)
            print(f"   ❌  Variable '{var}' — NOT FOUND")
        else:
            print(f"   ✔   Variable '{var}' = '{value}'")

    if missing:
        raise RuntimeError(
            f"Missing Airflow Variables: {missing}\n"
            "Run: docker exec airflow_scheduler "
            "python /opt/airflow/scripts/setup_airflow_connections.py"
        )

    print("✅  Variables check passed.")


def check_staging_dir(**context) -> None:
    """Verify the staging directory exists and is writable."""
    from airflow.models import Variable  # type: ignore

    staging = Variable.get("STAGING_DIR", default_var="/opt/airflow/staging")
    os.makedirs(staging, exist_ok=True)

    # Write + read a probe file
    probe = os.path.join(staging, ".write_probe")
    try:
        with open(probe, "w") as f:
            f.write("ok")
        with open(probe) as f:
            assert f.read() == "ok"
        os.remove(probe)
    except OSError as exc:
        raise RuntimeError(
            f"Staging directory '{staging}' is not writable: {exc}"
        ) from exc

    print(f"✅  Staging directory '{staging}' is writable.")


def print_summary(**context) -> None:
    """Print a final success banner."""
    print("\n" + "=" * 50)
    print("  🎉  ALL CHECKS PASSED — Phase 1 Complete!")
    print("=" * 50)
    print(
        "\n  Your Airflow environment is correctly configured.\n"
        "  Next step: implement etl/extractor.py (Day 5)\n"
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="hello_world_etl_check",
    description="Smoke-test DAG — verifies config, DB, Variables, and staging dir.",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # Manual trigger only
    catchup=False,
    tags=["smoke-test", "phase-1"],
) as dag:

    t1 = PythonOperator(
        task_id="check_config",
        python_callable=check_config,
    )

    t2 = PythonOperator(
        task_id="check_db_connection",
        python_callable=check_db_connection,
    )

    t3 = PythonOperator(
        task_id="check_airflow_variables",
        python_callable=check_variables,
    )

    t4 = PythonOperator(
        task_id="check_staging_directory",
        python_callable=check_staging_dir,
    )

    t5 = PythonOperator(
        task_id="print_summary",
        python_callable=print_summary,
    )

    # All checks run independently in parallel, then summary gate
    [t1, t2, t3, t4] >> t5