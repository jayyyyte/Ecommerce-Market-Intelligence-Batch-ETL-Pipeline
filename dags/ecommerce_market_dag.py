"""Production Airflow DAG for the E-Commerce Market ETL pipeline."""

from __future__ import annotations

import json
import logging
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import sqlalchemy as sa
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl.config import PipelineConfig, get_config
from etl.extractor import FakeStoreExtractor
from etl.loader import DataLoader
from etl.notifier import PipelineNotifier, notify_on_failure
from etl.transformer import DataTransformer

logger = logging.getLogger(__name__)

DAG_ID = "ecommerce_market_etl"
RAW_FILENAME = "raw_products.json"
CLEAN_FILENAME = "clean_products.json"
REJECTED_FILENAME = "rejected_records.json"
METRICS_FILENAME = "transform_metrics.json"
ALLOWED_EXTRA_RAW_COLUMNS = {"description", "extraction_timestamp"}


def deterministic_run_id(dag_id: str, execution_date: str) -> str:
    """Create a stable run id for one DAG and logical execution date."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{dag_id}:{execution_date}"))


def build_postgres_engine(cfg: PipelineConfig) -> sa.Engine:
    """Build the application DB engine from the Airflow connection."""
    from airflow.hooks.base import BaseHook

    connection = BaseHook.get_connection(cfg.postgres_conn_id)
    url = sa.engine.URL.create(
        "postgresql+psycopg2",
        username=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port,
        database=connection.schema,
    )
    return sa.create_engine(url)


def extract_data(**context: Any) -> dict[str, Any]:
    """Extract FakeStore data, stage raw JSON, and create the run audit row."""
    cfg = get_config()
    execution_date = context["ds"]
    run_id = deterministic_run_id(DAG_ID, execution_date)
    engine = build_postgres_engine(cfg)
    loader = DataLoader(engine, cfg)
    loader.log_run_start(run_id=run_id, dag_id=DAG_ID, execution_date=execution_date)

    extractor = FakeStoreExtractor(staging_dir=cfg.staging_dir)
    records = extractor.extract(execution_date=execution_date, run_id=run_id)
    raw_path = Path(cfg.staging_dir) / execution_date / RAW_FILENAME

    metadata = {
        "run_id": run_id,
        "execution_date": execution_date,
        "raw_path": str(raw_path),
        "rows_extracted": len(records),
    }
    _push_xcom(context, metadata)
    return metadata


def validate_schema(**context: Any) -> dict[str, Any]:
    """Validate staged raw data structure before transformation."""
    extract_meta = _pull_return_value(context, "extract_data")
    run_id = extract_meta["run_id"]
    raw_path = Path(extract_meta["raw_path"])
    raw_df = _read_json_records(raw_path)

    transformer = DataTransformer(get_config())
    _raise_on_unexpected_columns(raw_df, transformer)
    working_df = transformer._prune_columns(raw_df)
    if not working_df.empty:
        transformer._raise_if_required_columns_missing(working_df)
    working_df = transformer._ensure_expected_columns(working_df)
    working_df = transformer._standardize_types(working_df)
    transformer._run_structural_check(working_df)

    metadata = {
        "run_id": run_id,
        "execution_date": extract_meta["execution_date"],
        "raw_path": str(raw_path),
        "rows_validated": len(raw_df),
    }
    _push_xcom(context, metadata)
    return metadata


def transform_data(**context: Any) -> dict[str, Any]:
    """Transform staged raw data and write transformed artifacts to staging."""
    extract_meta = _pull_return_value(context, "extract_data")
    run_id = extract_meta["run_id"]
    execution_date = extract_meta["execution_date"]
    raw_path = Path(extract_meta["raw_path"])
    output_dir = raw_path.parent

    raw_df = _read_json_records(raw_path)
    transformer = DataTransformer(get_config())
    clean_df, rejected_df, quality_score = transformer.transform(raw_df, run_id)

    clean_path = output_dir / CLEAN_FILENAME
    rejected_path = output_dir / REJECTED_FILENAME
    metrics_path = output_dir / METRICS_FILENAME
    _write_dataframe(clean_df, clean_path)
    _write_dataframe(rejected_df, rejected_path)

    metrics = {
        "run_id": run_id,
        "execution_date": execution_date,
        "rows_extracted": int(extract_meta["rows_extracted"]),
        "rows_transformed": int(len(clean_df)),
        "rows_rejected": int(len(rejected_df)),
        "data_quality_score": float(quality_score),
    }
    metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    metadata = {
        **metrics,
        "raw_path": str(raw_path),
        "clean_path": str(clean_path),
        "rejected_path": str(rejected_path),
        "metrics_path": str(metrics_path),
    }
    _push_xcom(context, metadata)
    return metadata


def load_to_db(**context: Any) -> dict[str, Any]:
    """Load transformed artifacts into PostgreSQL and archive raw data."""
    transform_meta = _pull_return_value(context, "transform_data")
    cfg = get_config()
    engine = build_postgres_engine(cfg)
    loader = DataLoader(engine, cfg)

    clean_df = _read_json_records(Path(transform_meta["clean_path"]))
    rejected_df = _read_json_records(Path(transform_meta["rejected_path"]))
    result = loader.run_load(
        clean_df,
        rejected_df,
        run_id=transform_meta["run_id"],
        dag_id=DAG_ID,
        execution_date=transform_meta["execution_date"],
        rows_extracted=int(transform_meta["rows_extracted"]),
        rows_transformed=int(transform_meta["rows_transformed"]),
        quality_score=float(transform_meta["data_quality_score"]),
    )

    raw_path = Path(transform_meta["raw_path"])
    archived_path = _archive_raw_file(loader, raw_path, transform_meta["execution_date"])
    deleted_archives = loader.cleanup_archive()

    metadata = {
        **transform_meta,
        **result,
        "archived_path": str(archived_path) if archived_path else None,
        "deleted_archives": deleted_archives,
    }
    _push_xcom(context, metadata)
    return metadata


def notify_status(**context: Any) -> dict[str, Any]:
    """Send a final status notification and mark pre-load failures in DB."""
    execution_date = context["ds"]
    run_id = deterministic_run_id(DAG_ID, execution_date)
    failed_tasks = _failed_task_ids(context)
    notifier = PipelineNotifier(get_config())

    load_meta = _pull_return_value(context, "load_to_db", default={})
    transform_meta = _pull_return_value(context, "transform_data", default={})
    metrics = {**transform_meta, **load_meta}

    if failed_tasks:
        error_message = f"Failed task(s): {', '.join(failed_tasks)}"
        _mark_failed_run(context, run_id, execution_date, error_message, metrics)
        notifier.send_failure(
            context,
            run_id=run_id,
            failed_tasks=failed_tasks,
            error_message=error_message,
        )
        raise AirflowException(error_message)

    status = str(metrics.get("status", "SUCCESS"))
    notifier.send_success(
        context,
        run_id=run_id,
        status=status,
        rows_extracted=int(metrics.get("rows_extracted", 0)),
        rows_loaded=int(metrics.get("rows_loaded", 0)),
        rows_rejected=int(metrics.get("rows_rejected", 0)),
        quality_score=float(metrics.get("data_quality_score", 0.0)),
    )
    return {"status": status, "run_id": run_id}


def _push_xcom(context: dict[str, Any], metadata: dict[str, Any]) -> None:
    task_instance = context.get("ti")
    if task_instance is None:
        return
    for key, value in metadata.items():
        task_instance.xcom_push(key=key, value=value)


def _pull_return_value(
    context: dict[str, Any],
    task_id: str,
    default: dict[str, Any] | None = None,
) -> dict[str, Any]:
    task_instance = context.get("ti")
    if task_instance is None:
        return default or {}
    value = task_instance.xcom_pull(task_ids=task_id, key="return_value")
    return value or default or {}


def _read_json_records(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Expected staged artifact is missing: {path}")
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return pd.DataFrame()
    return pd.DataFrame(json.loads(text))


def _write_dataframe(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(df.to_json(orient="records", date_format="iso"), encoding="utf-8")


def _raise_on_unexpected_columns(
    df: pd.DataFrame,
    transformer: DataTransformer,
) -> None:
    expected = set(transformer.expected_columns) | ALLOWED_EXTRA_RAW_COLUMNS
    unexpected = sorted(set(df.columns) - expected)
    if unexpected:
        raise ValueError(
            "Schema drift detected -- unexpected columns in raw data: "
            + ", ".join(unexpected)
        )


def _archive_raw_file(
    loader: DataLoader,
    raw_path: Path,
    execution_date: str,
) -> Path | None:
    if raw_path.exists():
        return loader.archive_staging(raw_path, execution_date)

    archive_path = (
        loader.archive_root
        / loader._coerce_execution_date(execution_date).strftime("%Y/%m/%d")
        / raw_path.name
    )
    if archive_path.exists():
        logger.info("Raw file already archived at %s", archive_path)
        return archive_path

    raise FileNotFoundError(f"Raw file missing and no archive exists: {raw_path}")


def _failed_task_ids(context: dict[str, Any]) -> list[str]:
    dag_run = context.get("dag_run")
    if dag_run is None:
        return []
    return [
        task_instance.task_id
        for task_instance in dag_run.get_task_instances()
        if task_instance.state == "failed"
    ]


def _mark_failed_run(
    context: dict[str, Any],
    run_id: str,
    execution_date: str,
    error_message: str,
    metrics: dict[str, Any],
) -> None:
    try:
        cfg = get_config()
        loader = DataLoader(build_postgres_engine(cfg), cfg)
        loader.mark_run_failed(
            run_id=run_id,
            dag_id=DAG_ID,
            execution_date=execution_date,
            error_msg=error_message,
            counts={
                "rows_extracted": int(metrics.get("rows_extracted", 0)),
                "rows_transformed": int(metrics.get("rows_transformed", 0)),
                "rows_rejected": int(metrics.get("rows_rejected", 0)),
                "rows_loaded": int(metrics.get("rows_loaded", 0)),
                "data_quality_score": metrics.get("data_quality_score"),
            },
        )
    except Exception as exc:
        logger.warning("Could not mark failed pipeline run: %s", exc)


default_args = {
    "owner": "etl-team",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": notify_on_failure,
}


with DAG(
    dag_id=DAG_ID,
    description="Daily E-Commerce market ETL pipeline.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 0 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "sprint-3"],
) as dag:
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        execution_timeout=timedelta(minutes=30),
    )

    validate_task = PythonOperator(
        task_id="validate_schema",
        python_callable=validate_schema,
        execution_timeout=timedelta(minutes=10),
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        execution_timeout=timedelta(minutes=30),
    )

    load_task = PythonOperator(
        task_id="load_to_db",
        python_callable=load_to_db,
        execution_timeout=timedelta(minutes=30),
    )

    notify_task = PythonOperator(
        task_id="notify_status",
        python_callable=notify_status,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=5),
        retries=0,
        on_failure_callback=None,
    )

    extract_task >> validate_task >> transform_task >> load_task >> notify_task
