"""
Loading layer for the Batch ETL pipeline.

This module persists cleaned product rows and rejected records into PostgreSQL,
tracks pipeline run status, and archives raw staging files after successful
loads.
"""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
from contextlib import nullcontext
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID, insert
from sqlalchemy.engine import Connection, Engine

from etl.config import PipelineConfig, get_config

logger = logging.getLogger(__name__)

metadata = sa.MetaData()

pipeline_runs = sa.Table(
    "pipeline_runs",
    metadata,
    sa.Column("run_id", UUID(as_uuid=False), primary_key=True),
    sa.Column("dag_id", sa.String(200), nullable=False),
    sa.Column("execution_date", sa.Date(), nullable=False),
    sa.Column("status", sa.String(20), nullable=False),
    sa.Column("rows_extracted", sa.Integer()),
    sa.Column("rows_transformed", sa.Integer()),
    sa.Column("rows_rejected", sa.Integer()),
    sa.Column("rows_loaded", sa.Integer()),
    sa.Column("data_quality_score", sa.Numeric(5, 2)),
    sa.Column("started_at", sa.DateTime(timezone=True)),
    sa.Column("finished_at", sa.DateTime(timezone=True)),
    sa.Column("error_message", sa.Text()),
)

products_market = sa.Table(
    "products_market",
    metadata,
    sa.Column("id", sa.Integer(), primary_key=True),
    sa.Column("product_id", sa.String(100), nullable=False),
    sa.Column("name", sa.String(500), nullable=False),
    sa.Column("category", sa.String(100)),
    sa.Column("price", sa.Numeric(12, 2), nullable=False),
    sa.Column("rating", sa.Numeric(3, 2)),
    sa.Column("review_count", sa.Integer()),
    sa.Column("stock_status", sa.String(50)),
    sa.Column("source", sa.String(100), nullable=False),
    sa.Column("source_url", sa.Text()),
    sa.Column("extraction_date", sa.Date(), nullable=False),
    sa.Column("pipeline_run_id", UUID(as_uuid=False)),
    sa.Column("created_at", sa.DateTime(timezone=True)),
    sa.Column("updated_at", sa.DateTime(timezone=True)),
)

rejected_records = sa.Table(
    "rejected_records",
    metadata,
    sa.Column("id", sa.Integer(), primary_key=True),
    sa.Column("run_id", UUID(as_uuid=False)),
    sa.Column("raw_data", JSONB, nullable=False),
    sa.Column("rejection_reason", sa.String(200), nullable=False),
    sa.Column("rejected_at", sa.DateTime(timezone=True), nullable=False),
)


class DataLoader:
    """Persist transformed pipeline data into PostgreSQL."""

    def __init__(self, engine: Engine, cfg: PipelineConfig | None = None) -> None:
        self.engine = engine
        self.cfg = cfg or get_config()
        self.archive_root = self._derive_archive_root(self.cfg.staging_dir)

    def log_run_start(self, run_id: str, dag_id: str, execution_date: str | date) -> None:
        """Insert or reset the pipeline run record with RUNNING status."""
        started_at = datetime.now(timezone.utc)
        base_insert = insert(pipeline_runs).values(
            run_id=run_id,
            dag_id=dag_id,
            execution_date=self._coerce_execution_date(execution_date),
            status="RUNNING",
            rows_extracted=0,
            rows_transformed=0,
            rows_rejected=0,
            rows_loaded=0,
            data_quality_score=None,
            started_at=started_at,
            finished_at=None,
            error_message=None,
        )
        statement = base_insert.on_conflict_do_update(
            index_elements=["run_id"],
            set_={
                "status": "RUNNING",
                "rows_extracted": 0,
                "rows_transformed": 0,
                "rows_rejected": 0,
                "rows_loaded": 0,
                "data_quality_score": None,
                "started_at": sa.case(
                    (pipeline_runs.c.status == "RUNNING", pipeline_runs.c.started_at),
                    else_=started_at,
                ),
                "finished_at": None,
                "error_message": None,
            },
        )
        with self.engine.begin() as connection:
            connection.execute(statement)

    def load_products(
        self,
        df: pd.DataFrame,
        run_id: str,
        connection: Connection | None = None,
    ) -> tuple[int, int]:
        """UPSERT cleaned products into products_market and return insert/update counts."""
        if df.empty:
            return 0, 0

        records = [self._serialize_product_row(row, run_id) for _, row in df.iterrows()]
        base_insert = insert(products_market).values(records)
        upsert = base_insert.on_conflict_do_update(
            index_elements=["product_id", "source", "extraction_date"],
            set_={
                "price": base_insert.excluded.price,
                "rating": base_insert.excluded.rating,
                "review_count": base_insert.excluded.review_count,
                "stock_status": base_insert.excluded.stock_status,
                "pipeline_run_id": run_id,
                "updated_at": sa.func.now(),
            },
        ).returning(sa.literal_column("xmax = 0").label("inserted"))

        context = self._connection_context(connection)
        with context as active_connection:
            results = active_connection.execute(upsert).fetchall()

        inserted = sum(1 for row in results if row.inserted)
        updated = len(results) - inserted
        return inserted, updated

    def load_rejected(
        self,
        df: pd.DataFrame,
        run_id: str,
        connection: Connection | None = None,
    ) -> int:
        """Bulk insert rejected rows into rejected_records."""
        if df.empty:
            return 0

        records = [self._serialize_rejected_row(row, run_id) for _, row in df.iterrows()]
        statement = insert(rejected_records).values(records)

        context = self._connection_context(connection)
        with context as active_connection:
            active_connection.execute(statement)

        return len(records)

    def delete_rejected_for_run(
        self,
        run_id: str,
        connection: Connection | None = None,
    ) -> int:
        """Remove rejected rows for a run before replaying an idempotent load."""
        statement = sa.delete(rejected_records).where(rejected_records.c.run_id == run_id)
        context = self._connection_context(connection)
        with context as active_connection:
            result = active_connection.execute(statement)
        return int(result.rowcount or 0)

    def log_run_end(
        self,
        run_id: str,
        status: str,
        counts: dict[str, Any],
        error_msg: str | None = None,
        connection: Connection | None = None,
    ) -> None:
        """Update the pipeline run row with final counts and status."""
        statement = (
            sa.update(pipeline_runs)
            .where(pipeline_runs.c.run_id == run_id)
            .values(
                status=status,
                rows_extracted=int(counts.get("rows_extracted", 0)),
                rows_transformed=int(counts.get("rows_transformed", 0)),
                rows_rejected=int(counts.get("rows_rejected", 0)),
                rows_loaded=int(counts.get("rows_loaded", 0)),
                data_quality_score=counts.get("data_quality_score"),
                finished_at=datetime.now(timezone.utc),
                error_message=error_msg,
            )
        )
        context = self._connection_context(connection)
        with context as active_connection:
            active_connection.execute(statement)

    def mark_run_failed(
        self,
        run_id: str,
        dag_id: str,
        execution_date: str | date,
        error_msg: str,
        counts: dict[str, Any] | None = None,
    ) -> None:
        """Ensure a pipeline run row exists, then mark it FAILED."""
        if not self._pipeline_run_exists(run_id):
            self.log_run_start(run_id=run_id, dag_id=dag_id, execution_date=execution_date)
        self.log_run_end(
            run_id=run_id,
            status="FAILED",
            counts=counts or {},
            error_msg=error_msg,
        )

    def run_load(
        self,
        clean_df: pd.DataFrame,
        rejected_df: pd.DataFrame,
        *,
        run_id: str,
        dag_id: str,
        execution_date: str | date,
        rows_extracted: int,
        rows_transformed: int,
        quality_score: float,
    ) -> dict[str, int | float | str | None]:
        """Run the end-to-end database load and update pipeline_runs."""
        self.log_run_start(run_id=run_id, dag_id=dag_id, execution_date=execution_date)

        if quality_score < self.cfg.min_quality_score:
            logger.warning(
                "Data quality score %.2f%% is below threshold %.2f%%",
                quality_score,
                self.cfg.min_quality_score,
            )

        inserted = 0
        updated = 0
        rejected_count = 0

        try:
            with self.engine.begin() as connection:
                inserted, updated = self.load_products(clean_df, run_id, connection=connection)
                self.delete_rejected_for_run(run_id, connection=connection)
                rejected_count = self.load_rejected(
                    rejected_df, run_id, connection=connection
                )

                rows_loaded = inserted + updated
                status = "SUCCESS" if rejected_count == 0 else "PARTIAL"
                counts = {
                    "rows_extracted": rows_extracted,
                    "rows_transformed": rows_transformed,
                    "rows_rejected": rejected_count,
                    "rows_loaded": rows_loaded,
                    "data_quality_score": quality_score,
                }
                self.log_run_end(
                    run_id=run_id,
                    status=status,
                    counts=counts,
                    connection=connection,
                )
            return {
                "status": status,
                "rows_extracted": rows_extracted,
                "rows_transformed": rows_transformed,
                "rows_rejected": rejected_count,
                "rows_loaded": rows_loaded,
                "rows_inserted": inserted,
                "rows_updated": updated,
                "data_quality_score": quality_score,
                "error_message": None,
            }
        except Exception as exc:
            counts = {
                "rows_extracted": rows_extracted,
                "rows_transformed": rows_transformed,
                "rows_rejected": 0,
                "rows_loaded": 0,
                "data_quality_score": quality_score,
            }
            self.log_run_end(
                run_id=run_id,
                status="FAILED",
                counts=counts,
                error_msg=str(exc),
            )
            raise

    def archive_staging(self, staging_path: str | Path, execution_date: str | date) -> Path:
        """Copy a staging file into the dated archive path and verify MD5 integrity."""
        source = Path(staging_path)
        if not source.exists():
            raise FileNotFoundError(f"Staging file not found: {source}")

        archive_dir = self.archive_root / self._coerce_execution_date(execution_date).strftime(
            "%Y/%m/%d"
        )
        archive_dir.mkdir(parents=True, exist_ok=True)
        destination = archive_dir / source.name

        shutil.copy2(source, destination)

        source_md5 = self._compute_md5(source)
        destination_md5 = self._compute_md5(destination)
        if source_md5 != destination_md5:
            destination.unlink(missing_ok=True)
            raise ValueError(f"Checksum mismatch while archiving {source}")

        source.unlink()
        logger.info("Archived %s to %s", source.name, destination)
        return destination

    def cleanup_archive(self, now: datetime | None = None) -> int:
        """Delete archived files older than the configured retention window."""
        if not self.archive_root.exists():
            return 0

        reference = now or datetime.now(timezone.utc)
        cutoff = reference.timestamp() - (self.cfg.archive_retention_days * 86400)
        deleted = 0

        for path in self.archive_root.rglob("*"):
            if not path.is_file():
                continue
            if path.stat().st_mtime < cutoff:
                path.unlink()
                deleted += 1

        return deleted

    def _connection_context(self, connection: Connection | None):
        if connection is not None:
            return nullcontext(connection)
        return self.engine.begin()

    def _serialize_product_row(self, row: pd.Series, run_id: str) -> dict[str, Any]:
        return {
            "product_id": self._normalize_scalar(row["product_id"]),
            "name": self._normalize_scalar(row["name"]),
            "category": self._normalize_scalar(row.get("category")),
            "price": self._normalize_scalar(row["price"]),
            "rating": self._normalize_scalar(row.get("rating")),
            "review_count": int(self._normalize_scalar(row.get("review_count")) or 0),
            "stock_status": self._normalize_scalar(row.get("stock_status")),
            "source": self._normalize_scalar(row["source"]),
            "source_url": self._normalize_scalar(row.get("source_url")),
            "extraction_date": self._coerce_execution_date(row["extraction_date"]),
            "pipeline_run_id": run_id,
            "updated_at": datetime.now(timezone.utc),
        }

    def _serialize_rejected_row(self, row: pd.Series, run_id: str) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "raw_data": self._json_safe(row["raw_data"]),
            "rejection_reason": self._normalize_scalar(row["rejection_reason"]),
            "rejected_at": self._coerce_datetime(row.get("rejected_at")),
        }

    def _normalize_scalar(self, value: Any) -> Any:
        if isinstance(value, (dict, list, tuple, set)):
            return value
        if isinstance(value, pd.Timestamp):
            return self._coerce_datetime(value)
        if isinstance(value, datetime):
            return self._coerce_datetime(value)
        if self._is_missing_scalar(value):
            return None
        return value

    def _json_safe(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {str(key): self._json_safe(item) for key, item in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self._json_safe(item) for item in value]
        if self._is_missing_scalar(value):
            return None
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, Decimal):
            return float(value)
        if type(value).__module__ == "numpy" and hasattr(value, "item"):
            return self._json_safe(value.item())
        return value

    def _is_missing_scalar(self, value: Any) -> bool:
        try:
            is_missing = pd.isna(value)
        except Exception:
            return False
        if isinstance(is_missing, bool) or type(is_missing).__module__ == "numpy":
            return bool(is_missing)
        return False

    def _coerce_execution_date(self, value: Any) -> date:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, pd.Timestamp):
            return value.date()
        if isinstance(value, datetime):
            return value.date()
        text = str(value)
        try:
            return date.fromisoformat(text)
        except ValueError:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).date()

    def _coerce_datetime(self, value: Any) -> datetime:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return datetime.now(timezone.utc)
        if isinstance(value, pd.Timestamp):
            value = value.to_pydatetime()
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        parsed = datetime.fromisoformat(str(value))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _pipeline_run_exists(self, run_id: str) -> bool:
        statement = (
            sa.select(pipeline_runs.c.run_id)
            .where(pipeline_runs.c.run_id == run_id)
            .limit(1)
        )
        with self.engine.connect() as connection:
            return connection.execute(statement).first() is not None

    def _derive_archive_root(self, staging_dir: str | os.PathLike[str]) -> Path:
        staging_path = Path(staging_dir)
        if not staging_path.is_absolute():
            staging_path = staging_path.resolve()
        return staging_path.parent / "archive"

    def _compute_md5(self, path: Path) -> str:
        digest = hashlib.md5()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(8192), b""):
                digest.update(chunk)
        return digest.hexdigest()
