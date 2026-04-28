from __future__ import annotations

import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
import sqlalchemy as sa

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl.config import PipelineConfig
from etl.loader import DataLoader


TEST_DB_URL = os.environ.get("TEST_DATABASE_URL")
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SQL_FILE = PROJECT_ROOT / "sql" / "create_tables.sql"


def _require_test_engine() -> sa.Engine:
    if not TEST_DB_URL:
        pytest.skip("Set TEST_DATABASE_URL to run PostgreSQL loader tests.")

    engine = sa.create_engine(TEST_DB_URL)
    try:
        with engine.connect() as connection:
            connection.execute(sa.text("SELECT 1"))
    except Exception as exc:
        pytest.skip(f"PostgreSQL test database unavailable: {exc}")

    raw = engine.raw_connection()
    try:
        with raw.cursor() as cursor:
            cursor.execute(SQL_FILE.read_text(encoding="utf-8"))
        raw.commit()
    finally:
        raw.close()

    return engine


@pytest.fixture
def pg_engine() -> sa.Engine:
    engine = _require_test_engine()
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "TRUNCATE TABLE rejected_records, products_market, pipeline_runs "
                "RESTART IDENTITY CASCADE"
            )
        )
    return engine


@pytest.fixture
def loader_cfg(tmp_path: Path) -> PipelineConfig:
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    return PipelineConfig(staging_dir=str(staging_dir), archive_retention_days=30)


@pytest.fixture
def loader(pg_engine: sa.Engine, loader_cfg: PipelineConfig) -> DataLoader:
    return DataLoader(pg_engine, loader_cfg)


def _start_test_run(
    loader: DataLoader,
    run_id: str | None = None,
    execution_date: str = "2026-04-21",
) -> str:
    run_id = run_id or str(uuid.uuid4())
    loader.log_run_start(
        run_id=run_id,
        dag_id=f"test_dag_{run_id}",
        execution_date=execution_date,
    )
    return run_id


@pytest.fixture
def clean_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "product_id": "sku-1",
                "name": "Backpack",
                "category": "bags",
                "price": 10.5,
                "rating": 4.2,
                "review_count": 3,
                "stock_status": "in_stock",
                "source": "fakestore_api",
                "source_url": "https://example.com/sku-1",
                "extraction_date": pd.Timestamp("2026-04-21"),
                "pipeline_run_id": "ignore-me",
            },
            {
                "product_id": "sku-2",
                "name": "Shoes",
                "category": "footwear",
                "price": 19.99,
                "rating": 3.7,
                "review_count": 5,
                "stock_status": "unknown",
                "source": "fakestore_api",
                "source_url": "https://example.com/sku-2",
                "extraction_date": pd.Timestamp("2026-04-21"),
                "pipeline_run_id": "ignore-me",
            },
        ]
    )


@pytest.fixture
def rejected_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "run_id": "old-run",
                "raw_data": {"product_id": None, "price": 20},
                "rejection_reason": "null_product_id",
                "rejected_at": datetime(2026, 4, 21, 0, 0, tzinfo=timezone.utc),
            }
        ]
    )


def test_log_run_start_inserts_running_row(loader: DataLoader) -> None:
    run_id = str(uuid.uuid4())
    loader.log_run_start(run_id=run_id, dag_id="test_dag", execution_date="2026-04-21")

    with loader.engine.connect() as connection:
        row = connection.execute(
            sa.text(
                "SELECT run_id, dag_id, execution_date, status "
                "FROM pipeline_runs WHERE run_id = :run_id"
            ),
            {"run_id": run_id},
        ).mappings().one()

    assert row["run_id"] == run_id
    assert row["dag_id"] == "test_dag"
    assert row["execution_date"].isoformat() == "2026-04-21"
    assert row["status"] == "RUNNING"


def test_log_run_start_is_idempotent_for_same_run_id(loader: DataLoader) -> None:
    run_id = str(uuid.uuid4())
    loader.log_run_start(run_id=run_id, dag_id="test_dag", execution_date="2026-04-21")
    loader.log_run_end(
        run_id=run_id,
        status="SUCCESS",
        counts={
            "rows_extracted": 2,
            "rows_transformed": 2,
            "rows_rejected": 0,
            "rows_loaded": 2,
            "data_quality_score": 100.0,
        },
    )

    loader.log_run_start(run_id=run_id, dag_id="test_dag", execution_date="2026-04-21")

    with loader.engine.connect() as connection:
        row = connection.execute(
            sa.text(
                "SELECT status, rows_loaded, finished_at, error_message "
                "FROM pipeline_runs WHERE run_id = :run_id"
            ),
            {"run_id": run_id},
        ).mappings().one()
        run_count = connection.execute(
            sa.text("SELECT COUNT(*) FROM pipeline_runs WHERE run_id = :run_id"),
            {"run_id": run_id},
        ).scalar_one()

    assert run_count == 1
    assert row["status"] == "RUNNING"
    assert row["rows_loaded"] == 0
    assert row["finished_at"] is None
    assert row["error_message"] is None


def test_mark_run_failed_creates_auditable_failed_row(loader: DataLoader) -> None:
    run_id = str(uuid.uuid4())

    loader.mark_run_failed(
        run_id=run_id,
        dag_id="test_dag",
        execution_date="2026-04-21",
        error_msg="schema validation failed",
        counts={"rows_extracted": 3},
    )

    with loader.engine.connect() as connection:
        row = connection.execute(
            sa.text(
                "SELECT status, rows_extracted, rows_loaded, error_message "
                "FROM pipeline_runs WHERE run_id = :run_id"
            ),
            {"run_id": run_id},
        ).mappings().one()

    assert row["status"] == "FAILED"
    assert row["rows_extracted"] == 3
    assert row["rows_loaded"] == 0
    assert row["error_message"] == "schema validation failed"


def test_load_products_inserts_new_rows(
    loader: DataLoader, clean_df: pd.DataFrame
) -> None:
    run_id = _start_test_run(loader)
    inserted, updated = loader.load_products(clean_df, run_id=run_id)

    with loader.engine.connect() as connection:
        row_count = connection.execute(
            sa.text("SELECT COUNT(*) FROM products_market")
        ).scalar_one()

    assert inserted == 2
    assert updated == 0
    assert row_count == 2


def test_load_products_is_idempotent_for_identical_rows(
    loader: DataLoader, clean_df: pd.DataFrame
) -> None:
    first_run_id = _start_test_run(loader)
    second_run_id = _start_test_run(loader)
    loader.load_products(clean_df, run_id=first_run_id)
    inserted, updated = loader.load_products(clean_df, run_id=second_run_id)

    with loader.engine.connect() as connection:
        row_count = connection.execute(
            sa.text("SELECT COUNT(*) FROM products_market")
        ).scalar_one()

    assert inserted == 0
    assert updated == 2
    assert row_count == 2


def test_load_products_updates_only_mutable_columns(
    loader: DataLoader, clean_df: pd.DataFrame
) -> None:
    first_run_id = _start_test_run(loader)
    second_run_id = _start_test_run(loader)
    loader.load_products(clean_df, run_id=first_run_id)

    updated_df = clean_df.copy()
    updated_df.loc[0, "name"] = "Renamed Backpack"
    updated_df.loc[0, "category"] = "renamed_category"
    updated_df.loc[0, "source_url"] = "https://example.com/new-sku-1"
    updated_df.loc[0, "price"] = 15.25
    updated_df.loc[0, "rating"] = 4.8
    updated_df.loc[0, "review_count"] = 99
    updated_df.loc[0, "stock_status"] = "out_of_stock"

    inserted, updated = loader.load_products(updated_df, run_id=second_run_id)

    with loader.engine.connect() as connection:
        row = connection.execute(
            sa.text(
                "SELECT name, category, source_url, price, rating, review_count, "
                "stock_status, pipeline_run_id "
                "FROM products_market WHERE product_id = 'sku-1'"
            )
        ).mappings().one()

    assert inserted == 0
    assert updated == 2
    assert row["name"] == "Backpack"
    assert row["category"] == "bags"
    assert row["source_url"] == "https://example.com/sku-1"
    assert float(row["price"]) == pytest.approx(15.25)
    assert float(row["rating"]) == pytest.approx(4.8)
    assert row["review_count"] == 99
    assert row["stock_status"] == "out_of_stock"
    assert row["pipeline_run_id"] == second_run_id


def test_load_rejected_inserts_rows_with_current_run_id(
    loader: DataLoader, rejected_df: pd.DataFrame
) -> None:
    run_id = _start_test_run(loader)
    inserted = loader.load_rejected(rejected_df, run_id=run_id)

    with loader.engine.connect() as connection:
        row = connection.execute(
            sa.text(
                "SELECT run_id, rejection_reason FROM rejected_records WHERE run_id = :run_id"
            ),
            {"run_id": run_id},
        ).mappings().one()

    assert inserted == 1
    assert row["run_id"] == run_id
    assert row["rejection_reason"] == "null_product_id"


def test_log_run_end_updates_status_counts_and_error(loader: DataLoader) -> None:
    run_id = str(uuid.uuid4())
    loader.log_run_start(run_id=run_id, dag_id="test_dag", execution_date="2026-04-21")
    loader.log_run_end(
        run_id=run_id,
        status="FAILED",
        counts={
            "rows_extracted": 10,
            "rows_transformed": 8,
            "rows_rejected": 2,
            "rows_loaded": 0,
            "data_quality_score": 80.0,
        },
        error_msg="forced failure",
    )

    with loader.engine.connect() as connection:
        row = connection.execute(
            sa.text(
                "SELECT status, rows_extracted, rows_transformed, rows_rejected, "
                "rows_loaded, data_quality_score, error_message, finished_at "
                "FROM pipeline_runs WHERE run_id = :run_id"
            ),
            {"run_id": run_id},
        ).mappings().one()

    assert row["status"] == "FAILED"
    assert row["rows_extracted"] == 10
    assert row["rows_transformed"] == 8
    assert row["rows_rejected"] == 2
    assert row["rows_loaded"] == 0
    assert float(row["data_quality_score"]) == pytest.approx(80.0)
    assert row["error_message"] == "forced failure"
    assert row["finished_at"] is not None


def test_run_load_rolls_back_when_rejected_insert_fails(
    pg_engine: sa.Engine,
    loader_cfg: PipelineConfig,
    clean_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
) -> None:
    class FailingLoader(DataLoader):
        def load_rejected(self, df, run_id, connection=None) -> int:  # type: ignore[override]
            raise RuntimeError("forced rejected insert failure")

    run_id = str(uuid.uuid4())
    loader = FailingLoader(pg_engine, loader_cfg)

    with pytest.raises(RuntimeError, match="forced rejected insert failure"):
        loader.run_load(
            clean_df,
            rejected_df,
            run_id=run_id,
            dag_id="test_dag",
            execution_date="2026-04-21",
            rows_extracted=3,
            rows_transformed=2,
            quality_score=66.67,
        )

    with loader.engine.connect() as connection:
        product_count = connection.execute(
            sa.text("SELECT COUNT(*) FROM products_market")
        ).scalar_one()
        run_row = connection.execute(
            sa.text("SELECT status, rows_loaded, error_message FROM pipeline_runs WHERE run_id = :run_id"),
            {"run_id": run_id},
        ).mappings().one()

    assert product_count == 0
    assert run_row["status"] == "FAILED"
    assert run_row["rows_loaded"] == 0
    assert "forced rejected insert failure" in run_row["error_message"]


def test_run_load_rolls_back_when_final_status_update_fails(
    pg_engine: sa.Engine,
    loader_cfg: PipelineConfig,
    clean_df: pd.DataFrame,
) -> None:
    class FailingFinalStatusLoader(DataLoader):
        def log_run_end(  # type: ignore[override]
            self,
            run_id,
            status,
            counts,
            error_msg=None,
            connection=None,
        ) -> None:
            if connection is not None and status == "SUCCESS":
                raise RuntimeError("forced final status failure")
            return super().log_run_end(run_id, status, counts, error_msg, connection)

    run_id = str(uuid.uuid4())
    loader = FailingFinalStatusLoader(pg_engine, loader_cfg)

    with pytest.raises(RuntimeError, match="forced final status failure"):
        loader.run_load(
            clean_df,
            pd.DataFrame(),
            run_id=run_id,
            dag_id="test_dag",
            execution_date="2026-04-21",
            rows_extracted=2,
            rows_transformed=2,
            quality_score=100.0,
        )

    with loader.engine.connect() as connection:
        product_count = connection.execute(
            sa.text("SELECT COUNT(*) FROM products_market")
        ).scalar_one()
        run_row = connection.execute(
            sa.text("SELECT status, rows_loaded, error_message FROM pipeline_runs WHERE run_id = :run_id"),
            {"run_id": run_id},
        ).mappings().one()

    assert product_count == 0
    assert run_row["status"] == "FAILED"
    assert run_row["rows_loaded"] == 0
    assert "forced final status failure" in run_row["error_message"]


def test_archive_staging_moves_file_and_preserves_checksum(
    loader_cfg: PipelineConfig,
) -> None:
    engine = sa.create_engine("sqlite://")
    loader = DataLoader(engine, loader_cfg)
    staging_file = Path(loader_cfg.staging_dir) / "raw_products.json"
    staging_file.write_text('{"hello":"world"}', encoding="utf-8")

    archived = loader.archive_staging(staging_file, "2026-04-21")

    assert archived.exists()
    assert archived.name == "raw_products.json"
    assert not staging_file.exists()
    assert archived.read_text(encoding="utf-8") == '{"hello":"world"}'


def test_cleanup_archive_removes_only_expired_files(loader_cfg: PipelineConfig) -> None:
    engine = sa.create_engine("sqlite://")
    loader = DataLoader(engine, loader_cfg)
    old_file = loader.archive_root / "2026" / "03" / "01" / "old.json"
    new_file = loader.archive_root / "2026" / "04" / "20" / "new.json"
    old_file.parent.mkdir(parents=True, exist_ok=True)
    new_file.parent.mkdir(parents=True, exist_ok=True)
    old_file.write_text("old", encoding="utf-8")
    new_file.write_text("new", encoding="utf-8")

    reference = datetime(2026, 4, 22, tzinfo=timezone.utc)
    old_mtime = (reference - timedelta(days=60)).timestamp()
    new_mtime = (reference - timedelta(days=5)).timestamp()
    os.utime(old_file, (old_mtime, old_mtime))
    os.utime(new_file, (new_mtime, new_mtime))

    deleted = loader.cleanup_archive(now=reference)

    assert deleted == 1
    assert not old_file.exists()
    assert new_file.exists()


def test_normalize_scalar_preserves_json_like_values(loader_cfg: PipelineConfig) -> None:
    engine = sa.create_engine("sqlite://")
    loader = DataLoader(engine, loader_cfg)

    assert loader._normalize_scalar({"items": [1, 2]}) == {"items": [1, 2]}
    assert loader._normalize_scalar([1, 2, 3]) == [1, 2, 3]


def test_json_safe_normalizes_nested_pandas_values(loader_cfg: PipelineConfig) -> None:
    engine = sa.create_engine("sqlite://")
    loader = DataLoader(engine, loader_cfg)

    raw_data = {
        "seen_at": pd.Timestamp("2026-04-21T08:30:00Z"),
        "price": pd.NA,
        "items": [{"count": pd.Series([3], dtype="int64").iloc[0]}],
    }

    assert loader._json_safe(raw_data) == {
        "seen_at": "2026-04-21T08:30:00+00:00",
        "price": None,
        "items": [{"count": 3}],
    }


def test_coerce_datetime_treats_naive_iso_strings_as_utc(
    loader_cfg: PipelineConfig,
) -> None:
    engine = sa.create_engine("sqlite://")
    loader = DataLoader(engine, loader_cfg)

    coerced = loader._coerce_datetime("2026-04-21T00:00:00")

    assert coerced.isoformat() == "2026-04-21T00:00:00+00:00"
