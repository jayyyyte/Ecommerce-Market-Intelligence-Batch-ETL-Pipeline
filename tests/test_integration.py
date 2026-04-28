from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

import pandas as pd
import pytest
import sqlalchemy as sa

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl.config import PipelineConfig
from etl.loader import DataLoader
from etl.transformer import DataTransformer


TEST_DB_URL = os.environ.get("TEST_DATABASE_URL")
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SQL_FILE = PROJECT_ROOT / "sql" / "create_tables.sql"


def _require_test_engine() -> sa.Engine:
    if not TEST_DB_URL:
        pytest.skip("Set TEST_DATABASE_URL to run integration tests.")

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
def cfg(tmp_path: Path) -> PipelineConfig:
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    return PipelineConfig(staging_dir=str(staging_dir), min_quality_score=70.0)


def test_happy_path_transform_to_load_persists_expected_rows(
    pg_engine: sa.Engine, cfg: PipelineConfig
) -> None:
    run_id = str(uuid.uuid4())
    transformer = DataTransformer(cfg)
    loader = DataLoader(pg_engine, cfg)
    raw_df = pd.DataFrame(
        [
            {
                "product_id": "1",
                "name": "Backpack",
                "category": "men's clothing",
                "price": "10.999",
                "rating": 4.4,
                "review_count": None,
                "stock_status": None,
                "source": "fakestore_api",
                "source_url": "https://example.com/1",
                "extraction_date": "2026-04-21",
                "pipeline_run_id": "old-run",
            },
            {
                "product_id": "2",
                "name": "Shoes",
                "category": "fashion",
                "price": 22.0,
                "rating": 3.5,
                "review_count": 4,
                "stock_status": "in_stock",
                "source": "fakestore_api",
                "source_url": "https://example.com/2",
                "extraction_date": "2026-04-21",
                "pipeline_run_id": "old-run",
            },
        ]
    )

    clean_df, rejected_df, quality_score = transformer.transform(raw_df, run_id)
    result = loader.run_load(
        clean_df,
        rejected_df,
        run_id=run_id,
        dag_id="integration_happy_path",
        execution_date="2026-04-21",
        rows_extracted=len(raw_df),
        rows_transformed=len(clean_df),
        quality_score=quality_score,
    )

    with pg_engine.connect() as connection:
        product_count = connection.execute(
            sa.text("SELECT COUNT(*) FROM products_market")
        ).scalar_one()
        rejected_count = connection.execute(
            sa.text("SELECT COUNT(*) FROM rejected_records")
        ).scalar_one()
        run_status = connection.execute(
            sa.text("SELECT status FROM pipeline_runs WHERE run_id = :run_id"),
            {"run_id": run_id},
        ).scalar_one()

    assert result["status"] == "SUCCESS"
    assert product_count == 2
    assert rejected_count == 0
    assert run_status == "SUCCESS"


def test_same_execution_date_rerun_does_not_duplicate_products(
    pg_engine: sa.Engine, cfg: PipelineConfig
) -> None:
    transformer = DataTransformer(cfg)
    loader = DataLoader(pg_engine, cfg)
    raw_df = pd.DataFrame(
        [
            {
                "product_id": "1",
                "name": "Backpack",
                "category": "bags",
                "price": 12.0,
                "rating": 4.1,
                "review_count": 2,
                "stock_status": "in_stock",
                "source": "fakestore_api",
                "source_url": "https://example.com/1",
                "extraction_date": "2026-04-21",
                "pipeline_run_id": "old-run",
            }
        ]
    )

    first_run_id = str(uuid.uuid4())
    second_run_id = str(uuid.uuid4())
    clean_df, rejected_df, quality_score = transformer.transform(raw_df, first_run_id)

    loader.run_load(
        clean_df,
        rejected_df,
        run_id=first_run_id,
        dag_id="integration_idempotent_a",
        execution_date="2026-04-21",
        rows_extracted=len(raw_df),
        rows_transformed=len(clean_df),
        quality_score=quality_score,
    )
    loader.run_load(
        clean_df,
        rejected_df,
        run_id=second_run_id,
        dag_id="integration_idempotent_b",
        execution_date="2026-04-21",
        rows_extracted=len(raw_df),
        rows_transformed=len(clean_df),
        quality_score=quality_score,
    )

    with pg_engine.connect() as connection:
        product_count = connection.execute(
            sa.text("SELECT COUNT(*) FROM products_market")
        ).scalar_one()

    assert product_count == 1


def test_new_execution_date_creates_new_historical_snapshot(
    pg_engine: sa.Engine, cfg: PipelineConfig
) -> None:
    transformer = DataTransformer(cfg)
    loader = DataLoader(pg_engine, cfg)
    raw_df = pd.DataFrame(
        [
            {
                "product_id": "1",
                "name": "Backpack",
                "category": "bags",
                "price": 12.0,
                "rating": 4.1,
                "review_count": 2,
                "stock_status": "in_stock",
                "source": "fakestore_api",
                "source_url": "https://example.com/1",
                "extraction_date": "2026-04-21",
                "pipeline_run_id": "old-run",
            }
        ]
    )

    first_run_id = str(uuid.uuid4())
    second_run_id = str(uuid.uuid4())
    first_clean_df, first_rejected_df, first_quality_score = transformer.transform(
        raw_df, first_run_id
    )

    next_day_raw_df = raw_df.copy()
    next_day_raw_df["extraction_date"] = "2026-04-22"
    second_clean_df, second_rejected_df, second_quality_score = transformer.transform(
        next_day_raw_df, second_run_id
    )

    loader.run_load(
        first_clean_df,
        first_rejected_df,
        run_id=first_run_id,
        dag_id="integration_snapshot_day_1",
        execution_date="2026-04-21",
        rows_extracted=len(raw_df),
        rows_transformed=len(first_clean_df),
        quality_score=first_quality_score,
    )
    loader.run_load(
        second_clean_df,
        second_rejected_df,
        run_id=second_run_id,
        dag_id="integration_snapshot_day_2",
        execution_date="2026-04-22",
        rows_extracted=len(next_day_raw_df),
        rows_transformed=len(second_clean_df),
        quality_score=second_quality_score,
    )

    with pg_engine.connect() as connection:
        rows = connection.execute(
            sa.text(
                "SELECT extraction_date FROM products_market "
                "WHERE product_id = '1' ORDER BY extraction_date"
            )
        ).scalars().all()

    assert [row.isoformat() for row in rows] == ["2026-04-21", "2026-04-22"]


def test_partial_quality_load_persists_clean_rows_and_rejections(
    pg_engine: sa.Engine, cfg: PipelineConfig
) -> None:
    run_id = str(uuid.uuid4())
    transformer = DataTransformer(cfg)
    loader = DataLoader(pg_engine, cfg)
    raw_df = pd.DataFrame(
        [
            {
                "product_id": "1",
                "name": "Valid Product",
                "category": "electronics",
                "price": 50.0,
                "rating": 4.0,
                "review_count": 7,
                "stock_status": "in_stock",
                "source": "fakestore_api",
                "source_url": "https://example.com/1",
                "extraction_date": "2026-04-21",
                "pipeline_run_id": "old-run",
            },
            {
                "product_id": None,
                "name": "Broken Product",
                "category": "electronics",
                "price": 25.0,
                "rating": 2.0,
                "review_count": 1,
                "stock_status": "in_stock",
                "source": "fakestore_api",
                "source_url": "https://example.com/2",
                "extraction_date": "2026-04-21",
                "pipeline_run_id": "old-run",
            },
        ]
    )

    clean_df, rejected_df, quality_score = transformer.transform(raw_df, run_id)
    result = loader.run_load(
        clean_df,
        rejected_df,
        run_id=run_id,
        dag_id="integration_partial",
        execution_date="2026-04-21",
        rows_extracted=len(raw_df),
        rows_transformed=len(clean_df),
        quality_score=quality_score,
    )

    with pg_engine.connect() as connection:
        product_count = connection.execute(
            sa.text("SELECT COUNT(*) FROM products_market")
        ).scalar_one()
        rejected_count = connection.execute(
            sa.text("SELECT COUNT(*) FROM rejected_records")
        ).scalar_one()
        run_row = connection.execute(
            sa.text(
                "SELECT status, rows_rejected, rows_loaded FROM pipeline_runs WHERE run_id = :run_id"
            ),
            {"run_id": run_id},
        ).mappings().one()

    assert result["status"] == "PARTIAL"
    assert product_count == 1
    assert rejected_count == 1
    assert run_row["status"] == "PARTIAL"
    assert run_row["rows_rejected"] == 1
    assert run_row["rows_loaded"] == 1


def test_db_failure_rolls_back_partial_load_and_marks_run_failed(
    pg_engine: sa.Engine, cfg: PipelineConfig
) -> None:
    class FailingLoader(DataLoader):
        def load_rejected(self, df, run_id, connection=None) -> int:  # type: ignore[override]
            raise RuntimeError("simulated database failure")

    run_id = str(uuid.uuid4())
    transformer = DataTransformer(cfg)
    loader = FailingLoader(pg_engine, cfg)
    raw_df = pd.DataFrame(
        [
            {
                "product_id": "1",
                "name": "Backpack",
                "category": "bags",
                "price": 12.0,
                "rating": 4.1,
                "review_count": 2,
                "stock_status": "in_stock",
                "source": "fakestore_api",
                "source_url": "https://example.com/1",
                "extraction_date": "2026-04-21",
                "pipeline_run_id": "old-run",
            }
        ]
    )

    clean_df, rejected_df, quality_score = transformer.transform(raw_df, run_id)

    with pytest.raises(RuntimeError, match="simulated database failure"):
        loader.run_load(
            clean_df,
            rejected_df,
            run_id=run_id,
            dag_id="integration_failure",
            execution_date="2026-04-21",
            rows_extracted=len(raw_df),
            rows_transformed=len(clean_df),
            quality_score=quality_score,
        )

    with pg_engine.connect() as connection:
        product_count = connection.execute(
            sa.text("SELECT COUNT(*) FROM products_market")
        ).scalar_one()
        run_row = connection.execute(
            sa.text(
                "SELECT status, rows_loaded, error_message FROM pipeline_runs WHERE run_id = :run_id"
            ),
            {"run_id": run_id},
        ).mappings().one()

    assert product_count == 0
    assert run_row["status"] == "FAILED"
    assert run_row["rows_loaded"] == 0
    assert "simulated database failure" in run_row["error_message"]
