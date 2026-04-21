from __future__ import annotations

import pandas as pd
import pytest
from pandera.errors import SchemaError
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl.config import PipelineConfig
from etl.transformer import DataTransformer


RUN_ID = "11111111-1111-1111-1111-111111111111"


@pytest.fixture
def transformer() -> DataTransformer:
    return DataTransformer(PipelineConfig())


def test_transform_cleans_rows_tracks_rejections_and_overwrites_run_id(
    transformer: DataTransformer,
) -> None:
    raw_df = pd.DataFrame(
        [
            {
                "product_id": "1",
                "name": "Backpack",
                "category": "men's clothing",
                "price": "10.999",
                "rating": 6.2,
                "review_count": None,
                "stock_status": None,
                "source": "fakestore_api",
                "source_url": "https://example.com/1",
                "extraction_date": "2026-04-21",
                "pipeline_run_id": "old-run-id",
                "description": "drop me",
                "extraction_timestamp": "2026-04-21T00:00:00Z",
            },
            {
                "product_id": "2",
                "name": "Invalid Price",
                "category": "electronics",
                "price": 0,
                "rating": 4.5,
                "review_count": 3,
                "stock_status": "in_stock",
                "source": "fakestore_api",
                "source_url": "https://example.com/2",
                "extraction_date": "2026-04-21",
                "pipeline_run_id": "old-run-id",
            },
            {
                "product_id": None,
                "name": "Missing Product ID",
                "category": "jewelery",
                "price": 25.0,
                "rating": 3.0,
                "review_count": 1,
                "stock_status": "in_stock",
                "source": "fakestore_api",
                "source_url": "https://example.com/3",
                "extraction_date": "2026-04-21",
                "pipeline_run_id": "old-run-id",
            },
        ]
    )

    clean_df, rejected_df, quality_score = transformer.transform(raw_df, RUN_ID)

    assert quality_score == 33.33
    assert len(clean_df) == 1
    assert len(rejected_df) == 2
    assert list(clean_df.columns) == transformer.expected_columns

    row = clean_df.iloc[0]
    assert row["category"] == "mens_clothing"
    assert row["price"] == pytest.approx(11.0)
    assert row["rating"] == pytest.approx(5.0)
    assert row["review_count"] == 0
    assert row["stock_status"] == "unknown"
    assert row["pipeline_run_id"] == RUN_ID
    assert pd.Timestamp("2026-04-21") == row["extraction_date"]
    assert "description" not in clean_df.columns
    assert "extraction_timestamp" not in clean_df.columns

    assert set(rejected_df["rejection_reason"]) == {"invalid_price", "null_product_id"}


def test_category_normalization_handles_special_chars_and_nulls(
    transformer: DataTransformer,
) -> None:
    raw_df = pd.DataFrame(
        [
            {
                "product_id": "1",
                "name": "Phone",
                "category": "Electronics & Gadgets",
                "price": 100.0,
                "rating": 4.8,
                "review_count": 10,
                "stock_status": "in_stock",
                "source": "fakestore_api",
                "source_url": "https://example.com/1",
                "extraction_date": "2026-04-21",
                "pipeline_run_id": "old-run-id",
            },
            {
                "product_id": "2",
                "name": "Mystery Box",
                "category": None,
                "price": 50.0,
                "rating": 4.2,
                "review_count": 5,
                "stock_status": "in_stock",
                "source": "fakestore_api",
                "source_url": "https://example.com/2",
                "extraction_date": "2026-04-21",
                "pipeline_run_id": "old-run-id",
            },
        ]
    )

    clean_df, rejected_df, quality_score = transformer.transform(raw_df, RUN_ID)

    assert quality_score == 100.0
    assert rejected_df.empty
    assert clean_df["category"].tolist() == ["electronics_gadgets", None]


def test_schema_violation_rows_are_rejected(transformer: DataTransformer) -> None:
    raw_df = pd.DataFrame(
        [
            {
                "product_id": "1",
                "name": "Unsupported Stock",
                "category": "electronics",
                "price": 42.0,
                "rating": 4.0,
                "review_count": 7,
                "stock_status": "preorder",
                "source": "fakestore_api",
                "source_url": "https://example.com/1",
                "extraction_date": "2026-04-21",
                "pipeline_run_id": "old-run-id",
            }
        ]
    )

    clean_df, rejected_df, quality_score = transformer.transform(raw_df, RUN_ID)

    assert clean_df.empty
    assert quality_score == 0.0
    assert rejected_df["rejection_reason"].tolist() == ["schema_violation"]


def test_quality_score_uses_original_input_row_count(transformer: DataTransformer) -> None:
    raw_df = pd.DataFrame(
        [
            {
                "product_id": "1",
                "name": "Valid A",
                "category": "electronics",
                "price": 10.0,
                "rating": 4.0,
                "review_count": 1,
                "stock_status": "in_stock",
                "source": "fakestore_api",
                "source_url": "https://example.com/1",
                "extraction_date": "2026-04-21",
                "pipeline_run_id": "old-run-id",
            },
            {
                "product_id": "2",
                "name": "Valid B",
                "category": "electronics",
                "price": 20.0,
                "rating": 4.0,
                "review_count": 1,
                "stock_status": "in_stock",
                "source": "fakestore_api",
                "source_url": "https://example.com/2",
                "extraction_date": "2026-04-21",
                "pipeline_run_id": "old-run-id",
            },
            {
                "product_id": "3",
                "name": "Valid C",
                "category": "electronics",
                "price": 30.0,
                "rating": 4.0,
                "review_count": 1,
                "stock_status": "in_stock",
                "source": "fakestore_api",
                "source_url": "https://example.com/3",
                "extraction_date": "2026-04-21",
                "pipeline_run_id": "old-run-id",
            },
            {
                "product_id": "",
                "name": "Invalid",
                "category": "electronics",
                "price": 40.0,
                "rating": 4.0,
                "review_count": 1,
                "stock_status": "in_stock",
                "source": "fakestore_api",
                "source_url": "https://example.com/4",
                "extraction_date": "2026-04-21",
                "pipeline_run_id": "old-run-id",
            },
        ]
    )

    clean_df, rejected_df, quality_score = transformer.transform(raw_df, RUN_ID)

    assert len(clean_df) == 3
    assert len(rejected_df) == 1
    assert quality_score == 75.0


def test_empty_input_returns_zero_score_without_crashing(
    transformer: DataTransformer,
) -> None:
    clean_df, rejected_df, quality_score = transformer.transform(pd.DataFrame(), RUN_ID)

    assert clean_df.empty
    assert rejected_df.empty
    assert quality_score == 0.0
    assert list(clean_df.columns) == transformer.expected_columns


def test_missing_required_column_halts_pipeline(transformer: DataTransformer) -> None:
    raw_df = pd.DataFrame(
        [
            {
                "product_id": "1",
                "name": "Broken Row",
                "category": "electronics",
                "price": 10.0,
                "rating": 4.0,
                "review_count": 1,
                "stock_status": "in_stock",
                "source_url": "https://example.com/1",
                "extraction_date": "2026-04-21",
                "pipeline_run_id": "old-run-id",
            }
        ]
    )

    with pytest.raises((ValueError, SchemaError)):
        transformer.transform(raw_df, RUN_ID)
