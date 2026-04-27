"""
Coverage
--------
Integration
  test_transform_cleans_rows_tracks_rejections_and_overwrites_run_id
    - extra columns (description, extraction_timestamp) are pruned
    - run_id is overwritten on clean rows
    - price string "10.999" -> float 11.0
    - rating 6.2 clipped to 5.0
    - review_count None -> 0
    - stock_status None -> "unknown"
    - clean_df has exactly the expected_columns, nothing more
    - rejected_df contains null_product_id and invalid_price rows
    - quality_score denominator is original row count (33.33)

Step 1 -- drop_invalid_rows
  test_null_product_id_is_rejected
  test_empty_string_product_id_is_rejected
  test_null_price_is_rejected_as_null_price      <- distinct from zero-price
  test_empty_string_price_is_rejected_as_null_price

Step 2 -- normalize_price
  test_invalid_price_string_is_rejected
  test_zero_price_is_rejected_as_invalid_price
  test_negative_price_is_rejected_as_invalid_price

Step 3 -- normalize_category
  test_category_normalization_handles_special_chars_and_nulls
  test_apostrophe_category_normalized_correctly
  test_ampersand_category_normalized_correctly

Step 4 -- validate_rating
  test_rating_above_5_is_clipped_not_rejected
  test_rating_below_0_is_clipped_to_zero

Step 5 -- fill_defaults
  test_fill_defaults_null_review_count
  test_fill_defaults_empty_string_stock_status

Step 7 -- Pandera schema validation
  test_schema_violation_rows_are_rejected
  test_missing_required_column_halts_pipeline

Quality score
  test_quality_score_uses_original_input_row_count
  test_empty_input_returns_zero_score_without_crashing
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest
from pandera.errors import SchemaError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl.config import PipelineConfig
from etl.transformer import DataTransformer

RUN_ID = "11111111-1111-1111-1111-111111111111"



# Fixtures
@pytest.fixture
def transformer() -> DataTransformer:
    return DataTransformer(PipelineConfig())


def _valid_row(**overrides) -> dict:
    """Return a minimal valid raw row. Override any field for targeted tests."""
    base = {
        "product_id": "1",
        "name": "Test Product",
        "category": "electronics",
        "price": 42.0,
        "rating": 4.0,
        "review_count": 5,
        "stock_status": "in_stock",
        "source": "fakestore_api",
        "source_url": "https://example.com/1",
        "extraction_date": "2026-04-21",
        "pipeline_run_id": "old-run-id",
    }
    base.update(overrides)
    return base


def _transform_one(transformer: DataTransformer, **overrides):
    """Run transform on a single row and return (clean_df, rejected_df, score)."""
    return transformer.transform(pd.DataFrame([_valid_row(**overrides)]), RUN_ID)


# Integration test
class TestTransformIntegration:

    def test_transform_cleans_rows_tracks_rejections_and_overwrites_run_id(
        self, transformer: DataTransformer
    ) -> None:
        """End-to-end: three input rows produce 1 clean, 2 rejected, score 33.33."""
        raw_df = pd.DataFrame([
            {
                # Row 1: valid after cleaning
                "product_id": "1",
                "name": "Backpack",
                "category": "men's clothing",
                "price": "10.999",       # string price -> 11.0 after rounding
                "rating": 6.2,           # clipped to 5.0
                "review_count": None,    # -> 0
                "stock_status": None,    # -> "unknown"
                "source": "fakestore_api",
                "source_url": "https://example.com/1",
                "extraction_date": "2026-04-21",
                "pipeline_run_id": "old-run-id",
                "description": "drop me",               # non-schema column
                "extraction_timestamp": "2026-04-21T00:00:00Z",  # non-schema column
            },
            {
                # Row 2: price = 0 -> rejected as invalid_price
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
                # Row 3: product_id = None -> rejected as null_product_id
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
        ])

        clean_df, rejected_df, quality_score = transformer.transform(raw_df, RUN_ID)

        # Quality score denominator is the original 3 rows
        assert quality_score == 33.33
        assert len(clean_df) == 1
        assert len(rejected_df) == 2

        # Clean df has exactly the schema columns — no extras
        assert list(clean_df.columns) == transformer.expected_columns
        assert "description" not in clean_df.columns
        assert "extraction_timestamp" not in clean_df.columns

        row = clean_df.iloc[0]
        assert row["category"] == "mens_clothing"
        assert row["price"] == pytest.approx(11.0)
        assert row["rating"] == pytest.approx(5.0)
        assert row["review_count"] == 0
        assert row["stock_status"] == "unknown"
        assert row["pipeline_run_id"] == RUN_ID        # overwritten, not "old-run-id"
        assert row["extraction_date"] == pd.Timestamp("2026-04-21")

        # Both rejection reasons are present
        assert set(rejected_df["rejection_reason"]) == {"invalid_price", "null_product_id"}

    def test_rejected_raw_data_preserves_original_extra_fields(
        self, transformer: DataTransformer
    ) -> None:
        raw_df = pd.DataFrame([
            _valid_row(
                price=0,
                description="original description",
                extraction_timestamp="2026-04-21T00:00:00Z",
                source_payload={"tags": ["sale", None], "seen_at": pd.Timestamp("2026-04-21")},
            )
        ])

        clean_df, rejected_df, _ = transformer.transform(raw_df, RUN_ID)

        raw_data = rejected_df.iloc[0]["raw_data"]
        assert clean_df.empty
        assert raw_data["description"] == "original description"
        assert raw_data["extraction_timestamp"] == "2026-04-21T00:00:00Z"
        assert raw_data["source_payload"] == {
            "tags": ["sale", None],
            "seen_at": "2026-04-21T00:00:00",
        }

    def test_tiki_scrape_source_is_accepted(self, transformer: DataTransformer) -> None:
        clean_df, rejected_df, quality_score = _transform_one(
            transformer,
            source="tiki_scrape",
            price=100000,
            source_url="https://tiki.vn/example-product",
        )

        assert len(clean_df) == 1
        assert rejected_df.empty
        assert quality_score == 100.0


# ===========================================================================
# Step 1 -- drop_invalid_rows
# ===========================================================================

class TestDropInvalidRows:

    def test_null_product_id_is_rejected(self, transformer: DataTransformer) -> None:
        clean_df, rejected_df, _ = _transform_one(transformer, product_id=None)

        assert clean_df.empty
        assert len(rejected_df) == 1
        assert rejected_df.iloc[0]["rejection_reason"] == "null_product_id"

    def test_empty_string_product_id_is_rejected(self, transformer: DataTransformer) -> None:
        clean_df, rejected_df, _ = _transform_one(transformer, product_id="   ")

        assert clean_df.empty
        assert rejected_df.iloc[0]["rejection_reason"] == "null_product_id"

    def test_null_price_is_rejected_as_null_price(self, transformer: DataTransformer) -> None:
        """None price must produce reason 'null_price', not 'invalid_price'."""
        clean_df, rejected_df, _ = _transform_one(transformer, price=None)

        assert clean_df.empty
        assert rejected_df.iloc[0]["rejection_reason"] == "null_price"

    def test_empty_string_price_is_rejected_as_null_price(
        self, transformer: DataTransformer
    ) -> None:
        clean_df, rejected_df, _ = _transform_one(transformer, price="")

        assert clean_df.empty
        assert rejected_df.iloc[0]["rejection_reason"] == "null_price"


# ===========================================================================
# Step 2 -- normalize_price
# ===========================================================================

class TestNormalizePrice:

    def test_string_price_is_cast_to_float(self, transformer: DataTransformer) -> None:
        clean_df, _, _ = _transform_one(transformer, price="29.99")
        assert clean_df.iloc[0]["price"] == pytest.approx(29.99)

    def test_price_is_rounded_to_two_decimal_places(
        self, transformer: DataTransformer
    ) -> None:
        clean_df, _, _ = _transform_one(transformer, price="10.999")
        assert clean_df.iloc[0]["price"] == pytest.approx(11.0)

    def test_zero_price_is_rejected_as_invalid_price(
        self, transformer: DataTransformer
    ) -> None:
        clean_df, rejected_df, _ = _transform_one(transformer, price=0)
        assert clean_df.empty
        assert rejected_df.iloc[0]["rejection_reason"] == "invalid_price"

    def test_negative_price_is_rejected_as_invalid_price(
        self, transformer: DataTransformer
    ) -> None:
        clean_df, rejected_df, _ = _transform_one(transformer, price=-5.0)
        assert clean_df.empty
        assert rejected_df.iloc[0]["rejection_reason"] == "invalid_price"

    def test_non_numeric_price_string_is_rejected_as_invalid_price(
        self, transformer: DataTransformer
    ) -> None:
        clean_df, rejected_df, _ = _transform_one(transformer, price="N/A")
        assert clean_df.empty
        assert rejected_df.iloc[0]["rejection_reason"] == "invalid_price"


# ===========================================================================
# Step 3 -- normalize_category
# ===========================================================================

class TestNormalizeCategory:

    def test_category_normalization_handles_special_chars_and_nulls(
        self, transformer: DataTransformer
    ) -> None:
        raw_df = pd.DataFrame([
            _valid_row(product_id="1", category="Electronics & Gadgets"),
            _valid_row(product_id="2", category=None),
        ])

        clean_df, rejected_df, quality_score = transformer.transform(raw_df, RUN_ID)

        assert quality_score == 100.0
        assert rejected_df.empty

        # FIX: None category is stored as NaN in the pandas Series after
        # Pandera validation with coerce=True. Use pd.isna() to check null.
        assert clean_df.iloc[0]["category"] == "electronics_gadgets"
        assert pd.isna(clean_df.iloc[1]["category"])

    def test_apostrophe_category_normalized_correctly(
        self, transformer: DataTransformer
    ) -> None:
        """'men's clothing' -> 'mens_clothing' (apostrophe stripped, not replaced)."""
        clean_df, _, _ = _transform_one(transformer, category="men's clothing")
        assert clean_df.iloc[0]["category"] == "mens_clothing"

    def test_ampersand_category_normalized_correctly(
        self, transformer: DataTransformer
    ) -> None:
        clean_df, _, _ = _transform_one(transformer, category="Electronics & Gadgets")
        assert clean_df.iloc[0]["category"] == "electronics_gadgets"

    def test_category_with_only_special_chars_becomes_none(
        self, transformer: DataTransformer
    ) -> None:
        """A category that slugifies to empty string should become None (nullable)."""
        clean_df, _, _ = _transform_one(transformer, category="'")
        assert pd.isna(clean_df.iloc[0]["category"])


# ===========================================================================
# Step 4 -- validate_rating
# ===========================================================================

class TestValidateRating:

    def test_rating_above_5_is_clipped_not_rejected(
        self, transformer: DataTransformer
    ) -> None:
        """Rating 6.2 -> clipped to 5.0. Row is kept, not rejected."""
        clean_df, rejected_df, _ = _transform_one(transformer, rating=6.2)
        assert len(clean_df) == 1
        assert rejected_df.empty
        assert clean_df.iloc[0]["rating"] == pytest.approx(5.0)

    def test_rating_below_0_is_clipped_to_zero(
        self, transformer: DataTransformer
    ) -> None:
        """Negative rating -> clipped to 0.0. Row is kept, not rejected."""
        clean_df, rejected_df, _ = _transform_one(transformer, rating=-1.5)
        assert len(clean_df) == 1
        assert rejected_df.empty
        assert clean_df.iloc[0]["rating"] == pytest.approx(0.0)

    def test_null_rating_is_preserved_as_nan(
        self, transformer: DataTransformer
    ) -> None:
        """Null rating is allowed (nullable=true in schema_spec)."""
        clean_df, rejected_df, _ = _transform_one(transformer, rating=None)
        assert len(clean_df) == 1
        assert rejected_df.empty
        assert pd.isna(clean_df.iloc[0]["rating"])


# ===========================================================================
# Step 5 -- fill_defaults
# ===========================================================================

class TestFillDefaults:

    def test_null_review_count_is_filled_to_zero(
        self, transformer: DataTransformer
    ) -> None:
        clean_df, _, _ = _transform_one(transformer, review_count=None)
        assert clean_df.iloc[0]["review_count"] == 0

    def test_empty_string_stock_status_is_filled_to_unknown(
        self, transformer: DataTransformer
    ) -> None:
        clean_df, _, _ = _transform_one(transformer, stock_status="")
        assert clean_df.iloc[0]["stock_status"] == "unknown"

    def test_null_stock_status_is_filled_to_unknown(
        self, transformer: DataTransformer
    ) -> None:
        clean_df, _, _ = _transform_one(transformer, stock_status=None)
        assert clean_df.iloc[0]["stock_status"] == "unknown"


# ===========================================================================
# Step 7 -- Pandera schema validation
# ===========================================================================

class TestSchemaValidation:

    def test_schema_violation_rows_are_rejected(
        self, transformer: DataTransformer
    ) -> None:
        """stock_status='preorder' is not in the allowed list -> schema_violation."""
        clean_df, rejected_df, quality_score = _transform_one(
            transformer, stock_status="preorder"
        )

        assert clean_df.empty
        assert quality_score == 0.0
        assert rejected_df.iloc[0]["rejection_reason"] == "schema_violation"

    def test_missing_required_column_halts_pipeline(
        self, transformer: DataTransformer
    ) -> None:
        """A DataFrame missing the 'source' column must halt the pipeline."""
        row = _valid_row()
        row.pop("source")
        raw_df = pd.DataFrame([row])

        with pytest.raises((ValueError, SchemaError)):
            transformer.transform(raw_df, RUN_ID)


# ===========================================================================
# Quality score
# ===========================================================================

class TestQualityScore:

    def test_quality_score_uses_original_input_row_count(
        self, transformer: DataTransformer
    ) -> None:
        """Score = clean / original, not clean / (original - step1_rejected)."""
        rows = [
            _valid_row(product_id="1"),
            _valid_row(product_id="2"),
            _valid_row(product_id="3"),
            _valid_row(product_id=""),   # rejected
        ]
        clean_df, rejected_df, quality_score = transformer.transform(
            pd.DataFrame(rows), RUN_ID
        )

        assert len(clean_df) == 3
        assert len(rejected_df) == 1
        assert quality_score == 75.0   # 3/4 * 100, NOT 3/3 * 100

    def test_empty_input_returns_zero_score_without_crashing(
        self, transformer: DataTransformer
    ) -> None:
        clean_df, rejected_df, quality_score = transformer.transform(
            pd.DataFrame(), RUN_ID
        )

        assert clean_df.empty
        assert rejected_df.empty
        assert quality_score == 0.0
        assert list(clean_df.columns) == transformer.expected_columns
