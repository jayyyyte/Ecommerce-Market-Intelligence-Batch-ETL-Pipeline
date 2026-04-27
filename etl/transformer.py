"""
Transformation layer for the Batch ETL pipeline.

This module converts raw extracted rows into a clean, schema-valid DataFrame
ready for loading into PostgreSQL. It also records rejected rows with a reason
code and computes the per-run data quality score.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pandera as pa
import yaml
from pandera.errors import SchemaError, SchemaErrors

from etl.config import PipelineConfig, get_config

logger = logging.getLogger(__name__)


class DataTransformer:
    """Clean, validate, and score extracted product data."""

    def __init__(self, cfg: PipelineConfig | None = None) -> None:
        self.cfg = cfg or get_config()
        self.schema_path = (
            Path(__file__).resolve().parent.parent / "config" / "schema_spec.yaml"
        )
        self.schema_spec = self._load_schema_spec()
        self.expected_columns = list(self.schema_spec["transformed_columns"].keys())
        self._rejected_rows: list[dict[str, Any]] = []
        self._raw_rows_by_index: dict[int, pd.Series] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def transform(
        self,
        df: pd.DataFrame,
        run_id: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame, float]:
        """
        Run the full cleaning pipeline.

        Parameters
        ----------
        df : pd.DataFrame
            Raw extracted DataFrame (may contain extra columns like
            'description' or 'extraction_timestamp' from the extractor).
        run_id : str
            UUID of the current pipeline run. Overwrites any run_id value
            already present in the data to guarantee traceability.

        Returns
        -------
        tuple[clean_df, rejected_df, quality_score]
            clean_df       - rows that passed all checks; ready for DB load
            rejected_df    - rows dropped at any step with a reason code
            quality_score  - clean_count / original_count * 100 (0.0 if empty)
        """
        self._rejected_rows = []
        source_df = df.reset_index(drop=True)
        self._raw_rows_by_index = {
            int(index): row.copy() for index, row in source_df.iterrows()
        }
        original_row_count = len(source_df)

        # Step 0: prune extra columns; raises ValueError on schema drift
        working_df = self._prune_columns(source_df)

        if not working_df.empty:
            self._raise_if_required_columns_missing(working_df)
            # Steps 1-2: critical field validation (rejection happens here)
            working_df = self.drop_invalid_rows(working_df, run_id)
            working_df = self.normalize_price(working_df, run_id)
        else:
            working_df = self._ensure_expected_columns(working_df)

        # Steps 3-6: non-rejecting cleaning (always run, even on empty df)
        working_df = self.normalize_category(working_df)
        working_df = self.validate_rating(working_df)
        working_df = self.fill_defaults(working_df)
        working_df = self.add_run_metadata(working_df, run_id)

        # Normalize Python/numpy types before Pandera validation
        working_df = self._standardize_types(working_df)

        # Step 7: two-pass Pandera schema validation
        self._run_structural_check(working_df)         # halt on drift
        clean_df = self._reject_schema_violations(working_df, run_id)  # row rejection

        rejected_df = self._build_rejected_df()
        quality_score = self._calculate_quality_score(len(clean_df), original_row_count)

        if original_row_count > 0 and quality_score < self.cfg.min_quality_score:
            logger.warning(
                "Data quality score %.2f%% is below threshold %.2f%%",
                quality_score,
                self.cfg.min_quality_score,
            )

        return clean_df.reset_index(drop=True), rejected_df, quality_score

    # ------------------------------------------------------------------
    # Cleaning steps
    # ------------------------------------------------------------------

    def drop_invalid_rows(self, df: pd.DataFrame, run_id: str) -> pd.DataFrame:
        """Step 1 -- Reject rows where product_id or price is null/empty."""
        working_df = df.copy()

        # product_id
        product_id_series = (
            working_df["product_id"].astype("string").str.strip().replace({"<NA>": pd.NA})
        )
        missing_product_id = product_id_series.isna() | (product_id_series == "")
        self._reject(working_df.loc[missing_product_id], run_id, "null_product_id")
        working_df = working_df.loc[~missing_product_id].copy()

        # price
        price_text = working_df["price"].astype("string").str.strip().replace({"<NA>": pd.NA})
        missing_price = price_text.isna() | (price_text == "")
        self._reject(working_df.loc[missing_price], run_id, "null_price")

        return working_df.loc[~missing_price].copy()

    def normalize_price(self, df: pd.DataFrame, run_id: str) -> pd.DataFrame:
        """Step 2 -- Cast price to float, round to 2dp, and reject price <= 0."""
        working_df = df.copy()
        price_numeric = pd.to_numeric(working_df["price"], errors="coerce")
        invalid_price = price_numeric.isna() | (price_numeric <= 0)

        self._reject(working_df.loc[invalid_price], run_id, "invalid_price")
        working_df = working_df.loc[~invalid_price].copy()
        working_df["price"] = pd.to_numeric(working_df["price"], errors="coerce").round(2)

        return working_df

    def normalize_category(self, df: pd.DataFrame) -> pd.DataFrame:
        """Step 3 -- Normalize category values to lowercase underscore slugs.

        Null/NaN categories are preserved as None (nullable per schema_spec).
        Examples:
            "men's clothing"        -> "mens_clothing"
            "Electronics & Gadgets" -> "electronics_gadgets"
            None                    -> None
        """
        working_df = df.copy()

        def _slugify(value: Any) -> Any:
            if pd.isna(value):
                return None
            text = str(value).strip().lower()
            if not text:
                return None
            text = text.replace("'", "")                 # drop apostrophes before slug
            text = re.sub(r"[^a-z0-9]+", "_", text)     # non-slug chars -> _
            text = re.sub(r"_+", "_", text).strip("_")  # collapse/trim underscores
            return text or None

        working_df["category"] = working_df["category"].apply(_slugify)
        return working_df

    def validate_rating(self, df: pd.DataFrame) -> pd.DataFrame:
        """Step 4 -- Clip ratings to [0.0, 5.0] and log rows that were clipped.

        Out-of-range ratings are clipped, not rejected. The corrected value is
        kept so the row is not lost. A WARNING is emitted for observability.
        """
        working_df = df.copy()
        ratings = pd.to_numeric(working_df["rating"], errors="coerce")
        clipped_mask = ratings.notna() & ((ratings < 0.0) | (ratings > 5.0))

        if clipped_mask.any():
            logger.warning(
                "Clipped %d out-of-range rating value(s) to [0.0, 5.0]",
                int(clipped_mask.sum()),
            )

        working_df["rating"] = ratings.clip(lower=0.0, upper=5.0)
        return working_df

    def fill_defaults(self, df: pd.DataFrame) -> pd.DataFrame:
        """Step 5 -- Fill defaults for optional fields used by downstream loading."""
        working_df = df.copy()
        working_df["review_count"] = (
            pd.to_numeric(working_df["review_count"], errors="coerce")
            .fillna(0)
            .astype("int64")
        )
        working_df["stock_status"] = (
            working_df["stock_status"]
            .replace("", pd.NA)
            .fillna("unknown")
        )
        return working_df

    def add_run_metadata(self, df: pd.DataFrame, run_id: str) -> pd.DataFrame:
        """Step 6 -- Overwrite pipeline_run_id to guarantee traceability.

        The extractor sets pipeline_run_id at extraction time. The transformer
        overwrites it so backfill re-runs are traced to the re-run's run_id,
        not the original extraction run_id.
        """
        working_df = df.copy()
        working_df["pipeline_run_id"] = run_id
        return working_df

    # ------------------------------------------------------------------
    # Schema loading
    # ------------------------------------------------------------------

    def _load_schema_spec(self) -> dict[str, Any]:
        with self.schema_path.open("r", encoding="utf-8") as handle:
            return yaml.safe_load(handle)

    # ------------------------------------------------------------------
    # Column management
    # ------------------------------------------------------------------

    def _prune_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop non-schema columns (e.g. 'description', 'extraction_timestamp').

        Returns a DataFrame containing only columns declared in
        transformed_columns. Missing required columns are detected downstream
        by _raise_if_required_columns_missing(), not here.
        """
        if df.empty and len(df.columns) == 0:
            return pd.DataFrame(columns=self.expected_columns)

        present_columns = [col for col in df.columns if col in self.expected_columns]
        pruned_df = df.loc[:, present_columns].copy()

        if df.empty:
            return self._ensure_expected_columns(pruned_df)

        return pruned_df

    def _ensure_expected_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add any missing expected columns as empty object-dtype Series."""
        working_df = df.copy()
        for column in self.expected_columns:
            if column not in working_df.columns:
                working_df[column] = pd.Series(dtype="object")
        return working_df.loc[:, self.expected_columns]

    def _raise_if_required_columns_missing(self, df: pd.DataFrame) -> None:
        """Halt the pipeline if any expected column is absent (schema drift)."""
        missing_columns = [col for col in self.expected_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(
                "Schema drift detected -- expected columns are missing from the input: "
                + ", ".join(missing_columns)
            )

    # ------------------------------------------------------------------
    # Type normalization
    # ------------------------------------------------------------------

    def _standardize_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize column types so Pandera can validate deterministically."""
        working_df = self._ensure_expected_columns(df)

        for col in ("product_id", "name", "source", "source_url", "stock_status", "pipeline_run_id"):
            working_df[col] = working_df[col].apply(self._clean_string)

        working_df["category"] = working_df["category"].apply(self._clean_string)
        working_df["price"] = pd.to_numeric(working_df["price"], errors="coerce")
        working_df["rating"] = pd.to_numeric(working_df["rating"], errors="coerce")
        working_df["review_count"] = (
            pd.to_numeric(working_df["review_count"], errors="coerce")
            .fillna(0)
            .astype("int64")
        )
        working_df["extraction_date"] = pd.to_datetime(
            working_df["extraction_date"], errors="coerce"
        ).dt.normalize()

        return working_df.loc[:, self.expected_columns]

    # ------------------------------------------------------------------
    # Pandera schema building
    # ------------------------------------------------------------------

    def _build_schema(self, include_checks: bool) -> pa.DataFrameSchema:
        """Build a Pandera DataFrameSchema from schema_spec.yaml.

        Parameters
        ----------
        include_checks : bool
            False -> structural schema only (column presence + dtype coercion).
                     Used by _run_structural_check() to detect schema drift.
            True  -> full schema including value checks (min/max/pattern/isin).
                     Used by _reject_schema_violations() for row-level checks.
        """
        columns: dict[str, pa.Column] = {}
        transformed_columns = self.schema_spec["transformed_columns"]

        for name, spec in transformed_columns.items():
            checks = self._build_checks(spec) if include_checks else None
            nullable = bool(spec.get("nullable", True)) if include_checks else True
            columns[name] = pa.Column(
                self._map_pandera_dtype(spec["type"]),
                required=True,
                nullable=nullable,
                checks=checks,
                coerce=True,
            )

        return pa.DataFrameSchema(columns=columns, strict=True, ordered=False)

    def _build_checks(self, spec: dict[str, Any]) -> list[pa.Check]:
        checks: list[pa.Check] = []

        if "min" in spec:
            checks.append(pa.Check.ge(spec["min"]))
        if "max" in spec:
            checks.append(pa.Check.le(spec["max"]))
        if "max_length" in spec:
            checks.append(pa.Check.str_length(max_value=spec["max_length"]))
        if "pattern" in spec:
            checks.append(pa.Check.str_matches(spec["pattern"]))
        if "allowed_values" in spec:
            checks.append(pa.Check.isin(spec["allowed_values"]))
        # Note: 'precision' from schema_spec is enforced implicitly by
        # normalize_price (round to 2dp), not as a separate Pandera check.

        return checks

    def _map_pandera_dtype(self, dtype_name: str) -> Any:
        dtype_map = {
            "string":  pa.String,
            "float":   pa.Float,
            "integer": pa.Int,
            "date":    pa.DateTime,
        }
        try:
            return dtype_map[dtype_name]
        except KeyError as exc:
            raise ValueError(
                f"Unsupported schema type in schema_spec.yaml: {dtype_name!r}"
            ) from exc

    # ------------------------------------------------------------------
    # Two-pass Pandera validation
    # ------------------------------------------------------------------

    def _run_structural_check(self, df: pd.DataFrame) -> None:
        """Pass 1 -- Halt on schema drift: missing columns or incompatible dtypes.

        This is a pipeline-halting event. It fires when the source API has
        added or removed columns, signaling that schema_spec.yaml needs updating.
        Row-level value violations are NOT caught here (include_checks=False).
        """
        structural_schema = self._build_schema(include_checks=False)
        structural_schema.validate(df, lazy=True)

    def _reject_schema_violations(self, df: pd.DataFrame, run_id: str) -> pd.DataFrame:
        """Pass 2 -- Row-level value checks; invalid rows go to rejected_df.

        Iterates row-by-row so valid rows can still be loaded when some rows
        fail value constraints (e.g. stock_status not in the allowed list).

        Performance note: acceptable for <=10K rows/run. For larger volumes,
        use Pandera's lazy frame-level validation and parse SchemaErrors to
        get the failing row indices instead of iterating row-by-row.
        """
        if df.empty:
            return df.loc[:, self.expected_columns].copy()

        full_schema = self._build_schema(include_checks=True)
        valid_indices: list[int] = []
        invalid_indices: list[int] = []

        for index, row in df.iterrows():
            row_df = pd.DataFrame([row.to_dict()])
            try:
                full_schema.validate(row_df, lazy=True)
            except (SchemaError, SchemaErrors):
                invalid_indices.append(index)
            else:
                valid_indices.append(index)

        if invalid_indices:
            self._reject(df.loc[invalid_indices], run_id, "schema_violation")

        return df.loc[valid_indices, self.expected_columns].copy()

    # ------------------------------------------------------------------
    # Rejection tracking
    # ------------------------------------------------------------------

    def _reject(self, rows: pd.DataFrame, run_id: str, reason: str) -> None:
        """Accumulate rejected rows in the shape expected by the loader."""
        if rows.empty:
            return

        rejected_at = datetime.now(timezone.utc)
        for index, row in rows.iterrows():
            raw_row = self._raw_rows_by_index.get(int(index), row)
            self._rejected_rows.append(
                {
                    "run_id": run_id,
                    "raw_data": self._serialize_row(raw_row),
                    "rejection_reason": reason,
                    "rejected_at": rejected_at,
                }
            )

    def _build_rejected_df(self) -> pd.DataFrame:
        columns = ["run_id", "raw_data", "rejection_reason", "rejected_at"]
        if not self._rejected_rows:
            return pd.DataFrame(columns=columns)
        return pd.DataFrame(self._rejected_rows, columns=columns)

    # ------------------------------------------------------------------
    # Quality score
    # ------------------------------------------------------------------

    def _calculate_quality_score(self, clean_count: int, original_count: int) -> float:
        """Compute rows_loaded / rows_extracted * 100.

        The denominator is always the original input row count (before any
        rejection step), so the score reflects source data quality rather
        than cleaning effectiveness. Returns 0.0 when the input is empty.
        """
        if original_count == 0:
            return 0.0
        return round((clean_count / original_count) * 100, 2)

    # ------------------------------------------------------------------
    # Serialization helpers
    # ------------------------------------------------------------------

    def _serialize_row(self, row: pd.Series) -> dict[str, Any]:
        return {col: self._serialize_value(val) for col, val in row.items()}

    def _serialize_value(self, value: Any) -> Any:
        """Convert a single cell value to a JSON-safe Python scalar.

        pandas Series.to_dict() already converts numpy.int64 -> int and
        numpy.float64 -> float, so those cases do not need explicit handling.
        The two remaining non-serializable cases are pd.Timestamp and NaN/NA.
        """
        if isinstance(value, dict):
            return {str(key): self._serialize_value(val) for key, val in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self._serialize_value(item) for item in value]
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if isinstance(value, datetime):
            return value.isoformat()
        try:
            is_missing = pd.isna(value)
        except Exception:
            is_missing = False
        if isinstance(is_missing, (bool, np.bool_)) and bool(is_missing):
            return None
        return value

    def _clean_string(self, value: Any) -> Any:
        """Return a stripped string, or None for null/empty inputs."""
        if pd.isna(value):
            return None
        text = str(value).strip()
        return text or None
