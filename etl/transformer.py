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

    def transform(
        self,
        df: pd.DataFrame,
        run_id: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame, float]:
        """
        Run the full cleaning pipeline.

        Returns:
            tuple[clean_df, rejected_df, quality_score]
        """
        self._rejected_rows = []
        original_row_count = len(df)

        working_df = self._prune_columns(df)

        if not working_df.empty:
            self._raise_if_required_columns_missing(working_df)
            working_df = self.drop_invalid_rows(working_df, run_id)
            working_df = self.normalize_price(working_df, run_id)
        else:
            working_df = self._ensure_expected_columns(working_df)

        working_df = self.normalize_category(working_df)
        working_df = self.validate_rating(working_df)
        working_df = self.fill_defaults(working_df)
        working_df = self.add_run_metadata(working_df, run_id)
        working_df = self._standardize_types(working_df)

        self._run_structural_check(working_df)
        clean_df = self._reject_schema_violations(working_df, run_id)
        rejected_df = self._build_rejected_df()
        quality_score = self._calculate_quality_score(len(clean_df), original_row_count)

        if original_row_count > 0 and quality_score < self.cfg.min_quality_score:
            logger.warning(
                "Data quality score %.2f is below threshold %.2f",
                quality_score,
                self.cfg.min_quality_score,
            )

        return clean_df.reset_index(drop=True), rejected_df, quality_score

    def drop_invalid_rows(self, df: pd.DataFrame, run_id: str) -> pd.DataFrame:
        """Reject rows where product_id or price is null/empty."""
        working_df = df.copy()

        product_id_series = (
            working_df["product_id"].astype("string").str.strip().replace({"<NA>": pd.NA})
        )
        missing_product_id = product_id_series.isna() | (product_id_series == "")
        self._reject(working_df.loc[missing_product_id], run_id, "null_product_id")
        working_df = working_df.loc[~missing_product_id].copy()

        price_text = working_df["price"].astype("string").str.strip().replace({"<NA>": pd.NA})
        missing_price = price_text.isna() | (price_text == "")
        self._reject(working_df.loc[missing_price], run_id, "null_price")

        return working_df.loc[~missing_price].copy()

    def normalize_price(self, df: pd.DataFrame, run_id: str) -> pd.DataFrame:
        """Cast price to float, round to 2dp, and reject price <= 0."""
        working_df = df.copy()
        price_numeric = pd.to_numeric(working_df["price"], errors="coerce")
        invalid_price = price_numeric.isna() | (price_numeric <= 0)

        self._reject(working_df.loc[invalid_price], run_id, "invalid_price")
        working_df = working_df.loc[~invalid_price].copy()
        working_df["price"] = pd.to_numeric(working_df["price"], errors="coerce").round(2)

        return working_df

    def normalize_category(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize category values to lowercase underscore slugs."""
        working_df = df.copy()

        def _slugify(value: Any) -> Any:
            if pd.isna(value):
                return None
            text = str(value).strip().lower()
            if not text:
                return None
            text = text.replace("'", "")
            text = re.sub(r"[^a-z0-9]+", "_", text)
            text = re.sub(r"_+", "_", text).strip("_")
            return text or None

        working_df["category"] = working_df["category"].apply(_slugify)
        return working_df

    def validate_rating(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clip ratings to [0.0, 5.0] and log any clipped rows."""
        working_df = df.copy()
        ratings = pd.to_numeric(working_df["rating"], errors="coerce")
        clipped_mask = ratings.notna() & ((ratings < 0.0) | (ratings > 5.0))

        if clipped_mask.any():
            logger.warning("Clipped %d out-of-range rating values", int(clipped_mask.sum()))

        working_df["rating"] = ratings.clip(lower=0.0, upper=5.0)
        return working_df

    def fill_defaults(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fill defaults for optional fields used by downstream loading."""
        working_df = df.copy()
        review_counts = pd.to_numeric(working_df["review_count"], errors="coerce").fillna(0)
        working_df["review_count"] = review_counts.astype("int64")
        working_df["stock_status"] = (
            working_df["stock_status"].replace("", pd.NA).fillna("unknown")
        )
        return working_df

    def add_run_metadata(self, df: pd.DataFrame, run_id: str) -> pd.DataFrame:
        """Overwrite pipeline_run_id to guarantee traceability for the current run."""
        working_df = df.copy()
        working_df["pipeline_run_id"] = run_id
        return working_df

    def _load_schema_spec(self) -> dict[str, Any]:
        with self.schema_path.open("r", encoding="utf-8") as handle:
            return yaml.safe_load(handle)

    def _prune_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Keep only columns declared in transformed_columns."""
        if df.empty and len(df.columns) == 0:
            return pd.DataFrame(columns=self.expected_columns)

        present_columns = [col for col in df.columns if col in self.expected_columns]
        pruned_df = df.loc[:, present_columns].copy()

        if df.empty:
            return self._ensure_expected_columns(pruned_df)

        return pruned_df

    def _ensure_expected_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        working_df = df.copy()
        for column in self.expected_columns:
            if column not in working_df.columns:
                working_df[column] = pd.Series(dtype="object")
        return working_df.loc[:, self.expected_columns]

    def _raise_if_required_columns_missing(self, df: pd.DataFrame) -> None:
        missing_columns = [col for col in self.expected_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(
                "Schema drift detected: missing transformed columns: "
                + ", ".join(missing_columns)
            )

    def _standardize_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize types so the Pandera schema can validate deterministically."""
        working_df = self._ensure_expected_columns(df)

        for column in ("product_id", "name", "source", "source_url", "stock_status", "pipeline_run_id"):
            working_df[column] = working_df[column].apply(self._clean_string)

        working_df["category"] = working_df["category"].apply(self._clean_string)
        working_df["price"] = pd.to_numeric(working_df["price"], errors="coerce")
        working_df["rating"] = pd.to_numeric(working_df["rating"], errors="coerce")
        working_df["review_count"] = pd.to_numeric(
            working_df["review_count"], errors="coerce"
        ).fillna(0).astype("int64")
        working_df["extraction_date"] = pd.to_datetime(
            working_df["extraction_date"], errors="coerce"
        ).dt.normalize()

        return working_df.loc[:, self.expected_columns]

    def _build_schema(self, include_checks: bool) -> pa.DataFrameSchema:
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

        return checks

    def _map_pandera_dtype(self, dtype_name: str) -> Any:
        dtype_map = {
            "string": pa.String,
            "float": pa.Float,
            "integer": pa.Int,
            "date": pa.DateTime,
        }
        try:
            return dtype_map[dtype_name]
        except KeyError as exc:
            raise ValueError(f"Unsupported schema type: {dtype_name}") from exc

    def _run_structural_check(self, df: pd.DataFrame) -> None:
        """Halt on schema drift such as missing columns or incompatible frame structure."""
        structural_schema = self._build_schema(include_checks=False)
        structural_schema.validate(df, lazy=True)

    def _reject_schema_violations(self, df: pd.DataFrame, run_id: str) -> pd.DataFrame:
        """Reject row-level schema failures while keeping the valid rows."""
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

    def _reject(self, rows: pd.DataFrame, run_id: str, reason: str) -> None:
        """Accumulate rejected rows in the loader-ready output shape."""
        if rows.empty:
            return

        rejected_at = datetime.now(timezone.utc)
        for _, row in rows.iterrows():
            self._rejected_rows.append(
                {
                    "run_id": run_id,
                    "raw_data": self._serialize_row(row),
                    "rejection_reason": reason,
                    "rejected_at": rejected_at,
                }
            )

    def _build_rejected_df(self) -> pd.DataFrame:
        columns = ["run_id", "raw_data", "rejection_reason", "rejected_at"]
        if not self._rejected_rows:
            return pd.DataFrame(columns=columns)
        return pd.DataFrame(self._rejected_rows, columns=columns)

    def _calculate_quality_score(self, clean_count: int, original_count: int) -> float:
        if original_count == 0:
            return 0.0
        return round((clean_count / original_count) * 100, 2)

    def _serialize_row(self, row: pd.Series) -> dict[str, Any]:
        return {column: self._serialize_value(value) for column, value in row.items()}

    def _serialize_value(self, value: Any) -> Any:
        if pd.isna(value):
            return None
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if isinstance(value, datetime):
            return value.isoformat()
        return value

    def _clean_string(self, value: Any) -> Any:
        if pd.isna(value):
            return None
        text = str(value).strip()
        return text or None
