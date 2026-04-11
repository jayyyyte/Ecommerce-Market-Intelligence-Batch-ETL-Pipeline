"""
etl/config.py
-------------
Central configuration module for the E-Commerce ETL pipeline.
Priority order for every setting:
  1. Airflow Variable  (when running inside an Airflow task)
  2. Environment variable  (Docker / shell / .env file)
  3. Hard-coded default  (safe development fallback)

All other ETL modules should import from here — never call
os.environ or airflow.models.Variable directly.

Usage:
    from etl.config import get_config
    cfg = get_config()
    print(cfg.data_source_url)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Internal helper — reads one value with the priority chain above
# ---------------------------------------------------------------------------
def _get(key: str, default: str = "") -> str:
    """
    Try Airflow Variable first, then os.environ, then the default.
    Gracefully degrades when Airflow is not importable (e.g. during local
    unit tests that do not need a running Airflow instance).
    """
    # 1. Airflow Variable (only available inside Airflow runtime)
    try:
        from airflow.models import Variable  # type: ignore
        value = Variable.get(key, default_var=None)
        if value is not None:
            return str(value)
    except Exception:
        # Airflow not installed or not initialised — skip silently
        pass

    # 2. Environment variable
    value = os.environ.get(key)
    if value is not None:
        return value

    # 3. Hard-coded default
    return default


# ---------------------------------------------------------------------------
# Config dataclass — one field per setting
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class PipelineConfig:
    """Immutable snapshot of all pipeline configuration values."""

    # ── Data source ──────────────────────────────────────────────────────────
    data_source_url: str = ""
    """URL of the e-commerce API or first page of the scraping target."""

    # ── Filesystem paths ─────────────────────────────────────────────────────
    staging_dir: str = "/opt/airflow/staging"
    """Directory where raw extracted files are written before transformation."""

    archive_retention_days: int = 90
    """Number of days to keep archived raw files before deletion."""

    # ── PostgreSQL ───────────────────────────────────────────────────────────
    postgres_user: str = "etl_user"
    postgres_password: str = "etl_password"
    postgres_host: str = "postgres"
    postgres_port: str = "5432"
    postgres_db: str = "ecommerce_db"

    # ── Airflow connection id (used in Airflow operators) ─────────────────────
    postgres_conn_id: str = "postgres_ecommerce"

    # ── Retry / resilience ───────────────────────────────────────────────────
    max_retries: int = 3
    """Number of HTTP retry attempts for the extractor."""

    request_timeout: int = 30
    """HTTP request timeout in seconds."""

    rate_limit_delay: float = 1.5
    """Seconds to wait between page requests (scraping)."""

    # ── Alerting ─────────────────────────────────────────────────────────────
    slack_webhook_url: str = ""
    """Incoming Webhook URL for Slack notifications. Empty disables Slack."""

    # ── Environment ──────────────────────────────────────────────────────────
    pipeline_env: str = "dev"
    """Runtime environment: dev | staging | prod."""

    # ── Data quality ─────────────────────────────────────────────────────────
    min_quality_score: float = 70.0
    """Minimum acceptable data quality score (%). Below this triggers a warning."""

    # ── Derived property (not a field) ───────────────────────────────────────
    @property
    def db_url(self) -> str:
        """Full SQLAlchemy connection URL for the ecommerce_db."""
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def is_production(self) -> bool:
        return self.pipeline_env == "prod"


# ---------------------------------------------------------------------------
# Factory — the single public entry point
# ---------------------------------------------------------------------------
def get_config() -> PipelineConfig:
    """
    Build and return a PipelineConfig populated from Airflow Variables /
    environment variables. Call this once at the start of each ETL task.

    Example::

        from etl.config import get_config

        def my_airflow_task(**context):
            cfg = get_config()
            print(cfg.data_source_url)
    """
    return PipelineConfig(
        # Data source
        data_source_url=_get("DATA_SOURCE_URL", "https://fakestoreapi.com/products"),

        # Filesystem
        staging_dir=_get("STAGING_DIR", "/opt/airflow/staging"),
        archive_retention_days=int(_get("ARCHIVE_RETENTION_DAYS", "90")),

        # PostgreSQL
        postgres_user=_get("POSTGRES_USER", "etl_user"),
        postgres_password=_get("POSTGRES_PASSWORD", "etl_password"),
        postgres_host=_get("POSTGRES_HOST", "postgres"),
        postgres_port=_get("POSTGRES_PORT", "5432"),
        postgres_db=_get("POSTGRES_DB", "ecommerce_db"),
        postgres_conn_id=_get("POSTGRES_CONN_ID", "postgres_ecommerce"),

        # Retry
        max_retries=int(_get("MAX_RETRIES", "3")),
        request_timeout=int(_get("REQUEST_TIMEOUT", "30")),
        rate_limit_delay=float(_get("RATE_LIMIT_DELAY", "1.5")),

        # Alerting
        slack_webhook_url=_get("SLACK_WEBHOOK_URL", ""),

        # Environment
        pipeline_env=_get("PIPELINE_ENV", "dev"),

        # Data quality
        min_quality_score=float(_get("MIN_QUALITY_SCORE", "70.0")),
    )