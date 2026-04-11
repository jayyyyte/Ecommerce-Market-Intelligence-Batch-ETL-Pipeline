"""
Data extraction layer for the Batch ETL pipeline.

Classes
-------
BaseExtractor   – Abstract base defining the extractor contract.
FakeStoreExtractor – Concrete implementation for https://fakestoreapi.com

FakeStoreAPI response shape (per manual inspection):
[
  {
    "id": 1,
    "title": "Fjallraven - Foldsack No. 1 Backpack",
    "price": 109.95,
    "description": "Your...",
    "category": "men's clothing",
    "image": "https://fakestoreapi.com/img/...",
    "rating": {
      "rate": 3.9,
      "count": 120
    }
  },
  ...
]

Pagination: ?limit=N  (max ~20 products total — single page sufficient)
No auth required. Rate-limit: none enforced, but we add delays for good practice.
"""

import json
import logging
import os
import time
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from utils.retry import (
    RateLimitError,
    ServerError,
    raise_for_status_with_context,
    retry_with_backoff,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
FAKESTORE_BASE_URL = "https://fakestoreapi.com"
FAKESTORE_PRODUCTS_ENDPOINT = "/products"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; EcommerceETL/1.0; "
    "+https://github.com/your-org/ecommerce-etl)"
)
REQUEST_TIMEOUT = 30          # seconds
INTER_REQUEST_DELAY = 1.0     # seconds between paginated requests (polite crawling)


# ---------------------------------------------------------------------------
# Base class
class BaseExtractor(ABC):
    """
    Abstract base class for all data-source extractors.

    Subclasses must implement:
        extract(execution_date, run_id) -> list[dict]
        validate_response(raw_response)  -> bool
        save_raw(data, execution_date)   -> Path
    """

    def __init__(self, staging_dir: str | Path = "staging") -> None:
        self.staging_dir = Path(staging_dir)
        self.source_name: str = "unknown"   # override in subclass

    @abstractmethod
    def extract(self, execution_date: str, run_id: str) -> list[dict]:
        """
        Fetch all records from the data source for a given execution date.

        Parameters
        ----------
        execution_date : str
            ISO date string (YYYY-MM-DD), e.g. '2025-04-11'.
            Used for partitioning staging files and as extraction_date value.
        run_id : str
            UUID string identifying this pipeline run.

        Returns
        -------
        list[dict]
            Flat list of raw product records, with extraction_timestamp and
            source fields appended.
        """

    @abstractmethod
    def validate_response(self, raw_response: Any) -> bool:
        """
        Validate that an HTTP response body has the expected structure.

        Returns True if valid, False (or raises) otherwise.
        """

    @abstractmethod
    def save_raw(self, data: list[dict], execution_date: str) -> Path:
        """
        Atomically persist raw records to the staging area.

        Uses a write-to-.tmp-then-rename strategy to avoid partial files.

        Returns
        -------
        Path
            Absolute path to the saved file.
        """

    # ------------------------------------------------------------------
    # Shared helpers available to all subclasses
    def _make_staging_path(self, execution_date: str, filename: str) -> Path:
        """Build and create the staging directory for a given date."""
        directory = self.staging_dir / execution_date
        directory.mkdir(parents=True, exist_ok=True)
        return directory / filename

    def _atomic_write(self, path: Path, data: list[dict]) -> None:
        """Write JSON to a .tmp file then atomically rename to final path."""
        tmp_path = path.with_suffix(".tmp")
        try:
            with tmp_path.open("w", encoding="utf-8") as fh:
                json.dump(data, fh, indent=2, ensure_ascii=False)
            tmp_path.rename(path)
            logger.info("Saved %d records → %s", len(data), path)
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
            raise


# ---------------------------------------------------------------------------
# FakeStoreAPI extractor
class FakeStoreExtractor(BaseExtractor):
    """
    Extractor for https://fakestoreapi.com — used for development / CI.

    FakeStoreAPI has a fixed catalogue of ~20 products.
    We fetch all products in a single request (limit=20) and handle the
    'pagination loop' by iterating with a configurable limit until the
    response returns an empty list or we reach max_pages.

    Retry behaviour is supplied by @retry_with_backoff (30 s, 60 s, 120 s).
    """

    SOURCE_NAME = "fakestore_api"
    PRODUCTS_URL = f"{FAKESTORE_BASE_URL}{FAKESTORE_PRODUCTS_ENDPOINT}"
    PAGE_LIMIT = 20          # items per page request
    MAX_PAGES = 10           # safety ceiling

    def __init__(
        self,
        staging_dir: str | Path = "staging",
        base_url: str = FAKESTORE_BASE_URL,
    ) -> None:
        super().__init__(staging_dir)
        self.source_name = self.SOURCE_NAME
        self.base_url = base_url.rstrip("/")
        self.products_url = f"{self.base_url}{FAKESTORE_PRODUCTS_ENDPOINT}"

        # Persistent session — reuses TCP connection across paginated requests
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": DEFAULT_USER_AGENT})

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract(self, execution_date: str, run_id: str) -> list[dict]:
        """
        Fetch all products from FakeStoreAPI and return enriched records.

        Steps
        -----
        1. Paginate through all available products.
        2. Append extraction_timestamp, source, source_url, pipeline_run_id.
        3. Save raw JSON to staging/<execution_date>/raw_products.json.
        4. Return the list of enriched records.
        """
        logger.info(
            "[extract] Starting extraction | source=%s | date=%s | run_id=%s",
            self.SOURCE_NAME,
            execution_date,
            run_id,
        )

        start_time = time.monotonic()
        all_records: list[dict] = []

        # --- Pagination loop ---
        for page in range(self.MAX_PAGES):
            page_start = time.monotonic()
            page_records = self._fetch_page(page)

            if not page_records:
                logger.info("[extract] Empty response on page %d — stopping.", page)
                break

            logger.info(
                "[extract] Page %d: fetched %d rows (%.2f s)",
                page,
                len(page_records),
                time.monotonic() - page_start,
            )
            all_records.extend(page_records)

            # FakeStoreAPI returns the same ~20 products regardless of offset.
            # If the page count < PAGE_LIMIT, we've reached the end.
            if len(page_records) < self.PAGE_LIMIT:
                logger.info(
                    "[extract] Page %d returned %d < %d items — last page reached.",
                    page,
                    len(page_records),
                    self.PAGE_LIMIT,
                )
                break

            # Polite delay between requests (SRS FR-E06)
            if page < self.MAX_PAGES - 1:
                time.sleep(INTER_REQUEST_DELAY)

        elapsed = time.monotonic() - start_time
        logger.info(
            "[extract] Completed: %d total rows in %.2f s",
            len(all_records),
            elapsed,
        )

        # --- Enrich records ---
        extraction_timestamp = datetime.now(timezone.utc).isoformat()
        enriched = [
            self._enrich_record(rec, extraction_timestamp, execution_date, run_id)
            for rec in all_records
        ]

        # --- Persist to staging ---
        staged_path = self.save_raw(enriched, execution_date)
        logger.info("[extract] Raw data archived → %s", staged_path)

        return enriched

    def validate_response(self, raw_response: Any) -> bool:
        """
        Check that the API returned a non-empty list of dicts, each with
        the minimum required fields: id, title, price.

        Raises
        ------
        ValueError
            If the response structure is unexpected.
        """
        if not isinstance(raw_response, list):
            raise ValueError(
                f"Expected list from FakeStoreAPI, got {type(raw_response).__name__}"
            )

        if len(raw_response) == 0:
            return False  # valid but empty — caller decides how to handle

        required_keys = {"id", "title", "price"}
        missing = required_keys - set(raw_response[0].keys())
        if missing:
            raise ValueError(
                f"FakeStoreAPI response missing expected keys: {missing}. "
                f"Got: {list(raw_response[0].keys())}"
            )

        return True

    def save_raw(self, data: list[dict], execution_date: str) -> Path:
        """Atomically write raw records to staging/<execution_date>/raw_products.json."""
        path = self._make_staging_path(execution_date, "raw_products.json")
        self._atomic_write(path, data)
        return path

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @retry_with_backoff(
        max_retries=3,
        base_delay=30.0,
        jitter_max=5.0,
        retryable_exceptions=(
            requests.Timeout,
            requests.ConnectionError,
            RateLimitError,
            ServerError,
        ),
    )
    def _fetch_page(self, page: int) -> list[dict]:
        """
        Fetch a single page of products from FakeStoreAPI.

        FakeStoreAPI uses ?limit=N (no true offset), so for a real paginated
        source this would also pass an offset/page param.

        Decorated with @retry_with_backoff for automatic retries.
        """
        params = {"limit": self.PAGE_LIMIT}
        # For a source with real pagination: params["skip"] = page * self.PAGE_LIMIT

        logger.debug("[_fetch_page] GET %s params=%s", self.products_url, params)

        response = self.session.get(
            self.products_url,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )

        raise_for_status_with_context(response)

        raw = response.json()
        self.validate_response(raw)

        return raw

    def _enrich_record(
        self,
        raw: dict,
        extraction_timestamp: str,
        execution_date: str,
        run_id: str,
    ) -> dict:
        """
        Flatten nested fields and append pipeline metadata to a raw record.

        Mapping (FakeStoreAPI field → our schema):
            id            → product_id  (stringified)
            title         → name
            price         → price
            category      → category   (raw; normalisation happens in transformer)
            image         → source_url
            rating.rate   → rating
            rating.count  → review_count
            description   → description (kept for reference; not in products_market)
        """
        rating_obj = raw.get("rating") or {}

        return {
            "product_id": str(raw.get("id", "")),
            "name": raw.get("title", ""),
            "price": raw.get("price"),
            "category": raw.get("category"),
            "description": raw.get("description"),
            "rating": rating_obj.get("rate"),
            "review_count": rating_obj.get("count", 0),
            "stock_status": "in_stock",   # FakeStoreAPI has no stock field; default
            "source": self.SOURCE_NAME,
            "source_url": raw.get("image"),    # closest URL available in FakeStore
            "extraction_timestamp": extraction_timestamp,
            "extraction_date": execution_date,
            "pipeline_run_id": run_id,
        }


# ---------------------------------------------------------------------------
# Quick manual smoke-test (run: python -m etl.extractor)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    run_id = str(uuid.uuid4())

    extractor = FakeStoreExtractor(staging_dir="staging")
    records = extractor.extract(execution_date=today, run_id=run_id)

    print(f"\n✅  Extracted {len(records)} records")
    if records:
        print("Sample record:")
        print(json.dumps(records[0], indent=2))
    sys.exit(0)