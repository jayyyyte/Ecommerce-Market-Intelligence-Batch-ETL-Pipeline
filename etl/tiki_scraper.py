"""
Web scraping extractor for tiki.vn — production demo source.

Strategy
--------
Tiki product listing pages are JavaScript-rendered 
-> plain HTML scraping with requests+BS4 returns skeleton HTML only.  
We use Tiki's semi-public listing API endpoint (same JSON the frontend fetches) to 
get structured product data, then optionally enrich with BS4 parsing 
of individual product detail pages when extra fields are needed.

Primary endpoint (listing, paginated):
    GET https://tiki.vn/api/v2/products
    Params: category_id, page, limit, sort, platform=web
    Returns JSON: { data: [...products], paging: { total, current_page, last_page } }

Product detail endpoint (optional enrichment):
    GET https://tiki.vn/api/v2/products/<product_id>

This approach is:
  ✅ More reliable than HTML scraping (no JS rendering needed)
  ✅ Structured data — no fragile CSS selector maintenance
  ✅ Still demonstrates requests + real market data for demo

HTML parsing with BeautifulSoup is used as a fallback for fields not
available in the listing API response (e.g., detailed specs, stock status
from product detail pages).

Rate limiting
-------------
- 1.5 s sleep between paginated requests (SRS FR-E06)
- User-Agent rotated from BROWSER_USER_AGENTS pool
- Retry via @retry_with_backoff (inherited mechanism)

Usage
-----
    from etl.tiki_scraper import TikiScraper
    scraper = TikiScraper(staging_dir="staging", category_id=8095)
    records = scraper.extract(execution_date="2025-04-11", run_id="<uuid>")
"""

import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from etl.extractor import BaseExtractor
from utils.retry import (
    RateLimitError,
    ServerError,
    raise_for_status_with_context,
    retry_with_backoff,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TIKI_BASE_URL = "https://tiki.vn"
TIKI_LISTING_API = "https://tiki.vn/api/v2/products"
TIKI_PRODUCT_API = "https://tiki.vn/api/v2/products/{product_id}"

PAGE_LIMIT = 40          # items per page (Tiki supports up to 40)
MAX_PAGES = 10           # safety ceiling per category
INTER_REQUEST_DELAY = 1.5   # seconds between requests (SRS FR-E06)

# Default category: Laptops (8095). Override via constructor.
DEFAULT_CATEGORY_ID = 8095

# 5 real browser User-Agent strings for rotation (SRS Day 8-9 requirement)
BROWSER_USER_AGENTS = [
    # Chrome 124 / Windows
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    # Firefox 125 / Windows
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) "
        "Gecko/20100101 Firefox/125.0"
    ),
    # Safari 17 / macOS
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.4.1 Safari/605.1.15"
    ),
    # Edge 124 / Windows
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0"
    ),
    # Chrome 124 / macOS
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
]


# ---------------------------------------------------------------------------
# TikiScraper
# ---------------------------------------------------------------------------

class TikiScraper(BaseExtractor):
    """
    Extractor for https://tiki.vn — production demo data source.

    Uses Tiki's listing API for product data and BeautifulSoup for any
    supplementary HTML parsing needed from detail pages.

    Parameters
    ----------
    staging_dir : str | Path
        Root directory for staging files.
    category_id : int
        Tiki category ID to scrape. Default: 8095 (Laptops).
    max_pages : int
        Maximum number of pages to fetch per run. Default: 10.
    """

    SOURCE_NAME = "tiki_scrape"

    def __init__(
        self,
        staging_dir: str | Path = "staging",
        category_id: int = DEFAULT_CATEGORY_ID,
        max_pages: int = MAX_PAGES,
    ) -> None:
        super().__init__(staging_dir)
        self.source_name = self.SOURCE_NAME
        self.category_id = category_id
        self.max_pages = max_pages
        self._session = self._build_session()

    # ------------------------------------------------------------------
    # BaseExtractor interface
    # ------------------------------------------------------------------

    def extract(self, execution_date: str, run_id: str) -> list[dict]:
        """
        Scrape all products from the configured Tiki category.

        Steps
        -----
        1. Paginate through listing API pages until empty or max_pages reached.
        2. For each product, extract & normalise fields.
        3. Skip (log warning) any product missing required fields.
        4. Enrich with pipeline metadata.
        5. Save to staging/{execution_date}/raw_tiki_products.json.
        """
        logger.info(
            "[tiki] Starting extraction | category_id=%s | date=%s | run_id=%s",
            self.category_id,
            execution_date,
            run_id,
        )

        start_time = time.monotonic()
        all_records: list[dict] = []
        extraction_timestamp = datetime.now(timezone.utc).isoformat()

        for page in range(1, self.max_pages + 1):
            logger.info("[tiki] Fetching page %d / max %d", page, self.max_pages)

            # Rotate User-Agent on every request
            self._rotate_user_agent()

            page_raw = self._fetch_listing_page(page)

            if not page_raw:
                logger.info("[tiki] Empty page %d — stopping pagination.", page)
                break

            logger.info("[tiki] Page %d: %d raw products received.", page, len(page_raw))

            for raw in page_raw:
                record = self._parse_product(raw, execution_date, run_id, extraction_timestamp)
                if record is None:
                    # _parse_product returns None and logs warning for invalid rows
                    continue
                all_records.append(record)

            # Check if this was the last page
            if len(page_raw) < PAGE_LIMIT:
                logger.info(
                    "[tiki] Page %d returned %d < %d items — last page reached.",
                    page, len(page_raw), PAGE_LIMIT,
                )
                break

            # Polite delay before next page (SRS FR-E06)
            if page < self.max_pages:
                logger.debug("[tiki] Sleeping %.1f s before next page.", INTER_REQUEST_DELAY)
                time.sleep(INTER_REQUEST_DELAY)

        elapsed = time.monotonic() - start_time
        logger.info(
            "[tiki] Extraction complete: %d valid records in %.2f s",
            len(all_records),
            elapsed,
        )

        if not all_records:
            logger.warning(
                "[tiki] No records extracted for category_id=%s. "
                "Tiki may have changed their API or blocked the request.",
                self.category_id,
            )
            return []

        staged_path = self.save_raw(all_records, execution_date)
        logger.info("[tiki] Raw data archived → %s", staged_path)

        return all_records

    def validate_response(self, raw_response: Any) -> bool:
        """
        Validate Tiki listing API response structure.

        Expected: { "data": [...], "paging": {...} }
        or a plain list of products.

        Returns False for empty/missing data (not an error — just no records).
        """
        if isinstance(raw_response, list):
            return len(raw_response) > 0

        if not isinstance(raw_response, dict):
            raise ValueError(
                f"[tiki] Unexpected response type: {type(raw_response).__name__}"
            )

        data = raw_response.get("data")
        if data is None:
            raise ValueError(
                f"[tiki] Response missing 'data' key. Keys present: {list(raw_response.keys())}"
            )

        return len(data) > 0

    def save_raw(self, data: list[dict], execution_date: str) -> Path:
        """Save raw records to staging/<execution_date>/raw_tiki_products.json."""
        path = self._make_staging_path(execution_date, "raw_tiki_products.json")
        self._atomic_write(path, data)
        return path

    # ------------------------------------------------------------------
    # HTTP helpers
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
    def _fetch_listing_page(self, page: int) -> list[dict]:
        """
        Fetch one page of products from Tiki's listing API.

        Returns a flat list of raw product dicts (may be empty on last page).
        """
        params = {
            "category_id": self.category_id,
            "page": page,
            "limit": PAGE_LIMIT,
            "sort": "top_seller",
            "platform": "web",
            "version": 5,
        }

        response = self._session.get(
            TIKI_LISTING_API,
            params=params,
            timeout=30,
        )

        raise_for_status_with_context(response)

        body = response.json()

        # Tiki wraps products in body["data"]
        if isinstance(body, dict):
            products = body.get("data", [])
        elif isinstance(body, list):
            products = body
        else:
            raise ValueError(f"[tiki] Unexpected response shape: {type(body)}")

        return products

    def _fetch_product_detail_html(self, product_url: str) -> BeautifulSoup | None:
        """
        Fetch and parse the HTML of a product detail page with BeautifulSoup.

        Used as a supplementary enrichment step for fields not in the listing
        API (e.g., more granular stock status, detailed specs).

        Returns None on any error — callers must handle gracefully.
        """
        try:
            self._rotate_user_agent()
            response = self._session.get(product_url, timeout=20)
            raise_for_status_with_context(response)
            return BeautifulSoup(response.text, "html.parser")
        except Exception as exc:
            logger.warning(
                "[tiki] Could not fetch product detail page %s: %s",
                product_url,
                exc,
            )
            return None

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_product(
        self,
        raw: dict,
        execution_date: str,
        run_id: str,
        extraction_timestamp: str,
    ) -> dict | None:
        """
        Extract and validate fields from a single raw Tiki product dict.

        Tiki listing API field mapping:
            id              → product_id
            name            → name
            price           → price  (in VND)
            rating_average  → rating
            review_count    → review_count
            categories.name → category  (first breadcrumb)
            url_path        → source_url (relative; we build absolute)
            inventory_status→ stock_status  ('available' | 'out_of_stock' | etc.)

        Returns None and logs a warning if any CRITICAL field is missing/invalid.
        Calling code must skip None returns — do NOT raise here (SRS Day 8-9).
        """
        # --- Required fields ---
        product_id = str(raw.get("id", "")).strip()
        name = str(raw.get("name", "")).strip()
        price_raw = raw.get("price")

        if not product_id:
            logger.warning("[tiki] Skipping product — missing id. Raw keys: %s", list(raw.keys()))
            return None

        if not name:
            logger.warning("[tiki] Skipping product_id=%s — missing name.", product_id)
            return None

        try:
            price = float(price_raw)
            if price <= 0:
                raise ValueError("price must be > 0")
        except (TypeError, ValueError):
            logger.warning(
                "[tiki] Skipping product_id=%s — invalid price: %r",
                product_id,
                price_raw,
            )
            return None

        # --- Optional fields (degrade gracefully) ---
        rating_raw = raw.get("rating_average")
        try:
            rating = float(rating_raw) if rating_raw is not None else None
            if rating is not None:
                rating = max(0.0, min(5.0, rating))   # clip to [0, 5]
        except (TypeError, ValueError):
            rating = None

        review_count = int(raw.get("review_count", 0) or 0)

        # Category: Tiki nests it in breadcrumbs or categories list
        category = self._extract_category(raw)

        # Stock status
        inventory_raw = raw.get("inventory_status", "")
        stock_status = self._normalise_stock_status(inventory_raw)

        # Source URL
        url_path = raw.get("url_path") or raw.get("url_key") or ""
        source_url = (
            urljoin(TIKI_BASE_URL, url_path) if url_path
            else TIKI_BASE_URL
        )

        return {
            "product_id": product_id,
            "name": name,
            "price": price,
            "category": category,
            "rating": rating,
            "review_count": review_count,
            "stock_status": stock_status,
            "source": self.SOURCE_NAME,
            "source_url": source_url,
            "extraction_timestamp": extraction_timestamp,
            "extraction_date": execution_date,
            "pipeline_run_id": run_id,
        }

    @staticmethod
    def _extract_category(raw: dict) -> str | None:
        """
        Pull the most specific category name from the raw product dict.

        Tiki returns category info in several possible shapes:
          - raw["categories"]["name"]   (single category object)
          - raw["breadcrumbs"][-1]["name"]  (breadcrumb trail)
          - raw["primary_category"]["name"]
        """
        # Try breadcrumbs first (most specific leaf)
        breadcrumbs = raw.get("breadcrumbs")
        if breadcrumbs and isinstance(breadcrumbs, list) and len(breadcrumbs) > 0:
            leaf = breadcrumbs[-1]
            if isinstance(leaf, dict):
                return leaf.get("name")

        # Try categories object
        categories = raw.get("categories")
        if isinstance(categories, dict):
            return categories.get("name")

        # Try primary_category
        primary = raw.get("primary_category")
        if isinstance(primary, dict):
            return primary.get("name")

        return None

    @staticmethod
    def _normalise_stock_status(raw_status: str) -> str:
        """
        Map Tiki's inventory_status strings to our schema values.

        Tiki values observed: 'available', 'out_of_stock', 'preorder',
        'discontinued', 'backorder'.
        """
        mapping = {
            "available":    "in_stock",
            "preorder":     "in_stock",
            "backorder":    "in_stock",
            "out_of_stock": "out_of_stock",
            "discontinued": "out_of_stock",
        }
        return mapping.get(str(raw_status).lower().strip(), "unknown")

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------

    def _build_session(self) -> requests.Session:
        """Create a session with Tiki-appropriate headers."""
        session = requests.Session()
        session.headers.update({
            "User-Agent": random.choice(BROWSER_USER_AGENTS),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://tiki.vn/",
            "x-guest-token": "",   # Tiki expects this header; empty is fine for listing
        })
        return session

    def _rotate_user_agent(self) -> None:
        """Replace the session User-Agent with a randomly selected string."""
        new_ua = random.choice(BROWSER_USER_AGENTS)
        self._session.headers.update({"User-Agent": new_ua})
        logger.debug("[tiki] Rotated User-Agent → %s…", new_ua[:50])


# ---------------------------------------------------------------------------
# Smoke test (run: python -m etl.tiki_scraper)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    run_id = str(uuid.uuid4())

    # Scrape first page of Laptop category only (quick demo)
    scraper = TikiScraper(staging_dir="staging", category_id=8095, max_pages=1)
    records = scraper.extract(execution_date=today, run_id=run_id)

    print(f"\n✅  Scraped {len(records)} Tiki products")
    if records:
        print("Sample record:")
        print(json.dumps(records[0], indent=2, ensure_ascii=False))
    sys.exit(0)