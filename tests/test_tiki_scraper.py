"""
Unit tests for etl/tiki_scraper.py — all HTTP mocked, no live network.

Coverage
--------
TikiScraper.validate_response
  - Valid dict with data list → True
  - Valid plain list → True
  - Empty data list → False
  - Empty plain list → False
  - Non-dict/non-list → raises ValueError
  - Dict missing 'data' key → raises ValueError

TikiScraper._parse_product
  - Valid product returns fully populated record
  - Missing product_id → returns None (graceful skip)
  - Missing name → returns None
  - Missing price → returns None
  - Negative price → returns None
  - Invalid price string → returns None
  - Missing optional fields default gracefully (rating, review_count, category)
  - Rating clipped to [0.0, 5.0] range
  - source_url built correctly from url_path
  - stock_status mapped correctly for all known Tiki values

TikiScraper._extract_category
  - Extracts from breadcrumbs (preferred)
  - Falls back to categories dict
  - Falls back to primary_category
  - Returns None when all options absent

TikiScraper._normalise_stock_status
  - 'available' → 'in_stock'
  - 'preorder' → 'in_stock'
  - 'out_of_stock' → 'out_of_stock'
  - 'discontinued' → 'out_of_stock'
  - Unknown value → 'unknown'

TikiScraper._fetch_listing_page
  - Parses response with data wrapper dict correctly
  - Parses plain list response correctly
  - HTTP 429 raises RateLimitError (retryable)
  - HTTP 503 raises ServerError (retryable)

TikiScraper.extract (integration-level, mocked)
  - Happy path: returns enriched list, saves staging file
  - Invalid products in page are skipped, valid ones pass through
  - Stops pagination when page < PAGE_LIMIT
  - Empty first page returns [] without crashing
  - All records have required metadata fields
  - save_raw creates staging/{date}/raw_tiki_products.json
"""

import json
import sys
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl.tiki_scraper import (
    BROWSER_USER_AGENTS,
    PAGE_LIMIT,
    TIKI_BASE_URL,
    TikiScraper,
)
from utils.retry import RateLimitError, ServerError

# ---------------------------------------------------------------------------
# Fixtures & shared helpers
# ---------------------------------------------------------------------------

EXECUTION_DATE = "2025-04-11"
RUN_ID = str(uuid.uuid4())


def make_product(
    pid: int = 1,
    name: str = "Laptop ASUS VivoBook",
    price: float = 12_990_000.0,
    rating: float = 4.5,
    review_count: int = 230,
    inventory_status: str = "available",
    url_path: str = "/laptop-asus-vivobook-p1.html",
) -> dict:
    """Build a realistic Tiki API product dict."""
    return {
        "id": pid,
        "name": name,
        "price": price,
        "rating_average": rating,
        "review_count": review_count,
        "inventory_status": inventory_status,
        "url_path": url_path,
        "breadcrumbs": [
            {"name": "Laptop & Máy tính"},
            {"name": "Laptop"},
        ],
    }


def make_mock_response(status_code: int, body) -> MagicMock:
    mock = MagicMock(spec=requests.Response)
    mock.status_code = status_code
    mock.url = "https://tiki.vn/api/v2/products"
    mock.text = str(body)
    mock.json.return_value = body
    return mock


@pytest.fixture
def scraper(tmp_path) -> TikiScraper:
    return TikiScraper(staging_dir=tmp_path, category_id=8095, max_pages=5)


# ===========================================================================
# Section 1 — validate_response
# ===========================================================================

class TestValidateResponse:

    def test_valid_dict_with_data_list(self, scraper):
        resp = {"data": [make_product()], "paging": {"total": 1}}
        assert scraper.validate_response(resp) is True

    def test_valid_plain_list(self, scraper):
        assert scraper.validate_response([make_product()]) is True

    def test_empty_data_list(self, scraper):
        assert scraper.validate_response({"data": []}) is False

    def test_empty_plain_list(self, scraper):
        assert scraper.validate_response([]) is False

    def test_non_dict_non_list_raises(self, scraper):
        with pytest.raises(ValueError, match="Unexpected response type"):
            scraper.validate_response("not valid")

    def test_missing_data_key_raises(self, scraper):
        with pytest.raises(ValueError, match="missing 'data' key"):
            scraper.validate_response({"paging": {}, "filters": []})


# ===========================================================================
# Section 2 — _parse_product
# ===========================================================================

class TestParseProduct:

    TS = "2025-04-11T00:00:00+00:00"

    def _parse(self, scraper, raw):
        return scraper._parse_product(raw, EXECUTION_DATE, RUN_ID, self.TS)

    def test_valid_product_returns_record(self, scraper):
        result = self._parse(scraper, make_product(pid=42))
        assert result is not None
        assert result["product_id"] == "42"
        assert result["name"] == "Laptop ASUS VivoBook"
        assert result["price"] == 12_990_000.0
        assert result["source"] == "tiki_scrape"
        assert result["extraction_date"] == EXECUTION_DATE
        assert result["pipeline_run_id"] == RUN_ID

    def test_missing_product_id_returns_none(self, scraper):
        raw = make_product()
        del raw["id"]
        assert self._parse(scraper, raw) is None

    def test_empty_string_product_id_returns_none(self, scraper):
        raw = {**make_product(), "id": ""}
        assert self._parse(scraper, raw) is None

    def test_missing_name_returns_none(self, scraper):
        raw = {**make_product(), "name": ""}
        assert self._parse(scraper, raw) is None

    def test_none_price_returns_none(self, scraper):
        raw = {**make_product(), "price": None}
        assert self._parse(scraper, raw) is None

    def test_zero_price_returns_none(self, scraper):
        raw = {**make_product(), "price": 0}
        assert self._parse(scraper, raw) is None

    def test_negative_price_returns_none(self, scraper):
        raw = {**make_product(), "price": -500}
        assert self._parse(scraper, raw) is None

    def test_invalid_price_string_returns_none(self, scraper):
        raw = {**make_product(), "price": "N/A"}
        assert self._parse(scraper, raw) is None

    def test_missing_rating_defaults_to_none(self, scraper):
        raw = {**make_product(), "rating_average": None}
        result = self._parse(scraper, raw)
        assert result is not None
        assert result["rating"] is None

    def test_missing_review_count_defaults_to_zero(self, scraper):
        raw = {**make_product()}
        del raw["review_count"]
        result = self._parse(scraper, raw)
        assert result is not None
        assert result["review_count"] == 0

    def test_rating_clipped_above_5(self, scraper):
        raw = {**make_product(), "rating_average": 6.5}
        result = self._parse(scraper, raw)
        assert result["rating"] == 5.0

    def test_rating_clipped_below_0(self, scraper):
        raw = {**make_product(), "rating_average": -1.0}
        result = self._parse(scraper, raw)
        assert result["rating"] == 0.0

    def test_source_url_built_from_url_path(self, scraper):
        raw = {**make_product(), "url_path": "/san-pham/laptop-p123.html"}
        result = self._parse(scraper, raw)
        assert result["source_url"] == "https://tiki.vn/san-pham/laptop-p123.html"

    def test_source_url_fallback_when_no_url_path(self, scraper):
        raw = {**make_product()}
        raw.pop("url_path", None)
        result = self._parse(scraper, raw)
        assert result["source_url"] == TIKI_BASE_URL

    def test_category_extracted_from_breadcrumbs(self, scraper):
        result = self._parse(scraper, make_product())
        assert result["category"] == "Laptop"

    def test_category_falls_back_to_categories_dict(self, scraper):
        raw = {**make_product()}
        raw.pop("breadcrumbs", None)
        raw["categories"] = {"name": "Điện tử"}
        result = self._parse(scraper, raw)
        assert result["category"] == "Điện tử"

    def test_category_none_when_all_absent(self, scraper):
        raw = {**make_product()}
        raw.pop("breadcrumbs", None)
        result = self._parse(scraper, raw)
        assert result["category"] is None


# ===========================================================================
# Section 3 — _extract_category
# ===========================================================================

class TestExtractCategory:

    def test_uses_breadcrumbs_last_item(self, scraper):
        raw = {"breadcrumbs": [{"name": "Root"}, {"name": "Leaf"}]}
        assert scraper._extract_category(raw) == "Leaf"

    def test_single_breadcrumb(self, scraper):
        raw = {"breadcrumbs": [{"name": "Only"}]}
        assert scraper._extract_category(raw) == "Only"

    def test_falls_back_to_categories_dict(self, scraper):
        raw = {"categories": {"name": "Electronics"}}
        assert scraper._extract_category(raw) == "Electronics"

    def test_falls_back_to_primary_category(self, scraper):
        raw = {"primary_category": {"name": "Phones"}}
        assert scraper._extract_category(raw) == "Phones"

    def test_returns_none_when_all_absent(self, scraper):
        assert scraper._extract_category({}) is None

    def test_empty_breadcrumbs_falls_through(self, scraper):
        raw = {"breadcrumbs": [], "categories": {"name": "Backup"}}
        assert scraper._extract_category(raw) == "Backup"


# ===========================================================================
# Section 4 — _normalise_stock_status
# ===========================================================================

class TestNormaliseStockStatus:

    @pytest.mark.parametrize("raw,expected", [
        ("available",    "in_stock"),
        ("preorder",     "in_stock"),
        ("backorder",    "in_stock"),
        ("out_of_stock", "out_of_stock"),
        ("discontinued", "out_of_stock"),
        ("AVAILABLE",    "in_stock"),       # case-insensitive
        ("",             "unknown"),
        ("whatever",     "unknown"),
    ])
    def test_mapping(self, raw, expected):
        assert TikiScraper._normalise_stock_status(raw) == expected


# ===========================================================================
# Section 5 — _fetch_listing_page
# ===========================================================================

class TestFetchListingPage:

    def test_parses_dict_wrapper_response(self, scraper):
        products = [make_product(i) for i in range(3)]
        mock_resp = make_mock_response(200, {"data": products, "paging": {}})

        with patch.object(scraper._session, "get", return_value=mock_resp):
            result = scraper._fetch_listing_page(1)

        assert len(result) == 3
        assert result[0]["id"] == 0

    def test_parses_plain_list_response(self, scraper):
        products = [make_product(i) for i in range(2)]
        mock_resp = make_mock_response(200, products)

        with patch.object(scraper._session, "get", return_value=mock_resp):
            result = scraper._fetch_listing_page(1)

        assert len(result) == 2

    def test_http_429_raises_rate_limit_error(self, scraper):
        mock_resp = make_mock_response(429, {})
        with patch.object(scraper._session, "get", return_value=mock_resp):
            with patch("utils.retry.time.sleep"):
                with pytest.raises(RateLimitError):
                    scraper._fetch_listing_page(1)

    def test_http_503_raises_server_error(self, scraper):
        mock_resp = make_mock_response(503, {})
        with patch.object(scraper._session, "get", return_value=mock_resp):
            with patch("utils.retry.time.sleep"):
                with pytest.raises(ServerError):
                    scraper._fetch_listing_page(1)


# ===========================================================================
# Section 6 — extract() integration-level (fully mocked)
# ===========================================================================

class TestExtract:

    @patch("time.sleep")
    def test_happy_path_returns_enriched_records(self, mock_sleep, scraper, tmp_path):
        products = [make_product(i + 1) for i in range(3)]
        mock_resp = make_mock_response(200, {"data": products})

        with patch.object(scraper._session, "get", return_value=mock_resp):
            results = scraper.extract(EXECUTION_DATE, RUN_ID)

        assert len(results) == 3
        for r in results:
            assert r["source"] == "tiki_scrape"
            assert r["extraction_date"] == EXECUTION_DATE
            assert r["pipeline_run_id"] == RUN_ID
            assert "product_id" in r
            assert "price" in r

    @patch("time.sleep")
    def test_invalid_products_are_skipped(self, mock_sleep, scraper):
        """Mix of valid and invalid; only valid ones should be in output."""
        products = [
            make_product(1),               # valid
            {**make_product(2), "price": None},   # invalid — no price
            {**make_product(3), "name": ""},      # invalid — no name
            make_product(4),               # valid
        ]
        mock_resp = make_mock_response(200, {"data": products})

        with patch.object(scraper._session, "get", return_value=mock_resp):
            results = scraper.extract(EXECUTION_DATE, RUN_ID)

        assert len(results) == 2
        assert {r["product_id"] for r in results} == {"1", "4"}

    @patch("time.sleep")
    def test_stops_pagination_at_partial_page(self, mock_sleep, scraper):
        """When page returns < PAGE_LIMIT items, only 1 HTTP call should be made."""
        products = [make_product(i) for i in range(5)]  # 5 < PAGE_LIMIT=40
        mock_resp = make_mock_response(200, {"data": products})

        with patch.object(scraper._session, "get", return_value=mock_resp) as mock_get:
            scraper.extract(EXECUTION_DATE, RUN_ID)

        assert mock_get.call_count == 1

    @patch("time.sleep")
    def test_empty_first_page_returns_empty_list(self, mock_sleep, scraper):
        mock_resp = make_mock_response(200, {"data": []})

        with patch.object(scraper._session, "get", return_value=mock_resp):
            results = scraper.extract(EXECUTION_DATE, RUN_ID)

        assert results == []

    @patch("time.sleep")
    def test_staging_file_created(self, mock_sleep, scraper, tmp_path):
        products = [make_product(1)]
        mock_resp = make_mock_response(200, {"data": products})

        with patch.object(scraper._session, "get", return_value=mock_resp):
            scraper.extract(EXECUTION_DATE, RUN_ID)

        staged = tmp_path / EXECUTION_DATE / "raw_tiki_products.json"
        assert staged.exists()
        saved = json.loads(staged.read_text())
        assert len(saved) == 1

    @patch("time.sleep")
    def test_user_agent_rotated_on_each_request(self, mock_sleep, scraper):
        """User-Agent header must be one of our known 5 strings after rotation."""
        products = [make_product(1)]
        mock_resp = make_mock_response(200, {"data": products})

        captured_uas = []

        original_get = scraper._session.get

        def capture_get(*args, **kwargs):
            captured_uas.append(scraper._session.headers.get("User-Agent"))
            return mock_resp

        with patch.object(scraper._session, "get", side_effect=capture_get):
            scraper.extract(EXECUTION_DATE, RUN_ID)

        assert len(captured_uas) >= 1
        for ua in captured_uas:
            assert ua in BROWSER_USER_AGENTS, f"Unknown User-Agent: {ua}"

    @patch("time.sleep")
    def test_retry_on_timeout_succeeds(self, mock_sleep, scraper):
        """Timeout on first call, success on second — overall extract succeeds."""
        call_count = 0
        products = [make_product(1)]

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise requests.Timeout("Timed out")
            return make_mock_response(200, {"data": products})

        with patch.object(scraper._session, "get", side_effect=side_effect):
            with patch("utils.retry.time.sleep"):
                results = scraper.extract(EXECUTION_DATE, RUN_ID)

        assert len(results) == 1
        assert call_count == 2

    @patch("time.sleep")
    def test_multiple_pages_fetched_when_full_page(self, mock_sleep, scraper):
        """
        When page 1 returns exactly PAGE_LIMIT=40 items, the scraper must
        fetch page 2.  Page 2 returns < 40 → stops.
        Total: 2 HTTP calls, 40+3=43 records.
        """
        full_page = [make_product(i) for i in range(PAGE_LIMIT)]
        partial_page = [make_product(i + PAGE_LIMIT) for i in range(3)]

        responses = [
            make_mock_response(200, {"data": full_page}),
            make_mock_response(200, {"data": partial_page}),
        ]
        response_iter = iter(responses)

        with patch.object(scraper._session, "get", side_effect=lambda *a, **k: next(response_iter)) as mock_get:
            results = scraper.extract(EXECUTION_DATE, RUN_ID)

        assert mock_get.call_count == 2
        assert len(results) == PAGE_LIMIT + 3