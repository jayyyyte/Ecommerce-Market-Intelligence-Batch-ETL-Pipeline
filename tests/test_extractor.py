"""
tests/test_extractor.py
-----------------------
Unit tests for etl/extractor.py and utils/retry.py.

All HTTP calls are mocked — no live network required (CI/CD safe).

Test coverage
-------------
RetryDecorator
  ✓ Succeeds on first attempt (no retries needed)
  ✓ Retries on Timeout, succeeds on 3rd attempt
  ✓ Retries on ConnectionError, succeeds on 2nd attempt
  ✓ Raises RateLimitError and retries with extended delay
  ✓ Raises ServerError (5xx) and retries
  ✓ Exhausts all retries and re-raises last exception
  ✓ Does NOT retry on non-retryable ExtractionError

FakeStoreExtractor
  ✓ Successful extraction returns enriched records list
  ✓ validate_response() accepts valid list
  ✓ validate_response() rejects non-list response
  ✓ validate_response() rejects list missing required keys
  ✓ validate_response() returns False for empty list
  ✓ extract() stops pagination when page < PAGE_LIMIT items returned
  ✓ save_raw() writes atomic JSON file and returns correct path
  ✓ _enrich_record() flattens nested rating fields
  ✓ _enrich_record() defaults stock_status and review_count
  ✓ HTTP 429 raises RateLimitError (via raise_for_status_with_context)
  ✓ HTTP 503 raises ServerError
  ✓ HTTP 404 raises ExtractionError (non-retryable)
"""

import json
import sys
import time
import uuid
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
import requests

# Make project root importable when running pytest from repo root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl.extractor import FakeStoreExtractor
from utils.retry import (
    ExtractionError,
    RateLimitError,
    ServerError,
    raise_for_status_with_context,
    retry_with_backoff,
)

# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

SAMPLE_PRODUCT = {
    "id": 1,
    "title": "Test Backpack",
    "price": 109.95,
    "description": "A bag.",
    "category": "men's clothing",
    "image": "https://fakestoreapi.com/img/test.jpg",
    "rating": {"rate": 3.9, "count": 120},
}

SAMPLE_PRODUCT_2 = {
    "id": 2,
    "title": "Second Product",
    "price": 22.30,
    "description": "Another.",
    "category": "electronics",
    "image": "https://fakestoreapi.com/img/test2.jpg",
    "rating": {"rate": 4.1, "count": 55},
}

EXECUTION_DATE = "2025-04-11"
RUN_ID = str(uuid.uuid4())


@pytest.fixture
def extractor(tmp_path):
    """Return a FakeStoreExtractor writing to a temp staging directory."""
    return FakeStoreExtractor(staging_dir=tmp_path)


def make_mock_response(status_code: int, json_body=None) -> MagicMock:
    """Build a mock requests.Response."""
    mock_resp = MagicMock(spec=requests.Response)
    mock_resp.status_code = status_code
    mock_resp.url = "https://fakestoreapi.com/products"
    mock_resp.text = str(json_body)
    mock_resp.json.return_value = json_body
    return mock_resp


# ===========================================================================
# Section 1 — Retry decorator
# ===========================================================================

class TestRetryWithBackoff:
    """Tests for utils/retry.py @retry_with_backoff decorator."""

    def test_succeeds_on_first_attempt(self):
        """No retry needed — function should be called exactly once."""
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=0, jitter_max=0)
        def always_succeeds():
            nonlocal call_count
            call_count += 1
            return "ok"

        result = always_succeeds()
        assert result == "ok"
        assert call_count == 1

    @patch("time.sleep", return_value=None)
    def test_retries_on_timeout_succeeds_on_third(self, mock_sleep):
        """
        KEY TEST (from Sprint Plan):
        Mock raises Timeout on first 2 calls, succeeds on 3rd.
        """
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=30, jitter_max=0)
        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise requests.Timeout("Connection timed out")
            return "success"

        result = flaky()
        assert result == "success"
        assert call_count == 3
        assert mock_sleep.call_count == 2   # slept between attempt 1→2 and 2→3

    @patch("time.sleep", return_value=None)
    def test_retries_on_connection_error(self, mock_sleep):
        """ConnectionError should trigger a retry."""
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=0, jitter_max=0)
        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise requests.ConnectionError("Connection refused")
            return "ok"

        result = flaky()
        assert result == "ok"
        assert call_count == 2

    @patch("time.sleep", return_value=None)
    def test_retries_on_rate_limit_error(self, mock_sleep):
        """RateLimitError (HTTP 429) should trigger a retry."""
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=0, jitter_max=0)
        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RateLimitError("HTTP 429")
            return "ok"

        result = flaky()
        assert result == "ok"
        assert call_count == 2

    @patch("time.sleep", return_value=None)
    def test_retries_on_server_error(self, mock_sleep):
        """ServerError (HTTP 5xx) should trigger a retry."""
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=0, jitter_max=0)
        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise ServerError("HTTP 503")
            return "ok"

        result = flaky()
        assert result == "ok"
        assert call_count == 3

    @patch("time.sleep", return_value=None)
    def test_exhausts_retries_and_raises(self, mock_sleep):
        """After max_retries, the last exception must propagate."""

        @retry_with_backoff(max_retries=2, base_delay=0, jitter_max=0)
        def always_fails():
            raise requests.Timeout("Always times out")

        with pytest.raises(requests.Timeout):
            always_fails()

        # Called 3 times total: 1 initial + 2 retries
        assert mock_sleep.call_count == 2

    def test_does_not_retry_on_non_retryable_exception(self):
        """ExtractionError (4xx) must NOT be retried — raises immediately."""
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=0, jitter_max=0)
        def client_error():
            nonlocal call_count
            call_count += 1
            raise ExtractionError("HTTP 404 Not Found")

        with pytest.raises(ExtractionError):
            client_error()

        assert call_count == 1   # called only once — no retries

    @patch("time.sleep", return_value=None)
    def test_exponential_backoff_delay_sequence(self, mock_sleep):
        """Sleep durations must follow base * 2^attempt (no jitter in test)."""

        @retry_with_backoff(max_retries=3, base_delay=30, jitter_max=0)
        def always_fails():
            raise requests.Timeout()

        with pytest.raises(requests.Timeout):
            always_fails()

        # Attempt 0 fails → sleep 30*(2^0)=30, attempt 1 → 60, attempt 2 → 120
        sleep_calls = [c.args[0] for c in mock_sleep.call_args_list]
        assert sleep_calls == pytest.approx([30.0, 60.0, 120.0], abs=1.0)


# ===========================================================================
# Section 2 — raise_for_status_with_context
# ===========================================================================

class TestRaiseForStatusWithContext:

    def test_200_no_exception(self):
        resp = make_mock_response(200, [])
        raise_for_status_with_context(resp)   # should not raise

    def test_429_raises_rate_limit_error(self):
        resp = make_mock_response(429)
        with pytest.raises(RateLimitError):
            raise_for_status_with_context(resp)

    def test_503_raises_server_error(self):
        resp = make_mock_response(503)
        with pytest.raises(ServerError):
            raise_for_status_with_context(resp)

    def test_500_raises_server_error(self):
        resp = make_mock_response(500)
        with pytest.raises(ServerError):
            raise_for_status_with_context(resp)

    def test_404_raises_extraction_error(self):
        resp = make_mock_response(404)
        with pytest.raises(ExtractionError):
            raise_for_status_with_context(resp)

    def test_400_raises_extraction_error(self):
        resp = make_mock_response(400)
        with pytest.raises(ExtractionError):
            raise_for_status_with_context(resp)


# ===========================================================================
# Section 3 — FakeStoreExtractor.validate_response
# ===========================================================================

class TestValidateResponse:

    def test_valid_list_returns_true(self, extractor):
        assert extractor.validate_response([SAMPLE_PRODUCT]) is True

    def test_empty_list_returns_false(self, extractor):
        assert extractor.validate_response([]) is False

    def test_non_list_raises(self, extractor):
        with pytest.raises(ValueError, match="Expected list"):
            extractor.validate_response({"id": 1})

    def test_missing_required_key_raises(self, extractor):
        bad = [{"id": 1, "title": "x"}]   # missing 'price'
        with pytest.raises(ValueError, match="missing expected keys"):
            extractor.validate_response(bad)


# ===========================================================================
# Section 4 — FakeStoreExtractor._enrich_record
# ===========================================================================

class TestEnrichRecord:

    def test_flattens_nested_rating(self, extractor):
        enriched = extractor._enrich_record(
            SAMPLE_PRODUCT, "2025-04-11T00:00:00+00:00", EXECUTION_DATE, RUN_ID
        )
        assert enriched["rating"] == 3.9
        assert enriched["review_count"] == 120

    def test_product_id_is_stringified(self, extractor):
        enriched = extractor._enrich_record(
            SAMPLE_PRODUCT, "ts", EXECUTION_DATE, RUN_ID
        )
        assert enriched["product_id"] == "1"
        assert isinstance(enriched["product_id"], str)

    def test_source_field_set_correctly(self, extractor):
        enriched = extractor._enrich_record(
            SAMPLE_PRODUCT, "ts", EXECUTION_DATE, RUN_ID
        )
        assert enriched["source"] == "fakestore_api"

    def test_default_stock_status(self, extractor):
        enriched = extractor._enrich_record(
            SAMPLE_PRODUCT, "ts", EXECUTION_DATE, RUN_ID
        )
        assert enriched["stock_status"] == "in_stock"

    def test_missing_rating_defaults_gracefully(self, extractor):
        product_no_rating = {**SAMPLE_PRODUCT, "rating": None}
        enriched = extractor._enrich_record(
            product_no_rating, "ts", EXECUTION_DATE, RUN_ID
        )
        assert enriched["rating"] is None
        assert enriched["review_count"] == 0

    def test_run_id_attached(self, extractor):
        enriched = extractor._enrich_record(
            SAMPLE_PRODUCT, "ts", EXECUTION_DATE, RUN_ID
        )
        assert enriched["pipeline_run_id"] == RUN_ID

    def test_extraction_date_attached(self, extractor):
        enriched = extractor._enrich_record(
            SAMPLE_PRODUCT, "ts", EXECUTION_DATE, RUN_ID
        )
        assert enriched["extraction_date"] == EXECUTION_DATE


# ===========================================================================
# Section 5 — FakeStoreExtractor.save_raw
# ===========================================================================

class TestSaveRaw:

    def test_creates_json_file(self, extractor, tmp_path):
        data = [{"product_id": "1", "name": "Test"}]
        path = extractor.save_raw(data, EXECUTION_DATE)

        assert path.exists()
        assert path.suffix == ".json"
        with path.open() as f:
            loaded = json.load(f)
        assert loaded == data

    def test_no_tmp_file_left_on_success(self, extractor, tmp_path):
        data = [{"product_id": "1"}]
        path = extractor.save_raw(data, EXECUTION_DATE)
        tmp_path_file = path.with_suffix(".tmp")
        assert not tmp_path_file.exists()

    def test_staging_directory_created(self, extractor, tmp_path):
        data = [{"product_id": "99"}]
        path = extractor.save_raw(data, "2030-01-01")
        assert (tmp_path / "2030-01-01").is_dir()


# ===========================================================================
# Section 6 — FakeStoreExtractor.extract (integration-level, no network)
# ===========================================================================

class TestExtract:

    @patch("time.sleep", return_value=None)
    def test_successful_extraction_returns_enriched_list(self, mock_sleep, extractor):
        """Full happy-path: mock HTTP returns two products."""
        mock_resp = make_mock_response(200, [SAMPLE_PRODUCT, SAMPLE_PRODUCT_2])

        with patch.object(extractor.session, "get", return_value=mock_resp):
            results = extractor.extract(EXECUTION_DATE, RUN_ID)

        assert len(results) == 2
        assert results[0]["product_id"] == "1"
        assert results[1]["product_id"] == "2"
        assert all(r["source"] == "fakestore_api" for r in results)
        assert all(r["extraction_date"] == EXECUTION_DATE for r in results)

    @patch("time.sleep", return_value=None)
    def test_stops_pagination_when_fewer_than_limit(self, mock_sleep, extractor):
        """
        When the page returns < PAGE_LIMIT items, the loop should stop.
        With 2 products < PAGE_LIMIT=20, we expect exactly 1 HTTP call.
        """
        mock_resp = make_mock_response(200, [SAMPLE_PRODUCT, SAMPLE_PRODUCT_2])

        with patch.object(extractor.session, "get", return_value=mock_resp) as mock_get:
            extractor.extract(EXECUTION_DATE, RUN_ID)

        assert mock_get.call_count == 1

    @patch("time.sleep", return_value=None)
    def test_saves_raw_file_to_staging(self, mock_sleep, extractor, tmp_path):
        """extract() must produce a JSON file in staging/<date>/raw_products.json."""
        mock_resp = make_mock_response(200, [SAMPLE_PRODUCT])

        with patch.object(extractor.session, "get", return_value=mock_resp):
            extractor.extract(EXECUTION_DATE, RUN_ID)

        staged_file = tmp_path / EXECUTION_DATE / "raw_products.json"
        assert staged_file.exists()
        with staged_file.open() as f:
            saved = json.load(f)
        assert len(saved) == 1

    @patch("time.sleep", return_value=None)
    def test_empty_api_response_returns_empty_list(self, mock_sleep, extractor):
        """Empty response from API should not crash — return [] gracefully."""
        mock_resp = make_mock_response(200, [])

        with patch.object(extractor.session, "get", return_value=mock_resp):
            results = extractor.extract(EXECUTION_DATE, RUN_ID)

        assert results == []

    @patch("time.sleep", return_value=None)
    def test_retries_on_timeout_and_succeeds(self, mock_sleep, extractor):
        """
        KEY TEST (Sprint Plan Day 6-7):
        First 2 calls raise Timeout; 3rd succeeds.
        """
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise requests.Timeout("Timed out")
            return make_mock_response(200, [SAMPLE_PRODUCT])

        with patch.object(extractor.session, "get", side_effect=side_effect):
            # Patch time.sleep in retry module to skip actual waits
            with patch("utils.retry.time.sleep", return_value=None):
                results = extractor.extract(EXECUTION_DATE, RUN_ID)

        assert len(results) == 1
        assert call_count == 3

    @patch("time.sleep", return_value=None)
    def test_malformed_json_raises(self, mock_sleep, extractor):
        """If the API returns non-list JSON, validate_response should raise."""
        mock_resp = make_mock_response(200, {"error": "not a list"})

        with patch.object(extractor.session, "get", return_value=mock_resp):
            with patch("utils.retry.time.sleep", return_value=None):
                with pytest.raises(ValueError, match="Expected list"):
                    extractor.extract(EXECUTION_DATE, RUN_ID)