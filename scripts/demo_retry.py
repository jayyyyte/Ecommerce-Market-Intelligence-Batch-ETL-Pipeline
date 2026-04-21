"""
Verifies the @retry_with_backoff decorator behaviour.
No Docker, no Airflow, no internet needed.

Run from the ecommerce-etl project root:
    Windows:  python scripts\\demo_retry.py
    Mac/Linux: python scripts/demo_retry.py

Strategy: we swap out the `time` module inside utils.retry with a fake
module whose `sleep()` records delays but doesn't actually wait.
This avoids the infinite-recursion problem that occurs when a patched
sleep() tries to call itself.
"""

import logging
import sys
import time
import types
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import utils.retry as retry_module  # import the module so we can swap time inside it
from utils.retry import (
    ExtractionError,
    RateLimitError,
    ServerError,
    retry_with_backoff,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)-8s]  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("demo_retry")

SEP = "-" * 60


# ---------------------------------------------------------------------------
# Fake time module: records sleep calls, uses real time.sleep for tiny waits
# ---------------------------------------------------------------------------

class _FakeTime:
    """Drop-in replacement for the `time` module inside utils.retry."""

    def __init__(self, real_sleep_secs=0.05):
        self._real_sleep_secs = real_sleep_secs
        self.sleep_log = []          # records the delays the decorator requested
        self._real_sleep = time.sleep

    def sleep(self, seconds):
        self.sleep_log.append(round(seconds))
        logger.info(
            "  [SLEEP] Decorator requested %.0f s -> fast demo: %.2f s",
            seconds,
            self._real_sleep_secs,
        )
        self._real_sleep(self._real_sleep_secs)   # real sleep, very short

    def monotonic(self):
        return time.monotonic()


def _install_fake_time(fake):
    """Swap utils.retry's reference to `time` with our fake object."""
    retry_module.time = fake


def _restore_real_time():
    """Restore the real time module in utils.retry."""
    retry_module.time = time


# ---------------------------------------------------------------------------
# Demos
# ---------------------------------------------------------------------------

def section(title):
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)


def demo_1_timeout_retry():
    section("DEMO 1 -- Network Timeout: retries 2x, succeeds on 3rd attempt")

    fake = _FakeTime()
    _install_fake_time(fake)
    call_count = 0

    try:
        @retry_with_backoff(max_retries=3, base_delay=30, jitter_max=0)
        def fetch():
            nonlocal call_count
            call_count += 1
            logger.info("  Attempt %d", call_count)
            if call_count < 3:
                raise requests.Timeout("Timed out")
            return {"rows": 20}

        result = fetch()
    finally:
        _restore_real_time()

    print(f"\n  Result     : {result}")
    print(f"  Calls made : {call_count}   (expected: 3)")
    print(f"  Sleeps     : {fake.sleep_log}  (expected: [30, 60])")
    ok = call_count == 3 and fake.sleep_log == [30, 60]
    print("  >> PASS" if ok else "  >> FAIL")


def demo_2_rate_limit():
    section("DEMO 2 -- HTTP 429 Rate Limit: retries once, min 60s delay enforced")

    fake = _FakeTime()
    _install_fake_time(fake)
    call_count = 0

    try:
        @retry_with_backoff(max_retries=3, base_delay=30, jitter_max=0)
        def fetch():
            nonlocal call_count
            call_count += 1
            logger.info("  Attempt %d", call_count)
            if call_count == 1:
                raise RateLimitError("HTTP 429 Too Many Requests")
            return [{"id": 99}]

        result = fetch()
    finally:
        _restore_real_time()

    print(f"\n  Result     : {result}")
    print(f"  Calls made : {call_count}   (expected: 2)")
    print(f"  Delay used : {fake.sleep_log[0]}s  (expected: >= 60)")
    ok = call_count == 2 and fake.sleep_log[0] >= 60
    print("  >> PASS" if ok else "  >> FAIL")


def demo_3_server_error_backoff():
    section("DEMO 3 -- HTTP 503 Server Error: exponential backoff 30 -> 60 -> 120")

    fake = _FakeTime()
    _install_fake_time(fake)
    call_count = 0

    try:
        @retry_with_backoff(max_retries=3, base_delay=30, jitter_max=0)
        def fetch():
            nonlocal call_count
            call_count += 1
            logger.info("  Attempt %d", call_count)
            if call_count <= 2:
                raise ServerError("HTTP 503")
            return {"status": "ok"}

        result = fetch()
    finally:
        _restore_real_time()

    print(f"\n  Result         : {result}")
    print(f"  Calls made     : {call_count}   (expected: 3)")
    print(f"  Backoff delays : {fake.sleep_log}  (expected: [30, 60])")
    ok = call_count == 3 and fake.sleep_log == [30, 60]
    print("  >> PASS" if ok else "  >> FAIL")


def demo_4_all_retries_exhausted():
    section("DEMO 4 -- All retries exhausted: last exception propagates to caller")

    fake = _FakeTime()
    _install_fake_time(fake)
    call_count = 0
    caught = None

    try:
        @retry_with_backoff(max_retries=3, base_delay=30, jitter_max=0)
        def fetch():
            nonlocal call_count
            call_count += 1
            logger.warning("  Attempt %d always fails", call_count)
            raise requests.Timeout("Server unreachable")

        fetch()
    except requests.Timeout as exc:
        caught = exc
    finally:
        _restore_real_time()

    print(f"\n  Exception    : {type(caught).__name__}: {caught}")
    print(f"  Total calls  : {call_count}   (expected: 4 = 1 initial + 3 retries)")
    ok = caught is not None and call_count == 4
    print("  >> PASS" if ok else "  >> FAIL")


def demo_5_no_retry_on_4xx():
    section("DEMO 5 -- HTTP 404 ExtractionError: NOT retried (client error != transient)")

    call_count = 0
    caught = None

    # No fake time needed — ExtractionError is not in retryable_exceptions,
    # so the decorator never reaches the sleep() call at all.
    @retry_with_backoff(max_retries=3, base_delay=30, jitter_max=0)
    def fetch():
        nonlocal call_count
        call_count += 1
        logger.warning("  Attempt %d -- HTTP 404", call_count)
        raise ExtractionError("HTTP 404: endpoint not found")

    try:
        fetch()
    except ExtractionError as exc:
        caught = exc

    print(f"\n  Exception    : {type(caught).__name__}: {caught}")
    print(f"  Total calls  : {call_count}   (expected: 1, no retries)")
    ok = caught is not None and call_count == 1
    print("  >> PASS" if ok else "  >> FAIL")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("\n" + "=" * 60)
    print("  RETRY DECORATOR DEMO  --  utils/retry.py")
    print("  Production delays (30/60/120s) compressed to 0.05s")
    print("=" * 60)

    demo_1_timeout_retry()
    demo_2_rate_limit()
    demo_3_server_error_backoff()
    demo_4_all_retries_exhausted()
    demo_5_no_retry_on_4xx()

    print(f"\n{SEP}")
    print("  Done. All 5 demos ran. Look for PASS / FAIL above.")
    print(SEP + "\n")


if __name__ == "__main__":
    main()