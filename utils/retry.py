"""
Provides a @retry_with_backoff decorator for HTTP-related operations.

Retry strategy (per SRS Section 3.5):
  - Max 3 retries
  - Exponential backoff: base_delay * (2 ** attempt)  →  30s, 60s, 120s
  - Jitter: random 0–5 s added to each delay to avoid thundering-herd
  - Distinguishes between retryable vs non-retryable failures
"""

import functools
import logging
import random
import time
from typing import Callable, Tuple, Type

import requests

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exception hierarchy
# ---------------------------------------------------------------------------

class RateLimitError(Exception):
    """Raised when the source returns HTTP 429 Too Many Requests."""


class ServerError(Exception):
    """Raised when the source returns an HTTP 5xx response."""


class ExtractionError(Exception):
    """Non-retryable extraction failure (e.g. 4xx client errors)."""


# ---------------------------------------------------------------------------
# Retry decorator
# ---------------------------------------------------------------------------

def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 30.0,
    jitter_max: float = 5.0,
    retryable_exceptions: Tuple[Type[Exception], ...] = (
        requests.Timeout,
        requests.ConnectionError,
        RateLimitError,
        ServerError,
    ),
) -> Callable:
    """
    Decorator that retries the wrapped function on transient failures.

    Parameters
    ----------
    max_retries : int
        Number of retry attempts (not counting the first call).
    base_delay : float
        Base sleep time in seconds. Doubled on each subsequent attempt.
        Sequence with base_delay=30 → 30 s, 60 s, 120 s.
    jitter_max : float
        Upper bound (seconds) of random jitter added to each delay.
    retryable_exceptions : tuple
        Exception types that trigger a retry. All other exceptions propagate
        immediately.

    Raises
    ------
    The last exception encountered after all retries are exhausted.
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc: Exception | None = None

            for attempt in range(max_retries + 1):  # attempt 0 = first try
                try:
                    return func(*args, **kwargs)

                except retryable_exceptions as exc:
                    last_exc = exc
                    exc_name = type(exc).__name__

                    if attempt == max_retries:
                        logger.error(
                            "[retry] %s failed after %d attempts. "
                            "Last error: %s — giving up.",
                            func.__qualname__,
                            max_retries + 1,
                            exc,
                        )
                        raise

                    # Calculate delay with jitter
                    delay = base_delay * (2 ** attempt) + random.uniform(0, jitter_max)

                    # Special handling for RateLimitError — use a fixed minimum
                    if isinstance(exc, RateLimitError):
                        delay = max(delay, 60.0)
                        logger.warning(
                            "[retry] HTTP 429 Rate Limited in %s. "
                            "Sleeping %.1f s before retry %d/%d.",
                            func.__qualname__,
                            delay,
                            attempt + 1,
                            max_retries,
                        )
                    else:
                        logger.warning(
                            "[retry] %s in %s. "
                            "Sleeping %.1f s before retry %d/%d. Error: %s",
                            exc_name,
                            func.__qualname__,
                            delay,
                            attempt + 1,
                            max_retries,
                            exc,
                        )

                    time.sleep(delay)

            # Should be unreachable, but satisfies type checkers
            raise last_exc  # type: ignore[misc]

        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# HTTP response → exception mapper
# (call this right after requests.get / session.get)
# ---------------------------------------------------------------------------

def raise_for_status_with_context(response: requests.Response) -> None:
    """
    Inspect an HTTP response and raise an appropriate typed exception.

    - 429 → RateLimitError  (retryable)
    - 5xx → ServerError     (retryable)
    - 4xx → ExtractionError (non-retryable — bug in our request)
    - 2xx → no-op
    """
    code = response.status_code

    if code == 429:
        raise RateLimitError(
            f"HTTP 429 Too Many Requests from {response.url}"
        )
    if 500 <= code < 600:
        raise ServerError(
            f"HTTP {code} Server Error from {response.url}: {response.text[:200]}"
        )
    if 400 <= code < 500:
        raise ExtractionError(
            f"HTTP {code} Client Error from {response.url} — check request params."
        )
    # 2xx / 3xx: fine