# blast_assessment/shared/retry.py

import asyncio
import functools
import random
import time
from typing import Callable, Optional, Tuple, Type, Union

from shared.logging import get_logger

logger = get_logger(__name__)


class RetryExhausted(Exception):
    def __init__(self, message: str, last_exception: Exception):
        super().__init__(message)
        self.last_exception = last_exception


def _compute_backoff(attempt: int, base_delay: float, max_delay: float, jitter: bool) -> float:
    delay = min(base_delay * (2 ** attempt), max_delay)
    if jitter:
        delay = delay * (0.5 + random.random() * 0.5)
    return delay


def retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: bool = True,
    retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
    non_retryable_exceptions: Tuple[Type[Exception], ...] = (),
    on_retry: Optional[Callable] = None,
):
    """
    Decorator for synchronous functions with exponential backoff and jitter.

    Args:
        max_attempts: Total number of attempts (including first try).
        base_delay: Base delay in seconds before first retry.
        max_delay: Maximum delay cap in seconds.
        jitter: Add random jitter to avoid thundering herd.
        retryable_exceptions: Only retry on these exception types.
        non_retryable_exceptions: Never retry on these (takes precedence).
        on_retry: Optional callback(attempt, exception, delay) called before each retry.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except non_retryable_exceptions as e:
                    logger.warning(
                        "Non-retryable exception, aborting",
                        extra={"func": func.__name__, "attempt": attempt, "error": str(e)},
                    )
                    raise
                except retryable_exceptions as e:
                    last_exc = e
                    if attempt + 1 == max_attempts:
                        break
                    delay = _compute_backoff(attempt, base_delay, max_delay, jitter)
                    logger.warning(
                        "Retryable exception, will retry",
                        extra={
                            "func": func.__name__,
                            "attempt": attempt + 1,
                            "max_attempts": max_attempts,
                            "retry_in_seconds": round(delay, 2),
                            "error": str(e),
                        },
                    )
                    if on_retry:
                        on_retry(attempt, e, delay)
                    time.sleep(delay)

            raise RetryExhausted(
                f"{func.__name__} failed after {max_attempts} attempts", last_exception=last_exc
            )

        return wrapper

    return decorator


def async_retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: bool = True,
    retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
    non_retryable_exceptions: Tuple[Type[Exception], ...] = (),
    on_retry: Optional[Callable] = None,
):
    """Async variant of the retry decorator."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except non_retryable_exceptions as e:
                    logger.warning(
                        "Non-retryable exception, aborting",
                        extra={"func": func.__name__, "attempt": attempt, "error": str(e)},
                    )
                    raise
                except retryable_exceptions as e:
                    last_exc = e
                    if attempt + 1 == max_attempts:
                        break
                    delay = _compute_backoff(attempt, base_delay, max_delay, jitter)
                    logger.warning(
                        "Retryable exception, will retry",
                        extra={
                            "func": func.__name__,
                            "attempt": attempt + 1,
                            "max_attempts": max_attempts,
                            "retry_in_seconds": round(delay, 2),
                            "error": str(e),
                        },
                    )
                    if on_retry:
                        if asyncio.iscoroutinefunction(on_retry):
                            await on_retry(attempt, e, delay)
                        else:
                            on_retry(attempt, e, delay)
                    await asyncio.sleep(delay)

            raise RetryExhausted(
                f"{func.__name__} failed after {max_attempts} attempts", last_exception=last_exc
            )

        return wrapper

    return decorator


class RateLimiter:
    """
    Token bucket rate limiter for third-party API calls.
    Thread-safe for single-process use.
    """

    def __init__(self, rate: float, burst: int = 1):
        """
        Args:
            rate: Tokens added per second.
            burst: Maximum token bucket size.
        """
        self.rate = rate
        self.burst = burst
        self._tokens = float(burst)
        self._last_refill = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
        self._last_refill = now

    def acquire(self, tokens: int = 1) -> float:
        """
        Block until tokens are available.
        Returns the wait time in seconds.
        """
        self._refill()
        if self._tokens >= tokens:
            self._tokens -= tokens
            return 0.0

        # How long until we have enough tokens
        wait = (tokens - self._tokens) / self.rate
        time.sleep(wait)
        self._tokens = 0.0
        return wait

    async def async_acquire(self, tokens: int = 1) -> float:
        """Async variant."""
        self._refill()
        if self._tokens >= tokens:
            self._tokens -= tokens
            return 0.0

        wait = (tokens - self._tokens) / self.rate
        await asyncio.sleep(wait)
        self._tokens = 0.0
        return wait