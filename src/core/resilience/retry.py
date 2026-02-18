"""
Retry utilities with exception-aware handling.

Uses the exception hierarchy to make intelligent retry decisions:
- Transient errors: retry with exponential backoff
- Auth errors: refresh credentials, then retry
- Permanent errors: fail immediately (no retry)
- Circuit open: fail immediately with retry_after hint
"""

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from functools import wraps

# Import exception handling utilities for retry logic
from core.errors.exceptions import (
    PipelineError,
    ThrottlingError,
    classify_exception,
    wrap_exception,
)

# Import ErrorCategory from core.types to avoid circular dependency
from core.types import ErrorCategory

logger = logging.getLogger(__name__)


def _extract_error_category(wrapped: Exception) -> str:
    """Return a string error category from a wrapped exception."""
    cat = wrapped.category if isinstance(wrapped, PipelineError) else classify_exception(wrapped)
    return cat.value if hasattr(cat, "value") else str(cat)


def _log_retry_failure(
    func_name: str,
    wrapped: Exception,
    e: Exception,
    error_category: str,
    config: "RetryConfig",
) -> bool:
    """Log permanent-error or max-retries-exhausted and return True if permanent."""
    error_type = type(wrapped).__name__
    if isinstance(wrapped, PipelineError) and not wrapped.is_retryable:
        logger.warning(
            "Permanent error for %s, not retrying: %s",
            func_name,
            str(e)[:200],
            exc_info=e,
            extra={
                "operation": func_name,
                "error_type": error_type,
                "error_category": error_category,
                "error_message": str(e)[:200],
            },
        )
        return True

    logger.error(
        "Max retries exhausted for %s: %s",
        func_name,
        str(e)[:200],
        exc_info=e,
        extra={
            "operation": func_name,
            "error_type": error_type,
            "error_category": error_category,
            "max_attempts": config.max_attempts,
            "error_message": str(e)[:200],
        },
    )
    return False


def _log_retry_attempt(
    func_name: str,
    attempt: int,
    config: "RetryConfig",
    error_category: str,
    delay: float,
    e: Exception,
    wrapped: Exception,
) -> None:
    """Build log extras and emit the retry-attempt warning."""
    log_extras: dict[str, object] = {
        "operation": func_name,
        "attempt": attempt + 1,
        "max_attempts": config.max_attempts,
        "error_category": error_category,
        "delay_seconds": round(delay, 2),
        "error_message": str(e)[:200],
    }

    using_server_delay = (
        config.respect_retry_after
        and isinstance(wrapped, ThrottlingError)
        and wrapped.retry_after is not None
    )

    if using_server_delay:
        log_extras["server_retry_after"] = wrapped.retry_after
        log_extras["delay_source"] = "server"
        log_message = "Retryable error for %s, will retry (using server-provided delay)"
    else:
        log_extras["delay_source"] = "exponential_backoff"
        log_message = "Retryable error for %s, will retry"

    logger.warning(log_message, func_name, extra=log_extras)


def _safe_invoke_on_retry(
    on_retry: Callable[[Exception, int, float], None],
    wrapped: Exception,
    attempt: int,
    delay: float,
    func_name: str,
) -> None:
    """Call the on_retry callback, swallowing and logging any errors."""
    try:
        on_retry(wrapped, attempt, delay)
    except Exception as cb_err:
        logger.warning(
            "Error in on_retry callback for %s: %s",
            func_name,
            str(cb_err)[:100],
            extra={
                "operation": func_name,
                "callback_error": str(cb_err)[:100],
            },
        )


def _log_recovery(func_name: str, attempt: int, config: "RetryConfig") -> None:
    """Log successful recovery after retries."""
    logger.info(
        "Retry succeeded for %s after %d attempts",
        func_name,
        attempt + 1,
        extra={
            "operation": func_name,
            "attempt": attempt + 1,
            "total_attempts": config.max_attempts,
        },
    )


def _handle_exception(
    e: Exception,
    attempt: int,
    config: "RetryConfig",
    func_name: str,
    on_retry: Callable[[Exception, int, float], None] | None,
    wrap_errors: bool,
) -> tuple[float, Exception, bool]:
    """Process an exception in the retry loop.

    Returns (delay, wrapped_exception, needs_auth_refresh).
    Raises wrapped or original exception if the error should not be retried.
    """
    wrapped = wrap_exception(e) if wrap_errors and not isinstance(e, PipelineError) else e
    error_category = _extract_error_category(wrapped)

    # Detect auth refresh need
    needs_auth = isinstance(wrapped, PipelineError) and wrapped.should_refresh_auth
    if needs_auth:
        logger.info(
            "Auth error detected for %s, refreshing credentials",
            func_name,
            extra={
                "operation": func_name,
                "error_category": error_category,
            },
        )

    # Check if we should retry
    if not config.should_retry(wrapped, attempt):
        _log_retry_failure(func_name, wrapped, e, error_category, config)
        if wrap_errors:
            raise wrapped from e
        raise

    delay = config.get_delay(attempt, wrapped)
    _log_retry_attempt(func_name, attempt, config, error_category, delay, e, wrapped)

    if on_retry:
        _safe_invoke_on_retry(on_retry, wrapped, attempt, delay, func_name)

    return delay, wrapped, needs_auth


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""

    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 30.0
    exponential_base: float = 2.0

    # If True, don't retry permanent errors even if max_attempts > 0
    respect_permanent: bool = True

    # If True, use retry_after from ThrottlingError when available
    respect_retry_after: bool = True

    # Optional set of exception types to always retry (overrides classification)
    always_retry: set[type[Exception]] = field(default_factory=set)

    # Optional set of exception types to never retry (overrides classification)
    never_retry: set[type[Exception]] = field(default_factory=set)

    def __post_init__(self):
        """Ensure proper types from YAML/env vars."""
        self.max_attempts = int(self.max_attempts)
        self.base_delay = float(self.base_delay)
        self.max_delay = float(self.max_delay)
        self.exponential_base = float(self.exponential_base)
        # Keep boolean if already bool, otherwise convert
        # (bool('false') would be True, so we need this check)
        self.respect_permanent = (
            self.respect_permanent
            if isinstance(self.respect_permanent, bool)
            else bool(self.respect_permanent)
        )
        self.respect_retry_after = (
            self.respect_retry_after
            if isinstance(self.respect_retry_after, bool)
            else bool(self.respect_retry_after)
        )

    def get_delay(self, attempt: int, error: Exception | None = None) -> float:
        """
        Calculate delay with equal jitter to prevent thundering herd.

        Args:
            attempt: 0-indexed attempt number
            error: Optional exception to check for retry_after

        Returns:
            Delay in seconds
        """
        # Check for explicit retry_after (e.g., from 429 response)
        if self.respect_retry_after and isinstance(error, ThrottlingError) and error.retry_after:
            return min(error.retry_after, self.max_delay)

        # Calculate base exponential delay
        base_delay = self.base_delay * (self.exponential_base**attempt)

        # Apply equal jitter: half fixed, half random
        # This spreads retry attempts over time to prevent synchronized retries
        jitter = random.uniform(0, base_delay / 2)
        delay = (base_delay / 2) + jitter

        # Cap at max_delay
        return min(delay, self.max_delay)

    def should_retry(self, error: Exception, attempt: int) -> bool:
        """
        Determine if error should be retried.

        Args:
            error: The exception that occurred
            attempt: 0-indexed current attempt

        Returns:
            True if should retry
        """
        # Check attempt count first
        if attempt >= self.max_attempts - 1:
            return False

        # Check never_retry list
        if self.never_retry and isinstance(error, tuple(self.never_retry)):
            return False

        # Check always_retry list
        if self.always_retry and isinstance(error, tuple(self.always_retry)):
            return True

        return self._is_retryable_error(error)

    def _is_retryable_error(self, error: Exception) -> bool:
        """Classify whether an error is retryable based on exception type."""
        if isinstance(error, PipelineError):
            if self.respect_permanent and not error.is_retryable:
                return False
            return error.is_retryable

        category = classify_exception(error)
        if self.respect_permanent and category == ErrorCategory.PERMANENT:
            return False

        return category in (
            ErrorCategory.TRANSIENT,
            ErrorCategory.AUTH,
            ErrorCategory.UNKNOWN,
        )


# Default configurations
DEFAULT_RETRY = RetryConfig(max_attempts=3, base_delay=1.0)
AUTH_RETRY = RetryConfig(max_attempts=2, base_delay=0.5)


def with_retry(
    config: RetryConfig | None = None,
    on_auth_error: Callable[[], None] | None = None,
    on_retry: Callable[[Exception, int, float], None] | None = None,
    wrap_errors: bool = True,
):
    """
    Decorator for retrying functions with intelligent backoff.

    Args:
        config: Retry configuration (defaults to DEFAULT_RETRY)
        on_auth_error: Callback when auth error detected (e.g., clear token cache)
        on_retry: Callback before each retry (error, attempt, delay)
        wrap_errors: If True, wrap unknown exceptions in PipelineError

    Usage:
        @with_retry(on_auth_error=lambda: auth.clear_cache())
        def fetch_data():
            ...

        @with_retry(config=AGGRESSIVE_RETRY)
        def download_file():
            ...
    """
    if config is None:
        config = DEFAULT_RETRY

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error: Exception | None = None

            for attempt in range(config.max_attempts):
                try:
                    result = func(*args, **kwargs)
                    if attempt > 0:
                        _log_recovery(func.__name__, attempt, config)
                    return result
                except Exception as e:
                    last_error = e
                    delay, _, needs_auth = _handle_exception(
                        e, attempt, config, func.__name__, on_retry, wrap_errors
                    )
                    if needs_auth and on_auth_error:
                        on_auth_error()
                    time.sleep(delay)

            # Should not reach here, but just in case
            if last_error:
                raise last_error

        return wrapper

    return decorator


def with_retry_async(
    config: RetryConfig | None = None,
    on_auth_error: Callable[[], None] | Callable[[], Awaitable[None]] | None = None,
    on_retry: Callable[[Exception, int, float], None] | None = None,
    wrap_errors: bool = True,
):
    """
    Decorator for retrying async functions with intelligent backoff.

    Async version of with_retry. Uses asyncio.sleep() and supports async callbacks.
    Reuses all error classification logic from sync version.
    """
    if config is None:
        config = DEFAULT_RETRY

    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_error: Exception | None = None

            for attempt in range(config.max_attempts):
                try:
                    result = await func(*args, **kwargs)
                    if attempt > 0:
                        _log_recovery(func.__name__, attempt, config)
                    return result
                except Exception as e:
                    last_error = e
                    delay, _, needs_auth = _handle_exception(
                        e, attempt, config, func.__name__, on_retry, wrap_errors
                    )
                    if needs_auth and on_auth_error:
                        if asyncio.iscoroutinefunction(on_auth_error):
                            await on_auth_error()
                        else:
                            on_auth_error()
                    await asyncio.sleep(delay)

            if last_error:
                raise last_error

        return wrapper

    return decorator


__all__ = [
    "RetryConfig",
    "with_retry",
    "with_retry_async",
    "DEFAULT_RETRY",
    "AUTH_RETRY",
]
