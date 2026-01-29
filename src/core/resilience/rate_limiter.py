"""
Token bucket rate limiter for API throttling.

Prevents hitting external API rate limits by controlling request throughput.
Uses token bucket algorithm for smooth rate limiting with burst capacity.

How Token Bucket Works:
- Bucket holds N tokens (burst capacity)
- Tokens refill at R tokens/second (rate)
- Each request consumes 1 token
- If no tokens available, request waits

Usage:
    # Decorator style (recommended)
    limiter = RateLimiter(calls_per_second=10)

    @rate_limited(limiter)
    async def api_call():
        ...

    # Manual style
    async with limiter.acquire():
        result = await make_request()

    # Or
    await limiter.acquire()
    result = await make_request()

Configuration via Environment:
    CLAIMX_API_RATE_LIMIT_ENABLED=true
    CLAIMX_API_RATE_LIMIT_PER_SECOND=10
"""

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import wraps
from typing import Callable, Optional, Protocol, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class MetricsCollector(Protocol):
    """Protocol for metrics collection (optional dependency)."""

    def increment_counter(self, name: str, labels: Optional[dict] = None) -> None: ...
    def observe_histogram(self, name: str, value: float, labels: Optional[dict] = None) -> None: ...


@dataclass
class RateLimiterConfig:
    """Configuration for rate limiter behavior."""

    # Maximum requests per second
    calls_per_second: float = 10.0

    # Maximum burst capacity (tokens that can accumulate)
    # If None, defaults to calls_per_second (allows 1 second of burst)
    burst_capacity: Optional[float] = None

    # Enable/disable rate limiting (for testing or gradual rollout)
    enabled: bool = True

    # Name for logging and metrics
    name: str = "rate_limiter"


# Standard configs for different APIs

# ClaimX API - conservative default, can be tuned per environment
CLAIMX_API_RATE_CONFIG = RateLimiterConfig(
    calls_per_second=float(os.getenv("CLAIMX_API_RATE_LIMIT_PER_SECOND", "10")),
    burst_capacity=None,  # 1 second burst
    enabled=os.getenv("CLAIMX_API_RATE_LIMIT_ENABLED", "false").lower() == "true",
    name="claimx_api",
)

# External downloads - more aggressive since files are larger
EXTERNAL_DOWNLOAD_RATE_CONFIG = RateLimiterConfig(
    calls_per_second=float(os.getenv("EXTERNAL_DOWNLOAD_RATE_LIMIT_PER_SECOND", "5")),
    burst_capacity=None,
    enabled=os.getenv("EXTERNAL_DOWNLOAD_RATE_LIMIT_ENABLED", "false").lower() == "true",
    name="external_download",
)


class RateLimiter:
    """
    Token bucket rate limiter for async operations.

    Thread-safe and async-safe rate limiter that controls throughput
    to prevent hitting external API rate limits.

    Attributes:
        config: Configuration for rate limiting behavior
        _tokens: Current token count (protected by _lock)
        _last_update: Last time tokens were refilled
        _lock: Asyncio lock for thread safety
        _metrics: Optional metrics collector
    """

    def __init__(
        self,
        config: Optional[RateLimiterConfig] = None,
        calls_per_second: Optional[float] = None,
        enabled: Optional[bool] = None,
        metrics: Optional[MetricsCollector] = None,
    ):
        """
        Initialize rate limiter.

        Args:
            config: Full configuration object (preferred)
            calls_per_second: Shorthand for config.calls_per_second
            enabled: Shorthand for config.enabled
            metrics: Optional metrics collector
        """
        # Allow shorthand initialization
        if config is None:
            config = RateLimiterConfig(
                calls_per_second=calls_per_second or 10.0,
                enabled=enabled if enabled is not None else True,
            )
        elif calls_per_second is not None or enabled is not None:
            # Override config values if provided
            config = RateLimiterConfig(
                calls_per_second=calls_per_second or config.calls_per_second,
                burst_capacity=config.burst_capacity,
                enabled=enabled if enabled is not None else config.enabled,
                name=config.name,
            )

        self.config = config
        self._rate = config.calls_per_second
        self._burst_capacity = config.burst_capacity or config.calls_per_second
        self._tokens = self._burst_capacity  # Start with full bucket
        self._last_update = time.monotonic()
        self._lock = asyncio.Lock()
        self._metrics = metrics

        if not config.enabled:
            logger.info(
                f"Rate limiter '{config.name}' initialized but DISABLED",
                extra={"rate_limiter": config.name},
            )
        else:
            logger.info(
                f"Rate limiter '{config.name}' initialized",
                extra={
                    "rate_limiter": config.name,
                    "calls_per_second": config.calls_per_second,
                    "burst_capacity": self._burst_capacity,
                },
            )

    async def acquire(self, tokens: float = 1.0) -> None:
        """
        Acquire tokens from the bucket (blocking if necessary).

        Args:
            tokens: Number of tokens to acquire (default 1.0)

        Raises:
            ValueError: If tokens requested exceeds burst capacity
        """
        # Skip rate limiting if disabled
        if not self.config.enabled:
            return

        if tokens > self._burst_capacity:
            raise ValueError(
                f"Requested tokens ({tokens}) exceeds burst capacity ({self._burst_capacity})"
            )

        async with self._lock:
            # Refill tokens based on elapsed time
            now = time.monotonic()
            elapsed = now - self._last_update

            # Add tokens for elapsed time (up to burst capacity)
            self._tokens = min(self._burst_capacity, self._tokens + elapsed * self._rate)
            self._last_update = now

            # If not enough tokens, calculate wait time
            if self._tokens < tokens:
                deficit = tokens - self._tokens
                wait_time = deficit / self._rate

                logger.debug(
                    f"Rate limit reached for '{self.config.name}', waiting {wait_time:.3f}s",
                    extra={
                        "rate_limiter": self.config.name,
                        "wait_seconds": wait_time,
                        "tokens_available": self._tokens,
                        "tokens_requested": tokens,
                    },
                )

                if self._metrics:
                    self._metrics.increment_counter(
                        "rate_limiter_wait_total",
                        labels={"limiter": self.config.name},
                    )
                    self._metrics.observe_histogram(
                        "rate_limiter_wait_seconds",
                        wait_time,
                        labels={"limiter": self.config.name},
                    )

                # Wait for tokens to refill
                await asyncio.sleep(wait_time)

                # After waiting, we have exactly the tokens we need
                self._tokens = 0
                self._last_update = time.monotonic()
            else:
                # Enough tokens available, consume them
                self._tokens -= tokens

            if self._metrics:
                self._metrics.increment_counter(
                    "rate_limiter_acquire_total",
                    labels={"limiter": self.config.name},
                )

    @asynccontextmanager
    async def acquire_context(self, tokens: float = 1.0):
        """
        Context manager for rate limiting.

        Usage:
            async with limiter.acquire_context():
                await make_request()
        """
        await self.acquire(tokens)
        yield

    def get_stats(self) -> dict:
        """
        Get current rate limiter statistics.

        Returns:
            Dict with current token count and configuration
        """
        return {
            "name": self.config.name,
            "enabled": self.config.enabled,
            "calls_per_second": self._rate,
            "burst_capacity": self._burst_capacity,
            "tokens_available": self._tokens,
            "last_update": self._last_update,
        }


def rate_limited(limiter: RateLimiter, tokens: float = 1.0):
    """
    Decorator to apply rate limiting to async functions.

    Args:
        limiter: RateLimiter instance to use
        tokens: Number of tokens to consume per call

    Usage:
        limiter = RateLimiter(calls_per_second=10)

        @rate_limited(limiter)
        async def api_call():
            return await make_request()
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            await limiter.acquire(tokens)
            return await func(*args, **kwargs)

        return wrapper

    return decorator


# Global registry for rate limiters (similar to circuit breakers)
_rate_limiters: dict[str, RateLimiter] = {}


def get_rate_limiter(
    name: str,
    config: Optional[RateLimiterConfig] = None,
    metrics: Optional[MetricsCollector] = None,
) -> RateLimiter:
    """
    Get or create a named rate limiter (singleton pattern).

    Args:
        name: Unique identifier for this rate limiter
        config: Configuration (only used on first call)
        metrics: Optional metrics collector

    Returns:
        RateLimiter instance for the given name

    Usage:
        limiter = get_rate_limiter("claimx_api", CLAIMX_API_RATE_CONFIG)
    """
    if name not in _rate_limiters:
        if config is None:
            config = RateLimiterConfig(name=name)
        _rate_limiters[name] = RateLimiter(config=config, metrics=metrics)
    return _rate_limiters[name]


__all__ = [
    "RateLimiter",
    "RateLimiterConfig",
    "rate_limited",
    "get_rate_limiter",
    "CLAIMX_API_RATE_CONFIG",
    "EXTERNAL_DOWNLOAD_RATE_CONFIG",
]
