"""
Resilience patterns module.

Provides fault tolerance primitives for distributed systems.

Components:
    - CircuitBreaker: State machine (closed/open/half-open)
    - RetryConfig: Exponential backoff configuration
    - @with_retry decorator: Retry with jitter
    - RateLimiter: Token bucket rate limiting
    - @rate_limited: Decorator for rate limiting
    - Standard configs: KUSTO_CIRCUIT_CONFIG, CLAIMX_API_CIRCUIT_CONFIG, etc.
"""

from .circuit_breaker import (
    CLAIMX_API_CIRCUIT_CONFIG,
    EXTERNAL_DOWNLOAD_CIRCUIT_CONFIG,
    KUSTO_CIRCUIT_CONFIG,
    ONELAKE_CIRCUIT_CONFIG,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitOpenError,
    CircuitState,
    CircuitStats,
    get_circuit_breaker,
)
from .rate_limiter import (
    CLAIMX_API_RATE_CONFIG,
    EXTERNAL_DOWNLOAD_RATE_CONFIG,
    RateLimiter,
    RateLimiterConfig,
    get_rate_limiter,
    rate_limited,
)
from .retry import (
    AUTH_RETRY,
    DEFAULT_RETRY,
    RetryConfig,
    with_retry,
    with_retry_async,
)

__all__ = [
    # Circuit Breaker
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitOpenError",
    "CircuitState",
    "CircuitStats",
    "get_circuit_breaker",
    # Rate Limiter
    "RateLimiter",
    "RateLimiterConfig",
    "rate_limited",
    "get_rate_limiter",
    # Retry
    "RetryConfig",
    "with_retry",
    "with_retry_async",
    "DEFAULT_RETRY",
    "AUTH_RETRY",
    # Circuit Breaker Configs
    "CLAIMX_API_CIRCUIT_CONFIG",
    "EXTERNAL_DOWNLOAD_CIRCUIT_CONFIG",
    "KUSTO_CIRCUIT_CONFIG",
    "ONELAKE_CIRCUIT_CONFIG",
    # Rate Limiter Configs
    "CLAIMX_API_RATE_CONFIG",
    "EXTERNAL_DOWNLOAD_RATE_CONFIG",
]
