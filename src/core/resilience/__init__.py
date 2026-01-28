"""
Resilience patterns module.

Provides fault tolerance primitives for distributed systems.

Components:
    - CircuitBreaker: State machine (closed/open/half-open)
    - @circuit_protected: Decorator for circuit breaker protection
    - RetryConfig: Exponential backoff configuration
    - @with_retry decorator: Retry with jitter
    - RateLimiter: Token bucket rate limiting
    - @rate_limited: Decorator for rate limiting
    - Standard configs: KAFKA_CIRCUIT_CONFIG, KUSTO_CIRCUIT_CONFIG, etc.
"""

from .circuit_breaker import (
    CLAIMX_API_CIRCUIT_CONFIG,
    EXTERNAL_DOWNLOAD_CIRCUIT_CONFIG,
    KAFKA_CIRCUIT_CONFIG,
    KUSTO_CIRCUIT_CONFIG,
    ONELAKE_CIRCUIT_CONFIG,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitOpenError,
    CircuitState,
    CircuitStats,
    circuit_protected,
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
    RetryStats,
    with_retry,
)

__all__ = [
    # Circuit Breaker
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitOpenError",
    "CircuitState",
    "CircuitStats",
    "circuit_protected",
    "get_circuit_breaker",
    # Rate Limiter
    "RateLimiter",
    "RateLimiterConfig",
    "rate_limited",
    "get_rate_limiter",
    # Retry
    "RetryConfig",
    "RetryStats",
    "with_retry",
    "DEFAULT_RETRY",
    "AUTH_RETRY",
    # Circuit Breaker Configs
    "CLAIMX_API_CIRCUIT_CONFIG",
    "EXTERNAL_DOWNLOAD_CIRCUIT_CONFIG",
    "KAFKA_CIRCUIT_CONFIG",
    "KUSTO_CIRCUIT_CONFIG",
    "ONELAKE_CIRCUIT_CONFIG",
    # Rate Limiter Configs
    "CLAIMX_API_RATE_CONFIG",
    "EXTERNAL_DOWNLOAD_RATE_CONFIG",
]
