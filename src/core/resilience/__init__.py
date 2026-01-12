"""
Resilience patterns module.

Provides fault tolerance primitives for distributed systems.

Components:
    - CircuitBreaker: State machine (closed/open/half-open)
    - @circuit_protected: Decorator for circuit breaker protection
    - RetryConfig: Exponential backoff configuration
    - @with_retry decorator: Retry with jitter
    - RetryBudget: Retry amplification prevention
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
from .retry import (
    AUTH_RETRY,
    DEFAULT_RETRY,
    RetryBudget,
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
    # Retry
    "RetryConfig",
    "RetryStats",
    "RetryBudget",
    "with_retry",
    "DEFAULT_RETRY",
    "AUTH_RETRY",
    # Circuit Breaker Configs
    "CLAIMX_API_CIRCUIT_CONFIG",
    "EXTERNAL_DOWNLOAD_CIRCUIT_CONFIG",
    "KAFKA_CIRCUIT_CONFIG",
    "KUSTO_CIRCUIT_CONFIG",
    "ONELAKE_CIRCUIT_CONFIG",
]
