"""
Retry handling for Kafka pipeline.

Provides:
- Delta batch retry handler for Delta Lake writes
- Delayed redelivery scheduling and dead-letter queue (DLQ) handling
- Retry decorator with itelligent backoff for transient failures
- Common retry utility functions to reduce handler duplication
"""

from kafka_pipeline.common.retry import retry_utils
from core.resilience.retry import RetryConfig, with_retry, DEFAULT_RETRY, AUTH_RETRY


def __getattr__(name: str):
    # Lazy import: DeltaRetryHandler pulls in producer → aiokafka → gssapi.
    # Eagerly importing it breaks Windows environments that lack Kerberos
    # for Windows (KfW) even when GSSAPI auth is not used.
    if name == "DeltaRetryHandler":
        from kafka_pipeline.common.retry.delta_handler import DeltaRetryHandler

        return DeltaRetryHandler
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "DeltaRetryHandler",
    "retry_utils",
    "RetryConfig",
    "with_retry",
    "DEFAULT_RETRY",
    "AUTH_RETRY",
]
