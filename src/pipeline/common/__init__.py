"""Common infrastructure shared across all pipeline domains.

This package provides domain-agnostic infrastructure including:
- MessageConsumer: Base class for Kafka consumers
- MessageProducer: Base class for Kafka producers
- Logging utilities
- Exception classes and error classification
- Resilience patterns (circuit breaker, retry)

Import classes directly from submodules to avoid loading heavy dependencies:
    from pipeline.common.consumer import MessageConsumer
    from pipeline.common.producer import MessageProducer
    import logging
    from core.types import ErrorCategory
"""

# Don't import concrete implementations here to avoid loading
# heavy dependencies (aiokafka, aiohttp, etc.) at package import time.
# Users should import directly from submodules.

__all__: list[str] = []
