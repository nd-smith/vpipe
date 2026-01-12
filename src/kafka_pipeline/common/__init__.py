"""Common infrastructure shared across all pipeline domains.

This package provides domain-agnostic infrastructure including:
- BaseKafkaConsumer: Base class for Kafka consumers
- BaseKafkaProducer: Base class for Kafka producers
- Logging utilities
- Exception classes and error classification
- Resilience patterns (circuit breaker, retry)

Import classes directly from submodules to avoid loading heavy dependencies:
    from kafka_pipeline.common.consumer import BaseKafkaConsumer
    from kafka_pipeline.common.producer import BaseKafkaProducer
    from core.logging import get_logger, log_with_context
    from core.types import ErrorCategory
"""

# Don't import concrete implementations here to avoid loading
# heavy dependencies (aiokafka, aiohttp, etc.) at package import time.
# Users should import directly from submodules.

__all__: list[str] = []
