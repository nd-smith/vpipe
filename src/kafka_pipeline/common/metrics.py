"""
Prometheus metrics for pipeline monitoring.

Focused on essential metrics:
- Message production and consumption counts
- Consumer lag and offsets
- Error rates
- Delta write tracking
- Connection health

If prometheus-client is not available, metrics gracefully degrade to no-ops.
"""

import logging
from typing import Optional, Any

logger = logging.getLogger(__name__)

# Lazy-loaded prometheus client module
_prometheus_client: Optional[Any] = None
_metrics_available = False


# =============================================================================
# No-Op Metric Classes
# =============================================================================


class NoOpCounter:
    """No-op counter when prometheus-client is unavailable."""

    def inc(self, amount=1):
        pass

    def labels(self, **kwargs):
        return self


class NoOpGauge:
    """No-op gauge when prometheus-client is unavailable."""

    def set(self, value):
        pass

    def inc(self, amount=1):
        pass

    def dec(self, amount=1):
        pass

    def labels(self, **kwargs):
        return self


class NoOpHistogram:
    """No-op histogram when prometheus-client is unavailable."""

    def observe(self, value):
        pass

    def labels(self, **kwargs):
        return self


# =============================================================================
# Lazy Initialization
# =============================================================================


def _ensure_prometheus():
    """Ensure prometheus_client is loaded (lazy import)."""
    global _prometheus_client, _metrics_available

    if _prometheus_client is not None:
        return True

    try:
        import prometheus_client

        _prometheus_client = prometheus_client
        _metrics_available = True
        return True
    except ImportError:
        logger.debug("prometheus-client not available, using no-op metrics")
        return False


def _get_registry():
    """Get the Prometheus registry from telemetry module."""
    try:
        from kafka_pipeline.common.telemetry import get_prometheus_registry

        return get_prometheus_registry()
    except ImportError:
        return None


def _create_counter(name: str, description: str, labelnames=None):
    """Create a Counter or NoOpCounter."""
    if not _ensure_prometheus():
        return NoOpCounter()

    registry = _get_registry()
    if registry is None:
        return NoOpCounter()

    try:
        return _prometheus_client.Counter(
            name, description, labelnames=labelnames or [], registry=registry
        )
    except ValueError:
        return registry._collector_to_names.get((name,), NoOpCounter())


def _create_gauge(name: str, description: str, labelnames=None):
    """Create a Gauge or NoOpGauge."""
    if not _ensure_prometheus():
        return NoOpGauge()

    registry = _get_registry()
    if registry is None:
        return NoOpGauge()

    try:
        return _prometheus_client.Gauge(
            name, description, labelnames=labelnames or [], registry=registry
        )
    except ValueError:
        return registry._collector_to_names.get((name,), NoOpGauge())


def _create_histogram(name: str, description: str, labelnames=None, buckets=None):
    """Create a Histogram or NoOpHistogram."""
    if not _ensure_prometheus():
        return NoOpHistogram()

    registry = _get_registry()
    if registry is None:
        return NoOpHistogram()

    try:
        kwargs = {
            "name": name,
            "documentation": description,
            "labelnames": labelnames or [],
            "registry": registry,
        }
        if buckets:
            kwargs["buckets"] = buckets
        return _prometheus_client.Histogram(**kwargs)
    except ValueError:
        return registry._collector_to_names.get((name,), NoOpHistogram())


# =============================================================================
# Core Metrics (10 essential metrics)
# =============================================================================

# Message counts
messages_produced_counter = _create_counter(
    "pipeline_messages_produced_total",
    "Total number of messages produced to topics",
    labelnames=["topic"],
)

messages_consumed_counter = _create_counter(
    "pipeline_messages_consumed_total",
    "Total number of messages consumed from topics",
    labelnames=["topic", "consumer_group"],
)

# Consumer lag monitoring
consumer_lag_gauge = _create_gauge(
    "pipeline_consumer_lag",
    "Current consumer lag (messages behind latest offset)",
    labelnames=["topic", "partition", "consumer_group"],
)

consumer_offset_gauge = _create_gauge(
    "pipeline_consumer_offset",
    "Current consumer offset position",
    labelnames=["topic", "partition", "consumer_group"],
)

# Error tracking
processing_errors_counter = _create_counter(
    "pipeline_processing_errors_total",
    "Total processing errors by error category",
    labelnames=["topic", "consumer_group", "error_category"],
)

producer_errors_counter = _create_counter(
    "pipeline_producer_errors_total",
    "Total producer errors by error type",
    labelnames=["topic", "error_type"],
)

# Delta Lake operations
delta_writes_counter = _create_counter(
    "delta_writes_total", "Total Delta Lake write operations", labelnames=["table", "success"]
)

# DLQ tracking
dlq_messages_counter = _create_counter(
    "pipeline_dlq_messages_total",
    "Total messages sent to dead letter queue",
    labelnames=["domain", "reason"],
)

# Connection health
kafka_connection_status_gauge = _create_gauge(
    "pipeline_connection_status",
    "Pipeline connection status (1=connected, 0=disconnected)",
    labelnames=["component"],
)

# Partition assignment
consumer_assigned_partitions_gauge = _create_gauge(
    "pipeline_consumer_assigned_partitions",
    "Number of partitions assigned to consumer",
    labelnames=["consumer_group"],
)

# Processing duration (keeping one histogram for performance monitoring)
message_processing_duration_seconds = _create_histogram(
    "pipeline_message_processing_duration_seconds",
    "Time spent processing individual messages",
    labelnames=["topic", "consumer_group"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
)

# ClaimX handler metrics
claimx_handler_duration_seconds = _create_histogram(
    "claimx_handler_duration_seconds",
    "Time spent in ClaimX handlers processing events",
    labelnames=["handler_name", "status"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

claimx_handler_events_total = _create_counter(
    "claimx_handler_events_total",
    "Total events processed by ClaimX handlers",
    labelnames=["handler_name", "status"],
)

# Retry scheduler metrics
retry_messages_queued_gauge = _create_gauge(
    "retry_messages_queued", "Current number of messages in retry queue", labelnames=["domain"]
)

retry_messages_routed_counter = _create_counter(
    "retry_messages_routed_total",
    "Total messages routed from retry queue back to target topics",
    labelnames=["domain"],
)

retry_messages_delayed_counter = _create_counter(
    "retry_messages_delayed_total", "Total messages added to retry queue", labelnames=["domain"]
)

retry_messages_exhausted_counter = _create_counter(
    "retry_messages_exhausted_total",
    "Total messages that exhausted max retries (sent to DLQ)",
    labelnames=["domain"],
)


# =============================================================================
# Convenience Functions (minimal set, callers can use metrics directly)
# =============================================================================


def record_message_produced(topic: str, message_bytes: int, success: bool = True) -> None:
    """Record a produced message."""
    messages_produced_counter.labels(topic=topic).inc()
    if not success:
        producer_errors_counter.labels(topic=topic, error_type="send_failed").inc()


def record_message_consumed(
    topic: str, consumer_group: str, message_bytes: int, success: bool = True
) -> None:
    """Record a consumed message."""
    messages_consumed_counter.labels(topic=topic, consumer_group=consumer_group).inc()
    if not success:
        processing_errors_counter.labels(
            topic=topic, consumer_group=consumer_group, error_category="processing_failed"
        ).inc()


def record_processing_error(topic: str, consumer_group: str, error_category: str) -> None:
    """Record a message processing error."""
    processing_errors_counter.labels(
        topic=topic, consumer_group=consumer_group, error_category=error_category
    ).inc()


def record_producer_error(topic: str, error_type: str) -> None:
    """Record a producer error."""
    producer_errors_counter.labels(topic=topic, error_type=error_type).inc()


def update_consumer_lag(topic: str, partition: int, consumer_group: str, lag: int) -> None:
    """Update consumer lag metric."""
    consumer_lag_gauge.labels(
        topic=topic, partition=str(partition), consumer_group=consumer_group
    ).set(lag)


def update_consumer_offset(topic: str, partition: int, consumer_group: str, offset: int) -> None:
    """Update consumer offset metric."""
    consumer_offset_gauge.labels(
        topic=topic, partition=str(partition), consumer_group=consumer_group
    ).set(offset)


def update_connection_status(component: str, connected: bool) -> None:
    """Update pipeline connection status."""
    kafka_connection_status_gauge.labels(component=component).set(1 if connected else 0)


def update_assigned_partitions(consumer_group: str, count: int) -> None:
    """Update assigned partition count."""
    consumer_assigned_partitions_gauge.labels(consumer_group=consumer_group).set(count)


def record_delta_write(table: str, event_count: int, success: bool = True) -> None:
    """Record a Delta Lake write operation."""
    delta_writes_counter.labels(table=table, success="true" if success else "false").inc()


def record_dlq_message(domain: str, reason: str) -> None:
    """Record a message sent to DLQ."""
    dlq_messages_counter.labels(domain=domain, reason=reason).inc()


__all__ = [
    # Metrics
    "messages_produced_counter",
    "messages_consumed_counter",
    "consumer_lag_gauge",
    "consumer_offset_gauge",
    "processing_errors_counter",
    "producer_errors_counter",
    "delta_writes_counter",
    "dlq_messages_counter",
    "kafka_connection_status_gauge",
    "consumer_assigned_partitions_gauge",
    "message_processing_duration_seconds",
    "claimx_handler_duration_seconds",
    "claimx_handler_events_total",
    # Helper functions
    "record_message_produced",
    "record_message_consumed",
    "record_processing_error",
    "record_producer_error",
    "update_consumer_lag",
    "update_consumer_offset",
    "update_connection_status",
    "update_assigned_partitions",
    "record_delta_write",
    "record_dlq_message",
]
