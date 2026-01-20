"""
Prometheus metrics for Kafka pipeline monitoring.

Provides comprehensive instrumentation for:
- Message production and consumption rates
- Consumer lag monitoring
- Error tracking by category
- Processing time histograms
- Circuit breaker state tracking

Metrics are fully optional:
- If prometheus-client is not available, gracefully degrades to no-op metrics
- All helper functions continue to work (but do nothing when unavailable)
"""

import logging
from typing import Dict, Tuple, Optional, Any

logger = logging.getLogger(__name__)

# Lazy-loaded prometheus client module
_prometheus_client: Optional[Any] = None
_metrics_available = False


# =============================================================================
# No-Op Metric Classes (when prometheus-client unavailable)
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
            name,
            description,
            labelnames=labelnames or [],
            registry=registry
        )
    except Exception as e:
        logger.warning(f"Failed to create counter {name}: {e}")
        return NoOpCounter()


def _create_gauge(name: str, description: str, labelnames=None):
    """Create a Gauge or NoOpGauge."""
    if not _ensure_prometheus():
        return NoOpGauge()

    registry = _get_registry()
    if registry is None:
        return NoOpGauge()

    try:
        return _prometheus_client.Gauge(
            name,
            description,
            labelnames=labelnames or [],
            registry=registry
        )
    except Exception as e:
        logger.warning(f"Failed to create gauge {name}: {e}")
        return NoOpGauge()


def _create_histogram(name: str, description: str, labelnames=None):
    """Create a Histogram or NoOpHistogram."""
    if not _ensure_prometheus():
        return NoOpHistogram()

    registry = _get_registry()
    if registry is None:
        return NoOpHistogram()

    try:
        return _prometheus_client.Histogram(
            name,
            description,
            labelnames=labelnames or [],
            registry=registry
        )
    except Exception as e:
        logger.warning(f"Failed to create histogram {name}: {e}")
        return NoOpHistogram()


# =============================================================================
# Message Production Metrics
# =============================================================================

messages_produced_counter = _create_counter(
    "kafka_messages_produced_total",
    "Total number of messages produced to Kafka topics",
    labelnames=["topic", "status"]
)

messages_produced_bytes_counter = _create_counter(
    "kafka_messages_produced_bytes_total",
    "Total bytes of message data produced to Kafka topics",
    labelnames=["topic"]
)


# =============================================================================
# Message Consumption Metrics
# =============================================================================

messages_consumed_counter = _create_counter(
    "kafka_messages_consumed_total",
    "Total number of messages consumed from Kafka topics",
    labelnames=["topic", "consumer_group", "status"]
)

messages_consumed_bytes_counter = _create_counter(
    "kafka_messages_consumed_bytes_total",
    "Total bytes of message data consumed from Kafka topics",
    labelnames=["topic", "consumer_group"]
)


# =============================================================================
# Consumer Lag Tracking (Gauges)
# =============================================================================

consumer_lag_gauge = _create_gauge(
    "kafka_consumer_lag",
    "Current lag in messages for consumer partitions",
    labelnames=["topic", "partition", "consumer_group"]
)

consumer_offset_gauge = _create_gauge(
    "kafka_consumer_offset",
    "Current offset position for consumer partitions",
    labelnames=["topic", "partition", "consumer_group"]
)


# =============================================================================
# Error Tracking
# =============================================================================

processing_errors_counter = _create_counter(
    "kafka_processing_errors_total",
    "Total number of message processing errors by category",
    labelnames=["topic", "consumer_group", "error_category"]
)

producer_errors_counter = _create_counter(
    "kafka_producer_errors_total",
    "Total number of producer errors",
    labelnames=["topic", "error_type"]
)


# =============================================================================
# Processing Time Metrics
# =============================================================================

message_processing_duration_histogram = _create_histogram(
    "kafka_message_processing_duration_seconds",
    "Time spent processing individual messages",
    labelnames=["topic", "consumer_group"]
)

batch_processing_duration_histogram = _create_histogram(
    "kafka_batch_processing_duration_seconds",
    "Time spent processing message batches",
    labelnames=["topic", "consumer_group"]
)


# =============================================================================
# Circuit Breaker Metrics
# =============================================================================

circuit_breaker_state_gauge = _create_gauge(
    "kafka_circuit_breaker_state",
    "Circuit breaker state (0=closed, 1=open, 2=half-open)",
    labelnames=["component"]
)

circuit_breaker_failures_counter = _create_counter(
    "kafka_circuit_breaker_failures_total",
    "Total number of circuit breaker failures",
    labelnames=["component"]
)


# =============================================================================
# Connection Health Metrics
# =============================================================================

kafka_connection_status_gauge = _create_gauge(
    "kafka_connection_status",
    "Kafka connection status (1=connected, 0=disconnected)",
    labelnames=["component"]
)


# =============================================================================
# Partition Assignment Metrics
# =============================================================================

consumer_assigned_partitions_gauge = _create_gauge(
    "kafka_consumer_assigned_partitions",
    "Number of partitions assigned to this consumer",
    labelnames=["consumer_group"]
)


# =============================================================================
# Download/Upload Concurrency Metrics
# =============================================================================

downloads_concurrent_gauge = _create_gauge(
    "kafka_downloads_concurrent",
    "Number of downloads currently in progress",
    labelnames=["worker"]
)

downloads_batch_size_gauge = _create_gauge(
    "kafka_downloads_batch_size",
    "Size of the current download batch being processed",
    labelnames=["worker"]
)

uploads_concurrent_gauge = _create_gauge(
    "kafka_uploads_concurrent",
    "Number of uploads currently in progress",
    labelnames=["worker"]
)


# =============================================================================
# Delta Lake Write Metrics
# =============================================================================

delta_writes_counter = _create_counter(
    "delta_writes_total",
    "Total number of Delta Lake write operations",
    labelnames=["table", "status"]
)

delta_events_written_counter = _create_counter(
    "delta_events_written_total",
    "Total number of events written to Delta tables",
    labelnames=["table"]
)

delta_write_duration_histogram = _create_histogram(
    "delta_write_duration_seconds",
    "Time spent writing to Delta Lake tables",
    labelnames=["table"]
)


# =============================================================================
# ClaimX API Metrics
# =============================================================================

claimx_api_requests_counter = _create_counter(
    "claimx_api_requests_total",
    "Total number of requests to ClaimX API",
    labelnames=["endpoint", "status"]
)

claimx_api_request_duration_histogram = _create_histogram(
    "claimx_api_request_duration_seconds",
    "Time spent waiting for ClaimX API responses",
    labelnames=["endpoint"]
)


# =============================================================================
# ClaimX Business Logic Metrics
# =============================================================================

claim_processing_histogram = _create_histogram(
    "claim_processing_duration_seconds",
    "Time spent processing claim artifacts",
    labelnames=["claim_type"]
)

claim_media_bytes_counter = _create_counter(
    "claim_media_bytes_total",
    "Total bytes of media processed",
    labelnames=["media_type"]
)

claimx_handler_duration_histogram = _create_histogram(
    "claimx_handler_duration_seconds",
    "Time spent processing events by handler",
    labelnames=["handler", "event_type"]
)

claimx_handler_events_counter = _create_counter(
    "claimx_handler_events_total",
    "Total events processed by handler",
    labelnames=["handler", "event_type", "status"]
)


# =============================================================================
# Event Ingestion Metrics
# =============================================================================

event_ingestion_counter = _create_counter(
    "kafka_events_ingested_total",
    "Total events ingested",
    labelnames=["domain", "status"]
)

event_ingestion_duration_histogram = _create_histogram(
    "kafka_event_ingestion_duration_seconds",
    "Time to ingest and process event",
    labelnames=["domain"]
)

event_tasks_produced_counter = _create_counter(
    "kafka_event_tasks_produced_total",
    "Total downstream tasks produced from events",
    labelnames=["domain", "task_type"]
)


# =============================================================================
# OneLake Storage Metrics
# =============================================================================

onelake_operations_counter = _create_counter(
    "onelake_operations_total",
    "Total OneLake operations",
    labelnames=["operation", "status"]
)

onelake_operation_duration_histogram = _create_histogram(
    "onelake_operation_duration_seconds",
    "Duration of OneLake operations",
    labelnames=["operation"]
)

onelake_bytes_transferred_counter = _create_counter(
    "onelake_bytes_transferred_total",
    "Total bytes transferred to/from OneLake",
    labelnames=["operation"]
)

onelake_operation_errors_counter = _create_counter(
    "onelake_operation_errors_total",
    "Total OneLake operation errors by type",
    labelnames=["operation", "error_type"]
)


# =============================================================================
# Retry Mechanism Metrics
# =============================================================================

retry_attempts_counter = _create_counter(
    "kafka_retry_attempts_total",
    "Total retry attempts by domain and error category",
    labelnames=["domain", "worker_type", "error_category"]
)

retry_exhausted_counter = _create_counter(
    "kafka_retry_exhausted_total",
    "Total retries exhausted (sent to DLQ after max retries)",
    labelnames=["domain", "error_category"]
)

dlq_messages_counter = _create_counter(
    "kafka_dlq_messages_total",
    "Total messages sent to dead-letter queue",
    labelnames=["domain", "reason"]
)

retry_delay_histogram = _create_histogram(
    "kafka_retry_delay_seconds",
    "Retry delay distribution",
    labelnames=["domain"]
)

messages_dlq_permanent_counter = _create_counter(
    "kafka_messages_dlq_permanent_total",
    "Total messages sent to DLQ due to permanent errors",
    labelnames=["topic", "consumer_group"]
)

messages_dlq_transient_counter = _create_counter(
    "kafka_messages_dlq_transient_total",
    "Total messages sent to DLQ due to transient errors (retry exhausted)",
    labelnames=["topic", "consumer_group"]
)


# =============================================================================
# Consumer Shutdown Metrics
# =============================================================================

consumer_shutdown_duration_histogram = _create_histogram(
    "kafka_consumer_shutdown_duration_seconds",
    "Time taken to shutdown Kafka consumer",
    labelnames=["consumer_group", "status"]
)

consumer_shutdown_timeout_counter = _create_counter(
    "kafka_consumer_shutdown_timeout_total",
    "Total number of consumer shutdown timeouts",
    labelnames=["consumer_group"]
)

consumer_shutdown_error_counter = _create_counter(
    "kafka_consumer_shutdown_error_total",
    "Total number of consumer shutdown errors",
    labelnames=["consumer_group", "error_type"]
)


# =============================================================================
# Helper Functions
# =============================================================================


def record_event_ingested(domain: str, status: str = "success") -> None:
    """
    Record an event ingestion.

    Args:
        domain: Domain identifier (e.g., "claimx", "xact")
        status: Ingestion status (success, parse_error, validation_error)
    """
    event_ingestion_counter.labels(domain=domain, status=status).inc()


def record_event_task_produced(domain: str, task_type: str) -> None:
    """
    Record a downstream task produced from event.

    Args:
        domain: Domain identifier (e.g., "claimx", "xact")
        task_type: Type of task produced (download_task, enrichment_task)
    """
    event_tasks_produced_counter.labels(domain=domain, task_type=task_type).inc()


def record_onelake_operation(
    operation: str, status: str, duration: float, bytes_transferred: int = 0
) -> None:
    """
    Record a OneLake operation.

    Args:
        operation: Operation type (upload, download, delete, exists)
        status: Operation status (success, error)
        duration: Operation duration in seconds
        bytes_transferred: Number of bytes transferred (for upload/download)
    """
    onelake_operations_counter.labels(operation=operation, status=status).inc()
    onelake_operation_duration_histogram.labels(operation=operation).observe(duration)
    if bytes_transferred > 0:
        onelake_bytes_transferred_counter.labels(operation=operation).inc(bytes_transferred)


def record_onelake_error(operation: str, error_type: str) -> None:
    """
    Record a OneLake operation error.

    Args:
        operation: Operation type (upload, download, delete, exists)
        error_type: Error category (timeout, auth, not_found, unknown)
    """
    onelake_operation_errors_counter.labels(operation=operation, error_type=error_type).inc()


def record_retry_attempt(
    domain: str,
    worker_type: str,
    error_category: str,
    delay_seconds: int = 0
) -> None:
    """
    Record a retry attempt.

    Args:
        domain: Domain identifier (e.g., "claimx", "xact")
        worker_type: Type of worker for observability (e.g., "download_worker", "delta_worker")
        error_category: Error category (transient, auth, circuit_open, unknown)
        delay_seconds: Delay before retry in seconds
    """
    retry_attempts_counter.labels(
        domain=domain,
        worker_type=worker_type,
        error_category=error_category
    ).inc()
    if delay_seconds > 0:
        retry_delay_histogram.labels(domain=domain).observe(delay_seconds)


def record_retry_exhausted(domain: str, error_category: str) -> None:
    """
    Record when retries are exhausted (sent to DLQ).

    Args:
        domain: Domain identifier (e.g., "claimx", "xact")
        error_category: Error category that exhausted retries
    """
    retry_exhausted_counter.labels(domain=domain, error_category=error_category).inc()


def record_dlq_message(domain: str, reason: str) -> None:
    """
    Record a message sent to dead-letter queue.

    Args:
        domain: Domain identifier (e.g., "claimx", "xact")
        reason: Reason for DLQ (exhausted, permanent, error)
    """
    dlq_messages_counter.labels(domain=domain, reason=reason).inc()


def record_dlq_permanent(topic: str, consumer_group: str) -> None:
    """
    Record a message sent to DLQ due to permanent error.

    Args:
        topic: Kafka topic name
        consumer_group: Consumer group ID
    """
    messages_dlq_permanent_counter.labels(topic=topic, consumer_group=consumer_group).inc()


def record_dlq_transient(topic: str, consumer_group: str) -> None:
    """
    Record a message sent to DLQ due to transient error (retry exhausted).

    Args:
        topic: Kafka topic name
        consumer_group: Consumer group ID
    """
    messages_dlq_transient_counter.labels(topic=topic, consumer_group=consumer_group).inc()


def record_message_produced(topic: str, message_bytes: int, success: bool = True) -> None:
    """
    Record a message production event.

    Args:
        topic: Kafka topic name
        message_bytes: Size of the message in bytes
        success: Whether the production was successful
    """
    status = "success" if success else "error"
    messages_produced_counter.labels(topic=topic, status=status).inc()
    if success:
        messages_produced_bytes_counter.labels(topic=topic).inc(message_bytes)


def record_message_consumed(
    topic: str, consumer_group: str, message_bytes: int, success: bool = True
) -> None:
    """
    Record a message consumption event.

    Args:
        topic: Kafka topic name
        consumer_group: Consumer group ID
        message_bytes: Size of the message in bytes
        success: Whether the consumption was successful
    """
    status = "success" if success else "error"
    messages_consumed_counter.labels(
        topic=topic,
        consumer_group=consumer_group,
        status=status
    ).inc()
    if success:
        messages_consumed_bytes_counter.labels(
            topic=topic,
            consumer_group=consumer_group
        ).inc(message_bytes)


def record_processing_error(topic: str, consumer_group: str, error_category: str) -> None:
    """
    Record a message processing error.

    Args:
        topic: Kafka topic name
        consumer_group: Consumer group ID
        error_category: Error category (transient, permanent, auth, etc.)
    """
    processing_errors_counter.labels(
        topic=topic,
        consumer_group=consumer_group,
        error_category=error_category
    ).inc()


def record_producer_error(topic: str, error_type: str) -> None:
    """
    Record a producer error.

    Args:
        topic: Kafka topic name
        error_type: Type of error (e.g., timeout, connection_error)
    """
    producer_errors_counter.labels(topic=topic, error_type=error_type).inc()


def update_consumer_lag(topic: str, partition: int, consumer_group: str, lag: int) -> None:
    """
    Update consumer lag gauge.

    Args:
        topic: Kafka topic name
        partition: Partition number
        consumer_group: Consumer group ID
        lag: Number of messages behind the high watermark
    """
    consumer_lag_gauge.labels(
        topic=topic,
        partition=str(partition),
        consumer_group=consumer_group
    ).set(lag)


def update_consumer_offset(
    topic: str, partition: int, consumer_group: str, offset: int
) -> None:
    """
    Update consumer offset gauge.

    Args:
        topic: Kafka topic name
        partition: Partition number
        consumer_group: Consumer group ID
        offset: Current offset position
    """
    consumer_offset_gauge.labels(
        topic=topic,
        partition=str(partition),
        consumer_group=consumer_group
    ).set(offset)


def update_circuit_breaker_state(component: str, state: int) -> None:
    """
    Update circuit breaker state gauge.

    Args:
        component: Component name (producer, consumer)
        state: Circuit state (0=closed, 1=open, 2=half-open)
    """
    circuit_breaker_state_gauge.labels(component=component).set(state)


def record_circuit_breaker_failure(component: str) -> None:
    """
    Record a circuit breaker failure.

    Args:
        component: Component name (producer, consumer)
    """
    circuit_breaker_failures_counter.labels(component=component).inc()


def update_connection_status(component: str, connected: bool) -> None:
    """
    Update Kafka connection status.

    Args:
        component: Component name (producer, consumer)
        connected: Whether the component is connected
    """
    kafka_connection_status_gauge.labels(component=component).set(1 if connected else 0)


def update_assigned_partitions(consumer_group: str, count: int) -> None:
    """
    Update number of assigned partitions.

    Args:
        consumer_group: Consumer group ID
        count: Number of partitions assigned
    """
    consumer_assigned_partitions_gauge.labels(consumer_group=consumer_group).set(count)


def record_delta_write(table: str, event_count: int, success: bool = True) -> None:
    """
    Record a Delta Lake write operation.

    Args:
        table: Delta table name (e.g., xact_events, xact_attachments)
        event_count: Number of events written
        success: Whether the write was successful
    """
    status = "success" if success else "error"
    delta_writes_counter.labels(table=table, status=status).inc()
    if success:
        delta_events_written_counter.labels(table=table).inc(event_count)


def update_downloads_concurrent(worker: str, count: int) -> None:
    """
    Update the number of concurrent downloads in progress.

    Args:
        worker: Worker identifier (e.g., "download_worker")
        count: Number of downloads currently in progress
    """
    downloads_concurrent_gauge.labels(worker=worker).set(count)


def update_downloads_batch_size(worker: str, size: int) -> None:
    """
    Update the current download batch size.

    Args:
        worker: Worker identifier (e.g., "download_worker")
        size: Number of messages in the current batch
    """
    downloads_batch_size_gauge.labels(worker=worker).set(size)


def update_uploads_concurrent(worker: str, count: int) -> None:
    """
    Update the number of concurrent uploads in progress.

    Args:
        worker: Worker identifier (e.g., "upload_worker")
        count: Number of uploads currently in progress
    """
    uploads_concurrent_gauge.labels(worker=worker).set(count)


def record_consumer_shutdown(
    consumer_group: str, duration: float, status: str = "success"
) -> None:
    """
    Record a consumer shutdown event.

    Args:
        consumer_group: Consumer group ID
        duration: Shutdown duration in seconds
        status: Shutdown status (success, timeout, error)
    """
    consumer_shutdown_duration_histogram.labels(
        consumer_group=consumer_group,
        status=status
    ).observe(duration)
    if status == "timeout":
        consumer_shutdown_timeout_counter.labels(consumer_group=consumer_group).inc()


def record_consumer_shutdown_error(consumer_group: str, error_type: str) -> None:
    """
    Record a consumer shutdown error.

    Args:
        consumer_group: Consumer group ID
        error_type: Type of error (timeout, force_close, unknown)
    """
    consumer_shutdown_error_counter.labels(
        consumer_group=consumer_group,
        error_type=error_type
    ).inc()


# =============================================================================
# Legacy Compatibility - Direct metric exports
# =============================================================================

# For backward compatibility, export metrics with old names
messages_produced_total = messages_produced_counter
messages_produced_bytes = messages_produced_bytes_counter
messages_consumed_total = messages_consumed_counter
messages_consumed_bytes = messages_consumed_bytes_counter
consumer_lag = consumer_lag_gauge
consumer_offset = consumer_offset_gauge
processing_errors_total = processing_errors_counter
producer_errors_total = producer_errors_counter
message_processing_duration_seconds = message_processing_duration_histogram
batch_processing_duration_seconds = batch_processing_duration_histogram
circuit_breaker_state = circuit_breaker_state_gauge
circuit_breaker_failures = circuit_breaker_failures_counter
kafka_connection_status = kafka_connection_status_gauge
consumer_assigned_partitions = consumer_assigned_partitions_gauge
downloads_concurrent = downloads_concurrent_gauge
downloads_batch_size = downloads_batch_size_gauge
uploads_concurrent = uploads_concurrent_gauge
delta_writes_total = delta_writes_counter
delta_events_written_total = delta_events_written_counter
delta_write_duration_seconds = delta_write_duration_histogram
claimx_api_requests_total = claimx_api_requests_counter
claimx_api_request_duration_seconds = claimx_api_request_duration_histogram
claim_processing_seconds = claim_processing_histogram
claim_media_bytes_total = claim_media_bytes_counter
claimx_handler_duration_seconds = claimx_handler_duration_histogram
claimx_handler_events_total = claimx_handler_events_counter
event_ingestion_total = event_ingestion_counter
event_ingestion_duration_seconds = event_ingestion_duration_histogram
event_tasks_produced_total = event_tasks_produced_counter
onelake_operations_total = onelake_operations_counter
onelake_operation_duration_seconds = onelake_operation_duration_histogram
onelake_bytes_transferred_total = onelake_bytes_transferred_counter
onelake_operation_errors_total = onelake_operation_errors_counter
retry_attempts_total = retry_attempts_counter
retry_exhausted_total = retry_exhausted_counter
dlq_messages_total = dlq_messages_counter
retry_delay_seconds = retry_delay_histogram
messages_dlq_permanent = messages_dlq_permanent_counter
messages_dlq_transient = messages_dlq_transient_counter
consumer_shutdown_duration_seconds = consumer_shutdown_duration_histogram
consumer_shutdown_timeout_total = consumer_shutdown_timeout_counter
consumer_shutdown_error_total = consumer_shutdown_error_counter


__all__ = [
    # Metrics
    "messages_produced_total",
    "messages_produced_bytes",
    "messages_consumed_total",
    "messages_consumed_bytes",
    "consumer_lag",
    "consumer_offset",
    "processing_errors_total",
    "producer_errors_total",
    "message_processing_duration_seconds",
    "batch_processing_duration_seconds",
    "circuit_breaker_state",
    "circuit_breaker_failures",
    "kafka_connection_status",
    "consumer_assigned_partitions",
    "downloads_concurrent",
    "downloads_batch_size",
    "uploads_concurrent",
    "delta_writes_total",
    "delta_events_written_total",
    "delta_write_duration_seconds",
    # Event ingestion metrics
    "event_ingestion_total",
    "event_ingestion_duration_seconds",
    "event_tasks_produced_total",
    "record_event_ingested",
    "record_event_task_produced",
    # OneLake metrics
    "onelake_operations_total",
    "onelake_operation_duration_seconds",
    "onelake_bytes_transferred_total",
    "onelake_operation_errors_total",
    # Retry mechanism metrics
    "retry_attempts_total",
    "retry_exhausted_total",
    "dlq_messages_total",
    "retry_delay_seconds",
    "record_retry_attempt",
    "record_retry_exhausted",
    "record_dlq_message",
    # DLQ routing metrics
    "messages_dlq_permanent",
    "messages_dlq_transient",
    "record_dlq_permanent",
    "record_dlq_transient",
    # Consumer shutdown metrics
    "consumer_shutdown_duration_seconds",
    "consumer_shutdown_timeout_total",
    "consumer_shutdown_error_total",
    "record_consumer_shutdown",
    "record_consumer_shutdown_error",
    # Helper functions
    "record_message_produced",
    "record_message_consumed",
    "record_processing_error",
    "record_producer_error",
    "update_consumer_lag",
    "update_consumer_offset",
    "update_circuit_breaker_state",
    "record_circuit_breaker_failure",
    "update_connection_status",
    "update_assigned_partitions",
    "update_downloads_concurrent",
    "update_downloads_batch_size",
    "update_uploads_concurrent",
    "record_delta_write",
    # OneLake helper functions
    "record_onelake_operation",
    "record_onelake_error",
    # ClaimX metrics
    "claimx_api_requests_total",
    "claimx_api_request_duration_seconds",
    "claim_processing_seconds",
    "claim_media_bytes_total",
    "claimx_handler_duration_seconds",
    "claimx_handler_events_total",
]
