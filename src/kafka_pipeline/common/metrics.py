"""
Prometheus metrics for Kafka pipeline monitoring.

Provides comprehensive instrumentation for:
- Message production and consumption rates
- Consumer lag monitoring
- Error tracking by category
- Processing time histograms
- Circuit breaker state tracking
"""

from prometheus_client import Counter, Gauge, Histogram

# Message production metrics
messages_produced_total = Counter(
    "kafka_messages_produced_total",
    "Total number of messages produced to Kafka topics",
    ["topic", "status"],  # status: success, error
)

messages_produced_bytes = Counter(
    "kafka_messages_produced_bytes_total",
    "Total bytes of message data produced to Kafka topics",
    ["topic"],
)

# Message consumption metrics
messages_consumed_total = Counter(
    "kafka_messages_consumed_total",
    "Total number of messages consumed from Kafka topics",
    ["topic", "consumer_group", "status"],  # status: success, error
)

messages_consumed_bytes = Counter(
    "kafka_messages_consumed_bytes_total",
    "Total bytes of message data consumed from Kafka topics",
    ["topic", "consumer_group"],
)

# Consumer lag tracking
consumer_lag = Gauge(
    "kafka_consumer_lag",
    "Current lag in messages for consumer partitions",
    ["topic", "partition", "consumer_group"],
)

consumer_offset = Gauge(
    "kafka_consumer_offset",
    "Current offset position for consumer partitions",
    ["topic", "partition", "consumer_group"],
)

# Error tracking by category
processing_errors_total = Counter(
    "kafka_processing_errors_total",
    "Total number of message processing errors by category",
    ["topic", "consumer_group", "error_category"],
)

producer_errors_total = Counter(
    "kafka_producer_errors_total",
    "Total number of producer errors",
    ["topic", "error_type"],
)

# Processing time metrics
message_processing_duration_seconds = Histogram(
    "kafka_message_processing_duration_seconds",
    "Time spent processing individual messages",
    ["topic", "consumer_group"],
    buckets=(
        0.005,
        0.01,
        0.025,
        0.05,
        0.1,
        0.25,
        0.5,
        1.0,
        2.5,
        5.0,
        10.0,
        30.0,
        60.0,
    ),  # From 5ms to 60s
)

batch_processing_duration_seconds = Histogram(
    "kafka_batch_processing_duration_seconds",
    "Time spent processing message batches",
    ["topic"],
    buckets=(0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0),  # From 100ms to 2min
)

# Circuit breaker metrics
circuit_breaker_state = Gauge(
    "kafka_circuit_breaker_state",
    "Circuit breaker state (0=closed, 1=open, 2=half-open)",
    ["component"],  # component: producer, consumer
)

circuit_breaker_failures = Counter(
    "kafka_circuit_breaker_failures_total",
    "Total number of circuit breaker failures",
    ["component"],
)

# Connection health metrics
kafka_connection_status = Gauge(
    "kafka_connection_status",
    "Kafka connection status (1=connected, 0=disconnected)",
    ["component"],  # component: producer, consumer
)

# Partition assignment metrics
consumer_assigned_partitions = Gauge(
    "kafka_consumer_assigned_partitions",
    "Number of partitions assigned to this consumer",
    ["consumer_group"],
)

# Download concurrency metrics (WP-313)
downloads_concurrent = Gauge(
    "kafka_downloads_concurrent",
    "Number of downloads currently in progress",
    ["worker"],
)

downloads_batch_size = Gauge(
    "kafka_downloads_batch_size",
    "Size of the current download batch being processed",
    ["worker"],
)

uploads_concurrent = Gauge(
    "kafka_uploads_concurrent",
    "Number of uploads currently in progress",
    ["worker"],
)

# Delta Lake write metrics
delta_writes_total = Counter(
    "delta_writes_total",
    "Total number of Delta Lake write operations",
    ["table", "status"],  # status: success, error
)

delta_events_written_total = Counter(
    "delta_events_written_total",
    "Total number of events written to Delta tables",
    ["table"],
)

delta_write_duration_seconds = Histogram(
    "delta_write_duration_seconds",
    "Time spent writing to Delta Lake tables",
    ["table"],
    buckets=(0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0),  # From 100ms to 2min
)

# ClaimX API metrics
claimx_api_requests_total = Counter(
    "claimx_api_requests_total",
    "Total number of requests to ClaimX API",
    ["method", "endpoint", "status"],  # status: success, error
)

claimx_api_request_duration_seconds = Histogram(
    "claimx_api_request_duration_seconds",
    "Time spent waiting for ClaimX API responses",
    ["method", "endpoint"],
    buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)

# ClaimX Business Logic metrics
claim_processing_seconds = Histogram(
    "claim_processing_seconds",
    "Time spent processing claim artifacts",
    ["step"],  # step: download, enrichment, upload
    buckets=(0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0),
)

claim_media_bytes_total = Counter(
    "claim_media_bytes_total",
    "Total bytes of media processed",
    ["type"],  # type: image, video, document
)

# =============================================================================
# Event Ingestion Metrics (WP1)
# =============================================================================
event_ingestion_total = Counter(
    "kafka_events_ingested_total",
    "Total events ingested",
    ["domain", "status"],  # domain: claimx/xact, status: success/parse_error/validation_error
)

event_ingestion_duration_seconds = Histogram(
    "kafka_event_ingestion_duration_seconds",
    "Time to ingest and process event",
    ["domain"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)

event_tasks_produced_total = Counter(
    "kafka_event_tasks_produced_total",
    "Total downstream tasks produced from events",
    ["domain", "task_type"],  # task_type: download_task, enrichment_task
)

# =============================================================================
# OneLake Storage Metrics (WP2)
# =============================================================================
onelake_operations_total = Counter(
    "onelake_operations_total",
    "Total OneLake operations",
    ["operation", "status"],  # operation: upload/download/delete, status: success/error
)

onelake_operation_duration_seconds = Histogram(
    "onelake_operation_duration_seconds",
    "Duration of OneLake operations",
    ["operation"],
    buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0),
)

onelake_bytes_transferred_total = Counter(
    "onelake_bytes_transferred_total",
    "Total bytes transferred to/from OneLake",
    ["operation"],  # upload/download
)

onelake_operation_errors_total = Counter(
    "onelake_operation_errors_total",
    "Total OneLake operation errors by type",
    ["operation", "error_type"],  # error_type: timeout/auth/not_found/unknown
)

# =============================================================================
# Retry Mechanism Metrics (WP6)
# =============================================================================
retry_attempts_total = Counter(
    "kafka_retry_attempts_total",
    "Total retry attempts by domain and error category",
    ["domain", "error_category"],  # error_category: transient/auth/circuit_open/unknown
)

retry_exhausted_total = Counter(
    "kafka_retry_exhausted_total",
    "Total retries exhausted (sent to DLQ after max retries)",
    ["domain", "error_category"],
)

dlq_messages_total = Counter(
    "kafka_dlq_messages_total",
    "Total messages sent to dead-letter queue",
    ["domain", "reason"],  # reason: exhausted/permanent/error
)

retry_delay_seconds = Histogram(
    "kafka_retry_delay_seconds",
    "Retry delay distribution",
    ["domain"],
    buckets=(60, 120, 300, 600, 1200, 2400, 3600),  # 1m to 1h
)


def record_event_ingested(domain: str, status: str = "success") -> None:
    """
    Record an event ingestion.

    Args:
        domain: Domain identifier (e.g., "claimx", "xact")
        status: Ingestion status (success, parse_error, validation_error)
    """
    event_ingestion_total.labels(domain=domain, status=status).inc()


def record_event_task_produced(domain: str, task_type: str) -> None:
    """
    Record a downstream task produced from event.

    Args:
        domain: Domain identifier (e.g., "claimx", "xact")
        task_type: Type of task produced (download_task, enrichment_task)
    """
    event_tasks_produced_total.labels(domain=domain, task_type=task_type).inc()


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
    onelake_operations_total.labels(operation=operation, status=status).inc()
    onelake_operation_duration_seconds.labels(operation=operation).observe(duration)
    if bytes_transferred > 0:
        onelake_bytes_transferred_total.labels(operation=operation).inc(bytes_transferred)


def record_onelake_error(operation: str, error_type: str) -> None:
    """
    Record a OneLake operation error.

    Args:
        operation: Operation type (upload, download, delete, exists)
        error_type: Error category (timeout, auth, not_found, unknown)
    """
    onelake_operation_errors_total.labels(operation=operation, error_type=error_type).inc()


def record_retry_attempt(domain: str, error_category: str, delay_seconds: int = 0) -> None:
    """
    Record a retry attempt.

    Args:
        domain: Domain identifier (e.g., "claimx", "xact")
        error_category: Error category (transient, auth, circuit_open, unknown)
        delay_seconds: Delay before retry in seconds
    """
    retry_attempts_total.labels(domain=domain, error_category=error_category).inc()
    if delay_seconds > 0:
        retry_delay_seconds.labels(domain=domain).observe(delay_seconds)


def record_retry_exhausted(domain: str, error_category: str) -> None:
    """
    Record when retries are exhausted (sent to DLQ).

    Args:
        domain: Domain identifier (e.g., "claimx", "xact")
        error_category: Error category that exhausted retries
    """
    retry_exhausted_total.labels(domain=domain, error_category=error_category).inc()


def record_dlq_message(domain: str, reason: str) -> None:
    """
    Record a message sent to dead-letter queue.

    Args:
        domain: Domain identifier (e.g., "claimx", "xact")
        reason: Reason for DLQ (exhausted, permanent, error)
    """
    dlq_messages_total.labels(domain=domain, reason=reason).inc()


def record_message_produced(topic: str, message_bytes: int, success: bool = True) -> None:
    """
    Record a message production event.

    Args:
        topic: Kafka topic name
        message_bytes: Size of the message in bytes
        success: Whether the production was successful
    """
    status = "success" if success else "error"
    messages_produced_total.labels(topic=topic, status=status).inc()
    if success:
        messages_produced_bytes.labels(topic=topic).inc(message_bytes)


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
    messages_consumed_total.labels(
        topic=topic, consumer_group=consumer_group, status=status
    ).inc()
    if success:
        messages_consumed_bytes.labels(topic=topic, consumer_group=consumer_group).inc(
            message_bytes
        )


def record_processing_error(
    topic: str, consumer_group: str, error_category: str
) -> None:
    """
    Record a message processing error.

    Args:
        topic: Kafka topic name
        consumer_group: Consumer group ID
        error_category: Error category (transient, permanent, auth, etc.)
    """
    processing_errors_total.labels(
        topic=topic, consumer_group=consumer_group, error_category=error_category
    ).inc()


def record_producer_error(topic: str, error_type: str) -> None:
    """
    Record a producer error.

    Args:
        topic: Kafka topic name
        error_type: Type of error (e.g., timeout, connection_error)
    """
    producer_errors_total.labels(topic=topic, error_type=error_type).inc()


def update_consumer_lag(
    topic: str, partition: int, consumer_group: str, lag: int
) -> None:
    """
    Update consumer lag gauge.

    Args:
        topic: Kafka topic name
        partition: Partition number
        consumer_group: Consumer group ID
        lag: Number of messages behind the high watermark
    """
    consumer_lag.labels(
        topic=topic, partition=str(partition), consumer_group=consumer_group
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
    consumer_offset.labels(
        topic=topic, partition=str(partition), consumer_group=consumer_group
    ).set(offset)


def update_circuit_breaker_state(component: str, state: int) -> None:
    """
    Update circuit breaker state gauge.

    Args:
        component: Component name (producer, consumer)
        state: Circuit state (0=closed, 1=open, 2=half-open)
    """
    circuit_breaker_state.labels(component=component).set(state)


def record_circuit_breaker_failure(component: str) -> None:
    """
    Record a circuit breaker failure.

    Args:
        component: Component name (producer, consumer)
    """
    circuit_breaker_failures.labels(component=component).inc()


def update_connection_status(component: str, connected: bool) -> None:
    """
    Update Kafka connection status.

    Args:
        component: Component name (producer, consumer)
        connected: Whether the component is connected
    """
    kafka_connection_status.labels(component=component).set(1 if connected else 0)


def update_assigned_partitions(consumer_group: str, count: int) -> None:
    """
    Update number of assigned partitions.

    Args:
        consumer_group: Consumer group ID
        count: Number of partitions assigned
    """
    consumer_assigned_partitions.labels(consumer_group=consumer_group).set(count)


def record_delta_write(table: str, event_count: int, success: bool = True) -> None:
    """
    Record a Delta Lake write operation.

    Args:
        table: Delta table name (e.g., xact_events, xact_attachments)
        event_count: Number of events written
        success: Whether the write was successful
    """
    status = "success" if success else "error"
    delta_writes_total.labels(table=table, status=status).inc()
    if success:
        delta_events_written_total.labels(table=table).inc(event_count)


def update_downloads_concurrent(worker: str, count: int) -> None:
    """
    Update the number of concurrent downloads in progress.

    Args:
        worker: Worker identifier (e.g., "download_worker")
        count: Number of downloads currently in progress
    """
    downloads_concurrent.labels(worker=worker).set(count)


def update_downloads_batch_size(worker: str, size: int) -> None:
    """
    Update the current download batch size.

    Args:
        worker: Worker identifier (e.g., "download_worker")
        size: Number of messages in the current batch
    """
    downloads_batch_size.labels(worker=worker).set(size)


def update_uploads_concurrent(worker: str, count: int) -> None:
    """
    Update the number of concurrent uploads in progress.

    Args:
        worker: Worker identifier (e.g., "upload_worker")
        count: Number of uploads currently in progress
    """
    uploads_concurrent.labels(worker=worker).set(count)


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
    "delta_writes_total",
    "delta_events_written_total",
    "delta_write_duration_seconds",
    # Event ingestion metrics (WP1)
    "event_ingestion_total",
    "event_ingestion_duration_seconds",
    "event_tasks_produced_total",
    "record_event_ingested",
    "record_event_task_produced",
    # OneLake metrics (WP2)
    "onelake_operations_total",
    "onelake_operation_duration_seconds",
    "onelake_bytes_transferred_total",
    "onelake_operation_errors_total",
    # Retry mechanism metrics (WP6)
    "retry_attempts_total",
    "retry_exhausted_total",
    "dlq_messages_total",
    "retry_delay_seconds",
    "record_retry_attempt",
    "record_retry_exhausted",
    "record_dlq_message",
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
    "record_delta_write",
    # OneLake helper functions
    "record_onelake_operation",
    "record_onelake_error",
    "claimx_api_requests_total",
    "claimx_api_request_duration_seconds",
    "claim_processing_seconds",
    "claim_media_bytes_total",
    "update_uploads_concurrent",
    "uploads_concurrent",
]
