"""
OpenTelemetry metrics for Kafka pipeline monitoring.

Provides comprehensive instrumentation for:
- Message production and consumption rates
- Consumer lag monitoring
- Error tracking by category
- Processing time histograms
- Circuit breaker state tracking
"""

from typing import Dict, Tuple
from opentelemetry import metrics
from opentelemetry.metrics import Observation

meter = metrics.get_meter(__name__)
_gauge_state: Dict[Tuple[str, ...], float] = {}


# =============================================================================
# Message Production Metrics
# =============================================================================

messages_produced_counter = meter.create_counter(
    name="kafka.messages.produced",
    description="Total number of messages produced to Kafka topics",
    unit="1",
)

messages_produced_bytes_counter = meter.create_counter(
    name="kafka.messages.produced.bytes",
    description="Total bytes of message data produced to Kafka topics",
    unit="By",
)


# =============================================================================
# Message Consumption Metrics
# =============================================================================

messages_consumed_counter = meter.create_counter(
    name="kafka.messages.consumed",
    description="Total number of messages consumed from Kafka topics",
    unit="1",
)

messages_consumed_bytes_counter = meter.create_counter(
    name="kafka.messages.consumed.bytes",
    description="Total bytes of message data consumed from Kafka topics",
    unit="By",
)


# =============================================================================
# Consumer Lag Tracking (Observable Gauges)
# =============================================================================

def _observe_consumer_lag(options):
    for (topic, partition, consumer_group), lag in _gauge_state.items():
        if isinstance(lag, (int, float)) and "lag" in str(topic):
            yield Observation(
                lag,
                attributes={
                    "topic": topic.split(":")[1] if ":" in str(topic) else topic,
                    "partition": partition,
                    "consumer_group": consumer_group,
                },
            )


def _observe_consumer_offset(options):
    for (topic, partition, consumer_group), offset in _gauge_state.items():
        if isinstance(offset, (int, float)) and "offset" in str(topic):
            yield Observation(
                offset,
                attributes={
                    "topic": topic.split(":")[1] if ":" in str(topic) else topic,
                    "partition": partition,
                    "consumer_group": consumer_group,
                },
            )


consumer_lag_gauge = meter.create_observable_gauge(
    name="kafka.consumer.lag",
    callbacks=[_observe_consumer_lag],
    description="Current lag in messages for consumer partitions",
    unit="1",
)

consumer_offset_gauge = meter.create_observable_gauge(
    name="kafka.consumer.offset",
    callbacks=[_observe_consumer_offset],
    description="Current offset position for consumer partitions",
    unit="1",
)


# =============================================================================
# Error Tracking
# =============================================================================

processing_errors_counter = meter.create_counter(
    name="kafka.processing.errors",
    description="Total number of message processing errors by category",
    unit="1",
)

producer_errors_counter = meter.create_counter(
    name="kafka.producer.errors",
    description="Total number of producer errors",
    unit="1",
)


# =============================================================================
# Processing Time Metrics
# =============================================================================

message_processing_duration_histogram = meter.create_histogram(
    name="kafka.message.processing.duration",
    description="Time spent processing individual messages",
    unit="s",
)

batch_processing_duration_histogram = meter.create_histogram(
    name="kafka.batch.processing.duration",
    description="Time spent processing message batches",
    unit="s",
)


# =============================================================================
# Circuit Breaker Metrics
# =============================================================================

def _observe_circuit_breaker_state(options):
    for (component,), state in _gauge_state.items():
        if "circuit_breaker" in str(component):
            yield Observation(
                state,
                attributes={"component": component.replace("circuit_breaker:", "")},
            )


circuit_breaker_state_gauge = meter.create_observable_gauge(
    name="kafka.circuit_breaker.state",
    callbacks=[_observe_circuit_breaker_state],
    description="Circuit breaker state (0=closed, 1=open, 2=half-open)",
    unit="1",
)

circuit_breaker_failures_counter = meter.create_counter(
    name="kafka.circuit_breaker.failures",
    description="Total number of circuit breaker failures",
    unit="1",
)


# =============================================================================
# Connection Health Metrics
# =============================================================================

def _observe_connection_status(options):
    """Callback for connection status observable gauge."""
    for (component,), status in _gauge_state.items():
        if "connection" in str(component):
            yield Observation(
                status,
                attributes={"component": component.replace("connection:", "")},
            )


kafka_connection_status_gauge = meter.create_observable_gauge(
    name="kafka.connection.status",
    callbacks=[_observe_connection_status],
    description="Kafka connection status (1=connected, 0=disconnected)",
    unit="1",
)


# =============================================================================
# Partition Assignment Metrics
# =============================================================================

def _observe_assigned_partitions(options):
    """Callback for assigned partitions observable gauge."""
    for (consumer_group,), count in _gauge_state.items():
        if "partitions" in str(consumer_group):
            yield Observation(
                count,
                attributes={"consumer_group": consumer_group.replace("partitions:", "")},
            )


consumer_assigned_partitions_gauge = meter.create_observable_gauge(
    name="kafka.consumer.assigned_partitions",
    callbacks=[_observe_assigned_partitions],
    description="Number of partitions assigned to this consumer",
    unit="1",
)


# =============================================================================
# Download/Upload Concurrency Metrics
# =============================================================================

def _observe_downloads_concurrent(options):
    """Callback for concurrent downloads observable gauge."""
    for (worker,), count in _gauge_state.items():
        if "downloads_concurrent" in str(worker):
            yield Observation(
                count,
                attributes={"worker": worker.replace("downloads_concurrent:", "")},
            )


def _observe_downloads_batch_size(options):
    """Callback for download batch size observable gauge."""
    for (worker,), size in _gauge_state.items():
        if "downloads_batch" in str(worker):
            yield Observation(
                size,
                attributes={"worker": worker.replace("downloads_batch:", "")},
            )


def _observe_uploads_concurrent(options):
    """Callback for concurrent uploads observable gauge."""
    for (worker,), count in _gauge_state.items():
        if "uploads_concurrent" in str(worker):
            yield Observation(
                count,
                attributes={"worker": worker.replace("uploads_concurrent:", "")},
            )


downloads_concurrent_gauge = meter.create_observable_gauge(
    name="kafka.downloads.concurrent",
    callbacks=[_observe_downloads_concurrent],
    description="Number of downloads currently in progress",
    unit="1",
)

downloads_batch_size_gauge = meter.create_observable_gauge(
    name="kafka.downloads.batch_size",
    callbacks=[_observe_downloads_batch_size],
    description="Size of the current download batch being processed",
    unit="1",
)

uploads_concurrent_gauge = meter.create_observable_gauge(
    name="kafka.uploads.concurrent",
    callbacks=[_observe_uploads_concurrent],
    description="Number of uploads currently in progress",
    unit="1",
)


# =============================================================================
# Delta Lake Write Metrics
# =============================================================================

delta_writes_counter = meter.create_counter(
    name="delta.writes",
    description="Total number of Delta Lake write operations",
    unit="1",
)

delta_events_written_counter = meter.create_counter(
    name="delta.events.written",
    description="Total number of events written to Delta tables",
    unit="1",
)

delta_write_duration_histogram = meter.create_histogram(
    name="delta.write.duration",
    description="Time spent writing to Delta Lake tables",
    unit="s",
)


# =============================================================================
# ClaimX API Metrics
# =============================================================================

claimx_api_requests_counter = meter.create_counter(
    name="claimx.api.requests",
    description="Total number of requests to ClaimX API",
    unit="1",
)

claimx_api_request_duration_histogram = meter.create_histogram(
    name="claimx.api.request.duration",
    description="Time spent waiting for ClaimX API responses",
    unit="s",
)


# =============================================================================
# ClaimX Business Logic Metrics
# =============================================================================

claim_processing_histogram = meter.create_histogram(
    name="claim.processing.duration",
    description="Time spent processing claim artifacts",
    unit="s",
)

claim_media_bytes_counter = meter.create_counter(
    name="claim.media.bytes",
    description="Total bytes of media processed",
    unit="By",
)

claimx_handler_duration_histogram = meter.create_histogram(
    name="claimx.handler.duration",
    description="Time spent processing events by handler",
    unit="s",
)

claimx_handler_events_counter = meter.create_counter(
    name="claimx.handler.events",
    description="Total events processed by handler",
    unit="1",
)


# =============================================================================
# Event Ingestion Metrics
# =============================================================================

event_ingestion_counter = meter.create_counter(
    name="kafka.events.ingested",
    description="Total events ingested",
    unit="1",
)

event_ingestion_duration_histogram = meter.create_histogram(
    name="kafka.event.ingestion.duration",
    description="Time to ingest and process event",
    unit="s",
)

event_tasks_produced_counter = meter.create_counter(
    name="kafka.event.tasks.produced",
    description="Total downstream tasks produced from events",
    unit="1",
)


# =============================================================================
# OneLake Storage Metrics
# =============================================================================

onelake_operations_counter = meter.create_counter(
    name="onelake.operations",
    description="Total OneLake operations",
    unit="1",
)

onelake_operation_duration_histogram = meter.create_histogram(
    name="onelake.operation.duration",
    description="Duration of OneLake operations",
    unit="s",
)

onelake_bytes_transferred_counter = meter.create_counter(
    name="onelake.bytes.transferred",
    description="Total bytes transferred to/from OneLake",
    unit="By",
)

onelake_operation_errors_counter = meter.create_counter(
    name="onelake.operation.errors",
    description="Total OneLake operation errors by type",
    unit="1",
)


# =============================================================================
# Retry Mechanism Metrics
# =============================================================================

retry_attempts_counter = meter.create_counter(
    name="kafka.retry.attempts",
    description="Total retry attempts by domain and error category",
    unit="1",
)

retry_exhausted_counter = meter.create_counter(
    name="kafka.retry.exhausted",
    description="Total retries exhausted (sent to DLQ after max retries)",
    unit="1",
)

dlq_messages_counter = meter.create_counter(
    name="kafka.dlq.messages",
    description="Total messages sent to dead-letter queue",
    unit="1",
)

retry_delay_histogram = meter.create_histogram(
    name="kafka.retry.delay",
    description="Retry delay distribution",
    unit="s",
)

# DLQ routing metrics by error category
messages_dlq_permanent_counter = meter.create_counter(
    name="kafka.messages.dlq.permanent",
    description="Total messages sent to DLQ due to permanent errors",
    unit="1",
)

messages_dlq_transient_counter = meter.create_counter(
    name="kafka.messages.dlq.transient",
    description="Total messages sent to DLQ due to transient errors (retry exhausted)",
    unit="1",
)


# =============================================================================
# Consumer Shutdown Metrics
# =============================================================================

consumer_shutdown_duration_histogram = meter.create_histogram(
    name="kafka.consumer.shutdown.duration",
    description="Time taken to shutdown Kafka consumer",
    unit="s",
)

consumer_shutdown_timeout_counter = meter.create_counter(
    name="kafka.consumer.shutdown.timeout",
    description="Total number of consumer shutdown timeouts",
    unit="1",
)

consumer_shutdown_error_counter = meter.create_counter(
    name="kafka.consumer.shutdown.error",
    description="Total number of consumer shutdown errors",
    unit="1",
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
    event_ingestion_counter.add(1, attributes={"domain": domain, "status": status})


def record_event_task_produced(domain: str, task_type: str) -> None:
    """
    Record a downstream task produced from event.

    Args:
        domain: Domain identifier (e.g., "claimx", "xact")
        task_type: Type of task produced (download_task, enrichment_task)
    """
    event_tasks_produced_counter.add(1, attributes={"domain": domain, "task_type": task_type})


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
    onelake_operations_counter.add(
        1, attributes={"operation": operation, "status": status}
    )
    onelake_operation_duration_histogram.record(duration, attributes={"operation": operation})
    if bytes_transferred > 0:
        onelake_bytes_transferred_counter.add(
            bytes_transferred, attributes={"operation": operation}
        )


def record_onelake_error(operation: str, error_type: str) -> None:
    """
    Record a OneLake operation error.

    Args:
        operation: Operation type (upload, download, delete, exists)
        error_type: Error category (timeout, auth, not_found, unknown)
    """
    onelake_operation_errors_counter.add(
        1, attributes={"operation": operation, "error_type": error_type}
    )


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
    retry_attempts_counter.add(
        1,
        attributes={
            "domain": domain,
            "worker_type": worker_type,
            "error_category": error_category,
        }
    )
    if delay_seconds > 0:
        retry_delay_histogram.record(delay_seconds, attributes={"domain": domain})


def record_retry_exhausted(domain: str, error_category: str) -> None:
    """
    Record when retries are exhausted (sent to DLQ).

    Args:
        domain: Domain identifier (e.g., "claimx", "xact")
        error_category: Error category that exhausted retries
    """
    retry_exhausted_counter.add(
        1, attributes={"domain": domain, "error_category": error_category}
    )


def record_dlq_message(domain: str, reason: str) -> None:
    """
    Record a message sent to dead-letter queue.

    Args:
        domain: Domain identifier (e.g., "claimx", "xact")
        reason: Reason for DLQ (exhausted, permanent, error)
    """
    dlq_messages_counter.add(1, attributes={"domain": domain, "reason": reason})


def record_dlq_permanent(topic: str, consumer_group: str) -> None:
    """
    Record a message sent to DLQ due to permanent error.

    Args:
        topic: Kafka topic name
        consumer_group: Consumer group ID
    """
    messages_dlq_permanent_counter.add(
        1, attributes={"topic": topic, "consumer_group": consumer_group}
    )


def record_dlq_transient(topic: str, consumer_group: str) -> None:
    """
    Record a message sent to DLQ due to transient error (retry exhausted).

    Args:
        topic: Kafka topic name
        consumer_group: Consumer group ID
    """
    messages_dlq_transient_counter.add(
        1, attributes={"topic": topic, "consumer_group": consumer_group}
    )


def record_message_produced(topic: str, message_bytes: int, success: bool = True) -> None:
    """
    Record a message production event.

    Args:
        topic: Kafka topic name
        message_bytes: Size of the message in bytes
        success: Whether the production was successful
    """
    status = "success" if success else "error"
    messages_produced_counter.add(1, attributes={"topic": topic, "status": status})
    if success:
        messages_produced_bytes_counter.add(message_bytes, attributes={"topic": topic})


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
    messages_consumed_counter.add(
        1, attributes={"topic": topic, "consumer_group": consumer_group, "status": status}
    )
    if success:
        messages_consumed_bytes_counter.add(
            message_bytes, attributes={"topic": topic, "consumer_group": consumer_group}
        )


def record_processing_error(topic: str, consumer_group: str, error_category: str) -> None:
    """
    Record a message processing error.

    Args:
        topic: Kafka topic name
        consumer_group: Consumer group ID
        error_category: Error category (transient, permanent, auth, etc.)
    """
    processing_errors_counter.add(
        1,
        attributes={
            "topic": topic,
            "consumer_group": consumer_group,
            "error_category": error_category,
        },
    )


def record_producer_error(topic: str, error_type: str) -> None:
    """
    Record a producer error.

    Args:
        topic: Kafka topic name
        error_type: Type of error (e.g., timeout, connection_error)
    """
    producer_errors_counter.add(1, attributes={"topic": topic, "error_type": error_type})


def update_consumer_lag(topic: str, partition: int, consumer_group: str, lag: int) -> None:
    """
    Update consumer lag gauge.

    Args:
        topic: Kafka topic name
        partition: Partition number
        consumer_group: Consumer group ID
        lag: Number of messages behind the high watermark
    """
    # Store state for observable gauge callback
    key = (f"lag:{topic}", str(partition), consumer_group)
    _gauge_state[key] = lag


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
    # Store state for observable gauge callback
    key = (f"offset:{topic}", str(partition), consumer_group)
    _gauge_state[key] = offset


def update_circuit_breaker_state(component: str, state: int) -> None:
    """
    Update circuit breaker state gauge.

    Args:
        component: Component name (producer, consumer)
        state: Circuit state (0=closed, 1=open, 2=half-open)
    """
    # Store state for observable gauge callback
    key = (f"circuit_breaker:{component}",)
    _gauge_state[key] = state


def record_circuit_breaker_failure(component: str) -> None:
    """
    Record a circuit breaker failure.

    Args:
        component: Component name (producer, consumer)
    """
    circuit_breaker_failures_counter.add(1, attributes={"component": component})


def update_connection_status(component: str, connected: bool) -> None:
    """
    Update Kafka connection status.

    Args:
        component: Component name (producer, consumer)
        connected: Whether the component is connected
    """
    # Store state for observable gauge callback
    key = (f"connection:{component}",)
    _gauge_state[key] = 1 if connected else 0


def update_assigned_partitions(consumer_group: str, count: int) -> None:
    """
    Update number of assigned partitions.

    Args:
        consumer_group: Consumer group ID
        count: Number of partitions assigned
    """
    # Store state for observable gauge callback
    key = (f"partitions:{consumer_group}",)
    _gauge_state[key] = count


def record_delta_write(table: str, event_count: int, success: bool = True) -> None:
    """
    Record a Delta Lake write operation.

    Args:
        table: Delta table name (e.g., xact_events, xact_attachments)
        event_count: Number of events written
        success: Whether the write was successful
    """
    status = "success" if success else "error"
    delta_writes_counter.add(1, attributes={"table": table, "status": status})
    if success:
        delta_events_written_counter.add(event_count, attributes={"table": table})


def update_downloads_concurrent(worker: str, count: int) -> None:
    """
    Update the number of concurrent downloads in progress.

    Args:
        worker: Worker identifier (e.g., "download_worker")
        count: Number of downloads currently in progress
    """
    # Store state for observable gauge callback
    key = (f"downloads_concurrent:{worker}",)
    _gauge_state[key] = count


def update_downloads_batch_size(worker: str, size: int) -> None:
    """
    Update the current download batch size.

    Args:
        worker: Worker identifier (e.g., "download_worker")
        size: Number of messages in the current batch
    """
    # Store state for observable gauge callback
    key = (f"downloads_batch:{worker}",)
    _gauge_state[key] = size


def update_uploads_concurrent(worker: str, count: int) -> None:
    """
    Update the number of concurrent uploads in progress.

    Args:
        worker: Worker identifier (e.g., "upload_worker")
        count: Number of uploads currently in progress
    """
    # Store state for observable gauge callback
    key = (f"uploads_concurrent:{worker}",)
    _gauge_state[key] = count


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
    consumer_shutdown_duration_histogram.record(
        duration, attributes={"consumer_group": consumer_group, "status": status}
    )
    if status == "timeout":
        consumer_shutdown_timeout_counter.add(1, attributes={"consumer_group": consumer_group})


def record_consumer_shutdown_error(consumer_group: str, error_type: str) -> None:
    """
    Record a consumer shutdown error.

    Args:
        consumer_group: Consumer group ID
        error_type: Type of error (timeout, force_close, unknown)
    """
    consumer_shutdown_error_counter.add(
        1, attributes={"consumer_group": consumer_group, "error_type": error_type}
    )


# =============================================================================
# Legacy Compatibility - Direct metric exports
# =============================================================================

# For backward compatibility, export metrics with old names
# (though in OTel, metrics are accessed through helper functions)
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
    "updates_batch_size",
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
