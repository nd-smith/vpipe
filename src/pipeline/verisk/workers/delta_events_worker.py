"""
Delta Events Worker - Writes events to Delta Lake xact_events table.

This worker consumes events from the events.raw topic and writes them to
the xact_events Delta table for analytics.

Separated from EventIngesterWorker to follow single-responsibility principle:
- EventIngesterWorker: Parse events → produce download tasks
- DeltaEventsWorker: Parse events → write to Delta Lake

Features:
- Batch accumulation for efficient Delta writes
- Configurable batch size via delta_events_batch_size
- Optional batch limit for testing via delta_events_max_batches
- Retry via Kafka topics with exponential backoff

Consumer group: {prefix}-delta-events
Input topic: com.allstate.pcesdopodappv1.xact.events.raw
Output: Delta table xact_events (no Kafka output)
Retry topics: com.allstate.pcesdopodappv1.delta-events.retry.{delay}m
DLQ topic: com.allstate.pcesdopodappv1.delta-events.dlq
"""

import asyncio
import contextlib
import json
import logging
import uuid
from typing import Any

from config.config import KafkaConfig
from core.logging.context import set_log_context
from core.logging.periodic_logger import PeriodicStatsLogger
from core.logging.utilities import format_cycle_output, log_worker_error
from pipeline.common.health import HealthCheckServer
from pipeline.common.metrics import record_delta_write
from pipeline.common.retry.delta_handler import DeltaRetryHandler
from pipeline.common.transport import create_consumer
from pipeline.common.types import PipelineMessage
from pipeline.verisk.workers.worker_defaults import WorkerDefaults
from pipeline.verisk.writers import DeltaEventsWriter

logger = logging.getLogger(__name__)


class DeltaEventsWorker:
    """
    Worker to consume events and write them to Delta Lake in batches.

    Processes EventMessage records from the events.raw topic and writes
    them to the xact_events Delta table using the flatten_events() transform.

    This worker runs independently of the EventIngesterWorker, consuming
    from the same topic but with a different consumer group. This allows:
    - Independent scaling of Delta writes vs download task creation
    - Fault isolation between Delta writes and Kafka pipeline
    - Batching optimization for Delta writes

    Features:
    - Batch accumulation for efficient Delta writes
    - Configurable batch size via config.delta_events_batch_size
    - Optional batch limit for testing via config.delta_events_max_batches
    - Graceful shutdown with pending batch flush
    - Failed batches route to Kafka retry topics
    - Deduplication handled by daily Fabric maintenance job

    Usage:
        >>> config = KafkaConfig.from_env()
        >>> producer = BaseKafkaProducer(config)
        >>> await producer.start()
        >>> worker = DeltaEventsWorker(
        ...     config=config,
        ...     producer=producer,
        ...     events_table_path="abfss://..."
        ... )
        >>> await worker.start()
    """

    WORKER_NAME = "delta_events_writer"

    # Cycle output configuration
    CYCLE_LOG_INTERVAL_SECONDS = WorkerDefaults.CYCLE_LOG_INTERVAL_SECONDS

    def __init__(
        self,
        config: KafkaConfig,
        producer: Any,
        events_table_path: str,
        domain: str = "verisk",
        instance_id: str | None = None,
    ):
        """
        Initialize Delta events worker.

        Args:
            config: Kafka configuration for consumer (topic names, connection settings).
                    Also provides delta_events_batch_size and delta_events_max_batches.
            producer: Kafka producer for retry topic routing (required).
            events_table_path: Full abfss:// path to xact_events Delta table
            domain: Domain identifier (default: "xact")
        """
        self.config = config
        self.domain = domain
        self.instance_id = instance_id
        self.events_table_path = events_table_path
        self.consumer = None
        self.producer = producer

        # Create worker_id with instance suffix (ordinal) if provided
        if instance_id:
            self.worker_id = f"{self.WORKER_NAME}-{instance_id}"
        else:
            self.worker_id = self.WORKER_NAME

        # Batch configuration - use worker-specific config
        processing_config = config.get_worker_config(
            domain, "delta_events_writer", "processing"
        )
        self.batch_size = processing_config.get(
            "batch_size", WorkerDefaults.MAX_POLL_RECORDS
        )
        self.max_batches = processing_config.get("max_batches")  # None = unlimited
        self.batch_timeout_seconds = processing_config.get(
            "batch_timeout_seconds", 10.0
        )

        # Retry configuration from worker processing settings
        self._retry_delays = processing_config.get(
            "retry_delays", [300, 600, 1200, 2400]
        )
        self._retry_topic_prefix = processing_config.get(
            "retry_topic_prefix", "com.allstate.pcesdopodappv1.delta-events.retry"
        )
        self._dlq_topic = processing_config.get(
            "dlq_topic", "com.allstate.pcesdopodappv1.delta-events.dlq"
        )

        # Batch state
        self._batch: list[dict[str, Any]] = []
        self._batch_lock = asyncio.Lock()
        self._batch_timer: asyncio.Task | None = None
        self._batches_written = 0
        self._total_events_written = 0

        # Cycle output tracking
        self._records_processed = 0
        self._records_succeeded = 0
        self._stats_logger: PeriodicStatsLogger | None = None
        self._running = False

        # Initialize Delta writer
        if not events_table_path:
            raise ValueError("events_table_path is required for DeltaEventsWorker")

        self.delta_writer = DeltaEventsWriter(
            table_path=events_table_path,
        )

        # Initialize retry handler
        self.retry_handler = DeltaRetryHandler(
            config=config,
            table_path=events_table_path,
            retry_delays=self._retry_delays,
            retry_topic_prefix=self._retry_topic_prefix,
            dlq_topic=self._dlq_topic,
            domain=self.domain,
        )

        # Health check server - use worker-specific port from config
        health_port = processing_config.get("health_port", 8093)
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name="xact-delta-events",
        )

        logger.info(
            "Initialized DeltaEventsWorker",
            extra={
                "domain": domain,
                "worker_id": self.worker_id,
                "worker_name": self.WORKER_NAME,
                "instance_id": instance_id,
                "consumer_group": config.get_consumer_group(
                    domain, "delta_events_writer"
                ),
                "events_topic": config.get_topic(domain, "events"),
                "events_table_path": events_table_path,
                "batch_size": self.batch_size,
                "batch_timeout_seconds": self.batch_timeout_seconds,
                "max_batches": self.max_batches,
                "retry_delays": self._retry_delays,
                "retry_topic_prefix": self._retry_topic_prefix,
                "dlq_topic": self._dlq_topic,
            },
        )

    async def start(self) -> None:
        """
        Start the delta events worker.

        Initializes consumer and begins consuming events from the events.raw topic.
        Runs until stop() is called or max_batches is reached (if configured).

        Raises Exception if consumer fails to start.
        """
        logger.info(
            "Starting DeltaEventsWorker",
            extra={
                "batch_size": self.batch_size,
                "max_batches": self.max_batches,
            },
        )
        self._running = True

        # Start health server first for immediate liveness probe response
        await self.health_server.start()

        from pipeline.common.telemetry import initialize_worker_telemetry

        initialize_worker_telemetry(self.domain, "delta-events-worker")

        # Start retry handler producers
        await self.retry_handler.start()

        # Start periodic stats logger
        self._stats_logger = PeriodicStatsLogger(
            interval_seconds=self.CYCLE_LOG_INTERVAL_SECONDS,
            get_stats=self._get_cycle_stats,
            stage="delta_write",
            worker_id=self.worker_id,
        )
        self._stats_logger.start()

        # Start batch timer for periodic flushing
        self._reset_batch_timer()

        # Create and start consumer with message handler (uses transport factory)
        # Disable per-message commits - we commit after batch writes to ensure
        # offsets are only committed after data is durably written to Delta Lake
        self.consumer = await create_consumer(
            config=self.config,
            domain=self.domain,
            worker_name="delta_events_writer",
            topics=[self.config.get_topic(self.domain, "events")],
            message_handler=self._handle_event_message,
            enable_message_commit=False,
            instance_id=self.instance_id,
            topic_key="events",
        )

        # Update health check readiness
        self.health_server.set_ready(kafka_connected=True)

        try:
            # Start consumer (this blocks until stopped)
            await self.consumer.start()
        finally:
            self._running = False

    async def stop(self) -> None:
        """
        Stop the delta events worker.

        Flushes any pending batch, then gracefully shuts down consumer,
        committing any pending offsets.
        """
        logger.info("Stopping DeltaEventsWorker")
        self._running = False

        if self._stats_logger:
            await self._stats_logger.stop()

        # Cancel batch timer
        if self._batch_timer and not self._batch_timer.done():
            self._batch_timer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._batch_timer

        # Flush any remaining events in the batch
        if self._batch:
            logger.info(
                "Flushing remaining batch on shutdown",
                extra={"batch_size": len(self._batch)},
            )
            await self._flush_batch()

        # Stop consumer
        if self.consumer:
            await self.consumer.stop()

        # Stop retry handler producers
        if self.retry_handler:
            await self.retry_handler.stop()

        # Stop health check server
        await self.health_server.stop()

        logger.info(
            "DeltaEventsWorker stopped successfully",
            extra={
                "batches_written": self._batches_written,
                "records_succeeded": self._records_succeeded,
            },
        )

    async def _handle_event_message(self, record: PipelineMessage) -> None:
        """
        Process a single event message from Kafka.

        Adds the event to the batch and flushes when batch is full.
        """
        # Track events received for cycle output
        self._records_processed += 1

        # Check if we've reached max batches limit
        if self.max_batches is not None and self._batches_written >= self.max_batches:
            logger.info(
                "Reached max_batches limit, stopping consumer",
                extra={
                    "max_batches": self.max_batches,
                    "batches_written": self._batches_written,
                },
            )
            if self.consumer:
                await self.consumer.stop()
            return

        # Decode message - keep as raw dict, don't convert to EventMessage
        try:
            message_data = json.loads(record.value.decode("utf-8"))
        except json.JSONDecodeError as e:
            log_worker_error(
                logger,
                "Failed to parse message JSON",
                error_category="PERMANENT",
                exc=e,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
            )
            raise

        # Set logging context for correlation
        trace_id = message_data.get("traceId")
        if trace_id:
            set_log_context(trace_id=trace_id)

        # Add to batch with lock (raw dict with data already as dict)
        async with self._batch_lock:
            self._batch.append(message_data)

            logger.debug(
                "Added event to batch",
                extra={
                    "trace_id": message_data.get("traceId"),
                    "event_id": message_data.get("eventId"),
                    "batch_size": len(self._batch),
                    "batch_threshold": self.batch_size,
                },
            )

            # Flush batch if full
            if len(self._batch) >= self.batch_size:
                await self._flush_batch()
                self._reset_batch_timer()

    async def _flush_batch(self) -> None:
        """
        Write the accumulated batch to Delta Lake.

        On success: clears batch and updates counters.
        On failure: routes batch to Kafka retry topic.
        """
        if not self._batch:
            return

        # Generate short batch ID for log correlation
        batch_id = uuid.uuid4().hex[:8]
        batch_size = len(self._batch)
        batch_to_write = self._batch
        self._batch = []  # Clear immediately to accept new events

        success = await self._write_batch(batch_to_write, batch_id)

        if success:
            self._batches_written += 1
            self._records_succeeded += batch_size

            # Commit offsets after successful Delta write
            # This ensures at-least-once semantics: offsets are only committed
            # after data is durably written to Delta Lake
            if self.consumer:
                await self.consumer.commit()

            # Build progress message
            if self.max_batches:
                progress = f"Batch {self._batches_written}/{self.max_batches}"
            else:
                progress = f"Batch {self._batches_written}"

            logger.info(
                f"{progress}: Successfully wrote {batch_size} events to Delta",
                extra={
                    "batch_id": batch_id,
                    "batch_size": batch_size,
                    "batches_written": self._batches_written,
                    "records_succeeded": self._records_succeeded,
                    "max_batches": self.max_batches,
                },
            )

            # Stop immediately if we've reached max_batches
            if self.max_batches and self._batches_written >= self.max_batches:
                logger.info(
                    "Reached max_batches limit, stopping consumer",
                    extra={
                        "batch_id": batch_id,
                        "max_batches": self.max_batches,
                        "batches_written": self._batches_written,
                    },
                )
                if self.consumer:
                    await self.consumer.stop()
        else:
            # Route to Kafka retry topic
            trace_ids = []
            event_ids = []
            for event_dict in batch_to_write[:10]:
                if event_dict.get("traceId") or event_dict.get("trace_id"):
                    trace_ids.append(
                        event_dict.get("traceId") or event_dict.get("trace_id")
                    )
                if event_dict.get("eventId") or event_dict.get("event_id"):
                    event_ids.append(
                        event_dict.get("eventId") or event_dict.get("event_id")
                    )
            logger.warning(
                "Batch write failed, routing to retry topic",
                extra={
                    "batch_id": batch_id,
                    "batch_size": batch_size,
                    "trace_ids": trace_ids,
                    "event_ids": event_ids,
                },
            )
            await self.retry_handler.handle_batch_failure(
                batch=batch_to_write,
                error=Exception("Delta write returned failure status"),
                retry_count=0,
                error_category="transient",
                batch_id=batch_id,
            )

    async def _write_batch(self, batch: list[dict[str, Any]], batch_id: str) -> bool:
        """
        Attempt to write a batch to Delta Lake.

        Returns True if write succeeded, False otherwise.
        """
        batch_size = len(batch)

        from pipeline.common.telemetry import get_tracer

        tracer = get_tracer(__name__)
        with tracer.start_active_span("delta.write") as scope:
            span = scope.span if hasattr(scope, "span") else scope
            span.set_tag("span.kind", "client")
            span.set_tag("batch_id", batch_id)
            span.set_tag("batch_size", batch_size)
            span.set_tag("table", "xact_events")
            try:
                success = await self.delta_writer.write_raw_events(
                    batch, batch_id=batch_id
                )

                span.set_tag("write.success", success)
                record_delta_write(
                    table="xact_events",
                    event_count=batch_size,
                    success=success,
                )

                return success

            except Exception as e:
                # Classify error using DeltaRetryHandler for proper DLQ routing
                error_category = self.retry_handler.classify_delta_error(e)

                # Store error info for _flush_batch to use
                self._last_write_error = e
                self._last_error_category = error_category

                trace_ids = []
                event_ids = []
                for evt in batch[:10]:
                    if evt.get("traceId") or evt.get("trace_id"):
                        trace_ids.append(evt.get("traceId") or evt.get("trace_id"))
                    if evt.get("eventId") or evt.get("event_id"):
                        event_ids.append(evt.get("eventId") or evt.get("event_id"))

                span.set_tag("write.success", False)
                span.set_tag("error.category", error_category.value)
                span.set_tag("error.type", type(e).__name__)
                span.set_tag("error.message", str(e)[:200])

                log_worker_error(
                    logger,
                    "Delta write error - classified for routing",
                    error_category=error_category.value,
                    exc=e,
                    batch_id=batch_id,
                    batch_size=batch_size,
                    error_type=type(e).__name__,
                    trace_ids=trace_ids,
                    event_ids=event_ids,
                )
                record_delta_write(
                    table="xact_events",
                    event_count=batch_size,
                    success=False,
                )
                return False

    def _get_cycle_stats(self, cycle_count: int) -> tuple[str, dict[str, Any]]:
        """Get cycle statistics for periodic logging."""
        msg = format_cycle_output(
            cycle_count=cycle_count,
            succeeded=self._records_succeeded,
            failed=0,
        )
        extra = {
            "records_processed": self._records_processed,
            "batches_written": self._batches_written,
            "records_succeeded": self._records_succeeded,
            "pending_batch_size": len(self._batch),
        }
        return msg, extra

    async def _periodic_flush(self) -> None:
        """
        Timer callback to periodically flush batch regardless of size.

        This ensures events are written even during low-traffic periods,
        improving latency for the last events in a batch.
        """
        try:
            while self._running:
                await asyncio.sleep(self.batch_timeout_seconds)
                async with self._batch_lock:
                    if self._batch:
                        logger.debug(
                            "Flushing batch on timeout",
                            extra={
                                "batch_size": len(self._batch),
                                "timeout_seconds": self.batch_timeout_seconds,
                            },
                        )
                        await self._flush_batch()
        except asyncio.CancelledError:
            logger.debug("Periodic flush task cancelled")
            raise

    def _reset_batch_timer(self) -> None:
        """Reset the batch flush timer."""
        if self._batch_timer and not self._batch_timer.done():
            self._batch_timer.cancel()
        self._batch_timer = asyncio.create_task(self._periodic_flush())


__all__ = ["DeltaEventsWorker"]
