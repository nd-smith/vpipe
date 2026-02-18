"""Delta events worker for ClaimX.

Writes event messages to Delta Lake with batch processing.
Separate from event ingester for independent scaling.
"""

import asyncio
import json
import logging
import uuid
from typing import Any

from pydantic import ValidationError

from config.config import MessageConfig
from core.logging.periodic_logger import PeriodicStatsLogger
from core.logging.utilities import log_worker_error
from core.types import ErrorCategory
from pipeline.claimx.schemas.events import ClaimXEventMessage
from pipeline.claimx.writers import ClaimXEventsDeltaWriter
from pipeline.common.health import HealthCheckServer
from pipeline.common.metrics import record_delta_write
from pipeline.common.retry.delta_handler import DeltaRetryHandler
from pipeline.common.transport import create_consumer, get_source_connection_string
from pipeline.common.types import PipelineMessage

from core.errors.exceptions import PermanentError

logger = logging.getLogger(__name__)

# Lookup list for Delta error classification: (markers, category)
_DELTA_ERROR_RULES: list[tuple[tuple[str, ...], ErrorCategory]] = [
    (("schema", "validation"), ErrorCategory.PERMANENT),
    (("not found", "404"), ErrorCategory.PERMANENT),
    (("401", "403", "unauthorized"), ErrorCategory.AUTH),
    (("timeout",), ErrorCategory.TRANSIENT),
    (("connection", "network"), ErrorCategory.TRANSIENT),
    (("429", "throttl", "rate limit"), ErrorCategory.TRANSIENT),
    (("503", "service unavailable"), ErrorCategory.TRANSIENT),
]


class ClaimXDeltaEventsWorker:
    """
    Worker to consume ClaimX events and write them to Delta Lake in batches.

    Processes ClaimXEventMessage records from the claimx events topic and writes
    them to the claimx_events Delta table.

    This worker runs independently of the ClaimXEventIngesterWorker, consuming
    from the same topic but with a different consumer group.

    Features:
    - Batch accumulation for efficient Delta writes
    - Configurable batch size
    - Graceful shutdown with pending batch flush
    - Failed batches route to Kafka retry topics

    Usage:
        >>> config = MessageConfig.from_env()
        >>> producer = MessageProducer(config)
        >>> await producer.start()
        >>> worker = ClaimXDeltaEventsWorker(
        ...     config=config,
        ...     producer=producer,
        ...     events_table_path="abfss://..."
        ... )
        >>> await worker.start()
    """

    WORKER_NAME = "delta_events_writer"

    # Cycle output configuration
    CYCLE_LOG_INTERVAL_SECONDS = 30

    def __init__(
        self,
        config: MessageConfig,
        producer: Any,
        events_table_path: str,
        domain: str = "claimx",
        instance_id: str | None = None,
    ):
        """
        Initialize ClaimX Delta events worker.

        Args:
            config: Kafka configuration for consumer.
            producer: Kafka producer for retry topic routing (required).
            events_table_path: Full abfss:// path to claimx_events Delta table
            domain: Domain identifier (default: "claimx")
        """
        self.config = config
        self.domain = domain
        self.instance_id = instance_id
        self.events_table_path = events_table_path
        self.consumer = None
        self.producer = producer

        if instance_id:
            self.worker_id = f"{self.WORKER_NAME}-{instance_id}"
        else:
            self.worker_id = self.WORKER_NAME

        # Batch configuration - use worker-specific config
        processing_config = config.get_worker_config(domain, "delta_events_writer", "processing")
        self.batch_size = processing_config.get("batch_size", 100)
        self.batch_timeout_seconds = processing_config.get("batch_timeout_seconds", 30.0)

        # Retry configuration from worker processing settings
        # Note: retry_topic_prefix and dlq_topic removed - DeltaRetryHandler
        # uses EventHub names from config.yaml (claimx.retry and claimx.dlq)
        self._retry_delays = processing_config.get("retry_delays", [300, 600, 1200, 2400])

        # Batch state
        self._batch: list[dict[str, Any]] = []
        self._batch_lock = asyncio.Lock()
        self._batch_timer: asyncio.Task | None = None
        self._batches_written = 0

        # Cycle output tracking
        self._records_processed = 0
        self._records_succeeded = 0
        self._records_failed = 0
        self._records_skipped = 0
        self._stats_logger: PeriodicStatsLogger | None = None
        self._cycle_offset_start_ts = None
        self._cycle_offset_end_ts = None
        self._running = False

        if not events_table_path:
            raise ValueError("events_table_path is required for ClaimXDeltaEventsWorker")

        # DIAGNOSTIC: Log the exact table path being used
        logger.info(
            "Initializing ClaimX Delta writer",
            extra={
                "events_table_path": events_table_path,
                "table_path_length": len(events_table_path),
                "table_path_prefix": events_table_path[:50]
                if len(events_table_path) > 50
                else events_table_path,
            },
        )

        self.delta_writer = ClaimXEventsDeltaWriter(
            table_path=events_table_path,
        )

        # Health check server - use worker-specific port from config
        health_port = processing_config.get("health_port", 8085)
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name="claimx-delta-events-worker",
        )

        self.retry_handler = DeltaRetryHandler(
            config=config,
            table_path=events_table_path,
            retry_delays=self._retry_delays,
            domain=self.domain,
            # retry_topic_prefix and dlq_topic removed - uses EventHub names from config
        )

        logger.info(
            "Initialized ClaimXDeltaEventsWorker",
            extra={
                "domain": domain,
                "worker_id": self.worker_id,
                "worker_name": self.WORKER_NAME,
                "instance_id": instance_id,
                "consumer_group": config.get_consumer_group(domain, "delta_events_writer"),
                "events_topic": config.get_topic(domain, "events"),
                "events_table_path": events_table_path,
                "batch_size": self.batch_size,
                "retry_delays": self._retry_delays,
                # retry/dlq topics logged by DeltaRetryHandler using EventHub names from config
            },
        )

    async def start(self) -> None:
        """
        Start the ClaimX delta events worker.

        Initializes consumer and begins consuming events.
        """
        logger.info("Starting ClaimXDeltaEventsWorker")
        self._running = True

        # Start health server first for immediate liveness probe response
        await self.health_server.start()

        from pipeline.common.telemetry import initialize_worker_telemetry

        initialize_worker_telemetry(self.domain, "delta-events-worker")

        # Close resources from a previous failed start attempt to prevent leak.
        if self.consumer:
            try:
                await self.consumer.stop()
            except Exception as e:
                logger.warning("Error cleaning up stale consumer", extra={"error": str(e)})
            finally:
                self.consumer = None
        if self._stats_logger:
            try:
                await self._stats_logger.stop()
            except Exception as e:
                logger.warning("Error cleaning up stale stats logger", extra={"error": str(e)})
            finally:
                self._stats_logger = None

        # Start retry handler producers
        await self.retry_handler.start()

        # Start cycle output background task
        self._stats_logger = PeriodicStatsLogger(
            interval_seconds=self.CYCLE_LOG_INTERVAL_SECONDS,
            get_stats=self._get_cycle_stats,
            stage="delta_write",
            worker_id=self.worker_id,
        )
        self._stats_logger.start()

        # Start batch timer
        self._reset_batch_timer()

        # Create and start consumer
        # Disable per-message commits - we commit after batch writes
        self.consumer = await create_consumer(
            config=self.config,
            domain=self.domain,
            worker_name="delta_events_writer",
            topics=[self.config.get_topic(self.domain, "events")],
            message_handler=self._handle_event_message,
            enable_message_commit=False,
            instance_id=self.instance_id,
            topic_key="events",
            connection_string=get_source_connection_string(),
        )

        # Update health check readiness
        self.health_server.set_ready(transport_connected=True)

        try:
            await self.consumer.start()
        finally:
            self._running = False

    async def _close_resource(self, name: str, method: str = "stop", *, clear: bool = False) -> None:
        """Close a resource by attribute name, logging errors. Optionally set to None."""
        resource = getattr(self, name, None)
        if resource is None:
            return
        try:
            await getattr(resource, method)()
        except asyncio.CancelledError:
            logger.warning(f"Cancelled while stopping {name}")
        except Exception as e:
            logger.error(f"Error stopping {name}", extra={"error": str(e)})
        finally:
            if clear:
                setattr(self, name, None)

    async def stop(self) -> None:
        """
        Stop the ClaimX delta events worker.

        Flushes pending batch and shuts down consumer.
        """
        logger.info("Stopping ClaimXDeltaEventsWorker")
        self._running = False

        await self._close_resource("_stats_logger", clear=True)

        if self._batch_timer:
            self._batch_timer.cancel()
            self._batch_timer = None

        try:
            async with self._batch_lock:
                if self._batch:
                    logger.info("Flushing remaining batch on shutdown")
                    await self._flush_batch()
        except Exception as e:
            logger.error("Error flushing batch on shutdown", extra={"error": str(e)})

        await self._close_resource("consumer", clear=True)
        await self._close_resource("retry_handler", clear=True)
        await self._close_resource("health_server")

        logger.info("ClaimXDeltaEventsWorker stopped successfully")

    def _update_cycle_offsets(self, ts: int | None) -> None:
        """Update cycle offset tracking with a message timestamp."""
        if ts is None:
            return
        if self._cycle_offset_start_ts is None or ts < self._cycle_offset_start_ts:
            self._cycle_offset_start_ts = ts
        if self._cycle_offset_end_ts is None or ts > self._cycle_offset_end_ts:
            self._cycle_offset_end_ts = ts

    def _record_log_context(self, record: PipelineMessage) -> dict[str, Any]:
        """Build log context dict from a PipelineMessage."""
        return {
            "topic": record.topic,
            "partition": record.partition,
            "offset": record.offset,
            "trace_id": record.key.decode("utf-8") if record.key else None,
        }

    async def _handle_event_message(self, record: PipelineMessage) -> None:
        """
        Process a single event message.

        Parses raw event through ClaimXEventMessage.from_raw_event() to normalize
        field names (camelCase → snake_case) and generate deterministic trace_id
        when the source does not provide one.
        """
        self._records_processed += 1
        self._update_cycle_offsets(record.timestamp)

        try:
            message_data = json.loads(record.value.decode("utf-8"))
        except json.JSONDecodeError as e:
            logger.exception("Failed to parse message JSON", extra=self._record_log_context(record))
            raise PermanentError(str(e)) from e

        try:
            event = ClaimXEventMessage.from_raw_event(message_data)
            event_data = event.model_dump(exclude={"raw_data"})
        except ValidationError as e:
            logger.exception("Failed to parse ClaimXEventMessage", extra=self._record_log_context(record))
            raise PermanentError(str(e)) from e

        async with self._batch_lock:
            self._batch.append(event_data)
            if len(self._batch) >= self.batch_size:
                await self._flush_batch()
                self._reset_batch_timer()

    async def _flush_batch(self) -> None:
        """Write accumulated batch to Delta Lake."""
        if not self._batch:
            return

        batch_to_write = list(self._batch)
        self._batch.clear()

        # Use simple try/except for write
        try:
            success = await self.delta_writer.write_events(batch_to_write)

            record_delta_write(
                table="claimx_events", event_count=len(batch_to_write), success=success
            )

            if success:
                self._batches_written += 1
                self._records_succeeded += len(batch_to_write)
                if self.consumer:
                    await self.consumer.commit()

                logger.debug(
                    "Batch written successfully",
                    extra={
                        "batch_size": len(batch_to_write),
                    },
                )
            else:
                self._records_failed += len(batch_to_write)
                await self._handle_failed_batch(batch_to_write, Exception("Write returned failure"))

        except Exception as e:
            self._records_failed += len(batch_to_write)
            await self._handle_failed_batch(batch_to_write, e)

    async def _handle_failed_batch(self, batch: list[dict[str, Any]], error: Exception) -> None:
        """
        Handle failed batch with error classification and DLQ routing.

        Routes PERMANENT errors to DLQ and clears batch.
        Keeps batch intact for TRANSIENT errors to enable retry.
        """
        # Classify error to determine handling strategy
        error_category = self._classify_delta_error(error)

        # Use standardized error logging
        log_worker_error(
            logger,
            "Batch write failed",
            error_category=error_category.value,
            exc=error,
            batch_size=len(batch),
        )

        # Handle PERMANENT errors: send to DLQ and clear batch
        if error_category == ErrorCategory.PERMANENT:
            logger.warning(
                "Permanent Delta write error detected, sending batch to DLQ",
                extra={
                    "batch_size": len(batch),
                    "error_type": type(error).__name__,
                },
            )

            # Send to DLQ via retry handler
            if hasattr(self, "retry_handler"):
                await self.retry_handler.handle_batch_failure(
                    batch=batch,
                    error=error,
                    retry_count=0,
                    error_category=error_category.value,
                    batch_id=uuid.uuid4().hex[:8],
                )

            # Clear batch after routing to DLQ since this error won't succeed on retry
            self._batch.clear()
            return

        # For TRANSIENT errors, route to retry topic but keep batch intact
        logger.info(
            "Transient Delta write error, routing to retry topic",
            extra={
                "batch_size": len(batch),
                "error_category": error_category.value,
                "error_type": type(error).__name__,
            },
        )

        if hasattr(self, "retry_handler"):
            await self.retry_handler.handle_batch_failure(
                batch=batch,
                error=error,
                retry_count=0,
                error_category=error_category.value,
                batch_id=uuid.uuid4().hex[:8],
            )

        # Keep batch intact for TRANSIENT errors - don't clear
        # This prevents data loss if retry topic send fails

    def _classify_delta_error(self, error: Exception) -> ErrorCategory:
        """
        Classify Delta write errors into categories for handling decisions.

        Args:
            error: Exception from Delta write operation

        Returns:
            ErrorCategory indicating how to handle this error
        """
        error_str = str(error).lower()
        error_type = type(error).__name__.lower()
        combined = error_str + " " + error_type

        for markers, category in _DELTA_ERROR_RULES:
            if any(m in combined for m in markers):
                return category

        # Default to TRANSIENT for unknown errors (safe default - allows retry)
        return ErrorCategory.TRANSIENT

    async def _periodic_flush(self) -> None:
        """Timer callback to flush batch."""
        while self._running:
            await asyncio.sleep(self.batch_timeout_seconds)
            async with self._batch_lock:
                if self._batch:
                    await self._flush_batch()

    def _reset_batch_timer(self) -> None:
        """Reset the batch flush timer."""
        if self._batch_timer:
            self._batch_timer.cancel()
        self._batch_timer = asyncio.create_task(self._periodic_flush())

    def _get_cycle_stats(self, cycle_count: int) -> tuple[str, dict]:
        extra = {
            "records_processed": self._records_processed,
            "records_succeeded": self._records_succeeded,
            "records_failed": self._records_failed,
            "records_skipped": self._records_skipped,
            "records_deduplicated": 0,
            "batches_written": self._batches_written,
            "cycle_offset_start_ts": self._cycle_offset_start_ts,
            "cycle_offset_end_ts": self._cycle_offset_end_ts,
        }
        self._cycle_offset_start_ts = None
        self._cycle_offset_end_ts = None
        return "", extra


__all__ = ["ClaimXDeltaEventsWorker"]
