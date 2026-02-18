"""ClaimX result processor worker.

Processes upload results, emits metrics, and writes to Delta Lake.
Final stage of the download pipeline.
"""

import asyncio
import contextlib
import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

import polars as pl
from pydantic import ValidationError

from config.config import MessageConfig
from core.logging.context import set_log_context
from core.logging.periodic_logger import PeriodicStatsLogger
from core.logging.utilities import log_worker_error
from pipeline.claimx.schemas.results import ClaimXUploadResultMessage
from pipeline.common.health import HealthCheckServer
from pipeline.common.metrics import (
    record_delta_write,
    record_message_consumed,
    record_processing_error,
)
from pipeline.common.transport import create_consumer
from pipeline.common.types import PipelineMessage
from pipeline.common.writers.base import BaseDeltaWriter

from core.errors.exceptions import PermanentError

logger = logging.getLogger(__name__)


class ClaimXResultProcessor:
    """
    Worker to consume and process ClaimX download/upload results.

    Processes ClaimXUploadResultMessage records from the results topic,
    logs outcomes, emits metrics, and tracks success/failure rates for monitoring.

    Features:
    - Batch accumulation for efficient Delta writes
    - Size-based and time-based flushing
    - Graceful shutdown with pending batch flush
    - Success/failure rate tracking
    - Detailed logging for operational monitoring
    - Metrics emission for dashboards

    Usage:
        >>> config = MessageConfig.from_env()
        >>> processor = ClaimXResultProcessor(config)
        >>> await processor.start()
        >>> # Processor runs until stopped
        >>> await processor.stop()
    """

    WORKER_NAME = "result_processor"

    # Batching configuration
    BATCH_SIZE = 500
    BATCH_TIMEOUT_SECONDS = 5

    def __init__(
        self,
        config: MessageConfig,
        domain: str = "claimx",
        results_topic: str = "",
        inventory_table_path: str = "",
        batch_size: int | None = None,
        batch_timeout_seconds: float | None = None,
        instance_id: str | None = None,
    ):
        """
        Initialize ClaimX result processor.

        Args:
            config: Message broker configuration for consumer
            domain: Pipeline domain identifier (default: "claimx")
            results_topic: Topic name for upload results (e.g., "claimx-downloads-results")
            inventory_table_path: Full abfss:// path to claimx_attachments table (optional)
            batch_size: Optional custom batch size (default: 500)
            batch_timeout_seconds: Optional custom timeout (default: 5.0)
            instance_id: Optional instance ID for multiple workers (default: None)
        """
        self.config = config
        self.domain = domain
        self.worker_name = "result_processor"
        self.instance_id = instance_id

        # Create worker_id with instance suffix (ordinal) if provided
        if instance_id:
            self.worker_id = f"{self.WORKER_NAME}-{instance_id}"
        else:
            self.worker_id = self.WORKER_NAME

        # Batching configuration
        self.batch_size = batch_size or self.BATCH_SIZE
        self.batch_timeout_seconds = batch_timeout_seconds or self.BATCH_TIMEOUT_SECONDS

        # Get topic from hierarchical config or use provided/default
        self.results_topic = results_topic or config.get_topic(self.domain, "downloads_results")
        self.consumer = None

        # Consumer group from hierarchical config
        self.consumer_group = config.get_consumer_group(self.domain, self.worker_name)

        # Batch state
        self._batch: list[ClaimXUploadResultMessage] = []
        self._batch_lock = asyncio.Lock()
        self._last_flush = time.monotonic()
        self._flush_task: asyncio.Task | None = None

        # Stats tracking
        self._records_processed = 0
        self._records_succeeded = 0
        self._records_failed = 0
        self._records_skipped = 0
        self._cycle_offset_start_ts = None
        self._cycle_offset_end_ts = None
        self._batches_written = 0
        self._total_records_written = 0
        self._stats_logger: PeriodicStatsLogger | None = None

        # Initialize Delta writer for inventory if path is provided
        self.inventory_writer: BaseDeltaWriter | None = None
        if inventory_table_path:
            self.inventory_writer = BaseDeltaWriter(
                table_path=inventory_table_path,
                partition_column="project_id",
            )

        self._running = False

        # Health check server - use worker-specific port from config
        processing_config = config.get_worker_config(self.domain, self.worker_name, "processing")
        health_port = processing_config.get("health_port", 8087)
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name="claimx-result-processor",
        )

        logger.info(
            "Initialized ClaimXResultProcessor",
            extra={
                "domain": self.domain,
                "worker_id": self.worker_id,
                "worker_name": self.WORKER_NAME,
                "instance_id": instance_id,
                "consumer_group": self.consumer_group,
                "results_topic": self.results_topic,
                "inventory_table": inventory_table_path,
                "batch_size": self.batch_size,
                "batch_timeout": self.batch_timeout_seconds,
            },
        )

    async def start(self) -> None:
        """
        Start the ClaimX result processor.

        Begins consuming result messages from the results topic.
        This method runs until stop() is called.

        Raises:
            Exception: If consumer fails to start
        """
        if self._running:
            logger.warning("ClaimXResultProcessor already running")
            return

        logger.info("Starting ClaimXResultProcessor")
        self._running = True

        # Start health server first for immediate liveness probe response
        await self.health_server.start()

        from pipeline.common.telemetry import initialize_worker_telemetry

        initialize_worker_telemetry(self.domain, "result-processor")

        # Create and start consumer with message handler
        # Disable auto-commit to allow manual commit after batch write
        self.consumer = await create_consumer(
            config=self.config,
            domain=self.domain,
            worker_name=self.worker_name,
            topics=[self.results_topic],
            message_handler=self._handle_result_message,
            enable_message_commit=False,
            instance_id=self.instance_id,
            topic_key="downloads_results",
        )

        # Start periodic background tasks
        self._stats_logger = PeriodicStatsLogger(
            interval_seconds=30,
            get_stats=self._get_cycle_stats,
            stage="result_processing",
            worker_id=self.worker_id,
        )
        self._stats_logger.start()
        self._flush_task = asyncio.create_task(self._periodic_flush())

        # Update health check readiness
        self.health_server.set_ready(transport_connected=True)

        try:
            # Start consumer (this blocks until stopped)
            await self.consumer.start()
        except asyncio.CancelledError:
            logger.info("ClaimXResultProcessor cancelled, shutting down...")
            raise
        finally:
            self._running = False

    async def _close_resource(
        self, name: str, method: Callable[[], Awaitable[None]], *, clear: str | None = None
    ) -> None:
        """Shut down a single async resource with error handling."""
        try:
            await method()
        except asyncio.CancelledError:
            logger.warning(f"Cancelled while stopping {name}")
        except Exception as e:
            logger.error(f"Error stopping {name}", extra={"error": str(e)}, exc_info=True)
        finally:
            if clear:
                setattr(self, clear, None)

    async def _stop_stats_logger(self) -> None:
        if self._stats_logger:
            await self._stats_logger.stop()

    async def _cancel_flush_task(self) -> None:
        if self._flush_task and not self._flush_task.done():
            self._flush_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._flush_task

    async def _flush_pending_batch(self) -> None:
        async with self._batch_lock:
            if self._batch:
                logger.info(
                    "Flushing pending batch on shutdown",
                    extra={"batch_size": len(self._batch)},
                )
                await self._flush_batch()

    async def _stop_consumer(self) -> None:
        if self.consumer:
            await self.consumer.stop()

    async def stop(self) -> None:
        """
        Stop the ClaimX result processor.

        Gracefully shuts down the consumer, flushes pending batches,
        and logs final statistics.
        """
        logger.info("Stopping ClaimXResultProcessor")
        self._running = False

        await self._close_resource("stats_logger", self._stop_stats_logger)
        await self._close_resource("flush_task", self._cancel_flush_task)
        await self._close_resource("pending_batch", self._flush_pending_batch)
        await self._close_resource("consumer", self._stop_consumer, clear="consumer")
        await self._close_resource("health_server", self.health_server.stop)

        logger.info(
            "ClaimXResultProcessor stopped successfully",
            extra={
                "batches_written": self._batches_written,
                "total_records_written": self._total_records_written,
            },
        )

    async def request_shutdown(self) -> None:
        """Request graceful shutdown."""
        logger.info("Graceful shutdown requested for ClaimXResultProcessor")
        self._running = False

    async def _handle_result_message(self, record: PipelineMessage) -> None:
        """
        Process a single upload result message.

        Routes valid results to the batch buffer.
        Triggers flush if batch size threshold is reached.

        Args:
            record: PipelineMessage containing ClaimXUploadResultMessage JSON
        """
        # Parse ClaimXUploadResultMessage
        try:
            result = ClaimXUploadResultMessage.model_validate_json(record.value)
        except ValidationError as e:
            log_worker_error(
                logger,
                "Failed to parse ClaimXUploadResultMessage",
                error_category="permanent",
                exc=e,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                trace_id=record.key.decode("utf-8") if record.key else None,
            )
            record_processing_error(record.topic, self.consumer_group, "parse_error")
            raise PermanentError(str(e)) from e

        # Update statistics and logs
        self._records_processed += 1
        self._update_cycle_offsets(record.timestamp)
        set_log_context(trace_id=result.trace_id)

        if result.status == "completed":
            self._records_succeeded += 1
            logger.debug(
                "Upload completed successfully",
                extra={"media_id": result.media_id, "blob_path": result.blob_path},
            )
            await self._accumulate_result(result)

        elif result.status in ("failed_permanent", "failed"):
            self._records_failed += 1
            category = "permanent" if result.status == "failed_permanent" else "transient"
            label = "Upload failed permanently" if category == "permanent" else "Upload failed (transient)"
            log_worker_error(
                logger, label,
                error_category=category,
                media_id=result.media_id,
                upstream_error=result.error_message,
            )

        # Record message consumption metric
        record_message_consumed(
            record.topic,
            self.consumer_group,
            len(record.value),
            success=True,
        )

    def _update_cycle_offsets(self, ts: int | None) -> None:
        """Track min/max message timestamps for stats reporting."""
        if ts is None:
            return
        if self._cycle_offset_start_ts is None or ts < self._cycle_offset_start_ts:
            self._cycle_offset_start_ts = ts
        if self._cycle_offset_end_ts is None or ts > self._cycle_offset_end_ts:
            self._cycle_offset_end_ts = ts

    async def _accumulate_result(self, result: ClaimXUploadResultMessage) -> None:
        """Add a completed result to the batch, flushing if size threshold reached."""
        if not self.inventory_writer:
            return
        async with self._batch_lock:
            self._batch.append(result)
            if len(self._batch) >= self.batch_size:
                await self._flush_batch()

    async def _periodic_flush(self) -> None:
        """
        Background task for timeout-based batch flushing.
        """
        while self._running:
            await asyncio.sleep(1)

            async with self._batch_lock:
                elapsed = time.monotonic() - self._last_flush
                if self._batch and elapsed >= self.batch_timeout_seconds:
                    logger.debug(
                        "Batch timeout threshold reached, flushing",
                        extra={"batch_size": len(self._batch), "elapsed": elapsed},
                    )
                    await self._flush_batch()

    async def _flush_batch(self) -> None:
        """
        Flush current batch to Delta Lake.

        Assumes lock is held by caller.
        """
        if not self._batch or not self.inventory_writer:
            return

        batch = self._batch
        batch_size = len(batch)
        self._batch = []
        self._last_flush = time.monotonic()

        batch_id = uuid.uuid4().hex[:8]

        try:
            # Convert batch to Polars DataFrame for efficient merge
            now = datetime.now(UTC)
            rows = []

            for upload_result in batch:
                rows.append(
                    {
                        "media_id": upload_result.media_id,
                        "project_id": upload_result.project_id,
                        "file_name": upload_result.file_name,
                        "file_type": upload_result.file_type,
                        "blob_path": upload_result.blob_path,
                        "bytes": upload_result.bytes_uploaded,
                        "trace_id": upload_result.trace_id,
                        "created_at": now,
                        "updated_at": now,
                    }
                )

            df = pl.DataFrame(rows)

            # Write to Delta
            await self.inventory_writer._async_merge(
                df,
                merge_keys=["media_id"],
                preserve_columns=["created_at"],
            )

            record_delta_write(
                table="claimx_attachments",
                event_count=batch_size,
                success=True,
            )

            # Commit offsets only after successful write
            if self.consumer:
                await self.consumer.commit()

            self._batches_written += 1
            self._total_records_written += batch_size

            logger.debug(
                "Successfully flushed batch of records",
                extra={
                    "batch_id": batch_id,
                    "batch_size": batch_size,
                    "batches_written": self._batches_written,
                },
            )

        except Exception as e:
            log_worker_error(
                logger,
                "Failed to flush batch to Delta",
                error_category="transient",
                exc=e,
                batch_id=batch_id,
                batch_size=batch_size,
            )

    def _get_cycle_stats(self, cycle_count: int) -> tuple[str, dict[str, Any]]:
        """Return current stats for PeriodicStatsLogger."""
        extra = {
            "records_processed": self._records_processed,
            "records_succeeded": self._records_succeeded,
            "records_failed": self._records_failed,
            "records_skipped": self._records_skipped,
            "records_deduplicated": 0,
            "batches_written": self._batches_written,
            "total_records_written": self._total_records_written,
            "cycle_offset_start_ts": self._cycle_offset_start_ts,
            "cycle_offset_end_ts": self._cycle_offset_end_ts,
        }
        self._cycle_offset_start_ts = None
        self._cycle_offset_end_ts = None
        return "", extra


__all__ = ["ClaimXResultProcessor"]
