"""
ClaimX Result Processor - Processes download/upload results and tracks outcomes.

This worker is the final stage of the ClaimX download pipeline:
1. Consumes ClaimXUploadResultMessage from results topic
2. Logs outcomes for monitoring and alerting
3. Emits metrics on success/failure rates
4. Optionally writes to Delta Lake for audit trail

Consumer group: {prefix}-claimx-result-processor
Input topic: claimx.downloads.results
Delta table (optional): claimx_download_results
"""

import asyncio
import contextlib
import json
import time
import uuid
from datetime import UTC, datetime

import polars as pl
from pydantic import ValidationError

from config.config import KafkaConfig
from core.logging.context import set_log_context
from core.logging.setup import get_logger
from core.logging.utilities import format_cycle_output, log_worker_error
from kafka_pipeline.claimx.schemas.results import ClaimXUploadResultMessage
from kafka_pipeline.common.consumer import BaseKafkaConsumer
from kafka_pipeline.common.health import HealthCheckServer
from kafka_pipeline.common.metrics import (
    record_message_consumed,
    record_processing_error,
)
from kafka_pipeline.common.types import PipelineMessage
from kafka_pipeline.common.writers.base import BaseDeltaWriter

logger = get_logger(__name__)


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
        >>> config = KafkaConfig.from_env()
        >>> processor = ClaimXResultProcessor(config)
        >>> await processor.start()
        >>> # Processor runs until stopped
        >>> await processor.stop()
    """

    WORKER_NAME = "result_processor"

    # Batching configuration
    BATCH_SIZE = 2000
    BATCH_TIMEOUT_SECONDS = 5

    def __init__(
        self,
        config: KafkaConfig,
        results_topic: str = "",
        inventory_table_path: str = "",
        batch_size: int | None = None,
        batch_timeout_seconds: float | None = None,
        instance_id: str | None = None,
    ):
        """
        Initialize ClaimX result processor.

        Args:
            config: Kafka configuration for consumer
            results_topic: Topic name for upload results (e.g., "com.allstate.pcesdopodappv1.claimx.downloads.results")
            inventory_table_path: Full abfss:// path to claimx_attachments table (optional)
            batch_size: Optional custom batch size (default: 100)
            batch_timeout_seconds: Optional custom timeout (default: 5.0)
            instance_id: Optional instance ID for multiple workers (default: None)
        """
        self.config = config
        self.domain = "claimx"
        self.worker_name = "result_processor"
        self.instance_id = instance_id

        # Create worker_id with instance suffix (coolname) if provided
        if instance_id:
            self.worker_id = f"{self.WORKER_NAME}-{instance_id}"
        else:
            self.worker_id = self.WORKER_NAME

        # Batching configuration
        self.batch_size = batch_size or self.BATCH_SIZE
        self.batch_timeout_seconds = batch_timeout_seconds or self.BATCH_TIMEOUT_SECONDS

        # Get topic from hierarchical config or use provided/default
        self.results_topic = results_topic or config.get_topic(
            self.domain, "downloads_results"
        )
        self.consumer: BaseKafkaConsumer | None = None

        # Consumer group from hierarchical config
        self.consumer_group = config.get_consumer_group(self.domain, self.worker_name)

        # Batch state
        self._batch: list[ClaimXUploadResultMessage] = []
        self._batch_lock = asyncio.Lock()
        self._last_flush = time.monotonic()
        self._flush_task: asyncio.Task | None = None

        # Cycle output tracking
        self._records_processed = 0
        self._records_succeeded = 0
        self._records_failed = 0
        self._records_skipped = 0
        self._batches_written = 0
        self._total_records_written = 0
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0
        self._cycle_task: asyncio.Task | None = None

        # Cycle-specific metrics (reset each cycle)
        self._last_cycle_processed = 0
        self._last_cycle_failed = 0

        # Initialize Delta writer for inventory if path is provided
        self.inventory_writer: BaseDeltaWriter | None = None
        if inventory_table_path:
            self.inventory_writer = BaseDeltaWriter(
                table_path=inventory_table_path,
                partition_column="project_id",
            )

        self._running = False

        # Health check server - use worker-specific port from config
        processing_config = config.get_worker_config(
            self.domain, self.worker_name, "processing"
        )
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

        from kafka_pipeline.common.telemetry import initialize_worker_telemetry

        initialize_worker_telemetry(self.domain, "result-processor")

        # Start health check server first
        await self.health_server.start()

        # Create and start consumer with message handler
        # Disable auto-commit to allow manual commit after batch write
        self.consumer = BaseKafkaConsumer(
            config=self.config,
            domain=self.domain,
            worker_name=self.worker_name,
            topics=[self.results_topic],
            message_handler=self._handle_result_message,
            enable_message_commit=False,
            instance_id=self.instance_id,
        )

        # Start periodic background tasks
        self._cycle_task = asyncio.create_task(self._periodic_cycle_output())
        self._flush_task = asyncio.create_task(self._periodic_flush())

        # Update health check readiness
        self.health_server.set_ready(kafka_connected=True)

        try:
            # Start consumer (this blocks until stopped)
            await self.consumer.start()
        finally:
            self._running = False

    async def stop(self) -> None:
        """
        Stop the ClaimX result processor.

        Gracefully shuts down the consumer, flushes pending batches,
        and logs final statistics.
        """
        logger.info("Stopping ClaimXResultProcessor")
        self._running = False

        # Cancel background tasks
        for task in [self._cycle_task, self._flush_task]:
            if task and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

        # Flush any pending batch
        async with self._batch_lock:
            if self._batch:
                logger.info(
                    "Flushing pending batch on shutdown",
                    extra={"batch_size": len(self._batch)},
                )
                await self._flush_batch()

        # Stop consumer
        if self.consumer:
            await self.consumer.stop()

        # Stop health check server
        await self.health_server.stop()

        logger.info(
            "ClaimXResultProcessor stopped successfully",
            extra={
                "batches_written": self._batches_written,
                "total_records_written": self._total_records_written,
            },
        )

    async def request_shutdown(self) -> None:
        """
        Request graceful shutdown.

        This method is compatible with the shutdown pattern used in other workers.
        For BaseKafkaConsumer-based workers, stop() handles the graceful shutdown
        logic (flushing batches), but this method allows for a consistent interface.
        """
        logger.info("Graceful shutdown requested for ClaimXResultProcessor")
        # In this implementation, stop() does the heavy lifting of flushing.
        # We can trigger stop() from here if we're not blocking in consume loop,
        # but typical usage is that the caller will call stop() after this.
        pass

    async def _handle_result_message(self, record: PipelineMessage) -> None:
        """
        Process a single upload result message from Kafka.

        Routes valid results to the batch buffer.
        Triggers flush if batch size threshold is reached.

        Args:
            record: PipelineMessage containing ClaimXUploadResultMessage JSON
        """
        # Decode and parse ClaimXUploadResultMessage
        try:
            message_data = json.loads(record.value.decode("utf-8"))
            result = ClaimXUploadResultMessage.model_validate(message_data)
        except (json.JSONDecodeError, ValidationError) as e:
            # Use standardized error logging
            log_worker_error(
                logger,
                "Failed to parse ClaimXUploadResultMessage",
                error_category="permanent",
                exc=e,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
            )
            record_processing_error(record.topic, self.consumer_group, "parse_error")
            # We raise here to let BaseKafkaConsumer handle DLQ routing if configured
            raise

        # Update statistics and logs
        self._records_processed += 1
        set_log_context(trace_id=result.source_event_id)

        if result.status == "completed":
            self._records_succeeded += 1

            # Log success (sampled or debug to reduce noise)
            logger.debug(
                "Upload completed successfully",
                extra={
                    "media_id": result.media_id,
                    "blob_path": result.blob_path,
                },
            )

            # Add to batch if we have a writer
            if self.inventory_writer:
                async with self._batch_lock:
                    self._batch.append(result)

                    # Check if flush needed (size-based)
                    if len(self._batch) >= self.batch_size:
                        await self._flush_batch()

        elif result.status == "failed_permanent":
            self._records_failed += 1
            # Use standardized error logging
            log_worker_error(
                logger,
                "Upload failed permanently",
                error_category="permanent",
                media_id=result.media_id,
                error_message=result.error_message,
            )

        elif result.status == "failed":
            self._records_failed += 1
            # Use standardized error logging
            log_worker_error(
                logger,
                "Upload failed (transient)",
                error_category="transient",
                media_id=result.media_id,
                error_message=result.error_message,
            )

        # Record message consumption metric
        record_message_consumed(
            record.topic,
            self.consumer_group,
            len(record.value),
            success=True,
        )

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
                        "source_event_id": upload_result.source_event_id,
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
            # Use standardized error logging
            log_worker_error(
                logger,
                "Failed to flush batch to Delta",
                error_category="transient",
                exc=e,
                batch_id=batch_id,
                batch_size=batch_size,
            )
            # In a real scenario, we might want to route to a retry topic or DLQ here
            # For now, we unfortunately lose the batch from a Delta perspective,
            # but offsets are NOT committed so they will be reprocessed on restart.

    async def _periodic_cycle_output(self) -> None:
        """
        Background task for periodic cycle logging.
        """
        # Initial cycle output
        logger.info(
            format_cycle_output(
                cycle_count=0,
                succeeded=0,
                failed=0,
                skipped=0,
                deduplicated=0,
            ),
            extra={
                "worker_id": self.worker_id,
                "stage": "result_processing",
                "cycle": 0,
                "cycle_id": "cycle-0",
            },
        )
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0

        try:
            while self._running:
                await asyncio.sleep(1)

                cycle_elapsed = time.monotonic() - self._last_cycle_log
                if cycle_elapsed >= 30:  # 30 matches standard interval
                    self._cycle_count += 1
                    self._last_cycle_log = time.monotonic()

                    async with self._batch_lock:
                        pending = len(self._batch)

                    # Calculate cycle-specific deltas
                    (
                        self._records_processed - self._last_cycle_processed
                    )
                    self._records_failed - self._last_cycle_failed

                    # Use standardized cycle output format
                    logger.info(
                        format_cycle_output(
                            cycle_count=self._cycle_count,
                            succeeded=self._records_succeeded,
                            failed=self._records_failed,
                            skipped=self._records_skipped,
                            deduplicated=0,
                        ),
                        extra={
                            "worker_id": self.worker_id,
                            "stage": "result_processing",
                            "cycle": self._cycle_count,
                            "cycle_id": f"cycle-{self._cycle_count}",
                            "records_processed": self._records_processed,
                            "records_succeeded": self._records_succeeded,
                            "records_failed": self._records_failed,
                            "records_skipped": self._records_skipped,
                            "pending": pending,
                            "total_written": self._total_records_written,
                            "cycle_interval_seconds": 30,
                        },
                    )

                    # Update last cycle counters
                    self._last_cycle_processed = self._records_processed
                    self._last_cycle_failed = self._records_failed

        except asyncio.CancelledError:
            logger.debug("Periodic cycle output task cancelled")
            raise


__all__ = ["ClaimXResultProcessor"]
