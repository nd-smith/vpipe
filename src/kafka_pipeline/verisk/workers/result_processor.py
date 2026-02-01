"""
Result processor worker for batching download results.

Consumes from downloads.results topic and batches results
for efficient Delta table writes.

Features:
- Size-based batching (default: 100 records)
- Timeout-based batching (default: 5 seconds)
- Thread-safe batch accumulation
- Graceful shutdown with pending batch flush
- Writes successful downloads to xact_attachments table
- Writes permanent failures to xact_attachments_failed table (optional)
- Batch ID for log correlation
- Delta write metrics
- Optional max_batches limit for testing
- Retry via Kafka topics on Delta write failure
"""

import asyncio
import contextlib
import time
import uuid

from config.config import KafkaConfig
from core.logging.context import set_log_context
from core.logging.setup import get_logger
from core.logging.utilities import format_cycle_output, log_worker_error
from kafka_pipeline.common.consumer import BaseKafkaConsumer
from kafka_pipeline.common.health import HealthCheckServer
from kafka_pipeline.common.metrics import record_delta_write
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.common.retry.delta_handler import DeltaRetryHandler
from kafka_pipeline.common.telemetry import initialize_worker_telemetry
from kafka_pipeline.common.types import PipelineMessage
from kafka_pipeline.verisk.schemas.results import DownloadResultMessage
from kafka_pipeline.verisk.writers.delta_inventory import (
    DeltaFailedAttachmentsWriter,
    DeltaInventoryWriter,
)
from kafka_pipeline.verisk.workers.worker_defaults import WorkerDefaults

logger = get_logger(__name__)


class ResultProcessor:
    """
    Consumes download results and batches for Delta table writes.

    The result processor is the final stage of the pipeline:
    1. Consumes from downloads.results topic
    2. Routes results by status:
       - success → xact_attachments table
       - failed_permanent → xact_attachments_failed table (if configured)
       - failed_transient → skipped (still retrying)
    3. Batches results by size or timeout
    4. Flushes batches to Delta tables

    Batching Strategy:
    - Size flush: When batch reaches BATCH_SIZE (default: 100)
    - Timeout flush: When BATCH_TIMEOUT_SECONDS elapsed (default: 5s)
    - Shutdown flush: Pending batches flushed on graceful shutdown

    Thread Safety:
    - Uses asyncio.Lock for batch accumulation
    - Safe for concurrent message processing

    Usage:
        >>> config = KafkaConfig.from_env()
        >>> producer = BaseKafkaProducer(config)
        >>> await producer.start()
        >>> processor = ResultProcessor(
        ...     config=config,
        ...     producer=producer,
        ...     inventory_table_path="abfss://.../xact_attachments",
        ...     failed_table_path="abfss://.../xact_attachments_failed",
        ... )
        >>> await processor.start()
        >>> # Processor runs until stopped
        >>> await processor.stop()
    """

    WORKER_NAME = "result_processor"

    # Batching configuration
    BATCH_SIZE = 100
    BATCH_TIMEOUT_SECONDS = 5

    # Cycle output configuration
    CYCLE_LOG_INTERVAL_SECONDS = WorkerDefaults.CYCLE_LOG_INTERVAL_SECONDS

    def __init__(
        self,
        config: KafkaConfig,
        producer: BaseKafkaProducer,
        inventory_table_path: str,
        failed_table_path: str | None = None,
        batch_size: int | None = None,
        batch_timeout_seconds: float | None = None,
        max_batches: int | None = None,
        instance_id: str | None = None,
    ):
        """
        Initialize result processor.

        Args:
            config: Kafka configuration
            producer: Kafka producer for retry topic routing (required)
            inventory_table_path: Full abfss:// path to xact_attachments Delta table
            failed_table_path: Optional path to xact_attachments_failed Delta table.
                               If provided, permanent failures will be written here.
            batch_size: Optional custom batch size (default: 100)
            batch_timeout_seconds: Optional custom timeout (default: 5.0)
            max_batches: Optional limit on batches to write (for testing). None = unlimited.
        """
        self.config = config
        self.producer = producer
        self.batch_size = batch_size or self.BATCH_SIZE
        self.batch_timeout_seconds = batch_timeout_seconds or self.BATCH_TIMEOUT_SECONDS
        self.max_batches = max_batches

        # Domain and worker configuration (must be set before using them)
        self.domain = "verisk"
        self.worker_name = "result_processor"
        self.instance_id = instance_id

        # Create worker_id with instance suffix (ordinal) if provided
        if instance_id:
            self.worker_id = f"{self.WORKER_NAME}-{instance_id}"
        else:
            self.worker_id = self.WORKER_NAME

        # Delta writers
        self._inventory_writer = DeltaInventoryWriter(table_path=inventory_table_path)
        self._failed_writer: DeltaFailedAttachmentsWriter | None = None
        if failed_table_path:
            self._failed_writer = DeltaFailedAttachmentsWriter(
                table_path=failed_table_path
            )

        # Retry handler for failed Delta writes
        self._retry_handler = DeltaRetryHandler(
            config=config,
            producer=producer,
            table_path=inventory_table_path,
            retry_topic_prefix="result-processor.retry",
            dlq_topic="result-processor.dlq",
            domain=self.domain,
        )

        # Batching state - separate batches for success and failed
        self._batch: list[DownloadResultMessage] = []
        self._failed_batch: list[DownloadResultMessage] = []
        self._batch_lock = asyncio.Lock()
        self._last_flush = time.monotonic()

        # Progress tracking
        self._batches_written = 0
        self._failed_batches_written = 0
        self._total_records_written = 0

        # Cycle output tracking
        self._records_processed = 0
        self._records_succeeded = 0
        self._records_failed = 0
        self._records_skipped = 0
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0

        # Store topics for logging
        self._results_topic = config.get_topic(self.domain, "downloads_results")

        # Kafka consumer - disable auto-commit for manual control after Delta writes
        self._consumer = BaseKafkaConsumer(
            config=config,
            domain=self.domain,
            worker_name=self.worker_name,
            topics=[self._results_topic],
            message_handler=self._handle_result,
            enable_message_commit=False,
            instance_id=self.instance_id,
        )

        # Background flush task
        self._flush_task: asyncio.Task | None = None
        self._running = False

        # Health check server - use worker-specific port from config
        processing_config = config.get_worker_config(
            self.domain, self.worker_name, "processing"
        )
        health_port = processing_config.get("health_port", 8094)
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name="xact-result-processor",
        )

        logger.info(
            "Initialized result processor",
            extra={
                "domain": self.domain,
                "worker_id": self.worker_id,
                "worker_name": self.WORKER_NAME,
                "instance_id": instance_id,
                "batch_size": self.batch_size,
                "batch_timeout_seconds": self.batch_timeout_seconds,
                "max_batches": self.max_batches,
                "results_topic": self._results_topic,
                "inventory_table_path": inventory_table_path,
                "failed_table_path": failed_table_path,
                "failed_tracking_enabled": failed_table_path is not None,
            },
        )

    async def start(self) -> None:
        """
        Start the result processor.

        Starts consuming from results topic and begins background flush timer.
        This method runs until stop() is called.

        Raises Exception if consumer fails to start.
        """
        if self._running:
            logger.warning("Result processor already running, ignoring duplicate start")
            return

        logger.info("Starting result processor")
        self._running = True

        initialize_worker_telemetry(self.domain, "result-processor")

        # Start health check server first
        await self.health_server.start()

        # Start background flush task for timeout-based flushing
        self._flush_task = asyncio.create_task(self._periodic_flush())

        # Update health check readiness
        self.health_server.set_ready(kafka_connected=True)

        try:
            # Start consumer (blocks until stopped)
            await self._consumer.start()
        except asyncio.CancelledError:
            logger.info("Result processor cancelled, shutting down")
            raise
        except Exception as e:
            logger.error(
                "Result processor terminated with error",
                extra={"error": str(e)},
                exc_info=True,
            )
            raise
        finally:
            self._running = False

    async def stop(self) -> None:
        """
        Stop the result processor gracefully.

        Flushes any pending batch before stopping consumer.
        Safe to call multiple times.
        """
        if not self._running:
            logger.debug("Result processor not running or already stopped")
            return

        logger.info("Stopping result processor")
        self._running = False

        try:
            # Cancel periodic flush task
            if self._flush_task and not self._flush_task.done():
                self._flush_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._flush_task

            # Flush any pending batches
            async with self._batch_lock:
                if self._batch:
                    logger.info(
                        "Flushing pending success batch on shutdown",
                        extra={"batch_size": len(self._batch)},
                    )
                    await self._flush_batch()
                if self._failed_batch:
                    logger.info(
                        "Flushing pending failed batch on shutdown",
                        extra={"batch_size": len(self._failed_batch)},
                    )
                    await self._flush_failed_batch()

            # Stop consumer
            await self._consumer.stop()

            # Stop health check server
            await self.health_server.stop()

            logger.info(
                "Result processor stopped successfully",
                extra={
                    "batches_written": self._batches_written,
                    "failed_batches_written": self._failed_batches_written,
                    "total_records_written": self._total_records_written,
                },
            )

        except Exception as e:
            logger.error(
                "Error stopping result processor",
                extra={"error": str(e)},
                exc_info=True,
            )
            raise

    async def _handle_result(self, message: PipelineMessage) -> None:
        """
        Handle a single result message.

        Routes results by status:
        - success → inventory batch
        - failed_permanent → failed batch (if tracking enabled)
        - failed_transient → skip (still retrying)

        Triggers flush if size or timeout threshold reached.

        Raises Exception if message parsing or batch flush fails.
        """
        # Track messages received
        self._records_processed += 1

        # Parse message
        try:
            result = DownloadResultMessage.model_validate_json(message.value)
        except Exception as e:
            log_worker_error(
                logger,
                "Failed to parse result message",
                error_category="PERMANENT",
                exc=e,
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
            )
            raise

        # Set logging context
        set_log_context(trace_id=result.trace_id)

        # Route by status
        if result.status == "completed":
            # Add to success batch
            self._records_succeeded += 1
            async with self._batch_lock:
                self._batch.append(result)

                # Check if flush needed (size-based)
                if len(self._batch) >= self.batch_size:
                    logger.debug(
                        "Success batch size threshold reached, flushing",
                        extra={"batch_size": len(self._batch)},
                    )
                    await self._flush_batch()

        elif result.status == "failed_permanent" and self._failed_writer:
            # Add to failed batch (only if tracking enabled)
            self._records_failed += 1
            async with self._batch_lock:
                self._failed_batch.append(result)

                # Check if flush needed (size-based)
                if len(self._failed_batch) >= self.batch_size:
                    logger.debug(
                        "Failed batch size threshold reached, flushing",
                        extra={"batch_size": len(self._failed_batch)},
                    )
                    await self._flush_failed_batch()

        else:
            # Skip transient failures (still retrying) or permanent without writer
            self._records_skipped += 1
            logger.debug(
                "Skipping result",
                extra={
                    "trace_id": result.trace_id,
                    "status": result.status,
                    "reason": (
                        "transient"
                        if result.status == "failed_transient"
                        else "no_failed_writer"
                    ),
                    "attachment_url": result.attachment_url[
                        :100
                    ],  # truncate for logging
                },
            )

    async def _periodic_flush(self) -> None:
        """
        Background task for timeout-based batch flushing and cycle logging.

        Runs continuously while processor is active.
        Flushes batches when timeout threshold exceeded.
        Logs cycle output at regular intervals for operational visibility.
        """
        logger.info(
            f"{format_cycle_output(0, 0, 0, 0)} [cycle output every {self.CYCLE_LOG_INTERVAL_SECONDS}s]",
            extra={
                "worker_id": self.worker_id,
                "stage": "result_processing",
                "cycle": 0,
            },
        )
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0

        try:
            while self._running:
                # Sleep for check interval (1 second)
                await asyncio.sleep(1)

                # Check if timeout threshold exceeded
                async with self._batch_lock:
                    elapsed = time.monotonic() - self._last_flush

                    if elapsed >= self.batch_timeout_seconds:
                        # Flush success batch if not empty
                        if self._batch:
                            logger.debug(
                                "Success batch timeout threshold reached, flushing",
                                extra={
                                    "batch_size": len(self._batch),
                                    "elapsed_seconds": elapsed,
                                },
                            )
                            await self._flush_batch()

                        # Flush failed batch if not empty
                        if self._failed_batch:
                            logger.debug(
                                "Failed batch timeout threshold reached, flushing",
                                extra={
                                    "batch_size": len(self._failed_batch),
                                    "elapsed_seconds": elapsed,
                                },
                            )
                            await self._flush_failed_batch()

                # Log cycle output at regular intervals
                cycle_elapsed = time.monotonic() - self._last_cycle_log
                if cycle_elapsed >= self.CYCLE_LOG_INTERVAL_SECONDS:
                    self._cycle_count += 1
                    self._last_cycle_log = time.monotonic()

                    # Get current batch sizes for the log
                    async with self._batch_lock:
                        pending_success = len(self._batch)
                        pending_failed = len(self._failed_batch)

                    logger.info(
                        format_cycle_output(
                            cycle_count=self._cycle_count,
                            succeeded=self._records_succeeded,
                            failed=self._records_failed,
                            skipped=self._records_skipped,
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
                            "batches_written": self._batches_written,
                            "failed_batches_written": self._failed_batches_written,
                            "total_records_written": self._total_records_written,
                            "pending_success_batch": pending_success,
                            "pending_failed_batch": pending_failed,
                            "cycle_interval_seconds": self.CYCLE_LOG_INTERVAL_SECONDS,
                        },
                    )

        except asyncio.CancelledError:
            logger.debug("Periodic flush task cancelled")
            raise
        except Exception as e:
            logger.error(
                "Error in periodic flush task",
                extra={"error": str(e)},
                exc_info=True,
            )
            raise

    async def _flush_batch(self) -> None:
        """
        Flush current batch (internal, assumes lock held).

        Converts batch to inventory records and writes to Delta.
        On success: commits offsets, updates counters.
        On failure: routes batch to retry topics.

        Note: This method assumes the caller holds self._batch_lock
        """
        if not self._batch:
            return

        # Generate batch ID for log correlation
        batch_id = uuid.uuid4().hex[:8]

        # Snapshot current batch and reset
        batch = self._batch
        self._batch = []
        self._last_flush = time.monotonic()

        batch_size = len(batch)

        # Write to Delta inventory table
        # Uses asyncio.to_thread internally for non-blocking I/O
        success = await self._inventory_writer.write_results(batch)

        # Record metrics
        record_delta_write(
            table="xact_attachments",
            event_count=batch_size,
            success=success,
        )

        if success:
            self._batches_written += 1
            self._total_records_written += batch_size

            # Commit offsets after successful Delta write
            await self._consumer.commit()

            # Build progress message
            if self.max_batches:
                progress = f"Batch {self._batches_written}/{self.max_batches}"
            else:
                progress = f"Batch {self._batches_written}"

            logger.info(
                f"{progress}: Successfully wrote {batch_size} records to xact_attachments",
                extra={
                    "batch_id": batch_id,
                    "batch_size": batch_size,
                    "batches_written": self._batches_written,
                    "total_records_written": self._total_records_written,
                    "first_trace_id": batch[0].trace_id,
                    "last_trace_id": batch[-1].trace_id,
                },
            )
        else:
            # Route failed batch to retry topics
            logger.warning(
                "Delta write failed, routing batch to retry topic",
                extra={
                    "batch_id": batch_id,
                    "batch_size": batch_size,
                    "first_trace_id": batch[0].trace_id,
                    "last_trace_id": batch[-1].trace_id,
                },
            )
            # Convert DownloadResultMessage objects to dicts for retry handler
            batch_dicts = [msg.model_dump() for msg in batch]
            await self._retry_handler.handle_batch_failure(
                batch=batch_dicts,
                error=Exception("Delta write to xact_attachments failed"),
                retry_count=0,
                error_category="transient",
                batch_id=batch_id,
            )

    async def _flush_failed_batch(self) -> None:
        """
        Flush current failed batch (internal, assumes lock held).

        Converts failed batch to records and writes to Delta.
        On success: commits offsets, updates counters.
        On failure: routes batch to retry topics.

        Note: This method assumes the caller holds self._batch_lock
        """
        if not self._failed_batch or not self._failed_writer:
            return

        # Generate batch ID for log correlation
        batch_id = uuid.uuid4().hex[:8]

        # Snapshot current batch and reset
        batch = self._failed_batch
        self._failed_batch = []
        self._last_flush = time.monotonic()

        batch_size = len(batch)

        # Write to Delta failed attachments table
        # Uses asyncio.to_thread internally for non-blocking I/O
        success = await self._failed_writer.write_results(batch)

        # Record metrics
        record_delta_write(
            table="xact_attachments_failed",
            event_count=batch_size,
            success=success,
        )

        if success:
            self._failed_batches_written += 1
            self._total_records_written += batch_size

            # Commit offsets after successful Delta write
            await self._consumer.commit()

            logger.info(
                f"Successfully wrote {batch_size} records to xact_attachments_failed",
                extra={
                    "batch_id": batch_id,
                    "batch_size": batch_size,
                    "failed_batches_written": self._failed_batches_written,
                    "first_trace_id": batch[0].trace_id,
                    "last_trace_id": batch[-1].trace_id,
                },
            )
        else:
            # Route failed batch to retry topics
            logger.warning(
                "Delta write failed, routing batch to retry topic",
                extra={
                    "batch_id": batch_id,
                    "batch_size": batch_size,
                    "table": "xact_attachments_failed",
                    "first_trace_id": batch[0].trace_id,
                    "last_trace_id": batch[-1].trace_id,
                },
            )
            # Convert DownloadResultMessage objects to dicts for retry handler
            batch_dicts = [msg.model_dump() for msg in batch]
            await self._retry_handler.handle_batch_failure(
                batch=batch_dicts,
                error=Exception("Delta write to xact_attachments_failed failed"),
                retry_count=0,
                error_category="transient",
                batch_id=batch_id,
            )

    @property
    def is_running(self) -> bool:
        """Check if result processor is running."""
        return self._running and self._consumer.is_running


__all__ = [
    "ResultProcessor",
]
