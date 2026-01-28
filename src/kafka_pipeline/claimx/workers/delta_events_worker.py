"""
Delta Events Worker - Writes ClaimX events to Delta Lake claimx_events table.

This worker consumes events from the claimx events topic and writes them to
the claimx_events Delta table for analytics.

Separated from ClaimXEventIngesterWorker to follow single-responsibility principle:
- ClaimXEventIngesterWorker: Parse events → produce enrichment tasks
- ClaimXDeltaEventsWorker: Parse events → write to Delta Lake

Features:
- Batch accumulation for efficient Delta writes
- Configurable batch size via processing config
- Graceful shutdown with pending batch flush
- Retry via Kafka topics with exponential backoff

Consumer group: {prefix}-claimx-delta-events (different from ingester)
Input topic: com.allstate.pcesdopodappv1.claimx.events.raw (or configured events topic)
Output: Delta table claimx_events (no Kafka output)
Retry topics: com.allstate.pcesdopodappv1.claimx-delta-events.retry.{delay}m
DLQ topic: com.allstate.pcesdopodappv1.claimx-delta-events.dlq
"""

import asyncio
import json
import time
import uuid
from typing import Any, Dict, List, Optional

from aiokafka.structs import ConsumerRecord

from core.logging.setup import get_logger
from core.logging.utilities import format_cycle_output, log_worker_error
from core.types import ErrorCategory
from config.config import KafkaConfig
from kafka_pipeline.common.consumer import BaseKafkaConsumer
from kafka_pipeline.common.health import HealthCheckServer
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.common.metrics import record_delta_write
from kafka_pipeline.common.retry.delta_handler import DeltaRetryHandler
from kafka_pipeline.claimx.writers import ClaimXEventsDeltaWriter

logger = get_logger(__name__)


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
        >>> config = KafkaConfig.from_env()
        >>> producer = BaseKafkaProducer(config)
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
        config: KafkaConfig,
        producer: BaseKafkaProducer,
        events_table_path: str,
        domain: str = "claimx",
        instance_id: Optional[str] = None,
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
        self.consumer: Optional[BaseKafkaConsumer] = None
        self.producer = producer

        # Create worker_id with instance suffix (coolname) if provided
        if instance_id:
            self.worker_id = f"{self.WORKER_NAME}-{instance_id}"
        else:
            self.worker_id = self.WORKER_NAME

        # Batch configuration - use worker-specific config
        processing_config = config.get_worker_config(domain, "delta_events_writer", "processing")
        self.batch_size = processing_config.get("batch_size", 100)
        self.batch_timeout_seconds = processing_config.get("batch_timeout_seconds", 30.0)

        # Retry configuration from worker processing settings
        self._retry_delays = processing_config.get("retry_delays", [300, 600, 1200, 2400])
        self._retry_topic_prefix = processing_config.get(
            "retry_topic_prefix", "com.allstate.pcesdopodappv1.claimx-delta-events.retry"
        )
        self._dlq_topic = processing_config.get("dlq_topic", "com.allstate.pcesdopodappv1.claimx-delta-events.dlq")

        # Batch state
        self._batch: List[Dict[str, Any]] = []
        self._batch_lock = asyncio.Lock()
        self._batch_timer: Optional[asyncio.Task] = None
        self._batches_written = 0

        # Cycle output tracking
        self._records_processed = 0
        self._records_succeeded = 0
        self._records_failed = 0
        self._records_skipped = 0
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0
        self._cycle_task: Optional[asyncio.Task] = None
        self._running = False

        # Cycle-specific metrics (reset each cycle)
        self._last_cycle_processed = 0
        self._last_cycle_failed = 0

        # Initialize Delta writer
        if not events_table_path:
            raise ValueError("events_table_path is required for ClaimXDeltaEventsWorker")

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
            producer=producer,
            table_path=events_table_path,
            retry_delays=self._retry_delays,
            retry_topic_prefix=self._retry_topic_prefix,
            dlq_topic=self._dlq_topic,
            domain=self.domain,
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
                "retry_topic_prefix": self._retry_topic_prefix,
                "dlq_topic": self._dlq_topic,
            },
        )

    async def start(self) -> None:
        """
        Start the ClaimX delta events worker.

        Initializes consumer and begins consuming events.
        """
        logger.info("Starting ClaimXDeltaEventsWorker")
        self._running = True

        # Initialize telemetry
        from kafka_pipeline.common.telemetry import initialize_telemetry
        import os

        initialize_telemetry(
            service_name=f"{self.domain}-delta-events-worker",
            environment=os.getenv("ENVIRONMENT", "development"),
        )

        # Start health check server first
        await self.health_server.start()

        # Start cycle output background task
        self._cycle_task = asyncio.create_task(self._periodic_cycle_output())

        # Start batch timer
        self._reset_batch_timer()

        # Create and start consumer
        # Disable per-message commits - we commit after batch writes
        self.consumer = BaseKafkaConsumer(
            config=self.config,
            domain=self.domain,
            worker_name="delta_events_writer",
            topics=[self.config.get_topic(self.domain, "events")],
            message_handler=self._handle_event_message,
            enable_message_commit=False,
            instance_id=self.instance_id,
        )

        # Update health check readiness
        self.health_server.set_ready(kafka_connected=True)

        try:
            await self.consumer.start()
        finally:
            self._running = False

    async def stop(self) -> None:
        """
        Stop the ClaimX delta events worker.

        Flushes pending batch and shuts down consumer.
        """
        logger.info("Stopping ClaimXDeltaEventsWorker")
        self._running = False

        # Cancel tasks
        if self._cycle_task:
            self._cycle_task.cancel()
        if self._batch_timer:
            self._batch_timer.cancel()

        # Flush pending batch
        async with self._batch_lock:
            if self._batch:
                logger.info("Flushing remaining batch on shutdown")
                await self._flush_batch()

        # Stop consumer
        if self.consumer:
            await self.consumer.stop()

        # Stop health check server
        await self.health_server.stop()

        logger.info("ClaimXDeltaEventsWorker stopped successfully")

    async def _handle_event_message(self, record: ConsumerRecord) -> None:
        """
        Process a single event message.
        """
        self._records_processed += 1

        try:
            message_data = json.loads(record.value.decode("utf-8"))
        except json.JSONDecodeError as e:
            logger.error("Failed to parse message JSON", exc_info=True)
            return

        async with self._batch_lock:
            self._batch.append(message_data)
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
        from kafka_pipeline.common.telemetry import get_tracer

        tracer = get_tracer(__name__)
        try:
            with tracer.start_active_span("delta.write") as scope:
                span = scope.span if hasattr(scope, "span") else scope
                span.set_tag("span.kind", "client")
                span.set_tag("batch.size", len(batch_to_write))
                span.set_tag("table.name", "claimx_events")
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

    async def _handle_failed_batch(self, batch: List[Dict[str, Any]], error: Exception) -> None:
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

        # Schema validation errors are PERMANENT (won't succeed on retry)
        if "schema" in error_str or "validation" in error_str:
            return ErrorCategory.PERMANENT

        # File not found or path errors are PERMANENT
        if "not found" in error_str or "404" in error_str:
            return ErrorCategory.PERMANENT

        # Permission/auth errors need credential refresh
        if "401" in error_str or "403" in error_str or "unauthorized" in error_str:
            return ErrorCategory.AUTH

        # Timeout and connection errors are TRANSIENT
        if "timeout" in error_str or "timeout" in error_type:
            return ErrorCategory.TRANSIENT

        if "connection" in error_str or "network" in error_str:
            return ErrorCategory.TRANSIENT

        # Throttling errors are TRANSIENT
        if "429" in error_str or "throttl" in error_str or "rate limit" in error_str:
            return ErrorCategory.TRANSIENT

        # Service unavailable is TRANSIENT
        if "503" in error_str or "service unavailable" in error_str:
            return ErrorCategory.TRANSIENT

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

    async def _periodic_cycle_output(self) -> None:
        """Log progress periodically."""
        # Initial cycle output
        logger.info(format_cycle_output(0, 0, 0, 0, 0, 0, 0))
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0

        try:
            while self._running:
                await asyncio.sleep(1)

                cycle_elapsed = time.monotonic() - self._last_cycle_log
                if cycle_elapsed >= self.CYCLE_LOG_INTERVAL_SECONDS:
                    self._cycle_count += 1
                    self._last_cycle_log = time.monotonic()

                    # Calculate cycle-specific deltas
                    processed_cycle = self._records_processed - self._last_cycle_processed
                    errors_cycle = self._records_failed - self._last_cycle_failed

                    # Use standardized cycle output format
                    cycle_msg = format_cycle_output(
                        cycle_count=self._cycle_count,
                        processed_cycle=processed_cycle,
                        processed_total=self._records_processed,
                        errors_cycle=errors_cycle,
                        errors_total=self._records_failed,
                        deduped_cycle=0,
                        deduped_total=0,
                    )
                    logger.info(
                        cycle_msg,
                        extra={
                            "worker_id": self.worker_id,
                            "stage": "delta_write",
                            "cycle": self._cycle_count,
                            "cycle_id": f"cycle-{self._cycle_count}",
                            "records_processed": self._records_processed,
                            "records_succeeded": self._records_succeeded,
                            "records_failed": self._records_failed,
                            "records_skipped": self._records_skipped,
                            "batches_written": self._batches_written,
                            "cycle_interval_seconds": self.CYCLE_LOG_INTERVAL_SECONDS,
                        },
                    )

                    # Update last cycle counters
                    self._last_cycle_processed = self._records_processed
                    self._last_cycle_failed = self._records_failed

        except asyncio.CancelledError:
            logger.debug("Periodic cycle output task cancelled")
            raise


__all__ = ["ClaimXDeltaEventsWorker"]
