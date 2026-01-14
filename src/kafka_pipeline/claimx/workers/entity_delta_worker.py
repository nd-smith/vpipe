"""
ClaimX Entity Delta Worker - Writes entity rows to Delta Lake tables.

Consumers EntityRowsMessage from Kafka and uses ClaimXEntityWriter to write
to appropriate Delta tables (projects, contacts, media, etc.).
"""

import asyncio
import json
from typing import List, Optional

from aiokafka.structs import ConsumerRecord

from core.logging.setup import get_logger
from core.logging.utilities import format_cycle_output, log_worker_error
from core.types import ErrorCategory
from config.config import KafkaConfig
from kafka_pipeline.common.consumer import BaseKafkaConsumer
from kafka_pipeline.common.health import HealthCheckServer
from kafka_pipeline.common.metrics import record_delta_write
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.common.retry.delta_handler import DeltaRetryHandler
from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage
from kafka_pipeline.claimx.writers.delta_entities import ClaimXEntityWriter

logger = get_logger(__name__)


class ClaimXEntityDeltaWorker(BaseKafkaConsumer):
    """
    Worker to consume entity rows and write to Delta Lake.
    
    Consumes EntityRowsMessage batches from Kafka and writes them to
    ClaimX entity tables (projects, contacts, media, etc.) using ClaimXEntityWriter.
    
    Features:
    - Batch processing
    - Graceful shutdown
    - Retry handling via DeltaRetryHandler
    """

    def __init__(
        self,
        config: KafkaConfig,
        domain: str = "claimx",
        entity_rows_topic: str = "",
        projects_table_path: str = "",
        contacts_table_path: str = "",
        media_table_path: str = "",
        tasks_table_path: str = "",
        task_templates_table_path: str = "",
        external_links_table_path: str = "",
        video_collab_table_path: str = "",
        producer_config: Optional[KafkaConfig] = None,
    ):
        """
        Initialize ClaimX entity delta worker.
        """
        entity_rows_topic = entity_rows_topic or config.get_topic(domain, "entities_rows")
        
        super().__init__(
            config=config,
            domain=domain,
            worker_name="entity_delta_writer",
            topics=[entity_rows_topic],
            message_handler=self._handle_message,
        )

        self.producer: Optional[BaseKafkaProducer] = None
        self.producer_config = producer_config if producer_config else config
        self.retry_handler = None  # Initialized in start()
        
        # Initialize entity writer
        self.entity_writer = ClaimXEntityWriter(
            projects_table_path=projects_table_path,
            contacts_table_path=contacts_table_path,
            media_table_path=media_table_path,
            tasks_table_path=tasks_table_path,
            task_templates_table_path=task_templates_table_path,
            external_links_table_path=external_links_table_path,
            video_collab_table_path=video_collab_table_path,
        )
        
        # Get processing config
        processing_config = config.get_worker_config(domain, "entity_delta_writer", "processing")
        self.batch_size = processing_config.get("batch_size", 100)
        self.batch_timeout_seconds = processing_config.get("batch_timeout_seconds", 30.0)
        self.max_retries = processing_config.get("max_retries", 3)
        
        # Retry config
        self._retry_delays = processing_config.get("retry_delays", [60, 300, 900])
        self._retry_topic_prefix = processing_config.get("retry_topic_prefix", f"{entity_rows_topic}.retry")
        self._dlq_topic = processing_config.get("dlq_topic", f"{entity_rows_topic}.dlq")

        # Batch state
        self._batch: List[EntityRowsMessage] = []
        self._batch_lock = asyncio.Lock()
        self._batch_timer: Optional[asyncio.Task] = None

        # Metrics and cycle output tracking
        self._batches_written = 0
        self._records_processed = 0
        self._records_succeeded = 0
        self._records_failed = 0
        self._records_skipped = 0
        self._last_cycle_log = None
        self._cycle_count = 0
        self._cycle_task: Optional[asyncio.Task] = None

        # Health check server - use worker-specific port from config
        health_port = processing_config.get("health_port", 8086)
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name="claimx-entity-delta-worker",
        )

    async def start(self) -> None:
        """Start the worker."""
        # Start health check server first
        await self.health_server.start()

        # Start producer for retries
        self.producer = BaseKafkaProducer(
            config=self.producer_config,
            domain=self.domain,
            worker_name="entity_delta_writer",
        )
        await self.producer.start()

        # Initialize retry handler
        self.retry_handler = DeltaRetryHandler(
            config=self.producer_config,
            producer=self.producer,
            table_path="claimx_entities", # logical name for retry context
            retry_delays=self._retry_delays,
            retry_topic_prefix=self._retry_topic_prefix,
            dlq_topic=self._dlq_topic,
            domain=self.domain,
        )

        # Start batch timer for periodic flushing
        self._reset_batch_timer()

        # Start cycle output background task
        self._cycle_task = asyncio.create_task(self._periodic_cycle_output())

        # Update health check readiness
        self.health_server.set_ready(kafka_connected=True)

        await super().start()

    async def stop(self) -> None:
        """Stop the worker."""
        # Cancel cycle output task
        if self._cycle_task and not self._cycle_task.done():
            self._cycle_task.cancel()
            try:
                await self._cycle_task
            except asyncio.CancelledError:
                pass

        # Cancel batch timer
        if self._batch_timer:
            self._batch_timer.cancel()
            self._batch_timer = None

        # Flush remaining batch
        await self._flush_batch()

        await super().stop()

        if self.producer:
            await self.producer.stop()

        # Stop health check server
        await self.health_server.stop()

    async def _handle_message(self, record: ConsumerRecord) -> None:
        """
        Handle a single message (add to batch).
        """
        self._records_processed += 1

        try:
            message_data = json.loads(record.value.decode("utf-8"))
            entity_rows = EntityRowsMessage.model_validate(message_data)

            # Add to batch and check if flush needed
            should_flush = False
            async with self._batch_lock:
                self._batch.append(entity_rows)
                if len(self._batch) >= self.batch_size:
                    should_flush = True

            if should_flush:
                await self._flush_batch()
                self._reset_batch_timer()

        except Exception as e:
            # Use standardized error logging
            log_worker_error(
                logger,
                "Failed to parse EntityRowsMessage",
                error_category="permanent",
                exc=e,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
            )
            # Cannot retry parse errors, strict schema
            self._records_failed += 1
            
    async def _flush_batch(self) -> None:
        """Write accumulated batch to Delta Lake."""
        batch_size = 0
        merged_rows = None

        async with self._batch_lock:
            if not self._batch:
                return

            batch_size = len(self._batch)

            # Merge all EntityRowsMessages into one
            merged_rows = EntityRowsMessage()
            for msg in self._batch:
                merged_rows.merge(msg)

            # Don't clear batch yet - only after successful write and commit

        if merged_rows.is_empty():
            logger.warning(
                "Batch contained no entity data to write - messages may have empty entity arrays",
                extra={
                    "messages_in_batch": batch_size,
                    "entity_row_count": merged_rows.row_count(),
                },
            )
            # Clear batch since there's nothing to write
            async with self._batch_lock:
                self._batch.clear()
            return

        try:
            logger.info(
                "Flushing entity batch to Delta tables",
                extra={
                    "messages_in_batch": batch_size,
                    "entity_row_count": merged_rows.row_count(),
                },
            )

            counts = await self.entity_writer.write_all(merged_rows)

            # Commit offsets after successful write
            await self.commit()

            # Only clear batch after both write and commit succeeded
            async with self._batch_lock:
                self._batch.clear()

            total_rows = sum(counts.values())
            self._batches_written += 1
            self._records_succeeded += total_rows

            logger.debug(
                "Entity batch written to Delta tables",
                extra={
                    "tables_written": list(counts.keys()),
                    "total_rows": total_rows,
                    "batches_written": self._batches_written,
                },
            )

            # Metrics
            for table_name, row_count in counts.items():
                record_delta_write(
                    table=f"claimx_{table_name}",
                    event_count=row_count,
                    success=True
                )
                
        except Exception as e:
            self._records_failed += merged_rows.row_count()

            # Classify error to determine handling strategy
            error_category = self._classify_delta_error(e)

            # Use standardized error logging
            log_worker_error(
                logger,
                "Failed to write entity batch to Delta",
                error_category=error_category.value,
                exc=e,
                messages_in_batch=batch_size,
                entity_row_count=merged_rows.row_count(),
            )

            # Extract event data for retry context
            events = []
            for entity_type in ["projects", "contacts", "media", "tasks",
                                "task_templates", "external_links", "video_collab"]:
                entity_list = getattr(merged_rows, entity_type, [])
                for entity in entity_list:
                    events.append({
                        "entity_type": entity_type,
                        "event_id": merged_rows.event_id,
                        "event_type": merged_rows.event_type,
                        "project_id": merged_rows.project_id,
                        **entity,
                    })

            # Handle PERMANENT errors: send to DLQ and clear batch
            if error_category == ErrorCategory.PERMANENT:
                logger.warning(
                    "Permanent Delta write error detected, sending batch to DLQ",
                    extra={
                        "batch_size": batch_size,
                        "error_type": type(e).__name__,
                    },
                )

                # Route to DLQ via retry handler
                if self.retry_handler and events:
                    try:
                        await self.retry_handler.handle_batch_failure(
                            batch=events,
                            error=e,
                            retry_count=0,
                            error_category=error_category.value,
                        )
                        logger.info(
                            "Entity batch sent to DLQ",
                            extra={
                                "event_count": len(events),
                                "event_id": merged_rows.event_id,
                            },
                        )
                    except Exception as retry_error:
                        logger.error(
                            "Failed to send entity batch to DLQ - DATA LOSS",
                            extra={
                                "original_error": str(e),
                                "retry_error": str(retry_error),
                                "event_count": len(events),
                                "event_id": merged_rows.event_id,
                            },
                            exc_info=True,
                        )

                # Clear batch after routing to DLQ since this error won't succeed on retry
                async with self._batch_lock:
                    self._batch.clear()
                return

            # For TRANSIENT errors, route to retry topic but keep batch intact
            logger.info(
                "Transient Delta write error, routing to retry topic",
                extra={
                    "batch_size": batch_size,
                    "error_category": error_category.value,
                    "error_type": type(e).__name__,
                },
            )

            if self.retry_handler and events:
                try:
                    await self.retry_handler.handle_batch_failure(
                        batch=events,
                        error=e,
                        retry_count=0,
                        error_category=error_category.value,
                    )
                    logger.info(
                        "Entity batch sent to retry topic",
                        extra={
                            "event_count": len(events),
                            "event_id": merged_rows.event_id,
                        },
                    )
                except Exception as retry_error:
                    logger.error(
                        "Failed to send entity batch to retry topic - DATA LOSS",
                        extra={
                            "original_error": str(e),
                            "retry_error": str(retry_error),
                            "event_count": len(events),
                            "event_id": merged_rows.event_id,
                        },
                        exc_info=True,
                    )
            elif not self.retry_handler:
                # No retry handler - log critical error
                logger.critical(
                    "Entity batch write failed with no retry handler configured - DATA LOSS",
                    extra={
                        "error": str(e),
                        "messages_in_batch": batch_size,
                        "entity_row_count": merged_rows.row_count(),
                    },
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
        """Timer callback to periodically flush batch regardless of size."""
        try:
            while True:
                await asyncio.sleep(self.batch_timeout_seconds)
                # Check if there's anything to flush (without holding lock during write)
                should_flush = False
                async with self._batch_lock:
                    if self._batch:
                        should_flush = True
                        logger.debug(
                            "Flushing batch on timeout",
                            extra={"batch_size": len(self._batch)},
                        )
                if should_flush:
                    await self._flush_batch()
        except asyncio.CancelledError:
            pass  # Expected on shutdown

    def _reset_batch_timer(self) -> None:
        """Reset the batch flush timer."""
        if self._batch_timer:
            self._batch_timer.cancel()
        self._batch_timer = asyncio.create_task(self._periodic_flush())

    async def _periodic_cycle_output(self) -> None:
        """
        Background task for periodic cycle logging.
        """
        import time as time_module
        # Initial cycle output
        logger.info(format_cycle_output(0, 0, 0, 0))
        self._last_cycle_log = time_module.monotonic()
        self._cycle_count = 0

        try:
            while True:  # Runs until cancelled
                await asyncio.sleep(1)

                cycle_elapsed = time_module.monotonic() - self._last_cycle_log
                if cycle_elapsed >= 30:  # 30 matches standard interval
                    self._cycle_count += 1
                    self._last_cycle_log = time_module.monotonic()

                    # Use standardized cycle output format
                    cycle_msg = format_cycle_output(
                        cycle_count=self._cycle_count,
                        succeeded=self._records_succeeded,
                        failed=self._records_failed,
                        skipped=self._records_skipped,
                    )
                    logger.info(
                        cycle_msg,
                        extra={
                            "cycle": self._cycle_count,
                            "records_processed": self._records_processed,
                            "records_succeeded": self._records_succeeded,
                            "records_failed": self._records_failed,
                            "records_skipped": self._records_skipped,
                            "batches_written": self._batches_written,
                            "cycle_interval_seconds": 30,
                        },
                    )

        except asyncio.CancelledError:
            logger.debug("Periodic cycle output task cancelled")
            raise

