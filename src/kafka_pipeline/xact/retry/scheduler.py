"""
Delta batch retry scheduler.

Consumes failed batches from retry topics and attempts to write them
to Delta Lake after the configured delay has elapsed.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from aiokafka.structs import ConsumerRecord

from config.config import KafkaConfig
from kafka_pipeline.common.consumer import BaseKafkaConsumer
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.common.retry.delta_handler import DeltaRetryHandler
from kafka_pipeline.xact.schemas.delta_batch import FailedDeltaBatch
from kafka_pipeline.xact.writers import DeltaEventsWriter

logger = logging.getLogger(__name__)


class DeltaBatchRetryScheduler:
    """
    Scheduler for retrying failed Delta batch writes.

    Consumes from all delta retry topics and attempts to write batches
    to Delta Lake after the configured delay has elapsed. Routes permanently
    failed batches to DLQ.

    The scheduler:
    1. Consumes FailedDeltaBatch messages from retry topics
    2. Checks retry_at timestamp
    3. If delay elapsed: attempts Delta write
    4. If write succeeds: commits offset (batch processed)
    5. If write fails: routes to next retry topic or DLQ

    Usage:
        >>> config = KafkaConfig.from_env()
        >>> producer = BaseKafkaProducer(config)
        >>> await producer.start()
        >>>
        >>> scheduler = DeltaBatchRetryScheduler(
        ...     config=config,
        ...     producer=producer,
        ...     table_path="abfss://workspace@onelake/lakehouse/Tables/xact_events"
        ... )
        >>> await scheduler.start()
        >>> # Scheduler runs until stopped
        >>> await scheduler.stop()
    """

    def __init__(
        self,
        config: KafkaConfig,
        producer: BaseKafkaProducer,
        table_path: str,
    ):
        """
        Initialize Delta batch retry scheduler.

        Args:
            config: Kafka configuration
            producer: Kafka producer for DLQ messages
            table_path: Delta table path for writes
        """
        self.config = config
        self.producer = producer
        self.table_path = table_path
        self.domain = "xact"
        self.worker_name = "delta_retry_scheduler"

        # Initialize retry handler for routing failures
        self.retry_handler = DeltaRetryHandler(
            config=config,
            producer=producer,
            table_path=table_path,
            domain=self.domain,
        )

        # Get retry topics to consume
        self.retry_topics = self.retry_handler.get_all_retry_topics()

        # Initialize Delta writer for retries
        self.delta_writer = DeltaEventsWriter(
            table_path=table_path,
        )

        self._consumer: Optional[BaseKafkaConsumer] = None
        self._running = False

        # Metrics
        self._batches_retried = 0
        self._batches_succeeded = 0
        self._batches_failed = 0
        self._events_written = 0

        logger.info(
            "Initialized DeltaBatchRetryScheduler",
            extra={
                "retry_topics": self.retry_topics,
                "table_path": table_path,
            },
        )

    async def start(self) -> None:
        """
        Start the retry scheduler.

        Creates consumer for retry topics and begins processing.

        Raises:
            RuntimeError: If producer not started
            Exception: If consumer fails to start
        """
        if not self.producer.is_started:
            raise RuntimeError("Producer must be started before scheduler")

        if self._running:
            logger.warning("Scheduler already running, ignoring duplicate start call")
            return

        logger.info(
            "Starting DeltaBatchRetryScheduler",
            extra={"retry_topics": self.retry_topics},
        )

        # Create consumer for all retry topics
        self._consumer = BaseKafkaConsumer(
            config=self.config,
            domain=self.domain,
            worker_name=self.worker_name,
            topics=self.retry_topics,
            message_handler=self._handle_retry_message,
        )

        self._running = True

        # Start consumer (this blocks until stopped)
        try:
            await self._consumer.start()
        except asyncio.CancelledError:
            logger.info("Scheduler consumer cancelled")
            raise
        except Exception as e:
            logger.error(
                "Scheduler consumer failed",
                extra={"error": str(e)},
                exc_info=True,
            )
            raise
        finally:
            self._running = False

    async def stop(self) -> None:
        """
        Stop the retry scheduler gracefully.
        """
        if not self._running:
            logger.debug("Scheduler not running or already stopped")
            return

        logger.info(
            "Stopping DeltaBatchRetryScheduler",
            extra={
                "batches_retried": self._batches_retried,
                "batches_succeeded": self._batches_succeeded,
                "batches_failed": self._batches_failed,
                "events_written": self._events_written,
            },
        )

        self._running = False

        if self._consumer:
            await self._consumer.stop()
            self._consumer = None

        logger.info("DeltaBatchRetryScheduler stopped successfully")

    async def _handle_retry_message(self, message: ConsumerRecord) -> None:
        """
        Handle a message from a retry topic.

        Checks if delay has elapsed and attempts Delta write if ready.
        Routes failures to next retry topic or DLQ.

        Args:
            message: ConsumerRecord from retry topic

        Raises:
            RuntimeError: If delay not elapsed (prevents commit)
            Exception: If processing fails
        """
        # Parse message as FailedDeltaBatch
        try:
            failed_batch = FailedDeltaBatch.model_validate_json(message.value)
        except Exception as e:
            logger.error(
                "Failed to parse retry message as FailedDeltaBatch",
                extra={
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "error": str(e),
                },
                exc_info=True,
            )
            # Re-raise to prevent commit - malformed messages need investigation
            raise ValueError(f"Invalid FailedDeltaBatch: {e}") from e

        batch_id = failed_batch.batch_id
        event_count = failed_batch.event_count

        logger.debug(
            "Processing retry message",
            extra={
                "batch_id": batch_id,
                "event_count": event_count,
                "retry_count": failed_batch.retry_count,
                "topic": message.topic,
            },
        )

        # Check if delay has elapsed
        if failed_batch.retry_at:
            now = datetime.now(timezone.utc)
            retry_at = failed_batch.retry_at

            # Ensure timezone-aware comparison
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)

            if now < retry_at:
                seconds_remaining = (retry_at - now).total_seconds()
                logger.debug(
                    "Retry delay not elapsed, message will be reprocessed",
                    extra={
                        "batch_id": batch_id,
                        "retry_at": retry_at.isoformat(),
                        "seconds_remaining": seconds_remaining,
                    },
                )
                # Raise to prevent commit - message will retry later
                raise RuntimeError(
                    f"Delay not elapsed, {seconds_remaining:.0f}s remaining"
                )

        # Delay elapsed - attempt Delta write
        self._batches_retried += 1

        logger.info(
            "Attempting Delta write for retry batch",
            extra={
                "batch_id": batch_id,
                "event_count": event_count,
                "retry_count": failed_batch.retry_count,
            },
        )

        try:
            success = await self.delta_writer.write_raw_events(failed_batch.events)

            if success:
                self._batches_succeeded += 1
                self._events_written += event_count

                logger.info(
                    "Retry batch written to Delta successfully",
                    extra={
                        "batch_id": batch_id,
                        "event_count": event_count,
                        "retry_count": failed_batch.retry_count,
                        "total_batches_succeeded": self._batches_succeeded,
                    },
                )
                # Offset will be committed (message processed)
                return

            # Write returned False - route to next retry or DLQ
            self._batches_failed += 1
            error_message = "Delta write returned failure status"

            logger.warning(
                "Retry batch write failed",
                extra={
                    "batch_id": batch_id,
                    "event_count": event_count,
                    "retry_count": failed_batch.retry_count,
                },
            )

            await self.retry_handler.handle_batch_failure(
                batch=failed_batch.events,
                error=Exception(error_message),
                retry_count=failed_batch.retry_count,
                error_category=failed_batch.error_category,
                batch_id=batch_id,
                first_failure_at=failed_batch.first_failure_at,
            )

        except Exception as e:
            self._batches_failed += 1

            logger.error(
                "Exception during retry batch write",
                extra={
                    "batch_id": batch_id,
                    "event_count": event_count,
                    "retry_count": failed_batch.retry_count,
                    "error": str(e),
                },
                exc_info=True,
            )

            # Route to next retry or DLQ
            await self.retry_handler.handle_batch_failure(
                batch=failed_batch.events,
                error=e,
                retry_count=failed_batch.retry_count,
                error_category="transient",
                batch_id=batch_id,
                first_failure_at=failed_batch.first_failure_at,
            )

    @property
    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self._running and self._consumer is not None

    @property
    def stats(self) -> dict:
        """Get scheduler statistics."""
        return {
            "batches_retried": self._batches_retried,
            "batches_succeeded": self._batches_succeeded,
            "batches_failed": self._batches_failed,
            "events_written": self._events_written,
            "success_rate": (
                self._batches_succeeded / self._batches_retried
                if self._batches_retried > 0
                else 0.0
            ),
        }


__all__ = ["DeltaBatchRetryScheduler"]
