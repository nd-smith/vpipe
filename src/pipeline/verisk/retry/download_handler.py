"""
Download retry handler for Verisk domain.

Handles retry routing logic for failed download tasks with exponential
backoff via dedicated retry topics. Routes exhausted retries to dead-letter
queue (DLQ).
"""

import logging
from datetime import UTC, datetime, timedelta

from config.config import MessageConfig
from core.types import ErrorCategory
from pipeline.common.metrics import (
    record_dlq_message,
)
from pipeline.common.transport import create_producer
from pipeline.verisk.schemas.results import FailedDownloadMessage
from pipeline.verisk.schemas.tasks import DownloadTaskMessage

logger = logging.getLogger(__name__)


class RetryHandler:
    """
    Handles retry logic via Kafka topics with exponential backoff.

    Routes failed download tasks to appropriate retry topics based on
    retry count, or to DLQ when retries are exhausted. Preserves error
    context through retry chain for observability.

    The retry delays are configurable via MessageConfig:
    - Default: [300s, 600s, 1200s, 2400s] (5m, 10m, 20m, 40m)
    - Retry topics: {pending_topic}.retry.{delay}m

    Usage:
        >>> config = MessageConfig.from_env()
        >>> retry_handler = RetryHandler(config)
        >>> await retry_handler.start()
        >>>
        >>> # Handle a failed task
        >>> await retry_handler.handle_failure(
        ...     task=download_task,
        ...     error=ConnectionError("Network timeout"),
        ...     error_category=ErrorCategory.TRANSIENT
        ... )
        >>> await retry_handler.stop()
    """

    def __init__(
        self,
        config: MessageConfig,
        domain: str = "verisk",
    ):
        """
        Initialize retry handler.

        Args:
            config: Kafka configuration with retry settings
            domain: Domain identifier (default: "verisk")
        """
        self.config = config
        self.domain = domain

        # Retry configuration
        self._retry_delays = config.get_retry_delays(domain)
        self._max_retries = config.get_max_retries(domain)
        self._dlq_topic = config.get_topic(domain, "dlq")

        # Dedicated producers (created in start())
        self._retry_producer = None
        self._dlq_producer = None

        logger.info(
            "Initialized RetryHandler",
            extra={
                "domain": domain,
                "retry_delays": self._retry_delays,
                "max_retries": self._max_retries,
                "dlq_topic": self._dlq_topic,
            },
        )

    async def start(self) -> None:
        """Create and start dedicated producers for retry and DLQ topics."""
        self._retry_producer = create_producer(
            config=self.config,
            domain=self.domain,
            worker_name="download_retry",
            topic_key="retry",
        )
        await self._retry_producer.start()

        # Sync topic with producer's actual entity name (Event Hub entity may
        # differ from the Kafka topic name resolved by get_topic()).
        if hasattr(self._retry_producer, "eventhub_name"):
            self._retry_topic_resolved = self._retry_producer.eventhub_name

        self._dlq_producer = create_producer(
            config=self.config,
            domain=self.domain,
            worker_name="download_retry",
            topic_key="dlq",
        )
        await self._dlq_producer.start()

        if hasattr(self._dlq_producer, "eventhub_name"):
            self._dlq_topic = self._dlq_producer.eventhub_name

        logger.info(
            "RetryHandler producers started",
            extra={
                "dlq_topic": self._dlq_topic,
            },
        )

    async def stop(self) -> None:
        """Stop dedicated producers."""
        if self._retry_producer:
            await self._retry_producer.stop()
            self._retry_producer = None
        if self._dlq_producer:
            await self._dlq_producer.stop()
            self._dlq_producer = None
        logger.info("RetryHandler producers stopped")

    @property
    def dlq_topic(self) -> str:
        """Return the dead-letter queue topic name."""
        return self._dlq_topic

    async def handle_failure(
        self,
        task: DownloadTaskMessage,
        error: Exception,
        error_category: ErrorCategory,
    ) -> None:
        """
        Route failed task to appropriate retry topic or DLQ.

        Determines routing based on retry count and error category:
        - TRANSIENT errors: retry with exponential backoff
        - AUTH errors: retry (credentials may refresh)
        - PERMANENT errors: send directly to DLQ (no retry)
        - CIRCUIT_OPEN: retry (circuit may close)
        - UNKNOWN: retry conservatively

        Args:
            task: Download task that failed
            error: Exception that caused failure
            error_category: Classification of the error

        Raises:
            RuntimeError: If producer is not started
            Exception: If send to retry topic or DLQ fails
        """
        retry_count = task.retry_count

        logger.info(
            "Handling task failure",
            extra={
                "trace_id": task.trace_id,
                "retry_count": retry_count,
                "error_category": error_category.value,
                "error_type": type(error).__name__,
            },
        )

        # PERMANENT errors skip retry and go straight to DLQ
        if error_category == ErrorCategory.PERMANENT:
            logger.warning(
                "Permanent error detected, sending to DLQ without retry",
                extra={
                    "trace_id": task.trace_id,
                    "error": str(error)[:200],
                },
            )
            record_dlq_message(domain=self.domain, reason="permanent")
            await self._send_to_dlq(task, error, error_category)
            return

        # Check if retries exhausted
        if retry_count >= self._max_retries:
            logger.warning(
                "Retries exhausted, sending to DLQ",
                extra={
                    "trace_id": task.trace_id,
                    "retry_count": retry_count,
                    "max_retries": self._max_retries,
                },
            )
            # record_retry_exhausted removed - metrics simplified
            record_dlq_message(domain=self.domain, reason="exhausted")
            await self._send_to_dlq(task, error, error_category)
            return

        # Send to next retry topic
        await self._send_to_retry_topic(task, error, error_category)

    async def _send_to_retry_topic(
        self,
        task: DownloadTaskMessage,
        error: Exception,
        error_category: ErrorCategory,
    ) -> None:
        """
        Send task to appropriate retry topic with updated metadata.

        Increments retry count and adds error context to metadata.
        Calculates retry_at timestamp based on configured delay.

        Args:
            task: Download task to retry
            error: Exception that caused failure
            error_category: Classification of the error

        Raises:
            RuntimeError: If producer is not started
            Exception: If send to retry topic fails
        """
        retry_count = task.retry_count

        # NEW: Single unified retry topic per domain
        retry_topic = self.config.get_retry_topic(self.domain)
        delay_seconds = self._retry_delays[retry_count]

        # Create updated task with incremented retry count
        updated_task = task.model_copy(deep=True)
        updated_task.retry_count += 1

        # Add error context to metadata (truncate to prevent huge messages)
        error_message = str(error)[:500]
        updated_task.metadata["last_error"] = error_message
        updated_task.metadata["error_category"] = error_category.value

        # Calculate retry timestamp
        retry_at = datetime.now(UTC) + timedelta(seconds=delay_seconds)
        updated_task.metadata["retry_at"] = retry_at.isoformat()

        # NEW: Get target topic for routing
        target_topic = self.config.get_topic(self.domain, "downloads_pending")

        # Record retry attempt metric
        #         record_retry_attempt(
        #             domain=self.domain,
        #             worker_type="download_worker",
        #             error_category=error_category.value,
        #             delay_seconds=delay_seconds,
        #         )

        logger.info(
            "Sending task to retry topic",
            extra={
                "trace_id": task.trace_id,
                "retry_topic": retry_topic,
                "retry_count": updated_task.retry_count,
                "delay_seconds": delay_seconds,
                "retry_at": retry_at.isoformat(),
                "target_topic": target_topic,
            },
        )

        await self._retry_producer.send(
            value=updated_task,
            key=task.trace_id,
            headers={
                "retry_count": str(updated_task.retry_count),
                "scheduled_retry_time": retry_at.isoformat(),
                "retry_delay_seconds": str(delay_seconds),
                "target_topic": target_topic,
                "worker_type": "download_worker",
                "original_key": task.trace_id,
                "error_category": error_category.value,
                "domain": self.domain,
            },
        )

        logger.debug(
            "Task sent to retry topic successfully",
            extra={
                "trace_id": task.trace_id,
                "retry_topic": retry_topic,
                "target_topic": target_topic,
            },
        )

    async def _send_to_dlq(
        self,
        task: DownloadTaskMessage,
        error: Exception,
        error_category: ErrorCategory,
    ) -> None:
        """
        Send task to dead-letter queue (DLQ).

        Creates FailedDownloadMessage with complete context for manual
        review and potential replay. Preserves original task for replay.

        Args:
            task: Download task that failed permanently
            error: Final exception that caused failure
            error_category: Classification of the error

        Raises:
            RuntimeError: If producer is not started
            Exception: If send to DLQ fails
        """
        # Truncate error message to prevent huge DLQ messages
        error_message = str(error)
        if len(error_message) > 500:
            error_message = error_message[:497] + "..."

        # Create DLQ message
        dlq_message = FailedDownloadMessage(
            trace_id=task.trace_id,
            attachment_url=task.attachment_url,
            original_task=task,
            error_category=error_category.value,
            retry_count=task.retry_count,
            failed_at=datetime.now(UTC),
        )

        logger.error(
            "Sending task to DLQ",
            extra={
                "trace_id": task.trace_id,
                "retry_count": task.retry_count,
                "error_category": error_category.value,
                "final_error": error_message[:200],
            },
        )

        await self._dlq_producer.send(
            value=dlq_message,
            key=task.trace_id,
            headers={
                "retry_count": str(task.retry_count),
                "error_category": error_category.value,
                "failed": "true",
            },
        )

        logger.info(
            "Task sent to DLQ successfully",
            extra={
                "trace_id": task.trace_id,
                "dlq_topic": self._dlq_topic,
            },
        )


__all__ = [
    "RetryHandler",
]
