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
from pipeline.common.retry.retry_utils import (
    create_dlq_headers,
    create_retry_headers,
    log_retry_decision,
    should_send_to_dlq,
    truncate_error_message,
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
        """
        retry_count = task.retry_count

        logger.debug(
            "Handling task failure",
            extra={
                "media_id": task.media_id,
                "trace_id": task.trace_id,
                "retry_count": retry_count,
                "error_category": error_category.value,
                "error_type": type(error).__name__,
            },
        )

        send_to_dlq, dlq_reason = should_send_to_dlq(error_category, retry_count, self._max_retries)

        if send_to_dlq:
            action = "dlq_permanent" if dlq_reason == "permanent" else "dlq_exhausted"
            log_retry_decision(
                action,
                task.media_id,
                retry_count,
                error_category,
                error,
                extra_context={"trace_id": task.trace_id},
            )
            record_dlq_message(domain=self.domain, reason=dlq_reason)
            await self._send_to_dlq(task, error, error_category)
            return

        log_retry_decision(
            "retry",
            task.media_id,
            retry_count,
            error_category,
            error,
            extra_context={"trace_id": task.trace_id},
        )
        await self._send_to_retry_topic(task, error, error_category)

    async def _send_to_retry_topic(
        self,
        task: DownloadTaskMessage,
        error: Exception,
        error_category: ErrorCategory,
    ) -> None:
        """Send task to retry topic with incremented count and error context."""
        retry_count = task.retry_count

        # NEW: Single unified retry topic per domain
        retry_topic = self.config.get_retry_topic(self.domain)
        delay_seconds = self._retry_delays[retry_count]

        updated_task = task.model_copy(deep=True)
        updated_task.retry_count += 1

        updated_task.metadata["last_error"] = truncate_error_message(error)
        updated_task.metadata["error_category"] = error_category.value

        retry_at = datetime.now(UTC) + timedelta(seconds=delay_seconds)
        updated_task.metadata["retry_at"] = retry_at.isoformat()

        # NEW: Get target topic for routing
        target_topic = self.config.get_topic(self.domain, "downloads_pending")

        logger.info(
            "Sending task to retry topic",
            extra={
                "media_id": task.media_id,
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
            headers=create_retry_headers(
                retry_count=updated_task.retry_count,
                retry_at=retry_at,
                delay_seconds=delay_seconds,
                target_topic=target_topic,
                worker_type="download_worker",
                original_key=task.trace_id,
                error_category=error_category,
                domain=self.domain,
            ),
        )

        logger.debug(
            "Task sent to retry topic successfully",
            extra={
                "media_id": task.media_id,
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
        """Send task to DLQ with complete context for manual review and replay."""
        dlq_message = FailedDownloadMessage(
            trace_id=task.trace_id,
            attachment_url=task.attachment_url,
            original_task=task,
            error_category=error_category.value,
            retry_count=task.retry_count,
            failed_at=datetime.now(UTC),
        )

        reason = "permanent" if error_category == ErrorCategory.PERMANENT else "exhausted"
        logger.warning(
            "Sending task to DLQ",
            extra={
                "media_id": task.media_id,
                "trace_id": task.trace_id,
                "error_category": error_category.value,
                "retry_count": task.retry_count,
                "dlq_reason": reason,
            },
        )

        await self._dlq_producer.send(
            value=dlq_message,
            key=task.trace_id,
            headers=create_dlq_headers(task.retry_count, error_category),
        )

        logger.debug(
            "Task sent to DLQ successfully",
            extra={
                "media_id": task.media_id,
                "trace_id": task.trace_id,
                "dlq_topic": self._dlq_topic,
            },
        )


__all__ = [
    "RetryHandler",
]
