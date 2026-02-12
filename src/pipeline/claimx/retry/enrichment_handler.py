"""
Retry handler for ClaimX enrichment failures.

Handles retry routing logic with exponential backoff via dedicated
retry topics. Routes exhausted retries to dead-letter queue (DLQ).
"""

import logging
from datetime import UTC, datetime, timedelta

from config.config import MessageConfig
from core.types import ErrorCategory
from pipeline.claimx.handlers.utils import (
    LOG_ERROR_TRUNCATE_LONG,
    LOG_ERROR_TRUNCATE_SHORT,
)
from pipeline.claimx.schemas.results import FailedEnrichmentMessage
from pipeline.claimx.schemas.tasks import ClaimXEnrichmentTask
from pipeline.common.retry.retry_utils import (
    create_dlq_headers,
    create_retry_headers,
    log_retry_decision,
    should_send_to_dlq,
    truncate_error_message,
)
from pipeline.common.transport import create_producer

logger = logging.getLogger(__name__)


class EnrichmentRetryHandler:
    """Handles retry logic for ClaimX enrichment failures via Kafka topics.

    Routes tasks to retry topics or DLQ based on retry count and error category.
    """

    def __init__(
        self,
        config: MessageConfig,
        domain: str = "claimx",
    ):
        """
        Initialize enrichment retry handler.

        Args:
            config: Kafka configuration with retry settings
            domain: Domain identifier (default: "claimx")
        """
        self.config = config
        self.domain = domain
        self.pending_topic = config.get_topic(domain, "enrichment_pending")
        self.dlq_topic = config.get_topic(domain, "dlq")

        # Retry configuration
        self._retry_delays = config.get_retry_delays(domain)
        self._max_retries = config.get_max_retries(domain)

        # Dedicated producers (created in start())
        self._retry_producer = None
        self._dlq_producer = None

        logger.info(
            "Initialized EnrichmentRetryHandler",
            extra={
                "retry_delays": self._retry_delays,
                "max_retries": self._max_retries,
                "pending_topic": self.pending_topic,
                "dlq_topic": self.dlq_topic,
            },
        )

    async def start(self) -> None:
        """Create and start dedicated producers for retry and DLQ topics."""
        self._retry_producer = create_producer(
            config=self.config,
            domain=self.domain,
            worker_name="enrichment_retry",
            topic_key="retry",
        )
        await self._retry_producer.start()

        self._dlq_producer = create_producer(
            config=self.config,
            domain=self.domain,
            worker_name="enrichment_retry",
            topic_key="dlq",
        )
        await self._dlq_producer.start()

        if hasattr(self._dlq_producer, "eventhub_name"):
            self.dlq_topic = self._dlq_producer.eventhub_name

        logger.info(
            "EnrichmentRetryHandler producers started",
            extra={
                "dlq_topic": self.dlq_topic,
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
        logger.info("EnrichmentRetryHandler producers stopped")

    async def handle_failure(
        self,
        task: ClaimXEnrichmentTask,
        error: Exception,
        error_category: ErrorCategory,
    ) -> None:
        """
        Route failed enrichment task to appropriate retry topic or DLQ.

        Determines routing based on retry count and error category:
        - TRANSIENT errors: retry with exponential backoff
        - AUTH errors: retry (credentials may refresh)
        - PERMANENT errors: send directly to DLQ (no retry)
        - CIRCUIT_OPEN: retry (circuit may close)
        - UNKNOWN: retry conservatively

        Args:
            task: Enrichment task that failed
            error: Exception that caused failure
            error_category: Classification of the error

        Raises:
            RuntimeError: If producer is not started
            Exception: If send to retry topic or DLQ fails
        """
        retry_count = task.retry_count

        logger.info(
            "Handling enrichment task failure",
            extra={
                "event_id": task.event_id,
                "event_type": task.event_type,
                "project_id": task.project_id,
                "retry_count": retry_count,
                "error_category": error_category.value,
                "error_type": type(error).__name__,
            },
        )

        send_to_dlq, dlq_reason = should_send_to_dlq(error_category, retry_count, self._max_retries)

        if send_to_dlq:
            action = "dlq_permanent" if dlq_reason == "permanent" else "dlq_exhausted"
            log_retry_decision(action, task.event_id, retry_count, error_category, error)
            await self._send_to_dlq(task, error, error_category)
            return

        log_retry_decision("retry", task.event_id, retry_count, error_category, error)
        await self._send_to_retry_topic(task, error, error_category)

    async def _send_to_retry_topic(
        self,
        task: ClaimXEnrichmentTask,
        error: Exception,
        error_category: ErrorCategory,
    ) -> None:
        """
        Send task to appropriate retry topic with updated metadata.

        Increments retry count and adds error context to metadata.
        Calculates retry_at timestamp based on configured delay.

        Args:
            task: Enrichment task to retry
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

        if updated_task.metadata is None:
            updated_task.metadata = {}
        updated_task.metadata["last_error"] = truncate_error_message(error)
        updated_task.metadata["error_category"] = error_category.value

        # Calculate retry timestamp
        retry_at = datetime.now(UTC) + timedelta(seconds=delay_seconds)
        updated_task.metadata["retry_at"] = retry_at.isoformat()

        # NEW: Get target topic for routing
        target_topic = self.pending_topic

        logger.info(
            "Sending task to retry topic",
            extra={
                "event_id": task.event_id,
                "retry_topic": retry_topic,
                "retry_count": updated_task.retry_count,
                "delay_seconds": delay_seconds,
                "retry_at": retry_at.isoformat(),
                "target_topic": target_topic,
            },
        )

        await self._retry_producer.send(
            value=updated_task,
            key=task.event_id,
            headers=create_retry_headers(
                retry_count=updated_task.retry_count,
                retry_at=retry_at,
                delay_seconds=delay_seconds,
                target_topic=target_topic,
                worker_type="enrichment_worker",
                original_key=task.event_id,
                error_category=error_category,
                domain=self.domain,
            ),
        )

        logger.debug(
            "Task sent to retry topic successfully",
            extra={
                "event_id": task.event_id,
                "retry_topic": retry_topic,
                "target_topic": target_topic,
            },
        )

    async def _send_to_dlq(
        self,
        task: ClaimXEnrichmentTask,
        error: Exception,
        error_category: ErrorCategory,
    ) -> None:
        """
        Send task to dead-letter queue (DLQ).

        Creates FailedEnrichmentMessage with complete context for manual
        review and potential replay. Preserves original task for replay.

        Args:
            task: Enrichment task that failed permanently
            error: Final exception that caused failure
            error_category: Classification of the error

        Raises:
            RuntimeError: If producer is not started
            Exception: If send to DLQ fails
        """
        # Create DLQ message
        dlq_message = FailedEnrichmentMessage(
            event_id=task.event_id,
            event_type=task.event_type,
            project_id=task.project_id,
            original_task=task,
            final_error=truncate_error_message(error),
            error_category=error_category.value,
            retry_count=task.retry_count,
            failed_at=datetime.now(UTC),
        )

        logger.info(
            "Sending task to DLQ",
            extra={
                "event_id": task.event_id,
                "error_category": error_category.value,
                "event_type": task.event_type,
                "project_id": task.project_id,
                "retry_count": task.retry_count,
            },
        )

        await self._dlq_producer.send(
            value=dlq_message,
            key=task.event_id,
            headers=create_dlq_headers(task.retry_count, error_category),
        )

        logger.debug(
            "Task sent to DLQ successfully",
            extra={
                "event_id": task.event_id,
                "dlq_topic": self.dlq_topic,
            },
        )
