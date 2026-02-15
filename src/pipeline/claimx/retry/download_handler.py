"""
Retry handler for ClaimX download failures with URL refresh capability.

Handles retry routing logic with exponential backoff via dedicated
retry topics. Refreshes expired presigned URLs before retry. Routes
exhausted retries to dead-letter queue (DLQ).
"""

import logging
from datetime import UTC, datetime, timedelta

from config.config import MessageConfig
from core.types import ErrorCategory
from pipeline.claimx.api_client import ClaimXApiClient
from pipeline.claimx.handlers.utils import (
    LOG_ERROR_TRUNCATE_SHORT,
    LOG_VALUE_TRUNCATE,
)
from pipeline.claimx.schemas.results import FailedDownloadMessage
from pipeline.claimx.schemas.tasks import ClaimXDownloadTask
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

logger = logging.getLogger(__name__)


class DownloadRetryHandler:
    """Handles retry logic for ClaimX download failures with URL refresh.

    Routes tasks to retry topics or DLQ based on retry count. Detects expired
    presigned URLs and refreshes them from ClaimX API before retry.
    """

    def __init__(
        self,
        config: MessageConfig,
        api_client: ClaimXApiClient,
        domain: str = "claimx",
    ):
        """
        Initialize download retry handler.

        Args:
            config: Kafka configuration with retry settings
            api_client: ClaimX API client for URL refresh
            domain: Domain identifier (default: "claimx")
        """
        self.config = config
        self.api_client = api_client
        self.domain = domain
        self.pending_topic = config.get_topic(domain, "downloads_pending")
        self.dlq_topic = config.get_topic(domain, "dlq")
        self._retry_delays = config.get_retry_delays(domain)
        self._max_retries = config.get_max_retries(domain)

        # Dedicated producers (created in start())
        self._retry_producer = None
        self._dlq_producer = None

        logger.info(
            "Initialized DownloadRetryHandler with URL refresh capability",
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
            worker_name="download_retry",
            topic_key="retry",
        )
        await self._retry_producer.start()

        self._dlq_producer = create_producer(
            config=self.config,
            domain=self.domain,
            worker_name="download_retry",
            topic_key="dlq",
        )
        await self._dlq_producer.start()

        if hasattr(self._dlq_producer, "eventhub_name"):
            self.dlq_topic = self._dlq_producer.eventhub_name

        logger.info(
            "DownloadRetryHandler producers started",
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
        logger.info("DownloadRetryHandler producers stopped")

    async def handle_failure(
        self,
        task: ClaimXDownloadTask,
        error: Exception,
        error_category: ErrorCategory,
    ) -> None:
        """
        Route failed download task to appropriate retry topic or DLQ.

        Determines routing based on retry count and error category:
        - TRANSIENT errors: check for URL expiration, refresh if needed, then retry
        - AUTH errors: retry (credentials may refresh)
        - PERMANENT errors: send directly to DLQ (no retry)
        - CIRCUIT_OPEN: retry (circuit may close)
        - UNKNOWN: check for URL expiration, then retry conservatively

        For transient/unknown errors, detects expired URLs and refreshes
        them from ClaimX API before routing to retry topic.

            Exception: If send to retry topic or DLQ fails
        """
        retry_count = task.retry_count

        logger.info(
            "Handling download task failure",
            extra={
                "media_id": task.media_id,
                "project_id": task.project_id,
                "download_url": task.download_url[:LOG_VALUE_TRUNCATE],  # Truncate URL
                "retry_count": retry_count,
                "error_category": error_category.value,
                "error_type": type(error).__name__,
            },
        )

        send_to_dlq_flag, dlq_reason = should_send_to_dlq(
            error_category, retry_count, self._max_retries
        )

        if send_to_dlq_flag:
            action = "dlq_permanent" if dlq_reason == "permanent" else "dlq_exhausted"
            log_retry_decision(
                action, task.media_id, retry_count, error_category, error,
                extra_context={"project_id": task.project_id},
            )
            await self._send_to_dlq(task, error, error_category, url_refresh_attempted=False)
            return

        # For transient/unknown errors, check if URL refresh is needed
        if self._should_refresh_url(error, error_category):
            logger.info(
                "Detected potential URL expiration, attempting refresh",
                extra={
                    "media_id": task.media_id,
                    "error": str(error)[:LOG_ERROR_TRUNCATE_SHORT],
                },
            )

            refreshed_task = await self._try_refresh_url(task)
            if refreshed_task:
                task = refreshed_task
                logger.info(
                    "Successfully refreshed download URL",
                    extra={
                        "media_id": task.media_id,
                        "new_url": task.download_url[:LOG_VALUE_TRUNCATE],
                    },
                )
            else:
                # URL refresh failed - send to DLQ
                logger.error(
                    "URL refresh failed, sending to DLQ",
                    extra={
                        "media_id": task.media_id,
                        "retry_count": retry_count,
                    },
                )
                await self._send_to_dlq(task, error, error_category, url_refresh_attempted=True)
                return

        await self._send_to_retry_topic(task, error, error_category)

    def _should_refresh_url(self, error: Exception, error_category: ErrorCategory) -> bool:
        """
        Determine if URL refresh should be attempted based on error.

        URL refresh is attempted for:
        - Transient or unknown errors with HTTP 403 (Forbidden)
        - Errors containing "expired", "forbidden", or "access denied"

        """
        # Only consider URL refresh for transient/unknown errors
        if error_category not in (ErrorCategory.TRANSIENT, ErrorCategory.UNKNOWN):
            return False

        error_str = str(error).lower()

        # Check for indicators of URL expiration
        url_expiration_indicators = [
            "403",
            "forbidden",
            "expired",
            "access denied",
            "unauthorized",
        ]

        return any(indicator in error_str for indicator in url_expiration_indicators)

    async def _try_refresh_url(self, task: ClaimXDownloadTask) -> ClaimXDownloadTask | None:
        """
        Attempt to refresh presigned URL from ClaimX API.

        Calls get_project_media() to retrieve fresh presigned URL for the media.
        Creates updated task with new URL if successful.

        """
        try:
            # Extract media_id (may be string or int)
            try:
                media_id_int = int(task.media_id)
            except ValueError:
                logger.error(
                    "Cannot refresh URL: media_id is not an integer",
                    extra={"media_id": task.media_id},
                )
                return None

            # Extract project_id (may be string or int)
            try:
                project_id_int = int(task.project_id)
            except ValueError:
                logger.error(
                    "Cannot refresh URL: project_id is not an integer",
                    extra={"project_id": task.project_id},
                )
                return None

            # Call API to get fresh media metadata with presigned URL
            media_list = await self.api_client.get_project_media(
                project_id=project_id_int,
                media_ids=[media_id_int],
            )

            if not media_list:
                logger.warning(
                    "Media not found in API response",
                    extra={
                        "media_id": task.media_id,
                        "project_id": task.project_id,
                    },
                )
                return None

            media = media_list[0]

            new_url = media.get("full_download_link")
            if not new_url:
                logger.warning(
                    "No download link in media metadata",
                    extra={
                        "media_id": task.media_id,
                        "media_keys": list(media.keys()),
                    },
                )
                return None

            updated_task = task.model_copy(deep=True)
            updated_task.download_url = new_url

            if updated_task.metadata is None:
                updated_task.metadata = {}
            updated_task.metadata["url_refreshed_at"] = datetime.now(UTC).isoformat()
            updated_task.metadata["original_url"] = task.download_url[
                :LOG_ERROR_TRUNCATE_SHORT
            ]  # Truncate

            return updated_task

        except Exception as e:
            logger.error(
                "Failed to refresh URL from API",
                extra={
                    "media_id": task.media_id,
                    "project_id": task.project_id,
                    "error": str(e),
                },
                exc_info=True,
            )
            return None

    async def _send_to_retry_topic(
        self,
        task: ClaimXDownloadTask,
        error: Exception,
        error_category: ErrorCategory,
    ) -> None:
        """
        Send task to appropriate retry topic with updated metadata.

        Increments retry count and adds error context to metadata.
        Calculates retry_at timestamp based on configured delay.

            Exception: If send to retry topic fails
        """
        retry_count = task.retry_count

        # NEW: Single unified retry topic per domain
        retry_topic = self.config.get_retry_topic(self.domain)
        delay_seconds = self._retry_delays[retry_count]

        updated_task = task.model_copy(deep=True)
        updated_task.retry_count += 1

        if updated_task.metadata is None:
            updated_task.metadata = {}
        updated_task.metadata["last_error"] = truncate_error_message(error)
        updated_task.metadata["error_category"] = error_category.value
        retry_at = datetime.now(UTC) + timedelta(seconds=delay_seconds)
        updated_task.metadata["retry_at"] = retry_at.isoformat()

        # NEW: Get target topic for routing
        target_topic = self.pending_topic

        logger.info(
            "Sending task to retry topic",
            extra={
                "media_id": task.media_id,
                "retry_topic": retry_topic,
                "retry_count": updated_task.retry_count,
                "delay_seconds": delay_seconds,
                "retry_at": retry_at.isoformat(),
                "target_topic": target_topic,
            },
        )

        # Use trace_id as key for consistent partitioning across all ClaimX topics
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
                "trace_id": task.trace_id,
                "media_id": task.media_id,
                "retry_topic": retry_topic,
                "target_topic": target_topic,
            },
        )

    async def _send_to_dlq(
        self,
        task: ClaimXDownloadTask,
        error: Exception,
        error_category: ErrorCategory,
        url_refresh_attempted: bool,
    ) -> None:
        """
        Send task to dead-letter queue (DLQ).

        Creates FailedDownloadMessage with complete context for manual
        review and potential replay. Preserves original task for replay.

            Exception: If send to DLQ fails
        """
        dlq_message = FailedDownloadMessage(
            trace_id=task.trace_id,
            media_id=task.media_id,
            project_id=task.project_id,
            download_url=task.download_url,
            original_task=task,
            error_category=error_category.value,
            retry_count=task.retry_count,
            failed_at=datetime.now(UTC),
        )

        reason = "permanent" if error_category == ErrorCategory.PERMANENT else "exhausted"
        logger.warning(
            "Sending task to DLQ",
            extra={
                "trace_id": task.trace_id,
                "media_id": task.media_id,
                "error_category": error_category.value,
                "project_id": task.project_id,
                "retry_count": task.retry_count,
                "url_refresh_attempted": url_refresh_attempted,
                "dlq_reason": reason,
            },
        )

        # Record DLQ routing metric
        record_dlq_message(domain="claimx", reason=reason)

        # Use trace_id as key for consistent partitioning across all ClaimX topics
        dlq_headers = create_dlq_headers(task.retry_count, error_category)
        dlq_headers["url_refresh_attempted"] = str(url_refresh_attempted)
        await self._dlq_producer.send(
            value=dlq_message,
            key=task.trace_id,
            headers=dlq_headers,
        )

        logger.debug(
            "Task sent to DLQ successfully",
            extra={
                "media_id": task.media_id,
                "dlq_topic": self.dlq_topic,
            },
        )
