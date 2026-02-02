"""
Retry handler for ClaimX download failures with URL refresh capability.

Handles retry routing logic with exponential backoff via dedicated
retry topics. Refreshes expired presigned URLs before retry. Routes
exhausted retries to dead-letter queue (DLQ).
"""

from datetime import UTC, datetime, timedelta

from config.config import KafkaConfig
from core.logging import get_logger
from core.types import ErrorCategory
from kafka_pipeline.claimx.api_client import ClaimXApiClient
from kafka_pipeline.claimx.handlers.utils import (
    LOG_ERROR_TRUNCATE_LONG,
    LOG_ERROR_TRUNCATE_SHORT,
    LOG_VALUE_TRUNCATE,
)
from kafka_pipeline.claimx.schemas.results import FailedDownloadMessage
from kafka_pipeline.claimx.schemas.tasks import ClaimXDownloadTask
from kafka_pipeline.common.metrics import (
    record_dlq_message,
)
from kafka_pipeline.common.producer import BaseKafkaProducer

logger = get_logger(__name__)


class DownloadRetryHandler:
    """
    Handles retry logic for ClaimX download failures with URL refresh.

    Routes failed download tasks to appropriate retry topics based on
    retry count, or to DLQ when retries are exhausted. Detects expired
    presigned URLs and refreshes them from ClaimX API before retry.

    The retry delays are configurable via KafkaConfig:
    - Default: [300s, 600s, 1200s, 2400s] (5m, 10m, 20m, 40m)
    - Retry topics: claimx.downloads.pending.retry.{delay}s
    - DLQ topic: claimx.downloads.dlq

    URL Refresh Strategy:
    - Detects expired URL errors (403 Forbidden, connection errors)
    - Calls ClaimX API to get fresh presigned URL
    - Updates task with new URL before routing to retry
    - Falls back to DLQ if URL refresh fails

    Usage:
        >>> config = KafkaConfig.from_env()
        >>> producer = BaseKafkaProducer(config)
        >>> api_client = ClaimXApiClient(config)
        >>> await producer.start()
        >>> retry_handler = DownloadRetryHandler(config, producer, api_client)
        >>>
        >>> # Handle a failed download task
        >>> await retry_handler.handle_failure(
        ...     task=download_task,
        ...     error=Exception("403 Forbidden"),
        ...     error_category=ErrorCategory.TRANSIENT
        ... )
    """

    def __init__(
        self,
        config: KafkaConfig,
        producer: BaseKafkaProducer,
        api_client: ClaimXApiClient,
    ):
        """
        Initialize download retry handler.

        """
        self.config = config
        self.producer = producer
        self.api_client = api_client
        self.pending_topic = config.get_topic("claimx", "downloads_pending")
        self.dlq_topic = config.get_topic("claimx", "dlq")
        self._retry_delays = config.get_retry_delays("claimx")
        self._max_retries = config.get_max_retries("claimx")

        logger.info(
            "Initialized DownloadRetryHandler with URL refresh capability",
            extra={
                "retry_delays": self._retry_delays,
                "max_retries": self._max_retries,
                "pending_topic": self.pending_topic,
                "dlq_topic": self.dlq_topic,
            },
        )

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

        # PERMANENT errors skip retry and go straight to DLQ
        if error_category == ErrorCategory.PERMANENT:
            logger.warning(
                "Permanent error detected, sending to DLQ without retry",
                extra={
                    "media_id": task.media_id,
                    "error": str(error)[:LOG_ERROR_TRUNCATE_SHORT],
                },
            )
            await self._send_to_dlq(
                task, error, error_category, url_refresh_attempted=False
            )
            return

        if retry_count >= self._max_retries:
            logger.warning(
                "Retries exhausted, sending to DLQ",
                extra={
                    "media_id": task.media_id,
                    "retry_count": retry_count,
                    "max_retries": self._max_retries,
                },
            )
            # Record retry exhausted metric
            #             record_retry_exhausted(
            #                 domain="claimx",
            #                 error_category=error_category.value,
            #             )
            await self._send_to_dlq(
                task, error, error_category, url_refresh_attempted=False
            )
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
                await self._send_to_dlq(
                    task, error, error_category, url_refresh_attempted=True
                )
                return

        # Send to next retry topic
        await self._send_to_retry_topic(task, error, error_category)

    def _should_refresh_url(
        self, error: Exception, error_category: ErrorCategory
    ) -> bool:
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

    async def _try_refresh_url(
        self, task: ClaimXDownloadTask
    ) -> ClaimXDownloadTask | None:
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

            # Get first media record
            media = media_list[0]

            # Extract new download URL
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

            # Create updated task with new URL
            updated_task = task.model_copy(deep=True)
            updated_task.download_url = new_url

            # Add metadata about URL refresh
            if not hasattr(updated_task, "metadata"):
                updated_task.metadata = {}
            updated_task.metadata["url_refreshed_at"] = datetime.now(
                UTC
            ).isoformat()
            updated_task.metadata["original_url"] = task.download_url[:LOG_ERROR_TRUNCATE_SHORT]  # Truncate

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
        retry_topic = self.config.get_retry_topic("claimx")
        delay_seconds = self._retry_delays[retry_count]

        # Create updated task with incremented retry count
        updated_task = task.model_copy(deep=True)
        updated_task.retry_count += 1

        # Add error context to metadata (truncate to prevent huge messages)
        error_message = str(error)[:LOG_ERROR_TRUNCATE_LONG]
        if not hasattr(updated_task, "metadata"):
            updated_task.metadata = {}
        updated_task.metadata["last_error"] = error_message
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

        # Record retry attempt metric
        #         record_retry_attempt(
        #             domain="claimx",
        #             worker_type="download_worker",
        #             error_category=error_category.value,
        #             delay_seconds=delay_seconds,
        #         )

        # Use source_event_id as key for consistent partitioning across all ClaimX topics
        await self.producer.send(
            topic=retry_topic,
            key=task.source_event_id,
            value=updated_task,
            headers={
                "retry_count": str(updated_task.retry_count),
                "scheduled_retry_time": retry_at.isoformat(),
                "retry_delay_seconds": str(delay_seconds),
                "target_topic": target_topic,
                "worker_type": "download_worker",
                "original_key": task.source_event_id,
                "error_category": error_category.value,
                "domain": "claimx",
            },
        )

        logger.debug(
            "Task sent to retry topic successfully",
            extra={
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
        # Truncate error message to prevent huge DLQ messages
        error_message = str(error)
        dlq_message = FailedDownloadMessage(
            media_id=task.media_id,
            project_id=task.project_id,
            download_url=task.download_url,
            original_task=task,
            error_category=error_category.value,
            retry_count=task.retry_count,
            failed_at=datetime.now(UTC),
        )

        logger.error(
            "Sending task to DLQ",
            extra={
                "media_id": task.media_id,
                "project_id": task.project_id,
                "retry_count": task.retry_count,
                "error_category": error_category.value,
                "url_refresh_attempted": url_refresh_attempted,
                "final_error": error_message[:LOG_ERROR_TRUNCATE_SHORT],
            },
        )

        # Record DLQ routing metric
        # Determine reason: permanent error or exhausted retries
        reason = "permanent" if error_category == ErrorCategory.PERMANENT else "exhausted"
        record_dlq_message(domain="claimx", reason=reason)

        # Use source_event_id as key for consistent partitioning across all ClaimX topics
        await self.producer.send(
            topic=self.dlq_topic,
            key=task.source_event_id,
            value=dlq_message,
            headers={
                "retry_count": str(task.retry_count),
                "error_category": error_category.value,
                "url_refresh_attempted": str(url_refresh_attempted),
                "failed": "true",
            },
        )

        logger.info(
            "Task sent to DLQ successfully",
            extra={
                "media_id": task.media_id,
                "dlq_topic": self.dlq_topic,
            },
        )
