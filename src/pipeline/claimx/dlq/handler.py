"""
ClaimX dead-letter queue (DLQ) handler for manual review and replay.

Provides ClaimX DLQ message management with:
- Manual consumption from DLQ topics (no auto-commit)
- Message replay capability to original pending topics
- Manual acknowledgment
- Support for both enrichment and download DLQ types
"""

import json
import logging

from config.config import KafkaConfig
from pipeline.common.consumer import BaseKafkaConsumer
from pipeline.common.producer import BaseKafkaProducer
from pipeline.common.types import PipelineMessage
from pipeline.claimx.schemas.results import FailedDownloadMessage, FailedEnrichmentMessage
from pipeline.claimx.schemas.tasks import ClaimXDownloadTask, ClaimXEnrichmentTask

logger = logging.getLogger(__name__)


class ClaimXDLQHandler:
    """
    ClaimX dead-letter queue handler for manual review and replay.

    Handles both enrichment and download DLQ messages with:
    - Manual offset commit (no auto-commit) for review workflow
    - Replay capability to send messages back to pending topics
    - Manual acknowledgment for audit trail
    - Structured logging for compliance

    Usage:
        >>> config = KafkaConfig.from_env()
        >>> handler = ClaimXDLQHandler(config, dlq_type="download")
        >>> await handler.start()
        >>>
        >>> # In message handler
        >>> async def handle_dlq_message(record: PipelineMessage):
        ...     dlq_msg = handler.parse_dlq_message(record)
        ...     print(f"DLQ message: {dlq_msg.media_id}")
        ...     # Review and decide
        ...     await handler.replay_message(record)  # or acknowledge
        >>>
        >>> await handler.stop()
    """

    def __init__(self, config: KafkaConfig, dlq_type: str = "download"):
        """
        Initialize ClaimX DLQ handler with Kafka configuration.

        Args:
            config: Kafka configuration with DLQ topic settings
            dlq_type: Type of DLQ ("download" or "enrichment")

        Raises:
            ValueError: If dlq_type is not "download" or "enrichment"
        """
        if dlq_type not in ("download", "enrichment"):
            raise ValueError(
                f"Invalid dlq_type: {dlq_type}. Must be 'download' or 'enrichment'"
            )

        self.config = config
        self.domain = "claimx"
        self.dlq_type = dlq_type
        self._consumer: BaseKafkaConsumer | None = None
        self._producer: BaseKafkaProducer | None = None

        logger.info(
            "Initialized ClaimX DLQ handler",
            extra={"dlq_type": dlq_type},
        )

    async def start(self) -> None:
        """
        Start DLQ handler consumer and producer.

        Creates consumer for DLQ topic and producer for replay operations.
        Consumer uses manual commit for review workflow.

        Raises:
            Exception: If consumer or producer fails to start
        """
        logger.info(
            "Starting ClaimX DLQ handler",
            extra={"dlq_type": self.dlq_type},
        )

        # Create producer for replay operations
        self._producer = BaseKafkaProducer(
            config=self.config,
            domain=self.domain,
            worker_name=f"{self.dlq_type}_dlq_handler",
        )
        await self._producer.start()

        # Consumer is created externally in CLI
        # to allow manual fetch mode

        logger.info(
            "ClaimX DLQ handler started successfully",
            extra={"dlq_type": self.dlq_type},
        )

    async def stop(self) -> None:
        """
        Stop DLQ handler and cleanup resources.

        Stops consumer and producer, commits pending offsets.
        Safe to call multiple times.
        """
        logger.info(
            "Stopping ClaimX DLQ handler",
            extra={"dlq_type": self.dlq_type},
        )

        if self._consumer:
            await self._consumer.stop()
            self._consumer = None

        if self._producer:
            await self._producer.stop()
            self._producer = None

        logger.info("ClaimX DLQ handler stopped successfully")

    async def _handle_dlq_message(self, record: PipelineMessage) -> None:
        """
        Default message handler for DLQ consumption.

        This is a placeholder that logs DLQ messages for review.
        In production, this would be replaced with custom logic
        or the handler would be used in a manual review workflow.

        Args:
            record: PipelineMessage from DLQ topic
        """
        try:
            dlq_msg = self.parse_dlq_message(record)

            if self.dlq_type == "download":
                logger.info(
                    "Download DLQ message received for review",
                    extra={
                        "media_id": dlq_msg.media_id,
                        "project_id": dlq_msg.project_id,
                        "download_url": dlq_msg.download_url[:100],
                        "retry_count": dlq_msg.retry_count,
                        "error_category": dlq_msg.error_category,
                        "topic": record.topic,
                        "partition": record.partition,
                        "offset": record.offset,
                    },
                )
            else:  # enrichment
                logger.info(
                    "Enrichment DLQ message received for review",
                    extra={
                        "event_id": dlq_msg.event_id,
                        "project_id": dlq_msg.project_id,
                        "event_type": dlq_msg.event_type,
                        "retry_count": dlq_msg.retry_count,
                        "error_category": dlq_msg.error_category,
                        "topic": record.topic,
                        "partition": record.partition,
                        "offset": record.offset,
                    },
                )

        except Exception as e:
            logger.error(
                "Failed to parse DLQ message",
                extra={
                    "topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                    "error": str(e),
                },
                exc_info=True,
            )

    def parse_dlq_message(
        self, record: PipelineMessage
    ) -> FailedDownloadMessage | FailedEnrichmentMessage:
        """
        Parse DLQ message from PipelineMessage.

        Returns:
            FailedDownloadMessage or FailedEnrichmentMessage depending on dlq_type

        Raises:
            ValueError: If message cannot be parsed
        """
        if not record.value:
            raise ValueError("DLQ message value is empty")

        try:
            # Decode JSON bytes to dict
            message_dict = json.loads(record.value.decode("utf-8"))

            # Parse with Pydantic based on DLQ type
            if self.dlq_type == "download":
                return FailedDownloadMessage.model_validate(message_dict)
            else:  # enrichment
                return FailedEnrichmentMessage.model_validate(message_dict)

        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in DLQ message: {e}")
        except Exception as e:
            raise ValueError(f"Failed to parse DLQ message: {e}")

    async def replay_message(self, record: PipelineMessage) -> None:
        """
        Replay a DLQ message back to the pending topic.

        Extracts the original task from the DLQ message and sends it
        back to the pending topic for reprocessing. Resets retry count to 0.

        Args:
            record: PipelineMessage from DLQ topic

        Raises:
            RuntimeError: If producer not started
            ValueError: If message cannot be parsed
        """
        if not self._producer or not self._producer.is_started:
            raise RuntimeError("Producer not started. Call start() first.")

        # Parse DLQ message to get original task
        dlq_msg = self.parse_dlq_message(record)

        if self.dlq_type == "download":
            # Create new task message with reset retry count
            replayed_task = ClaimXDownloadTask(
                media_id=dlq_msg.original_task.media_id,
                project_id=dlq_msg.original_task.project_id,
                download_url=dlq_msg.original_task.download_url,
                blob_path=dlq_msg.original_task.blob_path,
                file_type=dlq_msg.original_task.file_type,
                file_name=dlq_msg.original_task.file_name,
                source_event_id=dlq_msg.original_task.source_event_id,
                retry_count=0,  # Reset retry count
                metadata={
                    **(dlq_msg.original_task.metadata or {}),
                    "replayed_from_dlq": True,
                    "dlq_offset": record.offset,
                    "dlq_partition": record.partition,
                },
            )

            logger.info(
                "Replaying download DLQ message to pending topic",
                extra={
                    "media_id": dlq_msg.media_id,
                    "project_id": dlq_msg.project_id,
                    "download_url": dlq_msg.download_url[:100],
                    "original_retry_count": dlq_msg.retry_count,
                    "dlq_offset": record.offset,
                    "dlq_partition": record.partition,
                },
            )

            # Send to downloads_pending topic
            await self._producer.send(
                value=replayed_task,
                key=replayed_task.source_event_id,
                headers={
                    "source_event_id": replayed_task.source_event_id,
                    "replayed_from_dlq": "true",
                },
            )

            logger.info(
                "Download DLQ message replayed successfully",
                extra={
                    "media_id": dlq_msg.media_id,
                    "project_id": dlq_msg.project_id,
                },
            )

        else:  # enrichment
            # Create new task message with reset retry count
            replayed_task = ClaimXEnrichmentTask(
                event_id=dlq_msg.original_task.event_id,
                event_type=dlq_msg.original_task.event_type,
                project_id=dlq_msg.original_task.project_id,
                created_at=dlq_msg.original_task.created_at,
                retry_count=0,  # Reset retry count
                media_id=dlq_msg.original_task.media_id,
                task_assignment_id=dlq_msg.original_task.task_assignment_id,
                video_collaboration_id=dlq_msg.original_task.video_collaboration_id,
                master_file_name=dlq_msg.original_task.master_file_name,
                metadata={
                    **(dlq_msg.original_task.metadata or {}),
                    "replayed_from_dlq": True,
                    "dlq_offset": record.offset,
                    "dlq_partition": record.partition,
                },
            )

            logger.info(
                "Replaying enrichment DLQ message to pending topic",
                extra={
                    "event_id": dlq_msg.event_id,
                    "project_id": dlq_msg.project_id,
                    "event_type": dlq_msg.event_type,
                    "original_retry_count": dlq_msg.retry_count,
                    "dlq_offset": record.offset,
                    "dlq_partition": record.partition,
                },
            )

            # Send to enrichment_pending topic
            await self._producer.send(
                value=replayed_task,
                key=replayed_task.event_id,
                headers={
                    "event_id": replayed_task.event_id,
                    "replayed_from_dlq": "true",
                },
            )

            logger.info(
                "Enrichment DLQ message replayed successfully",
                extra={
                    "event_id": dlq_msg.event_id,
                    "project_id": dlq_msg.project_id,
                },
            )

    async def acknowledge_message(self, record: PipelineMessage) -> None:
        """
        Acknowledge a DLQ message as processed (commit offset).

        Manually commits the offset for the given message, marking it
        as handled. This removes it from the DLQ from the consumer's
        perspective.

        Should be called after successful replay or manual resolution.

        Args:
            record: PipelineMessage from DLQ topic

        Raises:
            RuntimeError: If consumer not started
        """
        if not self._consumer or not self._consumer._consumer:
            raise RuntimeError("Consumer not started. Call start() first.")

        dlq_msg = self.parse_dlq_message(record)

        if self.dlq_type == "download":
            logger.info(
                "Acknowledging download DLQ message",
                extra={
                    "media_id": dlq_msg.media_id,
                    "project_id": dlq_msg.project_id,
                    "topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                },
            )
        else:  # enrichment
            logger.info(
                "Acknowledging enrichment DLQ message",
                extra={
                    "event_id": dlq_msg.event_id,
                    "project_id": dlq_msg.project_id,
                    "topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                },
            )

        # Commit offset
        await self._consumer._consumer.commit()

        logger.info("DLQ message acknowledged successfully")


__all__ = [
    "ClaimXDLQHandler",
]
