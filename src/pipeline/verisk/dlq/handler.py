"""
XACT dead-letter queue (DLQ) handler for manual review and replay.

Provides XACT DLQ message management with:
- Manual consumption from DLQ topic (no auto-commit)
- Message replay capability to original pending topic
- Manual acknowledgment
"""

import json
import logging

from config.config import KafkaConfig
from pipeline.common.consumer import BaseKafkaConsumer
from pipeline.common.producer import BaseKafkaProducer
from pipeline.common.types import PipelineMessage
from pipeline.verisk.schemas.results import FailedDownloadMessage
from pipeline.verisk.schemas.tasks import DownloadTaskMessage

logger = logging.getLogger(__name__)


class DLQHandler:
    """
    XACT dead-letter queue handler for manual review and replay.

    Provides manual DLQ management with:
    - Manual offset commit (no auto-commit) for review workflow
    - Replay capability to send messages back to pending topic
    - Manual acknowledgment for audit trail
    - Structured logging for compliance

    Usage:
        >>> config = KafkaConfig.from_env()
        >>> handler = DLQHandler(config)
        >>> await handler.start()
        >>>
        >>> # In message handler
        >>> async def handle_dlq_message(record: PipelineMessage):
        ...     dlq_msg = handler.parse_dlq_message(record)
        ...     print(f"DLQ message: {dlq_msg.trace_id}")
        ...     # Review and decide
        ...     await handler.replay_message(record)  # or acknowledge
        >>>
        >>> await handler.stop()
    """

    def __init__(self, config: KafkaConfig):
        """
        Initialize XACT DLQ handler with Kafka configuration.

        Args:
            config: Kafka configuration with DLQ topic settings
        """
        self.config = config
        self.domain = "verisk"
        self._consumer: BaseKafkaConsumer | None = None
        self._producer: BaseKafkaProducer | None = None

        # Get topic names from config
        self._dlq_topic = config.get_topic("verisk", "dlq")
        self._pending_topic = config.get_topic("verisk", "downloads_pending")

        logger.info(
            "Initialized XACT DLQ handler",
            extra={
                "dlq_topic": self._dlq_topic,
                "pending_topic": self._pending_topic,
            },
        )

    async def start(self) -> None:
        """
        Start DLQ handler consumer and producer.

        Creates consumer for DLQ topic and producer for replay operations.
        Consumer uses manual commit for review workflow.

        Raises:
            Exception: If consumer or producer fails to start
        """
        logger.info("Starting XACT DLQ handler")

        # Create producer for replay operations
        self._producer = BaseKafkaProducer(
            config=self.config,
            domain=self.domain,
            worker_name="dlq_handler",
        )
        await self._producer.start()

        # Create consumer for DLQ topic with manual commit
        # The message handler is set to _handle_dlq_message
        self._consumer = BaseKafkaConsumer(
            config=self.config,
            domain=self.domain,
            worker_name="dlq_handler",
            topics=[self._dlq_topic],
            message_handler=self._handle_dlq_message,
        )

        logger.info(
            "XACT DLQ handler started successfully",
            extra={
                "dlq_topic": self._dlq_topic,
                "consumer_group": self.config.get_consumer_group("xact", "dlq_handler"),
            },
        )

        # Start consuming (this blocks until stopped)
        await self._consumer.start()

    async def stop(self) -> None:
        """
        Stop DLQ handler and cleanup resources.

        Stops consumer and producer, commits pending offsets.
        Safe to call multiple times.
        """
        logger.info("Stopping XACT DLQ handler")

        if self._consumer:
            await self._consumer.stop()
            self._consumer = None

        if self._producer:
            await self._producer.stop()
            self._producer = None

        logger.info("XACT DLQ handler stopped successfully")

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

            logger.info(
                "DLQ message received for review",
                extra={
                    "trace_id": dlq_msg.trace_id,
                    "attachment_url": dlq_msg.attachment_url,
                    "retry_count": dlq_msg.retry_count,
                    "error_category": dlq_msg.error_category,
                    "final_error": dlq_msg.final_error,
                    "topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                },
            )

            # In a real implementation, this would trigger manual review workflow
            # For now, just log and acknowledge
            # Note: We don't commit here - manual review workflow will call acknowledge_message

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
            # Don't re-raise - we don't want to fail processing of other DLQ messages

    def parse_dlq_message(self, record: PipelineMessage) -> FailedDownloadMessage:
        """
        Parse DLQ message from PipelineMessage.

        Raises ValueError if message cannot be parsed.
        """
        if not record.value:
            raise ValueError("DLQ message value is empty")

        try:
            # Decode JSON bytes to dict
            message_dict = json.loads(record.value.decode("utf-8"))

            # Parse with Pydantic
            return FailedDownloadMessage.model_validate(message_dict)

        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in DLQ message: {e}")
        except Exception as e:
            raise ValueError(f"Failed to parse DLQ message: {e}")

    async def replay_message(self, record: PipelineMessage) -> None:
        """
        Replay a DLQ message back to the pending topic.

        Extracts the original task from the DLQ message and sends it
        back to the pending topic for reprocessing. Resets retry count to 0.

        Raises RuntimeError if producer not started, ValueError if message cannot be parsed.
        """
        if not self._producer or not self._producer.is_started:
            raise RuntimeError("Producer not started. Call start() first.")

        # Parse DLQ message to get original task
        dlq_msg = self.parse_dlq_message(record)

        # Create new task message with reset retry count
        # This gives the task a fresh start
        replayed_task = DownloadTaskMessage(
            trace_id=dlq_msg.original_task.trace_id,
            media_id=dlq_msg.original_task.media_id,
            attachment_url=dlq_msg.original_task.attachment_url,
            blob_path=dlq_msg.original_task.blob_path,
            status_subtype=dlq_msg.original_task.status_subtype,
            file_type=dlq_msg.original_task.file_type,
            assignment_id=dlq_msg.original_task.assignment_id,
            estimate_version=dlq_msg.original_task.estimate_version,
            event_type=dlq_msg.original_task.event_type,
            event_subtype=dlq_msg.original_task.event_subtype,
            retry_count=0,  # Reset retry count
            original_timestamp=dlq_msg.original_task.original_timestamp,
            metadata={
                **dlq_msg.original_task.metadata,
                "replayed_from_dlq": True,
                "dlq_offset": record.offset,
                "dlq_partition": record.partition,
            },
        )

        logger.info(
            "Replaying DLQ message to pending topic",
            extra={
                "trace_id": dlq_msg.trace_id,
                "attachment_url": dlq_msg.attachment_url,
                "original_retry_count": dlq_msg.retry_count,
                "pending_topic": self._pending_topic,
                "dlq_offset": record.offset,
                "dlq_partition": record.partition,
            },
        )

        # Send to pending topic
        await self._producer.send(
            topic=self._pending_topic,
            key=replayed_task.trace_id,
            value=replayed_task,
            headers={
                "trace_id": replayed_task.trace_id,
                "replayed_from_dlq": "true",
            },
        )

        logger.info(
            "DLQ message replayed successfully",
            extra={
                "trace_id": dlq_msg.trace_id,
                "attachment_url": dlq_msg.attachment_url,
                "pending_topic": self._pending_topic,
            },
        )

        # Don't commit offset here - let caller decide when to acknowledge

    async def acknowledge_message(self, record: PipelineMessage) -> None:
        """
        Acknowledge a DLQ message as processed (commit offset).

        Manually commits the offset for the given message, marking it
        as handled. This removes it from the DLQ from the consumer's
        perspective.

        Should be called after successful replay or manual resolution.

        Raises RuntimeError if consumer not started.
        """
        if not self._consumer or not self._consumer._consumer:
            raise RuntimeError("Consumer not started. Call start() first.")

        dlq_msg = self.parse_dlq_message(record)

        logger.info(
            "Acknowledging DLQ message",
            extra={
                "trace_id": dlq_msg.trace_id,
                "attachment_url": dlq_msg.attachment_url,
                "topic": record.topic,
                "partition": record.partition,
                "offset": record.offset,
            },
        )

        # Commit offset for this message
        await self._consumer._consumer.commit()

        logger.info(
            "DLQ message acknowledged",
            extra={
                "trace_id": dlq_msg.trace_id,
                "topic": record.topic,
                "partition": record.partition,
                "offset": record.offset,
            },
        )

    @property
    def is_running(self):
        """Check if DLQ handler is running and ready to process messages."""
        return (
            self._consumer is not None
            and self._consumer.is_running
            and self._producer is not None
            and self._producer.is_started
        )


__all__ = [
    "DLQHandler",
]
