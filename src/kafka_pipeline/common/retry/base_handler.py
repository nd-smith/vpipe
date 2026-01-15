"""
Generic base retry handler for Kafka pipeline tasks.

Provides common retry logic that can be extended for different task types.
All concrete retry handlers should inherit from BaseRetryHandler and implement
the abstract methods for task-specific behavior.

Example:
    >>> from kafka_pipeline.common.retry.base_handler import BaseRetryHandler
    >>> from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage
    >>> from kafka_pipeline.xact.schemas.results import FailedDownloadMessage
    >>>
    >>> class DownloadRetryHandler(BaseRetryHandler[DownloadTaskMessage, FailedDownloadMessage]):
    ...     def _create_updated_task(self, task):
    ...         updated = task.model_copy(deep=True)
    ...         updated.retry_count += 1
    ...         return updated
    ...
    ...     def _create_dlq_message(self, task, error, error_category):
    ...         return FailedDownloadMessage(
    ...             trace_id=task.trace_id,
    ...             media_id=task.media_id,
    ...             original_task=task,
    ...             final_error=str(error)[:500],
    ...             error_category=error_category.value,
    ...             retry_count=task.retry_count,
    ...             failed_at=datetime.now(timezone.utc),
    ...         )
    ...
    ...     def _get_task_key(self, task):
    ...         return task.trace_id
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Generic, TypeVar, Optional, Dict, Any

from core.types import ErrorCategory
from config.config import KafkaConfig
from kafka_pipeline.common.metrics import (
    record_retry_attempt,
    record_retry_exhausted,
    record_dlq_message,
)
from kafka_pipeline.common.producer import BaseKafkaProducer

logger = logging.getLogger(__name__)

# Type variables for generic task and failed message types
TaskT = TypeVar("TaskT")
FailedMessageT = TypeVar("FailedMessageT")


class BaseRetryHandler(ABC, Generic[TaskT, FailedMessageT]):
    """
    Base class for retry handlers with exponential backoff via Kafka topics.

    This class provides the common retry logic pattern used across all task types:
    1. Check if error is PERMANENT -> send to DLQ immediately
    2. Check if retries exhausted -> send to DLQ
    3. Otherwise -> send to retry topic with exponential backoff

    Subclasses must implement three abstract methods:
    - _create_updated_task(): How to copy the task and increment retry_count
    - _create_dlq_message(): How to create the failed message for DLQ
    - _get_task_key(): What key to use for Kafka partitioning

    The base handler handles:
    - Checking retry limits
    - Checking for PERMANENT errors
    - Getting retry topic names
    - Calculating retry timestamps with exponential backoff
    - Recording metrics
    - Logging with structured context
    - Sending to Kafka topics

    Attributes:
        config: Kafka configuration
        producer: Kafka producer for sending messages
        domain: Domain identifier (e.g., "xact", "claimx")

    Usage:
        See module-level docstring for example implementation.
    """

    def __init__(
        self,
        config: KafkaConfig,
        producer: BaseKafkaProducer,
        domain: str,
    ):
        """
        Initialize base retry handler.

        Args:
            config: Kafka configuration with retry settings
            producer: Kafka producer for sending retry/DLQ messages
            domain: Domain identifier for metrics and topic names
        """
        self.config = config
        self.producer = producer
        self.domain = domain

        # Load retry configuration from config
        self._retry_delays = config.get_retry_delays(domain)
        self._max_retries = config.get_max_retries(domain)
        self._dlq_topic = config.get_topic(domain, "dlq")

        logger.info(
            f"Initialized {self.__class__.__name__}",
            extra={
                "domain": domain,
                "retry_delays": self._retry_delays,
                "max_retries": self._max_retries,
                "dlq_topic": self._dlq_topic,
            },
        )

    async def handle_failure(
        self,
        task: TaskT,
        error: Exception,
        error_category: ErrorCategory,
    ) -> None:
        """
        Route failed task to appropriate retry topic or DLQ.

        This is the main entry point for handling task failures. It determines
        routing based on retry count and error category:
        - PERMANENT errors: send directly to DLQ (no retry)
        - Retries exhausted: send to DLQ
        - Otherwise: send to next retry topic with exponential backoff

        Args:
            task: Task that failed (must have retry_count attribute)
            error: Exception that caused failure
            error_category: Classification of the error

        Raises:
            RuntimeError: If producer is not started
            Exception: If send to retry topic or DLQ fails
        """
        retry_count = self._get_retry_count(task)

        logger.info(
            "Handling task failure",
            extra={
                **self._get_log_context(task),
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
                    **self._get_log_context(task),
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
                    **self._get_log_context(task),
                    "retry_count": retry_count,
                    "max_retries": self._max_retries,
                },
            )
            record_retry_exhausted(domain=self.domain, error_category=error_category.value)
            record_dlq_message(domain=self.domain, reason="exhausted")
            await self._send_to_dlq(task, error, error_category)
            return

        # Send to next retry topic
        await self._send_to_retry_topic(task, error, error_category)

    async def _send_to_retry_topic(
        self,
        task: TaskT,
        error: Exception,
        error_category: ErrorCategory,
    ) -> None:
        """
        Send task to appropriate retry topic with updated metadata.

        Increments retry count, adds error context to metadata, and calculates
        retry_at timestamp based on configured exponential backoff delay.

        Args:
            task: Task to retry
            error: Exception that caused failure
            error_category: Classification of the error

        Raises:
            RuntimeError: If producer is not started
            Exception: If send to retry topic fails
        """
        retry_count = self._get_retry_count(task)
        retry_topic = self.config.get_retry_topic(self.domain, retry_count)
        delay_seconds = self._retry_delays[retry_count]

        # Create updated task with incremented retry count
        updated_task = self._create_updated_task(task)

        # Add error context to task metadata
        self._add_error_metadata(updated_task, error, error_category, delay_seconds)

        # Record retry attempt metric
        record_retry_attempt(
            domain=self.domain,
            error_category=error_category.value,
            delay_seconds=delay_seconds,
        )

        # Calculate retry timestamp
        retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)

        logger.info(
            "Sending task to retry topic",
            extra={
                **self._get_log_context(updated_task),
                "retry_topic": retry_topic,
                "retry_count": self._get_retry_count(updated_task),
                "delay_seconds": delay_seconds,
                "retry_at": retry_at.isoformat(),
            },
        )

        await self.producer.send(
            topic=retry_topic,
            key=self._get_task_key(updated_task),
            value=updated_task,
            headers={
                "retry_count": str(self._get_retry_count(updated_task)),
                "error_category": error_category.value,
            },
        )

        logger.debug(
            "Task sent to retry topic successfully",
            extra={
                **self._get_log_context(updated_task),
                "retry_topic": retry_topic,
            },
        )

    async def _send_to_dlq(
        self,
        task: TaskT,
        error: Exception,
        error_category: ErrorCategory,
    ) -> None:
        """
        Send task to dead-letter queue (DLQ).

        Creates a failed message with complete context for manual review
        and potential replay. Preserves original task for replay.

        Args:
            task: Task that failed permanently
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

        # Create DLQ message using subclass implementation
        dlq_message = self._create_dlq_message(task, error_message, error_category)

        logger.error(
            "Sending task to DLQ",
            extra={
                **self._get_log_context(task),
                "retry_count": self._get_retry_count(task),
                "error_category": error_category.value,
                "final_error": error_message[:200],
            },
        )

        await self.producer.send(
            topic=self._dlq_topic,
            key=self._get_task_key(task),
            value=dlq_message,
            headers={
                "retry_count": str(self._get_retry_count(task)),
                "error_category": error_category.value,
                "failed": "true",
            },
        )

        logger.info(
            "Task sent to DLQ successfully",
            extra={
                **self._get_log_context(task),
                "dlq_topic": self._dlq_topic,
            },
        )

    @abstractmethod
    def _create_updated_task(self, task: TaskT) -> TaskT:
        """
        Create a copy of the task with retry_count incremented.

        Subclasses must implement this to handle task-specific copying logic.
        Typically this involves:
        1. Deep copy the task (e.g., task.model_copy(deep=True))
        2. Increment retry_count
        3. Return the updated task

        Args:
            task: Original task to copy

        Returns:
            Updated task with retry_count incremented by 1
        """
        pass

    @abstractmethod
    def _create_dlq_message(
        self,
        task: TaskT,
        error_message: str,
        error_category: ErrorCategory,
    ) -> FailedMessageT:
        """
        Create a failed message for the DLQ.

        Subclasses must implement this to create domain-specific failed messages
        with all necessary context for manual review and potential replay.

        Args:
            task: Original task that failed
            error_message: Final error message (truncated to 500 chars)
            error_category: Classification of the error

        Returns:
            Failed message instance ready to send to DLQ
        """
        pass

    @abstractmethod
    def _get_task_key(self, task: TaskT) -> str:
        """
        Get the Kafka partition key for the task.

        Subclasses must implement this to return the appropriate key for
        partitioning in Kafka (e.g., trace_id, event_id, batch_id).

        Args:
            task: Task to get key from

        Returns:
            String key for Kafka partitioning
        """
        pass

    def _get_retry_count(self, task: TaskT) -> int:
        """
        Get the retry count from a task.

        Override this if your task stores retry_count differently.

        Args:
            task: Task to get retry count from

        Returns:
            Current retry count

        Raises:
            AttributeError: If task doesn't have retry_count attribute
        """
        return task.retry_count  # type: ignore

    def _add_error_metadata(
        self,
        task: TaskT,
        error: Exception,
        error_category: ErrorCategory,
        delay_seconds: int,
    ) -> None:
        """
        Add error context to task metadata.

        Override this if your task has different metadata structure.
        Default implementation assumes task has a .metadata dict attribute.

        Args:
            task: Task to add metadata to (modified in place)
            error: Exception that caused failure
            error_category: Classification of the error
            delay_seconds: Delay until next retry
        """
        # Truncate error message to prevent huge messages
        error_message = str(error)[:500]

        # Calculate retry timestamp
        retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)

        # Add error context to metadata
        task.metadata["last_error"] = error_message  # type: ignore
        task.metadata["error_category"] = error_category.value  # type: ignore
        task.metadata["retry_at"] = retry_at.isoformat()  # type: ignore

    def _get_log_context(self, task: TaskT) -> Dict[str, Any]:
        """
        Get logging context for a task.

        Override this to add task-specific fields to log messages.
        Default implementation returns empty dict.

        Args:
            task: Task to extract context from

        Returns:
            Dictionary of context fields for structured logging
        """
        return {}


__all__ = [
    "BaseRetryHandler",
    "TaskT",
    "FailedMessageT",
]
