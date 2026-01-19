"""Generic base retry handler for Kafka pipeline tasks.

Provides common retry logic that can be extended for different task types.
Concrete handlers implement _create_updated_task(), _create_dlq_message(), and _get_task_key().
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

TaskT = TypeVar("TaskT")
FailedMessageT = TypeVar("FailedMessageT")


class BaseRetryHandler(ABC, Generic[TaskT, FailedMessageT]):
    """Base class for retry handlers with exponential backoff via Kafka topics.

    Retry logic:
    1. PERMANENT errors -> DLQ immediately
    2. Retries exhausted -> DLQ
    3. Otherwise -> retry topic with exponential backoff

    Subclasses must implement:
    - _create_updated_task(): Copy task and increment retry_count
    - _create_dlq_message(): Create failed message for DLQ
    - _get_task_key(): Return Kafka partition key
    """

    def __init__(
        self,
        config: KafkaConfig,
        producer: BaseKafkaProducer,
        domain: str,
    ):
        self.config = config
        self.producer = producer
        self.domain = domain
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
        """Route failed task to retry topic or DLQ based on error category and retry count."""
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

        await self._send_to_retry_topic(task, error, error_category)

    async def _send_to_retry_topic(
        self,
        task: TaskT,
        error: Exception,
        error_category: ErrorCategory,
    ) -> None:
        """Send task to retry topic with incremented retry_count and error metadata."""
        retry_count = self._get_retry_count(task)
        retry_topic = self.config.get_retry_topic(self.domain, retry_count)
        delay_seconds = self._retry_delays[retry_count]

        updated_task = self._create_updated_task(task)
        self._add_error_metadata(updated_task, error, error_category, delay_seconds)
        record_retry_attempt(
            domain=self.domain,
            error_category=error_category.value,
            delay_seconds=delay_seconds,
        )

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
        """Send task to DLQ with complete context for manual review."""
        error_message = str(error)
        if len(error_message) > 500:
            error_message = error_message[:497] + "..."

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
        """Create copy of task with retry_count incremented."""
        pass

    @abstractmethod
    def _create_dlq_message(
        self,
        task: TaskT,
        error_message: str,
        error_category: ErrorCategory,
    ) -> FailedMessageT:
        """Create failed message for DLQ with context for manual review."""
        pass

    @abstractmethod
    def _get_task_key(self, task: TaskT) -> str:
        """Get Kafka partition key for the task."""
        pass

    def _get_retry_count(self, task: TaskT) -> int:
        """Get retry count from task (override if stored differently)."""
        return task.retry_count  # type: ignore

    def _add_error_metadata(
        self,
        task: TaskT,
        error: Exception,
        error_category: ErrorCategory,
        delay_seconds: int,
    ) -> None:
        """Add error context to task.metadata (override if different structure)."""
        error_message = str(error)[:500]
        retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
        task.metadata["last_error"] = error_message  # type: ignore
        task.metadata["error_category"] = error_category.value  # type: ignore
        task.metadata["retry_at"] = retry_at.isoformat()  # type: ignore

    def _get_log_context(self, task: TaskT) -> Dict[str, Any]:
        """Get logging context for task (override to add task-specific fields)."""
        return {}


__all__ = [
    "BaseRetryHandler",
    "TaskT",
    "FailedMessageT",
]
