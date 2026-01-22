# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
# 
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""
Retry handler for ClaimX enrichment failures.

Handles retry routing logic with exponential backoff via dedicated
retry topics. Routes exhausted retries to dead-letter queue (DLQ).
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from core.types import ErrorCategory
from config.config import KafkaConfig
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.claimx.schemas.results import FailedEnrichmentMessage
from kafka_pipeline.claimx.schemas.tasks import ClaimXEnrichmentTask

logger = logging.getLogger(__name__)


class EnrichmentRetryHandler:
    """
    Handles retry logic for ClaimX enrichment failures via Kafka topics.

    Routes failed enrichment tasks to appropriate retry topics based on
    retry count, or to DLQ when retries are exhausted. Preserves error
    context through retry chain for observability.

    The retry delays are configurable via KafkaConfig:
    - Default: [300s, 600s, 1200s, 2400s] (5m, 10m, 20m, 40m)
    - Retry topics: claimx.enrichment.pending.retry.{delay}m
    - DLQ topic: claimx.enrichment.dlq

    Usage:
        >>> config = KafkaConfig.from_env()
        >>> producer = BaseKafkaProducer(config)
        >>> await producer.start()
        >>> retry_handler = EnrichmentRetryHandler(config, producer)
        >>>
        >>> # Handle a failed enrichment task
        >>> await retry_handler.handle_failure(
        ...     task=enrichment_task,
        ...     error=ClaimXApiError("API returned 404"),
        ...     error_category=ErrorCategory.PERMANENT
        ... )
    """

    def __init__(
        self,
        config: KafkaConfig,
        producer: BaseKafkaProducer,
    ):
        """
        Initialize enrichment retry handler.

        Args:
            config: Kafka configuration with retry settings
            producer: Kafka producer for sending retry/DLQ messages
        """
        self.config = config
        self.producer = producer
        self.pending_topic = config.get_topic("claimx", "enrichment_pending")
        self.dlq_topic = config.get_topic("claimx", "dlq")

        # Retry configuration
        self._retry_delays = config.get_retry_delays("claimx")
        self._max_retries = config.get_max_retries("claimx")

        logger.info(
            "Initialized EnrichmentRetryHandler",
            extra={
                "retry_delays": self._retry_delays,
                "max_retries": self._max_retries,
                "pending_topic": self.pending_topic,
                "dlq_topic": self.dlq_topic,
            },
        )

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

        # PERMANENT errors skip retry and go straight to DLQ
        if error_category == ErrorCategory.PERMANENT:
            logger.warning(
                "Permanent error detected, sending to DLQ without retry",
                extra={
                    "event_id": task.event_id,
                    "error": str(error)[:200],
                },
            )
            await self._send_to_dlq(task, error, error_category)
            return

        # Check if retries exhausted
        if retry_count >= self._max_retries:
            logger.warning(
                "Retries exhausted, sending to DLQ",
                extra={
                    "event_id": task.event_id,
                    "retry_count": retry_count,
                    "max_retries": self._max_retries,
                },
            )
            await self._send_to_dlq(task, error, error_category)
            return

        # Send to next retry topic
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
        retry_topic = self.config.get_retry_topic("claimx")
        delay_seconds = self._retry_delays[retry_count]

        # Create updated task with incremented retry count
        updated_task = task.model_copy(deep=True)
        updated_task.retry_count += 1

        # Add error context to metadata (truncate to prevent huge messages)
        error_message = str(error)[:500]
        if updated_task.metadata is None:
            updated_task.metadata = {}
        updated_task.metadata["last_error"] = error_message
        updated_task.metadata["error_category"] = error_category.value

        # Calculate retry timestamp
        retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
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

        await self.producer.send(
            topic=retry_topic,
            key=task.event_id,
            value=updated_task,
            headers={
                "retry_count": str(updated_task.retry_count),
                "scheduled_retry_time": retry_at.isoformat(),
                "retry_delay_seconds": str(delay_seconds),
                "target_topic": target_topic,
                "worker_type": "enrichment_worker",
                "original_key": task.event_id,
                "error_category": error_category.value,
                "domain": "claimx",
            },
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
        # Truncate error message to prevent huge DLQ messages
        error_message = str(error)
        if len(error_message) > 500:
            error_message = error_message[:497] + "..."

        # Create DLQ message
        dlq_message = FailedEnrichmentMessage(
            event_id=task.event_id,
            event_type=task.event_type,
            project_id=task.project_id,
            original_task=task,
            final_error=error_message,
            error_category=error_category.value,
            retry_count=task.retry_count,
            failed_at=datetime.now(timezone.utc),
        )

        logger.error(
            "Sending task to DLQ",
            extra={
                "event_id": task.event_id,
                "event_type": task.event_type,
                "project_id": task.project_id,
                "retry_count": task.retry_count,
                "error_category": error_category.value,
                "final_error": error_message[:200],
            },
        )

        await self.producer.send(
            topic=self.dlq_topic,
            key=task.event_id,
            value=dlq_message,
            headers={
                "retry_count": str(task.retry_count),
                "error_category": error_category.value,
                "failed": "true",
            },
        )

        logger.info(
            "Task sent to DLQ successfully",
            extra={
                "event_id": task.event_id,
                "dlq_topic": self.dlq_topic,
            },
        )

