"""
Delta batch retry handler.

Routes failed Delta batch writes to appropriate retry topics with
exponential backoff, or to DLQ when retries are exhausted.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from config.config import KafkaConfig
from kafka_pipeline.common.metrics import (
    record_retry_attempt,
    record_retry_exhausted,
    record_dlq_message,
)
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.xact.schemas.delta_batch import FailedDeltaBatch

logger = logging.getLogger(__name__)

# Default retry delays (seconds): 5m, 10m, 20m, 40m
DEFAULT_DELTA_RETRY_DELAYS = [300, 600, 1200, 2400]

# Max events per retry message - safety net for very large batches
# With 10MB max_request_size and ~2.8KB/event, 3000 events = ~8.4MB
# Normal 1000-event batches (~2.8MB) won't be chunked
MAX_EVENTS_PER_RETRY_MESSAGE = 3000


class DeltaRetryHandler:
    """
    Handles retry logic for failed Delta batch writes via Kafka topics.

    Routes failed batches to appropriate retry topics based on retry count,
    or to DLQ when retries are exhausted. Preserves entire batch for
    atomic retry operations.

    The retry delays are configurable:
    - Default: [300s, 600s, 1200s, 2400s] (5m, 10m, 20m, 40m)
    - Retry topics: delta-events.retry.{delay}m

    Usage:
        >>> config = KafkaConfig.from_env()
        >>> producer = BaseKafkaProducer(config)
        >>> await producer.start()
        >>> retry_handler = DeltaRetryHandler(
        ...     config=config,
        ...     producer=producer,
        ...     table_path="abfss://workspace@onelake/lakehouse/Tables/xact_events"
        ... )
        >>>
        >>> # Handle a failed batch
        >>> await retry_handler.handle_batch_failure(
        ...     batch=[{"traceId": "evt-001", ...}, ...],
        ...     error=ConnectionError("Timeout"),
        ...     retry_count=0
        ... )
    """

    def __init__(
        self,
        config: KafkaConfig,
        producer: BaseKafkaProducer,
        table_path: str,
        retry_delays: Optional[List[int]] = None,
        retry_topic_prefix: str = "delta-events.retry",
        dlq_topic: str = "delta-events.dlq",
        domain: str = "xact",
    ):
        """
        Initialize Delta retry handler.

        Args:
            config: Kafka configuration
            producer: Kafka producer for sending retry/DLQ messages
            table_path: Delta table path for context in retry messages
            retry_delays: List of delay seconds for each retry level
            retry_topic_prefix: Prefix for retry topic names
            dlq_topic: Topic name for dead-letter queue
            domain: Domain identifier for metrics (default: "xact")
        """
        self.config = config
        self.producer = producer
        self.table_path = table_path
        self.domain = domain
        self.retry_delays = retry_delays or DEFAULT_DELTA_RETRY_DELAYS
        self.max_retries = len(self.retry_delays)
        self.retry_topic_prefix = retry_topic_prefix
        self.dlq_topic = dlq_topic

        # Max events per retry message (to avoid MessageSizeTooLargeError)
        self.max_events_per_message = MAX_EVENTS_PER_RETRY_MESSAGE

        logger.info(
            "Initialized DeltaRetryHandler",
            extra={
                "retry_delays": self.retry_delays,
                "max_retries": self.max_retries,
                "dlq_topic": self.dlq_topic,
                "table_path": self.table_path,
                "max_events_per_message": self.max_events_per_message,
            },
        )

    @staticmethod
    def _extract_trace_ids(
        batch: List[Dict[str, Any]], max_ids: int = 10
    ) -> List[str]:
        """
        Extract trace_ids from batch for logging.

        Args:
            batch: List of event dictionaries
            max_ids: Maximum number of trace_ids to return

        Returns:
            List of trace_ids (up to max_ids)
        """
        trace_ids = []
        for event in batch[:max_ids]:
            trace_id = event.get("traceId") or event.get("trace_id")
            if trace_id:
                trace_ids.append(trace_id)
        return trace_ids

    def get_retry_topic(self, retry_count: int) -> str:
        """
        Get the retry topic name for a given retry count.

        Args:
            retry_count: Current retry count (0-indexed)

        Returns:
            Topic name like "delta-events.retry.5m"
        """
        if retry_count >= len(self.retry_delays):
            retry_count = len(self.retry_delays) - 1

        delay_minutes = self.retry_delays[retry_count] // 60
        return f"{self.retry_topic_prefix}.{delay_minutes}m"

    def get_all_retry_topics(self) -> List[str]:
        """
        Get list of all retry topics for consumer subscription.

        Returns:
            List of topic names
        """
        topics = []
        for delay in self.retry_delays:
            delay_minutes = delay // 60
            topics.append(f"{self.retry_topic_prefix}.{delay_minutes}m")
        return list(set(topics))  # Dedupe in case of duplicate delays

    async def handle_batch_failure(
        self,
        batch: List[Dict[str, Any]],
        error: Exception,
        retry_count: int = 0,
        error_category: str = "transient",
        batch_id: Optional[str] = None,
        first_failure_at: Optional[datetime] = None,
    ) -> None:
        """
        Route failed batch to appropriate retry topic or DLQ.

        Determines routing based on retry count:
        - If retries remaining: send to next retry topic
        - If retries exhausted: send to DLQ

        Args:
            batch: List of event dictionaries that failed to write
            error: Exception that caused the failure
            retry_count: Number of retries already attempted
            error_category: Classification of the error
            batch_id: Optional batch ID (generated if not provided)
            first_failure_at: Timestamp of first failure (now if not provided)

        Raises:
            RuntimeError: If producer is not started
            Exception: If send to retry topic or DLQ fails
        """
        batch_size = len(batch)
        error_message = str(error)[:500]
        trace_ids = self._extract_trace_ids(batch)

        logger.info(
            "Handling Delta batch failure",
            extra={
                "batch_size": batch_size,
                "retry_count": retry_count,
                "error_category": error_category,
                "error_type": type(error).__name__,
                "trace_ids": trace_ids,
            },
        )

        # Check if retries exhausted
        if retry_count >= self.max_retries:
            logger.warning(
                "Retries exhausted, sending batch to DLQ",
                extra={
                    "batch_size": batch_size,
                    "retry_count": retry_count,
                    "max_retries": self.max_retries,
                },
            )
            record_retry_exhausted(domain=self.domain, error_category=error_category)
            record_dlq_message(domain=self.domain, reason="exhausted")
            await self._send_to_dlq(
                batch=batch,
                error_message=error_message,
                error_category=error_category,
                retry_count=retry_count,
                batch_id=batch_id,
                first_failure_at=first_failure_at or datetime.now(timezone.utc),
            )
            return

        # Send to next retry topic
        await self._send_to_retry_topic(
            batch=batch,
            error_message=error_message,
            error_category=error_category,
            retry_count=retry_count,
            batch_id=batch_id,
            first_failure_at=first_failure_at or datetime.now(timezone.utc),
        )

    async def _send_to_retry_topic(
        self,
        batch: List[Dict[str, Any]],
        error_message: str,
        error_category: str,
        retry_count: int,
        batch_id: Optional[str],
        first_failure_at: datetime,
    ) -> None:
        """
        Send batch to appropriate retry topic.

        Creates FailedDeltaBatch with scheduled retry_at timestamp
        based on configured delay. Chunks large batches to avoid
        MessageSizeTooLargeError.

        Args:
            batch: Events to retry
            error_message: Error description
            error_category: Error classification
            retry_count: Current retry count
            batch_id: Batch identifier
            first_failure_at: Original failure timestamp
        """
        retry_topic = self.get_retry_topic(retry_count)
        delay_seconds = self.retry_delays[retry_count]
        retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)

        # Record retry attempt metric
        record_retry_attempt(
            domain=self.domain,
            error_category=error_category,
            delay_seconds=delay_seconds,
        )

        # Chunk batch if too large to avoid MessageSizeTooLargeError
        if len(batch) > self.max_events_per_message:
            chunks = [
                batch[i : i + self.max_events_per_message]
                for i in range(0, len(batch), self.max_events_per_message)
            ]
            logger.info(
                "Chunking large batch for retry topic",
                extra={
                    "original_size": len(batch),
                    "chunk_count": len(chunks),
                    "max_events_per_message": self.max_events_per_message,
                },
            )
        else:
            chunks = [batch]

        for chunk_idx, chunk in enumerate(chunks):
            # Create failed batch message for this chunk
            failed_batch = FailedDeltaBatch(
                events=chunk,
                retry_count=retry_count + 1,  # Increment for next attempt
                first_failure_at=first_failure_at,
                last_error=error_message,
                error_category=error_category,
                retry_at=retry_at,
                table_path=self.table_path,
            )

            # Use provided batch_id with chunk suffix if chunking
            if batch_id:
                if len(chunks) > 1:
                    failed_batch.batch_id = f"{batch_id}-chunk{chunk_idx}"
                else:
                    failed_batch.batch_id = batch_id

            logger.info(
                "Sending batch to retry topic",
                extra={
                    "batch_id": failed_batch.batch_id,
                    "batch_size": failed_batch.event_count,
                    "retry_topic": retry_topic,
                    "retry_count": failed_batch.retry_count,
                    "delay_seconds": delay_seconds,
                    "retry_at": retry_at.isoformat(),
                    "chunk": f"{chunk_idx + 1}/{len(chunks)}" if len(chunks) > 1 else None,
                },
            )

            await self.producer.send(
                topic=retry_topic,
                key=failed_batch.batch_id,
                value=failed_batch,
                headers={
                    "retry_count": str(failed_batch.retry_count),
                    "error_category": error_category,
                    "event_count": str(failed_batch.event_count),
                },
            )

            logger.debug(
                "Batch sent to retry topic successfully",
                extra={
                    "batch_id": failed_batch.batch_id,
                    "retry_topic": retry_topic,
                },
            )

    async def _send_to_dlq(
        self,
        batch: List[Dict[str, Any]],
        error_message: str,
        error_category: str,
        retry_count: int,
        batch_id: Optional[str],
        first_failure_at: datetime,
    ) -> None:
        """
        Send batch to dead-letter queue.

        Creates FailedDeltaBatch with no retry_at (terminal state).
        Chunks large batches to avoid MessageSizeTooLargeError.

        Args:
            batch: Events that exhausted retries
            error_message: Final error description
            error_category: Error classification
            retry_count: Total retries attempted
            batch_id: Batch identifier
            first_failure_at: Original failure timestamp
        """
        # Chunk batch if too large to avoid MessageSizeTooLargeError
        if len(batch) > self.max_events_per_message:
            chunks = [
                batch[i : i + self.max_events_per_message]
                for i in range(0, len(batch), self.max_events_per_message)
            ]
            logger.info(
                "Chunking large batch for DLQ",
                extra={
                    "original_size": len(batch),
                    "chunk_count": len(chunks),
                    "max_events_per_message": self.max_events_per_message,
                },
            )
        else:
            chunks = [batch]

        for chunk_idx, chunk in enumerate(chunks):
            # Create failed batch message (no retry_at for DLQ)
            failed_batch = FailedDeltaBatch(
                events=chunk,
                retry_count=retry_count,
                first_failure_at=first_failure_at,
                last_error=error_message,
                error_category=error_category,
                retry_at=None,  # No more retries
                table_path=self.table_path,
            )

            # Use provided batch_id with chunk suffix if chunking
            if batch_id:
                if len(chunks) > 1:
                    failed_batch.batch_id = f"{batch_id}-chunk{chunk_idx}"
                else:
                    failed_batch.batch_id = batch_id

            trace_ids = self._extract_trace_ids(chunk)

            logger.error(
                "Sending batch to DLQ",
                extra={
                    "batch_id": failed_batch.batch_id,
                    "batch_size": failed_batch.event_count,
                    "retry_count": retry_count,
                    "error_category": error_category,
                    "final_error": error_message[:200],
                    "trace_ids": trace_ids,
                    "chunk": f"{chunk_idx + 1}/{len(chunks)}" if len(chunks) > 1 else None,
                },
            )

            await self.producer.send(
                topic=self.dlq_topic,
                key=failed_batch.batch_id,
                value=failed_batch,
                headers={
                    "retry_count": str(retry_count),
                    "error_category": error_category,
                    "event_count": str(failed_batch.event_count),
                    "failed": "true",
                },
            )

            logger.info(
                "Batch sent to DLQ successfully",
                extra={
                    "batch_id": failed_batch.batch_id,
                    "dlq_topic": self.dlq_topic,
                },
            )


__all__ = ["DeltaRetryHandler"]
