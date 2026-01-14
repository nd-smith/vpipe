"""
Delta batch retry handler for batch Delta Lake writes.

Handles retry routing logic for failed Delta batch writes with exponential
backoff via dedicated retry topics. Routes exhausted retries to dead-letter
queue (DLQ).

This handler is specialized for Delta Lake batch operations where:
- Input is a batch of events (List[Dict[str, Any]])
- Errors are classified based on Delta-specific failure modes
- DLQ routing includes batch metadata for replay

Error Classification:
    TRANSIENT (retry):
        - Timeout errors (network, operation timeout)
        - Connection errors (network failures, DNS issues)
        - Throttling errors (429, rate limiting)
        - Service errors (503, 502, 504)
        - Deadlock/lock conflicts in Delta

    PERMANENT (DLQ immediately):
        - Schema mismatch errors
        - Type conversion errors
        - Constraint violations
        - Permission denied (403)
        - Authentication errors requiring manual intervention

    UNKNOWN (retry cautiously):
        - Unrecognized errors
        - Generic exceptions without clear classification

Usage:
    >>> config = KafkaConfig.from_env()
    >>> producer = BaseKafkaProducer(config)
    >>> await producer.start()
    >>>
    >>> handler = DeltaRetryHandler(
    ...     config=config,
    ...     producer=producer,
    ...     table_path="abfss://workspace@onelake/lakehouse/Tables/xact_events",
    ...     domain="xact"
    ... )
    >>>
    >>> # Handle a failed batch
    >>> await handler.handle_batch_failure(
    ...     batch=[{"traceId": "123", ...}],
    ...     error=TimeoutError("Delta write timeout"),
    ...     retry_count=0,
    ...     error_category="transient",
    ...     batch_id="abc123"
    ... )
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from config.config import KafkaConfig
from core.types import ErrorCategory
from kafka_pipeline.common.metrics import (
    record_retry_attempt,
    record_retry_exhausted,
    record_dlq_message,
)
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.xact.schemas.delta_batch import FailedDeltaBatch

logger = logging.getLogger(__name__)


class DeltaRetryHandler:
    """
    Handles retry logic for failed Delta batch writes via Kafka topics.

    Routes failed Delta batches to appropriate retry topics based on
    retry count and error classification, or to DLQ when retries are exhausted.
    Preserves batch context and error information through retry chain for
    observability.

    The retry delays are configurable via constructor parameters:
    - Default: [300s, 600s, 1200s, 2400s] (5m, 10m, 20m, 40m)
    - Retry topics: {retry_topic_prefix}.{delay}s

    Attributes:
        config: Kafka configuration
        producer: Kafka producer for retry/DLQ messages
        table_path: Delta table path for writes
        domain: Domain identifier (e.g., "xact")
        retry_delays: List of retry delays in seconds
        retry_topic_prefix: Prefix for retry topics
        dlq_topic: Dead-letter queue topic name

    Example:
        >>> handler = DeltaRetryHandler(
        ...     config=config,
        ...     producer=producer,
        ...     table_path="abfss://.../xact_events",
        ...     retry_delays=[300, 600, 1200, 2400],
        ...     retry_topic_prefix="delta-events.retry",
        ...     dlq_topic="delta-events.dlq",
        ...     domain="xact"
        ... )
    """

    def __init__(
        self,
        config: KafkaConfig,
        producer: BaseKafkaProducer,
        table_path: str,
        retry_delays: Optional[List[int]] = None,
        retry_topic_prefix: Optional[str] = None,
        dlq_topic: Optional[str] = None,
        domain: str = "xact",
    ):
        """
        Initialize Delta retry handler.

        Args:
            config: Kafka configuration
            producer: Kafka producer for sending retry/DLQ messages
            table_path: Delta table path for writes
            retry_delays: List of retry delays in seconds (default: [300, 600, 1200, 2400])
            retry_topic_prefix: Prefix for retry topics (default: "delta-events.retry")
            dlq_topic: Dead-letter queue topic name (default: "delta-events.dlq")
            domain: Domain identifier (default: "xact")
        """
        self.config = config
        self.producer = producer
        self.table_path = table_path
        self.domain = domain

        # Retry configuration with defaults
        self._retry_delays = retry_delays or [300, 600, 1200, 2400]  # 5m, 10m, 20m, 40m
        self._max_retries = len(self._retry_delays)
        self._retry_topic_prefix = retry_topic_prefix or "delta-events.retry"
        self._dlq_topic = dlq_topic or "delta-events.dlq"

        logger.info(
            "Initialized DeltaRetryHandler",
            extra={
                "domain": domain,
                "table_path": table_path,
                "retry_delays": self._retry_delays,
                "max_retries": self._max_retries,
                "retry_topic_prefix": self._retry_topic_prefix,
                "dlq_topic": self._dlq_topic,
            },
        )

    def classify_delta_error(self, error: Exception) -> ErrorCategory:
        """
        Classify a Delta error into ErrorCategory for routing decisions.

        Analyzes the error message and type to determine whether the error
        is transient (retry), permanent (DLQ), or unknown (retry cautiously).

        Error Classification Rules:

        TRANSIENT (retry with backoff):
            - Timeout errors (operation timeout, network timeout)
            - Connection errors (network failure, DNS issues)
            - Throttling errors (429, rate limiting, ServerBusy)
            - Service errors (503, 502, 504, service unavailable)
            - Deadlock/lock conflicts in Delta Lake
            - Delta commit conflicts (concurrent writes)

        PERMANENT (route to DLQ immediately):
            - Schema mismatch errors (column type incompatibility)
            - Type conversion errors (cannot cast data)
            - Constraint violations (primary key, not null, check)
            - Permission denied (403, insufficient permissions)
            - Table not found (incorrect configuration)
            - Invalid path/URI (malformed table path)

        UNKNOWN (retry cautiously):
            - Unrecognized error patterns
            - Generic exceptions without clear indicators

        Args:
            error: Exception from Delta write operation

        Returns:
            ErrorCategory indicating how to handle this error
        """
        error_str = str(error).lower()
        error_type = type(error).__name__

        # PERMANENT errors - do not retry
        permanent_patterns = [
            # Schema and type errors
            "schema mismatch",
            "schema incompatible",
            "type mismatch",
            "type error",
            "cannot cast",
            "invalid type",
            "datatype",
            "column not found",
            "column does not exist",
            "unknown column",

            # Constraint violations
            "constraint",
            "primary key",
            "unique constraint",
            "not null",
            "check constraint",
            "foreign key",

            # Permission and auth errors (permanent until manual intervention)
            "permission denied",
            "access denied",
            "forbidden",
            "403",
            "insufficient permissions",
            "authorization failed",

            # Configuration errors
            "table not found",
            "invalid path",
            "invalid uri",
            "malformed",
            "does not exist",
            "no such file",

            # Validation errors
            "validation error",
            "invalid argument",
            "invalid parameter",
        ]

        if any(pattern in error_str for pattern in permanent_patterns):
            logger.debug(
                "Classified error as PERMANENT",
                extra={
                    "error_type": error_type,
                    "error_message": str(error)[:200],
                },
            )
            return ErrorCategory.PERMANENT

        # TRANSIENT errors - retry with backoff
        transient_patterns = [
            # Timeout errors
            "timeout",
            "timed out",
            "time out",
            "deadline exceeded",

            # Connection errors
            "connection",
            "network",
            "dns",
            "unreachable",
            "connection refused",
            "connection reset",
            "broken pipe",

            # Throttling errors
            "429",
            "throttl",
            "rate limit",
            "too many requests",
            "server busy",
            "serverbusy",

            # Service errors (transient)
            "503",
            "502",
            "504",
            "service unavailable",
            "bad gateway",
            "gateway timeout",
            "internal error",
            "temporary error",

            # Delta-specific transient errors
            "deadlock",
            "lock",
            "conflict",
            "concurrent",
            "version conflict",
            "commit conflict",
        ]

        if any(pattern in error_str for pattern in transient_patterns):
            logger.debug(
                "Classified error as TRANSIENT",
                extra={
                    "error_type": error_type,
                    "error_message": str(error)[:200],
                },
            )
            return ErrorCategory.TRANSIENT

        # Default to UNKNOWN - retry cautiously
        logger.warning(
            "Classified error as UNKNOWN (will retry cautiously)",
            extra={
                "error_type": error_type,
                "error_message": str(error)[:200],
            },
        )
        return ErrorCategory.UNKNOWN

    async def handle_batch_failure(
        self,
        batch: List[Dict[str, Any]],
        error: Exception,
        retry_count: int,
        error_category: str,
        batch_id: Optional[str] = None,
        first_failure_at: Optional[datetime] = None,
    ) -> None:
        """
        Route failed Delta batch to appropriate retry topic or DLQ.

        Determines routing based on retry count and error category:
        - PERMANENT errors: send directly to DLQ (no retry)
        - Retries exhausted: send to DLQ
        - Otherwise: send to next retry topic with exponential backoff

        Args:
            batch: List of event dictionaries that failed to write
            error: Exception that caused the failure
            retry_count: Current retry count (0 for first failure)
            error_category: Error classification string (transient, permanent, unknown)
            batch_id: Optional batch identifier for correlation
            first_failure_at: Timestamp of first failure (for tracking total retry duration)

        Raises:
            RuntimeError: If producer is not started
            Exception: If send to retry topic or DLQ fails
        """
        if not batch:
            logger.warning("handle_batch_failure called with empty batch, ignoring")
            return

        # Normalize error_category to ErrorCategory enum
        if isinstance(error_category, str):
            try:
                error_cat = ErrorCategory(error_category.lower())
            except ValueError:
                # If string doesn't match enum, classify the error
                error_cat = self.classify_delta_error(error)
        else:
            error_cat = error_category

        logger.info(
            "Handling Delta batch failure",
            extra={
                "batch_id": batch_id,
                "event_count": len(batch),
                "retry_count": retry_count,
                "error_category": error_cat.value,
                "error_type": type(error).__name__,
            },
        )

        # PERMANENT errors skip retry and go straight to DLQ
        if error_cat == ErrorCategory.PERMANENT:
            logger.warning(
                "Permanent error detected, sending batch to DLQ without retry",
                extra={
                    "batch_id": batch_id,
                    "event_count": len(batch),
                    "error": str(error)[:200],
                },
            )
            record_dlq_message(domain=self.domain, reason="permanent")
            await self._send_to_dlq(
                batch=batch,
                error=error,
                error_category=error_cat,
                retry_count=retry_count,
                batch_id=batch_id,
                first_failure_at=first_failure_at,
            )
            return

        # Check if retries exhausted
        if retry_count >= self._max_retries:
            logger.warning(
                "Retries exhausted, sending batch to DLQ",
                extra={
                    "batch_id": batch_id,
                    "event_count": len(batch),
                    "retry_count": retry_count,
                    "max_retries": self._max_retries,
                },
            )
            record_retry_exhausted(domain=self.domain, error_category=error_cat.value)
            record_dlq_message(domain=self.domain, reason="exhausted")
            await self._send_to_dlq(
                batch=batch,
                error=error,
                error_category=error_cat,
                retry_count=retry_count,
                batch_id=batch_id,
                first_failure_at=first_failure_at,
            )
            return

        # Send to next retry topic
        await self._send_to_retry_topic(
            batch=batch,
            error=error,
            error_category=error_cat,
            retry_count=retry_count,
            batch_id=batch_id,
            first_failure_at=first_failure_at,
        )

    async def _send_to_retry_topic(
        self,
        batch: List[Dict[str, Any]],
        error: Exception,
        error_category: ErrorCategory,
        retry_count: int,
        batch_id: Optional[str] = None,
        first_failure_at: Optional[datetime] = None,
    ) -> None:
        """
        Send batch to appropriate retry topic with updated metadata.

        Creates FailedDeltaBatch message with incremented retry count, error
        context, and retry_at timestamp based on exponential backoff delay.

        Args:
            batch: List of event dictionaries to retry
            error: Exception that caused failure
            error_category: Classification of the error
            retry_count: Current retry count
            batch_id: Batch identifier for correlation
            first_failure_at: Timestamp of first failure

        Raises:
            RuntimeError: If producer is not started
            Exception: If send to retry topic fails
        """
        delay_seconds = self._retry_delays[retry_count]
        retry_topic = f"{self._retry_topic_prefix}.{delay_seconds}s"

        # Calculate retry timestamp
        retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)

        # Create FailedDeltaBatch message
        failed_batch = FailedDeltaBatch(
            batch_id=batch_id,
            events=batch,
            retry_count=retry_count + 1,
            first_failure_at=first_failure_at or datetime.now(timezone.utc),
            last_error=str(error)[:500],
            error_category=error_category.value,
            retry_at=retry_at,
            table_path=self.table_path,
            event_count=len(batch),
        )

        # Record retry attempt metric
        record_retry_attempt(
            domain=self.domain,
            error_category=error_category.value,
            delay_seconds=delay_seconds,
        )

        # Extract sample trace IDs for logging
        sample_trace_ids = []
        for event in batch[:5]:
            trace_id = event.get("traceId") or event.get("trace_id")
            if trace_id:
                sample_trace_ids.append(trace_id)

        logger.info(
            "Sending batch to retry topic",
            extra={
                "batch_id": batch_id,
                "event_count": len(batch),
                "retry_topic": retry_topic,
                "retry_count": retry_count + 1,
                "delay_seconds": delay_seconds,
                "retry_at": retry_at.isoformat(),
                "sample_trace_ids": sample_trace_ids,
            },
        )

        await self.producer.send(
            topic=retry_topic,
            key=batch_id or "batch",
            value=failed_batch,
            headers={
                "retry_count": str(retry_count + 1),
                "error_category": error_category.value,
            },
        )

        logger.debug(
            "Batch sent to retry topic successfully",
            extra={
                "batch_id": batch_id,
                "retry_topic": retry_topic,
            },
        )

    async def _send_to_dlq(
        self,
        batch: List[Dict[str, Any]],
        error: Exception,
        error_category: ErrorCategory,
        retry_count: int,
        batch_id: Optional[str] = None,
        first_failure_at: Optional[datetime] = None,
    ) -> None:
        """
        Send batch to dead-letter queue (DLQ).

        Creates FailedDeltaBatch message with complete context for manual
        review and potential replay. Preserves all events for replay.

        Args:
            batch: List of event dictionaries that failed permanently
            error: Final exception that caused failure
            error_category: Classification of the error
            retry_count: Final retry count
            batch_id: Batch identifier for correlation
            first_failure_at: Timestamp of first failure

        Raises:
            RuntimeError: If producer is not started
            Exception: If send to DLQ fails
        """
        # Truncate error message to prevent huge DLQ messages
        error_message = str(error)
        if len(error_message) > 500:
            error_message = error_message[:497] + "..."

        # Create DLQ message (no retry_at since this is final)
        dlq_message = FailedDeltaBatch(
            batch_id=batch_id,
            events=batch,
            retry_count=retry_count,
            first_failure_at=first_failure_at or datetime.now(timezone.utc),
            last_error=error_message,
            error_category=error_category.value,
            retry_at=None,  # No retry scheduled
            table_path=self.table_path,
            event_count=len(batch),
        )

        # Extract sample trace IDs for logging
        sample_trace_ids = []
        for event in batch[:5]:
            trace_id = event.get("traceId") or event.get("trace_id")
            if trace_id:
                sample_trace_ids.append(trace_id)

        logger.error(
            "Sending batch to DLQ",
            extra={
                "batch_id": batch_id,
                "event_count": len(batch),
                "retry_count": retry_count,
                "error_category": error_category.value,
                "final_error": error_message[:200],
                "sample_trace_ids": sample_trace_ids,
            },
        )

        await self.producer.send(
            topic=self._dlq_topic,
            key=batch_id or "batch",
            value=dlq_message,
            headers={
                "retry_count": str(retry_count),
                "error_category": error_category.value,
                "failed": "true",
            },
        )

        logger.info(
            "Batch sent to DLQ successfully",
            extra={
                "batch_id": batch_id,
                "dlq_topic": self._dlq_topic,
            },
        )

    def get_all_retry_topics(self) -> List[str]:
        """
        Get list of all retry topic names for this handler.

        Useful for configuring consumers that need to subscribe to all retry topics.

        Returns:
            List of retry topic names (e.g., ["delta-events.retry.300s", ...])
        """
        return [f"{self._retry_topic_prefix}.{delay}s" for delay in self._retry_delays]


__all__ = ["DeltaRetryHandler"]
