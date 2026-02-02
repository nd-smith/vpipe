"""
Utility functions for retry handling across domain-specific handlers.

Provides common retry logic that can be reused across different handler
implementations to reduce code duplication.
"""

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from core.types import ErrorCategory
from pipeline.common.metrics import (
    record_dlq_message,
)

logger = logging.getLogger(__name__)


def should_send_to_dlq(
    error_category: ErrorCategory,
    retry_count: int,
    max_retries: int,
) -> tuple[bool, str]:
    """
    Determine if a failed task should be sent to DLQ.

    Args:
        error_category: Classification of the error
        retry_count: Current retry attempt count
        max_retries: Maximum allowed retries

    Returns:
        Tuple of (should_dlq, reason) where:
        - should_dlq: True if task should go to DLQ
        - reason: Human-readable reason ("permanent", "exhausted", or empty string)
    """
    if error_category == ErrorCategory.PERMANENT:
        return True, "permanent"

    if retry_count >= max_retries:
        return True, "exhausted"

    return False, ""


def calculate_retry_timestamp(delay_seconds: int) -> datetime:
    """
    Calculate when a task should be retried.

    Args:
        delay_seconds: Delay before retry in seconds

    Returns:
        UTC datetime when retry should occur
    """
    return datetime.now(UTC) + timedelta(seconds=delay_seconds)


def create_retry_headers(
    retry_count: int,
    retry_at: datetime,
    delay_seconds: int,
    target_topic: str,
    worker_type: str,
    original_key: str,
    error_category: ErrorCategory,
    domain: str,
) -> dict[str, str]:
    """
    Create standard Kafka headers for retry messages.

    Args:
        retry_count: Current retry attempt count
        retry_at: When retry should occur
        delay_seconds: Delay before retry in seconds
        target_topic: Topic to route message when retry is ready
        worker_type: Type of worker for observability
        original_key: Original Kafka partition key
        error_category: Classification of the error
        domain: Domain identifier (e.g., "verisk", "claimx")

    Returns:
        Dict of header key-value pairs
    """
    return {
        "retry_count": str(retry_count),
        "scheduled_retry_time": retry_at.isoformat(),
        "retry_delay_seconds": str(delay_seconds),
        "target_topic": target_topic,
        "worker_type": worker_type,
        "original_key": original_key,
        "error_category": error_category.value,
        "domain": domain,
    }


def create_dlq_headers(
    retry_count: int,
    error_category: ErrorCategory,
) -> dict[str, str]:
    """
    Create standard Kafka headers for DLQ messages.

    Args:
        retry_count: Final retry attempt count
        error_category: Classification of the error

    Returns:
        Dict of header key-value pairs
    """
    return {
        "retry_count": str(retry_count),
        "error_category": error_category.value,
        "failed": "true",
    }


def truncate_error_message(error: Exception, max_length: int = 500) -> str:
    """
    Truncate error message to prevent huge Kafka messages.

    Args:
        error: Exception to extract message from
        max_length: Maximum length of error message

    Returns:
        Truncated error message with ellipsis if needed
    """
    error_message = str(error)
    if len(error_message) > max_length:
        return error_message[: max_length - 3] + "..."
    return error_message


def add_error_metadata_to_dict(
    metadata: dict[str, Any],
    error: Exception,
    error_category: ErrorCategory,
    retry_at: datetime,
) -> None:
    """
    Add error context to a metadata dictionary.

    Modifies the metadata dict in-place to add error information.

    Args:
        metadata: Metadata dict to update
        error: Exception that caused failure
        error_category: Classification of the error
        retry_at: When retry should occur
    """
    metadata["last_error"] = truncate_error_message(error)
    metadata["error_category"] = error_category.value
    metadata["retry_at"] = retry_at.isoformat()


def log_retry_decision(
    action: str,
    task_id: str,
    retry_count: int,
    error_category: ErrorCategory,
    error: Exception,
    extra_context: dict[str, Any] | None = None,
) -> None:
    """
    Log retry routing decision with consistent format.

    Args:
        action: Action being taken ("retry", "dlq_permanent", "dlq_exhausted")
        task_id: Unique identifier for the task
        retry_count: Current retry attempt count
        error_category: Classification of the error
        error: Exception that caused failure
        extra_context: Additional context to include in log
    """
    log_context = {
        "task_id": task_id,
        "retry_count": retry_count,
        "error_category": error_category.value,
        "error_type": type(error).__name__,
    }

    if extra_context:
        log_context.update(extra_context)

    if action == "dlq_permanent":
        logger.warning(
            "Permanent error detected, sending to DLQ without retry",
            extra={**log_context, "error": str(error)[:200]},
        )
    elif action == "dlq_exhausted":
        logger.warning(
            "Retries exhausted, sending to DLQ",
            extra=log_context,
        )
    elif action == "retry":
        logger.info(
            "Sending task to retry topic",
            extra=log_context,
        )


def record_retry_metrics(
    domain: str,
    error_category: ErrorCategory,
    delay_seconds: int,
    worker_type: str | None = None,
) -> None:
    """
    Record retry attempt metrics.

    Note: Simplified after metrics refactor - granular retry metrics removed.
    Retries are now tracked via DLQ messages and processing errors.

    Args:
        domain: Domain identifier (e.g., "verisk", "claimx")
        error_category: Classification of the error
        delay_seconds: Delay before retry in seconds
        worker_type: Optional worker type for additional granularity
    """
    # Metrics simplified - no per-retry tracking needed
    pass


def record_dlq_metrics(
    domain: str,
    reason: str,
    error_category: ErrorCategory | None = None,
) -> None:
    """
    Record DLQ routing metrics.

    Args:
        domain: Domain identifier (e.g., "verisk", "claimx")
        reason: Reason for DLQ routing ("permanent", "exhausted")
        error_category: Optional error classification for exhausted retries
    """
    record_dlq_message(domain=domain, reason=reason)


__all__ = [
    "should_send_to_dlq",
    "calculate_retry_timestamp",
    "create_retry_headers",
    "create_dlq_headers",
    "truncate_error_message",
    "add_error_metadata_to_dict",
    "log_retry_decision",
    "record_retry_metrics",
    "record_dlq_metrics",
]
