"""
Delta batch message schemas for retry and DLQ handling.

Contains Pydantic models for failed Delta batch writes that need
retry via Kafka topics or dead-letter queue processing.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, field_serializer, field_validator


class FailedDeltaBatch(BaseModel):
    """Schema for failed Delta batch writes sent to retry topics.

    When a batch write to Delta Lake fails, the entire batch is serialized
    to a retry topic for later reprocessing. This preserves all events
    together and allows for exponential backoff retry.

    Attributes:
        batch_id: Unique identifier for this batch (UUID)
        events: List of event dictionaries that failed to write
        retry_count: Number of retry attempts made so far
        first_failure_at: Timestamp of the initial failure
        last_error: Most recent error message (truncated to 500 chars)
        error_category: Classification of the error (transient, permanent, etc.)
        retry_at: Scheduled time for next retry attempt (None if going to DLQ)
        table_path: Delta table path this batch was destined for
        event_count: Number of events in the batch (for quick reference)

    Example:
        >>> from datetime import datetime, timezone, timedelta
        >>> batch = FailedDeltaBatch(
        ...     events=[{"traceId": "abc123", "data": {...}}, ...],
        ...     first_failure_at=datetime.now(timezone.utc),
        ...     last_error="Connection timeout to Delta Lake",
        ...     error_category="transient",
        ...     retry_at=datetime.now(timezone.utc) + timedelta(minutes=5),
        ...     table_path="abfss://workspace@onelake/lakehouse/Tables/xact_events"
        ... )
    """

    batch_id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique identifier for this batch (UUID)",
    )
    events: List[Dict[str, Any]] = Field(
        ...,
        description="List of event dictionaries that failed to write",
        min_length=1,
    )
    retry_count: int = Field(
        default=0,
        description="Number of retry attempts made so far",
        ge=0,
    )
    first_failure_at: datetime = Field(
        ...,
        description="Timestamp of the initial failure",
    )
    last_error: str = Field(
        ...,
        description="Most recent error message (truncated to 500 chars)",
    )
    error_category: str = Field(
        default="transient",
        description="Classification of the error (transient, permanent, auth, unknown)",
    )
    retry_at: Optional[datetime] = Field(
        default=None,
        description="Scheduled time for next retry attempt (None if going to DLQ)",
    )
    table_path: str = Field(
        ...,
        description="Delta table path this batch was destined for",
    )
    event_count: int = Field(
        default=0,
        description="Number of events in the batch (for quick reference)",
    )

    def __init__(self, **data):
        """Initialize with computed event_count if not provided."""
        if "event_count" not in data and "events" in data:
            data["event_count"] = len(data["events"])
        super().__init__(**data)

    @field_validator("last_error")
    @classmethod
    def truncate_error(cls, v: str) -> str:
        """Truncate error message to prevent huge messages."""
        if not v:
            return "Unknown error"
        if len(v) > 500:
            return v[:497] + "..."
        return v

    @field_validator("error_category")
    @classmethod
    def validate_error_category(cls, v: str) -> str:
        """Validate error category is one of the known types."""
        valid_categories = {"transient", "permanent", "auth", "circuit_open", "unknown"}
        if v.lower() not in valid_categories:
            return "unknown"
        return v.lower()

    @field_serializer("first_failure_at", "retry_at")
    def serialize_timestamp(self, timestamp: Optional[datetime]) -> Optional[str]:
        """Serialize datetime to ISO 8601 format."""
        if timestamp is None:
            return None
        return timestamp.isoformat()

    def increment_retry(self, delay_seconds: int, error: str) -> "FailedDeltaBatch":
        """
        Create a new FailedDeltaBatch with incremented retry count.

        Args:
            delay_seconds: Seconds until next retry
            error: The error message from the latest failure

        Returns:
            New FailedDeltaBatch with updated retry_count, retry_at, and last_error
        """
        return FailedDeltaBatch(
            batch_id=self.batch_id,
            events=self.events,
            retry_count=self.retry_count + 1,
            first_failure_at=self.first_failure_at,
            last_error=error,
            error_category=self.error_category,
            retry_at=datetime.now(timezone.utc)
            + __import__("datetime").timedelta(seconds=delay_seconds),
            table_path=self.table_path,
            event_count=self.event_count,
        )

    def to_dlq_format(self) -> Dict[str, Any]:
        """
        Convert to DLQ-friendly format for manual review.

        Returns:
            Dictionary with batch info and sample events for investigation
        """
        # Include first 3 events as samples (full batch may be huge)
        sample_events = self.events[:3] if len(self.events) > 3 else self.events
        sample_trace_ids = [e.get("traceId", "unknown") for e in sample_events]

        return {
            "batch_id": self.batch_id,
            "event_count": self.event_count,
            "retry_count": self.retry_count,
            "first_failure_at": self.first_failure_at.isoformat(),
            "last_error": self.last_error,
            "error_category": self.error_category,
            "table_path": self.table_path,
            "sample_trace_ids": sample_trace_ids,
            # Full events preserved for replay
            "events": self.events,
        }

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "batch_id": "550e8400-e29b-41d4-a716-446655440000",
                    "events": [
                        {"traceId": "evt-001", "eventType": "claim.created"},
                        {"traceId": "evt-002", "eventType": "claim.updated"},
                    ],
                    "retry_count": 1,
                    "first_failure_at": "2024-12-25T10:00:00Z",
                    "last_error": "Connection timeout after 30s",
                    "error_category": "transient",
                    "retry_at": "2024-12-25T10:05:00Z",
                    "table_path": "abfss://workspace@onelake/lakehouse/Tables/xact_events",
                    "event_count": 2,
                }
            ]
        }
    }


__all__ = ["FailedDeltaBatch"]
