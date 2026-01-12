"""
Download task message schemas for Kafka pipeline.

Contains Pydantic models for download work items sent to download workers.
Schema aligned with verisk_pipeline Task dataclass for compatibility.
"""

from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_serializer, field_validator


class DownloadTaskMessage(BaseModel):
    """Schema for download work items sent to download workers.

    Represents a single attachment download task derived from an event.
    Schema matches verisk_pipeline.xact.xact_models.Task for compatibility.

    Attributes:
        media_id: Unique deterministic ID for the attachment (UUID5 of trace_id + url)
        trace_id: Unique identifier from the source event (for correlation)
        attachment_url: URL of the attachment to download
        blob_path: Target path in OneLake/blob storage for the downloaded file
        status_subtype: Event status subtype (e.g., "documentsReceived", "estimateCreated")
        file_type: File type extracted from URL (e.g., "pdf", "esx", "jpg")
        assignment_id: Assignment ID from event payload (required for path generation)
        estimate_version: Estimate version from event payload (optional)
        retry_count: Number of times this task has been retried (starts at 0)
        event_type: Type of the originating event (e.g., "xact")
        event_subtype: Subtype of the originating event (e.g., "documentsReceived")
        original_timestamp: Timestamp from the original Kafka event
        metadata: Additional context passed through from original event

    Matches verisk_pipeline Task dataclass (core fields):
        @dataclass
        class Task:
            trace_id: str
            attachment_url: str
            blob_path: str
            status_subtype: str
            file_type: str
            assignment_id: str
            estimate_version: Optional[str] = None
            retry_count: int = 0

    Example:
        >>> from datetime import datetime, timezone
        >>> task = DownloadTaskMessage(
        ...     trace_id="abc123-def456",
        ...     attachment_url="https://example.com/docs/estimate.pdf",
        ...     blob_path="documentsReceived/A12345/pdf/estimate.pdf",
        ...     status_subtype="documentsReceived",
        ...     file_type="pdf",
        ...     assignment_id="A12345",
        ...     estimate_version="1.0",
        ...     retry_count=0,
        ...     event_type="xact",
        ...     event_subtype="documentsReceived",
        ...     original_timestamp=datetime.now(timezone.utc),
        ... )
    """

    trace_id: str = Field(
        ...,
        description="Unique event identifier (from traceId)",
        min_length=1
    )
    media_id: str = Field(
        ...,
        description="Unique deterministic ID for the attachment (UUID5 of trace_id + url)",
        min_length=1
    )
    attachment_url: str = Field(
        ...,
        description="URL of the attachment to download",
        min_length=1
    )
    blob_path: str = Field(
        ...,
        description="Target path in OneLake/blob storage",
        min_length=1
    )
    status_subtype: str = Field(
        ...,
        description="Event status subtype (last part of event type)",
        min_length=1
    )
    file_type: str = Field(
        ...,
        description="File type extracted from URL extension",
        min_length=1
    )
    assignment_id: str = Field(
        ...,
        description="Assignment ID from event payload",
        min_length=1
    )
    estimate_version: Optional[str] = Field(
        default=None,
        description="Estimate version from event payload (optional)"
    )
    retry_count: int = Field(
        default=0,
        description="Number of retry attempts (starts at 0)",
        ge=0
    )
    event_type: str = Field(
        ...,
        description="Type of the originating event (e.g., 'xact')",
        min_length=1
    )
    event_subtype: str = Field(
        ...,
        description="Subtype of the originating event (e.g., 'documentsReceived')",
        min_length=1
    )
    original_timestamp: datetime = Field(
        ...,
        description="Timestamp from the original Kafka event"
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional context passed through from original event"
    )

    @field_validator('trace_id', 'media_id', 'attachment_url', 'blob_path', 'status_subtype', 'file_type', 'assignment_id', 'event_type', 'event_subtype')
    @classmethod
    def validate_non_empty_strings(cls, v: str, info) -> str:
        """Ensure string fields are not empty or whitespace-only."""
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} cannot be empty or whitespace")
        return v.strip()

    @field_validator('retry_count')
    @classmethod
    def validate_retry_count(cls, v: int) -> int:
        """Ensure retry_count is non-negative."""
        if v < 0:
            raise ValueError("retry_count must be non-negative")
        return v

    @field_serializer('original_timestamp')
    def serialize_timestamp(self, timestamp: datetime) -> str:
        """Serialize datetime to ISO 8601 format."""
        return timestamp.isoformat()

    def to_verisk_task(self) -> "Task":
        """
        Convert to verisk_pipeline Task dataclass for compatibility.

        Returns:
            kafka_pipeline.xact.schemas.models.Task instance
        """
        from kafka_pipeline.xact.schemas.models import Task

        return Task(
            media_id=getattr(self, "media_id", None),  # Pass if verisk Task has been updated
            trace_id=self.trace_id,
            attachment_url=self.attachment_url,
            blob_path=self.blob_path,
            status_subtype=self.status_subtype,
            file_type=self.file_type,
            assignment_id=self.assignment_id,
            estimate_version=self.estimate_version,
            retry_count=self.retry_count,
        )

    @classmethod
    def from_verisk_task(cls, task: "Task") -> "DownloadTaskMessage":
        """
        Create from verisk_pipeline Task dataclass.

        Args:
            task: verisk_pipeline.xact.xact_models.Task instance

        Returns:
            DownloadTaskMessage instance
        """
        return cls(
            media_id=getattr(task, "media_id", "unknown"),  # Default if verisk Task not updated
            trace_id=task.trace_id,
            attachment_url=task.attachment_url,
            blob_path=task.blob_path,
            status_subtype=task.status_subtype,
            file_type=task.file_type,
            assignment_id=task.assignment_id,
            estimate_version=task.estimate_version,
            retry_count=task.retry_count,
        )

    model_config = {
        'json_schema_extra': {
            'examples': [
                {
                    'trace_id': 'abc123-def456-ghi789',
                    'attachment_url': 'https://xactware.com/docs/estimate.pdf',
                    'blob_path': 'documentsReceived/A12345/pdf/estimate.pdf',
                    'status_subtype': 'documentsReceived',
                    'file_type': 'pdf',
                    'assignment_id': 'A12345',
                    'estimate_version': '1.0',
                    'retry_count': 0,
                    'event_type': 'xact',
                    'event_subtype': 'documentsReceived',
                    'original_timestamp': '2024-12-25T10:30:00Z',
                    'metadata': {'source_partition': 0}
                },
                {
                    'trace_id': 'xyz789-abc123',
                    'attachment_url': 'https://xactware.com/estimates/v2.esx',
                    'blob_path': 'estimateCreated/B67890/esx/v2.esx',
                    'status_subtype': 'estimateCreated',
                    'file_type': 'esx',
                    'assignment_id': 'B67890',
                    'estimate_version': '2.0',
                    'retry_count': 2,
                    'event_type': 'xact',
                    'event_subtype': 'estimateCreated',
                    'original_timestamp': '2024-12-25T11:00:00Z',
                    'metadata': {'source_partition': 1}
                }
            ]
        }
    }
