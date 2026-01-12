"""
Download result and failure message schemas for Kafka pipeline.

Contains Pydantic models for download outcomes sent to results topic
and failure messages sent to dead-letter queue (DLQ).

Schema aligned with verisk_pipeline Task.to_tracking_row() for compatibility.
"""

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_serializer, field_validator

from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage


class DownloadResultMessage(BaseModel):
    """Schema for download outcomes (success or failure).

    Sent to results topic after download worker processes a task.
    Schema matches verisk_pipeline Task.to_tracking_row() for inventory table.

    Attributes:
        trace_id: Unique event identifier for correlation
        attachment_url: URL of the attachment that was processed
        blob_path: Target path in OneLake/blob storage
        status_subtype: Event status subtype (e.g., "documentsReceived")
        file_type: File type (e.g., "pdf", "esx")
        assignment_id: Assignment ID from event
        status: Outcome status (completed, failed, failed_permanent)
        http_status: HTTP response status code (optional)
        bytes_downloaded: Number of bytes downloaded (0 if failed)
        retry_count: Number of retry attempts made
        error_message: Error description if failed (truncated to 500 chars)
        created_at: Timestamp when result was created
        expires_at: URL expiration timestamp (optional)
        expired_at_ingest: Whether URL was expired at ingest time (optional)

    Matches verisk_pipeline Task.to_tracking_row() output for xact_attachments table.
    """

    trace_id: str = Field(
        ...,
        description="Unique event identifier for correlation",
        min_length=1
    )
    media_id: str = Field(
        ...,
        description="Unique deterministic ID for the attachment",
        min_length=1
    )
    attachment_url: str = Field(
        ...,
        description="URL of the attachment that was processed",
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
    status: Literal["completed", "failed", "failed_permanent"] = Field(
        ...,
        description="Outcome status: completed, failed (transient), or failed_permanent"
    )
    http_status: Optional[int] = Field(
        default=None,
        description="HTTP response status code"
    )
    bytes_downloaded: int = Field(
        default=0,
        description="Number of bytes downloaded (0 if failed)",
        ge=0
    )
    retry_count: int = Field(
        default=0,
        description="Number of retry attempts made",
        ge=0
    )
    error_message: Optional[str] = Field(
        default=None,
        description="Error description if failed (truncated to 500 chars)"
    )
    created_at: datetime = Field(
        ...,
        description="Timestamp when result was created"
    )
    expires_at: Optional[datetime] = Field(
        default=None,
        description="URL expiration timestamp (optional)"
    )
    expired_at_ingest: Optional[bool] = Field(
        default=None,
        description="Whether URL was expired at ingest time"
    )

    @field_validator('trace_id', 'media_id', 'attachment_url', 'blob_path', 'status_subtype', 'file_type', 'assignment_id')
    @classmethod
    def validate_non_empty_strings(cls, v: str, info) -> str:
        """Ensure string fields are not empty or whitespace-only."""
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} cannot be empty or whitespace")
        return v.strip()

    @field_validator('error_message')
    @classmethod
    def truncate_error_message(cls, v: Optional[str]) -> Optional[str]:
        """Truncate error message to prevent huge messages."""
        if v and len(v) > 500:
            return v[:497] + "..."
        return v

    @field_serializer('created_at', 'expires_at')
    def serialize_timestamp(self, timestamp: Optional[datetime]) -> Optional[str]:
        """Serialize datetime to ISO 8601 format."""
        if timestamp is None:
            return None
        return timestamp.isoformat()

    def to_tracking_row(self) -> dict:
        """
        Convert to tracking table row format.

        Returns:
            Dict suitable for xact_attachments table insert
        """
        return {
            "trace_id": self.trace_id,
            "media_id": self.media_id,
            "attachment_url": self.attachment_url,
            "blob_path": self.blob_path,
            "status_subtype": self.status_subtype,
            "file_type": self.file_type,
            "assignment_id": self.assignment_id,
            "status": self.status,
            "http_status": self.http_status,
            "bytes_downloaded": self.bytes_downloaded,
            "retry_count": self.retry_count,
            "error_message": self.error_message,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "expires_at": self.expires_at,
            "expired_at_ingest": self.expired_at_ingest,
        }

    model_config = {
        'json_schema_extra': {
            'examples': [
                {
                    'trace_id': 'abc123-def456',
                    'attachment_url': 'https://xactware.com/docs/estimate.pdf',
                    'blob_path': 'documentsReceived/A12345/pdf/estimate.pdf',
                    'status_subtype': 'documentsReceived',
                    'file_type': 'pdf',
                    'assignment_id': 'A12345',
                    'status': 'completed',
                    'http_status': 200,
                    'bytes_downloaded': 2048576,
                    'retry_count': 0,
                    'error_message': None,
                    'created_at': '2024-12-25T10:31:15Z',
                    'expires_at': None,
                    'expired_at_ingest': False
                },
                {
                    'trace_id': 'xyz789-abc123',
                    'attachment_url': 'https://xactware.com/estimates/v2.esx',
                    'blob_path': 'estimateCreated/B67890/esx/v2.esx',
                    'status_subtype': 'estimateCreated',
                    'file_type': 'esx',
                    'assignment_id': 'B67890',
                    'status': 'failed',
                    'http_status': 503,
                    'bytes_downloaded': 0,
                    'retry_count': 2,
                    'error_message': 'Service temporarily unavailable',
                    'created_at': '2024-12-25T10:31:45Z',
                    'expires_at': None,
                    'expired_at_ingest': False
                }
            ]
        }
    }


class FailedDownloadMessage(BaseModel):
    """Schema for messages sent to dead-letter queue (DLQ).

    Sent when a download task has exhausted all retry attempts.
    Preserves complete context for manual review and potential replay.

    Attributes:
        trace_id: Unique event identifier for correlation
        attachment_url: URL of the attachment that failed
        original_task: Complete original task message for replay capability
        final_error: Final error message (truncated to 500 chars)
        error_category: Error classification from final attempt
        retry_count: Total number of retry attempts made
        failed_at: Timestamp when task was sent to DLQ

    Example:
        >>> from datetime import datetime, timezone
        >>> task = DownloadTaskMessage(
        ...     trace_id="evt-456",
        ...     attachment_url="https://storage.example.com/bad.pdf",
        ...     blob_path="claims/C-789/bad.pdf",
        ...     status_subtype="documentsReceived",
        ...     file_type="pdf",
        ...     assignment_id="C-789",
        ...     event_type="claim",
        ...     event_subtype="created",
        ...     retry_count=4,
        ...     original_timestamp=datetime.now(timezone.utc)
        ... )
        >>> dlq = FailedDownloadMessage(
        ...     trace_id="evt-456",
        ...     attachment_url="https://storage.example.com/bad.pdf",
        ...     original_task=task,
        ...     final_error="File not found after 4 retries",
        ...     error_category="permanent",
        ...     retry_count=4,
        ...     failed_at=datetime.now(timezone.utc)
        ... )
    """

    trace_id: str = Field(
        ...,
        description="Unique event identifier for correlation",
        min_length=1
    )
    media_id: str = Field(
        ...,
        description="Unique deterministic ID for the attachment",
        min_length=1
    )
    attachment_url: str = Field(
        ...,
        description="URL of the attachment that failed",
        min_length=1
    )
    original_task: DownloadTaskMessage = Field(
        ...,
        description="Complete original task message for replay capability"
    )
    final_error: str = Field(
        ...,
        description="Final error message (truncated to 500 chars)"
    )
    error_category: str = Field(
        ...,
        description="Error classification from final attempt",
        min_length=1
    )
    retry_count: int = Field(
        ...,
        description="Total number of retry attempts made",
        ge=0
    )
    failed_at: datetime = Field(
        ...,
        description="Timestamp when task was sent to DLQ"
    )

    @field_validator('trace_id', 'media_id', 'attachment_url', 'error_category')
    @classmethod
    def validate_non_empty_strings(cls, v: str, info) -> str:
        """Ensure string fields are not empty or whitespace-only."""
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} cannot be empty or whitespace")
        return v.strip()

    @field_validator('final_error')
    @classmethod
    def truncate_final_error(cls, v: str) -> str:
        """Truncate error message to prevent huge messages."""
        if not v or not v.strip():
            raise ValueError("final_error cannot be empty or whitespace")
        v = v.strip()
        if len(v) > 500:
            return v[:497] + "..."
        return v

    @field_serializer('failed_at')
    def serialize_timestamp(self, timestamp: datetime) -> str:
        """Serialize datetime to ISO 8601 format."""
        return timestamp.isoformat()

    model_config = {
        'json_schema_extra': {
            'examples': [
                {
                    'trace_id': 'evt-2024-004',
                    'attachment_url': 'https://storage.example.com/claims/C-99999/missing.pdf',
                    'original_task': {
                        'trace_id': 'evt-2024-004',
                        'attachment_url': 'https://storage.example.com/claims/C-99999/missing.pdf',
                        'blob_path': 'claims/C-99999/missing.pdf',
                        'status_subtype': 'documentsReceived',
                        'file_type': 'pdf',
                        'assignment_id': 'C-99999',
                        'event_type': 'claim',
                        'event_subtype': 'created',
                        'retry_count': 4,
                        'original_timestamp': '2024-12-25T10:00:00Z',
                        'metadata': {
                            'last_error': 'File not found (404)',
                            'retry_at': '2024-12-25T10:40:00Z'
                        }
                    },
                    'final_error': 'File not found (404) - URL returned 404 Not Found after 4 retry attempts',
                    'error_category': 'permanent',
                    'retry_count': 4,
                    'failed_at': '2024-12-25T10:45:00Z'
                }
            ]
        }
    }
