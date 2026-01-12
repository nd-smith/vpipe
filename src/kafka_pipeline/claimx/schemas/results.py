"""
ClaimX result message schemas for Kafka pipeline.

Contains Pydantic models for:
- Upload outcomes sent to results topic after files are uploaded to OneLake
- Failed enrichment tasks sent to DLQ for manual review and replay
"""

from datetime import datetime
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, Field, field_serializer, field_validator

from kafka_pipeline.claimx.schemas.tasks import (
    ClaimXEnrichmentTask,
    ClaimXDownloadTask,
)


class ClaimXUploadResultMessage(BaseModel):
    """Schema for ClaimX upload outcomes (success or failure).

    Sent to claimx.downloads.results topic after upload worker processes
    a cached download and uploads it to OneLake.

    Attributes:
        media_id: Media file identifier from ClaimX
        project_id: ClaimX project ID this media belongs to
        download_url: Original S3 presigned URL the file was downloaded from
        blob_path: Target path in OneLake (relative to base path)
        file_type: File type/extension (e.g., "pdf", "jpg", "mp4")
        file_name: Original file name
        source_event_id: ID of the event that triggered this download
        status: Outcome status (completed, failed, failed_permanent)
        bytes_uploaded: Number of bytes uploaded (0 if failed)
        error_message: Error description if failed (truncated to 500 chars)
        created_at: Timestamp when result was created

    Example:
        >>> from datetime import datetime, timezone
        >>> result = ClaimXUploadResultMessage(
        ...     media_id="media_111",
        ...     project_id="proj_67890",
        ...     download_url="https://s3.amazonaws.com/claimx/presigned...",
        ...     blob_path="claimx/proj_67890/media/photo.jpg",
        ...     file_type="jpg",
        ...     file_name="photo.jpg",
        ...     source_event_id="evt_12345",
        ...     status="completed",
        ...     bytes_uploaded=2048576,
        ...     created_at=datetime.now(timezone.utc)
        ... )
    """

    media_id: str = Field(
        ...,
        description="Media file identifier from ClaimX",
        min_length=1
    )
    project_id: str = Field(
        ...,
        description="ClaimX project ID",
        min_length=1
    )
    download_url: str = Field(
        ...,
        description="Original S3 presigned URL the file was downloaded from",
        min_length=1
    )
    blob_path: str = Field(
        ...,
        description="Target path in OneLake (relative to base path)",
        min_length=1
    )
    file_type: str = Field(
        default="",
        description="File type/extension (e.g., 'pdf', 'jpg', 'mp4')"
    )
    file_name: str = Field(
        default="",
        description="Original file name"
    )
    source_event_id: str = Field(
        default="",
        description="ID of the event that triggered this download"
    )
    status: Literal["completed", "failed", "failed_permanent"] = Field(
        ...,
        description="Outcome status: completed, failed (transient), or failed_permanent"
    )
    bytes_uploaded: int = Field(
        default=0,
        description="Number of bytes uploaded (0 if failed)",
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

    @field_validator('media_id', 'project_id', 'download_url', 'blob_path')
    @classmethod
    def validate_non_empty_strings(cls, v: str, info) -> str:
        """Ensure required string fields are not empty or whitespace-only."""
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} cannot be empty or whitespace")
        return v.strip()

    @field_serializer('created_at')
    def serialize_timestamp(self, timestamp: datetime) -> str:
        """Serialize datetime to ISO 8601 format."""
        return timestamp.isoformat()

    model_config = {
        'json_schema_extra': {
            'examples': [
                {
                    'media_id': 'media_111',
                    'project_id': 'proj_67890',
                    'download_url': 'https://s3.amazonaws.com/claimx-media/presigned/photo.jpg?signature=...',
                    'blob_path': 'claimx/proj_67890/media/photo.jpg',
                    'file_type': 'jpg',
                    'file_name': 'photo.jpg',
                    'source_event_id': 'evt_12345',
                    'status': 'completed',
                    'bytes_uploaded': 2048576,
                    'created_at': '2024-12-25T10:30:10Z'
                },
                {
                    'media_id': 'media_222',
                    'project_id': 'proj_12345',
                    'download_url': 'https://s3.amazonaws.com/claimx-media/presigned/video.mp4?signature=...',
                    'blob_path': 'claimx/proj_12345/media/video.mp4',
                    'file_type': 'mp4',
                    'file_name': 'damage_video.mp4',
                    'source_event_id': 'evt_67890',
                    'status': 'failed_permanent',
                    'bytes_uploaded': 0,
                    'error_message': 'OneLake upload failed: Connection timeout',
                    'created_at': '2024-12-25T11:15:30Z'
                }
            ]
        }
    }


class FailedEnrichmentMessage(BaseModel):
    """Schema for failed enrichment tasks routed to Dead Letter Queue (DLQ).

    Sent to claimx.enrichment.dlq topic when:
    - Enrichment task fails with permanent error (404, validation error, etc.)
    - Enrichment task exceeds max retry attempts (4 retries)

    Used for:
    - Manual review of failures
    - Replay capability (send back to enrichment.pending with retry_count=0)
    - Audit trail for compliance

    Attributes:
        event_id: Unique event identifier from source event
        event_type: Type of event (e.g., PROJECT_CREATED, PROJECT_FILE_ADDED)
        project_id: ClaimX project ID this event belongs to
        original_task: Complete original enrichment task for replay capability
        final_error: Error message truncated to 500 chars to prevent huge DLQ messages
        error_category: Classification of error (transient, permanent, auth, etc.)
        retry_count: Number of retry attempts before reaching DLQ
        failed_at: Timestamp when task was moved to DLQ

    Example:
        >>> from datetime import datetime, timezone
        >>> failed = FailedEnrichmentMessage(
        ...     event_id="evt_12345",
        ...     event_type="PROJECT_CREATED",
        ...     project_id="proj_67890",
        ...     original_task=enrichment_task,
        ...     final_error="API returned 404: Project not found",
        ...     error_category="permanent",
        ...     retry_count=4,
        ...     failed_at=datetime.now(timezone.utc)
        ... )
    """

    event_id: str = Field(
        ...,
        description="Unique event identifier from source event",
        min_length=1
    )
    event_type: str = Field(
        ...,
        description="Type of event (e.g., PROJECT_CREATED, PROJECT_FILE_ADDED)",
        min_length=1
    )
    project_id: str = Field(
        ...,
        description="ClaimX project ID",
        min_length=1
    )
    original_task: ClaimXEnrichmentTask = Field(
        ...,
        description="Complete original enrichment task for replay capability"
    )
    final_error: str = Field(
        ...,
        description="Error message truncated to 500 chars",
        max_length=500
    )
    error_category: str = Field(
        ...,
        description="Classification of error (transient, permanent, auth, circuit_open, unknown)"
    )
    retry_count: int = Field(
        ...,
        description="Number of retry attempts before reaching DLQ",
        ge=0
    )
    failed_at: datetime = Field(
        ...,
        description="Timestamp when task was moved to DLQ"
    )

    @field_validator('event_id', 'event_type', 'project_id')
    @classmethod
    def validate_non_empty_strings(cls, v: str, info) -> str:
        """Ensure string fields are not empty or whitespace-only."""
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} cannot be empty or whitespace")
        return v.strip()

    @field_serializer('failed_at')
    def serialize_timestamp(self, timestamp: datetime) -> str:
        """Serialize datetime to ISO 8601 format."""
        return timestamp.isoformat()

    model_config = {
        'json_schema_extra': {
            'examples': [
                {
                    'event_id': 'evt_12345',
                    'event_type': 'PROJECT_CREATED',
                    'project_id': 'proj_67890',
                    'original_task': {
                        'event_id': 'evt_12345',
                        'event_type': 'PROJECT_CREATED',
                        'project_id': 'proj_67890',
                        'retry_count': 4,
                        'created_at': '2024-12-25T10:00:00Z'
                    },
                    'final_error': 'ClaimX API returned 404: Project not found. URL: https://api.claimx.com/projects/proj_67890',
                    'error_category': 'permanent',
                    'retry_count': 4,
                    'failed_at': '2024-12-25T12:30:45Z'
                },
                {
                    'event_id': 'evt_67890',
                    'event_type': 'PROJECT_FILE_ADDED',
                    'project_id': 'proj_12345',
                    'original_task': {
                        'event_id': 'evt_67890',
                        'event_type': 'PROJECT_FILE_ADDED',
                        'project_id': 'proj_12345',
                        'media_id': 'media_111',
                        'retry_count': 4,
                        'created_at': '2024-12-25T11:00:00Z'
                    },
                    'final_error': 'Delta write failed after 4 retries: Connection to OneLake timed out',
                    'error_category': 'transient',
                    'retry_count': 4,
                    'failed_at': '2024-12-25T13:15:30Z'
                }
            ]
        }
    }


class FailedDownloadMessage(BaseModel):
    """Schema for failed download tasks routed to Dead Letter Queue (DLQ).

    Sent to claimx.downloads.dlq topic when:
    - Download task fails with permanent error (404, 403, validation error, etc.)
    - Download task exceeds max retry attempts (4 retries)
    - URL refresh fails after expired URL detection

    Used for:
    - Manual review of failures
    - Replay capability (send back to downloads.pending with retry_count=0)
    - Audit trail for compliance

    Attributes:
        media_id: Media file identifier from ClaimX
        project_id: ClaimX project ID this media belongs to
        download_url: Original S3 presigned URL (may be expired)
        blob_path: Target path in OneLake (relative to base path)
        original_task: Complete original download task for replay capability
        final_error: Error message truncated to 500 chars to prevent huge DLQ messages
        error_category: Classification of error (transient, permanent, auth, etc.)
        retry_count: Number of retry attempts before reaching DLQ
        url_refresh_attempted: Whether URL refresh was attempted
        failed_at: Timestamp when task was moved to DLQ

    Example:
        >>> from datetime import datetime, timezone
        >>> failed = FailedDownloadMessage(
        ...     media_id="media_111",
        ...     project_id="proj_67890",
        ...     download_url="https://s3.amazonaws.com/presigned/expired.jpg",
        ...     blob_path="claimx/proj_67890/media/photo.jpg",
        ...     original_task=download_task,
        ...     final_error="403 Forbidden: Presigned URL expired",
        ...     error_category="transient",
        ...     retry_count=4,
        ...     url_refresh_attempted=True,
        ...     failed_at=datetime.now(timezone.utc)
        ... )
    """

    media_id: str = Field(
        ...,
        description="Media file identifier from ClaimX",
        min_length=1
    )
    project_id: str = Field(
        ...,
        description="ClaimX project ID",
        min_length=1
    )
    download_url: str = Field(
        ...,
        description="Original S3 presigned URL (may be expired)",
        min_length=1
    )
    blob_path: str = Field(
        ...,
        description="Target path in OneLake (relative to base path)",
        min_length=1
    )
    original_task: ClaimXDownloadTask = Field(
        ...,
        description="Complete original download task for replay capability"
    )
    final_error: str = Field(
        ...,
        description="Error message truncated to 500 chars",
        max_length=500
    )
    error_category: str = Field(
        ...,
        description="Classification of error (transient, permanent, auth, circuit_open, unknown)"
    )
    retry_count: int = Field(
        ...,
        description="Number of retry attempts before reaching DLQ",
        ge=0
    )
    url_refresh_attempted: bool = Field(
        default=False,
        description="Whether URL refresh was attempted before DLQ"
    )
    failed_at: datetime = Field(
        ...,
        description="Timestamp when task was moved to DLQ"
    )

    @field_validator('media_id', 'project_id', 'download_url', 'blob_path')
    @classmethod
    def validate_non_empty_strings(cls, v: str, info) -> str:
        """Ensure string fields are not empty or whitespace-only."""
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} cannot be empty or whitespace")
        return v.strip()

    @field_serializer('failed_at')
    def serialize_timestamp(self, timestamp: datetime) -> str:
        """Serialize datetime to ISO 8601 format."""
        return timestamp.isoformat()

    model_config = {
        'json_schema_extra': {
            'examples': [
                {
                    'media_id': 'media_111',
                    'project_id': 'proj_67890',
                    'download_url': 'https://s3.amazonaws.com/claimx-media/presigned/photo.jpg?expired=1',
                    'blob_path': 'claimx/proj_67890/media/photo.jpg',
                    'original_task': {
                        'media_id': 'media_111',
                        'project_id': 'proj_67890',
                        'download_url': 'https://s3.amazonaws.com/claimx-media/presigned/photo.jpg?expired=1',
                        'blob_path': 'claimx/proj_67890/media/photo.jpg',
                        'file_type': 'jpg',
                        'file_name': 'photo.jpg',
                        'source_event_id': 'evt_12345',
                        'retry_count': 4,
                        'created_at': '2024-12-25T10:00:00Z'
                    },
                    'final_error': '403 Forbidden: Presigned URL has expired',
                    'error_category': 'transient',
                    'retry_count': 4,
                    'url_refresh_attempted': True,
                    'failed_at': '2024-12-25T12:30:45Z'
                },
                {
                    'media_id': 'media_222',
                    'project_id': 'proj_12345',
                    'download_url': 'https://s3.amazonaws.com/claimx-media/missing.pdf',
                    'blob_path': 'claimx/proj_12345/media/document.pdf',
                    'original_task': {
                        'media_id': 'media_222',
                        'project_id': 'proj_12345',
                        'download_url': 'https://s3.amazonaws.com/claimx-media/missing.pdf',
                        'blob_path': 'claimx/proj_12345/media/document.pdf',
                        'file_type': 'pdf',
                        'file_name': 'document.pdf',
                        'source_event_id': 'evt_67890',
                        'retry_count': 0,
                        'created_at': '2024-12-25T11:00:00Z'
                    },
                    'final_error': '404 Not Found: Media file does not exist',
                    'error_category': 'permanent',
                    'retry_count': 0,
                    'url_refresh_attempted': False,
                    'failed_at': '2024-12-25T11:05:15Z'
                }
            ]
        }
    }
