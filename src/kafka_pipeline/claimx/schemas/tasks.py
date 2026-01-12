"""
ClaimX task message schemas for Kafka pipeline.

Contains Pydantic models for ClaimX work items sent to enrichment and download workers.
Enrichment tasks trigger API calls, download tasks handle media file downloads.
"""

from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_serializer, field_validator


class ClaimXEnrichmentTask(BaseModel):
    """Schema for ClaimX enrichment work items sent to enrichment workers.

    Represents a task to enrich a ClaimX event by calling the ClaimX API
    to fetch full entity data (projects, contacts, media, tasks, etc.).

    Unlike xact (which directly downloads attachments), claimx events require
    API enrichment first to get download URLs and entity data.

    Attributes:
        event_id: Unique identifier from the source event (for correlation)
        event_type: Type of event to process (PROJECT_CREATED, PROJECT_FILE_ADDED, etc.)
        project_id: ClaimX project ID to enrich
        retry_count: Number of times this task has been retried (starts at 0)
        created_at: Timestamp when this task was created
        media_id: Optional media ID (for file-related events)
        task_assignment_id: Optional task assignment ID (for task events)
        video_collaboration_id: Optional video collaboration ID (for video events)
        master_file_name: Optional master file name (for MFN events)

    Example:
        >>> task = ClaimXEnrichmentTask(
        ...     event_id="evt_12345",
        ...     event_type="PROJECT_FILE_ADDED",
        ...     project_id="proj_67890",
        ...     retry_count=0,
        ...     created_at=datetime.now(timezone.utc),
        ...     media_id="media_111"
        ... )
    """

    event_id: str = Field(
        ...,
        description="Unique event identifier (from source event)",
        min_length=1
    )
    event_type: str = Field(
        ...,
        description="Event type to process (e.g., PROJECT_CREATED, PROJECT_FILE_ADDED)",
        min_length=1
    )
    project_id: str = Field(
        ...,
        description="ClaimX project ID to enrich via API",
        min_length=1
    )
    retry_count: int = Field(
        default=0,
        description="Number of retry attempts (starts at 0)",
        ge=0
    )
    created_at: datetime = Field(
        ...,
        description="Timestamp when this enrichment task was created"
    )
    media_id: Optional[str] = Field(
        default=None,
        description="Media file ID (for file-related events)"
    )
    task_assignment_id: Optional[str] = Field(
        default=None,
        description="Task assignment ID (for task events)"
    )
    video_collaboration_id: Optional[str] = Field(
        default=None,
        description="Video collaboration ID (for video events)"
    )
    master_file_name: Optional[str] = Field(
        default=None,
        description="Master file name (for MFN events)"
    )
    metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Metadata for enrichment tracking (error context, retry info, etc.)"
    )

    @field_validator('event_id', 'event_type', 'project_id')
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

    @field_serializer('created_at')
    def serialize_timestamp(self, timestamp: datetime) -> str:
        """Serialize datetime to ISO 8601 format."""
        return timestamp.isoformat()

    model_config = {
        'json_schema_extra': {
            'examples': [
                {
                    'event_id': 'evt_12345',
                    'event_type': 'PROJECT_FILE_ADDED',
                    'project_id': 'proj_67890',
                    'retry_count': 0,
                    'created_at': '2024-12-25T10:30:00Z',
                    'media_id': 'media_111'
                },
                {
                    'event_id': 'evt_67890',
                    'event_type': 'PROJECT_CREATED',
                    'project_id': 'proj_12345',
                    'retry_count': 0,
                    'created_at': '2024-12-25T09:00:00Z'
                }
            ]
        }
    }


class ClaimXDownloadTask(BaseModel):
    """Schema for ClaimX media download work items sent to download workers.

    Represents a single media file download task derived from ClaimX API enrichment.
    Created by enrichment worker after calling ClaimX API to get presigned URLs.

    Schema aligned with verisk_pipeline.claimx.claimx_models.MediaTask for compatibility.

    Attributes:
        media_id: Media file identifier from ClaimX
        project_id: ClaimX project ID this media belongs to
        download_url: S3 presigned URL to download the file
        blob_path: Target path in OneLake/blob storage for the downloaded file
        file_type: File type/extension (e.g., "pdf", "jpg", "mp4")
        file_name: Original file name
        source_event_id: ID of the event that triggered this download
        retry_count: Number of times this task has been retried (starts at 0)
        expires_at: Optional ISO datetime when presigned URL expires
        refresh_count: Number of times URL was refreshed (for expired URLs)

    Example:
        >>> task = ClaimXDownloadTask(
        ...     media_id="media_111",
        ...     project_id="proj_67890",
        ...     download_url="https://s3.amazonaws.com/claimx/presigned...",
        ...     blob_path="claimx/proj_67890/media/photo.jpg",
        ...     file_type="jpg",
        ...     file_name="photo.jpg",
        ...     source_event_id="evt_12345",
        ...     retry_count=0
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
        description="S3 presigned URL to download the file",
        min_length=1
    )
    blob_path: str = Field(
        ...,
        description="Target path in OneLake/blob storage",
        min_length=1
    )
    file_type: str = Field(
        default="",
        description="File type/extension (e.g., pdf, jpg, mp4)"
    )
    file_name: str = Field(
        default="",
        description="Original file name"
    )
    source_event_id: str = Field(
        default="",
        description="ID of the event that triggered this download"
    )
    retry_count: int = Field(
        default=0,
        description="Number of retry attempts (starts at 0)",
        ge=0
    )
    expires_at: Optional[str] = Field(
        default=None,
        description="ISO datetime when presigned URL expires"
    )
    refresh_count: int = Field(
        default=0,
        description="Number of times URL was refreshed (for expired URLs)",
        ge=0
    )

    @field_validator('media_id', 'project_id', 'download_url', 'blob_path')
    @classmethod
    def validate_non_empty_strings(cls, v: str, info) -> str:
        """Ensure required string fields are not empty or whitespace-only."""
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} cannot be empty or whitespace")
        return v.strip()

    @field_validator('retry_count', 'refresh_count')
    @classmethod
    def validate_non_negative(cls, v: int, info) -> int:
        """Ensure counter fields are non-negative."""
        if v < 0:
            raise ValueError(f"{info.field_name} must be non-negative")
        return v

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
                    'retry_count': 0,
                    'expires_at': '2024-12-26T10:30:00Z',
                    'refresh_count': 0
                },
                {
                    'media_id': 'media_222',
                    'project_id': 'proj_12345',
                    'download_url': 'https://s3.amazonaws.com/claimx-media/presigned/video.mp4?signature=...',
                    'blob_path': 'claimx/proj_12345/media/video.mp4',
                    'file_type': 'mp4',
                    'file_name': 'damage_video.mp4',
                    'source_event_id': 'evt_67890',
                    'retry_count': 1,
                    'expires_at': '2024-12-26T11:00:00Z',
                    'refresh_count': 1
                }
            ]
        }
    }
