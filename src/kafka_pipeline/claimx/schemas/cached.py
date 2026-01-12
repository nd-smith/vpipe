"""
ClaimX cached download message schema for Kafka pipeline.

Contains Pydantic model for messages sent to the cached topic after
successful download, before upload to OneLake.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_serializer, field_validator


class ClaimXCachedDownloadMessage(BaseModel):
    """Schema for ClaimX cached downloads awaiting upload to OneLake.

    Sent to claimx.downloads.cached topic after a file has been successfully
    downloaded to local cache. The Upload Worker consumes these messages
    and uploads files to OneLake.

    Attributes:
        media_id: Media file identifier from ClaimX
        project_id: ClaimX project ID this media belongs to
        download_url: Original S3 presigned URL the file was downloaded from
        destination_path: Target path in OneLake (relative to base path)
        local_cache_path: Absolute path to cached file on local filesystem
        bytes_downloaded: Size of the downloaded file in bytes
        content_type: MIME type of the downloaded file (if available)
        file_type: File type/extension (e.g., "pdf", "jpg", "mp4")
        file_name: Original file name
        source_event_id: ID of the event that triggered this download
        downloaded_at: Timestamp when the file was downloaded to cache

    Example:
        >>> from datetime import datetime, timezone
        >>> cached = ClaimXCachedDownloadMessage(
        ...     media_id="media_111",
        ...     project_id="proj_67890",
        ...     download_url="https://s3.amazonaws.com/claimx/presigned...",
        ...     destination_path="claimx/proj_67890/media/photo.jpg",
        ...     local_cache_path="/tmp/kafka_pipeline_cache/media_111/photo.jpg",
        ...     bytes_downloaded=2048576,
        ...     content_type="image/jpeg",
        ...     file_type="jpg",
        ...     file_name="photo.jpg",
        ...     source_event_id="evt_12345",
        ...     downloaded_at=datetime.now(timezone.utc)
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
    destination_path: str = Field(
        ...,
        description="Target path in OneLake (relative to base path)",
        min_length=1
    )
    local_cache_path: str = Field(
        ...,
        description="Absolute path to cached file on local filesystem",
        min_length=1
    )
    bytes_downloaded: int = Field(
        ...,
        description="Size of the downloaded file in bytes",
        ge=0
    )
    content_type: Optional[str] = Field(
        default=None,
        description="MIME type of the downloaded file (if available)"
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
    downloaded_at: datetime = Field(
        ...,
        description="Timestamp when the file was downloaded to cache"
    )

    @field_validator('media_id', 'project_id', 'download_url',
                     'destination_path', 'local_cache_path')
    @classmethod
    def validate_non_empty_strings(cls, v: str, info) -> str:
        """Ensure required string fields are not empty or whitespace-only."""
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} cannot be empty or whitespace")
        return v.strip()

    @field_serializer('downloaded_at')
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
                    'destination_path': 'claimx/proj_67890/media/photo.jpg',
                    'local_cache_path': '/tmp/kafka_pipeline_cache/media_111/photo.jpg',
                    'bytes_downloaded': 2048576,
                    'content_type': 'image/jpeg',
                    'file_type': 'jpg',
                    'file_name': 'photo.jpg',
                    'source_event_id': 'evt_12345',
                    'downloaded_at': '2024-12-25T10:30:05Z'
                }
            ]
        }
    }
