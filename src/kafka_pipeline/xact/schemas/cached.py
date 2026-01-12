"""
Cached download message schema for Kafka pipeline.

Contains Pydantic model for messages sent to the cached topic after
successful download, before upload to OneLake.
"""

from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_serializer, field_validator


class CachedDownloadMessage(BaseModel):
    """Schema for cached downloads awaiting upload to OneLake.

    Sent to downloads.cached topic after a file has been successfully
    downloaded to local cache. The Upload Worker consumes these messages
    and uploads files to OneLake.

    Attributes:
        trace_id: Unique event identifier for correlation
        attachment_url: Original URL the file was downloaded from
        destination_path: Target path in OneLake (relative to base path)
        local_cache_path: Absolute path to cached file on local filesystem
        bytes_downloaded: Size of the downloaded file in bytes
        content_type: MIME type of the downloaded file (if available)
        event_type: Type of the originating event (e.g., "claim", "policy")
        event_subtype: Subtype of the originating event (e.g., "created")
        status_subtype: Event status subtype (e.g., "documentsReceived")
        file_type: File type extracted from URL extension (e.g., "pdf", "esx")
        assignment_id: Assignment ID from event payload
        original_timestamp: Timestamp from the original event
        downloaded_at: Timestamp when the file was downloaded to cache
        metadata: Additional context passed through from original task

    Example:
        >>> from datetime import datetime, timezone
        >>> cached = CachedDownloadMessage(
        ...     trace_id="evt-123",
        ...     attachment_url="https://storage.example.com/file.pdf",
        ...     destination_path="claims/C-456/file.pdf",
        ...     local_cache_path="/tmp/kafka_pipeline_cache/evt-123/file.pdf",
        ...     bytes_downloaded=2048576,
        ...     content_type="application/pdf",
        ...     event_type="claim",
        ...     event_subtype="created",
        ...     status_subtype="documentsReceived",
        ...     file_type="pdf",
        ...     assignment_id="A-789",
        ...     original_timestamp=datetime.now(timezone.utc),
        ...     downloaded_at=datetime.now(timezone.utc)
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
        description="Original URL the file was downloaded from",
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
    event_type: str = Field(
        ...,
        description="Type of the originating event",
        min_length=1
    )
    event_subtype: str = Field(
        ...,
        description="Subtype of the originating event",
        min_length=1
    )
    status_subtype: str = Field(
        ...,
        description="Event status subtype (e.g., 'documentsReceived')",
        min_length=1
    )
    file_type: str = Field(
        ...,
        description="File type extracted from URL extension (e.g., 'pdf', 'esx')",
        min_length=1
    )
    assignment_id: str = Field(
        ...,
        description="Assignment ID from event payload",
        min_length=1
    )
    original_timestamp: datetime = Field(
        ...,
        description="Timestamp from the original event"
    )
    downloaded_at: datetime = Field(
        ...,
        description="Timestamp when the file was downloaded to cache"
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional context passed through from original task"
    )

    @field_validator('trace_id', 'media_id', 'attachment_url', 'destination_path',
                     'local_cache_path', 'event_type', 'event_subtype',
                     'status_subtype', 'file_type', 'assignment_id')
    @classmethod
    def validate_non_empty_strings(cls, v: str, info) -> str:
        """Ensure string fields are not empty or whitespace-only."""
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} cannot be empty or whitespace")
        return v.strip()

    @field_serializer('original_timestamp', 'downloaded_at')
    def serialize_timestamp(self, timestamp: datetime) -> str:
        """Serialize datetime to ISO 8601 format."""
        return timestamp.isoformat()

    model_config = {
        'json_schema_extra': {
            'examples': [
                {
                    'trace_id': 'evt-2024-001',
                    'attachment_url': 'https://storage.example.com/claims/C-12345/document.pdf',
                    'destination_path': 'claims/C-12345/document.pdf',
                    'local_cache_path': '/tmp/kafka_pipeline_cache/evt-2024-001/document.pdf',
                    'bytes_downloaded': 2048576,
                    'content_type': 'application/pdf',
                    'event_type': 'claim',
                    'event_subtype': 'created',
                    'status_subtype': 'documentsReceived',
                    'file_type': 'pdf',
                    'assignment_id': 'A-789',
                    'original_timestamp': '2024-12-25T10:30:00Z',
                    'downloaded_at': '2024-12-25T10:30:05Z',
                    'metadata': {
                        'source_partition': 3
                    }
                }
            ]
        }
    }
