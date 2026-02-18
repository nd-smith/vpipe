"""
ClaimX event message schemas for Kafka pipeline.

Contains Pydantic models for raw ClaimX event messages consumed from source topics.
Schema aligned with verisk_pipeline ClaimXEvent for compatibility.
"""

import hashlib
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator


class ClaimXEventMessage(BaseModel):
    """Schema for raw ClaimX event messages from EventHub.

    Schema matches verisk_pipeline.claimx.claimx_models.ClaimXEvent for compatibility.
    Represents raw ClaimX webhook event data before enrichment.

    Attributes:
        trace_id: Unique identifier for the event
        event_type: Type of event (PROJECT_CREATED, PROJECT_FILE_ADDED, etc.)
        project_id: ClaimX project ID this event belongs to
        ingested_at: Timestamp when event was ingested from webhook
        media_id: Media file ID (for file-related events)
        task_assignment_id: Task assignment ID (for task events)
        video_collaboration_id: Video collaboration ID (for video events)
        master_file_name: Master file name (for MFN events)
        raw_data: Raw event payload for handler-specific parsing

    Event Types:
        - PROJECT_CREATED
        - PROJECT_FILE_ADDED
        - PROJECT_MFN_ADDED
        - CUSTOM_TASK_ASSIGNED
        - CUSTOM_TASK_COMPLETED
        - POLICYHOLDER_INVITED
        - POLICYHOLDER_JOINED
        - VIDEO_COLLABORATION_INVITE_SENT
        - VIDEO_COLLABORATION_COMPLETED

    Example:
        >>> event = ClaimXEventMessage(
        ...     trace_id="evt_12345",
        ...     event_type="PROJECT_FILE_ADDED",
        ...     project_id="proj_67890",
        ...     ingested_at=datetime.now(timezone.utc),
        ...     media_id="media_111",
        ...     raw_data={"fileName": "photo.jpg", "fileSize": 1024}
        ... )
        >>> event.event_type
        'PROJECT_FILE_ADDED'
    """

    trace_id: str = Field(
        ...,
        description="Unique trace identifier",
        min_length=1,
    )
    event_type: str = Field(
        ...,
        description="Event type (e.g., PROJECT_CREATED, PROJECT_FILE_ADDED)",
        min_length=1,
    )
    project_id: str = Field(..., description="ClaimX project ID", min_length=1)
    ingested_at: datetime = Field(..., description="Timestamp when event was ingested from webhook")
    media_id: str | None = Field(
        default=None, description="Media file ID (for file-related events)"
    )
    task_assignment_id: str | None = Field(
        default=None, description="Task assignment ID (for task events)"
    )
    video_collaboration_id: str | None = Field(
        default=None, description="Video collaboration ID (for video events)"
    )
    master_file_name: str | None = Field(
        default=None, description="Master file name (for MFN events)"
    )
    raw_data: dict[str, Any] | None = Field(
        default=None, description="Raw event payload for handler-specific parsing"
    )

    @field_validator("trace_id", "event_type", "project_id")
    @classmethod
    def validate_non_empty_strings(cls, v: str, info) -> str:
        """Ensure string fields are not empty or whitespace-only."""
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} cannot be empty or whitespace")
        return v.strip()

    @staticmethod
    def _get_field(row: dict[str, Any], snake: str, camel: str) -> Any:
        """Get a field from row, checking snake_case then camelCase."""
        return row.get(snake) or row.get(camel)

    @staticmethod
    def _build_trace_id(
        project_id: str, event_type: str, ingested_at: Any,
        media_id: Any, task_id: Any, video_id: Any, master_file: Any,
    ) -> str:
        """Build deterministic trace_id from stable composite key."""
        parts = [project_id, event_type]

        if ingested_at:
            parts.append(ingested_at.isoformat() if isinstance(ingested_at, datetime) else str(ingested_at))

        for prefix, val in [("media", media_id), ("task", task_id), ("video", video_id), ("file", master_file)]:
            if val:
                parts.append(f"{prefix}:{val}")

        return hashlib.sha256("|".join(parts).encode()).hexdigest()

    @classmethod
    def from_raw_event(cls, row: dict[str, Any]) -> "ClaimXEventMessage":
        """
        Create from raw event dict.

        Handles both camelCase and snake_case field names.

        Args:
            row: Dict with event fields

        Returns:
            ClaimXEventMessage instance
        """
        event_type = cls._get_field(row, "event_type", "eventType") or ""
        project_id = str(cls._get_field(row, "project_id", "projectId") or "")
        ingested_at = cls._get_field(row, "ingested_at", "ingestedAt") or datetime.now()

        media_id = cls._get_field(row, "media_id", "mediaId")
        task_id = cls._get_field(row, "task_assignment_id", "taskAssignmentId")
        video_id = cls._get_field(row, "video_collaboration_id", "videoCollaborationId")
        master_file = cls._get_field(row, "master_file_name", "masterFileName")

        trace_id = cls._build_trace_id(
            project_id, event_type, ingested_at,
            media_id, task_id, video_id, master_file,
        )

        return cls(
            trace_id=trace_id,
            event_type=event_type,
            project_id=project_id,
            ingested_at=ingested_at,
            media_id=str(media_id) if media_id is not None else None,
            task_assignment_id=str(task_id) if task_id is not None else None,
            video_collaboration_id=str(video_id) if video_id is not None else None,
            master_file_name=str(master_file) if master_file is not None else None,
            raw_data=row,
        )

    model_config = {
        "coerce_numbers_to_str": True,
        "json_schema_extra": {
            "examples": [
                {
                    "trace_id": "evt_12345",
                    "event_type": "PROJECT_FILE_ADDED",
                    "project_id": "proj_67890",
                    "ingested_at": "2024-12-25T10:30:00Z",
                    "media_id": "media_111",
                    "raw_data": {"fileName": "photo.jpg", "fileSize": 1024},
                },
                {
                    "trace_id": "evt_67890",
                    "event_type": "PROJECT_CREATED",
                    "project_id": "proj_12345",
                    "ingested_at": "2024-12-25T09:00:00Z",
                    "raw_data": {"projectName": "Insurance Claim 2024"},
                },
                {
                    "trace_id": "evt_11111",
                    "event_type": "CUSTOM_TASK_ASSIGNED",
                    "project_id": "proj_22222",
                    "ingested_at": "2024-12-25T14:15:00Z",
                    "task_assignment_id": "task_33333",
                    "raw_data": {
                        "taskName": "Review photos",
                        "assignee": "adjuster@insurance.com",
                    },
                },
            ]
        }
    }
