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
        event_id: Unique identifier for the event
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
        ...     event_id="evt_12345",
        ...     event_type="PROJECT_FILE_ADDED",
        ...     project_id="proj_67890",
        ...     ingested_at=datetime.now(timezone.utc),
        ...     media_id="media_111",
        ...     raw_data={"fileName": "photo.jpg", "fileSize": 1024}
        ... )
        >>> event.event_type
        'PROJECT_FILE_ADDED'
    """

    event_id: str = Field(..., description="Unique event identifier", min_length=1)
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

    @field_validator("event_id", "event_type", "project_id")
    @classmethod
    def validate_non_empty_strings(cls, v: str, info) -> str:
        """Ensure string fields are not empty or whitespace-only."""
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} cannot be empty or whitespace")
        return v.strip()

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
        event_id = row.get("event_id") or row.get("eventId") or ""
        event_type = row.get("event_type") or row.get("eventType") or ""
        project_id = row.get("project_id") or row.get("projectId") or ""
        ingested_at = row.get("ingested_at") or row.get("ingestedAt") or datetime.now()

        # Generate deterministic ID if missing
        if not event_id:
            composite_parts = [project_id, event_type]

            # Use ingested_at (webhook timestamp) as the stable timestamp
            if ingested_at:
                if isinstance(ingested_at, datetime):
                    composite_parts.append(ingested_at.isoformat())
                else:
                    composite_parts.append(str(ingested_at))

            # Add optional identifiers for additional uniqueness
            media_id = row.get("media_id") or row.get("mediaId")
            task_id = row.get("task_assignment_id") or row.get("taskAssignmentId")
            video_id = row.get("video_collaboration_id") or row.get("videoCollaborationId")
            master_file = row.get("master_file_name") or row.get("masterFileName")

            if media_id:
                composite_parts.append(f"media:{media_id}")
            if task_id:
                composite_parts.append(f"task:{task_id}")
            if video_id:
                composite_parts.append(f"video:{video_id}")
            if master_file:
                composite_parts.append(f"file:{master_file}")

            composite_key = "|".join(composite_parts)
            event_id = hashlib.sha256(composite_key.encode()).hexdigest()

        return cls(
            event_id=event_id,
            event_type=event_type,
            project_id=project_id,
            ingested_at=ingested_at,
            media_id=row.get("media_id") or row.get("mediaId"),
            task_assignment_id=row.get("task_assignment_id") or row.get("taskAssignmentId"),
            video_collaboration_id=row.get("video_collaboration_id")
            or row.get("videoCollaborationId"),
            master_file_name=row.get("master_file_name") or row.get("masterFileName"),
            raw_data=row,
        )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "event_id": "evt_12345",
                    "event_type": "PROJECT_FILE_ADDED",
                    "project_id": "proj_67890",
                    "ingested_at": "2024-12-25T10:30:00Z",
                    "media_id": "media_111",
                    "raw_data": {"fileName": "photo.jpg", "fileSize": 1024},
                },
                {
                    "event_id": "evt_67890",
                    "event_type": "PROJECT_CREATED",
                    "project_id": "proj_12345",
                    "ingested_at": "2024-12-25T09:00:00Z",
                    "raw_data": {"projectName": "Insurance Claim 2024"},
                },
                {
                    "event_id": "evt_11111",
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
