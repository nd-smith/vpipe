"""
ClaimX entity row schemas for Kafka pipeline.

Contains Pydantic models for ClaimX entity data extracted from API responses.
Schema aligned with verisk_pipeline EntityRows for compatibility.

Entity Types:
    - projects: Project metadata
    - contacts: Contact/policyholder information
    - media: Attachment metadata
    - tasks: Task information
    - task_templates: Task template definitions
    - external_links: External resource links
    - video_collab: Video collaboration sessions
"""

from typing import Any

from pydantic import BaseModel, Field


class EntityRowsMessage(BaseModel):
    """Entity rows extracted from ClaimX API for Delta table writes."""

    event_id: str | None = None
    event_type: str | None = None
    project_id: str | None = None
    projects: list[dict[str, Any]] = Field(default_factory=list)
    contacts: list[dict[str, Any]] = Field(default_factory=list)
    media: list[dict[str, Any]] = Field(default_factory=list)
    tasks: list[dict[str, Any]] = Field(default_factory=list)
    task_templates: list[dict[str, Any]] = Field(default_factory=list)
    external_links: list[dict[str, Any]] = Field(default_factory=list)
    video_collab: list[dict[str, Any]] = Field(default_factory=list)

    def is_empty(self) -> bool:
        return not any(
            [
                self.projects,
                self.contacts,
                self.media,
                self.tasks,
                self.task_templates,
                self.external_links,
                self.video_collab,
            ]
        )

    def merge(self, other: "EntityRowsMessage") -> None:
        """Merge another EntityRowsMessage into this one.

        Extends all entity lists with rows from the other message.

        Args:
            other: Another EntityRowsMessage to merge into this one
        """
        self.projects.extend(other.projects)
        self.contacts.extend(other.contacts)
        self.media.extend(other.media)
        self.tasks.extend(other.tasks)
        self.task_templates.extend(other.task_templates)
        self.external_links.extend(other.external_links)
        self.video_collab.extend(other.video_collab)

    def row_count(self) -> int:
        """Get total number of entity rows across all types.

        Returns:
            Total count of all entity rows
        """
        return (
            len(self.projects)
            + len(self.contacts)
            + len(self.media)
            + len(self.tasks)
            + len(self.task_templates)
            + len(self.external_links)
            + len(self.video_collab)
        )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "event_id": "evt_abc123",
                    "event_type": "PROJECT_CREATED",
                    "project_id": "proj_12345",
                    "projects": [
                        {
                            "project_id": "proj_12345",
                            "project_name": "Insurance Claim 2024",
                            "created_at": "2024-12-25T10:00:00Z",
                            "status": "active",
                        }
                    ],
                    "media": [
                        {
                            "media_id": "media_111",
                            "project_id": "proj_12345",
                            "file_name": "photo.jpg",
                            "file_type": "jpg",
                            "file_size": 1024000,
                            "uploaded_at": "2024-12-25T10:30:00Z",
                        }
                    ],
                    "contacts": [],
                    "tasks": [],
                    "task_templates": [],
                    "external_links": [],
                    "video_collab": [],
                }
            ]
        }
    }
