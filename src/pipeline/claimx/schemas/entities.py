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

    trace_id: str | None = None
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
        self.projects.extend(other.projects)
        self.contacts.extend(other.contacts)
        self.media.extend(other.media)
        self.tasks.extend(other.tasks)
        self.task_templates.extend(other.task_templates)
        self.external_links.extend(other.external_links)
        self.video_collab.extend(other.video_collab)

    def row_count(self) -> int:
        return (
            len(self.projects)
            + len(self.contacts)
            + len(self.media)
            + len(self.tasks)
            + len(self.task_templates)
            + len(self.external_links)
            + len(self.video_collab)
        )
