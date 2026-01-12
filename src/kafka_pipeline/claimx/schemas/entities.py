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

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class EntityRowsMessage(BaseModel):
    """Container for entity rows extracted from ClaimX API enrichment.

    Handlers populate the relevant lists based on event type and API responses.
    Each list contains dictionaries representing rows to write to Delta tables.

    Schema matches verisk_pipeline.claimx.claimx_models.EntityRows for compatibility.

    Entity Tables (7 types):
        - projects → claimx_projects (merge key: project_id)
        - contacts → claimx_contacts (merge keys: contact_id, project_id)
        - media → claimx_attachment_metadata (merge key: media_id)
        - tasks → claimx_tasks (merge key: task_id)
        - task_templates → claimx_task_templates (merge keys: template_id, project_id)
        - external_links → claimx_external_links (merge key: link_id)
        - video_collab → claimx_video_collab (merge key: collaboration_id)

    Attributes:
        event_id: Original ClaimX event ID for traceability
        event_type: Original event type (e.g., PROJECT_CREATED)
        project_id: Project ID from the original event
        projects: List of project entity rows
        contacts: List of contact/policyholder entity rows
        media: List of media/attachment metadata rows
        tasks: List of task entity rows
        task_templates: List of task template rows
        external_links: List of external link rows
        video_collab: List of video collaboration rows

    Example:
        >>> rows = EntityRowsMessage(event_id="evt_123", event_type="PROJECT_CREATED")
        >>> rows.projects.append({
        ...     'project_id': 'proj_12345',
        ...     'project_name': 'Insurance Claim 2024',
        ...     'created_at': '2024-12-25T10:00:00Z'
        ... })
        >>> rows.media.append({
        ...     'media_id': 'media_111',
        ...     'project_id': 'proj_12345',
        ...     'file_name': 'photo.jpg',
        ...     'file_type': 'jpg'
        ... })
        >>> rows.is_empty()
        False
    """

    # Traceability fields - track origin event for debugging and retry handling
    event_id: Optional[str] = Field(
        default=None,
        description="Original ClaimX event ID for end-to-end traceability"
    )
    event_type: Optional[str] = Field(
        default=None,
        description="Original event type (e.g., PROJECT_CREATED, PROJECT_FILE_ADDED)"
    )
    project_id: Optional[str] = Field(
        default=None,
        description="Project ID from the original event"
    )

    projects: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Project entity rows (table: claimx_projects)"
    )
    contacts: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Contact/policyholder entity rows (table: claimx_contacts)"
    )
    media: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Media/attachment metadata rows (table: claimx_attachment_metadata)"
    )
    tasks: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Task entity rows (table: claimx_tasks)"
    )
    task_templates: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Task template rows (table: claimx_task_templates)"
    )
    external_links: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="External link rows (table: claimx_external_links)"
    )
    video_collab: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Video collaboration rows (table: claimx_video_collab)"
    )

    def is_empty(self) -> bool:
        """Check if all entity lists are empty.

        Returns:
            True if no entity rows exist, False otherwise
        """
        return not any([
            self.projects,
            self.contacts,
            self.media,
            self.tasks,
            self.task_templates,
            self.external_links,
            self.video_collab,
        ])

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
        'json_schema_extra': {
            'examples': [
                {
                    'event_id': 'evt_abc123',
                    'event_type': 'PROJECT_CREATED',
                    'project_id': 'proj_12345',
                    'projects': [
                        {
                            'project_id': 'proj_12345',
                            'project_name': 'Insurance Claim 2024',
                            'created_at': '2024-12-25T10:00:00Z',
                            'status': 'active'
                        }
                    ],
                    'media': [
                        {
                            'media_id': 'media_111',
                            'project_id': 'proj_12345',
                            'file_name': 'photo.jpg',
                            'file_type': 'jpg',
                            'file_size': 1024000,
                            'uploaded_at': '2024-12-25T10:30:00Z'
                        }
                    ],
                    'contacts': [],
                    'tasks': [],
                    'task_templates': [],
                    'external_links': [],
                    'video_collab': []
                }
            ]
        }
    }
