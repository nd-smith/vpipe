"""
Data models for ClaimX Mitigation Task plugin.

Clear, typed structures for data flowing through the pipeline.
No magic, no guessing - explicit types everywhere.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class MitigationTaskEvent:
    """
    Input event from Kafka topic (claimx.mitigation.task.tracking).

    This is what the plugin publishes when a task changes.
    """
    event_id: str
    event_type: str
    event_timestamp: str

    task_id: int
    assignment_id: int
    project_id: str  # String from ClaimX (e.g., "5395115")

    task_name: str
    task_status: str  # ASSIGNED, IN_PROGRESS, COMPLETED

    # Optional user tracking
    assigned_to_user_id: Optional[int] = None
    assigned_by_user_id: Optional[int] = None

    # Optional timestamps
    task_created_at: Optional[str] = None
    task_completed_at: Optional[str] = None

    @classmethod
    def from_kafka_message(cls, raw: dict) -> "MitigationTaskEvent":
        """
        Parse Kafka message into typed MitigationTaskEvent.

        Raises:
            ValueError: If required fields are missing
        """
        try:
            return cls(
                event_id=raw['event_id'],
                event_type=raw['event_type'],
                event_timestamp=raw['timestamp'],
                task_id=raw['task_id'],
                assignment_id=raw['assignment_id'],
                project_id=raw['project_id'],
                task_name=raw['task_name'],
                task_status=raw['task_status'],
                assigned_to_user_id=raw.get('task', {}).get('assigned_to_user_id'),
                assigned_by_user_id=raw.get('task', {}).get('assigned_by_user_id'),
                task_created_at=raw.get('task', {}).get('created_at'),
                task_completed_at=raw.get('task', {}).get('completed_at'),
            )
        except KeyError as e:
            raise ValueError(f"Missing required field in Kafka message: {e}")


@dataclass
class MitigationSubmission:
    """
    Parsed mitigation task submission.

    This is the structured data extracted from ClaimX task response.
    Fields will be expanded as the model is built out.
    """
    # Primary identifiers
    assignment_id: int
    project_id: str  # String from ClaimX (e.g., "5395115")
    form_id: str  # MongoDB ObjectId string
    form_response_id: str  # MongoDB ObjectId string
    status: str
    event_id: str  # Kafka event ID for traceability

    # Task metadata
    task_id: Optional[int] = None
    task_name: Optional[str] = None

    # Dates
    date_assigned: Optional[str] = None
    date_completed: Optional[str] = None
    ingested_at: Optional[datetime] = None

    # Raw data blob (preserves original structure as JSON string)
    raw_data: Optional[str] = None

    # Metadata
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for Delta write or Kafka publish."""
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, datetime):
                result[key] = value.isoformat()
            else:
                result[key] = value
        return result


@dataclass
class MitigationAttachment:
    """
    Media attachment linked to form question.
    """
    assignment_id: int
    project_id: int  # ClaimX project ID
    event_id: str  # Kafka event ID for traceability
    control_id: str  # Form control ID for tracking
    question_key: str
    question_text: str
    topic_category: str  # e.g., "Mitigation", "Documentation"
    media_id: int  # ClaimX media ID
    url: Optional[str] = None  # Download URL from ClaimX
    display_order: int = 0
    created_at: Optional[datetime] = None
    is_active: bool = True
    media_type: str = "image/jpeg"  # Default assumption

    def to_dict(self) -> dict:
        """Convert to dictionary for Delta write."""
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, datetime):
                result[key] = value.isoformat()
            else:
                result[key] = value
        return result


@dataclass
class ProcessedMitigationTask:
    """
    Final output after pipeline processing.

    Contains everything we learned about the task.
    """
    event: MitigationTaskEvent
    submission: Optional[MitigationSubmission]
    attachments: list[MitigationAttachment]

    def was_enriched(self) -> bool:
        """Check if task was fully enriched (vs metadata-only)."""
        return self.submission is not None
