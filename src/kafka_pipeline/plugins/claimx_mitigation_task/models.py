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
    Flat mitigation task submission structure.

    All fields at top level for easy consumption.
    """
    # Primary identifiers
    event_id: str
    assignment_id: int
    project_id: str
    task_id: int
    task_name: str
    status: str

    # Form identifiers
    form_id: str
    form_response_id: str

    # Project data from export/project API
    master_filename: Optional[str] = None
    type_of_loss: Optional[str] = None
    claim_number: Optional[str] = None  # from projectNumber
    policy_number: Optional[str] = None  # from secondaryNumber

    # Dates from API response
    date_assigned: Optional[str] = None
    date_completed: Optional[str] = None

    # Media IDs array from claimMediaIds
    claim_media_ids: Optional[list[int]] = None

    # Media metadata (filtered to claim_media_ids only)
    media: Optional[list[dict]] = None

    # Processing metadata
    ingested_at: Optional[str] = None

    def to_flat_dict(self) -> dict:
        """Convert to flat dictionary for Kafka publish."""
        return {
            'event_id': self.event_id,
            'assignment_id': self.assignment_id,
            'project_id': self.project_id,
            'task_id': self.task_id,
            'task_name': self.task_name,
            'status': self.status,
            'form_id': self.form_id,
            'form_response_id': self.form_response_id,
            'master_filename': self.master_filename,
            'type_of_loss': self.type_of_loss,
            'claim_number': self.claim_number,
            'policy_number': self.policy_number,
            'date_assigned': self.date_assigned,
            'date_completed': self.date_completed,
            'claim_media_ids': self.claim_media_ids,
            'media': self.media,
            'ingested_at': self.ingested_at,
        }


@dataclass
class ProcessedMitigationTask:
    """
    Final output after pipeline processing.
    """
    event: MitigationTaskEvent
    submission: Optional[MitigationSubmission]

    def was_enriched(self) -> bool:
        """Check if task was fully enriched."""
        return self.submission is not None
