"""
Data models for ClaimX Mitigation Task plugin.

Clear, typed structures for data flowing through the pipeline.
No magic, no guessing - explicit types everywhere.
"""

from dataclasses import dataclass


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
    assigned_to_user_id: int | None = None
    assigned_by_user_id: int | None = None

    # Optional timestamps
    task_created_at: str | None = None
    task_completed_at: str | None = None

    @classmethod
    def from_kafka_message(cls, raw: dict) -> "MitigationTaskEvent":
        """
        Parse Kafka message into typed MitigationTaskEvent.

        Raises:
            ValueError: If required fields are missing
        """
        try:
            return cls(
                event_id=raw["event_id"],
                event_type=raw["event_type"],
                event_timestamp=raw["timestamp"],
                task_id=raw["task_id"],
                assignment_id=raw["assignment_id"],
                project_id=raw["project_id"],
                task_name=raw["task_name"],
                task_status=raw["task_status"],
                assigned_to_user_id=raw.get("task", {}).get("assigned_to_user_id"),
                assigned_by_user_id=raw.get("task", {}).get("assigned_by_user_id"),
                task_created_at=raw.get("task", {}).get("created_at"),
                task_completed_at=raw.get("task", {}).get("completed_at"),
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

    # Project data from export/project API
    master_filename: str | None = None
    type_of_loss: str | None = None
    claim_number: str | None = None  # from projectNumber
    policy_number: str | None = None  # from secondaryNumber

    # Dates from API response
    date_assigned: str | None = None
    date_completed: str | None = None

    # All project media metadata
    media: list[dict] | None = None

    # Processing metadata
    ingested_at: str | None = None

    def to_flat_dict(self) -> dict:
        """Convert to flat dictionary for Kafka publish."""
        return {
            "event_id": self.event_id,
            "assignment_id": self.assignment_id,
            "project_id": self.project_id,
            "task_id": self.task_id,
            "task_name": self.task_name,
            "status": self.status,
            "master_filename": self.master_filename,
            "type_of_loss": self.type_of_loss,
            "claim_number": self.claim_number,
            "policy_number": self.policy_number,
            "date_assigned": self.date_assigned,
            "date_completed": self.date_completed,
            "media": self.media,
            "ingested_at": self.ingested_at,
        }


@dataclass
class ProcessedMitigationTask:
    """
    Final output after pipeline processing.
    """

    event: MitigationTaskEvent
    submission: MitigationSubmission | None

    def was_enriched(self) -> bool:
        """Check if task was fully enriched."""
        return self.submission is not None
