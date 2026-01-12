"""
Data models for iTel Cabinet API plugin.

Clear, typed structures for data flowing through the pipeline.
No magic, no guessing - explicit types everywhere.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class TaskEvent:
    """
    Input event from Kafka topic (itel.cabinet.task.tracking).

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
    def from_kafka_message(cls, raw: dict) -> "TaskEvent":
        """
        Parse Kafka message into typed TaskEvent.

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
class CabinetSubmission:
    """
    Parsed iTel Cabinet form submission.

    This is the structured data extracted from ClaimX task response.
    """
    # Primary identifiers
    assignment_id: int
    project_id: str  # String from ClaimX (e.g., "5395115")
    form_id: str  # MongoDB ObjectId string
    form_response_id: str  # MongoDB ObjectId string
    status: str
    event_id: str  # Kafka event ID for traceability

    # Dates
    date_assigned: Optional[str]
    date_completed: Optional[str]

    # Customer information
    customer_first_name: Optional[str]
    customer_last_name: Optional[str]
    customer_email: Optional[str]
    customer_phone: Optional[str]
    assignor_email: Optional[str]

    # General damage information
    damage_description: Optional[str]
    additional_notes: Optional[str]
    countertops_lf: Optional[float]

    # Lower cabinets
    lower_cabinets_damaged: Optional[bool]
    lower_cabinets_lf: Optional[float]
    num_damaged_lower_boxes: Optional[int]
    lower_cabinets_detached: Optional[bool]
    lower_face_frames_doors_drawers_available: Optional[bool]
    lower_face_frames_doors_drawers_damaged: Optional[bool]
    lower_finished_end_panels_damaged: Optional[bool]
    lower_end_panel_damage_present: Optional[bool]
    lower_counter_type: Optional[str]

    # Upper cabinets
    upper_cabinets_damaged: Optional[bool]
    upper_cabinets_lf: Optional[float]
    num_damaged_upper_boxes: Optional[int]
    upper_cabinets_detached: Optional[bool]
    upper_face_frames_doors_drawers_available: Optional[bool]
    upper_face_frames_doors_drawers_damaged: Optional[bool]
    upper_finished_end_panels_damaged: Optional[bool]
    upper_end_panel_damage_present: Optional[bool]

    # Full height cabinets
    full_height_cabinets_damaged: Optional[bool]
    full_height_cabinets_lf: Optional[float]
    num_damaged_full_height_boxes: Optional[int]
    full_height_cabinets_detached: Optional[bool]
    full_height_face_frames_doors_drawers_available: Optional[bool]
    full_height_face_frames_doors_drawers_damaged: Optional[bool]
    full_height_finished_end_panels_damaged: Optional[bool]

    # Island cabinets
    island_cabinets_damaged: Optional[bool]
    island_cabinets_lf: Optional[float]
    num_damaged_island_boxes: Optional[int]
    island_cabinets_detached: Optional[bool]
    island_face_frames_doors_drawers_available: Optional[bool]
    island_face_frames_doors_drawers_damaged: Optional[bool]
    island_finished_end_panels_damaged: Optional[bool]
    island_end_panel_damage_present: Optional[bool]
    island_counter_type: Optional[str]

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
class CabinetAttachment:
    """
    Media attachment linked to form question.
    """
    assignment_id: int
    event_id: str  # Kafka event ID for traceability
    question_key: str
    question_text: str
    claim_media_id: int
    blob_path: Optional[str] = None
    display_order: int = 0
    created_at: Optional[datetime] = None

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
class ProcessedTask:
    """
    Final output after pipeline processing.

    Contains everything we learned about the task.
    """
    event: TaskEvent
    submission: Optional[CabinetSubmission]
    attachments: list[CabinetAttachment]

    def was_enriched(self) -> bool:
        """Check if task was fully enriched (vs metadata-only)."""
        return self.submission is not None
