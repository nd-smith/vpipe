"""
Data models for iTel Cabinet API plugin.

Clear, typed structures for data flowing through the pipeline.
No magic, no guessing - explicit types everywhere.
"""

from dataclasses import dataclass
from datetime import datetime


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
    assigned_to_user_id: int | None = None
    assigned_by_user_id: int | None = None

    # Optional timestamps
    task_created_at: str | None = None
    task_completed_at: str | None = None

    @classmethod
    def from_message(cls, raw: dict) -> "TaskEvent":
        """Parse transport message into typed TaskEvent.

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
            raise ValueError(f"Missing required field in message: {e}") from e


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

    # Task metadata (from parse.py integration)
    task_id: int | None = None
    task_name: str | None = None

    # Dates
    date_assigned: str | None = None
    date_completed: str | None = None
    ingested_at: datetime | None = None

    # Customer information
    customer_first_name: str | None = None
    customer_last_name: str | None = None
    customer_email: str | None = None
    customer_phone: str | None = None
    assignor_email: str | None = None
    external_link_url: str | None = None

    # General damage information
    damage_description: str | None = None
    additional_notes: str | None = None
    countertops_lf: float | None = None

    # Raw data blob (preserves original structure as JSON string)
    raw_data: str | None = None

    # Lower cabinets
    lower_cabinets_damaged: bool | None = None
    lower_cabinets_lf: float | None = None
    num_damaged_lower_boxes: int | None = None
    lower_cabinets_detached: bool | None = None
    lower_face_frames_doors_drawers_available: str | None = (
        None  # "Yes"/"No" string per table schema
    )
    lower_face_frames_doors_drawers_damaged: bool | None = None
    lower_finished_end_panels_damaged: bool | None = None
    lower_end_panel_damage_present: bool | None = None
    lower_counter_type: str | None = None

    # Upper cabinets
    upper_cabinets_damaged: bool | None = None
    upper_cabinets_lf: float | None = None
    num_damaged_upper_boxes: int | None = None
    upper_cabinets_detached: bool | None = None
    upper_face_frames_doors_drawers_available: str | None = (
        None  # "Yes"/"No" string per table schema
    )
    upper_face_frames_doors_drawers_damaged: bool | None = None
    upper_finished_end_panels_damaged: bool | None = None
    upper_end_panel_damage_present: bool | None = None

    # Full height cabinets
    full_height_cabinets_damaged: bool | None = None
    full_height_cabinets_lf: float | None = None
    num_damaged_full_height_boxes: int | None = None
    full_height_cabinets_detached: bool | None = None
    full_height_face_frames_doors_drawers_available: str | None = (
        None  # "Yes"/"No" string per table schema
    )
    full_height_face_frames_doors_drawers_damaged: bool | None = None
    full_height_finished_end_panels_damaged: bool | None = None

    # Island cabinets
    island_cabinets_damaged: bool | None = None
    island_cabinets_lf: float | None = None
    num_damaged_island_boxes: int | None = None
    island_cabinets_detached: bool | None = None
    island_face_frames_doors_drawers_available: str | None = (
        None  # "Yes"/"No" string per table schema
    )
    island_face_frames_doors_drawers_damaged: bool | None = None
    island_finished_end_panels_damaged: bool | None = None
    island_end_panel_damage_present: bool | None = None
    island_counter_type: str | None = None

    # Metadata
    created_at: datetime | None = None
    updated_at: datetime | None = None

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
    project_id: int  # ClaimX project ID
    event_id: str  # Kafka event ID for traceability
    control_id: str  # Form control ID for tracking
    question_key: str
    question_text: str
    topic_category: str  # e.g., "Lower Cabinets", "Upper Cabinets", "General"
    media_id: int  # ClaimX media ID (was claim_media_id)
    url: str | None = None  # Download URL from ClaimX
    display_order: int = 0
    created_at: datetime | None = None
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
class ProcessedTask:
    """
    Final output after pipeline processing.

    Contains everything we learned about the task.
    """

    event: TaskEvent
    submission: CabinetSubmission | None
    attachments: list[CabinetAttachment]
    readable_report: dict | None = None  # Topic-organized report for API consumption

    def was_enriched(self) -> bool:
        """Check if task was fully enriched (vs metadata-only)."""
        return self.submission is not None
