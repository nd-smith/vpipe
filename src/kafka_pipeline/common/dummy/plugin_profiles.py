"""
Plugin-specific data generation profiles for testing workflows.

Provides pre-configured data generators that produce events matching
the requirements of specific plugin workflows, like the itel Cabinet API plugin.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from enum import Enum


class PluginProfile(Enum):
    """Available plugin profiles for dummy data generation."""

    ITEL_CABINET_API = "itel_cabinet_api"
    STANDARD_CLAIMX = "standard_claimx"
    STANDARD_XACT = "standard_xact"


@dataclass
class ItelCabinetFormData:
    """Data structure for itel Cabinet Repair Form."""

    # Required fields
    assignment_id: str
    project_id: str
    form_id: str
    form_response_id: str
    status: str
    date_assigned: str

    # Optional fields
    date_completed: Optional[str] = None
    assignor_email: Optional[str] = None
    damage_description: Optional[str] = None
    additional_notes: Optional[str] = None
    countertops_lf: Optional[float] = None

    # Customer info
    customer_first_name: Optional[str] = None
    customer_last_name: Optional[str] = None
    customer_email: Optional[str] = None
    customer_phone: Optional[str] = None

    # Cabinet sections
    lower_cabinets_damaged: Optional[bool] = None
    lower_cabinets_lf: Optional[float] = None
    num_damaged_lower_boxes: Optional[int] = None
    lower_cabinets_detached: Optional[bool] = None
    lower_face_frames_doors_drawers_available: Optional[bool] = None
    lower_face_frames_doors_drawers_damaged: Optional[bool] = None
    lower_finished_end_panels_damaged: Optional[bool] = None
    lower_end_panel_damage_present: Optional[bool] = None
    lower_counter_type: Optional[str] = None

    upper_cabinets_damaged: Optional[bool] = None
    upper_cabinets_lf: Optional[float] = None
    num_damaged_upper_boxes: Optional[int] = None
    upper_cabinets_detached: Optional[bool] = None
    upper_face_frames_doors_drawers_available: Optional[bool] = None
    upper_face_frames_doors_drawers_damaged: Optional[bool] = None
    upper_finished_end_panels_damaged: Optional[bool] = None
    upper_end_panel_damage_present: Optional[bool] = None
    upper_counter_type: Optional[str] = None

    # Media/attachments (question_key -> media_ids)
    overview_photos: List[str] = field(default_factory=list)
    lower_cabinet_box_photos: List[str] = field(default_factory=list)
    lower_cabinet_end_panel_photos: List[str] = field(default_factory=list)
    upper_cabinet_box_photos: List[str] = field(default_factory=list)
    upper_cabinet_end_panel_photos: List[str] = field(default_factory=list)


class ItelCabinetDataGenerator:
    """Generates realistic itel Cabinet Repair Form data."""

    # itel Cabinet specific task_id
    ITEL_TASK_ID = 32513
    ITEL_TASK_NAME = "iTel Cabinet Repair Form"

    # Cabinet types for damage scenarios
    CABINET_TYPES = ["lower", "upper", "full_height", "island"]

    # Counter types
    COUNTER_TYPES = [
        "Laminate",
        "Granite",
        "Quartz",
        "Marble",
        "Butcher Block",
        "Concrete",
        "Tile",
        "Solid Surface",
    ]

    # Damage scenarios
    DAMAGE_SCENARIOS = [
        {
            "description": "Water damage from burst pipe affecting lower cabinets",
            "lower_damaged": True,
            "upper_damaged": False,
            "lower_lf": 12.5,
            "num_damaged_lower": 3,
            "detached": True,
            "counter_type": "Laminate",
        },
        {
            "description": "Fire damage to kitchen cabinets - smoke and heat damage",
            "lower_damaged": True,
            "upper_damaged": True,
            "lower_lf": 18.0,
            "upper_lf": 15.0,
            "num_damaged_lower": 5,
            "num_damaged_upper": 4,
            "detached": False,
            "counter_type": "Granite",
        },
        {
            "description": "Impact damage from fallen shelving unit",
            "lower_damaged": True,
            "upper_damaged": False,
            "lower_lf": 8.0,
            "num_damaged_lower": 2,
            "detached": False,
            "counter_type": "Quartz",
        },
        {
            "description": "Flood damage affecting entire kitchen",
            "lower_damaged": True,
            "upper_damaged": True,
            "lower_lf": 24.0,
            "upper_lf": 20.0,
            "num_damaged_lower": 8,
            "num_damaged_upper": 6,
            "detached": True,
            "counter_type": "Marble",
        },
    ]

    def __init__(self, rng):
        """Initialize with random number generator."""
        self.rng = rng

    def generate_form_data(self, claim) -> ItelCabinetFormData:
        """Generate itel Cabinet form data for a claim.

        Args:
            claim: GeneratedClaim object from the main generator

        Returns:
            ItelCabinetFormData with realistic cabinet repair data
        """
        # Select damage scenario
        scenario = self.rng.choice(self.DAMAGE_SCENARIOS)

        # Generate IDs
        form_id = f"form_{self.rng.randint(10000, 99999)}"
        form_response_id = f"response_{self.rng.randint(100000, 999999)}"

        # Date assigned (1-5 days ago)
        date_assigned = (
            datetime.now(timezone.utc) - timedelta(days=self.rng.randint(1, 5))
        ).isoformat()

        # Date completed (for completed tasks)
        date_completed = None
        if self.rng.random() > 0.3:  # 70% chance task is completed
            date_completed = (
                datetime.now(timezone.utc) - timedelta(hours=self.rng.randint(1, 48))
            ).isoformat()

        status = "completed" if date_completed else "assigned"

        # Build form data
        form_data = ItelCabinetFormData(
            assignment_id=claim.claim_id,
            project_id=claim.project_id,
            form_id=form_id,
            form_response_id=form_response_id,
            status=status,
            date_assigned=date_assigned,
            date_completed=date_completed,
            assignor_email=f"{claim.assigned_adjuster.lower().replace(' ', '.')}@insurance.com",
            damage_description=scenario["description"],
            additional_notes=self._generate_additional_notes(),
            countertops_lf=scenario.get("lower_lf", 0) if scenario.get("lower_damaged") else 0,
            # Customer info
            customer_first_name=claim.policyholder["first_name"],
            customer_last_name=claim.policyholder["last_name"],
            customer_email=claim.policyholder["email"],
            customer_phone=claim.policyholder["phone"],
            # Lower cabinets
            lower_cabinets_damaged=scenario.get("lower_damaged", False),
            lower_cabinets_lf=scenario.get("lower_lf") if scenario.get("lower_damaged") else None,
            num_damaged_lower_boxes=(
                scenario.get("num_damaged_lower") if scenario.get("lower_damaged") else None
            ),
            lower_cabinets_detached=(
                scenario.get("detached", False) if scenario.get("lower_damaged") else None
            ),
            lower_face_frames_doors_drawers_available=(
                self.rng.choice([True, False]) if scenario.get("lower_damaged") else None
            ),
            lower_face_frames_doors_drawers_damaged=(
                self.rng.choice([True, False]) if scenario.get("lower_damaged") else None
            ),
            lower_finished_end_panels_damaged=(
                self.rng.choice([True, False]) if scenario.get("lower_damaged") else None
            ),
            lower_end_panel_damage_present=(
                self.rng.choice([True, False]) if scenario.get("lower_damaged") else None
            ),
            lower_counter_type=(
                scenario.get("counter_type") if scenario.get("lower_damaged") else None
            ),
            # Upper cabinets
            upper_cabinets_damaged=scenario.get("upper_damaged", False),
            upper_cabinets_lf=scenario.get("upper_lf") if scenario.get("upper_damaged") else None,
            num_damaged_upper_boxes=(
                scenario.get("num_damaged_upper") if scenario.get("upper_damaged") else None
            ),
            upper_cabinets_detached=(
                scenario.get("detached", False) if scenario.get("upper_damaged") else None
            ),
            upper_face_frames_doors_drawers_available=(
                self.rng.choice([True, False]) if scenario.get("upper_damaged") else None
            ),
            upper_face_frames_doors_drawers_damaged=(
                self.rng.choice([True, False]) if scenario.get("upper_damaged") else None
            ),
            upper_finished_end_panels_damaged=(
                self.rng.choice([True, False]) if scenario.get("upper_damaged") else None
            ),
            upper_end_panel_damage_present=(
                self.rng.choice([True, False]) if scenario.get("upper_damaged") else None
            ),
            upper_counter_type=(
                scenario.get("counter_type") if scenario.get("upper_damaged") else None
            ),
        )

        # Generate media IDs
        num_overview = self.rng.randint(2, 5)
        form_data.overview_photos = [
            f"media_{self.rng.randint(100000, 999999)}" for _ in range(num_overview)
        ]

        if scenario.get("lower_damaged"):
            num_lower_box = self.rng.randint(1, 3)
            form_data.lower_cabinet_box_photos = [
                f"media_{self.rng.randint(100000, 999999)}" for _ in range(num_lower_box)
            ]

            if form_data.lower_end_panel_damage_present:
                num_lower_panel = self.rng.randint(1, 2)
                form_data.lower_cabinet_end_panel_photos = [
                    f"media_{self.rng.randint(100000, 999999)}" for _ in range(num_lower_panel)
                ]

        if scenario.get("upper_damaged"):
            num_upper_box = self.rng.randint(1, 3)
            form_data.upper_cabinet_box_photos = [
                f"media_{self.rng.randint(100000, 999999)}" for _ in range(num_upper_box)
            ]

            if form_data.upper_end_panel_damage_present:
                num_upper_panel = self.rng.randint(1, 2)
                form_data.upper_cabinet_end_panel_photos = [
                    f"media_{self.rng.randint(100000, 999999)}" for _ in range(num_upper_panel)
                ]

        return form_data

    def _generate_additional_notes(self) -> str:
        """Generate realistic additional notes."""
        notes = [
            "Homeowner present during inspection. Access to all areas granted.",
            "Unable to access basement due to standing water. Follow-up inspection needed.",
            "Contractor estimate provided by homeowner. Appears reasonable.",
            "Some cabinets may be salvageable with refinishing. Further assessment recommended.",
            "Customer requests expedited service due to upcoming family event.",
            "Previous water damage visible in adjacent areas - may be pre-existing.",
            "Cabinet manufacturer confirmed discontinuation of this model. Custom match required.",
        ]
        return self.rng.choice(notes) if self.rng.random() > 0.3 else ""

    def build_claimx_task_details(self, form_data: ItelCabinetFormData, claim) -> Dict[str, Any]:
        """Build a mock ClaimX API response for task details.

        This mimics what the ClaimX API lookup handler would return,
        allowing the enrichment pipeline to process the data.

        Args:
            form_data: Generated itel form data
            claim: GeneratedClaim object

        Returns:
            Dict matching ClaimX API task response structure
        """
        now = datetime.now(timezone.utc)

        # Build form response structure (what ClaimX API returns)
        form_response = {
            "id": form_data.form_response_id,
            "form_id": form_data.form_id,
            "project_id": form_data.project_id,
            "assignment_id": form_data.assignment_id,
            "status": form_data.status,
            "submitted_at": form_data.date_completed if form_data.date_completed else None,
            "created_at": form_data.date_assigned,
            "updated_at": form_data.date_completed or form_data.date_assigned,
            # Form responses - this is the key part that the form parser extracts
            "responses": self._build_form_responses(form_data),
            # Attachments - media files associated with form questions
            "attachments": self._build_form_attachments(form_data),
        }

        # Full task details (what lookup handler stores in claimx_task_details)
        task_details = {
            "id": form_data.assignment_id,
            "task_id": self.ITEL_TASK_ID,
            "task_name": self.ITEL_TASK_NAME,
            "project_id": form_data.project_id,
            "status": form_data.status,
            "assigned_at": form_data.date_assigned,
            "completed_at": form_data.date_completed,
            "assigned_to_user_id": self.rng.randint(1000, 9999),
            "assigned_by_user_id": self.rng.randint(1000, 9999),
            "created_at": form_data.date_assigned,
            "updated_at": form_data.date_completed or form_data.date_assigned,
            # Embedded form response
            "form_response": form_response,
        }

        return task_details

    def _build_form_responses(self, form_data: ItelCabinetFormData) -> List[Dict[str, Any]]:
        """Build form responses array matching ClaimX form structure."""
        responses = []

        # Customer info questions
        if form_data.customer_first_name:
            responses.append(
                {
                    "question_key": "customer_first_name",
                    "question_text": "Customer First Name",
                    "answer": form_data.customer_first_name,
                }
            )

        if form_data.customer_last_name:
            responses.append(
                {
                    "question_key": "customer_last_name",
                    "question_text": "Customer Last Name",
                    "answer": form_data.customer_last_name,
                }
            )

        if form_data.customer_email:
            responses.append(
                {
                    "question_key": "customer_email",
                    "question_text": "Customer Email",
                    "answer": form_data.customer_email,
                }
            )

        if form_data.customer_phone:
            responses.append(
                {
                    "question_key": "customer_phone",
                    "question_text": "Customer Phone",
                    "answer": form_data.customer_phone,
                }
            )

        # Damage description
        if form_data.damage_description:
            responses.append(
                {
                    "question_key": "damage_description",
                    "question_text": "Describe the damage to the cabinets",
                    "answer": form_data.damage_description,
                }
            )

        # Additional notes
        if form_data.additional_notes:
            responses.append(
                {
                    "question_key": "additional_notes",
                    "question_text": "Additional Notes",
                    "answer": form_data.additional_notes,
                }
            )

        # Countertops
        if form_data.countertops_lf is not None:
            responses.append(
                {
                    "question_key": "countertops_lf",
                    "question_text": "Countertops Linear Feet",
                    "answer": str(form_data.countertops_lf),
                }
            )

        # Lower cabinets
        if form_data.lower_cabinets_damaged is not None:
            responses.extend(
                [
                    {
                        "question_key": "lower_cabinets_damaged",
                        "question_text": "Are lower cabinets damaged?",
                        "answer": "Yes" if form_data.lower_cabinets_damaged else "No",
                    }
                ]
            )

            if form_data.lower_cabinets_damaged:
                if form_data.lower_cabinets_lf is not None:
                    responses.append(
                        {
                            "question_key": "lower_cabinets_lf",
                            "question_text": "Lower Cabinets Linear Feet",
                            "answer": str(form_data.lower_cabinets_lf),
                        }
                    )

                if form_data.num_damaged_lower_boxes is not None:
                    responses.append(
                        {
                            "question_key": "num_damaged_lower_boxes",
                            "question_text": "Number of Damaged Lower Cabinet Boxes",
                            "answer": str(form_data.num_damaged_lower_boxes),
                        }
                    )

                if form_data.lower_counter_type:
                    responses.append(
                        {
                            "question_key": "lower_counter_type",
                            "question_text": "Lower Counter Type",
                            "answer": form_data.lower_counter_type,
                        }
                    )

        # Upper cabinets
        if form_data.upper_cabinets_damaged is not None:
            responses.append(
                {
                    "question_key": "upper_cabinets_damaged",
                    "question_text": "Are upper cabinets damaged?",
                    "answer": "Yes" if form_data.upper_cabinets_damaged else "No",
                }
            )

            if form_data.upper_cabinets_damaged:
                if form_data.upper_cabinets_lf is not None:
                    responses.append(
                        {
                            "question_key": "upper_cabinets_lf",
                            "question_text": "Upper Cabinets Linear Feet",
                            "answer": str(form_data.upper_cabinets_lf),
                        }
                    )

                if form_data.num_damaged_upper_boxes is not None:
                    responses.append(
                        {
                            "question_key": "num_damaged_upper_boxes",
                            "question_text": "Number of Damaged Upper Cabinet Boxes",
                            "answer": str(form_data.num_damaged_upper_boxes),
                        }
                    )

                if form_data.upper_counter_type:
                    responses.append(
                        {
                            "question_key": "upper_counter_type",
                            "question_text": "Upper Counter Type",
                            "answer": form_data.upper_counter_type,
                        }
                    )

        return responses

    def _build_form_attachments(self, form_data: ItelCabinetFormData) -> List[Dict[str, Any]]:
        """Build form attachments array matching ClaimX structure."""
        attachments = []

        # Overview photos
        for i, media_id in enumerate(form_data.overview_photos):
            attachments.append(
                {
                    "media_id": media_id,
                    "question_key": "overview_photos",
                    "question_text": "Overview Photos of Kitchen",
                    "file_name": f"overview_{i+1}.jpg",
                    "content_type": "image/jpeg",
                    "file_size": self.rng.randint(500000, 2000000),
                    "download_url": f"https://api.claimx.com/media/{media_id}/download",
                }
            )

        # Lower cabinet photos
        for i, media_id in enumerate(form_data.lower_cabinet_box_photos):
            attachments.append(
                {
                    "media_id": media_id,
                    "question_key": "lower_cabinet_box",
                    "question_text": "Lower Cabinet Box Damage Photos",
                    "file_name": f"lower_box_{i+1}.jpg",
                    "content_type": "image/jpeg",
                    "file_size": self.rng.randint(500000, 2000000),
                    "download_url": f"https://api.claimx.com/media/{media_id}/download",
                }
            )

        for i, media_id in enumerate(form_data.lower_cabinet_end_panel_photos):
            attachments.append(
                {
                    "media_id": media_id,
                    "question_key": "lower_cabinet_end_panels",
                    "question_text": "Lower Cabinet End Panel Photos",
                    "file_name": f"lower_panel_{i+1}.jpg",
                    "content_type": "image/jpeg",
                    "file_size": self.rng.randint(500000, 2000000),
                    "download_url": f"https://api.claimx.com/media/{media_id}/download",
                }
            )

        # Upper cabinet photos
        for i, media_id in enumerate(form_data.upper_cabinet_box_photos):
            attachments.append(
                {
                    "media_id": media_id,
                    "question_key": "upper_cabinet_box",
                    "question_text": "Upper Cabinet Box Damage Photos",
                    "file_name": f"upper_box_{i+1}.jpg",
                    "content_type": "image/jpeg",
                    "file_size": self.rng.randint(500000, 2000000),
                    "download_url": f"https://api.claimx.com/media/{media_id}/download",
                }
            )

        for i, media_id in enumerate(form_data.upper_cabinet_end_panel_photos):
            attachments.append(
                {
                    "media_id": media_id,
                    "question_key": "upper_cabinet_end_panels",
                    "question_text": "Upper Cabinet End Panel Photos",
                    "file_name": f"upper_panel_{i+1}.jpg",
                    "content_type": "image/jpeg",
                    "file_size": self.rng.randint(500000, 2000000),
                    "download_url": f"https://api.claimx.com/media/{media_id}/download",
                }
            )

        return attachments
