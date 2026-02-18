"""
Form parsing for iTel Cabinet Repair forms.

Extracts structured data from ClaimX task responses using the DataBuilder pattern.
Integrated from parse.py with improved column mapping and value extraction.
"""

import json
import logging
import re
import types
from collections import defaultdict
from dataclasses import dataclass, field, fields, is_dataclass
from datetime import UTC, datetime
from typing import Union
from urllib.parse import parse_qs, urlparse

from .models import CabinetAttachment, CabinetSubmission

logger = logging.getLogger(__name__)


# ==========================================
# UTILITY FUNCTIONS
# ==========================================


def camel_to_snake(name: str) -> str:
    """Convert camelCase to snake_case."""
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


def parse_date(date_str: str) -> datetime | None:
    """Parse ISO format date string to datetime."""
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except ValueError:
        return None


def _convert_field_value(field_type, value):
    """Convert a single field value to its target type, handling lists and optionals."""
    origin = getattr(field_type, "__origin__", None)
    args = getattr(field_type, "__args__", [])
    if origin is list:
        item_type = args[0]
        return [from_dict(item_type, item) for item in value]
    if (origin is Union or isinstance(field_type, types.UnionType)) and type(None) in args:
        non_none_type = next(t for t in args if t is not type(None))
        return from_dict(non_none_type, value)
    return from_dict(field_type, value)


def from_dict(data_class, data):
    """
    Recursively convert dict to dataclass instance.
    Handles camelCase to snake_case conversion.
    """
    if data is None:
        return None
    if data_class is datetime:
        return parse_date(data)
    if not is_dataclass(data_class):
        return data

    field_types = {f.name: f.type for f in fields(data_class)}
    kwargs = {}
    for key, value in data.items():
        snake_key = camel_to_snake(key)
        if snake_key in field_types:
            kwargs[snake_key] = _convert_field_value(field_types[snake_key], value)
    return data_class(**kwargs)


class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder that handles datetime objects."""

    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)


def parse_url_expiration(url: str) -> dict:
    """
    Extract TTL info from ClaimX signed URL.

    ClaimX URLs include expiration parameters:
        ?systemDate=<ms_timestamp>&expires=<duration_ms>&sign=...

    Args:
        url: ClaimX download URL

    Returns:
        {"expires_at": ISO timestamp, "ttl_seconds": int} or {} if unparseable
    """
    if not url:
        return {}
    try:
        params = parse_qs(urlparse(url).query)
        system_date_list = params.get("systemDate", [])
        expires_list = params.get("expires", [])

        if not system_date_list or not expires_list:
            return {}

        system_date = int(system_date_list[0])
        expires_ms = int(expires_list[0])

        if system_date and expires_ms:
            expires_at_ms = system_date + expires_ms
            expires_at = datetime.fromtimestamp(expires_at_ms / 1000, tz=UTC)
            return {
                "expires_at": expires_at.isoformat(),
                "ttl_seconds": expires_ms // 1000,
            }
    except (ValueError, IndexError, TypeError) as e:
        logger.debug("Could not parse URL expiration: %s", e)
    return {}


# ==========================================
# SCHEMA CLASSES (from parse.py)
# ==========================================


@dataclass
class OptionAnswer:
    name: str


@dataclass
class NumberAnswer:
    value: float


@dataclass
class ResponseAnswerExport:
    type: str
    text: str | None = None
    option_answer: OptionAnswer | None = None
    number_answer: NumberAnswer | None = None
    claim_media_ids: list[int] | None = None


@dataclass
class FormControl:
    id: str


@dataclass
class QuestionAndAnswer:
    question_text: str
    component: str
    response_answer_export: ResponseAnswerExport
    form_control: FormControl


@dataclass
class Group:
    name: str
    group_id: str
    question_and_answers: list[QuestionAndAnswer] = field(default_factory=list)


@dataclass
class ResponseData:
    groups: list[Group] = field(default_factory=list)


@dataclass
class ExternalLinkData:
    url: str
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
    phone: str | None = None


@dataclass
class ApiResponse:
    assignment_id: int
    task_id: int
    task_name: str
    project_id: int
    form_id: str
    status: str
    assignor_email: str | None = None
    form_response_id: str | None = None
    date_assigned: datetime | None = None
    date_completed: datetime | None = None
    external_link_data: ExternalLinkData | None = None
    response: ResponseData | None = None


# ==========================================
# COLUMN MAPPING CONFIGURATION
# ==========================================

# Maps (group_name, question_text) tuples to database column names
# Keys are stripped of trailing spaces to ensure matching works
COLUMN_MAP = {
    # Measurements
    ("Linear Feet Capture", "Countertops (linear feet)"): "countertops_lf",
    ("Linear Feet Capture", "Lower Cabinets (linear feet)"): "lower_cabinets_lf",
    ("Linear Feet Capture", "Upper Cabinets (linear feet)"): "upper_cabinets_lf",
    (
        "Linear Feet Capture",
        "Full-Height Cabinets (linear feet)",
    ): "full_height_cabinets_lf",
    ("Linear Feet Capture", "Island Cabinets (linear feet)"): "island_cabinets_lf",
    # Lower Cabinets
    ("Cabinet Types Damaged", "Lower Cabinets Damaged?"): "lower_cabinets_damaged",
    (
        "Lower Cabinets",
        "Enter Number of Damaged Lower Boxes",
    ): "num_damaged_lower_boxes",
    ("Lower Cabinets", "Are The Lower Cabinets Detached?"): "lower_cabinets_detached",
    (
        "Lower Cabinets",
        "Are All The Face Frames, Doors, And Drawers Available?",
    ): "lower_face_frames_doors_drawers_available",
    (
        "Lower Cabinets",
        "Are All The Face Frames, Doors, And Drawer Fronts Damaged?",
    ): "lower_face_frames_doors_drawers_damaged",
    (
        "Lower Cabinets",
        "Are The Lower Finished End Panels Damaged?",
    ): "lower_finished_end_panels_damaged",
    (
        "Capture Lower End Panel",
        "Is there lower cabinet end panel damage?",
    ): "lower_end_panel_damage_present",
    (
        "Lower Cabinet Counter Type",
        "Select Lower Cabinet Counter Type",
    ): "lower_counter_type",
    # Upper Cabinets
    ("Cabinet Types Damaged", "Upper Cabinets Damaged?"): "upper_cabinets_damaged",
    (
        "Upper Cabinets",
        "Enter Number of Damaged Upper Boxes",
    ): "num_damaged_upper_boxes",
    ("Upper Cabinets", "Are The Upper Cabinets Detached?"): "upper_cabinets_detached",
    (
        "Upper Cabinets",
        "Are All The Face Frames, Doors, And Drawers Available?",
    ): "upper_face_frames_doors_drawers_available",
    (
        "Upper Cabinets",
        "Are All The Face Frames, Doors, And Drawer Fronts Damaged?",
    ): "upper_face_frames_doors_drawers_damaged",
    (
        "Upper Cabinets",
        "Are The Upper Finished End Panels Damaged?",
    ): "upper_finished_end_panels_damaged",
    (
        "Capture Upper End Panel",
        "Is there upper cabinet end panel damage?",
    ): "upper_end_panel_damage_present",
    # Full Height
    (
        "Cabinet Types Damaged",
        "Full Height Cabinets Damaged?",
    ): "full_height_cabinets_damaged",
    (
        "Full Height/Pantry Cabinets",
        "Enter Number of Damaged Full Height Boxes",
    ): "num_damaged_full_height_boxes",
    (
        "Full Height/Pantry Cabinets",
        "Are The Full Height Cabinets Detached?",
    ): "full_height_cabinets_detached",
    (
        "Full Height/Pantry Cabinets",
        "Are All The Face Frames, Doors, And Drawers Available?",
    ): "full_height_face_frames_doors_drawers_available",
    (
        "Full Height/Pantry Cabinets",
        "Are All The Face Frames, Doors, And Drawer Fronts Damaged?",
    ): "full_height_face_frames_doors_drawers_damaged",
    (
        "Full Height/Pantry Cabinets",
        "Are the Full Height Finished End Panels Damaged?",
    ): "full_height_finished_end_panels_damaged",
    # Island
    ("Cabinet Types Damaged", "Island Cabinets Damaged?"): "island_cabinets_damaged",
    (
        "Island Cabinets",
        "Enter Number of Damaged Island Boxes",
    ): "num_damaged_island_boxes",
    (
        "Island Cabinets",
        "Are The Island Cabinets Detached?",
    ): "island_cabinets_detached",
    (
        "Island Cabinets",
        "Are All The Face Frames, Doors, And Drawers Available?",
    ): "island_face_frames_doors_drawers_available",
    (
        "Island Cabinets",
        "Are All The Face Frames, Doors, And Drawer Fronts Damaged?",
    ): "island_face_frames_doors_drawers_damaged",
    (
        "Island Cabinets",
        "Are the Island Finished End Panels Damaged?",
    ): "island_finished_end_panels_damaged",
    (
        "Capture Island End Panel",
        "Is there island cabinet end panel damage?",
    ): "island_end_panel_damage_present",
    (
        "Island Cabinet Counter Type",
        "Select Island Cabinet Counter Type",
    ): "island_counter_type",
    # General
    ("Damage Description", "Enter Damaged Description"): "damage_description",
    (
        "Other Details and Information",
        "Now that you have been through the process, please provide any other details that you think would be relevant to the cabinet damage",
    ): "additional_notes",
}


# ==========================================
# DATA BUILDER
# ==========================================


class DataBuilder:
    """
    Core parsing logic for transforming ClaimX API response into structured data.
    Produces three outputs: form_row (Delta), attachments_rows (Delta), readable_report (API).
    """

    @staticmethod
    def _extract_raw_value(export_data: ResponseAnswerExport):
        """Extract the raw value from a response answer by type."""
        if export_data.type == "text":
            return export_data.text
        if export_data.type == "number":
            return export_data.number_answer.value if export_data.number_answer else None
        if export_data.type == "option":
            return export_data.option_answer.name if export_data.option_answer else None
        if export_data.type == "image":
            return export_data.claim_media_ids if export_data.claim_media_ids else []
        return None

    @staticmethod
    def _normalize_boolean(val: str):
        """Normalize Yes/No string variants to True/False, or return None if not boolean."""
        clean = val.lower().strip()
        if clean == "yes" or clean.startswith("yes,"):
            return True
        if clean == "no" or clean.startswith("there is no"):
            return False
        return None

    @staticmethod
    def extract_value(export_data: ResponseAnswerExport):
        """
        Extracts and normalizes value from response answer.
        Handles Yes/No -> True/False conversion for boolean fields.
        """
        if not export_data:
            return None
        val = DataBuilder._extract_raw_value(export_data)
        if isinstance(val, str):
            normalized = DataBuilder._normalize_boolean(val)
            if normalized is not None:
                return normalized
        return val

    _TOPIC_KEYWORDS = [
        ("island", "Island Cabinets"),
        ("lower", "Lower Cabinets"),
        ("upper", "Upper Cabinets"),
        ("full height", "Full Height / Pantry"),
        ("pantry", "Full Height / Pantry"),
        ("countertop", "Countertops"),
    ]

    @staticmethod
    def get_topic_category(group_name: str, question_text: str) -> str:
        """Determine topic category for organizing attachments and readable reports."""
        text = (group_name + " " + question_text).lower()
        for keyword, category in DataBuilder._TOPIC_KEYWORDS:
            if keyword in text:
                return category
        return "General"

    @staticmethod
    def _extract_external_link_fields(ext_data: ExternalLinkData | None) -> dict:
        """Extract external link data fields, returning None for each if absent."""
        if not ext_data:
            return {
                "external_link_url": None,
                "customer_first_name": None,
                "customer_last_name": None,
                "customer_email": None,
                "customer_phone": None,
            }
        return {
            "external_link_url": ext_data.url,
            "customer_first_name": ext_data.first_name,
            "customer_last_name": ext_data.last_name,
            "customer_email": ext_data.email,
            "customer_phone": ext_data.phone,
        }

    @staticmethod
    def _process_response_groups(groups: list[Group]) -> tuple[dict, dict]:
        """Process response groups, returning (column_values, raw_data_flat)."""
        column_values = {}
        raw_data_flat = defaultdict(list)
        for group in groups:
            clean_group_name = group.name.strip()
            for qa in group.question_and_answers:
                clean_question_text = qa.question_text.strip()
                answer_val = DataBuilder.extract_value(qa.response_answer_export)
                map_key = (clean_group_name, clean_question_text)
                if map_key in COLUMN_MAP:
                    column_values[COLUMN_MAP[map_key]] = answer_val
                raw_data_flat[clean_group_name].append(
                    {"q": clean_question_text, "a": answer_val, "id": qa.form_control.id}
                )
        return column_values, dict(raw_data_flat)

    @staticmethod
    def build_form_row(api_obj: ApiResponse, event_id: str) -> dict:
        """Build form row for database insertion."""
        if not api_obj:
            return {}

        now = datetime.now(UTC)
        form_row = {
            "assignment_id": api_obj.assignment_id,
            "task_id": api_obj.task_id,
            "task_name": api_obj.task_name,
            "project_id": str(api_obj.project_id),
            "form_id": api_obj.form_id,
            "form_response_id": api_obj.form_response_id,
            "status": api_obj.status,
            "event_id": event_id,
            "date_assigned": api_obj.date_assigned,
            "date_completed": api_obj.date_completed,
            "ingested_at": now,
            "assignor_email": api_obj.assignor_email,
            **DataBuilder._extract_external_link_fields(api_obj.external_link_data),
            **dict.fromkeys(COLUMN_MAP.values()),
        }

        groups = api_obj.response.groups if api_obj.response and api_obj.response.groups else []
        column_values, raw_data_flat = DataBuilder._process_response_groups(groups)
        form_row.update(column_values)
        form_row["raw_data"] = json.dumps(raw_data_flat, cls=DateTimeEncoder)
        return form_row

    @staticmethod
    def _build_attachment_rows(
        api_obj: ApiResponse,
        qa: QuestionAndAnswer,
        media_ids: list,
        topic: str,
        event_id: str,
        now: datetime,
        media_url_map: dict[int, str],
    ) -> list[dict]:
        """Build attachment row dicts for a single image question."""
        clean_question_text = qa.question_text.strip()
        return [
            {
                "assignment_id": api_obj.assignment_id,
                "project_id": api_obj.project_id,
                "event_id": event_id,
                "control_id": qa.form_control.id,
                "question_key": _get_question_key(clean_question_text),
                "question_text": clean_question_text,
                "topic_category": topic,
                "media_id": media_id,
                "display_order": idx + 1,
                "created_at": now,
                "is_active": True,
                "media_type": "image/jpeg",
                "url": media_url_map.get(media_id),
            }
            for idx, media_id in enumerate(media_ids)
        ]

    @staticmethod
    def extract_attachments(
        api_obj: ApiResponse, event_id: str, media_url_map: dict[int, str] = None
    ) -> list[dict]:
        """Extract attachment rows from API response."""
        if media_url_map is None:
            media_url_map = {}
        if not api_obj:
            return []

        now = datetime.now(UTC)
        attachments_rows = []
        groups = api_obj.response.groups if api_obj.response and api_obj.response.groups else []

        for group in groups:
            clean_group_name = group.name.strip()
            for qa in group.question_and_answers:
                answer_val = DataBuilder.extract_value(qa.response_answer_export)
                if qa.response_answer_export.type != "image" or not isinstance(answer_val, list):
                    continue
                topic = DataBuilder.get_topic_category(clean_group_name, qa.question_text.strip())
                attachments_rows.extend(
                    DataBuilder._build_attachment_rows(
                        api_obj, qa, answer_val, topic, event_id, now, media_url_map
                    )
                )

        return attachments_rows

    @staticmethod
    def _build_report_meta(api_obj: ApiResponse, media_url_map: dict[int, str]) -> dict:
        """Build the meta section for a readable report."""
        meta = {
            "task_id": api_obj.task_id,
            "assignment_id": api_obj.assignment_id,
            "project_id": str(api_obj.project_id),
            "status": api_obj.status,
            "dates": {
                "assigned": api_obj.date_assigned.isoformat() if api_obj.date_assigned else None,
                "completed": (
                    api_obj.date_completed.isoformat() if api_obj.date_completed else None
                ),
            },
        }
        if media_url_map:
            first_url = next(iter(media_url_map.values()), None)
            url_expiration = parse_url_expiration(first_url)
            if url_expiration:
                meta["media_urls_expire_at"] = url_expiration["expires_at"]
                meta["media_urls_ttl_seconds"] = url_expiration["ttl_seconds"]
        return meta

    @staticmethod
    def _enrich_answer(answer_val, export_type: str, media_url_map: dict[int, str]):
        """Enrich image answers with URLs, pass through others."""
        if export_type == "image" and isinstance(answer_val, list):
            return [
                {"media_id": media_id, "url": media_url_map.get(media_id)}
                for media_id in answer_val
            ]
        return answer_val

    @staticmethod
    def build_readable_report(
        api_obj: ApiResponse, _event_id: str, media_url_map: dict[int, str] = None
    ) -> dict:
        """Build readable report with categorized topics."""
        if media_url_map is None:
            media_url_map = {}
        if not api_obj:
            return {}

        topics = defaultdict(list)
        groups = api_obj.response.groups if api_obj.response and api_obj.response.groups else []

        for group in groups:
            clean_group_name = group.name.strip()
            for qa in group.question_and_answers:
                clean_question_text = qa.question_text.strip()
                answer_val = DataBuilder.extract_value(qa.response_answer_export)
                topic = DataBuilder.get_topic_category(clean_group_name, clean_question_text)
                topics[topic].append({
                    "question": clean_question_text,
                    "answer": DataBuilder._enrich_answer(
                        answer_val, qa.response_answer_export.type, media_url_map
                    ),
                    "type": qa.response_answer_export.type,
                    "control_id": qa.form_control.id,
                })

        return {
            "meta": DataBuilder._build_report_meta(api_obj, media_url_map),
            "topics": dict(topics),
        }


# ==========================================
# HELPER FUNCTIONS
# ==========================================


def _get_question_key(question_text: str) -> str:
    """
    Generate question_key from question text.
    Maps known media questions to consistent keys.
    """
    # Known media question mappings
    media_mapping = {
        "Upload Overview Photo(s)": "overview_photos",
        "Captured Lower Cabinet Box": "lower_cabinet_box",
        "Captured Lower Face Frames, Doors, and Drawers": "lower_face_frames_doors_drawers",
        "Captured Lower Cabinet End Panels": "lower_cabinet_end_panels",
        "Captured Upper Cabinet Box": "upper_cabinet_box",
        "Captured Upper Face Frames, Doors, and Drawers": "upper_face_frames_doors_drawers",
        "Captured Upper Cabinet End Panels": "upper_cabinet_end_panels",
        "Captured Full Height/Pantry Cabinets": "full_height_cabinet_box",
        "Captured Full Height/Pantry Face Frames, Doors, and Drawers": "full_height_face_frames_doors_drawers",
        "Captured Full Height/Pantry End Panels": "full_height_end_panels",
        "Captured Island Cabinet Box": "island_cabinet_box",
        "Captured Island Face Frames, Doors, and Drawers": "island_face_frames_doors_drawers",
        "Captured Island Cabinet End Panels": "island_cabinet_end_panels",
    }

    if question_text in media_mapping:
        return media_mapping[question_text]

    return question_text.lower().replace(" ", "_").replace("/", "_").replace("?", "")


# ==========================================
# PUBLIC API FUNCTIONS
# ==========================================


def parse_cabinet_form(task_data: dict, event_id: str) -> CabinetSubmission:
    """
    Parse ClaimX task response into CabinetSubmission.

    Args:
        task_data: Full task data from ClaimX API
        event_id: Event ID for traceability

    Returns:
        Parsed submission data

    Raises:
        KeyError: If required fields are missing
        ValueError: If data is malformed
    """
    logger.debug("Parsing cabinet form for assignment_id=%s", task_data.get("assignmentId"))

    api_obj = from_dict(ApiResponse, task_data)
    form_row = DataBuilder.build_form_row(api_obj, event_id)

    return CabinetSubmission(
        # Primary identifiers
        assignment_id=form_row["assignment_id"],
        project_id=form_row["project_id"],
        form_id=form_row["form_id"],
        form_response_id=form_row["form_response_id"],
        status=form_row["status"],
        event_id=form_row["event_id"],
        # Task metadata
        task_id=form_row.get("task_id"),
        task_name=form_row.get("task_name"),
        # Dates
        date_assigned=form_row.get("date_assigned"),
        date_completed=form_row.get("date_completed"),
        ingested_at=form_row.get("ingested_at"),
        # Customer information
        customer_first_name=form_row.get("customer_first_name"),
        customer_last_name=form_row.get("customer_last_name"),
        customer_email=form_row.get("customer_email"),
        customer_phone=form_row.get("customer_phone"),
        assignor_email=form_row.get("assignor_email"),
        external_link_url=form_row.get("external_link_url"),
        # General damage information
        damage_description=form_row.get("damage_description"),
        additional_notes=form_row.get("additional_notes"),
        countertops_lf=form_row.get("countertops_lf"),
        # Raw data
        raw_data=form_row.get("raw_data"),
        # Lower cabinets
        lower_cabinets_damaged=form_row.get("lower_cabinets_damaged"),
        lower_cabinets_lf=form_row.get("lower_cabinets_lf"),
        num_damaged_lower_boxes=form_row.get("num_damaged_lower_boxes"),
        lower_cabinets_detached=form_row.get("lower_cabinets_detached"),
        lower_face_frames_doors_drawers_available=form_row.get(
            "lower_face_frames_doors_drawers_available"
        ),
        lower_face_frames_doors_drawers_damaged=form_row.get(
            "lower_face_frames_doors_drawers_damaged"
        ),
        lower_finished_end_panels_damaged=form_row.get("lower_finished_end_panels_damaged"),
        lower_end_panel_damage_present=form_row.get("lower_end_panel_damage_present"),
        lower_counter_type=form_row.get("lower_counter_type"),
        # Upper cabinets
        upper_cabinets_damaged=form_row.get("upper_cabinets_damaged"),
        upper_cabinets_lf=form_row.get("upper_cabinets_lf"),
        num_damaged_upper_boxes=form_row.get("num_damaged_upper_boxes"),
        upper_cabinets_detached=form_row.get("upper_cabinets_detached"),
        upper_face_frames_doors_drawers_available=form_row.get(
            "upper_face_frames_doors_drawers_available"
        ),
        upper_face_frames_doors_drawers_damaged=form_row.get(
            "upper_face_frames_doors_drawers_damaged"
        ),
        upper_finished_end_panels_damaged=form_row.get("upper_finished_end_panels_damaged"),
        upper_end_panel_damage_present=form_row.get("upper_end_panel_damage_present"),
        # Full height cabinets
        full_height_cabinets_damaged=form_row.get("full_height_cabinets_damaged"),
        full_height_cabinets_lf=form_row.get("full_height_cabinets_lf"),
        num_damaged_full_height_boxes=form_row.get("num_damaged_full_height_boxes"),
        full_height_cabinets_detached=form_row.get("full_height_cabinets_detached"),
        full_height_face_frames_doors_drawers_available=form_row.get(
            "full_height_face_frames_doors_drawers_available"
        ),
        full_height_face_frames_doors_drawers_damaged=form_row.get(
            "full_height_face_frames_doors_drawers_damaged"
        ),
        full_height_finished_end_panels_damaged=form_row.get(
            "full_height_finished_end_panels_damaged"
        ),
        # Island cabinets
        island_cabinets_damaged=form_row.get("island_cabinets_damaged"),
        island_cabinets_lf=form_row.get("island_cabinets_lf"),
        num_damaged_island_boxes=form_row.get("num_damaged_island_boxes"),
        island_cabinets_detached=form_row.get("island_cabinets_detached"),
        island_face_frames_doors_drawers_available=form_row.get(
            "island_face_frames_doors_drawers_available"
        ),
        island_face_frames_doors_drawers_damaged=form_row.get(
            "island_face_frames_doors_drawers_damaged"
        ),
        island_finished_end_panels_damaged=form_row.get("island_finished_end_panels_damaged"),
        island_end_panel_damage_present=form_row.get("island_end_panel_damage_present"),
        island_counter_type=form_row.get("island_counter_type"),
        # Metadata - use existing timestamps from form_row
        created_at=form_row.get("ingested_at"),
        updated_at=form_row.get("ingested_at"),
    )


def parse_cabinet_attachments(
    task_data: dict,
    assignment_id: int,
    _project_id: int,
    event_id: str,
    media_url_map: dict[int, str] = None,
) -> list[CabinetAttachment]:
    """
    Extract media attachments from ClaimX task response.

    Args:
        task_data: Full task data from ClaimX API
        assignment_id: Assignment ID
        _project_id: ClaimX project ID (unused, kept for interface consistency)
        event_id: Event ID for traceability
        media_url_map: Optional mapping of media_id to download URL

    Returns:
        List of attachment records with URLs enriched
    """
    logger.debug("Parsing attachments for assignment_id=%s", assignment_id)

    api_obj = from_dict(ApiResponse, task_data)
    attachments_rows = DataBuilder.extract_attachments(api_obj, event_id, media_url_map)

    attachments = []
    for att_row in attachments_rows:
        attachments.append(
            CabinetAttachment(
                assignment_id=att_row["assignment_id"],
                project_id=att_row["project_id"],
                event_id=att_row["event_id"],
                control_id=att_row["control_id"],
                question_key=att_row["question_key"],
                question_text=att_row["question_text"],
                topic_category=att_row["topic_category"],
                media_id=att_row["media_id"],
                url=att_row["url"],  # Download URL from ClaimX
                display_order=att_row["display_order"],
                created_at=att_row["created_at"],
                is_active=att_row["is_active"],
                media_type=att_row["media_type"],
            )
        )

    logger.debug("Parsed %s attachments", len(attachments))
    return attachments


def get_readable_report(
    task_data: dict, event_id: str, media_url_map: dict[int, str] = None
) -> dict:
    """
    Generate readable report for API consumption.

    This output format organizes form data by topic for easier consumption
    by the iTel API worker. Image answers are enriched with download URLs.

    Args:
        task_data: Full task data from ClaimX API
        event_id: Event ID for traceability
        media_url_map: Optional mapping of media_id to download URL

    Returns:
        Readable report dict with topics organized by category and media URLs enriched
    """
    logger.debug("Generating readable report for assignment_id=%s", task_data.get("assignmentId"))

    # Convert to typed dataclass using from_dict
    api_obj = from_dict(ApiResponse, task_data)

    return DataBuilder.build_readable_report(api_obj, event_id, media_url_map)
