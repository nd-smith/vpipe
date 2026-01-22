# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
# 
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""
Form parsing for iTel Cabinet Repair forms.

Extracts structured data from ClaimX task responses using the DataBuilder pattern.
Integrated from parse.py with improved column mapping and value extraction.
"""

import json
import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field, fields, is_dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union
from urllib.parse import parse_qs, urlparse

from .models import CabinetSubmission, CabinetAttachment

logger = logging.getLogger(__name__)


# ==========================================
# UTILITY FUNCTIONS
# ==========================================

def camel_to_snake(name: str) -> str:
    """Convert camelCase to snake_case."""
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


def parse_date(date_str: str) -> Optional[datetime]:
    """Parse ISO format date string to datetime."""
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    except ValueError:
        return None


def from_dict(data_class, data):
    """
    Recursively convert dict to dataclass instance.
    Handles camelCase to snake_case conversion.
    """
    if data is None:
        return None
    if is_dataclass(data_class):
        field_types = {f.name: f.type for f in fields(data_class)}
        kwargs = {}
        for key, value in data.items():
            snake_key = camel_to_snake(key)
            if snake_key in field_types:
                field_type = field_types[snake_key]
                origin = getattr(field_type, '__origin__', None)
                args = getattr(field_type, '__args__', [])
                if origin is list:
                    item_type = args[0]
                    kwargs[snake_key] = [from_dict(item_type, item) for item in value]
                elif origin is Union and type(None) in args:
                    non_none_type = next(t for t in args if t is not type(None))
                    kwargs[snake_key] = from_dict(non_none_type, value)
                else:
                    kwargs[snake_key] = from_dict(field_type, value)
        return data_class(**kwargs)
    elif data_class is datetime:
        return parse_date(data)
    return data


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
        system_date_list = params.get('systemDate', [])
        expires_list = params.get('expires', [])

        if not system_date_list or not expires_list:
            return {}

        system_date = int(system_date_list[0])
        expires_ms = int(expires_list[0])

        if system_date and expires_ms:
            expires_at_ms = system_date + expires_ms
            expires_at = datetime.fromtimestamp(expires_at_ms / 1000, tz=timezone.utc)
            return {
                "expires_at": expires_at.isoformat(),
                "ttl_seconds": expires_ms // 1000
            }
    except (ValueError, IndexError, TypeError) as e:
        logger.debug(f"Could not parse URL expiration: {e}")
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
    text: Optional[str] = None
    option_answer: Optional[OptionAnswer] = None
    number_answer: Optional[NumberAnswer] = None
    claim_media_ids: Optional[List[int]] = None


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
    question_and_answers: List[QuestionAndAnswer] = field(default_factory=list)


@dataclass
class ResponseData:
    groups: List[Group] = field(default_factory=list)


@dataclass
class ExternalLinkData:
    url: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None


@dataclass
class ApiResponse:
    assignment_id: int
    task_id: int
    task_name: str
    project_id: int
    form_id: str
    status: str
    assignor_email: Optional[str] = None
    form_response_id: Optional[str] = None
    date_assigned: Optional[datetime] = None
    date_completed: Optional[datetime] = None
    external_link_data: Optional[ExternalLinkData] = None
    response: Optional[ResponseData] = None


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
    ("Linear Feet Capture", "Full-Height Cabinets (linear feet)"): "full_height_cabinets_lf",
    ("Linear Feet Capture", "Island Cabinets (linear feet)"): "island_cabinets_lf",

    # Lower Cabinets
    ("Cabinet Types Damaged", "Lower Cabinets Damaged?"): "lower_cabinets_damaged",
    ("Lower Cabinets", "Enter Number of Damaged Lower Boxes"): "num_damaged_lower_boxes",
    ("Lower Cabinets", "Are The Lower Cabinets Detached?"): "lower_cabinets_detached",
    ("Lower Cabinets", "Are All The Face Frames, Doors, And Drawers Available?"): "lower_face_frames_doors_drawers_available",
    ("Lower Cabinets", "Are All The Face Frames, Doors, And Drawer Fronts Damaged?"): "lower_face_frames_doors_drawers_damaged",
    ("Lower Cabinets", "Are The Lower Finished End Panels Damaged?"): "lower_finished_end_panels_damaged",
    ("Capture Lower End Panel", "Is there lower cabinet end panel damage?"): "lower_end_panel_damage_present",
    ("Lower Cabinet Counter Type", "Select Lower Cabinet Counter Type"): "lower_counter_type",

    # Upper Cabinets
    ("Cabinet Types Damaged", "Upper Cabinets Damaged?"): "upper_cabinets_damaged",
    ("Upper Cabinets", "Enter Number of Damaged Upper Boxes"): "num_damaged_upper_boxes",
    ("Upper Cabinets", "Are The Upper Cabinets Detached?"): "upper_cabinets_detached",
    ("Upper Cabinets", "Are All The Face Frames, Doors, And Drawers Available?"): "upper_face_frames_doors_drawers_available",
    ("Upper Cabinets", "Are All The Face Frames, Doors, And Drawer Fronts Damaged?"): "upper_face_frames_doors_drawers_damaged",
    ("Upper Cabinets", "Are The Upper Finished End Panels Damaged?"): "upper_finished_end_panels_damaged",
    ("Capture Upper End Panel", "Is there upper cabinet end panel damage?"): "upper_end_panel_damage_present",

    # Full Height
    ("Cabinet Types Damaged", "Full Height Cabinets Damaged?"): "full_height_cabinets_damaged",
    ("Full Height/Pantry Cabinets", "Enter Number of Damaged Full Height Boxes"): "num_damaged_full_height_boxes",
    ("Full Height/Pantry Cabinets", "Are The Full Height Cabinets Detached?"): "full_height_cabinets_detached",
    ("Full Height/Pantry Cabinets", "Are All The Face Frames, Doors, And Drawers Available?"): "full_height_face_frames_doors_drawers_available",
    ("Full Height/Pantry Cabinets", "Are All The Face Frames, Doors, And Drawer Fronts Damaged?"): "full_height_face_frames_doors_drawers_damaged",
    ("Full Height/Pantry Cabinets", "Are the Full Height Finished End Panels Damaged?"): "full_height_finished_end_panels_damaged",

    # Island
    ("Cabinet Types Damaged", "Island Cabinets Damaged?"): "island_cabinets_damaged",
    ("Island Cabinets", "Enter Number of Damaged Island Boxes"): "num_damaged_island_boxes",
    ("Island Cabinets", "Are The Island Cabinets Detached?"): "island_cabinets_detached",
    ("Island Cabinets", "Are All The Face Frames, Doors, And Drawers Available?"): "island_face_frames_doors_drawers_available",
    ("Island Cabinets", "Are All The Face Frames, Doors, And Drawer Fronts Damaged?"): "island_face_frames_doors_drawers_damaged",
    ("Island Cabinets", "Are the Island Finished End Panels Damaged?"): "island_finished_end_panels_damaged",
    ("Capture Island End Panel", "Is there island cabinet end panel damage?"): "island_end_panel_damage_present",
    ("Island Cabinet Counter Type", "Select Island Cabinet Counter Type"): "island_counter_type",

    # General
    ("Damage Description", "Enter Damaged Description"): "damage_description",
    ("Other Details and Information", "Now that you have been through the process, please provide any other details that you think would be relevant to the cabinet damage"): "additional_notes"
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
    def extract_value(export_data: ResponseAnswerExport):
        """
        Extracts and normalizes value from response answer.
        Handles Yes/No -> True/False conversion for boolean fields.
        """
        if not export_data:
            return None

        val = None
        if export_data.type == 'text':
            val = export_data.text
        elif export_data.type == 'number':
            val = export_data.number_answer.value if export_data.number_answer else None
        elif export_data.type == 'option':
            val = export_data.option_answer.name if export_data.option_answer else None
        elif export_data.type == 'image':
            return export_data.claim_media_ids if export_data.claim_media_ids else []

        # Normalize Boolean
        if isinstance(val, str):
            clean_val = val.lower().strip()
            if clean_val == "yes":
                return True
            if clean_val == "no":
                return False
            if clean_val.startswith("yes,"):
                return True
            if clean_val.startswith("there is no"):
                return False

        return val

    @staticmethod
    def get_topic_category(group_name: str, question_text: str) -> str:
        """Determine topic category for organizing attachments and readable reports."""
        text = (group_name + " " + question_text).lower()
        if 'island' in text:
            return 'Island Cabinets'
        if 'lower' in text:
            return 'Lower Cabinets'
        if 'upper' in text:
            return 'Upper Cabinets'
        if 'full height' in text or 'pantry' in text:
            return 'Full Height / Pantry'
        if 'countertop' in text:
            return 'Countertops'
        return 'General'

    @staticmethod
    def process(api_obj: ApiResponse, event_id: str, media_url_map: dict[int, str] = None) -> tuple[dict, list[dict], dict]:
        """
        Main processing method - transforms API response into three outputs.

        Args:
            api_obj: Parsed API response dataclass
            event_id: Event ID for traceability
            media_url_map: Optional mapping of media_id to download URL

        Returns:
            Tuple of (form_row, attachments_rows, readable_report)
        """
        if media_url_map is None:
            media_url_map = {}
        if not api_obj:
            return {}, [], {}

        now = datetime.utcnow()

        # --- A. Setup Base Objects ---

        # 1. DB Row for forms table
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
            "external_link_url": api_obj.external_link_data.url if api_obj.external_link_data else None,
            "customer_first_name": api_obj.external_link_data.first_name if api_obj.external_link_data else None,
            "customer_last_name": api_obj.external_link_data.last_name if api_obj.external_link_data else None,
            "customer_email": api_obj.external_link_data.email if api_obj.external_link_data else None,
            "customer_phone": api_obj.external_link_data.phone if api_obj.external_link_data else None,
            **{v: None for v in COLUMN_MAP.values()}  # Init mapped cols with None
        }

        attachments_rows = []

        # 2. Readable/Raw Data Accumulators
        readable_report = {
            "meta": {
                "task_id": api_obj.task_id,
                "assignment_id": api_obj.assignment_id,
                "project_id": str(api_obj.project_id),
                "status": api_obj.status,
                "dates": {
                    "assigned": api_obj.date_assigned.isoformat() if api_obj.date_assigned else None,
                    "completed": api_obj.date_completed.isoformat() if api_obj.date_completed else None
                }
            },
            "topics": defaultdict(list)
        }

        # Extract media URL expiration from first available URL
        if media_url_map:
            first_url = next(iter(media_url_map.values()), None)
            url_expiration = parse_url_expiration(first_url)
            if url_expiration:
                readable_report["meta"]["media_urls_expire_at"] = url_expiration["expires_at"]
                readable_report["meta"]["media_urls_ttl_seconds"] = url_expiration["ttl_seconds"]

        # For the raw_data column (preserves original group structure)
        raw_data_flat = defaultdict(list)

        # --- B. Iterate & Populate ---
        if api_obj.response and api_obj.response.groups:
            for group in api_obj.response.groups:
                # Normalize group name for safer matching (remove trailing spaces)
                clean_group_name = group.name.strip()

                for qa in group.question_and_answers:
                    clean_question_text = qa.question_text.strip()
                    answer_val = DataBuilder.extract_value(qa.response_answer_export)

                    # 1. Map to Form Table
                    map_key = (clean_group_name, clean_question_text)
                    if map_key in COLUMN_MAP:
                        col_name = COLUMN_MAP[map_key]
                        form_row[col_name] = answer_val

                    # 2. Map to Attachments Table
                    topic = DataBuilder.get_topic_category(clean_group_name, clean_question_text)
                    if qa.response_answer_export.type == 'image' and isinstance(answer_val, list):
                        for idx, media_id in enumerate(answer_val):
                            attachments_rows.append({
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
                                "media_type": "image/jpeg",  # Default assumption
                                "url": media_url_map.get(media_id)  # Download URL from ClaimX
                            })

                    # 3. Populate Readable Report (Categorized)
                    # For image type, transform answer from list of IDs to list of objects with media_id and url
                    if qa.response_answer_export.type == 'image' and isinstance(answer_val, list):
                        enriched_answer = [
                            {"media_id": media_id, "url": media_url_map.get(media_id)}
                            for media_id in answer_val
                        ]
                    else:
                        enriched_answer = answer_val

                    readable_item = {
                        "question": clean_question_text,
                        "answer": enriched_answer,
                        "type": qa.response_answer_export.type,
                        "control_id": qa.form_control.id
                    }
                    readable_report["topics"][topic].append(readable_item)

                    # 4. Populate Raw Data Blob (Grouped by original section)
                    raw_data_flat[clean_group_name].append({
                        "q": clean_question_text,
                        "a": answer_val,
                        "id": qa.form_control.id
                    })

        # Finalize Form Row with raw_data blob
        form_row["raw_data"] = json.dumps(dict(raw_data_flat), cls=DateTimeEncoder)

        # Convert readable_report topics from defaultdict to regular dict
        readable_report["topics"] = dict(readable_report["topics"])

        return form_row, attachments_rows, readable_report


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

    # Check if it's a known media question
    if question_text in media_mapping:
        return media_mapping[question_text]

    # Otherwise, generate from question text
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
    logger.debug(f"Parsing cabinet form for assignment_id={task_data.get('assignmentId')}")

    # Convert to typed dataclass using from_dict
    api_obj = from_dict(ApiResponse, task_data)

    # Use DataBuilder to process
    form_row, _, _ = DataBuilder.process(api_obj, event_id)

    # Convert to CabinetSubmission model
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
        lower_face_frames_doors_drawers_available=form_row.get("lower_face_frames_doors_drawers_available"),
        lower_face_frames_doors_drawers_damaged=form_row.get("lower_face_frames_doors_drawers_damaged"),
        lower_finished_end_panels_damaged=form_row.get("lower_finished_end_panels_damaged"),
        lower_end_panel_damage_present=form_row.get("lower_end_panel_damage_present"),
        lower_counter_type=form_row.get("lower_counter_type"),

        # Upper cabinets
        upper_cabinets_damaged=form_row.get("upper_cabinets_damaged"),
        upper_cabinets_lf=form_row.get("upper_cabinets_lf"),
        num_damaged_upper_boxes=form_row.get("num_damaged_upper_boxes"),
        upper_cabinets_detached=form_row.get("upper_cabinets_detached"),
        upper_face_frames_doors_drawers_available=form_row.get("upper_face_frames_doors_drawers_available"),
        upper_face_frames_doors_drawers_damaged=form_row.get("upper_face_frames_doors_drawers_damaged"),
        upper_finished_end_panels_damaged=form_row.get("upper_finished_end_panels_damaged"),
        upper_end_panel_damage_present=form_row.get("upper_end_panel_damage_present"),

        # Full height cabinets
        full_height_cabinets_damaged=form_row.get("full_height_cabinets_damaged"),
        full_height_cabinets_lf=form_row.get("full_height_cabinets_lf"),
        num_damaged_full_height_boxes=form_row.get("num_damaged_full_height_boxes"),
        full_height_cabinets_detached=form_row.get("full_height_cabinets_detached"),
        full_height_face_frames_doors_drawers_available=form_row.get("full_height_face_frames_doors_drawers_available"),
        full_height_face_frames_doors_drawers_damaged=form_row.get("full_height_face_frames_doors_drawers_damaged"),
        full_height_finished_end_panels_damaged=form_row.get("full_height_finished_end_panels_damaged"),

        # Island cabinets
        island_cabinets_damaged=form_row.get("island_cabinets_damaged"),
        island_cabinets_lf=form_row.get("island_cabinets_lf"),
        num_damaged_island_boxes=form_row.get("num_damaged_island_boxes"),
        island_cabinets_detached=form_row.get("island_cabinets_detached"),
        island_face_frames_doors_drawers_available=form_row.get("island_face_frames_doors_drawers_available"),
        island_face_frames_doors_drawers_damaged=form_row.get("island_face_frames_doors_drawers_damaged"),
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
    project_id: int,
    event_id: str,
    media_url_map: dict[int, str] = None,
) -> list[CabinetAttachment]:
    """
    Extract media attachments from ClaimX task response.

    Args:
        task_data: Full task data from ClaimX API
        assignment_id: Assignment ID
        project_id: ClaimX project ID
        event_id: Event ID for traceability
        media_url_map: Optional mapping of media_id to download URL

    Returns:
        List of attachment records with URLs enriched
    """
    logger.debug(f"Parsing attachments for assignment_id={assignment_id}")

    # Convert to typed dataclass using from_dict
    api_obj = from_dict(ApiResponse, task_data)

    # Use DataBuilder to process
    _, attachments_rows, _ = DataBuilder.process(api_obj, event_id, media_url_map)

    # Convert to CabinetAttachment models
    attachments = []
    for att_row in attachments_rows:
        attachments.append(CabinetAttachment(
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
        ))

    logger.debug(f"Parsed {len(attachments)} attachments")
    return attachments


def get_readable_report(task_data: dict, event_id: str, media_url_map: dict[int, str] = None) -> dict:
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
    logger.debug(f"Generating readable report for assignment_id={task_data.get('assignmentId')}")

    # Convert to typed dataclass using from_dict
    api_obj = from_dict(ApiResponse, task_data)

    # Use DataBuilder to process
    _, _, readable_report = DataBuilder.process(api_obj, event_id, media_url_map)

    return readable_report
