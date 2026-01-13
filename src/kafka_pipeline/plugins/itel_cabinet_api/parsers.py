"""
Form parsing for iTel Cabinet Repair forms.

Extracts structured data from ClaimX task responses.
Clear, focused functions - no inheritance, no magic.
"""

import logging
from datetime import datetime
from typing import Any

from .models import CabinetSubmission, CabinetAttachment

logger = logging.getLogger(__name__)


# Question text to question_key mapping for media attachments
# (Control IDs are reused across cabinet types, so we map by question text)
MEDIA_QUESTION_TEXT_MAPPING = {
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

    response = task_data.get("response", {})
    external_link = task_data.get("externalLinkData", {})

    # Parse all form fields
    form_data = _parse_form_response(response)

    now = datetime.utcnow()

    return CabinetSubmission(
        # Primary identifiers
        assignment_id=task_data["assignmentId"],
        project_id=task_data["projectId"],
        form_id=task_data["formId"],
        form_response_id=task_data["formResponseId"],
        status=task_data["status"],
        event_id=event_id,

        # Dates
        date_assigned=task_data.get("dateAssigned"),
        date_completed=task_data.get("dateCompleted"),

        # Customer information
        customer_first_name=external_link.get("firstName"),
        customer_last_name=external_link.get("lastName"),
        customer_email=external_link.get("email"),
        customer_phone=str(external_link.get("phone")) if external_link.get("phone") else None,
        assignor_email=task_data.get("assignor"),

        # General damage information
        damage_description=form_data.get("damage_description"),
        additional_notes=form_data.get("additional_notes"),
        countertops_lf=form_data.get("countertops_lf"),

        # Lower cabinets
        lower_cabinets_damaged=form_data.get("lower_cabinets_damaged"),
        lower_cabinets_lf=form_data.get("lower_cabinets_lf"),
        num_damaged_lower_boxes=form_data.get("num_damaged_lower_boxes"),
        lower_cabinets_detached=form_data.get("lower_cabinets_detached"),
        lower_face_frames_doors_drawers_available=form_data.get("lower_face_frames_doors_drawers_available"),
        lower_face_frames_doors_drawers_damaged=form_data.get("lower_face_frames_doors_drawers_damaged"),
        lower_finished_end_panels_damaged=form_data.get("lower_finished_end_panels_damaged"),
        lower_end_panel_damage_present=form_data.get("lower_end_panel_damage_present"),
        lower_counter_type=form_data.get("lower_counter_type"),

        # Upper cabinets
        upper_cabinets_damaged=form_data.get("upper_cabinets_damaged"),
        upper_cabinets_lf=form_data.get("upper_cabinets_lf"),
        num_damaged_upper_boxes=form_data.get("num_damaged_upper_boxes"),
        upper_cabinets_detached=form_data.get("upper_cabinets_detached"),
        upper_face_frames_doors_drawers_available=form_data.get("upper_face_frames_doors_drawers_available"),
        upper_face_frames_doors_drawers_damaged=form_data.get("upper_face_frames_doors_drawers_damaged"),
        upper_finished_end_panels_damaged=form_data.get("upper_finished_end_panels_damaged"),
        upper_end_panel_damage_present=form_data.get("upper_end_panel_damage_present"),

        # Full height cabinets
        full_height_cabinets_damaged=form_data.get("full_height_cabinets_damaged"),
        full_height_cabinets_lf=form_data.get("full_height_cabinets_lf"),
        num_damaged_full_height_boxes=form_data.get("num_damaged_full_height_boxes"),
        full_height_cabinets_detached=form_data.get("full_height_cabinets_detached"),
        full_height_face_frames_doors_drawers_available=form_data.get("full_height_face_frames_doors_drawers_available"),
        full_height_face_frames_doors_drawers_damaged=form_data.get("full_height_face_frames_doors_drawers_damaged"),
        full_height_finished_end_panels_damaged=form_data.get("full_height_finished_end_panels_damaged"),

        # Island cabinets
        island_cabinets_damaged=form_data.get("island_cabinets_damaged"),
        island_cabinets_lf=form_data.get("island_cabinets_lf"),
        num_damaged_island_boxes=form_data.get("num_damaged_island_boxes"),
        island_cabinets_detached=form_data.get("island_cabinets_detached"),
        island_face_frames_doors_drawers_available=form_data.get("island_face_frames_doors_drawers_available"),
        island_face_frames_doors_drawers_damaged=form_data.get("island_face_frames_doors_drawers_damaged"),
        island_finished_end_panels_damaged=form_data.get("island_finished_end_panels_damaged"),
        island_end_panel_damage_present=form_data.get("island_end_panel_damage_present"),
        island_counter_type=form_data.get("island_counter_type"),

        # Metadata
        created_at=now,
        updated_at=now,
    )


def parse_cabinet_attachments(
    task_data: dict,
    assignment_id: int,
    project_id: int,
    event_id: str,
) -> list[CabinetAttachment]:
    """
    Extract media attachments from ClaimX task response.

    Args:
        task_data: Full task data from ClaimX API
        assignment_id: Assignment ID
        project_id: ClaimX project ID
        event_id: Event ID for traceability

    Returns:
        List of attachment records
    """
    logger.debug(f"Parsing attachments for assignment_id={assignment_id}")

    response = task_data.get("response", {})
    groups = response.get("groups", [])

    attachments = []
    display_order = 0
    now = datetime.utcnow()

    for group in groups:
        questions = group.get("questionAndAnswers", [])

        for question in questions:
            question_text = question.get("questionText", "")
            answer_export = question.get("responseAnswerExport", {})

            # Only process image type answers
            if answer_export.get("type") != "image":
                continue

            # Check if this is a media question we care about (by question text)
            question_key = MEDIA_QUESTION_TEXT_MAPPING.get(question_text)
            if not question_key:
                # Fall back to generic key from question text
                question_key = question_text.lower().replace(" ", "_").replace("/", "_")

            # Extract claimMediaIds from response
            claim_media_ids = answer_export.get("claimMediaIds", [])

            # Create attachment record for each media ID
            for mid in claim_media_ids:
                if mid:
                    attachments.append(CabinetAttachment(
                        assignment_id=assignment_id,
                        project_id=project_id,
                        event_id=event_id,
                        question_key=question_key,
                        question_text=question_text,
                        media_id=mid,
                        blob_path=None,  # Populated by media downloader if configured
                        display_order=display_order,
                        created_at=now,
                    ))
                    display_order += 1

    logger.debug(f"Parsed {len(attachments)} attachments")
    return attachments


def _parse_form_response(response: dict) -> dict:
    """
    Parse nested form response structure into flat key-value dict.

    Args:
        response: The 'response' object from task data

    Returns:
        Flat dict with form field names as keys
    """
    form_data = {}
    groups = response.get("groups", [])

    for group in groups:
        questions = group.get("questionAndAnswers", [])

        for question in questions:
            question_text = question.get("questionText", "")
            answer_export = question.get("responseAnswerExport", {})

            # Parse based on answer type
            answer_type = answer_export.get("type")

            if answer_type == "option":
                # Dropdown/radio answers
                option_answer = answer_export.get("optionAnswer", {})
                value = option_answer.get("name")
                field_name = _question_to_field_name(question_text)

                # Convert Yes/No to boolean for boolean fields
                # Exception: *_available fields stay as "Yes"/"No" strings per table schema
                if value in ("Yes", "No"):
                    is_available_field = field_name.endswith("_available")
                    if is_available_field:
                        # Keep as string for *_face_frames_doors_drawers_available fields
                        form_data[field_name] = value
                    elif any(keyword in question_text.lower() for keyword in ["damaged", "detached", "present"]):
                        form_data[field_name] = (value == "Yes")
                    else:
                        form_data[field_name] = value
                else:
                    form_data[field_name] = value

            elif answer_type == "number":
                # Numeric answers - may be a primitive or dict with value/name
                number_answer = answer_export.get("numberAnswer")
                field_name = _question_to_field_name(question_text)
                if number_answer is not None:
                    # Handle dict structure (e.g., {"value": 123} or {"name": 123})
                    if isinstance(number_answer, dict):
                        number_answer = number_answer.get("value") or number_answer.get("name")
                    if number_answer is not None:
                        form_data[field_name] = int(number_answer)

            elif answer_type == "text":
                # Text answers - API uses "text" field
                text_answer = answer_export.get("text")
                field_name = _question_to_field_name(question_text)
                form_data[field_name] = text_answer

            # Skip image type - handled separately in parse_cabinet_attachments

    return form_data


def _question_to_field_name(question_text: str) -> str:
    """
    Convert question text to database field name.

    Examples:
        "Lower Cabinets Damaged?" -> "lower_cabinets_damaged"
        "Number of Damaged Lower Cabinet Boxes" -> "num_damaged_lower_boxes"
    """
    # Explicit mapping for known questions (matching actual ClaimX API responses)
    field_mapping = {
        # Cabinet Types Damaged
        "Lower Cabinets Damaged?": "lower_cabinets_damaged",
        "Upper Cabinets Damaged?": "upper_cabinets_damaged",
        "Full Height Cabinets Damaged?": "full_height_cabinets_damaged",
        "Island Cabinets Damaged?": "island_cabinets_damaged",

        # Linear Feet Capture
        "Lower Cabinets (linear feet)": "lower_cabinets_lf",
        "Upper Cabinets (linear feet)": "upper_cabinets_lf",
        "Full-Height Cabinets (linear feet)": "full_height_cabinets_lf",
        "Island Cabinets (linear feet)": "island_cabinets_lf",
        "Countertops (linear feet)": "countertops_lf",

        # Damage Description
        "Enter Damaged Description": "damage_description",
        "Now that you have been through the process, please provide any other details that you think would be relevant to the cabinet damage": "additional_notes",

        # Lower Cabinets
        "Enter Number of Damaged Lower Boxes": "num_damaged_lower_boxes",
        "Are The Lower Cabinets Detached?": "lower_cabinets_detached",
        "Are All The Face Frames, Doors, And Drawers Available?": "lower_face_frames_doors_drawers_available",
        "Are All The Face Frames, Doors, And Drawer Fronts Damaged?": "lower_face_frames_doors_drawers_damaged",
        "Are The Lower Finished End Panels Damaged?": "lower_finished_end_panels_damaged",
        "Is there lower cabinet end panel damage?": "lower_end_panel_damage_present",
        "Select Lower Cabinet Counter Type": "lower_counter_type",
        "If multiple counter types are present, please list the additional types here": "lower_counter_type_additional",

        # Upper Cabinets
        "Enter Number of Damaged Upper Boxes": "num_damaged_upper_boxes",
        "Are The Upper Cabinets Detached?": "upper_cabinets_detached",
        "Is there upper cabinet end panel damage?": "upper_end_panel_damage_present",
        "Are The Upper Finished End Panels Damaged?": "upper_finished_end_panels_damaged",

        # Full Height/Pantry Cabinets
        "Enter Number of Damaged Full Height Boxes": "num_damaged_full_height_boxes",
        "Are The Full Height Cabinets Detached?": "full_height_cabinets_detached",
        " Are All The Face Frames, Doors, And Drawer Fronts Damaged?": "full_height_face_frames_doors_drawers_damaged",
        "Are the Full Height Finished End Panels Damaged?": "full_height_finished_end_panels_damaged",

        # Island Cabinets
        "Enter Number of Damaged Island Boxes": "num_damaged_island_boxes",
        "Are The Island Cabinets Detached?": "island_cabinets_detached",
        "Are the Island Finished End Panels Damaged?": "island_finished_end_panels_damaged",
        "Is there island cabinet end panel damage?": "island_end_panel_damage_present",
        "Select Island Cabinet Counter Type": "island_counter_type",
    }

    return field_mapping.get(question_text, question_text.lower().replace(" ", "_").replace("?", ""))
