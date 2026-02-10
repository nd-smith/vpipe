"""
Delta table writers for iTel Cabinet data.

Separate writer per table — no path-swapping needed.
Each writer wraps BaseDeltaWriter for one Delta table with an explicit schema.
"""

import logging
from datetime import UTC, datetime

import polars as pl

from pipeline.common.writers.base import BaseDeltaWriter

logger = logging.getLogger(__name__)


# Explicit schema for claimx_itel_forms table
# Matches the Delta table schema exactly to prevent type inference issues
# IMPORTANT: Column order must match the target table exactly
SUBMISSIONS_SCHEMA: dict[str, pl.DataType] = {
    "assignment_id": pl.Int64,
    "task_id": pl.Int64,
    "task_name": pl.Utf8,
    "project_id": pl.Utf8,
    "form_id": pl.Utf8,
    "form_response_id": pl.Utf8,
    "status": pl.Utf8,
    "event_id": pl.Utf8,  # Position 8 in DB schema (after adding task_id/task_name)
    "date_assigned": pl.Datetime("us", "UTC"),
    "date_completed": pl.Datetime("us", "UTC"),
    "ingested_at": pl.Datetime("us", "UTC"),
    "customer_first_name": pl.Utf8,
    "customer_last_name": pl.Utf8,
    "customer_email": pl.Utf8,
    "customer_phone": pl.Utf8,
    "assignor_email": pl.Utf8,
    "external_link_url": pl.Utf8,
    "damage_description": pl.Utf8,
    "additional_notes": pl.Utf8,
    "countertops_lf": pl.Int32,
    "raw_data": pl.Utf8,  # JSON blob
    # Lower cabinets
    "lower_cabinets_damaged": pl.Boolean,
    "lower_cabinets_lf": pl.Int32,
    "num_damaged_lower_boxes": pl.Int32,
    "lower_cabinets_detached": pl.Boolean,
    "lower_face_frames_doors_drawers_available": pl.Utf8,  # "Yes"/"No" string
    "lower_face_frames_doors_drawers_damaged": pl.Boolean,
    "lower_finished_end_panels_damaged": pl.Boolean,
    "lower_end_panel_damage_present": pl.Boolean,
    "lower_counter_type": pl.Utf8,
    # Upper cabinets
    "upper_cabinets_damaged": pl.Boolean,
    "upper_cabinets_lf": pl.Int32,
    "num_damaged_upper_boxes": pl.Int32,
    "upper_cabinets_detached": pl.Boolean,
    "upper_face_frames_doors_drawers_available": pl.Utf8,  # "Yes"/"No" string
    "upper_face_frames_doors_drawers_damaged": pl.Boolean,
    "upper_finished_end_panels_damaged": pl.Boolean,
    "upper_end_panel_damage_present": pl.Boolean,
    # Full height cabinets (no end_panel_damage_present or counter_type)
    "full_height_cabinets_damaged": pl.Boolean,
    "full_height_cabinets_lf": pl.Int32,
    "num_damaged_full_height_boxes": pl.Int32,
    "full_height_cabinets_detached": pl.Boolean,
    "full_height_face_frames_doors_drawers_available": pl.Utf8,  # "Yes"/"No" string
    "full_height_face_frames_doors_drawers_damaged": pl.Boolean,
    "full_height_finished_end_panels_damaged": pl.Boolean,
    # Island cabinets
    "island_cabinets_damaged": pl.Boolean,
    "island_cabinets_lf": pl.Int32,
    "num_damaged_island_boxes": pl.Int32,
    "island_cabinets_detached": pl.Boolean,
    "island_face_frames_doors_drawers_available": pl.Utf8,  # "Yes"/"No" string
    "island_face_frames_doors_drawers_damaged": pl.Boolean,
    "island_finished_end_panels_damaged": pl.Boolean,
    "island_end_panel_damage_present": pl.Boolean,
    "island_counter_type": pl.Utf8,
    # Metadata
    "created_at": pl.Datetime("us", "UTC"),
    "updated_at": pl.Datetime("us", "UTC"),
}


# Explicit schema for claimx_itel_attachments table
ATTACHMENTS_SCHEMA: dict[str, pl.DataType] = {
    "assignment_id": pl.Int64,
    "project_id": pl.Int64,
    "event_id": pl.Utf8,
    "control_id": pl.Utf8,
    "question_key": pl.Utf8,
    "question_text": pl.Utf8,
    "topic_category": pl.Utf8,
    "media_id": pl.Int64,
    "url": pl.Utf8,
    "display_order": pl.Int32,
    "created_at": pl.Datetime("us", "UTC"),
    "is_active": pl.Boolean,
    "media_type": pl.Utf8,
}


def _process_row(row: dict, schema: dict[str, pl.DataType]) -> dict:
    """Pre-process a row dict to match a Polars schema.

    Handles datetime parsing, numeric/boolean coercion, and null defaults.
    """
    processed = {}

    for col_name, col_type in schema.items():
        val = row.get(col_name)

        if val is None:
            processed[col_name] = None
        elif col_type == pl.Datetime("us", "UTC"):
            if isinstance(val, str):
                processed[col_name] = datetime.fromisoformat(val.replace("Z", "+00:00"))
            elif isinstance(val, datetime):
                if val.tzinfo is None:
                    processed[col_name] = val.replace(tzinfo=UTC)
                else:
                    processed[col_name] = val
            else:
                processed[col_name] = None
        elif col_type == pl.Int64 or col_type == pl.Int32:
            processed[col_name] = int(val) if val is not None else None
        elif col_type == pl.Boolean:
            if isinstance(val, bool):
                processed[col_name] = val
            elif isinstance(val, str):
                processed[col_name] = val.lower() in ("yes", "true", "1")
            else:
                processed[col_name] = bool(val) if val is not None else None
        elif col_type == pl.Utf8:
            processed[col_name] = str(val) if val is not None else None
        else:
            processed[col_name] = val

    return processed


class ItelSubmissionsDeltaWriter(BaseDeltaWriter):
    """Writes to the claimx_itel_forms Delta table."""

    def __init__(self, table_path: str):
        super().__init__(table_path, timestamp_column="updated_at")
        logger.info("Initialized iTel submissions Delta writer", extra={"table_path": table_path})

    async def write(self, row: dict) -> bool:
        processed = _process_row(row, SUBMISSIONS_SCHEMA)
        df = pl.DataFrame([processed], schema=SUBMISSIONS_SCHEMA)
        return await self._async_merge(
            df,
            merge_keys=["assignment_id"],
            preserve_columns=["created_at"],
        )


class ItelAttachmentsDeltaWriter(BaseDeltaWriter):
    """Writes to the claimx_itel_attachments Delta table."""

    def __init__(self, table_path: str):
        super().__init__(table_path, timestamp_column="created_at")
        logger.info("Initialized iTel attachments Delta writer", extra={"table_path": table_path})

    async def write(self, rows: list[dict]) -> bool:
        if not rows:
            return True
        processed = [_process_row(r, ATTACHMENTS_SCHEMA) for r in rows]
        df = pl.DataFrame(processed, schema=ATTACHMENTS_SCHEMA)
        return await self._async_merge(
            df,
            merge_keys=["assignment_id", "media_id"],
            preserve_columns=["created_at"],
        )
