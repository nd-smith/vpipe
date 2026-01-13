"""
Delta table writer for iTel Cabinet data.

Writes submissions and attachments to Delta tables with explicit schemas
to ensure type compatibility and prevent NULL type coercion errors.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

import polars as pl

from kafka_pipeline.common.writers.base import BaseDeltaWriter

logger = logging.getLogger(__name__)


# Explicit schema for claimx_itel_forms table
# Matches the Delta table schema exactly to prevent type inference issues
# IMPORTANT: Column order must match the target table exactly
SUBMISSIONS_SCHEMA: Dict[str, pl.DataType] = {
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
ATTACHMENTS_SCHEMA: Dict[str, pl.DataType] = {
    "assignment_id": pl.Int64,
    "project_id": pl.Int64,
    "event_id": pl.Utf8,
    "control_id": pl.Utf8,
    "question_key": pl.Utf8,
    "question_text": pl.Utf8,
    "topic_category": pl.Utf8,
    "media_id": pl.Int64,
    "blob_path": pl.Utf8,
    "display_order": pl.Int32,
    "created_at": pl.Datetime("us", "UTC"),
    "is_active": pl.Boolean,
    "media_type": pl.Utf8,
}


class ItelCabinetDeltaWriter(BaseDeltaWriter):
    """
    Writes to both iTel Cabinet Delta tables with explicit schema handling.

    Uses pre-defined schemas to ensure type compatibility and prevent
    NULL type coercion errors during Delta merge operations.
    """

    def __init__(self, submissions_table_path: str, attachments_table_path: str):
        """
        Initialize writers for both tables.

        Args:
            submissions_table_path: Full path to submissions Delta table
            attachments_table_path: Full path to attachments Delta table
        """
        self.submissions_table_path = submissions_table_path
        self.attachments_table_path = attachments_table_path

        # Initialize base class with submissions table (for logging context)
        super().__init__(submissions_table_path, timestamp_column="updated_at")

        logger.info(
            "Initialized iTel Cabinet Delta writers",
            extra={
                'submissions_table': submissions_table_path,
                'attachments_table': attachments_table_path,
            }
        )

    def _process_submission_row(self, row: dict) -> dict:
        """
        Pre-process submission row to ensure correct types.

        Handles datetime parsing, type conversion, and null defaults.
        """
        processed = {}

        for col_name, col_type in SUBMISSIONS_SCHEMA.items():
            val = row.get(col_name)

            if val is None:
                processed[col_name] = None
            elif col_type == pl.Datetime("us", "UTC"):
                # Handle datetime conversion
                if isinstance(val, str):
                    processed[col_name] = datetime.fromisoformat(
                        val.replace("Z", "+00:00")
                    )
                elif isinstance(val, datetime):
                    if val.tzinfo is None:
                        processed[col_name] = val.replace(tzinfo=timezone.utc)
                    else:
                        processed[col_name] = val
                else:
                    processed[col_name] = None
            elif col_type == pl.Int64:
                processed[col_name] = int(val) if val is not None else None
            elif col_type == pl.Int32:
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

    def _process_attachment_row(self, row: dict) -> dict:
        """
        Pre-process attachment row to ensure correct types.
        """
        processed = {}

        for col_name, col_type in ATTACHMENTS_SCHEMA.items():
            val = row.get(col_name)

            if val is None:
                processed[col_name] = None
            elif col_type == pl.Datetime("us", "UTC"):
                if isinstance(val, str):
                    processed[col_name] = datetime.fromisoformat(
                        val.replace("Z", "+00:00")
                    )
                elif isinstance(val, datetime):
                    if val.tzinfo is None:
                        processed[col_name] = val.replace(tzinfo=timezone.utc)
                    else:
                        processed[col_name] = val
                else:
                    processed[col_name] = None
            elif col_type == pl.Int64:
                processed[col_name] = int(val) if val is not None else None
            elif col_type == pl.Int32:
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

    async def write_submission(self, submission_row: dict) -> bool:
        """
        Write submission to Delta table using MERGE (upsert on assignment_id).

        Args:
            submission_row: Submission data dict

        Returns:
            True if successful
        """
        try:
            # Pre-process row with explicit type conversion
            processed = self._process_submission_row(submission_row)

            # Create DataFrame with explicit schema
            df = pl.DataFrame([processed], schema=SUBMISSIONS_SCHEMA)

            # Temporarily switch table path for this operation
            original_path = self.table_path
            self.table_path = self.submissions_table_path

            # Merge into Delta table
            success = await self._async_merge(
                df,
                merge_keys=["assignment_id"],
                preserve_columns=["created_at"],
            )

            self.table_path = original_path

            if success:
                logger.info(
                    "Successfully wrote submission to Delta",
                    extra={
                        "assignment_id": submission_row.get("assignment_id"),
                        "table_path": self.submissions_table_path,
                    },
                )

            return success

        except Exception as e:
            logger.error(
                "Failed to write submission to Delta",
                extra={
                    "assignment_id": submission_row.get("assignment_id"),
                    "error": str(e),
                },
                exc_info=True,
            )
            return False

    async def write_attachments(self, attachment_rows: list[dict]) -> bool:
        """
        Write attachments to Delta table using MERGE.

        Args:
            attachment_rows: List of attachment dicts

        Returns:
            True if successful
        """
        if not attachment_rows:
            return True

        try:
            # Pre-process all rows with explicit type conversion
            processed_rows = [
                self._process_attachment_row(row) for row in attachment_rows
            ]

            # Create DataFrame with explicit schema
            df = pl.DataFrame(processed_rows, schema=ATTACHMENTS_SCHEMA)

            # Temporarily switch table path for this operation
            original_path = self.table_path
            self.table_path = self.attachments_table_path

            # Merge into Delta table
            success = await self._async_merge(
                df,
                merge_keys=["assignment_id", "media_id"],
                preserve_columns=["created_at"],
            )

            self.table_path = original_path

            if success:
                logger.info(
                    "Successfully wrote attachments to Delta",
                    extra={
                        "attachment_count": len(attachment_rows),
                        "table_path": self.attachments_table_path,
                    },
                )

            return success

        except Exception as e:
            logger.error(
                "Failed to write attachments to Delta",
                extra={
                    "attachment_count": len(attachment_rows),
                    "error": str(e),
                },
                exc_info=True,
            )
            return False
