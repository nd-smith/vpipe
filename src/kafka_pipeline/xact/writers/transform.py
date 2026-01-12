"""
Event transformation logic for xact domain.

Flattens nested JSON event structures into tabular format for Delta tables.
Migrated from verisk_pipeline.xact.stages.transform.
"""

import json
import logging
from typing import Any, Dict, List, Optional, Union

import polars as pl

logger = logging.getLogger(__name__)


def _safe_get(d: Optional[Dict], *keys: str, default: Any = None) -> Any:
    """
    Safely navigate nested dict keys.

    Args:
        d: Dictionary to navigate (can be None)
        *keys: Keys to traverse
        default: Default value if any key is missing

    Returns:
        Value at the nested key path, or default
    """
    if d is None:
        return default
    current = d
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def _parse_data_column(data: Optional[Any]) -> Optional[Dict]:
    """Parse data to dict, handling dict, JSON string, None, and errors."""
    if data is None:
        return None
    # If already a dict, return as-is
    if isinstance(data, dict):
        return data
    # Otherwise try to parse as JSON string
    try:
        return json.loads(data)
    except (json.JSONDecodeError, TypeError):
        return None


def _extract_row_fields(data_dict: Optional[Dict]) -> Dict[str, Any]:
    """
    Extract all fields from a parsed data dict.

    Args:
        data_dict: Parsed JSON data dict (can be None)

    Returns:
        Dict with all extracted fields
    """
    if data_dict is None:
        return {
            "description": None,
            "assignment_id": None,
            "original_assignment_id": None,
            "xn_address": None,
            "carrier_id": None,
            "estimate_version": None,
            "note": None,
            "author": None,
            "sender_reviewer_name": None,
            "sender_reviewer_email": None,
            "carrier_reviewer_name": None,
            "carrier_reviewer_email": None,
            "event_datetime_mdt": None,
            "attachments": None,
            "claim_number": None,
            "contact_type": None,
            "contact_name": None,
            "contact_phone_type": None,
            "contact_phone_number": None,
            "contact_phone_extension": None,
            "contact_email_address": None,
        }

    # Extract attachments - join list to comma-separated string
    attachments_list = data_dict.get("attachments")
    attachments_str = None
    if attachments_list and isinstance(attachments_list, list):
        attachments_str = ",".join(str(a) for a in attachments_list if a)

    return {
        # Simple fields
        "description": data_dict.get("description"),
        "assignment_id": data_dict.get("assignmentId"),
        "original_assignment_id": data_dict.get("originalAssignmentId"),
        "xn_address": data_dict.get("xnAddress"),
        "carrier_id": data_dict.get("carrierId"),
        "estimate_version": data_dict.get("estimateVersion"),
        "note": data_dict.get("note"),
        "author": data_dict.get("author"),
        "sender_reviewer_name": data_dict.get("senderReviewerName"),
        "sender_reviewer_email": data_dict.get("senderReviewerEmail"),
        "carrier_reviewer_name": data_dict.get("carrierReviewerName"),
        "carrier_reviewer_email": data_dict.get("carrierReviewerEmail"),
        "event_datetime_mdt": data_dict.get("dateTime"),
        "attachments": attachments_str,
        # Nested fields
        "claim_number": _safe_get(data_dict, "adm", "coverageLoss", "claimNumber"),
        "contact_type": _safe_get(data_dict, "contact", "type"),
        "contact_name": _safe_get(data_dict, "contact", "name"),
        "contact_phone_type": _safe_get(
            data_dict, "contact", "contactMethods", "phone", "type"
        ),
        "contact_phone_number": _safe_get(
            data_dict, "contact", "contactMethods", "phone", "number"
        ),
        "contact_phone_extension": _safe_get(
            data_dict, "contact", "contactMethods", "phone", "extension"
        ),
        "contact_email_address": _safe_get(
            data_dict, "contact", "contactMethods", "email", "address"
        ),
    }


def flatten_events(df: pl.DataFrame) -> pl.DataFrame:
    # 1. Resolve event_id source logic BEFORE the initial select
    if "event_id" in df.columns:
        event_id_expr = pl.col("event_id")
    elif "eventId" in df.columns:
        event_id_expr = pl.col("eventId").alias("event_id")
    else:
        # Initialize as Utf8 nulls if it doesn't exist in source yet
        event_id_expr = pl.lit(None).cast(pl.Utf8).alias("event_id")

    # 2. Build base columns including the resolved event_id_expr
    base_df = df.select(
        [
            pl.col("type"),
            pl.col("type").str.split(".").list.last().alias("status_subtype"),
            pl.col("version"),
            pl.col("utcDateTime").cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
            pl.col("utcDateTime")
            .cast(pl.Datetime("us", "UTC"))
            .dt.date()
            .alias("event_date"),
            pl.col("traceId").alias("trace_id"),
            event_id_expr, # Successfully added to the schema here
        ]
    )

    # Parse data column and extract fields row by row
    data_json_list = df["data"].to_list()

    # Parse all rows
    parsed_rows = [_extract_row_fields(_parse_data_column(d)) for d in data_json_list]

    # Build columns from parsed data
    extracted_columns = (
        {field: [row[field] for row in parsed_rows] for field in parsed_rows[0].keys()}
        if parsed_rows
        else {}
    )

    # Create DataFrame from extracted fields with explicit schema to prevent Null types
    # All extracted fields are strings - Delta Lake doesn't support Null type columns
    extracted_schema = {col: pl.Utf8 for col in extracted_columns.keys()}
    extracted_df = pl.DataFrame(extracted_columns, schema=extracted_schema)

    # Add raw_json column (the original data column)
    raw_json_col = df.select([pl.struct(pl.all()).cast(pl.Utf8).alias("raw_json")])

    # Combine all columns
    result = pl.concat([base_df, extracted_df, raw_json_col], how="horizontal")

    logger.info(
        f"Events flattened: {len(result)} rows, {len(result.columns)} columns"
    )
    return result


# Output schema for reference (all columns produced by flatten_events)
FLATTENED_SCHEMA = {
    "type": pl.Utf8,
    "status_subtype": pl.Utf8,
    "version": pl.Utf8,
    "ingested_at": pl.Datetime(time_zone="UTC"),
    "event_date": pl.Date,
    "trace_id": pl.Utf8,
    "event_id": pl.Utf8,
    "description": pl.Utf8,
    "assignment_id": pl.Utf8,
    "original_assignment_id": pl.Utf8,
    "xn_address": pl.Utf8,
    "carrier_id": pl.Utf8,
    "estimate_version": pl.Utf8,
    "note": pl.Utf8,
    "author": pl.Utf8,
    "sender_reviewer_name": pl.Utf8,
    "sender_reviewer_email": pl.Utf8,
    "carrier_reviewer_name": pl.Utf8,
    "carrier_reviewer_email": pl.Utf8,
    "event_datetime_mdt": pl.Utf8,
    "attachments": pl.Utf8,
    "claim_number": pl.Utf8,
    "contact_type": pl.Utf8,
    "contact_name": pl.Utf8,
    "contact_phone_type": pl.Utf8,
    "contact_phone_number": pl.Utf8,
    "contact_phone_extension": pl.Utf8,
    "contact_email_address": pl.Utf8,
    "raw_json": pl.Utf8,
}


def get_expected_columns() -> list:
    """Get list of expected output column names."""
    return list(FLATTENED_SCHEMA.keys())
