"""
Event transformation logic for verisk domain.

Flattens nested JSON event structures into tabular format for Delta tables.
Migrated from verisk_pipeline.verisk.stages.transform.
"""

import json
import logging

import polars as pl

logger = logging.getLogger(__name__)


# JSONPath → output column mappings for scalar fields extracted from the data column.
# All are extracted as Utf8 via json_path_match (returns null for missing paths).
_SCALAR_FIELD_MAPPINGS: list[tuple[str, str]] = [
    # (json_path, output_column_name)
    ("$.description", "description"),
    ("$.assignmentId", "assignment_id"),
    ("$.originalAssignmentId", "original_assignment_id"),
    ("$.xnAddress", "xn_address"),
    ("$.carrierId", "carrier_id"),
    ("$.estimateVersion", "estimate_version"),
    ("$.note", "note"),
    ("$.author", "author"),
    ("$.senderReviewerName", "sender_reviewer_name"),
    ("$.senderReviewerEmail", "sender_reviewer_email"),
    ("$.carrierReviewerName", "carrier_reviewer_name"),
    ("$.carrierReviewerEmail", "carrier_reviewer_email"),
    ("$.dateTime", "event_datetime_mdt"),
    # Nested fields
    ("$.adm.coverageLoss.claimNumber", "claim_number"),
    ("$.contact.type", "contact_type"),
    ("$.contact.name", "contact_name"),
    ("$.contact.contactMethods.phone.type", "contact_phone_type"),
    ("$.contact.contactMethods.phone.number", "contact_phone_number"),
    ("$.contact.contactMethods.phone.extension", "contact_phone_extension"),
    ("$.contact.contactMethods.email.address", "contact_email_address"),
]

# Struct dtype used for extracting the attachments list from JSON
_ATTACHMENTS_DTYPE = pl.Struct({"attachments": pl.List(pl.Utf8)})


def _normalize_data_column(df: pl.DataFrame) -> pl.DataFrame:
    """Ensure the ``data`` column is Utf8 (JSON strings).

    When raw events arrive with ``data`` already parsed as dicts, Polars
    creates a Struct column.  We serialize those back to JSON strings so
    the rest of the pipeline can use ``json_path_match`` uniformly.
    """
    if df.schema["data"] == pl.Utf8:
        return df
    # Struct / other → serialize to JSON strings (uncommon path)
    return df.with_columns(
        pl.Series(
            "data",
            [json.dumps(v) if v is not None else None for v in df["data"].to_list()],
        )
    )


def flatten_events(df: pl.DataFrame) -> pl.DataFrame:
    # 1. Resolve event_id source logic BEFORE the initial select
    if "event_id" in df.columns:
        event_id_expr = pl.col("event_id")
    elif "eventId" in df.columns:
        event_id_expr = pl.col("eventId").alias("event_id")
    else:
        event_id_expr = pl.lit(None).cast(pl.Utf8).alias("event_id")

    # 2. Normalize data column to Utf8 (handles dict-valued data)
    df = _normalize_data_column(df)

    # 3. Build base columns
    base_exprs = [
        pl.col("type"),
        pl.col("type").str.split(".").list.last().alias("status_subtype"),
        pl.col("version"),
        pl.col("utcDateTime")
        .str.replace("Z$", "")
        .str.to_datetime("%Y-%m-%dT%H:%M:%S")
        .dt.replace_time_zone("UTC")
        .alias("ingested_at"),
        pl.col("utcDateTime")
        .str.replace("Z$", "")
        .str.to_datetime("%Y-%m-%dT%H:%M:%S")
        .dt.replace_time_zone("UTC")
        .dt.date()
        .alias("event_date"),
        pl.col("traceId").alias("trace_id"),
        event_id_expr,
    ]

    # 4. Extract scalar fields from JSON via json_path_match (no Python loop)
    data_col = pl.col("data")

    # Fields before attachments (matches FLATTENED_SCHEMA order)
    pre_attach_exprs = [
        data_col.str.json_path_match(path).alias(alias)
        for path, alias in _SCALAR_FIELD_MAPPINGS
        if alias != "claim_number"
        and not alias.startswith("contact_")
    ]

    # 5. Extract attachments: parse as list, filter nulls/empties, join with comma
    attachments_raw = (
        data_col.str.json_decode(_ATTACHMENTS_DTYPE)
        .struct.field("attachments")
        .list.eval(pl.element().drop_nulls().replace("", None).drop_nulls())
        .list.join(",")
    )
    attachments_expr = (
        pl.when(attachments_raw == "")
        .then(None)
        .otherwise(attachments_raw)
        .cast(pl.Utf8)
        .alias("attachments")
    )

    # Fields after attachments (nested fields)
    post_attach_exprs = [
        data_col.str.json_path_match(path).alias(alias)
        for path, alias in _SCALAR_FIELD_MAPPINGS
        if alias == "claim_number"
        or alias.startswith("contact_")
    ]

    # 6. Raw JSON column (full original row serialized from source columns)
    raw_json_col = df.select(pl.struct(pl.all()).struct.json_encode().alias("raw_json"))

    # 7. Build extracted columns, then combine with raw_json
    extracted = df.select(
        base_exprs
        + pre_attach_exprs
        + [attachments_expr]
        + post_attach_exprs
    )
    result = pl.concat([extracted, raw_json_col], how="horizontal")

    logger.info("Events flattened: %s rows, %s columns", len(result), len(result.columns))
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
