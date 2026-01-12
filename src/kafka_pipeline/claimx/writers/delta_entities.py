"""
Delta Lake writer for ClaimX entity tables.

Writes ClaimX entity data to 7 separate Delta tables:
- claimx_projects: Project metadata
- claimx_contacts: Contact/policyholder information
- claimx_attachment_metadata: Attachment metadata
- claimx_tasks: Task information
- claimx_task_templates: Task template definitions
- claimx_external_links: External resource links
- claimx_video_collab: Video collaboration sessions

Uses merge (upsert) operations with appropriate primary keys for idempotency.
"""

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

import polars as pl

from core.logging.setup import get_logger
from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage
from kafka_pipeline.common.writers.base import BaseDeltaWriter


# Schema definitions matching actual Delta table schemas exactly
# These schemas are derived from the Fabric Delta tables to ensure type compatibility
# Spark/Delta type mapping:
#   StringType -> pl.Utf8
#   BooleanType -> pl.Boolean
#   IntegerType -> pl.Int32
#   LongType -> pl.Int64
#   DoubleType -> pl.Float64
#   TimestampType -> pl.Datetime("us", "UTC")
#   DateType -> pl.Date

CONTACTS_SCHEMA = {
    "project_id": pl.Utf8,
    "contact_email": pl.Utf8,
    "contact_type": pl.Utf8,
    "first_name": pl.Utf8,
    "last_name": pl.Utf8,
    "phone_number": pl.Utf8,
    "is_primary_contact": pl.Boolean,
    "master_file_name": pl.Utf8,
    "task_assignment_id": pl.Int32,
    "video_collaboration_id": pl.Utf8,
    "event_id": pl.Utf8,
    "created_at": pl.Datetime("us", "UTC"),
    "last_enriched_at": pl.Datetime("us", "UTC"),
    "created_date": pl.Date,
    "phone_country_code": pl.Int64,
    "updated_at": pl.Utf8,
}

MEDIA_SCHEMA = {
    "media_id": pl.Utf8,
    "project_id": pl.Utf8,
    "task_assignment_id": pl.Utf8,
    "file_type": pl.Utf8,
    "file_name": pl.Utf8,
    "media_description": pl.Utf8,
    "media_comment": pl.Utf8,
    "latitude": pl.Utf8,
    "longitude": pl.Utf8,
    "gps_source": pl.Utf8,
    "taken_date": pl.Utf8,
    "full_download_link": pl.Utf8,
    "expires_at": pl.Utf8,
    "event_id": pl.Utf8,
    "created_at": pl.Utf8,
    "updated_at": pl.Utf8,
    "last_enriched_at": pl.Utf8,
}

PROJECTS_SCHEMA = {
    "project_id": pl.Utf8,
    "project_number": pl.Utf8,
    "master_file_name": pl.Utf8,
    "secondary_number": pl.Utf8,
    "status": pl.Utf8,
    "created_date": pl.Utf8,
    "date_of_loss": pl.Utf8,
    "type_of_loss": pl.Utf8,
    "cause_of_loss": pl.Utf8,
    "loss_description": pl.Utf8,
    "customer_first_name": pl.Utf8,
    "customer_last_name": pl.Utf8,
    "street1": pl.Utf8,
    "city": pl.Utf8,
    "state_province": pl.Utf8,
    "zip_postcode": pl.Utf8,
    "primary_email": pl.Utf8,
    "primary_phone": pl.Utf8,
    "custom_attribute1": pl.Utf8,
    "custom_attribute2": pl.Utf8,
    "custom_attribute3": pl.Utf8,
    "coverages": pl.Utf8,
    "contents_task_sent": pl.Boolean,
    "contents_task_at": pl.Datetime("us", "UTC"),
    "xa_autolink_fail": pl.Boolean,
    "xa_autolink_fail_at": pl.Datetime("us", "UTC"),
    "event_id": pl.Utf8,
    "created_at": pl.Datetime("us", "UTC"),
    "updated_at": pl.Datetime("us", "UTC"),
    "last_enriched_at": pl.Datetime("us", "UTC"),
}

TASKS_SCHEMA = {
    "assignment_id": pl.Int64,
    "task_id": pl.Int64,
    "project_id": pl.Utf8,
    "assignee_id": pl.Int64,
    "assignor_id": pl.Int64,
    "date_assigned": pl.Utf8,
    "date_completed": pl.Utf8,
    "status": pl.Utf8,
    "stp_enabled": pl.Boolean,
    "mfn": pl.Utf8,
    "event_id": pl.Utf8,
    "created_at": pl.Datetime("us", "UTC"),
    "updated_at": pl.Datetime("us", "UTC"),
    "last_enriched_at": pl.Datetime("us", "UTC"),
}

TASK_TEMPLATES_SCHEMA = {
    "task_id": pl.Int64,
    "comp_id": pl.Int64,
    "name": pl.Utf8,
    "description": pl.Utf8,
    "form_id": pl.Utf8,
    "form_name": pl.Utf8,
    "enabled": pl.Boolean,
    "is_default": pl.Boolean,
    "is_manual_delivery": pl.Boolean,
    "is_external_link_delivery": pl.Boolean,
    "provide_portal_access": pl.Boolean,
    "notify_assigned_send_recipient": pl.Boolean,
    "notify_assigned_send_recipient_sms": pl.Boolean,
    "notify_assigned_subject": pl.Utf8,
    "notify_task_completed": pl.Boolean,
    "notify_completed_subject": pl.Utf8,
    "allow_resubmit": pl.Boolean,
    "auto_generate_pdf": pl.Boolean,
    "modified_by": pl.Utf8,
    "modified_by_id": pl.Int64,
    "modified_date": pl.Utf8,
    "event_id": pl.Utf8,
    "created_at": pl.Datetime("us", "UTC"),
    "updated_at": pl.Datetime("us", "UTC"),
    "last_enriched_at": pl.Datetime("us", "UTC"),
}

EXTERNAL_LINKS_SCHEMA = {
    "link_id": pl.Int64,
    "assignment_id": pl.Int64,
    "project_id": pl.Utf8,
    "link_code": pl.Utf8,
    "url": pl.Utf8,
    "notification_access_method": pl.Utf8,
    "country_id": pl.Int64,
    "state_id": pl.Int64,
    "created_date": pl.Utf8,
    "accessed_count": pl.Int64,
    "last_accessed": pl.Utf8,
    "event_id": pl.Utf8,
    "created_at": pl.Utf8,
    "updated_at": pl.Utf8,
}

VIDEO_COLLAB_SCHEMA = {
    "video_collaboration_id": pl.Int64,
    "claim_id": pl.Int64,
    "mfn": pl.Utf8,
    "claim_number": pl.Utf8,
    "policy_number": pl.Utf8,
    "email_user_name": pl.Utf8,
    "claim_rep_first_name": pl.Utf8,
    "claim_rep_last_name": pl.Utf8,
    "claim_rep_full_name": pl.Utf8,
    "number_of_videos": pl.Int64,
    "number_of_photos": pl.Int64,
    "number_of_viewers": pl.Int64,
    "session_count": pl.Int64,
    "total_time_seconds": pl.Utf8,
    "total_time": pl.Utf8,
    "created_date": pl.Utf8,
    "live_call_first_session": pl.Utf8,
    "live_call_last_session": pl.Utf8,
    "company_id": pl.Int64,
    "company_name": pl.Utf8,
    "guid": pl.Utf8,
    "event_id": pl.Utf8,
    "created_at": pl.Datetime("us", "UTC"),
    "updated_at": pl.Datetime("us", "UTC"),
    "last_enriched_at": pl.Datetime("us", "UTC"),
}

# Map table names to their schema definitions
TABLE_SCHEMAS: Dict[str, Dict[str, pl.DataType]] = {
    "contacts": CONTACTS_SCHEMA,
    "media": MEDIA_SCHEMA,
    "projects": PROJECTS_SCHEMA,
    "tasks": TASKS_SCHEMA,
    "task_templates": TASK_TEMPLATES_SCHEMA,
    "external_links": EXTERNAL_LINKS_SCHEMA,
    "video_collab": VIDEO_COLLAB_SCHEMA,
}


# Merge keys for each entity table (from verisk_pipeline)
MERGE_KEYS: Dict[str, List[str]] = {
    "projects": ["project_id"],
    "contacts": ["project_id", "contact_email", "contact_type"],
    "media": ["media_id"],
    "tasks": ["assignment_id"],
    "task_templates": ["task_id"],
    "external_links": ["link_id"],
    "video_collab": ["video_collaboration_id"],
}


class ClaimXEntityWriter:
    """
    Manages writes to all ClaimX entity Delta tables.

    Uses merge operations with merge keys for idempotency.
    Each entity type is written to its own Delta table with appropriate merge keys.

    Entity Tables:
        - projects → claimx_projects (merge key: project_id)
        - contacts → claimx_contacts (merge keys: project_id, contact_email, contact_type)
        - media → claimx_attachment_metadata (merge key: media_id)
        - tasks → claimx_tasks (merge key: assignment_id)
        - task_templates → claimx_task_templates (merge key: task_id)
        - external_links → claimx_external_links (merge key: link_id)
        - video_collab → claimx_video_collab (merge key: video_collaboration_id)

    Usage:
        >>> writer = ClaimXEntityWriter(
        ...     projects_table_path="abfss://.../claimx_projects",
        ...     contacts_table_path="abfss://.../claimx_contacts",
        ...     # ... other table paths
        ... )
        >>> entity_rows = EntityRowsMessage(projects=[...], contacts=[...])
        >>> await writer.write_all(entity_rows)
    """

    def __init__(
        self,
        projects_table_path: str,
        contacts_table_path: str,
        media_table_path: str,
        tasks_table_path: str,
        task_templates_table_path: str,
        external_links_table_path: str,
        video_collab_table_path: str,
    ):
        """
        Initialize ClaimX entity writer with table paths.

        Args:
            projects_table_path: Full abfss:// path to claimx_projects table
            contacts_table_path: Full abfss:// path to claimx_contacts table
            media_table_path: Full abfss:// path to claimx_attachment_metadata table
            tasks_table_path: Full abfss:// path to claimx_tasks table
            task_templates_table_path: Full abfss:// path to claimx_task_templates table
            external_links_table_path: Full abfss:// path to claimx_external_links table
            video_collab_table_path: Full abfss:// path to claimx_video_collab table
        """
        self.logger = get_logger(self.__class__.__name__)

        # Create individual writers for each entity table
        # Projects and Media are partitioned by project_id
        # Others use the default (no partitioning)
        self._writers: Dict[str, BaseDeltaWriter] = {
            "projects": BaseDeltaWriter(
                table_path=projects_table_path,
                partition_column="project_id",
            ),
            "contacts": BaseDeltaWriter(
                table_path=contacts_table_path,
            ),
            "media": BaseDeltaWriter(
                table_path=media_table_path,
                partition_column="project_id",
            ),
            "tasks": BaseDeltaWriter(
                table_path=tasks_table_path,
            ),
            "task_templates": BaseDeltaWriter(
                table_path=task_templates_table_path,
            ),
            "external_links": BaseDeltaWriter(
                table_path=external_links_table_path,
            ),
            "video_collab": BaseDeltaWriter(
                table_path=video_collab_table_path,
            ),
        }

        self.logger.info(
            "Initialized ClaimXEntityWriter",
            extra={
                "tables": list(self._writers.keys()),
            },
        )

    async def write_all(self, entity_rows: EntityRowsMessage) -> Dict[str, int]:
        """
        Write all entity rows to their respective Delta tables.

        Uses merge (upsert) operations for all tables except contacts (append-only).

        Args:
            entity_rows: EntityRowsMessage with data for each table

        Returns:
            Dict mapping table name to rows written
        """
        counts: Dict[str, int] = {}

        # Process each entity type
        if entity_rows.projects:
            result = await self._write_table(
                "projects",
                entity_rows.projects,
            )
            if result is not None:
                counts["projects"] = result

        if entity_rows.contacts:
            result = await self._write_table(
                "contacts",
                entity_rows.contacts,
            )
            if result is not None:
                counts["contacts"] = result

        if entity_rows.media:
            result = await self._write_table(
                "media",
                entity_rows.media,
            )
            if result is not None:
                counts["media"] = result

        if entity_rows.tasks:
            result = await self._write_table(
                "tasks",
                entity_rows.tasks,
            )
            if result is not None:
                counts["tasks"] = result

        if entity_rows.task_templates:
            result = await self._write_table(
                "task_templates",
                entity_rows.task_templates,
            )
            if result is not None:
                counts["task_templates"] = result

        if entity_rows.external_links:
            result = await self._write_table(
                "external_links",
                entity_rows.external_links,
            )
            if result is not None:
                counts["external_links"] = result

        if entity_rows.video_collab:
            result = await self._write_table(
                "video_collab",
                entity_rows.video_collab,
            )
            if result is not None:
                counts["video_collab"] = result

        self.logger.info(
            f"Write cycle complete: {sum(counts.values())} total rows across {len(counts)} tables",
            extra={
                "tables_written": counts,
                "total_rows": sum(counts.values()),
                "table_count": len(counts),
            },
        )

        return counts

    async def _write_table(
        self,
        table_name: str,
        rows: List[Dict[str, Any]],
    ) -> Optional[int]:
        """
        Write rows to a specific entity table using merge or append.

        Args:
            table_name: Name of the entity table
            rows: List of row dicts to write

        Returns:
            Number of rows affected, or None on error
        """
        if not rows:
            return 0

        self.logger.debug(
            f"Writing {len(rows)} rows to {table_name}",
            extra={
                "table_name": table_name,
                "row_count": len(rows),
            },
        )

        writer = self._writers.get(table_name)
        if not writer:
            self.logger.warning(
                f"No writer configured for table: {table_name}",
                extra={"table_name": table_name},
            )
            return None

        merge_keys = MERGE_KEYS.get(table_name)
        if not merge_keys:
            self.logger.warning(
                f"No merge keys defined for table: {table_name}",
                extra={"table_name": table_name},
            )
            return None

        try:
            # Create DataFrame from rows with explicit schema to prevent Polars
            # schema inference issues with mixed None/value columns
            df = self._create_dataframe_with_schema(table_name, rows)

            # Ensure created_at and updated_at have values (fill nulls with current time)
            # Delta tables declare these as non-nullable, so we must provide values
            now = datetime.now(timezone.utc)
            if "created_at" not in df.columns:
                df = df.with_columns(pl.lit(now).alias("created_at"))
            else:
                df = df.with_columns(pl.col("created_at").fill_null(now))
            if "updated_at" not in df.columns:
                df = df.with_columns(pl.lit(now).alias("updated_at"))
            else:
                df = df.with_columns(pl.col("updated_at").fill_null(now))

            # Contacts: append-only (new contacts from new projects/events)
            # Media: append-only (new media from new events)
            # Other tables: merge (upsert)
            # Note: Deduplication handled by daily Fabric maintenance job
            if table_name == "contacts":
                # Contacts are append-only
                success = await writer._async_append(df)
                rows_affected = len(df) if success else 0
            elif table_name == "media":
                # Media is append-only
                success = await writer._async_append(df)
                rows_affected = len(df) if success else 0
            elif table_name == "task_templates":
                # Task templates: merge only when modified_date has changed
                # This prevents unnecessary updates when template data hasn't changed
                success = await writer._async_merge(
                    df,
                    merge_keys=merge_keys,
                    preserve_columns=["created_at"],
                    update_condition="source.modified_date <> target.modified_date OR target.modified_date IS NULL",
                )
                rows_affected = len(df) if success else 0
            else:
                # Other tables use merge (upsert)
                # Preserve created_at on updates
                success = await writer._async_merge(
                    df,
                    merge_keys=merge_keys,
                    preserve_columns=["created_at"],
                )
                rows_affected = len(df) if success else 0

            if success:
                self.logger.info(
                    f"Wrote {rows_affected} rows to {table_name}",
                    extra={
                        "table_name": table_name,
                        "rows_written": rows_affected,
                    },
                )
                return rows_affected
            else:
                self.logger.error(
                    f"{table_name} table write failed",
                    extra={
                        "table_name": table_name,
                        "row_count": len(rows),
                    },
                )
                return None

        except Exception as e:
            self.logger.error(
                f"Error writing to {table_name} table",
                extra={
                    "table_name": table_name,
                    "row_count": len(rows),
                    "error": str(e),
                },
                exc_info=True,
            )
            return None

    def _create_dataframe_with_schema(
        self, table_name: str, rows: List[Dict[str, Any]]
    ) -> pl.DataFrame:
        """
        Create DataFrame with explicit schema to prevent type inference issues.

        Polars infers schema from first N rows. If columns are None in early rows
        and then actual values appear (especially numeric-looking strings like
        phone numbers or zip codes), schema inference can fail. Using explicit
        schema avoids this class of errors for all entity tables.

        Also handles conversion of ISO format timestamp strings to native datetime/date
        objects. This is necessary because data arrives as strings after Kafka/JSON
        serialization, but the schema expects native Polars datetime/date types.

        Args:
            table_name: Name of the entity table
            rows: List of row dicts

        Returns:
            DataFrame with correct schema
        """
        if not rows:
            return pl.DataFrame(rows)

        # Get schema definition for this table
        table_schema = TABLE_SCHEMAS.get(table_name)
        if not table_schema:
            # No schema defined, use default Polars inference
            return pl.DataFrame(rows)

        # Get all column names from the rows
        all_columns = set()
        for row in rows:
            all_columns.update(row.keys())

        # Build schema for columns that exist and have a defined type
        schema = {}
        for col in all_columns:
            if col in table_schema:
                schema[col] = table_schema[col]

        # Pre-convert ISO timestamp/date strings to native Python types
        # This is required because Polars cannot coerce strings to datetime/date
        # when using explicit schemas, and data arrives as strings after Kafka
        # JSON serialization (datetime objects become ISO format strings)
        converted_rows = []
        for row in rows:
            converted_row = dict(row)
            for col_name, col_type in schema.items():
                if col_name in converted_row and converted_row[col_name] is not None:
                    val = converted_row[col_name]
                    if col_type == pl.Datetime("us", "UTC") and isinstance(val, str):
                        # Parse ISO timestamp string (e.g., "2026-01-09T01:58:32.556819Z")
                        # Handle both "Z" suffix and "+00:00" timezone formats
                        converted_row[col_name] = datetime.fromisoformat(
                            val.replace("Z", "+00:00")
                        )
                    elif col_type == pl.Date and isinstance(val, str):
                        # Parse date string (e.g., "2026-01-09" or from ISO timestamp)
                        if "T" in val:
                            # ISO timestamp string - extract date part
                            converted_row[col_name] = datetime.fromisoformat(
                                val.replace("Z", "+00:00")
                            ).date()
                        else:
                            # Plain date string
                            converted_row[col_name] = date.fromisoformat(val)
            converted_rows.append(converted_row)

        return pl.DataFrame(converted_rows, schema=schema)


__all__ = ["ClaimXEntityWriter", "MERGE_KEYS", "TABLE_SCHEMAS"]
