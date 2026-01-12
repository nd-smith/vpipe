"""
Delta Lake writer for ClaimX events table.

Writes ClaimX events to the claimx_events Delta table with:
- Async/non-blocking writes
- Schema compatibility with verisk_pipeline claimx_events table

Unlike xact events, ClaimX events are not flattened - they maintain
the simple event structure from Eventhouse/webhooks.

Note: Deduplication handled by daily Fabric maintenance job.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List

import polars as pl

from kafka_pipeline.common.writers.base import BaseDeltaWriter


# Explicit schema for claimx_events table matching actual Delta table schema
# This ensures type compatibility and prevents inference issues
EVENTS_SCHEMA = {
    "event_id": pl.Utf8,
    "event_type": pl.Utf8,
    "project_id": pl.Utf8,
    "media_id": pl.Utf8,
    "task_assignment_id": pl.Utf8,
    "video_collaboration_id": pl.Utf8,
    "master_file_name": pl.Utf8,
    "ingested_at": pl.Datetime("us", "UTC"),
    "created_at": pl.Datetime("us", "UTC"),
    "event_date": pl.Date,
}


class ClaimXEventsDeltaWriter(BaseDeltaWriter):
    """
    Writer for claimx_events Delta table with async support.

    ClaimX events have a simpler structure than xact events - no flattening needed.
    Events are written directly from Eventhouse rows to the Delta table.

    Features:
    - Non-blocking writes using asyncio.to_thread
    - Schema compatibility with verisk_pipeline claimx_events table
    - Deduplication handled by daily Fabric maintenance job

    Input format (raw Eventhouse rows from ClaimXEventMessage):
        - event_id: Unique event identifier
        - event_type: Event type string (e.g., "PROJECT_CREATED", "PROJECT_FILE_ADDED")
        - project_id: ClaimX project identifier
        - ingested_at: Event ingestion timestamp
        - media_id: Optional media file identifier
        - task_assignment_id: Optional task assignment identifier
        - video_collaboration_id: Optional video collaboration identifier
        - master_file_name: Optional master file name
        - raw_data: Raw event payload (JSON object) - not written to Delta

    Output schema (columns written to Delta table):
        - event_id: Unique event identifier
        - event_type: Event type string
        - project_id: ClaimX project identifier
        - media_id: Optional media file identifier
        - task_assignment_id: Optional task assignment identifier
        - video_collaboration_id: Optional video collaboration identifier
        - master_file_name: Optional master file name
        - ingested_at: Event ingestion timestamp
        - created_at: Pipeline processing timestamp
        - event_date: Date partition column (derived from ingested_at)

    Usage:
        >>> writer = ClaimXEventsDeltaWriter(table_path="abfss://.../claimx_events")
        >>> await writer.write_events([{"event_id": "...", "event_type": "...", ...}])
    """

    def __init__(
        self,
        table_path: str,
    ):
        """
        Initialize ClaimX events writer.

        Args:
            table_path: Full abfss:// path to claimx_events Delta table
        """
        # Initialize base class
        super().__init__(
            table_path=table_path,
            timestamp_column="ingested_at",
            partition_column="event_date",
            z_order_columns=["project_id"],
        )

    async def write_events(self, events: List[Dict[str, Any]]) -> bool:
        """
        Write ClaimX events to Delta table (non-blocking).

        Args:
            events: List of event dicts from ClaimXEventMessage.model_dump():
                - event_id: Unique event identifier
                - event_type: Event type string
                - project_id: ClaimX project ID
                - ingested_at: Event ingestion timestamp
                - media_id: Optional media file ID
                - task_assignment_id: Optional task assignment ID
                - video_collaboration_id: Optional video collaboration ID
                - master_file_name: Optional master file name
                - raw_data: Optional raw event payload (excluded from Delta write)

        Returns:
            True if write succeeded, False otherwise
        """
        if not events:
            return True

        try:
            now = datetime.now(timezone.utc)

            # Pre-process events to ensure correct types before DataFrame creation
            # This is more reliable than post-hoc type conversion
            processed_events = []
            for event in events:
                processed = {
                    "event_id": event.get("event_id"),
                    "event_type": event.get("event_type"),
                    "project_id": event.get("project_id"),
                    "media_id": event.get("media_id"),
                    "task_assignment_id": event.get("task_assignment_id"),
                    "video_collaboration_id": event.get("video_collaboration_id"),
                    "master_file_name": event.get("master_file_name"),
                    "created_at": now,
                }

                # Parse ingested_at - handle string, datetime, or None
                ingested_at = event.get("ingested_at")
                if ingested_at is None:
                    processed["ingested_at"] = now
                elif isinstance(ingested_at, str):
                    # Parse ISO format string
                    processed["ingested_at"] = datetime.fromisoformat(
                        ingested_at.replace("Z", "+00:00")
                    )
                elif isinstance(ingested_at, datetime):
                    # Ensure timezone-aware
                    if ingested_at.tzinfo is None:
                        processed["ingested_at"] = ingested_at.replace(tzinfo=timezone.utc)
                    else:
                        processed["ingested_at"] = ingested_at
                else:
                    processed["ingested_at"] = now

                # Derive event_date from ingested_at
                processed["event_date"] = processed["ingested_at"].date()

                processed_events.append(processed)

            # Filter out events with null event_id or event_type before creating DataFrame
            valid_events = [
                e for e in processed_events
                if e.get("event_id") is not None and e.get("event_type") is not None
            ]

            if len(valid_events) < len(processed_events):
                self.logger.warning(
                    "Dropped events with null event_id or event_type",
                    extra={
                        "dropped_count": len(processed_events) - len(valid_events),
                        "remaining_count": len(valid_events),
                    },
                )

            if not valid_events:
                self.logger.warning("No valid events to write after filtering nulls")
                return True

            # Create DataFrame with explicit schema for type safety
            df = pl.DataFrame(valid_events, schema=EVENTS_SCHEMA)

            # Use base class async append method
            success = await self._async_append(df)

            if success:
                self.logger.info(
                    "Successfully wrote ClaimX events to Delta",
                    extra={
                        "event_count": len(events),
                        "columns": len(df.columns),
                        "table_path": self.table_path,
                    },
                )

            return success

        except Exception as e:
            self.logger.error(
                "Failed to write ClaimX events to Delta",
                extra={
                    "event_count": len(events),
                    "table_path": self.table_path,
                    "error": str(e),
                },
                exc_info=True,
            )
            return False


__all__ = ["ClaimXEventsDeltaWriter"]
