"""Delta Lake writer for ClaimX events table.

Writes events to claimx_events with async support. Deduplication handled by daily Fabric job.
"""

from datetime import UTC, datetime
from typing import Any

import polars as pl

from pipeline.common.writers.base import BaseDeltaWriter

# Explicit schema for claimx_events table matching actual Delta table schema
# This ensures type compatibility and prevents inference issues
EVENTS_SCHEMA = {
    "trace_id": pl.Utf8,
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
    """Writer for claimx_events Delta table with async support.

    Writes events directly from Eventhouse rows without flattening.
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
        super().__init__(
            table_path=table_path,
            timestamp_column="ingested_at",
            partition_column="event_date",
            z_order_columns=["project_id"],
        )

    @staticmethod
    def _normalize_ingested_at(value: Any, fallback: datetime) -> datetime:
        """Normalize ingested_at to a timezone-aware datetime."""
        if value is None:
            return fallback
        if isinstance(value, str):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        if isinstance(value, datetime):
            return value.replace(tzinfo=UTC) if value.tzinfo is None else value
        return fallback

    def _preprocess_events(self, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Pre-process events to ensure correct types before DataFrame creation."""
        now = datetime.now(UTC)
        processed = []
        for event in events:
            ingested_at = self._normalize_ingested_at(event.get("ingested_at"), now)
            processed.append({
                "trace_id": event.get("trace_id"),
                "event_type": event.get("event_type"),
                "project_id": event.get("project_id"),
                "media_id": event.get("media_id"),
                "task_assignment_id": event.get("task_assignment_id"),
                "video_collaboration_id": event.get("video_collaboration_id"),
                "master_file_name": event.get("master_file_name"),
                "created_at": now,
                "ingested_at": ingested_at,
                "event_date": ingested_at.date(),
            })
        return processed

    async def write_events(self, events: list[dict[str, Any]]) -> bool:
        """
        Write ClaimX events to Delta table (non-blocking).

        Args:
            events: List of event dicts from ClaimXEventMessage.model_dump():
                - trace_id: Unique trace identifier
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
            processed_events = self._preprocess_events(events)

            valid_events = [
                e for e in processed_events
                if e.get("trace_id") is not None and e.get("event_type") is not None
            ]

            if len(valid_events) < len(processed_events):
                self.logger.debug(
                    "Dropped events with null trace_id or event_type",
                    extra={
                        "dropped_count": len(processed_events) - len(valid_events),
                        "remaining_count": len(valid_events),
                    },
                )

            if not valid_events:
                self.logger.debug("No valid events to write after filtering nulls")
                return True

            df = pl.DataFrame(valid_events, schema=EVENTS_SCHEMA)

            self.logger.info(
                "ClaimX events prepared for write",
                extra={
                    "event_count": len(events),
                    "valid_count": len(valid_events),
                    "table_path": self.table_path,
                    "columns": df.columns,
                    "sample_event": valid_events[0] if valid_events else {},
                },
            )

            success = await self._async_append(df)

            if success:
                self.logger.info(
                    "Successfully wrote ClaimX events to Delta",
                    extra={
                        "event_count": len(events),
                        "valid_count": len(valid_events),
                        "columns": len(df.columns),
                        "table_path": self.table_path,
                    },
                )
            else:
                self.logger.error(
                    "Failed to write ClaimX events to Delta",
                    extra={
                        "event_count": len(events),
                        "valid_count": len(valid_events),
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
