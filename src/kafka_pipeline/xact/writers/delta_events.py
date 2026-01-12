"""
Delta Lake writer for event analytics table.

Writes events to the xact_events Delta table with:
- Flattening of nested JSON structures using xact transform module
- Async/non-blocking writes
- Schema compatibility with legacy xact_events table

Uses flatten_events() from kafka_pipeline.xact.writers.transform.

Note: Deduplication handled by daily Fabric maintenance job.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import polars as pl

from kafka_pipeline.common.writers.base import BaseDeltaWriter
from kafka_pipeline.xact.writers.transform import flatten_events


class DeltaEventsWriter(BaseDeltaWriter):
    """
    Writer for xact_events Delta table with async support.

    Uses flatten_events() from kafka_pipeline.xact.writers.transform to transform
    raw Eventhouse rows into the correct xact_events schema with all 28 columns.

    Features:
    - Flattening of nested JSON using xact transform module
    - Non-blocking writes using asyncio.to_thread
    - Schema compatibility with legacy xact_events table
    - Deduplication handled by daily Fabric maintenance job

    Input format (raw Eventhouse rows):
        - type: Full event type string (e.g., "verisk.claims.property.xn.documentsReceived")
        - version: Event version
        - utcDateTime: Event timestamp
        - traceId: Trace identifier
        - data: JSON string with nested event data

    Output schema matches kafka_pipeline.xact.writers.transform.FLATTENED_SCHEMA:
        - type, status_subtype, version, ingested_at, event_date, trace_id
        - description, assignment_id, original_assignment_id, xn_address, carrier_id
        - estimate_version, note, author, sender_reviewer_name, sender_reviewer_email
        - carrier_reviewer_name, carrier_reviewer_email, event_datetime_mdt
        - attachments (comma-joined), claim_number, contact_* fields, raw_json

    Usage:
        >>> writer = DeltaEventsWriter(table_path="abfss://.../xact_events")
        >>> await writer.write_raw_events([{"type": "...", "traceId": "...", ...}])
    """

    def __init__(
        self,
        table_path: str,
    ):
        """
        Initialize Delta events writer.

        Args:
            table_path: Full abfss:// path to xact_events Delta table
        """
        # Initialize base class
        super().__init__(
            table_path=table_path,
            timestamp_column="created_at",
            partition_column="event_date",
            z_order_columns=["event_date", "trace_id", "event_id", "type"],
        )

    async def write_raw_events(
        self,
        raw_events: List[Dict[str, Any]],
        batch_id: Optional[str] = None,
    ) -> bool:
        """
        Write raw Eventhouse events to Delta table (non-blocking).

        Transforms raw Eventhouse rows using flatten_events() from verisk_pipeline
        to ensure schema compatibility with xact_events table.

        Args:
            raw_events: List of raw event dicts with Eventhouse schema:
                - type: Full event type string
                - version: Event version
                - utcDateTime: Event timestamp
                - traceId: Trace identifier
                - data: JSON string with nested event data
            batch_id: Optional short identifier for log correlation

        Returns:
            True if write succeeded, False otherwise
        """
        import asyncio

        if not raw_events:
            return True

        try:
            # Run synchronous transform in thread to avoid blocking event loop.
            # This is critical: flatten_events() does row-by-row JSON parsing which
            # can take 30+ seconds for large batches. Blocking the event loop would
            # prevent Kafka heartbeats, causing consumer group rebalancing.
            def _sync_transform() -> pl.DataFrame:
                raw_df = pl.DataFrame(raw_events)
                flattened_df = flatten_events(raw_df)
                now = datetime.now(timezone.utc)
                return flattened_df.with_columns([
                    pl.lit(now).alias("created_at"),
                    pl.lit(now.date()).alias("created_date"),
                ])

            flattened_df = await asyncio.to_thread(_sync_transform)

            # Use base class async append method
            success = await self._async_append(flattened_df, batch_id=batch_id)

            if success:
                self.logger.info(
                    "Successfully wrote events to Delta",
                    extra={
                        "batch_id": batch_id,
                        "event_count": len(raw_events),
                        "columns": len(flattened_df.columns),
                        "table_path": self.table_path,
                    },
                )

            return success

        except Exception as e:
            self.logger.error(
                "Failed to write events to Delta",
                extra={
                    "batch_id": batch_id,
                    "event_count": len(raw_events),
                    "table_path": self.table_path,
                    "error": str(e),
                },
                exc_info=True,
            )
            return False


__all__ = ["DeltaEventsWriter"]
