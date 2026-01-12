"""
Delta Lake writer for attachment inventory table.

Writes download results to the xact_attachments Delta table with:
- Append-only writes for completed downloads
- Async/non-blocking writes
- Partitioned by event_date for efficient querying

This is an inventory table - it tracks where files are stored in OneLake.
"""

import asyncio
import time
from datetime import datetime, timezone
from typing import List

import polars as pl

from kafka_pipeline.common.writers.base import BaseDeltaWriter
from kafka_pipeline.xact.schemas.results import DownloadResultMessage


# Schema for xact_attachments inventory table
INVENTORY_SCHEMA = {
    "media_id": pl.Utf8,
    "trace_id": pl.Utf8,
    "attachment_url": pl.Utf8,
    "blob_path": pl.Utf8,
    "file_type": pl.Utf8,
    "status_subtype": pl.Utf8,
    "assignment_id": pl.Utf8,
    "bytes_downloaded": pl.Int64,
    "downloaded_at": pl.Datetime(time_zone="UTC"),
    "created_at": pl.Datetime(time_zone="UTC"),
    "event_date": pl.Date,
}


class DeltaInventoryWriter(BaseDeltaWriter):
    """
    Writer for xact_attachments Delta table (append-only inventory).

    This is an inventory table tracking where files are stored in OneLake.
    Only completed downloads are written here.

    Schema:
    - media_id: Unique deterministic ID for the attachment
    - trace_id: Unique event identifier
    - attachment_url: Source URL of the file
    - blob_path: Destination path in OneLake
    - file_type: File extension (e.g., "pdf", "esx")
    - status_subtype: Event type suffix (e.g., "documentsReceived")
    - assignment_id: Assignment ID from event
    - bytes_downloaded: Size of downloaded file
    - downloaded_at: When the file was downloaded
    - created_at: When the record was created
    - event_date: Date partition column

    Usage:
        >>> writer = DeltaInventoryWriter(table_path="abfss://.../xact_attachments")
        >>> await writer.write_results([result1, result2, result3])
    """

    def __init__(self, table_path: str):
        """
        Initialize Delta inventory writer.

        Args:
            table_path: Full abfss:// path to xact_attachments Delta table
        """
        super().__init__(
            table_path=table_path,
            partition_column="event_date",
        )

    async def write_result(self, result: DownloadResultMessage) -> bool:
        """
        Write a single download result to Delta table (non-blocking).

        Args:
            result: DownloadResultMessage to write

        Returns:
            True if write succeeded, False otherwise
        """
        return await self.write_results([result])

    async def write_results(self, results: List[DownloadResultMessage]) -> bool:
        """
        Write multiple download results to Delta table (non-blocking, append-only).

        Converts DownloadResultMessage objects to DataFrame and appends to Delta.
        Uses asyncio.to_thread to avoid blocking the event loop.

        Args:
            results: List of DownloadResultMessage objects to write

        Returns:
            True if write succeeded, False otherwise
        """
        if not results:
            return True

        batch_size = len(results)
        start_time = time.monotonic()

        try:
            # Run synchronous DataFrame conversion in thread to avoid blocking event loop.
            # This is critical: blocking the event loop would prevent Kafka heartbeats,
            # causing consumer group rebalancing.
            df = await asyncio.to_thread(self._results_to_dataframe, results)

            # Use base class async append method
            success = await self._async_append(df)

            # Calculate latency metric
            latency_ms = (time.monotonic() - start_time) * 1000

            if success:
                self.logger.info(
                    "Successfully wrote results to Delta inventory",
                    extra={
                        "batch_size": batch_size,
                        "latency_ms": round(latency_ms, 2),
                        "table_path": self.table_path,
                    },
                )

            return success

        except Exception as e:
            latency_ms = (time.monotonic() - start_time) * 1000

            self.logger.error(
                "Failed to write results to Delta inventory",
                extra={
                    "batch_size": batch_size,
                    "latency_ms": round(latency_ms, 2),
                    "table_path": self.table_path,
                    "error": str(e),
                },
                exc_info=True,
            )
            return False

    def _results_to_dataframe(self, results: List[DownloadResultMessage]) -> pl.DataFrame:
        """
        Convert DownloadResultMessage objects to Polars DataFrame.

        Schema for xact_attachments inventory table:
        - media_id: str
        - trace_id: str
        - attachment_url: str
        - blob_path: str
        - file_type: str
        - status_subtype: str
        - assignment_id: str
        - bytes_downloaded: int
        - downloaded_at: datetime
        - created_at: datetime
        - event_date: date (partition column)

        Args:
            results: List of DownloadResultMessage objects

        Returns:
            Polars DataFrame with xact_attachments schema
        """
        now = datetime.now(timezone.utc)
        today = now.date()

        rows = []
        for result in results:
            rows.append({
                "media_id": result.media_id,
                "trace_id": result.trace_id,
                "attachment_url": result.attachment_url,
                "blob_path": result.blob_path,
                "file_type": result.file_type,
                "status_subtype": result.status_subtype,
                "assignment_id": result.assignment_id,
                "bytes_downloaded": result.bytes_downloaded,
                "downloaded_at": result.created_at,
                "created_at": now,
                "event_date": today,
            })

        # Create DataFrame with explicit schema
        df = pl.DataFrame(rows, schema=INVENTORY_SCHEMA)

        return df


class DeltaFailedAttachmentsWriter(BaseDeltaWriter):
    """
    Writer for xact_attachments_failed Delta table.

    Records permanent download failures for tracking and potential replay.

    Features:
    - Merge on media_id for idempotency
    - Non-blocking writes using asyncio.to_thread
    - Captures error details for debugging and replay decisions

    Usage:
        >>> writer = DeltaFailedAttachmentsWriter(table_path="abfss://.../xact_attachments_failed")
        >>> await writer.write_results([failed_result1, failed_result2])
    """

    def __init__(self, table_path: str):
        """
        Initialize Delta failed attachments writer.

        Args:
            table_path: Full abfss:// path to xact_attachments_failed Delta table
        """
        # Initialize base class with z_order columns
        super().__init__(
            table_path=table_path,
            z_order_columns=["media_id", "failed_at"],
        )

    async def write_result(self, result: DownloadResultMessage) -> bool:
        """
        Write a single failed result to Delta table (non-blocking).

        Args:
            result: DownloadResultMessage with failed_permanent status

        Returns:
            True if write succeeded, False otherwise
        """
        return await self.write_results([result])

    async def write_results(self, results: List[DownloadResultMessage]) -> bool:
        """
        Write multiple failed results to Delta table (non-blocking).

        Converts DownloadResultMessage objects to DataFrame and merges into Delta.
        Uses asyncio.to_thread to avoid blocking the event loop.

        Merge strategy:
        - Match on media_id for idempotency (unique per attachment)
        - UPDATE existing rows with new data (e.g., if replayed and failed again)
        - INSERT new rows that don't match

        Args:
            results: List of DownloadResultMessage objects to write

        Returns:
            True if write succeeded, False otherwise
        """
        if not results:
            return True

        batch_size = len(results)
        start_time = time.monotonic()

        try:
            # Run synchronous DataFrame conversion in thread to avoid blocking event loop.
            # This is critical: blocking the event loop would prevent Kafka heartbeats,
            # causing consumer group rebalancing.
            df = await asyncio.to_thread(self._results_to_dataframe, results)

            # Use base class async merge method
            # Merge on media_id for idempotency
            success = await self._async_merge(
                df,
                merge_keys=["media_id"],
                preserve_columns=["created_at"],
            )

            # Calculate latency metric
            latency_ms = (time.monotonic() - start_time) * 1000

            if success:
                self.logger.info(
                    "Successfully wrote failed results to Delta",
                    extra={
                        "batch_size": batch_size,
                        "latency_ms": round(latency_ms, 2),
                        "table_path": self.table_path,
                    },
                )

            return success

        except Exception as e:
            latency_ms = (time.monotonic() - start_time) * 1000

            self.logger.error(
                "Failed to write failed results to Delta",
                extra={
                    "batch_size": batch_size,
                    "latency_ms": round(latency_ms, 2),
                    "table_path": self.table_path,
                    "error": str(e),
                },
                exc_info=True,
            )
            return False

    def _results_to_dataframe(self, results: List[DownloadResultMessage]) -> pl.DataFrame:
        """
        Convert failed DownloadResultMessage objects to Polars DataFrame.

        Schema for xact_attachments_failed table:
        - media_id: str
        - trace_id: str
        - attachment_url: str
        - error_message: str
        - status: str (failed_permanent)
        - failed_at: datetime (created_at from result)
        - retry_count: int
        - http_status: int (optional)
        - created_at: datetime (current UTC time)

        Args:
            results: List of DownloadResultMessage objects

        Returns:
            Polars DataFrame with xact_attachments_failed schema
        """
        # Current time for failed tracking
        now = datetime.now(timezone.utc)

        # Convert to list of dicts matching table schema
        rows = []
        for result in results:
            row = {
                "media_id": result.media_id,
                "trace_id": result.trace_id,
                "attachment_url": result.attachment_url,
                "error_message": result.error_message or "Unknown error",
                "status": result.status,
                "failed_at": result.created_at,
                "retry_count": result.retry_count,
                "http_status": result.http_status,
                "created_at": now,
            }
            rows.append(row)

        # Create DataFrame with explicit schema
        df = pl.DataFrame(
            rows,
            schema={
                "media_id": pl.Utf8,
                "trace_id": pl.Utf8,
                "attachment_url": pl.Utf8,
                "error_message": pl.Utf8,
                "status": pl.Utf8,
                "failed_at": pl.Datetime(time_zone="UTC"),
                "retry_count": pl.Int64,
                "http_status": pl.Int64,
                "created_at": pl.Datetime(time_zone="UTC"),
            },
        )

        return df


__all__ = ["DeltaInventoryWriter", "DeltaFailedAttachmentsWriter"]
