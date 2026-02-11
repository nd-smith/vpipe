"""
Base class for Delta table writers.

Provides common functionality for all Delta writers:
- Async wrapper methods for non-blocking operations
- Common error handling and logging patterns
- Initialization of underlying DeltaTableWriter

Subclasses should inherit from BaseDeltaWriter and implement domain-specific
data transformation methods.
"""

import asyncio
import logging
import time

import polars as pl

from pipeline.common.metrics import delta_write_duration_seconds
from pipeline.common.storage.delta import DeltaTableWriter


class BaseDeltaWriter:
    """
    Base class for Delta table writers with async support.

    Wraps verisk_pipeline.storage.delta.DeltaTableWriter to provide:
    - Async/non-blocking writes via asyncio.to_thread
    - Common error handling patterns
    - Logging integration

    Subclasses should:
    1. Call super().__init__() with table_path and optional DeltaTableWriter params
    2. Implement domain-specific data transformation methods
    3. Use _async_append() or _async_merge() for non-blocking writes

    Example:
        class MyDomainWriter(BaseDeltaWriter):
            def __init__(self, table_path: str):
                super().__init__(
                    table_path=table_path
                )

            async def write_records(self, records: List[Dict]) -> bool:
                df = self._records_to_dataframe(records)
                return await self._async_append(df)
    """

    def __init__(
        self,
        table_path: str,
        timestamp_column: str = "ingested_at",
        partition_column: str | None = None,
        z_order_columns: list[str] | None = None,
    ):
        """
        Initialize base Delta writer.

        No deduplication at write time - duplicates are handled by:
        1. Download worker's in-memory cache (for downloads)
        2. Daily maintenance job (for events tables)

        Args:
            table_path: Full abfss:// path to Delta table
            timestamp_column: Column used for time-based operations (default: "ingested_at")
            partition_column: Column used for partitioning (default: None, no partitioning)
            z_order_columns: Columns for Z-ordering optimization (optional)
        """
        self.table_path = table_path
        self.logger = logging.getLogger(self.__class__.__name__)

        self._delta_writer = DeltaTableWriter(
            table_path=table_path,
            timestamp_column=timestamp_column,
            partition_column=partition_column,
            z_order_columns=z_order_columns or [],
        )

        self.logger.info(
            f"Initialized {self.__class__.__name__}",
            extra={
                "table_path": table_path,
                "z_order_columns": z_order_columns,
            },
        )

    async def _async_append(
        self,
        df: pl.DataFrame,
        batch_id: str | None = None,
    ) -> bool:
        """
        Append DataFrame to Delta table (non-blocking).

        Uses asyncio.to_thread to avoid blocking the event loop.

        Args:
            df: Polars DataFrame to append
            batch_id: Optional short identifier for log correlation

        Returns:
            True if write succeeded, False otherwise
        """
        if df.is_empty():
            return True

        table_name = self.table_path.rstrip("/").split("/")[-1]

        try:
            start = time.monotonic()
            rows_written = await asyncio.to_thread(
                self._delta_writer.append,
                df,
                batch_id=batch_id,
            )
            delta_write_duration_seconds.labels(
                table=table_name, operation="append"
            ).observe(time.monotonic() - start)

            self.logger.info(
                "Successfully appended to Delta table",
                extra={
                    "batch_id": batch_id,
                    "rows_written": rows_written,
                    "table_path": self.table_path,
                },
            )
            return True

        except Exception as e:
            self._last_append_error = e
            self.logger.error(
                "Failed to append to Delta table",
                extra={
                    "batch_id": batch_id,
                    "table_path": self.table_path,
                    "error": str(e),
                    "row_count": len(df),
                },
                exc_info=True,
            )
            return False

    async def _async_merge(
        self,
        df: pl.DataFrame,
        merge_keys: list[str],
        preserve_columns: list[str] | None = None,
        update_condition: str | None = None,
    ) -> bool:
        """
        Merge DataFrame into Delta table (non-blocking upsert).

        Uses asyncio.to_thread to avoid blocking the event loop.

        Args:
            df: Polars DataFrame to merge
            merge_keys: Columns forming primary key for merge
            preserve_columns: Columns to preserve on update (default: ["created_at"])
            update_condition: Optional SQL predicate for when to update matched rows.
                              E.g., "source.modified_date > target.modified_date" to only
                              update when the source has a newer modified_date.

        Returns:
            True if merge succeeded, False otherwise
        """
        if df.is_empty():
            return True

        table_name = self.table_path.rstrip("/").split("/")[-1]

        try:
            start = time.monotonic()
            rows_affected = await asyncio.to_thread(
                self._delta_writer.merge,
                df,
                merge_keys=merge_keys,
                preserve_columns=preserve_columns,
                update_condition=update_condition,
            )
            delta_write_duration_seconds.labels(
                table=table_name, operation="merge"
            ).observe(time.monotonic() - start)

            self.logger.info(
                "Successfully merged into Delta table",
                extra={
                    "rows_affected": rows_affected,
                    "merge_keys": merge_keys,
                    "table_path": self.table_path,
                },
            )
            return True

        except Exception as e:
            self.logger.error(
                "Failed to merge into Delta table",
                extra={
                    "table_path": self.table_path,
                    "merge_keys": merge_keys,
                    "error": str(e),
                    "row_count": len(df),
                },
                exc_info=True,
            )
            return False


__all__ = ["BaseDeltaWriter"]
