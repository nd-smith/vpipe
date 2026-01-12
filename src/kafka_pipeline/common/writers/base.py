"""
Base class for Delta table writers.

Provides common functionality for all Delta writers:
- Async wrapper methods for non-blocking operations
- Common error handling and logging patterns
- Initialization of underlying DeltaTableWriter
- Support for in-memory Delta tables in dev mode

Subclasses should inherit from BaseDeltaWriter and implement domain-specific
data transformation methods.

Dev Mode (In-Memory Delta):
    Set USE_INMEMORY_DELTA=true environment variable to use in-memory tables
    instead of real Delta/OneLake storage. Useful for local testing with
    the dummy data source.

    Example:
        USE_INMEMORY_DELTA=true python -m kafka_pipeline --worker xact-delta-writer --dev
"""

import asyncio
import os
from typing import List, Optional, Union

import polars as pl

from core.logging.setup import get_logger
from kafka_pipeline.common.storage.delta import DeltaTableWriter


# Global registry for in-memory tables (shared across workers in same process)
_inmemory_registry = None


def _get_inmemory_registry():
    """Get or create the global in-memory Delta registry."""
    global _inmemory_registry
    if _inmemory_registry is None:
        from kafka_pipeline.common.storage.inmemory_delta import InMemoryDeltaRegistry
        _inmemory_registry = InMemoryDeltaRegistry()
    return _inmemory_registry


def is_inmemory_mode() -> bool:
    """Check if in-memory Delta mode is enabled."""
    return os.getenv("USE_INMEMORY_DELTA", "").lower() in ("true", "1", "yes")


def get_inmemory_registry():
    """
    Get the global in-memory Delta registry for inspection.

    Useful for debugging/querying in-memory tables:
        from kafka_pipeline.common.writers.base import get_inmemory_registry
        registry = get_inmemory_registry()
        print(registry.get_stats())
        events = registry.get_table("xact_events").read()
    """
    return _get_inmemory_registry()


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
        partition_column: Optional[str] = None,
        z_order_columns: Optional[List[str]] = None,
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
        self.logger = get_logger(self.__class__.__name__)
        self._using_inmemory = is_inmemory_mode()

        if self._using_inmemory:
            # Use in-memory Delta table for dev/testing
            registry = _get_inmemory_registry()

            # Determine table name from path
            table_name = self._extract_table_name(table_path)

            # Get writer from registry (shares storage across workers)
            self._delta_writer = registry.get_writer(
                name=table_name,
                timestamp_column=timestamp_column,
                partition_column=partition_column,
                z_order_columns=z_order_columns,
            )

            self.logger.info(
                f"Initialized {self.__class__.__name__} with IN-MEMORY storage",
                extra={
                    "table_path": table_path,
                    "table_name": table_name,
                    "z_order_columns": z_order_columns,
                    "inmemory": True,
                },
            )
        else:
            # Use real Delta table
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

    @staticmethod
    def _extract_table_name(table_path: str) -> str:
        """Extract table name from path for in-memory registry."""
        path_lower = table_path.lower()

        # Common table patterns
        patterns = [
            ("xact_events", "xact_events"),
            ("xact_attachments", "xact_attachments"),
            ("claimx_events", "claimx_events"),
            ("claimx_projects", "claimx_projects"),
            ("claimx_contacts", "claimx_contacts"),
            ("claimx_media", "claimx_media"),
            ("claimx_tasks", "claimx_tasks"),
            ("claimx_task_templates", "claimx_task_templates"),
            ("claimx_external_links", "claimx_external_links"),
            ("claimx_video_collab", "claimx_video_collab"),
            ("inventory", "inventory"),
            ("failed", "failed"),
        ]

        for pattern, name in patterns:
            if pattern in path_lower:
                return name

        # Fallback: use last path component
        parts = table_path.rstrip("/").split("/")
        return parts[-1] if parts else "default"

    async def _async_append(
        self,
        df: pl.DataFrame,
        batch_id: Optional[str] = None,
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

        try:
            rows_written = await asyncio.to_thread(
                self._delta_writer.append,
                df,
                batch_id=batch_id,
            )

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
        merge_keys: List[str],
        preserve_columns: Optional[List[str]] = None,
        update_condition: Optional[str] = None,
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

        try:
            rows_affected = await asyncio.to_thread(
                self._delta_writer.merge,
                df,
                merge_keys=merge_keys,
                preserve_columns=preserve_columns,
                update_condition=update_condition,
            )

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


__all__ = ["BaseDeltaWriter", "is_inmemory_mode", "get_inmemory_registry"]
