"""
Generic Delta table writer for simple write operations.

Provides a ready-to-use writer for cases where domain-specific
transformations are not needed. Simply pass DataFrames or records to write.
"""

from typing import Any, Dict, List, Optional

import polars as pl

from kafka_pipeline.common.writers.base import BaseDeltaWriter


class DeltaWriter(BaseDeltaWriter):
    """
    Generic Delta table writer with async support.

    Use this writer when you have a DataFrame ready to write and don't need
    domain-specific transformations. For domain-specific logic (e.g., events,
    inventory), create a custom writer that inherits from BaseDeltaWriter.

    Features:
    - Direct DataFrame append/merge operations
    - Non-blocking writes via asyncio
    - Z-ordering optimization

    Usage:
        # Simple append
        writer = DeltaWriter(table_path="abfss://.../my_table")
        await writer.write_dataframe(df)

        # Merge/upsert
        writer = DeltaWriter(
            table_path="abfss://.../my_table",
            z_order_columns=["id", "created_at"]
        )
        await writer.merge_dataframe(df, merge_keys=["id"])
    """

    async def write_dataframe(
        self,
        df: pl.DataFrame,
    ) -> bool:
        """
        Write DataFrame to Delta table using append mode.

        Args:
            df: Polars DataFrame to write

        Returns:
            True if write succeeded, False otherwise
        """
        return await self._async_append(df)

    async def merge_dataframe(
        self,
        df: pl.DataFrame,
        merge_keys: List[str],
        preserve_columns: Optional[List[str]] = None,
    ) -> bool:
        """
        Merge DataFrame into Delta table (upsert operation).

        Args:
            df: Polars DataFrame to merge
            merge_keys: Columns forming primary key for merge
            preserve_columns: Columns to preserve on update (default: ["created_at"])

        Returns:
            True if merge succeeded, False otherwise
        """
        return await self._async_merge(
            df,
            merge_keys=merge_keys,
            preserve_columns=preserve_columns,
        )

    async def write_records(
        self,
        records: List[Dict[str, Any]],
    ) -> bool:
        """
        Write records (list of dicts) to Delta table.

        Convenience method that converts records to DataFrame before writing.

        Args:
            records: List of dictionaries to write

        Returns:
            True if write succeeded, False otherwise
        """
        if not records:
            return True

        df = pl.DataFrame(records)
        return await self._async_append(df)

    async def merge_records(
        self,
        records: List[Dict[str, Any]],
        merge_keys: List[str],
        preserve_columns: Optional[List[str]] = None,
    ) -> bool:
        """
        Merge records (list of dicts) into Delta table.

        Convenience method that converts records to DataFrame before merging.

        Args:
            records: List of dictionaries to merge
            merge_keys: Columns forming primary key for merge
            preserve_columns: Columns to preserve on update (default: ["created_at"])

        Returns:
            True if merge succeeded, False otherwise
        """
        if not records:
            return True

        df = pl.DataFrame(records)
        return await self._async_merge(
            df,
            merge_keys=merge_keys,
            preserve_columns=preserve_columns,
        )


__all__ = ["DeltaWriter"]
