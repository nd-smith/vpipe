"""
In-memory Delta table implementation for testing without external dependencies.

Provides a drop-in replacement for DeltaTableWriter that stores data in Polars
DataFrames, enabling full end-to-end testing of the pipeline without Azure/OneLake.

Features:
- Same interface as DeltaTableWriter (append, merge, read operations)
- Polars DataFrame storage for accurate schema validation
- Schema evolution support (adding new columns)
- Partition tracking (logical, in-memory)
- Read-back capability for test assertions
- Write history for debugging/verification

Usage:
    # Direct usage
    table = InMemoryDeltaTable("test://events", partition_column="event_date")
    table.append(df)
    result = table.read()

    # As DeltaTableWriter replacement via InMemoryDeltaTableWriter
    writer = InMemoryDeltaTableWriter("test://events")
    writer.append(df)  # Same interface as production DeltaTableWriter
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import polars as pl

logger = logging.getLogger(__name__)


@dataclass
class WriteRecord:
    """Record of a single write operation for audit/debugging."""

    operation: str  # "append" or "merge"
    rows_affected: int
    batch_id: Optional[str]
    timestamp: datetime
    columns: List[str]
    schema_changes: List[str] = field(default_factory=list)


class InMemoryDeltaTable:
    """
    In-memory Delta table for testing without external dependencies.

    Mirrors the behavior of a real Delta table while storing all data in memory
    using Polars DataFrames. This allows full pipeline testing including:
    - Schema validation and evolution
    - Append and merge operations
    - Partition column tracking
    - Read-back for assertions

    Thread Safety:
        This class is NOT thread-safe. For concurrent access in tests,
        use appropriate synchronization or separate instances per test.
    """

    def __init__(
        self,
        table_path: str,
        timestamp_column: str = "ingested_at",
        partition_column: Optional[str] = None,
        z_order_columns: Optional[List[str]] = None,
    ):
        """
        Initialize in-memory Delta table.

        Args:
            table_path: Logical path for identification (e.g., "test://xact_events")
            timestamp_column: Column used for time-based operations
            partition_column: Column used for partitioning (tracked but not enforced)
            z_order_columns: Columns for Z-ordering (tracked for verification)
        """
        self.table_path = table_path
        self.timestamp_column = timestamp_column
        self.partition_column = partition_column
        self.z_order_columns = z_order_columns or []

        # Internal storage
        self._data: Optional[pl.DataFrame] = None
        self._schema: Dict[str, pl.DataType] = {}
        self._write_count = 0
        self._write_history: List[WriteRecord] = []
        self._row_count = 0

        logger.debug(
            f"Initialized InMemoryDeltaTable: {table_path}, "
            f"partition={partition_column}, z_order={z_order_columns}"
        )

    def _table_exists(self) -> bool:
        """Check if table has been initialized with data."""
        return self._data is not None

    def _evolve_schema(self, df: pl.DataFrame) -> Tuple[pl.DataFrame, List[str]]:
        """
        Evolve schema to accommodate new columns from incoming DataFrame.

        Args:
            df: Incoming DataFrame

        Returns:
            Tuple of (aligned DataFrame, list of schema changes)
        """
        schema_changes: List[str] = []

        if self._data is None:
            # First write - establish schema
            self._schema = {c: df[c].dtype for c in df.columns}
            return df, [f"Initial schema: {list(df.columns)}"]

        # Add new columns from incoming data to existing table
        for col in df.columns:
            if col not in self._schema:
                self._schema[col] = df[col].dtype
                schema_changes.append(f"Added column: {col} ({df[col].dtype})")
                # Add null column to existing data
                self._data = self._data.with_columns(
                    pl.lit(None).cast(df[col].dtype).alias(col)
                )

        # Add missing columns to incoming data (from existing schema)
        for col, dtype in self._schema.items():
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).cast(dtype).alias(col))

        # Reorder incoming columns to match existing table column order
        df = df.select(self._data.columns)

        return df, schema_changes

    def _cast_to_target_types(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Cast incoming DataFrame columns to match target schema types.

        Handles common type mismatches like:
        - Datetime timezone differences
        - Int vs Float
        - String variations
        """
        if not self._schema:
            return df

        cast_exprs = []
        for col in df.columns:
            if col not in self._schema:
                cast_exprs.append(pl.col(col))
                continue

            source_dtype = df[col].dtype
            target_dtype = self._schema[col]

            if source_dtype == target_dtype:
                cast_exprs.append(pl.col(col))
            elif isinstance(source_dtype, pl.Datetime) and isinstance(
                target_dtype, pl.Datetime
            ):
                # Handle timezone differences
                source_tz = source_dtype.time_zone
                target_tz = target_dtype.time_zone
                if source_tz != target_tz:
                    if target_tz is None:
                        cast_exprs.append(
                            pl.col(col).dt.replace_time_zone(None).alias(col)
                        )
                    elif source_tz is None:
                        cast_exprs.append(
                            pl.col(col).dt.replace_time_zone(target_tz).alias(col)
                        )
                    else:
                        cast_exprs.append(
                            pl.col(col).dt.convert_time_zone(target_tz).alias(col)
                        )
                else:
                    cast_exprs.append(pl.col(col).cast(target_dtype, strict=False))
            else:
                # General cast
                cast_exprs.append(pl.col(col).cast(target_dtype, strict=False))

        return df.select(cast_exprs)

    def append(
        self,
        df: pl.DataFrame,
        batch_id: Optional[str] = None,
    ) -> int:
        """
        Append DataFrame to table.

        Mirrors DeltaTableWriter.append() behavior:
        - Schema evolution (adds new columns)
        - No deduplication at write time
        - Returns row count

        Args:
            df: Polars DataFrame to append
            batch_id: Optional identifier for logging/tracking

        Returns:
            Number of rows written
        """
        if df.is_empty():
            logger.debug(f"Empty append to {self.table_path}, batch_id={batch_id}")
            return 0

        # Cast null-typed columns to string (matches production behavior)
        for col in df.columns:
            if df[col].dtype == pl.Null:
                df = df.with_columns(pl.col(col).cast(pl.Utf8))

        # Evolve schema and align columns
        df, schema_changes = self._evolve_schema(df)

        # Cast to target types
        if self._data is not None:
            df = self._cast_to_target_types(df)

        # Append data
        if self._data is None:
            self._data = df.clone()
        else:
            self._data = pl.concat([self._data, df], how="diagonal_relaxed")

        # Update tracking
        self._write_count += 1
        self._row_count += len(df)

        record = WriteRecord(
            operation="append",
            rows_affected=len(df),
            batch_id=batch_id,
            timestamp=datetime.now(timezone.utc),
            columns=list(df.columns),
            schema_changes=schema_changes,
        )
        self._write_history.append(record)

        logger.debug(
            f"Appended {len(df)} rows to {self.table_path}, "
            f"batch_id={batch_id}, total_rows={self._row_count}"
        )

        return len(df)

    def merge(
        self,
        df: pl.DataFrame,
        merge_keys: List[str],
        preserve_columns: Optional[List[str]] = None,
        update_condition: Optional[str] = None,
    ) -> int:
        """
        Merge DataFrame into table (upsert).

        Mirrors DeltaTableWriter.merge() behavior:
        - Matching rows: UPDATE all columns except merge_keys and preserve_columns
        - Non-matching rows: INSERT all columns
        - Handles duplicates within batch

        Args:
            df: Polars DataFrame to merge
            merge_keys: Columns forming primary key for matching
            preserve_columns: Columns to preserve on update (default: ["created_at"])
            update_condition: Optional predicate (not fully implemented in memory)

        Returns:
            Number of rows affected (inserted + updated)
        """
        if df.is_empty():
            return 0

        if preserve_columns is None:
            preserve_columns = ["created_at"]

        # Cast null-typed columns
        for col in df.columns:
            if df[col].dtype == pl.Null:
                df = df.with_columns(pl.col(col).cast(pl.Utf8))

        # Dedupe within batch by merge keys (keep last)
        initial_len = len(df)
        df = df.unique(subset=merge_keys, keep="last")
        if len(df) < initial_len:
            logger.debug(
                f"Deduped batch from {initial_len} to {len(df)} rows by {merge_keys}"
            )

        # If table doesn't exist, just insert all
        if self._data is None:
            self._schema = {c: df[c].dtype for c in df.columns}
            self._data = df.clone()
            self._write_count += 1
            self._row_count = len(df)

            record = WriteRecord(
                operation="merge",
                rows_affected=len(df),
                batch_id=None,
                timestamp=datetime.now(timezone.utc),
                columns=list(df.columns),
                schema_changes=[f"Initial schema: {list(df.columns)}"],
            )
            self._write_history.append(record)

            logger.debug(f"Initial merge insert of {len(df)} rows to {self.table_path}")
            return len(df)

        # Evolve schema
        df, schema_changes = self._evolve_schema(df)
        df = self._cast_to_target_types(df)

        # Perform merge logic
        # 1. Find matching rows in existing data
        existing_keys = self._data.select(merge_keys)
        incoming_keys = df.select(merge_keys)

        # Join to find matches
        matched = existing_keys.join(incoming_keys, on=merge_keys, how="inner")
        num_matched = len(matched)

        # 2. Remove matched rows from existing data
        if num_matched > 0:
            # Anti-join to get non-matched existing rows
            non_matched_existing = self._data.join(
                df.select(merge_keys), on=merge_keys, how="anti"
            )
        else:
            non_matched_existing = self._data

        # 3. For matched rows, preserve specified columns from existing data
        if num_matched > 0 and preserve_columns:
            # Filter to only preserve columns that exist in both existing and incoming data
            actual_preserve_cols = [
                col for col in preserve_columns
                if col in self._data.columns and col in df.columns
            ]
            if actual_preserve_cols:
                # Get preserve column values from existing data for matched keys
                preserve_data = self._data.select(merge_keys + actual_preserve_cols).join(
                    df.select(merge_keys), on=merge_keys, how="inner"
                )
                # Update incoming df with preserved values
                for col in actual_preserve_cols:
                    if col in preserve_data.columns:
                        # Join and replace
                        df = df.drop(col).join(
                            preserve_data.select(merge_keys + [col]),
                            on=merge_keys,
                            how="left",
                        )

        # 4. Combine: non-matched existing + all incoming (which includes updates)
        self._data = pl.concat([non_matched_existing, df], how="diagonal_relaxed")
        self._row_count = len(self._data)

        rows_inserted = len(df) - num_matched
        rows_updated = num_matched

        self._write_count += 1
        record = WriteRecord(
            operation="merge",
            rows_affected=len(df),
            batch_id=None,
            timestamp=datetime.now(timezone.utc),
            columns=list(df.columns),
            schema_changes=schema_changes,
        )
        self._write_history.append(record)

        logger.debug(
            f"Merged into {self.table_path}: {rows_inserted} inserted, "
            f"{rows_updated} updated, total_rows={self._row_count}"
        )

        return rows_inserted + rows_updated

    def read(self, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Read all data from table.

        Args:
            columns: Optional list of columns to select

        Returns:
            Polars DataFrame (empty if no data)
        """
        if self._data is None:
            return pl.DataFrame()

        if columns:
            available = [c for c in columns if c in self._data.columns]
            return self._data.select(available)

        return self._data.clone()

    def read_filtered(
        self,
        filter_expr: pl.Expr,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> pl.DataFrame:
        """
        Read data with filter expression.

        Args:
            filter_expr: Polars filter expression
            columns: Optional columns to select
            limit: Optional row limit

        Returns:
            Filtered Polars DataFrame
        """
        if self._data is None:
            return pl.DataFrame()

        result = self._data.filter(filter_expr)

        if columns:
            available = [c for c in columns if c in result.columns]
            result = result.select(available)

        if limit:
            result = result.head(limit)

        return result

    def scan(self, columns: Optional[List[str]] = None) -> pl.LazyFrame:
        """
        Create lazy scan of table data.

        Args:
            columns: Optional columns to select

        Returns:
            Polars LazyFrame
        """
        if self._data is None:
            return pl.LazyFrame()

        lf = self._data.lazy()
        if columns:
            available = [c for c in columns if c in self._data.columns]
            lf = lf.select(available)

        return lf

    def exists(self) -> bool:
        """Check if table exists (has data)."""
        return self._data is not None and not self._data.is_empty()

    # =========================================================================
    # Test Helper Methods
    # =========================================================================

    def get_write_history(self) -> List[WriteRecord]:
        """Get history of all write operations for debugging."""
        return self._write_history.copy()

    def get_write_count(self) -> int:
        """Get total number of write operations."""
        return self._write_count

    def get_row_count(self) -> int:
        """Get current row count."""
        return self._row_count

    def get_schema(self) -> Dict[str, pl.DataType]:
        """Get current schema."""
        return self._schema.copy()

    def get_columns(self) -> List[str]:
        """Get column names."""
        if self._data is None:
            return []
        return self._data.columns

    def get_unique_values(self, column: str) -> List[Any]:
        """Get unique values for a column (useful for assertions)."""
        if self._data is None or column not in self._data.columns:
            return []
        return self._data[column].unique().to_list()

    def get_partition_values(self) -> List[Any]:
        """Get unique partition values if partition column is set."""
        if self.partition_column:
            return self.get_unique_values(self.partition_column)
        return []

    def clear(self) -> None:
        """Clear all data and reset state (useful between tests)."""
        self._data = None
        self._schema = {}
        self._write_count = 0
        self._write_history = []
        self._row_count = 0
        logger.debug(f"Cleared InMemoryDeltaTable: {self.table_path}")

    def to_dicts(self) -> List[Dict[str, Any]]:
        """Convert all data to list of dicts (useful for assertions)."""
        if self._data is None:
            return []
        return self._data.to_dicts()

    def __len__(self) -> int:
        """Return current row count."""
        return self._row_count

    def __repr__(self) -> str:
        return (
            f"InMemoryDeltaTable(path={self.table_path}, "
            f"rows={self._row_count}, columns={len(self._schema)})"
        )


class InMemoryDeltaTableWriter(InMemoryDeltaTable):
    """
    Drop-in replacement for DeltaTableWriter using in-memory storage.

    This class provides the exact same interface as DeltaTableWriter,
    making it easy to swap in tests via dependency injection or monkeypatching.

    Usage:
        # In production code
        writer = DeltaTableWriter("abfss://...")

        # In tests - same interface
        writer = InMemoryDeltaTableWriter("test://...")

        # Or via monkeypatch
        monkeypatch.setattr(
            "kafka_pipeline.common.storage.delta.DeltaTableWriter",
            InMemoryDeltaTableWriter
        )
    """

    def write_rows(self, rows: List[dict], schema: Optional[dict] = None) -> int:
        """
        Write list of dicts to table.

        Mirrors DeltaTableWriter.write_rows() interface.

        Args:
            rows: List of row dictionaries
            schema: Optional Polars schema dict

        Returns:
            Number of rows written
        """
        if not rows:
            return 0

        if schema:
            df = pl.DataFrame(rows, schema=schema)
        else:
            df = pl.DataFrame(rows)

        return self.append(df)

    def merge_batched(
        self,
        df: pl.DataFrame,
        merge_keys: List[str],
        batch_size: Optional[int] = None,
        when_matched: str = "update",
        when_not_matched: str = "insert",
        preserve_columns: Optional[List[str]] = None,
    ) -> int:
        """
        Batched merge for large datasets.

        In-memory implementation just does a single merge since there's
        no need for batching in tests.
        """
        return self.merge(df, merge_keys, preserve_columns)

    def upsert_with_idempotency(
        self,
        df: pl.DataFrame,
        merge_key: str,
        event_id_field: str = "_event_id",
    ) -> int:
        """
        Upsert with deduplication by event ID.

        Args:
            df: Data to upsert
            merge_key: Primary key column for merge
            event_id_field: Column containing event ID for dedup

        Returns:
            Number of rows written
        """
        if df.is_empty():
            return 0

        # Dedupe by event_id
        if event_id_field in df.columns:
            df = df.unique(subset=[event_id_field], keep="last")

        # Dedupe by merge_key
        if merge_key in df.columns:
            df = df.unique(subset=[merge_key], keep="last")

        return self.append(df)


class InMemoryDeltaTableReader:
    """
    In-memory reader that wraps InMemoryDeltaTable for read operations.

    Provides the same interface as DeltaTableReader but reads from
    an InMemoryDeltaTable instance.
    """

    def __init__(self, table: InMemoryDeltaTable):
        """
        Initialize reader with an in-memory table.

        Args:
            table: InMemoryDeltaTable instance to read from
        """
        self.table = table
        self.table_path = table.table_path

    def read(self, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """Read entire table."""
        return self.table.read(columns)

    def scan(self, columns: Optional[List[str]] = None) -> pl.LazyFrame:
        """Create lazy scan."""
        return self.table.scan(columns)

    def read_filtered(
        self,
        filter_expr: pl.Expr,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
        descending: bool = False,
    ) -> pl.DataFrame:
        """Read with filter."""
        result = self.table.read_filtered(filter_expr, columns, limit)
        if order_by and order_by in result.columns:
            result = result.sort(order_by, descending=descending)
        return result

    def exists(self) -> bool:
        """Check if table exists."""
        return self.table.exists()


# =============================================================================
# Registry for managing multiple in-memory tables in tests
# =============================================================================


class InMemoryDeltaRegistry:
    """
    Registry for managing multiple in-memory Delta tables in tests.

    Provides a centralized way to create, access, and reset tables.
    Useful for E2E tests that need multiple tables (events, inventory, etc.).

    Usage:
        registry = InMemoryDeltaRegistry()

        # Get or create tables
        events_table = registry.get_table("events", partition_column="event_date")
        inventory_table = registry.get_table("inventory")

        # Reset between tests
        registry.clear_all()

        # Get writer for a table (DeltaTableWriter interface)
        writer = registry.get_writer("events")
    """

    def __init__(self):
        self._tables: Dict[str, InMemoryDeltaTable] = {}

    def get_table(
        self,
        name: str,
        timestamp_column: str = "ingested_at",
        partition_column: Optional[str] = None,
        z_order_columns: Optional[List[str]] = None,
    ) -> InMemoryDeltaTable:
        """
        Get or create an in-memory table by name.

        Args:
            name: Table name/identifier
            timestamp_column: Timestamp column
            partition_column: Partition column
            z_order_columns: Z-order columns

        Returns:
            InMemoryDeltaTable instance
        """
        if name not in self._tables:
            self._tables[name] = InMemoryDeltaTable(
                table_path=f"inmemory://{name}",
                timestamp_column=timestamp_column,
                partition_column=partition_column,
                z_order_columns=z_order_columns,
            )
        return self._tables[name]

    def get_writer(
        self,
        name: str,
        timestamp_column: str = "ingested_at",
        partition_column: Optional[str] = None,
        z_order_columns: Optional[List[str]] = None,
    ) -> "RegistryBackedWriter":
        """
        Get a writer for a table that writes directly to the registry.

        Returns a writer that shares storage with the registry table,
        so writes are immediately visible via get_table().

        Args:
            name: Table name
            timestamp_column: Timestamp column
            partition_column: Partition column
            z_order_columns: Z-order columns

        Returns:
            RegistryBackedWriter instance
        """
        # Get or create the underlying table
        table = self.get_table(name, timestamp_column, partition_column, z_order_columns)

        # Return a writer that delegates to the registry table
        return RegistryBackedWriter(table)

    def get_reader(self, name: str) -> Optional[InMemoryDeltaTableReader]:
        """
        Get a reader for an existing table.

        Args:
            name: Table name

        Returns:
            InMemoryDeltaTableReader or None if table doesn't exist
        """
        if name not in self._tables:
            return None
        return InMemoryDeltaTableReader(self._tables[name])

    def clear_table(self, name: str) -> None:
        """Clear a specific table."""
        if name in self._tables:
            self._tables[name].clear()

    def clear_all(self) -> None:
        """Clear all tables (useful between tests)."""
        for table in self._tables.values():
            table.clear()

    def reset(self) -> None:
        """Completely reset registry (remove all tables)."""
        self._tables.clear()

    def list_tables(self) -> List[str]:
        """List all table names."""
        return list(self._tables.keys())

    def get_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get stats for all tables."""
        return {
            name: {
                "row_count": table.get_row_count(),
                "write_count": table.get_write_count(),
                "columns": table.get_columns(),
            }
            for name, table in self._tables.items()
        }


class RegistryBackedWriter:
    """
    Writer that delegates to an InMemoryDeltaTable in the registry.

    This ensures writes are immediately visible via registry.get_table().
    Provides the same interface as InMemoryDeltaTableWriter.
    """

    def __init__(self, table: InMemoryDeltaTable):
        self._table = table
        self.table_path = table.table_path
        self.timestamp_column = table.timestamp_column
        self.partition_column = table.partition_column
        self.z_order_columns = table.z_order_columns

    def append(self, df: pl.DataFrame, batch_id: Optional[str] = None) -> int:
        """Append data to the underlying table."""
        return self._table.append(df, batch_id)

    def merge(
        self,
        df: pl.DataFrame,
        merge_keys: List[str],
        preserve_columns: Optional[List[str]] = None,
        update_condition: Optional[str] = None,
    ) -> int:
        """Merge data into the underlying table."""
        return self._table.merge(df, merge_keys, preserve_columns, update_condition)

    def write_rows(self, rows: List[dict], schema: Optional[dict] = None) -> int:
        """Write list of dicts to table."""
        if not rows:
            return 0
        if schema:
            df = pl.DataFrame(rows, schema=schema)
        else:
            df = pl.DataFrame(rows)
        return self.append(df)

    def merge_batched(
        self,
        df: pl.DataFrame,
        merge_keys: List[str],
        batch_size: Optional[int] = None,
        when_matched: str = "update",
        when_not_matched: str = "insert",
        preserve_columns: Optional[List[str]] = None,
    ) -> int:
        """Batched merge."""
        return self.merge(df, merge_keys, preserve_columns)

    def upsert_with_idempotency(
        self,
        df: pl.DataFrame,
        merge_key: str,
        event_id_field: str = "_event_id",
    ) -> int:
        """Upsert with deduplication."""
        if df.is_empty():
            return 0
        if event_id_field in df.columns:
            df = df.unique(subset=[event_id_field], keep="last")
        if merge_key in df.columns:
            df = df.unique(subset=[merge_key], keep="last")
        return self.append(df)

    def read(self, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """Read from underlying table."""
        return self._table.read(columns)

    def __len__(self) -> int:
        return len(self._table)


__all__ = [
    "InMemoryDeltaTable",
    "InMemoryDeltaTableWriter",
    "InMemoryDeltaTableReader",
    "InMemoryDeltaRegistry",
    "RegistryBackedWriter",
    "WriteRecord",
]
