"""
Delta table operations with retry and auth refresh support.

Read/write operations for Delta tables on OneLake/ADLS. All datetimes are UTC-aware.
Note: Query timeouts not supported; use time-bounded filters and partition pruning.

Migrated from verisk_pipeline.storage.delta for pipeline reorganization (REORG-502).
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import warnings
from datetime import UTC, datetime
from typing import Any

import polars as pl
from deltalake import DeltaTable
from deltalake import write_deltalake as _write_deltalake

from core.resilience.circuit_breaker import CircuitBreakerConfig
from pipeline.common.auth import get_auth, get_storage_options
from pipeline.common.retry import RetryConfig, with_retry
from pipeline.common.storage.onelake import _refresh_all_credentials

logger = logging.getLogger(__name__)


# Check if deltalake supports schema_mode parameter (added in newer versions)
_WRITE_DELTALAKE_SUPPORTS_SCHEMA_MODE = (
    "schema_mode" in inspect.signature(_write_deltalake).parameters
)


def write_deltalake(*args: Any, **kwargs: Any) -> None:
    """
    Wrapper for deltalake.write_deltalake with backwards compatibility.

    Older versions of deltalake don't support schema_mode parameter.
    This wrapper removes it if not supported to maintain compatibility.
    """
    if not _WRITE_DELTALAKE_SUPPORTS_SCHEMA_MODE and "schema_mode" in kwargs:
        kwargs.pop("schema_mode")
    return _write_deltalake(*args, **kwargs)


# Retry config for Delta operations
DELTA_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=1.0,
    max_delay=10.0,
)

# Circuit breaker config for Delta operations
DELTA_CIRCUIT_CONFIG = CircuitBreakerConfig(
    failure_threshold=5,
    timeout_seconds=30.0,
)


def _on_auth_error() -> None:
    _refresh_all_credentials()


def get_open_file_descriptors() -> int:
    try:
        pid = os.getpid()
        # On Linux/Unix systems, /proc/pid/fd contains one entry per open FD
        fd_dir = f"/proc/{pid}/fd"
        if os.path.exists(fd_dir):
            return len(os.listdir(fd_dir))

        # Fallback: try to count via resource module (less accurate but portable)
        import resource

        soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
        # We can't easily count open FDs on all platforms, return -1 to indicate unknown
        return -1
    except Exception as e:
        logger.debug("Could not get file descriptor count: %s", e)
        return -1


class DeltaTableReader:
    """
    Reader for Delta tables with auth retry support.

    Usage:
        reader = DeltaTableReader("abfss://workspace@onelake/lakehouse/Tables/events")
        df = reader.read()
        df = reader.read(columns=["trace_id", "ingested_at"])
    """

    def __init__(
        self, table_path: str, storage_options: dict[str, str] | None = None
    ):
        self.table_path = table_path
        self.storage_options = storage_options
        self._delta_table: DeltaTable | None = None
        self._closed = False
        self._init_lock = asyncio.Lock()

    async def __aenter__(self) -> DeltaTableReader:
        return self

    async def __aexit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: Any | None,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        if self._closed:
            return

        async with self._init_lock:
            if self._closed:
                return

            if self._delta_table is not None:
                logger.debug(
                    "Closing Delta table reader",
                    extra={"table_path": self.table_path},
                )
                # Release the DeltaTable reference to allow file handles to close
                self._delta_table = None

            self._closed = True

    def __del__(self) -> None:
        if hasattr(self, "_closed") and not self._closed:
            warnings.warn(
                f"DeltaTableReader for {self.table_path} was not properly closed. "
                "Use async context manager: 'async with DeltaTableReader(...) as reader:' "
                "or call await reader.close() explicitly.",
                ResourceWarning,
                stacklevel=2,
            )

    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def read(
        self,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        opts = self.storage_options or get_storage_options()

        df = pl.read_delta(
            self.table_path,
            storage_options=opts,
            columns=columns,
        )

        logger.debug("Read complete", extra={"rows_read": len(df)})
        return df

    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def scan(
        self,
        columns: list[str] | None = None,
    ) -> pl.LazyFrame:
        opts = self.storage_options or get_storage_options()

        lf = pl.scan_delta(self.table_path, storage_options=opts)

        if columns:
            lf = lf.select(columns)

        return lf

    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def read_filtered(
        self,
        filter_expr: pl.Expr,
        columns: list[str] | None = None,
        limit: int | None = None,
        order_by: str | None = None,
        descending: bool = False,
    ) -> pl.DataFrame:
        opts = self.storage_options or get_storage_options()

        lf = pl.scan_delta(self.table_path, storage_options=opts)
        lf = lf.filter(filter_expr)

        if columns:
            lf = lf.select(columns)

        if order_by:
            lf = lf.sort(order_by, descending=descending)

        if limit:
            lf = lf.head(limit)
        if limit and not order_by:
            logger.warning(
                "read_filtered called with limit but no order_by - results may be non-deterministic",
                extra={"limit": limit},
            )

        # streaming=True can cause "invalid SlotMap key used" panic with sort()
        use_streaming = order_by is None
        df = lf.collect(streaming=use_streaming)
        logger.debug("Read filtered complete", extra={"rows_read": len(df)})
        return df

    def read_as_polars(
        self,
        filters: list[tuple[str, str, Any]] | None = None,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        """
        Read Delta table as Polars DataFrame with optional filters.

        Args:
            filters: Optional list of (column, operator, value) tuples.
                    Operators: '=', '!=', '<', '>', '<=', '>='
            columns: Optional list of columns to select

        Returns:
            Polars DataFrame
        """
        opts = self.storage_options or get_storage_options()

        lf = pl.scan_delta(self.table_path, storage_options=opts)

        # Apply filters with robust type casting
        if filters:
            for col, op, value in filters:
                try:
                    # Cast column to match value type for numeric comparisons
                    col_expr = pl.col(col)
                    if isinstance(value, int):
                        # Cast to int, fill nulls with 0 to handle conversion failures
                        col_expr = col_expr.cast(pl.Int64, strict=False).fill_null(0)
                    elif isinstance(value, float):
                        # Cast to float, fill nulls with 0.0
                        col_expr = col_expr.cast(pl.Float64, strict=False).fill_null(
                            0.0
                        )

                    if op == "=":
                        lf = lf.filter(col_expr == value)
                    elif op == "!=":
                        lf = lf.filter(col_expr != value)
                    elif op == "<":
                        lf = lf.filter(col_expr < value)
                    elif op == ">":
                        lf = lf.filter(col_expr > value)
                    elif op == "<=":
                        lf = lf.filter(col_expr <= value)
                    elif op == ">=":
                        lf = lf.filter(col_expr >= value)
                    else:
                        raise ValueError(f"Unsupported filter operator: {op}")
                except Exception as e:
                    # Provide detailed error for debugging
                    raise TypeError(
                        f"Filter error on column '{col}' {op} {value} (type: {type(value).__name__}): {str(e)}"
                    ) from e

        if columns:
            lf = lf.select(columns)

        return lf.collect(streaming=True)

    def exists(self) -> bool:
        """Check if table exists and is readable."""
        try:
            # Try to read schema only
            opts = self.storage_options or get_storage_options()
            pl.scan_delta(self.table_path, storage_options=opts).collect_schema()
            return True
        except Exception as e:
            logger.debug(
                "Table does not exist or is not readable",
                extra={"error_message": str(e)[:200]},
            )
            return False


class DeltaTableWriter:
    """
    Writer for Delta tables with auth retry support.

    Deduplication handled by daily Fabric maintenance job.

    Usage:
        writer = DeltaTableWriter("abfss://workspace@onelake/lakehouse/Tables/events")
        rows_written = writer.append(df)
    """

    def __init__(
        self,
        table_path: str,
        timestamp_column: str = "ingested_at",
        partition_column: str | None = None,
        z_order_columns: list[str] | None = None,
    ):
        self.table_path = table_path
        self.timestamp_column = timestamp_column
        self.partition_column = partition_column
        self.z_order_columns = z_order_columns or []
        self._reader = DeltaTableReader(table_path)
        self._optimization_scheduler: Any | None = None
        self._delta_table: DeltaTable | None = None
        self._closed = False
        self._init_lock = asyncio.Lock()

    async def __aenter__(self) -> DeltaTableWriter:
        """Async context manager entry."""
        return self

    async def __aexit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: Any | None,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        if self._closed:
            return

        async with self._init_lock:
            if self._closed:
                return

            # Close reader first
            await self._reader.close()

            if self._delta_table is not None:
                logger.debug(
                    "Closing Delta table writer",
                    extra={"table_path": self.table_path},
                )
                # Release the DeltaTable reference to allow file handles to close
                self._delta_table = None

            self._closed = True

    def __del__(self) -> None:
        if hasattr(self, "_closed") and not self._closed:
            warnings.warn(
                f"DeltaTableWriter for {self.table_path} was not properly closed. "
                "Use async context manager: 'async with DeltaTableWriter(...) as writer:' "
                "or call await writer.close() explicitly.",
                ResourceWarning,
                stacklevel=2,
            )

    def _table_exists(self, opts: dict[str, str]) -> bool:
        try:
            DeltaTable(self.table_path, storage_options=opts)
            return True
        except Exception:
            return False

    def _cast_null_columns(
        self, df: pl.DataFrame, target_type: pl.DataType = pl.Utf8
    ) -> pl.DataFrame:
        """Cast NULL-typed columns to target type to avoid Delta Lake errors."""
        for col in df.columns:
            if df[col].dtype == pl.Null:
                df = df.with_columns(
                    pl.col(col).cast(target_type, strict=False).alias(col)
                )
        return df

    def _align_schema_with_target(
        self, df: pl.DataFrame, opts: dict[str, str]
    ) -> pl.DataFrame:
        """Aligns DataFrame schema with target table to prevent type coercion errors during merge."""
        try:
            dt = DeltaTable(self.table_path, storage_options=opts)
            target_schema = dt.schema()

            # Build ordered list of target column names and mapping to Arrow types
            target_column_order: list[str] = []
            target_types: dict[str, Any] = {}
            for field in target_schema.fields:
                target_column_order.append(field.name)
                target_types[field.name] = field.type

            source_columns = set(df.columns)

            # Build expressions in TARGET column order first
            cast_exprs = []
            mismatches = []
            for col in target_column_order:
                if col in source_columns:
                    target_arrow_type = target_types[col]
                    source_dtype = df[col].dtype
                    # Convert Arrow type to Polars type and cast
                    try:
                        polars_type = self._arrow_type_to_polars(target_arrow_type)
                        if polars_type is None:
                            # Unknown type - log warning and keep original
                            mismatches.append(
                                f"{col}: source={source_dtype}, target={target_arrow_type} (unmapped)"
                            )
                            cast_exprs.append(pl.col(col))
                        elif source_dtype == pl.Null:
                            # NULL-typed columns must be cast to target type for delta-rs CASE WHEN
                            mismatches.append(f"{col}: Null -> {polars_type}")
                            # Cast existing column (not literal) to preserve DataFrame structure
                            cast_exprs.append(
                                pl.col(col).cast(polars_type, strict=False).alias(col)
                            )
                        elif source_dtype != polars_type:
                            mismatches.append(f"{col}: {source_dtype} -> {polars_type}")
                            # Special handling for datetime timezone conversions
                            # Polars cast() doesn't handle timezone conversion properly
                            if isinstance(source_dtype, pl.Datetime) and isinstance(
                                polars_type, pl.Datetime
                            ):
                                # Handle timezone differences
                                source_tz = source_dtype.time_zone
                                target_tz = polars_type.time_zone
                                if source_tz != target_tz:
                                    if target_tz is None:
                                        # Remove timezone
                                        cast_exprs.append(
                                            pl.col(col)
                                            .dt.replace_time_zone(None)
                                            .alias(col)
                                        )
                                    elif source_tz is None:
                                        # Add timezone
                                        cast_exprs.append(
                                            pl.col(col)
                                            .dt.replace_time_zone(target_tz)
                                            .alias(col)
                                        )
                                    else:
                                        # Convert between timezones
                                        cast_exprs.append(
                                            pl.col(col)
                                            .dt.convert_time_zone(target_tz)
                                            .alias(col)
                                        )
                                else:
                                    cast_exprs.append(
                                        pl.col(col)
                                        .cast(polars_type, strict=False)
                                        .alias(col)
                                    )
                            else:
                                cast_exprs.append(
                                    pl.col(col)
                                    .cast(polars_type, strict=False)
                                    .alias(col)
                                )
                        else:
                            cast_exprs.append(pl.col(col))
                    except Exception as e:
                        # If cast fails, keep original column
                        mismatches.append(f"{col}: cast failed ({e})")
                        cast_exprs.append(pl.col(col))

            # Add any extra source columns not in target (at the end)
            # Schema evolution will handle adding these to the table
            extra_cols = []
            for col in df.columns:
                if col not in target_types:
                    extra_cols.append(col)
                    cast_exprs.append(pl.col(col))

            if cast_exprs:
                df = df.select(cast_exprs)

            # Log detailed info for debugging schema issues
            log_level = logging.WARNING if mismatches else logging.DEBUG
            logger.log(
                log_level,
                "Aligned source schema with target",
                extra={
                    "source_columns": len(source_columns),
                    "target_columns": len(target_types),
                    "casts_applied": len(mismatches),
                    "extra_source_cols": extra_cols if extra_cols else None,
                    "type_changes": mismatches if mismatches else None,
                },
            )

            return df

        except Exception as e:
            logger.warning(
                "Could not align schema with target, proceeding with original",
                extra={"error_message": str(e)[:200]},
            )
            return df

    def _arrow_type_to_polars(self, arrow_type: Any) -> pl.DataType | None:
        """
        Convert Arrow type to Polars type for schema alignment.

        Args:
            arrow_type: PyArrow type from Delta schema

        Returns:
            Corresponding Polars type, or None if unknown
        """
        import pyarrow as pa

        type_str = str(arrow_type).lower()

        # Timestamp types - always use UTC for consistency across all tables
        # All timestamps in the pipeline should be timezone-aware UTC
        if "timestamp" in type_str:
            return pl.Datetime("us", "UTC")

        # String types - check by string representation for flexibility
        # Handles: string, large_string, utf8, large_utf8
        if arrow_type == pa.string() or arrow_type == pa.large_string():
            return pl.Utf8
        if "string" in type_str or "utf8" in type_str:
            return pl.Utf8

        # Integer types - exact match first, then string fallback
        if arrow_type == pa.int64():
            return pl.Int64
        if arrow_type == pa.int32():
            return pl.Int32
        if arrow_type == pa.int16():
            return pl.Int16
        if arrow_type == pa.int8():
            return pl.Int8
        # String-based fallback for integer types
        if "int64" in type_str or type_str == "long":
            return pl.Int64
        if "int32" in type_str or type_str == "int":
            return pl.Int32

        # Float types
        if arrow_type == pa.float64():
            return pl.Float64
        if arrow_type == pa.float32():
            return pl.Float32
        if "float64" in type_str or "double" in type_str:
            return pl.Float64
        if "float32" in type_str or type_str == "float":
            return pl.Float32

        # Boolean
        if arrow_type == pa.bool_():
            return pl.Boolean
        if "bool" in type_str:
            return pl.Boolean

        # Date types
        if arrow_type == pa.date32() or arrow_type == pa.date64():
            return pl.Date
        if "date" in type_str and "timestamp" not in type_str:
            return pl.Date

        # Binary
        if arrow_type == pa.binary() or arrow_type == pa.large_binary():
            return pl.Binary
        if "binary" in type_str:
            return pl.Binary

        # Decimal types - convert to Float64 for compatibility
        if "decimal" in type_str:
            return pl.Float64

        # Null type
        if arrow_type == pa.null() or type_str == "null":
            return pl.Null

        return None

    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def append(
        self,
        df: pl.DataFrame,
        batch_id: str | None = None,
    ) -> int:
        """
        Append DataFrame to Delta table.

        No deduplication at write time - duplicates are handled by:
        1. Download worker's in-memory cache (for downloads)
        2. Daily maintenance job (for events tables)

        Args:
            df: Data to append
            batch_id: Optional short identifier for log correlation

        Returns:
            Number of rows written
        """
        if df.is_empty():
            logger.debug("No data to write", extra={"batch_id": batch_id})
            return 0

        # Write to Delta - convert to Arrow for write_deltalake
        # Log auth context before write to help debug 403 errors
        auth = get_auth()
        logger.info(
            "Delta write starting",
            extra={
                "batch_id": batch_id,
                "rows": len(df),
                "table_path": self.table_path,
                "auth_mode": auth.auth_mode,
            },
        )
        opts = get_storage_options()

        # Align source schema with target to ensure column order matches
        # (alignment also handles null-typed columns)
        if self._table_exists(opts):
            df = self._align_schema_with_target(df, opts)
        else:
            # Table doesn't exist - cast null-typed columns to avoid Delta Lake errors
            df = self._cast_null_columns(df)

        # Determine partition columns - use existing table's partitions if table exists,
        # otherwise use configured partition column. This prevents partition mismatch
        # errors when appending to tables with different/no partitioning.
        partition_by = None
        if self._table_exists(opts):
            try:
                dt = DeltaTable(self.table_path, storage_options=opts)
                existing_partitions = dt.metadata().partition_columns
                partition_by = existing_partitions if existing_partitions else None
            except Exception:
                # Fall back to configured partition column
                partition_by = (
                    [self.partition_column] if self.partition_column else None
                )
        else:
            partition_by = [self.partition_column] if self.partition_column else None

        write_deltalake(
            self.table_path,
            df.to_arrow(),
            mode="append",
            schema_mode="merge",
            storage_options=opts,
            partition_by=partition_by,
        )  # type: ignore[call-overload]

        return len(df)

    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def merge(
        self,
        df: pl.DataFrame,
        merge_keys: list[str],
        preserve_columns: list[str] | None = None,
        update_condition: str | None = None,
    ) -> int:
        """
        Merge DataFrame into table (true upsert via Delta merge API).

        - Matching rows: UPDATE all columns except merge_keys and preserve_columns
        - Non-matching rows: INSERT all columns
        - Multiple rows for same key in batch: Combined (later non-null values win)

        Args:
            df: Data to merge
            merge_keys: Columns forming primary key
            preserve_columns: Columns to preserve on update (default: ["created_at"])
            update_condition: Optional SQL predicate for when to update matched rows.
                              E.g., "source.modified_date > target.modified_date" to only
                              update when the source has a newer modified_date.

        Returns:
            Number of rows affected
        """
        if df.is_empty():
            return 0

        # Log auth context for merge operations to help debug 403 errors
        auth = get_auth()
        logger.info(
            "Delta merge starting",
            extra={
                "rows": len(df),
                "merge_keys": merge_keys,
                "table_path": self.table_path,
                "auth_mode": auth.auth_mode,
            },
        )

        if preserve_columns is None:
            preserve_columns = ["created_at"]

        initial_len = len(df)

        # Combine rows within batch by merge keys (later non-null values overlay earlier)
        # Always dedupe to ensure consistency - checking if needed is as expensive as deduping
        rows = df.to_dicts()
        merged: dict[tuple, dict] = {}
        for row in rows:
            key = tuple(row.get(k) for k in merge_keys)
            if key in merged:
                for col, val in row.items():
                    if val is not None:
                        merged[key][col] = val
            else:
                merged[key] = row.copy()

        del rows  # Free original list
        df = pl.DataFrame(list(merged.values()), infer_schema_length=None)
        del merged  # Free dict

        if len(df) < initial_len:
            logger.debug(
                "Deduped batch",
                extra={
                    "records_processed": initial_len,
                    "rows_written": len(df),
                },
            )

        opts = get_storage_options()

        # Align source schema with target to prevent type coercion errors in CASE WHEN
        # This also handles NULL-typed columns by casting to correct target type
        if self._table_exists(opts):
            df = self._align_schema_with_target(df, opts)
        else:
            # Table doesn't exist yet - cast NULL columns to Utf8 as default
            df = self._cast_null_columns(df)

        # Create table if doesn't exist
        if not self._table_exists(opts):
            arrow_table = df.to_arrow()
            write_deltalake(
                self.table_path,
                arrow_table,
                mode="overwrite",
                schema_mode="overwrite",
                storage_options=opts,
                partition_by=[self.partition_column] if self.partition_column else None,
            )
            del arrow_table
            logger.info(
                "Created table",
                extra={"rows_written": len(df)},
            )
            return len(df)

        # Build merge predicate
        predicate = " AND ".join(f"target.{k} = source.{k}" for k in merge_keys)

        # Build update dict - all columns except merge keys and preserved
        update_cols = [
            c for c in df.columns if c not in merge_keys and c not in preserve_columns
        ]
        update_dict = {c: f"source.{c}" for c in update_cols}

        # Build insert dict - all columns
        insert_dict = {c: f"source.{c}" for c in df.columns}

        # Convert to PyArrow for merge, then free polars df
        pa_df = df.to_arrow()
        del df

        # Perform merge
        dt = DeltaTable(self.table_path, storage_options=opts)

        merge_builder = dt.merge(
            source=pa_df,
            predicate=predicate,
            source_alias="source",
            target_alias="target",
        )

        # Apply update condition if provided (e.g., only update if modified_date changed)
        if update_condition:
            merge_builder = merge_builder.when_matched_update(
                update_dict, predicate=update_condition
            )
        else:
            merge_builder = merge_builder.when_matched_update(update_dict)

        result = merge_builder.when_not_matched_insert(insert_dict).execute()

        # Free PyArrow table and DeltaTable reference
        del pa_df, dt

        # Result contains metrics
        rows_updated = result.get("num_target_rows_updated", 0) or 0
        rows_inserted = result.get("num_target_rows_inserted", 0) or 0

        logger.debug(
            "Merge complete",
            extra={
                "rows_merged": rows_inserted + rows_updated,
                "rows_inserted": rows_inserted,
                "rows_updated": rows_updated,
            },
        )

        return rows_inserted + rows_updated

    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def merge_batched(
        self,
        df: pl.DataFrame,
        merge_keys: list[str],
        batch_size: int | None = None,
        when_matched: str = "update",
        when_not_matched: str = "insert",
        preserve_columns: list[str] | None = None,
    ) -> int:
        """
        Perform batched MERGE operations for large datasets (P2.3).

        Args:
            df: DataFrame to merge
            merge_keys: Columns to use for matching
            batch_size: Maximum rows per batch (uses config if None)
            when_matched: Action for matched rows ('update', 'delete', 'do_nothing')
            when_not_matched: Action for unmatched rows ('insert', 'do_nothing')
            preserve_columns: Columns to preserve on update

        Returns:
            Total rows merged
        """
        if df.is_empty():
            logger.debug("No data to merge")
            return 0

        # Get batch size from config if not provided
        if batch_size is None:
            batch_size = 100000  # Default max batch size for merges

        total_rows = len(df)

        # Single batch optimization
        if total_rows <= batch_size:
            logger.info("Single batch merge", extra={"total_rows": total_rows})
            return self.merge(
                df,
                merge_keys=merge_keys,
                preserve_columns=preserve_columns,
            )

        # Multi-batch merge
        num_batches = (total_rows + batch_size - 1) // batch_size
        logger.info(
            "Batched merge starting",
            extra={
                "total_rows": total_rows,
                "batch_size": batch_size,
                "num_batches": num_batches,
            },
        )

        rows_merged = 0
        for batch_num, batch_start in enumerate(
            range(0, total_rows, batch_size), start=1
        ):
            batch_end = min(batch_start + batch_size, total_rows)
            batch_df = df.slice(batch_start, batch_end - batch_start)

            logger.info(
                f"Merging batch {batch_num}/{num_batches}",
                extra={
                    "batch_start": batch_start,
                    "batch_end": batch_end,
                    "batch_rows": len(batch_df),
                },
            )

            # Perform merge for this batch
            result = self.merge(
                batch_df,
                merge_keys=merge_keys,
                preserve_columns=preserve_columns,
            )

            rows_merged += result

            # Progress logging
            progress_pct = (batch_end / total_rows) * 100
            logger.info(
                "Merge progress",
                extra={
                    "rows_processed": batch_end,
                    "total_rows": total_rows,
                    "progress_pct": round(progress_pct, 1),
                },
            )

        logger.info(
            "Batched merge complete",
            extra={
                "rows_merged": rows_merged,
                "total_rows": total_rows,
            },
        )
        return rows_merged

    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def write_rows(self, rows: list[dict], schema: dict | None = None) -> int:
        """
        Write list of dicts to Delta table.

        Args:
            rows: List of row dictionaries
            schema: Optional Polars schema dict

        Returns:
            Number of rows written
        """
        if not rows:
            return 0

        df = pl.DataFrame(rows, schema=schema) if schema else pl.DataFrame(rows)

        result: int = self.append(df)  # type: ignore[assignment]
        return result


class EventsTableReader(DeltaTableReader):
    """
    Specialized reader for the events table.

    Provides convenience methods for common event queries.
    """

    def __init__(self, table_path: str):
        super().__init__(table_path)

    def get_max_timestamp(
        self, timestamp_col: str = "ingested_at"
    ) -> datetime | None:
        """Get maximum timestamp from table."""
        try:
            df: pl.DataFrame = self.read(columns=[timestamp_col])  # type: ignore[assignment]
            if df is None or df.is_empty():
                return None

            # Column is UTC-aware, just get max directly
            max_ts = df.select(pl.col(timestamp_col).max()).item()

            return max_ts
        except Exception as e:
            logger.warning(
                "Could not get max timestamp",
                exc_info=True,
            )
            return None

    def read_after_watermark(
        self,
        watermark: datetime,
        timestamp_col: str = "ingested_at",
        limit: int | None = None,
    ) -> pl.DataFrame:
        """
        Read events after watermark timestamp.

        Args:
            watermark: Read events after this timestamp (UTC-aware)
            timestamp_col: Timestamp column name
            limit: Optional row limit

        Returns:
            DataFrame of events
        """
        # Both watermark and column are UTC-aware, comparison works directly
        filter_expr = pl.col(timestamp_col) > pl.lit(watermark)
        result: pl.DataFrame = self.read_filtered(filter_expr, limit=limit)  # type: ignore[assignment]
        return result

    def read_by_status_subtypes(
        self,
        status_subtypes: list[str],
        watermark: datetime | None = None,
        timestamp_col: str = "ingested_at",
        limit: int | None = None,
        order_by: str | None = None,
        columns: list[str] | None = None,
        require_attachments: bool = False,
    ) -> pl.DataFrame:
        """
        Read events filtered by status_subtypes and optionally after a watermark.

        Uses Delta Lake's native partition pruning via filter pushdown.
        The event_date partition column is used to limit data scanned.

        Memory optimization: Use `columns` parameter to project only needed columns
        early in the query plan, significantly reducing memory for wide tables.

        Args:
            status_subtypes: List of status_subtype values to filter on
            watermark: Only read events after this timestamp (required)
            timestamp_col: Timestamp column name (default: ingested_at)
            limit: Optional row limit
            order_by: Optional column to sort by
            columns: Optional list of columns to select (projects early for memory efficiency)
            require_attachments: If True, filter to only rows with non-null, non-empty attachments

        Returns:
            Filtered DataFrame
        """
        if watermark is None:
            raise ValueError("Watermark required for this method")

        logger.debug(
            "Reading events by status subtypes",
            extra={
                "status_subtypes": status_subtypes,
                "watermark": watermark.isoformat(),
                "columns": columns,
                "require_attachments": require_attachments,
            },
        )

        opts = self.storage_options or get_storage_options()

        # Calculate date range for partition pruning
        # Ensure we use date type for partition column comparison
        watermark_date = watermark.date()
        today = datetime.now(UTC).date()

        # Build lazy scan with filter pushdown
        # Delta Lake will automatically prune partitions based on event_date filter
        lf = pl.scan_delta(self.table_path, storage_options=opts)

        # OPTIMIZATION: Project columns early to reduce memory footprint
        # This must happen before any operations that might materialize data
        if columns:
            # Ensure partition and filter columns are included for pushdown
            required_cols = {"event_date", timestamp_col, "status_subtype"}
            if require_attachments:
                required_cols.add("attachments")
            all_cols = list(set(columns) | required_cols)
            lf = lf.select(all_cols)

        # Build filter expression - separate partition filters for better pushdown
        # Partition filter (file-level pruning)
        partition_filter = (pl.col("event_date") >= watermark_date) & (
            pl.col("event_date") <= today
        )

        # Row-level filters
        row_filter = (pl.col(timestamp_col) > watermark) & (
            pl.col("status_subtype").is_in(status_subtypes)
        )

        # Optional attachments filter - push into query instead of post-filtering
        if require_attachments:
            row_filter = (
                row_filter
                & pl.col("attachments").is_not_null()
                & (pl.col("attachments") != "")
            )

        # Apply filters - partition filter first for better optimization hints
        lf = lf.filter(partition_filter).filter(row_filter)

        # OPTIMIZATION: When sorting with a limit, we must materialize all matching
        # rows to sort them. For large datasets, consider:
        # 1. Using a tighter time window (reduce watermark lookback)
        # 2. Skipping sort if approximate ordering is acceptable
        # 3. Using a two-phase approach for very large datasets
        if order_by and limit:
            logger.debug(
                "Sort with limit: will materialize all matching rows for sort",
                extra={
                    "order_by": order_by,
                    "limit": limit,
                },
            )
            lf = lf.sort(order_by).head(limit)
        elif order_by:
            lf = lf.sort(order_by)
        elif limit:
            # No sort - can apply limit early without full materialization
            lf = lf.head(limit)

        # Collect - streaming mode helps when no blocking operations (like sort)
        # CRITICAL: streaming=True can cause "invalid SlotMap key used" panic when
        # combined with sort(), so only enable streaming when no sort is present
        use_streaming = order_by is None
        result = lf.collect(streaming=use_streaming)

        logger.debug(
            "Read by status subtypes complete",
            extra={
                "rows_read": len(result),
                "status_subtypes": status_subtypes,
                "watermark_date": str(watermark_date),
                "today": str(today),
            },
        )

        return result


__all__ = [
    "DeltaTableReader",
    "DeltaTableWriter",
    "EventsTableReader",
    "DELTA_RETRY_CONFIG",
    "DELTA_CIRCUIT_CONFIG",
    "get_open_file_descriptors",
]
