"""
Delta table operations with retry and auth refresh support.

Read/write operations for Delta tables on OneLake/ADLS. All datetimes are UTC-aware.
Note: Query timeouts not supported; use time-bounded filters and partition pruning.

Migrated from verisk_pipeline.storage.delta for pipeline reorganization (REORG-502).
"""

from __future__ import annotations

import asyncio
import logging
import operator
import os
import warnings
from datetime import UTC, datetime
from typing import Any

import polars as pl
from deltalake import DeltaTable, write_deltalake

from core.resilience.circuit_breaker import CircuitBreakerConfig
from pipeline.common.auth import get_auth, get_storage_options
from pipeline.common.retry import RetryConfig, with_retry
from pipeline.common.storage.onelake import _refresh_all_credentials

logger = logging.getLogger(__name__)


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

    def __init__(self, table_path: str, storage_options: dict[str, str] | None = None):
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
        _FILTER_OPS = {
            "=": operator.eq,
            "!=": operator.ne,
            "<": operator.lt,
            ">": operator.gt,
            "<=": operator.le,
            ">=": operator.ge,
        }

        opts = self.storage_options or get_storage_options()

        lf = pl.scan_delta(self.table_path, storage_options=opts)

        # Apply filters with robust type casting
        if filters:
            for col, op, value in filters:
                try:
                    # Cast column to match value type for numeric comparisons
                    col_expr = pl.col(col)
                    if isinstance(value, int):
                        col_expr = col_expr.cast(pl.Int64, strict=False).fill_null(0)
                    elif isinstance(value, float):
                        col_expr = col_expr.cast(pl.Float64, strict=False).fill_null(0.0)

                    op_fn = _FILTER_OPS.get(op)
                    if op_fn is None:
                        raise ValueError(f"Unsupported filter operator: {op}")
                    lf = lf.filter(op_fn(col_expr, value))
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

    def _load_table(self, opts: dict[str, str]) -> DeltaTable | None:
        """Load a DeltaTable instance, returning None if it doesn't exist.

        Caches the instance for reuse across operations within a single
        append/merge call.  Call _invalidate_cached_table() on errors.
        """
        if self._delta_table is not None:
            return self._delta_table
        try:
            self._delta_table = DeltaTable(self.table_path, storage_options=opts)
            return self._delta_table
        except Exception:
            return None

    def _invalidate_cached_table(self) -> None:
        """Drop the cached DeltaTable so the next call re-reads metadata."""
        self._delta_table = None

    def _table_exists(self, opts: dict[str, str]) -> bool:
        return self._load_table(opts) is not None

    def _cast_null_columns(
        self, df: pl.DataFrame, target_type: pl.DataType = pl.Utf8
    ) -> pl.DataFrame:
        """Cast NULL-typed columns to target type to avoid Delta Lake errors."""
        for col in df.columns:
            if df[col].dtype == pl.Null:
                df = df.with_columns(pl.col(col).cast(target_type, strict=False).alias(col))
        return df

    def _build_cast_expr(
        self, col: str, source_dtype: pl.DataType, target_arrow_type: Any
    ) -> tuple[pl.Expr, str | None]:
        """Build a Polars cast expression for a single column.

        Returns (expression, mismatch_note). mismatch_note is None when no cast needed.
        """
        try:
            polars_type = self._arrow_type_to_polars(target_arrow_type)
        except Exception as e:
            return pl.col(col), f"{col}: cast failed ({e})"

        if polars_type is None:
            return pl.col(col), f"{col}: source={source_dtype}, target={target_arrow_type} (unmapped)"

        if source_dtype == pl.Null:
            return pl.col(col).cast(polars_type, strict=False).alias(col), f"{col}: Null -> {polars_type}"

        if source_dtype == polars_type:
            return pl.col(col), None

        note = f"{col}: {source_dtype} -> {polars_type}"

        # Datetime timezone conversions need special handling (cast() doesn't convert TZ)
        if isinstance(source_dtype, pl.Datetime) and isinstance(polars_type, pl.Datetime):
            expr = self._build_datetime_cast(col, source_dtype.time_zone, polars_type.time_zone)
            if expr is not None:
                return expr, note

        return pl.col(col).cast(polars_type, strict=False).alias(col), note

    @staticmethod
    def _build_datetime_cast(
        col: str, source_tz: str | None, target_tz: str | None
    ) -> pl.Expr | None:
        """Build a timezone-aware datetime cast, or None if no TZ difference."""
        if source_tz == target_tz:
            return None
        if target_tz is None:
            return pl.col(col).dt.replace_time_zone(None).alias(col)
        if source_tz is None:
            return pl.col(col).dt.replace_time_zone(target_tz).alias(col)
        return pl.col(col).dt.convert_time_zone(target_tz).alias(col)

    def _build_schema_cast_exprs(
        self,
        df: pl.DataFrame,
        target_schema: Any,
    ) -> tuple[list[pl.Expr], list[str], list[str]]:
        """Diff source/target schemas and build cast expressions in target column order.

        Returns (cast_exprs, extra_cols, mismatches).
        """
        source_columns = set(df.columns)
        target_names: set[str] = set()

        cast_exprs: list[pl.Expr] = []
        mismatches: list[str] = []
        for field in target_schema.fields:
            target_names.add(field.name)
            if field.name not in source_columns:
                continue
            expr, note = self._build_cast_expr(field.name, df[field.name].dtype, field.type)
            cast_exprs.append(expr)
            if note:
                mismatches.append(note)

        # Append extra source columns not in target (schema evolution handles them)
        extra_cols = [col for col in df.columns if col not in target_names]
        cast_exprs.extend(pl.col(col) for col in extra_cols)

        return cast_exprs, extra_cols, mismatches

    def _align_schema_with_target(
        self,
        df: pl.DataFrame,
        opts: dict[str, str],
        dt: DeltaTable,
    ) -> pl.DataFrame:
        """Aligns DataFrame schema with target table to prevent type coercion errors during merge."""
        try:
            target_schema = dt.schema()
            cast_exprs, extra_cols, mismatches = self._build_schema_cast_exprs(
                df, target_schema
            )

            if cast_exprs:
                df = df.select(cast_exprs)

            log_level = logging.WARNING if mismatches else logging.DEBUG
            logger.log(
                log_level,
                "Aligned source schema with target",
                extra={
                    "source_columns": len(df.columns),
                    "target_columns": len(target_schema.fields),
                    "casts_applied": len(mismatches),
                    "extra_source_cols": extra_cols or None,
                    "type_changes": mismatches or None,
                },
            )

            return df

        except Exception as e:
            logger.warning(
                "Could not align schema with target, proceeding with original",
                extra={"error_message": str(e)[:200]},
            )
            return df

    @staticmethod
    def _arrow_type_to_polars(arrow_type: Any) -> pl.DataType | None:
        """
        Convert Arrow type to Polars type for schema alignment.

        Args:
            arrow_type: PyArrow type from Delta schema

        Returns:
            Corresponding Polars type, or None if unknown
        """
        import pyarrow as pa

        # Exact Arrow type matches (checked first for precision)
        _EXACT_MAP = {
            pa.string(): pl.Utf8,
            pa.large_string(): pl.Utf8,
            pa.int64(): pl.Int64,
            pa.int32(): pl.Int32,
            pa.int16(): pl.Int16,
            pa.int8(): pl.Int8,
            pa.float64(): pl.Float64,
            pa.float32(): pl.Float32,
            pa.bool_(): pl.Boolean,
            pa.date32(): pl.Date,
            pa.date64(): pl.Date,
            pa.binary(): pl.Binary,
            pa.large_binary(): pl.Binary,
            pa.null(): pl.Null,
        }

        exact = _EXACT_MAP.get(arrow_type)
        if exact is not None:
            return exact

        # String-based fallback for complex/parameterized types
        type_str = str(arrow_type).lower()

        # Timestamp types - always use UTC for consistency
        if "timestamp" in type_str:
            return pl.Datetime("us", "UTC")

        # Keyword-based fallback lookup (order matters: check timestamp before date)
        _STR_CHECKS: list[tuple[str, pl.DataType]] = [
            ("string", pl.Utf8),
            ("utf8", pl.Utf8),
            ("int64", pl.Int64),
            ("int32", pl.Int32),
            ("float64", pl.Float64),
            ("double", pl.Float64),
            ("float32", pl.Float32),
            ("bool", pl.Boolean),
            ("binary", pl.Binary),
            ("decimal", pl.Float64),
            ("null", pl.Null),
        ]
        for keyword, polars_type in _STR_CHECKS:
            if keyword in type_str:
                return polars_type

        # "date" must exclude "timestamp" (already handled above)
        if "date" in type_str:
            return pl.Date

        # Exact string fallbacks for aliases
        _ALIAS_MAP = {"long": pl.Int64, "int": pl.Int32, "float": pl.Float32}
        alias = _ALIAS_MAP.get(type_str)
        if alias is not None:
            return alias

        return None

    def _prepare_dataframe_for_append(
        self, df: pl.DataFrame, opts: dict[str, str]
    ) -> tuple[pl.DataFrame, list[str] | None]:
        """Align schema, resolve partitions for append.

        Uses a single cached DeltaTable instance for all metadata reads.

        Returns (prepared_df, partition_by).
        """
        dt = self._load_table(opts)

        if dt is not None:
            # Table exists — align schema and read partitions
            df = self._align_schema_with_target(df, opts, dt=dt)

            partition_by = None
            try:
                existing_partitions = dt.metadata().partition_columns
                partition_by = existing_partitions if existing_partitions else None
            except Exception:
                partition_by = [self.partition_column] if self.partition_column else None
        else:
            # New table — cast nulls and use configured partitioning
            df = self._cast_null_columns(df)
            partition_by = [self.partition_column] if self.partition_column else None

        return df, partition_by

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

        # Invalidate cached table so we read fresh metadata for this write
        self._invalidate_cached_table()
        df, partition_by = self._prepare_dataframe_for_append(df, opts)

        logger.info(
            "Delta write details",
            extra={
                "batch_id": batch_id,
                "table_path": self.table_path,
                "partition_by": partition_by,
                "partition_column_config": self.partition_column,
                "df_columns": df.columns,
                "df_schema": {
                    col: str(dtype) for col, dtype in zip(df.columns, df.dtypes, strict=False)
                },
                "rows_to_write": len(df),
                "first_row_sample": df.head(1).to_dicts()[0] if not df.is_empty() else {},
            },
        )

        write_deltalake(
            self.table_path,
            df.to_arrow(),
            mode="append",
            schema_mode="merge",
            storage_options=opts,
            partition_by=partition_by,
        )  # type: ignore[call-overload]

        logger.info(
            "Delta write completed",
            extra={
                "batch_id": batch_id,
                "rows_written": len(df),
            },
        )

        return len(df)

    def _deduplicate_batch(
        self, df: pl.DataFrame, merge_keys: list[str]
    ) -> pl.DataFrame:
        """Combine rows with the same merge keys; later non-null values win."""
        initial_len = len(df)

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

        del rows
        df = pl.DataFrame(list(merged.values()), infer_schema_length=None)
        del merged

        if len(df) < initial_len:
            logger.debug(
                "Deduped batch",
                extra={
                    "records_processed": initial_len,
                    "rows_written": len(df),
                },
            )

        return df

    def _build_merge_conditions(
        self,
        df_columns: list[str],
        merge_keys: list[str],
        preserve_columns: list[str],
    ) -> tuple[str, dict[str, str], dict[str, str]]:
        """Build the predicate, update dict, and insert dict for a Delta merge.

        Returns (predicate, update_dict, insert_dict).
        """
        predicate = " AND ".join(f"target.{k} = source.{k}" for k in merge_keys)

        update_cols = [c for c in df_columns if c not in merge_keys and c not in preserve_columns]
        update_dict = {c: f"source.{c}" for c in update_cols}

        insert_dict = {c: f"source.{c}" for c in df_columns}

        return predicate, update_dict, insert_dict

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

        df = self._deduplicate_batch(df, merge_keys)

        opts = get_storage_options()

        # Single DeltaTable load for existence check, schema alignment, and merge
        self._invalidate_cached_table()
        dt = self._load_table(opts)

        if dt is not None:
            # Align source schema with target to prevent type coercion errors in CASE WHEN
            # This also handles NULL-typed columns by casting to correct target type
            df = self._align_schema_with_target(df, opts, dt=dt)
        else:
            # Table doesn't exist yet - cast NULL columns to Utf8 as default
            df = self._cast_null_columns(df)

            # Create table
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
            self._invalidate_cached_table()
            logger.info(
                "Created table",
                extra={"rows_written": len(df)},
            )
            return len(df)

        predicate, update_dict, insert_dict = self._build_merge_conditions(
            df.columns, merge_keys, preserve_columns
        )

        pa_df = df.to_arrow()
        del df

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

        del pa_df
        self._invalidate_cached_table()

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
        for batch_num, batch_start in enumerate(range(0, total_rows, batch_size), start=1):
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

    def get_max_timestamp(self, timestamp_col: str = "ingested_at") -> datetime | None:
        """Get maximum timestamp from table."""
        try:
            df: pl.DataFrame = self.read(columns=[timestamp_col])  # type: ignore[assignment]
            if df is None or df.is_empty():
                return None

            # Column is UTC-aware, just get max directly
            max_ts = df.select(pl.col(timestamp_col).max()).item()

            return max_ts
        except Exception:
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
                row_filter & pl.col("attachments").is_not_null() & (pl.col("attachments") != "")
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
