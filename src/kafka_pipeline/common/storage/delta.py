"""
Delta table operations with retry and auth refresh support.

Read/write operations for Delta tables on OneLake/ADLS. All datetimes are UTC-aware.
Note: Query timeouts not supported; use time-bounded filters and partition pruning.

Migrated from verisk_pipeline.storage.delta for kafka_pipeline reorganization (REORG-502).
"""

from __future__ import annotations

import inspect
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import polars as pl
from deltalake import DeltaTable, write_deltalake as _write_deltalake

from kafka_pipeline.common.auth import get_storage_options
from kafka_pipeline.common.retry import RetryConfig, with_retry
from kafka_pipeline.common.storage.onelake import _refresh_all_credentials
from core.resilience.circuit_breaker import CircuitBreakerConfig
from core.logging import get_logger, log_with_context
from kafka_pipeline.common.logging import logged_operation, LoggedClass

logger = get_logger(__name__)


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


@dataclass
class WriteOperation:
    """Write operation for idempotency tracking. Token is a UUID uniquely identifying the operation."""

    token: str
    table_path: str
    timestamp: datetime
    row_count: int


def _on_auth_error() -> None:
    """Callback for auth errors - clears all credential caches."""
    _refresh_all_credentials()


class DeltaTableReader(LoggedClass):
    """
    Reader for Delta tables with auth retry support.

    Usage:
        reader = DeltaTableReader("abfss://workspace@onelake/lakehouse/Tables/events")
        df = reader.read()
        df = reader.read(columns=["trace_id", "ingested_at"])
    """

    def __init__(
        self, table_path: str, storage_options: Optional[Dict[str, str]] = None
    ):
        """
        Args:
            table_path: Full abfss:// path to Delta table
            storage_options: Optional storage options (default: from get_storage_options())
        """
        self.table_path = table_path
        self.storage_options = storage_options
        super().__init__()

    @logged_operation(level=logging.DEBUG)
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def read(
        self,
        columns: Optional[List[str]] = None,
    ) -> pl.DataFrame:
        """
        Read entire Delta table.

        Args:
            columns: Optional list of columns to read (None = all)

        Returns:
            Polars DataFrame
        """
        opts = self.storage_options or get_storage_options()

        df = pl.read_delta(
            self.table_path,
            storage_options=opts,
            columns=columns,
        )

        # Batch size validation (Task E.1)
        max_batch_size = 50000  # Default max batch size for reads
        if len(df) > max_batch_size:
            self._log(
                logging.WARNING,
                "Read batch exceeds configured limit",
                rows_read=len(df),
                max_batch_size=max_batch_size,
                table_path=self.table_path,
            )

        self._log(logging.DEBUG, "Read complete", rows_read=len(df))
        return df

    @logged_operation(level=logging.DEBUG)
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def scan(
        self,
        columns: Optional[List[str]] = None,
    ) -> pl.LazyFrame:
        """
        Create lazy scan of Delta table.

        Args:
            columns: Optional list of columns to select

        Returns:
            Polars LazyFrame for query building
        """
        opts = self.storage_options or get_storage_options()

        lf = pl.scan_delta(self.table_path, storage_options=opts)

        if columns:
            lf = lf.select(columns)

        return lf

    @logged_operation(level=logging.DEBUG)
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def read_filtered(
        self,
        filter_expr: pl.Expr,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
        descending: bool = False,
    ) -> pl.DataFrame:
        """
        Read Delta table with filter pushdown.

        Args:
            filter_expr: Polars filter expression
            columns: Optional list of columns
            limit: Optional row limit
            order_by: Optional column to sort by (applied before limit)
            descending: Sort descending if True

        Returns:
            Filtered Polars DataFrame
        """
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
            self._log(
                logging.WARNING,
                "read_filtered called with limit but no order_by - results may be non-deterministic",
                limit=limit,
            )

        # streaming=True can cause "invalid SlotMap key used" panic with sort()
        use_streaming = order_by is None
        df = lf.collect(streaming=use_streaming)
        self._log(logging.DEBUG, "Read filtered complete", rows_read=len(df))
        return df

    @logged_operation(operation_name="read_as_polars")
    def read_as_polars(
        self,
        filters: Optional[List[Tuple[str, str, Any]]] = None,
        columns: Optional[List[str]] = None,
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
            self._log(
                logging.DEBUG,
                "Table does not exist or is not readable",
                error_message=str(e)[:200],
            )
            return False


class DeltaTableWriter(LoggedClass):
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
        partition_column: Optional[str] = None,
        z_order_columns: Optional[List[str]] = None,
    ):
        self.table_path = table_path
        self.timestamp_column = timestamp_column
        self.partition_column = partition_column
        self.z_order_columns = z_order_columns or []
        self._reader = DeltaTableReader(table_path)
        self._optimization_scheduler: Optional[Any] = None

        super().__init__()

    def _table_exists(self, opts: Dict[str, str]) -> bool:
        """Check if Delta table exists."""
        try:
            DeltaTable(self.table_path, storage_options=opts)
            return True
        except Exception:
            return False

    def _align_schema_with_target(
        self, df: pl.DataFrame, opts: Dict[str, str]
    ) -> pl.DataFrame:
        """
        Align source DataFrame schema with target table schema.

        This prevents type coercion errors during Delta merge operations.
        When Delta builds CASE WHEN expressions for merge, it needs compatible
        types between source and target columns. This method:
        1. Reorders columns to match target table column order
        2. Casts source columns to match target types
        3. Adds extra source columns at the end (schema evolution handles adds)

        Args:
            df: Source DataFrame to align
            opts: Storage options for Delta table access

        Returns:
            DataFrame with schema aligned to target table
        """
        try:
            dt = DeltaTable(self.table_path, storage_options=opts)
            target_schema = dt.schema()

            # Build ordered list of target column names and mapping to Arrow types
            target_column_order: List[str] = []
            target_types: Dict[str, Any] = {}
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
                        elif source_dtype != polars_type:
                            mismatches.append(
                                f"{col}: {source_dtype} -> {polars_type}"
                            )
                            # Special handling for datetime timezone conversions
                            # Polars cast() doesn't handle timezone conversion properly
                            if (
                                isinstance(source_dtype, pl.Datetime)
                                and isinstance(polars_type, pl.Datetime)
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
                                        pl.col(col).cast(polars_type, strict=False).alias(col)
                                    )
                            else:
                                cast_exprs.append(
                                    pl.col(col).cast(polars_type, strict=False).alias(col)
                                )
                        else:
                            cast_exprs.append(pl.col(col))
                    except Exception as e:
                        # If cast fails, keep original column
                        mismatches.append(
                            f"{col}: cast failed ({e})"
                        )
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
            self._log(
                log_level,
                "Aligned source schema with target",
                source_columns=len(source_columns),
                target_columns=len(target_types),
                casts_applied=len(mismatches),
                extra_source_cols=extra_cols if extra_cols else None,
                type_changes=mismatches if mismatches else None,
            )

            return df

        except Exception as e:
            self._log(
                logging.WARNING,
                "Could not align schema with target, proceeding with original",
                error_message=str(e)[:200],
            )
            return df

    def _arrow_type_to_polars(self, arrow_type: Any) -> Optional[pl.DataType]:
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

    @logged_operation(level=logging.INFO)
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def append(
        self,
        df: pl.DataFrame,
        batch_id: Optional[str] = None,
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
            self._log(logging.DEBUG, "No data to write", batch_id=batch_id)
            return 0

        # Write to Delta - convert to Arrow for write_deltalake
        opts = get_storage_options()

        # Align source schema with target to ensure column order matches
        if self._table_exists(opts):
            df = self._align_schema_with_target(df, opts)

        # Cast any remaining null-typed columns to string to avoid Delta Lake errors.
        # This can happen when all values in a column are None (e.g., contacts batch
        # with only CLAIM_REP entries where first_name/last_name are all null).
        # Delta's schema evolution will handle type coercion during write.
        for col in df.columns:
            if df[col].dtype == pl.Null:
                df = df.with_columns(pl.col(col).cast(pl.Utf8))

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
                partition_by = [self.partition_column] if self.partition_column else None
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

    @logged_operation(level=logging.INFO)
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def merge(
        self,
        df: pl.DataFrame,
        merge_keys: List[str],
        preserve_columns: Optional[List[str]] = None,
        update_condition: Optional[str] = None,
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

        if preserve_columns is None:
            preserve_columns = ["created_at"]

        initial_len = len(df)

        # Check if we need to dedupe within batch (avoid expensive dict ops if not needed)
        unique_keys = df.select(merge_keys).unique()
        needs_dedupe = len(unique_keys) < initial_len
        del unique_keys  # Free immediately

        if needs_dedupe:
            # Combine rows within batch by merge keys (later non-null values overlay earlier)
            # This is expensive but necessary when batch has duplicate keys
            rows = df.to_dicts()
            merged: Dict[tuple, dict] = {}
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
            self._log(
                logging.DEBUG,
                "Deduped batch",
                records_processed=initial_len,
                rows_written=len(df),
            )

        # Cast any null-typed columns to string (they'll get cast again by Delta)
        for col in df.columns:
            if df[col].dtype == pl.Null:
                df = df.with_columns(pl.col(col).cast(pl.Utf8))

        opts = get_storage_options()

        # Align source schema with target to prevent type coercion errors in CASE WHEN
        if self._table_exists(opts):
            df = self._align_schema_with_target(df, opts)

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
            self._log(
                logging.INFO,
                "Created table",
                rows_written=len(df),
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

        self._log(
            logging.DEBUG,
            "Merge complete",
            rows_merged=rows_inserted + rows_updated,
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
        )

        return rows_inserted + rows_updated

    @logged_operation(level=logging.DEBUG)
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def upsert_with_idempotency(
        self,
        df: pl.DataFrame,
        merge_key: str,
        event_id_field: str = "_event_id",
    ) -> int:
        """
        Upsert with deduplication by event ID.

        Useful for idempotent processing where the same event may be
        processed multiple times but should only result in one row.

        Args:
            df: Data to upsert
            merge_key: Primary key column for merge (e.g., "project_id")
            event_id_field: Column containing event ID for dedup

        Returns:
            Number of rows written
        """
        if df.is_empty():
            self._log(logging.DEBUG, "No data to upsert")
            return 0

        initial_count = len(df)

        # Phase 1: Dedupe within batch by event_id (keep last occurrence)
        if event_id_field in df.columns:
            df = df.unique(subset=[event_id_field], keep="last")
            if len(df) < initial_count:
                self._log(
                    logging.DEBUG,
                    "Removed duplicates by event_id",
                    dedupe_field=event_id_field,
                    records_processed=initial_count - len(df),
                )

        # Phase 2: Dedupe within batch by merge_key (keep last occurrence)
        if merge_key in df.columns:
            before = len(df)
            df = df.unique(subset=[merge_key], keep="last")
            if len(df) < before:
                self._log(
                    logging.DEBUG,
                    "Removed duplicates by merge_key",
                    dedupe_field=merge_key,
                    records_processed=before - len(df),
                )

        if df.is_empty():
            return 0

        # Write using append mode (no dedup at write time)
        rows_written: int = self.append(df)  # type: ignore[assignment]
        return rows_written


    @logged_operation(level=logging.INFO)
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
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
            self._log(logging.DEBUG, "No data to merge")
            return 0

        # Get batch size from config if not provided
        if batch_size is None:
            batch_size = 100000  # Default max batch size for merges

        total_rows = len(df)

        # Single batch optimization
        if total_rows <= batch_size:
            self._log(logging.INFO, "Single batch merge", total_rows=total_rows)
            return self.merge(
                df,
                merge_keys=merge_keys,
                preserve_columns=preserve_columns,
            )

        # Multi-batch merge
        num_batches = (total_rows + batch_size - 1) // batch_size
        self._log(
            logging.INFO,
            "Batched merge starting",
            total_rows=total_rows,
            batch_size=batch_size,
            num_batches=num_batches,
        )

        rows_merged = 0
        for batch_num, batch_start in enumerate(
            range(0, total_rows, batch_size), start=1
        ):
            batch_end = min(batch_start + batch_size, total_rows)
            batch_df = df.slice(batch_start, batch_end - batch_start)

            self._log(
                logging.INFO,
                f"Merging batch {batch_num}/{num_batches}",
                batch_start=batch_start,
                batch_end=batch_end,
                batch_rows=len(batch_df),
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
            self._log(
                logging.INFO,
                "Merge progress",
                rows_processed=batch_end,
                total_rows=total_rows,
                progress_pct=round(progress_pct, 1),
            )

        self._log(
            logging.INFO,
            "Batched merge complete",
            rows_merged=rows_merged,
            total_rows=total_rows,
        )
        return rows_merged

    @logged_operation(level=logging.DEBUG)
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def write_rows(self, rows: List[dict], schema: Optional[dict] = None) -> int:
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

        if schema:
            df = pl.DataFrame(rows, schema=schema)
        else:
            df = pl.DataFrame(rows)

        result: int = self.append(df)  # type: ignore[assignment]
        return result

    @logged_operation(level=logging.INFO)
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def write_with_idempotency(
        self,
        df: pl.DataFrame,
        operation_token: Optional[str] = None,
    ) -> WriteOperation:
        """
        Write data with idempotency token to prevent duplicate writes (Task G.3).

        Uses a separate idempotency tracking table to record write tokens.
        If the same token is used again, the write is skipped.

        Args:
            df: Data to write
            operation_token: Optional idempotency token (UUID generated if not provided)

        Returns:
            WriteOperation containing token and write metadata

        Raises:
            Exception: On write failure after retries
        """
        # Generate token if not provided
        if operation_token is None:
            operation_token = str(uuid.uuid4())

        # Check if this operation was already completed
        if self._is_duplicate(operation_token):
            self._log(
                logging.INFO,
                "Skipping duplicate write operation",
                operation_token=operation_token,
                table_path=self.table_path,
            )
            return WriteOperation(
                token=operation_token,
                table_path=self.table_path,
                timestamp=datetime.now(timezone.utc),
                row_count=0,
            )

        # Perform the write
        rows_written = self.append(df)

        # Record token to prevent future duplicates
        write_op = WriteOperation(
            token=operation_token,
            table_path=self.table_path,
            timestamp=datetime.now(timezone.utc),
            row_count=rows_written,
        )
        self._record_token(write_op)

        return write_op

    def _is_duplicate(self, token: str) -> bool:
        """Check if operation token exists in idempotency table (Task G.3).

        Args:
            token: Operation token to check

        Returns:
            True if token exists (duplicate operation), False otherwise
        """
        try:
            # Idempotency table path
            idempotency_table = f"{self.table_path}_idempotency"

            # Check if idempotency table exists
            opts = get_storage_options()
            try:
                DeltaTable(idempotency_table, storage_options=opts)
            except Exception:
                # Table doesn't exist yet
                return False

            # Read idempotency table and check for token
            reader = DeltaTableReader(idempotency_table)
            if not reader.exists():
                return False

            df = reader.read(columns=["token"])
            if df.is_empty():
                return False

            return token in df["token"].to_list()

        except Exception as e:
            self._log_exception(
                e,
                "Error checking idempotency token",
                level=logging.WARNING,
                token=token,
            )
            # On error, assume not duplicate (fail open for writes)
            return False

    def _record_token(self, operation: WriteOperation) -> None:
        """Record operation token in idempotency table (Task G.3).

        Args:
            operation: WriteOperation to record
        """
        try:
            # Idempotency table path
            idempotency_table = f"{self.table_path}_idempotency"

            # Create record
            record_df = pl.DataFrame(
                {
                    "token": [operation.token],
                    "table_path": [operation.table_path],
                    "timestamp": [operation.timestamp],
                    "row_count": [operation.row_count],
                }
            )

            # Write to idempotency table
            opts = get_storage_options()
            write_deltalake(
                idempotency_table,
                record_df.to_arrow(),
                mode="append",
                schema_mode="merge",
                storage_options=opts,
            )  # type: ignore[call-overload]

            self._log(
                logging.DEBUG,
                "Recorded idempotency token",
                token=operation.token,
                idempotency_table=idempotency_table,
            )

        except Exception as e:
            self._log_exception(
                e,
                "Error recording idempotency token",
                level=logging.WARNING,
                token=operation.token,
            )
            # Don't raise - write succeeded, token recording is best-effort


class EventsTableReader(DeltaTableReader):
    """
    Specialized reader for the events table.

    Provides convenience methods for common event queries.
    """

    def __init__(self, table_path: str):
        super().__init__(table_path)

    def get_max_timestamp(
        self, timestamp_col: str = "ingested_at"
    ) -> Optional[datetime]:
        """Get maximum timestamp from table."""
        try:
            df: pl.DataFrame = self.read(columns=[timestamp_col])  # type: ignore[assignment]
            if df is None or df.is_empty():
                return None

            # Column is UTC-aware, just get max directly
            max_ts = df.select(pl.col(timestamp_col).max()).item()

            return max_ts
        except Exception as e:
            self._log_exception(
                e,
                "Could not get max timestamp",
                level=logging.WARNING,
            )
            return None

    def read_after_watermark(
        self,
        watermark: datetime,
        timestamp_col: str = "ingested_at",
        limit: Optional[int] = None,
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

    @logged_operation(operation_name="read_by_status_subtypes")
    def read_by_status_subtypes(
        self,
        status_subtypes: List[str],
        watermark: Optional[datetime] = None,
        timestamp_col: str = "ingested_at",
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
        columns: Optional[List[str]] = None,
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

        self._log(
            logging.DEBUG,
            "Reading events by status subtypes",
            status_subtypes=status_subtypes,
            watermark=watermark.isoformat(),
            columns=columns,
            require_attachments=require_attachments,
        )

        opts = self.storage_options or get_storage_options()

        # Calculate date range for partition pruning
        # Ensure we use date type for partition column comparison
        watermark_date = watermark.date()
        today = datetime.now(timezone.utc).date()

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
            self._log(
                logging.DEBUG,
                "Sort with limit: will materialize all matching rows for sort",
                order_by=order_by,
                limit=limit,
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

        self._log(
            logging.DEBUG,
            "Read by status subtypes complete",
            rows_read=len(result),
            status_subtypes=status_subtypes,
            watermark_date=str(watermark_date),
            today=str(today),
        )

        return result


# Z-Ordering Helper Functions and Constants (P3.2)

RECOMMENDED_Z_ORDER_COLUMNS = {
    "inventory": [
        "created_date",  # Time-based queries
        "status",  # Status filtering
        "trace_id",  # Lookup by trace
    ],
    "retry_queue": [
        "retry_count",  # Retry filtering
        "error_type",  # Error analysis
        "created_date",  # Time-based queries
    ],
    "attachments": [
        "download_status",  # Status filtering
        "attachment_url",  # Lookup by URL
        "created_date",  # Time-based queries
    ],
}
"""Recommended Z-order columns by table type for query optimization."""


def suggest_z_order_columns(
    table_path: str, query_patterns: Optional[List[str]] = None
) -> List[str]:
    """
    Suggest Z-order columns based on query patterns or table type (P3.2).

    Z-ordering improves query performance by co-locating related data.
    This function suggests appropriate columns based on common query
    patterns for known table types.

    Args:
        table_path: Path to Delta table
        query_patterns: Optional list of common filter columns

    Returns:
        Suggested Z-order columns

    Example:
        # Use explicit query patterns
        columns = suggest_z_order_columns(
            "path/to/table",
            query_patterns=["created_date", "status"]
        )

        # Infer from table name
        columns = suggest_z_order_columns("inventory_table")
        # Returns: ["created_date", "status", "trace_id"]
    """
    # If query patterns provided, use those
    if query_patterns:
        return query_patterns

    # Otherwise, use common patterns for this domain
    table_path_lower = table_path.lower()

    # Try to infer from table name
    for pattern_name, columns in RECOMMENDED_Z_ORDER_COLUMNS.items():
        if pattern_name in table_path_lower:
            logger.info(
                "Suggested Z-order columns for table",
                extra={
                    "table": table_path,
                    "pattern": pattern_name,
                    "columns": columns,
                },
            )
            return columns

    # Default: use timestamp columns
    logger.info(
        "No specific pattern matched, using default Z-order columns",
        extra={"table": table_path, "default_columns": ["created_date"]},
    )
    return ["created_date"]


__all__ = [
    "DeltaTableReader",
    "DeltaTableWriter",
    "EventsTableReader",
    "WriteOperation",
    "DELTA_RETRY_CONFIG",
    "DELTA_CIRCUIT_CONFIG",
    "suggest_z_order_columns",
    "RECOMMENDED_Z_ORDER_COLUMNS",
]
