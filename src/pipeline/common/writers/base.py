"""
Base class for Delta table writers.

Provides common functionality for all Delta writers:
- Async wrapper methods for non-blocking operations via subprocess
- Common error handling and logging patterns

Delta writes run in a subprocess (ProcessPoolExecutor) so that the GIL held
by the PyO3/Rust write_deltalake() call does not block the parent process's
health-check thread from responding to K8s liveness probes.

Subclasses should inherit from BaseDeltaWriter and implement domain-specific
data transformation methods.
"""

import asyncio
import atexit
import io
import logging
import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool

import polars as pl

from pipeline.common.metrics import delta_write_duration_seconds


# ---------------------------------------------------------------------------
# Module-level subprocess functions (must be top-level for pickling)
# ---------------------------------------------------------------------------

def _subprocess_delta_append(
    table_path: str,
    df_ipc_bytes: bytes,
    timestamp_column: str,
    partition_column: str | None,
    z_order_columns: list[str],
    batch_id: str | None,
) -> int:
    """Run DeltaTableWriter.append in a subprocess."""
    from pipeline.common.storage.delta import DeltaTableWriter

    df = pl.read_ipc(io.BytesIO(df_ipc_bytes))
    writer = DeltaTableWriter(
        table_path=table_path,
        timestamp_column=timestamp_column,
        partition_column=partition_column,
        z_order_columns=z_order_columns,
    )
    return writer.append(df, batch_id=batch_id)


def _subprocess_delta_merge(
    table_path: str,
    df_ipc_bytes: bytes,
    timestamp_column: str,
    partition_column: str | None,
    z_order_columns: list[str],
    merge_keys: list[str],
    preserve_columns: list[str] | None,
    update_condition: str | None,
) -> int:
    """Run DeltaTableWriter.merge in a subprocess."""
    from pipeline.common.storage.delta import DeltaTableWriter

    df = pl.read_ipc(io.BytesIO(df_ipc_bytes))
    writer = DeltaTableWriter(
        table_path=table_path,
        timestamp_column=timestamp_column,
        partition_column=partition_column,
        z_order_columns=z_order_columns,
    )
    return writer.merge(
        df,
        merge_keys=merge_keys,
        preserve_columns=preserve_columns,
        update_condition=update_condition,
    )


# ---------------------------------------------------------------------------
# Process pool management
# ---------------------------------------------------------------------------

_process_pool: ProcessPoolExecutor | None = None


def _get_delta_process_pool() -> ProcessPoolExecutor:
    """Lazily create a shared ProcessPoolExecutor for delta writes."""
    global _process_pool
    if _process_pool is None:
        _process_pool = ProcessPoolExecutor(max_workers=2)
        atexit.register(_shutdown_delta_process_pool)
    return _process_pool


def _shutdown_delta_process_pool() -> None:
    global _process_pool
    if _process_pool is not None:
        _process_pool.shutdown(wait=False)
        _process_pool = None


def _reset_delta_process_pool() -> None:
    """Replace a broken process pool with a fresh one."""
    global _process_pool
    if _process_pool is not None:
        try:
            _process_pool.shutdown(wait=False)
        except Exception:
            pass
    _process_pool = None


class BaseDeltaWriter:
    """
    Base class for Delta table writers with async support.

    Delta writes execute in a subprocess via ProcessPoolExecutor so the
    parent process's GIL is never held by write_deltalake(), allowing
    the health-check thread to respond to K8s probes.

    Subclasses should:
    1. Call super().__init__() with table_path and optional params
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

        self._timestamp_column = timestamp_column
        self._partition_column = partition_column
        self._z_order_columns = z_order_columns or []

        self.logger.info(
            f"Initialized {self.__class__.__name__}",
            extra={
                "table_path": table_path,
                "z_order_columns": z_order_columns,
            },
        )

    def _serialize_df(self, df: pl.DataFrame) -> bytes:
        """Serialize a Polars DataFrame to Arrow IPC bytes for subprocess transfer."""
        buf = io.BytesIO()
        df.write_ipc(buf)
        return buf.getvalue()

    async def _async_append(
        self,
        df: pl.DataFrame,
        batch_id: str | None = None,
    ) -> bool:
        """
        Append DataFrame to Delta table (non-blocking, subprocess).

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
            loop = asyncio.get_running_loop()
            pool = _get_delta_process_pool()
            rows_written = await loop.run_in_executor(
                pool,
                _subprocess_delta_append,
                self.table_path,
                self._serialize_df(df),
                self._timestamp_column,
                self._partition_column,
                self._z_order_columns,
                batch_id,
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

        except BrokenProcessPool:
            _reset_delta_process_pool()
            raise

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
        Merge DataFrame into Delta table (non-blocking upsert, subprocess).

        Args:
            df: Polars DataFrame to merge
            merge_keys: Columns forming primary key for merge
            preserve_columns: Columns to preserve on update (default: ["created_at"])
            update_condition: Optional SQL predicate for when to update matched rows.

        Returns:
            True if merge succeeded, False otherwise
        """
        if df.is_empty():
            return True

        table_name = self.table_path.rstrip("/").split("/")[-1]

        try:
            start = time.monotonic()
            loop = asyncio.get_running_loop()
            pool = _get_delta_process_pool()
            rows_affected = await loop.run_in_executor(
                pool,
                _subprocess_delta_merge,
                self.table_path,
                self._serialize_df(df),
                self._timestamp_column,
                self._partition_column,
                self._z_order_columns,
                merge_keys,
                preserve_columns,
                update_condition,
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

        except BrokenProcessPool:
            _reset_delta_process_pool()
            raise

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
