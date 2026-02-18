"""
Unit tests for Delta table writers.

Test Coverage:
    - BaseDeltaWriter initialization and configuration
    - Async append operations (empty df, success, failure, BrokenProcessPool)
    - Async merge operations (empty df, success, failure, BrokenProcessPool)
    - DeltaWriter convenience methods (records, dataframes)
    - Error handling and logging
    - Batch ID correlation

Mocks subprocess functions - focuses on async wrapper behavior.
"""

from concurrent.futures.process import BrokenProcessPool
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from pipeline.common.writers.base import BaseDeltaWriter
from pipeline.common.writers.delta_writer import DeltaWriter


# Patch target prefix for subprocess functions in base module
_BASE = "pipeline.common.writers.base"


def _make_writer(**kwargs):
    """Create a BaseDeltaWriter without side effects."""
    defaults = {"table_path": "abfss://workspace@onelake/table"}
    defaults.update(kwargs)
    return BaseDeltaWriter(**defaults)


class TestBaseDeltaWriterInitialization:
    """Test BaseDeltaWriter initialization."""

    def test_initialization_with_defaults(self):
        """BaseDeltaWriter initializes with default configuration."""
        writer = _make_writer()

        assert writer.table_path == "abfss://workspace@onelake/table"
        assert writer._timestamp_column == "ingested_at"
        assert writer._partition_column is None
        assert writer._z_order_columns == []

    def test_initialization_with_custom_config(self):
        """BaseDeltaWriter initializes with custom configuration."""
        writer = _make_writer(
            timestamp_column="created_at",
            partition_column="event_date",
            z_order_columns=["id", "created_at"],
        )

        assert writer.table_path == "abfss://workspace@onelake/table"
        assert writer._timestamp_column == "created_at"
        assert writer._partition_column == "event_date"
        assert writer._z_order_columns == ["id", "created_at"]

    def test_initialization_creates_logger(self):
        """BaseDeltaWriter creates logger with class name."""
        writer = _make_writer()

        assert writer.logger is not None
        assert writer.logger.name == "BaseDeltaWriter"


class TestBaseDeltaWriterAsyncAppend:
    """Test async append operations."""

    @pytest.mark.asyncio
    async def test_append_empty_dataframe_returns_true(self):
        """Async append with empty DataFrame returns True immediately."""
        writer = _make_writer()
        empty_df = pl.DataFrame({"id": [], "name": []})

        result = await writer._async_append(empty_df)

        assert result is True

    @pytest.mark.asyncio
    @patch(f"{_BASE}._get_delta_process_pool")
    async def test_append_success(self, mock_get_pool):
        """Async append succeeds and returns True."""
        mock_pool = MagicMock()
        mock_get_pool.return_value = mock_pool

        writer = _make_writer()
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.run_in_executor = AsyncMock(return_value=2)
            mock_loop_fn.return_value = mock_loop

            result = await writer._async_append(df)

            assert result is True
            call_args = mock_loop.run_in_executor.call_args[0]
            assert call_args[0] is mock_pool  # executor
            from pipeline.common.writers.base import _subprocess_delta_append
            assert call_args[1] is _subprocess_delta_append  # fn
            assert call_args[2] == "abfss://workspace@onelake/table"  # table_path
            assert isinstance(call_args[3], bytes)  # df_ipc_bytes
            assert call_args[7] is None  # batch_id

    @pytest.mark.asyncio
    @patch(f"{_BASE}._get_delta_process_pool")
    async def test_append_with_batch_id(self, mock_get_pool):
        """Async append passes batch_id for log correlation."""
        mock_get_pool.return_value = MagicMock()

        writer = _make_writer()
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.run_in_executor = AsyncMock(return_value=2)
            mock_loop_fn.return_value = mock_loop

            result = await writer._async_append(df, batch_id="batch-123")

            assert result is True
            call_args = mock_loop.run_in_executor.call_args[0]
            assert call_args[7] == "batch-123"  # batch_id

    @pytest.mark.asyncio
    @patch(f"{_BASE}._get_delta_process_pool")
    async def test_append_failure_returns_false(self, mock_get_pool):
        """Async append handles errors and returns False."""
        mock_get_pool.return_value = MagicMock()

        writer = _make_writer()
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.run_in_executor = AsyncMock(
                side_effect=Exception("Delta write failed")
            )
            mock_loop_fn.return_value = mock_loop

            result = await writer._async_append(df)

            assert result is False

    @pytest.mark.asyncio
    @patch(f"{_BASE}._get_delta_process_pool")
    async def test_append_logs_error_details(self, mock_get_pool):
        """Async append logs error details on failure."""
        mock_get_pool.return_value = MagicMock()

        writer = _make_writer()
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.run_in_executor = AsyncMock(
                side_effect=Exception("Schema mismatch")
            )
            mock_loop_fn.return_value = mock_loop

            with patch.object(writer.logger, "error") as mock_log:
                result = await writer._async_append(df, batch_id="batch-456")

                assert result is False
                mock_log.assert_called_once()
                log_extra = mock_log.call_args[1]["extra"]
                assert log_extra["batch_id"] == "batch-456"
                assert log_extra["row_count"] == 2
                assert "Schema mismatch" in log_extra["error"]

    @pytest.mark.asyncio
    @patch(f"{_BASE}._reset_delta_process_pool")
    @patch(f"{_BASE}._get_delta_process_pool")
    async def test_append_broken_pool_resets_and_raises(
        self, mock_get_pool, mock_reset
    ):
        """BrokenProcessPool resets the pool and re-raises."""
        mock_get_pool.return_value = MagicMock()

        writer = _make_writer()
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.run_in_executor = AsyncMock(
                side_effect=BrokenProcessPool("child died")
            )
            mock_loop_fn.return_value = mock_loop

            with pytest.raises(BrokenProcessPool):
                await writer._async_append(df)

            mock_reset.assert_called_once()


class TestBaseDeltaWriterAsyncMerge:
    """Test async merge operations."""

    @pytest.mark.asyncio
    async def test_merge_empty_dataframe_returns_true(self):
        """Async merge with empty DataFrame returns True immediately."""
        writer = _make_writer()
        empty_df = pl.DataFrame({"id": [], "name": []})

        result = await writer._async_merge(empty_df, merge_keys=["id"])

        assert result is True

    @pytest.mark.asyncio
    @patch(f"{_BASE}._get_delta_process_pool")
    async def test_merge_success(self, mock_get_pool):
        """Async merge succeeds and returns True."""
        mock_pool = MagicMock()
        mock_get_pool.return_value = mock_pool

        writer = _make_writer()
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.run_in_executor = AsyncMock(return_value=2)
            mock_loop_fn.return_value = mock_loop

            result = await writer._async_merge(df, merge_keys=["id"])

            assert result is True
            call_args = mock_loop.run_in_executor.call_args[0]
            assert call_args[0] is mock_pool
            from pipeline.common.writers.base import _subprocess_delta_merge
            assert call_args[1] is _subprocess_delta_merge
            assert call_args[7] == ["id"]  # merge_keys
            assert call_args[8] is None  # preserve_columns
            assert call_args[9] is None  # update_condition

    @pytest.mark.asyncio
    @patch(f"{_BASE}._get_delta_process_pool")
    async def test_merge_with_preserve_columns(self, mock_get_pool):
        """Async merge passes preserve_columns to subprocess."""
        mock_get_pool.return_value = MagicMock()

        writer = _make_writer()
        df = pl.DataFrame(
            {"id": [1, 2], "name": ["a", "b"], "created_at": [None, None]}
        )

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.run_in_executor = AsyncMock(return_value=2)
            mock_loop_fn.return_value = mock_loop

            result = await writer._async_merge(
                df,
                merge_keys=["id"],
                preserve_columns=["created_at", "updated_at"],
            )

            assert result is True
            call_args = mock_loop.run_in_executor.call_args[0]
            assert call_args[8] == ["created_at", "updated_at"]

    @pytest.mark.asyncio
    @patch(f"{_BASE}._get_delta_process_pool")
    async def test_merge_with_update_condition(self, mock_get_pool):
        """Async merge passes update_condition to subprocess."""
        mock_get_pool.return_value = MagicMock()

        writer = _make_writer()
        df = pl.DataFrame({"id": [1], "modified_date": ["2024-01-01"]})

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.run_in_executor = AsyncMock(return_value=1)
            mock_loop_fn.return_value = mock_loop

            result = await writer._async_merge(
                df,
                merge_keys=["id"],
                update_condition="source.modified_date > target.modified_date",
            )

            assert result is True
            call_args = mock_loop.run_in_executor.call_args[0]
            assert (
                call_args[9]
                == "source.modified_date > target.modified_date"
            )

    @pytest.mark.asyncio
    @patch(f"{_BASE}._get_delta_process_pool")
    async def test_merge_failure_returns_false(self, mock_get_pool):
        """Async merge handles errors and returns False."""
        mock_get_pool.return_value = MagicMock()

        writer = _make_writer()
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.run_in_executor = AsyncMock(
                side_effect=Exception("Merge conflict")
            )
            mock_loop_fn.return_value = mock_loop

            result = await writer._async_merge(df, merge_keys=["id"])

            assert result is False

    @pytest.mark.asyncio
    @patch(f"{_BASE}._get_delta_process_pool")
    async def test_merge_logs_error_details(self, mock_get_pool):
        """Async merge logs error details on failure."""
        mock_get_pool.return_value = MagicMock()

        writer = _make_writer()
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.run_in_executor = AsyncMock(
                side_effect=Exception("Key not found")
            )
            mock_loop_fn.return_value = mock_loop

            with patch.object(writer.logger, "error") as mock_log:
                result = await writer._async_merge(df, merge_keys=["id"])

                assert result is False
                mock_log.assert_called_once()
                log_extra = mock_log.call_args[1]["extra"]
                assert log_extra["merge_keys"] == ["id"]
                assert log_extra["row_count"] == 2
                assert "Key not found" in log_extra["error"]

    @pytest.mark.asyncio
    @patch(f"{_BASE}._reset_delta_process_pool")
    @patch(f"{_BASE}._get_delta_process_pool")
    async def test_merge_broken_pool_resets_and_raises(
        self, mock_get_pool, mock_reset
    ):
        """BrokenProcessPool resets the pool and re-raises."""
        mock_get_pool.return_value = MagicMock()

        writer = _make_writer()
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.run_in_executor = AsyncMock(
                side_effect=BrokenProcessPool("child died")
            )
            mock_loop_fn.return_value = mock_loop

            with pytest.raises(BrokenProcessPool):
                await writer._async_merge(df, merge_keys=["id"])

            mock_reset.assert_called_once()


class TestDeltaWriterDataFrameMethods:
    """Test DeltaWriter DataFrame convenience methods."""

    @pytest.mark.asyncio
    async def test_write_dataframe_calls_async_append(self):
        """write_dataframe calls _async_append."""
        writer = DeltaWriter(table_path="abfss://workspace@onelake/table")
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        with patch.object(writer, "_async_append", return_value=True) as mock_append:
            result = await writer.write_dataframe(df)

            assert result is True
            mock_append.assert_called_once_with(df)

    @pytest.mark.asyncio
    async def test_merge_dataframe_calls_async_merge(self):
        """merge_dataframe calls _async_merge with correct parameters."""
        writer = DeltaWriter(table_path="abfss://workspace@onelake/table")
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        with patch.object(writer, "_async_merge", return_value=True) as mock_merge:
            result = await writer.merge_dataframe(df, merge_keys=["id"])

            assert result is True
            mock_merge.assert_called_once_with(df, merge_keys=["id"], preserve_columns=None)

    @pytest.mark.asyncio
    async def test_merge_dataframe_with_preserve_columns(self):
        """merge_dataframe passes preserve_columns."""
        writer = DeltaWriter(table_path="abfss://workspace@onelake/table")
        df = pl.DataFrame({"id": [1], "name": ["a"], "created_at": [None]})

        with patch.object(writer, "_async_merge", return_value=True) as mock_merge:
            result = await writer.merge_dataframe(
                df, merge_keys=["id"], preserve_columns=["created_at"]
            )

            assert result is True
            mock_merge.assert_called_once_with(
                df, merge_keys=["id"], preserve_columns=["created_at"]
            )


class TestDeltaWriterRecordsMethods:
    """Test DeltaWriter records convenience methods."""

    @pytest.mark.asyncio
    async def test_write_records_empty_list_returns_true(self):
        """write_records with empty list returns True immediately."""
        writer = DeltaWriter(table_path="abfss://workspace@onelake/table")

        result = await writer.write_records([])

        assert result is True

    @pytest.mark.asyncio
    async def test_write_records_converts_to_dataframe(self):
        """write_records converts records to DataFrame."""
        writer = DeltaWriter(table_path="abfss://workspace@onelake/table")
        records = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]

        with patch.object(writer, "_async_append", return_value=True) as mock_append:
            result = await writer.write_records(records)

            assert result is True
            call_args = mock_append.call_args[0]
            assert isinstance(call_args[0], pl.DataFrame)
            assert len(call_args[0]) == 2

    @pytest.mark.asyncio
    async def test_merge_records_empty_list_returns_true(self):
        """merge_records with empty list returns True immediately."""
        writer = DeltaWriter(table_path="abfss://workspace@onelake/table")

        result = await writer.merge_records([], merge_keys=["id"])

        assert result is True

    @pytest.mark.asyncio
    async def test_merge_records_converts_to_dataframe(self):
        """merge_records converts records to DataFrame."""
        writer = DeltaWriter(table_path="abfss://workspace@onelake/table")
        records = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]

        with patch.object(writer, "_async_merge", return_value=True) as mock_merge:
            result = await writer.merge_records(records, merge_keys=["id"])

            assert result is True
            call_args = mock_merge.call_args
            assert isinstance(call_args[0][0], pl.DataFrame)
            assert call_args[1]["merge_keys"] == ["id"]

    @pytest.mark.asyncio
    async def test_merge_records_with_preserve_columns(self):
        """merge_records passes preserve_columns."""
        writer = DeltaWriter(table_path="abfss://workspace@onelake/table")
        records = [{"id": 1, "name": "a", "created_at": None}]

        with patch.object(writer, "_async_merge", return_value=True) as mock_merge:
            result = await writer.merge_records(
                records, merge_keys=["id"], preserve_columns=["created_at"]
            )

            assert result is True
            call_kwargs = mock_merge.call_args[1]
            assert call_kwargs["preserve_columns"] == ["created_at"]


class TestDeltaWriterConfiguration:
    """Test DeltaWriter configuration options."""

    def test_initialization_with_z_order(self):
        """DeltaWriter initializes with z_order_columns."""
        writer = DeltaWriter(
            table_path="abfss://workspace@onelake/table",
            z_order_columns=["id", "created_at"],
        )

        assert writer._z_order_columns == ["id", "created_at"]

    def test_initialization_with_partition_column(self):
        """DeltaWriter initializes with partition_column."""
        writer = DeltaWriter(
            table_path="abfss://workspace@onelake/table",
            partition_column="event_date",
        )

        assert writer._partition_column == "event_date"

    def test_initialization_with_custom_timestamp_column(self):
        """DeltaWriter initializes with custom timestamp_column."""
        writer = DeltaWriter(
            table_path="abfss://workspace@onelake/table",
            timestamp_column="modified_at",
        )

        assert writer._timestamp_column == "modified_at"
