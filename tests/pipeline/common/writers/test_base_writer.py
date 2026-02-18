"""
Tests for BaseDeltaWriter base class.

Covers:
- Initialization with default and custom parameters
- Logger creation with correct class name
- _async_append: empty df, success, failure, batch_id, logging, BrokenProcessPool
- _async_merge: empty df, success, failure, preserve_columns, update_condition, logging
- Subclass inheritance pattern
"""

from concurrent.futures.process import BrokenProcessPool
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl

from pipeline.common.writers.base import BaseDeltaWriter

_BASE = "pipeline.common.writers.base"


def _make_writer(**kwargs):
    defaults = {"table_path": "abfss://ws@acct/table"}
    defaults.update(kwargs)
    return BaseDeltaWriter(**defaults)


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------


class TestBaseDeltaWriterInit:
    def test_initializes_with_defaults(self):
        writer = _make_writer()

        assert writer.table_path == "abfss://ws@acct/table"
        assert writer._timestamp_column == "ingested_at"
        assert writer._partition_column is None
        assert writer._z_order_columns == []

    def test_initializes_with_custom_params(self):
        writer = _make_writer(
            timestamp_column="created_at",
            partition_column="event_date",
            z_order_columns=["id", "ts"],
        )

        assert writer._timestamp_column == "created_at"
        assert writer._partition_column == "event_date"
        assert writer._z_order_columns == ["id", "ts"]

    def test_creates_logger_with_class_name(self):
        writer = _make_writer()
        assert writer.logger.name == "BaseDeltaWriter"

    def test_none_z_order_becomes_empty_list(self):
        writer = _make_writer(z_order_columns=None)
        assert writer._z_order_columns == []

    def test_subclass_logger_uses_subclass_name(self):
        class MyWriter(BaseDeltaWriter):
            pass

        writer = MyWriter(table_path="abfss://ws@acct/table")
        assert writer.logger.name == "MyWriter"


# ---------------------------------------------------------------------------
# _async_append
# ---------------------------------------------------------------------------


class TestBaseDeltaWriterAsyncAppend:
    async def test_returns_true_for_empty_df(self):
        writer = _make_writer()
        mock_df = MagicMock()
        mock_df.is_empty.return_value = True

        result = await writer._async_append(mock_df)

        assert result is True

    @patch(f"{_BASE}._get_delta_process_pool")
    async def test_returns_true_on_success(self, mock_get_pool):
        mock_get_pool.return_value = MagicMock()

        writer = _make_writer()
        df = pl.DataFrame({"id": [1, 2]})

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.run_in_executor = AsyncMock(return_value=5)
            mock_loop_fn.return_value = mock_loop

            result = await writer._async_append(df)

        assert result is True

    @patch(f"{_BASE}._get_delta_process_pool")
    async def test_passes_batch_id(self, mock_get_pool):
        mock_get_pool.return_value = MagicMock()

        writer = _make_writer()
        df = pl.DataFrame({"id": [1]})

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.run_in_executor = AsyncMock(return_value=3)
            mock_loop_fn.return_value = mock_loop

            await writer._async_append(df, batch_id="batch-42")

        call_args = mock_loop.run_in_executor.call_args[0]
        assert call_args[7] == "batch-42"  # batch_id

    @patch(f"{_BASE}._get_delta_process_pool")
    async def test_returns_false_on_exception(self, mock_get_pool):
        mock_get_pool.return_value = MagicMock()

        writer = _make_writer()
        df = pl.DataFrame({"id": [1]})

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.run_in_executor = AsyncMock(side_effect=Exception("write boom"))
            mock_loop_fn.return_value = mock_loop

            result = await writer._async_append(df)

        assert result is False

    @patch(f"{_BASE}._get_delta_process_pool")
    async def test_logs_success_details(self, mock_get_pool):
        mock_get_pool.return_value = MagicMock()

        writer = _make_writer()
        df = pl.DataFrame({"id": [1]})

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.run_in_executor = AsyncMock(return_value=10)
            mock_loop_fn.return_value = mock_loop

            with patch.object(writer.logger, "info") as mock_log:
                await writer._async_append(df, batch_id="b1")

        calls = mock_log.call_args_list
        success_call = [c for c in calls if "Successfully appended" in str(c)]
        assert len(success_call) == 1

    @patch(f"{_BASE}._get_delta_process_pool")
    async def test_logs_error_details(self, mock_get_pool):
        mock_get_pool.return_value = MagicMock()

        writer = _make_writer()
        df = pl.DataFrame({"id": [1, 2, 3, 4, 5]})

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.run_in_executor = AsyncMock(side_effect=Exception("Schema mismatch"))
            mock_loop_fn.return_value = mock_loop

            with patch.object(writer.logger, "error") as mock_log:
                await writer._async_append(df, batch_id="b2")

        mock_log.assert_called_once()
        extra = mock_log.call_args[1]["extra"]
        assert extra["batch_id"] == "b2"
        assert extra["row_count"] == 5
        assert "Schema mismatch" in extra["error"]
        assert extra["table_path"] == "abfss://ws@acct/table"

    @patch(f"{_BASE}._reset_delta_process_pool")
    @patch(f"{_BASE}._get_delta_process_pool")
    async def test_broken_pool_resets_and_raises(self, mock_get_pool, mock_reset):
        mock_get_pool.return_value = MagicMock()

        writer = _make_writer()
        df = pl.DataFrame({"id": [1]})

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.run_in_executor = AsyncMock(side_effect=BrokenProcessPool("died"))
            mock_loop_fn.return_value = mock_loop

            try:
                await writer._async_append(df)
                assert False, "Should have raised"
            except BrokenProcessPool:
                pass

        mock_reset.assert_called_once()


# ---------------------------------------------------------------------------
# _async_merge
# ---------------------------------------------------------------------------


class TestBaseDeltaWriterAsyncMerge:
    async def test_returns_true_for_empty_df(self):
        writer = _make_writer()
        mock_df = MagicMock()
        mock_df.is_empty.return_value = True

        result = await writer._async_merge(mock_df, merge_keys=["id"])

        assert result is True

    @patch(f"{_BASE}._get_delta_process_pool")
    async def test_returns_true_on_success(self, mock_get_pool):
        mock_get_pool.return_value = MagicMock()

        writer = _make_writer()
        df = pl.DataFrame({"id": [1]})

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.run_in_executor = AsyncMock(return_value=3)
            mock_loop_fn.return_value = mock_loop

            result = await writer._async_merge(df, merge_keys=["id"])

        assert result is True

    @patch(f"{_BASE}._get_delta_process_pool")
    async def test_passes_merge_keys(self, mock_get_pool):
        mock_get_pool.return_value = MagicMock()

        writer = _make_writer()
        df = pl.DataFrame({"id": [1]})

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.run_in_executor = AsyncMock(return_value=1)
            mock_loop_fn.return_value = mock_loop

            await writer._async_merge(df, merge_keys=["id", "tenant_id"])

        call_args = mock_loop.run_in_executor.call_args[0]
        assert call_args[7] == ["id", "tenant_id"]  # merge_keys

    @patch(f"{_BASE}._get_delta_process_pool")
    async def test_passes_preserve_columns(self, mock_get_pool):
        mock_get_pool.return_value = MagicMock()

        writer = _make_writer()
        df = pl.DataFrame({"id": [1]})

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.run_in_executor = AsyncMock(return_value=1)
            mock_loop_fn.return_value = mock_loop

            await writer._async_merge(
                df,
                merge_keys=["id"],
                preserve_columns=["created_at", "owner"],
            )

        call_args = mock_loop.run_in_executor.call_args[0]
        assert call_args[8] == ["created_at", "owner"]  # preserve_columns

    @patch(f"{_BASE}._get_delta_process_pool")
    async def test_passes_update_condition(self, mock_get_pool):
        mock_get_pool.return_value = MagicMock()

        writer = _make_writer()
        df = pl.DataFrame({"id": [1]})

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.run_in_executor = AsyncMock(return_value=1)
            mock_loop_fn.return_value = mock_loop

            await writer._async_merge(
                df,
                merge_keys=["id"],
                update_condition="source.modified > target.modified",
            )

        call_args = mock_loop.run_in_executor.call_args[0]
        assert call_args[9] == "source.modified > target.modified"

    @patch(f"{_BASE}._get_delta_process_pool")
    async def test_defaults_preserve_and_condition_to_none(self, mock_get_pool):
        mock_get_pool.return_value = MagicMock()

        writer = _make_writer()
        df = pl.DataFrame({"id": [1]})

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.run_in_executor = AsyncMock(return_value=1)
            mock_loop_fn.return_value = mock_loop

            await writer._async_merge(df, merge_keys=["id"])

        call_args = mock_loop.run_in_executor.call_args[0]
        assert call_args[8] is None  # preserve_columns
        assert call_args[9] is None  # update_condition

    @patch(f"{_BASE}._get_delta_process_pool")
    async def test_returns_false_on_exception(self, mock_get_pool):
        mock_get_pool.return_value = MagicMock()

        writer = _make_writer()
        df = pl.DataFrame({"id": [1]})

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.run_in_executor = AsyncMock(side_effect=Exception("merge boom"))
            mock_loop_fn.return_value = mock_loop

            result = await writer._async_merge(df, merge_keys=["id"])

        assert result is False

    @patch(f"{_BASE}._get_delta_process_pool")
    async def test_logs_error_details(self, mock_get_pool):
        mock_get_pool.return_value = MagicMock()

        writer = _make_writer()
        df = pl.DataFrame({"id": [1, 2, 3, 4, 5, 6, 7]})

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.run_in_executor = AsyncMock(side_effect=Exception("Key violation"))
            mock_loop_fn.return_value = mock_loop

            with patch.object(writer.logger, "error") as mock_log:
                await writer._async_merge(df, merge_keys=["id"])

        mock_log.assert_called_once()
        extra = mock_log.call_args[1]["extra"]
        assert extra["merge_keys"] == ["id"]
        assert extra["row_count"] == 7
        assert "Key violation" in extra["error"]

    @patch(f"{_BASE}._get_delta_process_pool")
    async def test_logs_success_details(self, mock_get_pool):
        mock_get_pool.return_value = MagicMock()

        writer = _make_writer()
        df = pl.DataFrame({"id": [1]})

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.run_in_executor = AsyncMock(return_value=5)
            mock_loop_fn.return_value = mock_loop

            with patch.object(writer.logger, "info") as mock_log:
                await writer._async_merge(df, merge_keys=["id"])

        calls = mock_log.call_args_list
        success_call = [c for c in calls if "Successfully merged" in str(c)]
        assert len(success_call) == 1


# ---------------------------------------------------------------------------
# Subclass pattern
# ---------------------------------------------------------------------------


class TestBaseDeltaWriterSubclass:
    async def test_subclass_can_call_async_append(self):
        class CustomWriter(BaseDeltaWriter):
            async def write(self, data):
                mock_df = MagicMock()
                mock_df.is_empty.return_value = True
                return await self._async_append(mock_df)

        writer = CustomWriter(table_path="abfss://ws@acct/table")
        result = await writer.write({"key": "val"})
        assert result is True

    async def test_subclass_can_call_async_merge(self):
        class CustomWriter(BaseDeltaWriter):
            async def upsert(self, data):
                mock_df = MagicMock()
                mock_df.is_empty.return_value = True
                return await self._async_merge(mock_df, merge_keys=["id"])

        writer = CustomWriter(table_path="abfss://ws@acct/table")
        result = await writer.upsert({"key": "val"})
        assert result is True
