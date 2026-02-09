"""
Tests for BaseDeltaWriter base class.

Covers:
- Initialization with default and custom parameters
- Logger creation with correct class name
- _async_append: empty df, success, failure, batch_id, logging
- _async_merge: empty df, success, failure, preserve_columns, update_condition, logging
- Subclass inheritance pattern
"""

import pytest
from unittest.mock import MagicMock, patch

import polars as pl

from pipeline.common.writers.base import BaseDeltaWriter


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------

class TestBaseDeltaWriterInit:

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    def test_initializes_with_defaults(self, mock_dtw):
        writer = BaseDeltaWriter(table_path="abfss://ws@acct/table")

        assert writer.table_path == "abfss://ws@acct/table"
        mock_dtw.assert_called_once_with(
            table_path="abfss://ws@acct/table",
            timestamp_column="ingested_at",
            partition_column=None,
            z_order_columns=[],
        )

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    def test_initializes_with_custom_params(self, mock_dtw):
        writer = BaseDeltaWriter(
            table_path="abfss://ws@acct/table",
            timestamp_column="created_at",
            partition_column="event_date",
            z_order_columns=["id", "ts"],
        )

        mock_dtw.assert_called_once_with(
            table_path="abfss://ws@acct/table",
            timestamp_column="created_at",
            partition_column="event_date",
            z_order_columns=["id", "ts"],
        )

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    def test_creates_logger_with_class_name(self, mock_dtw):
        writer = BaseDeltaWriter(table_path="abfss://ws@acct/table")
        assert writer.logger.name == "BaseDeltaWriter"

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    def test_none_z_order_becomes_empty_list(self, mock_dtw):
        writer = BaseDeltaWriter(
            table_path="abfss://ws@acct/table",
            z_order_columns=None,
        )
        call_kwargs = mock_dtw.call_args[1]
        assert call_kwargs["z_order_columns"] == []

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    def test_subclass_logger_uses_subclass_name(self, mock_dtw):
        class MyWriter(BaseDeltaWriter):
            pass

        writer = MyWriter(table_path="abfss://ws@acct/table")
        assert writer.logger.name == "MyWriter"


# ---------------------------------------------------------------------------
# _async_append
# ---------------------------------------------------------------------------

class TestBaseDeltaWriterAsyncAppend:

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    async def test_returns_true_for_empty_df(self, mock_dtw):
        writer = BaseDeltaWriter(table_path="abfss://ws@acct/table")
        mock_df = MagicMock()
        mock_df.is_empty.return_value = True

        result = await writer._async_append(mock_df)

        assert result is True
        mock_dtw.return_value.append.assert_not_called()

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    @patch("pipeline.common.writers.base.asyncio.to_thread")
    async def test_returns_true_on_success(self, mock_thread, mock_dtw):
        mock_thread.return_value = 5

        writer = BaseDeltaWriter(table_path="abfss://ws@acct/table")
        mock_df = MagicMock()
        mock_df.is_empty.return_value = False

        result = await writer._async_append(mock_df)

        assert result is True
        mock_thread.assert_called_once_with(
            writer._delta_writer.append,
            mock_df,
            batch_id=None,
        )

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    @patch("pipeline.common.writers.base.asyncio.to_thread")
    async def test_passes_batch_id(self, mock_thread, mock_dtw):
        mock_thread.return_value = 3

        writer = BaseDeltaWriter(table_path="abfss://ws@acct/table")
        mock_df = MagicMock()
        mock_df.is_empty.return_value = False

        await writer._async_append(mock_df, batch_id="batch-42")

        mock_thread.assert_called_once_with(
            writer._delta_writer.append,
            mock_df,
            batch_id="batch-42",
        )

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    @patch("pipeline.common.writers.base.asyncio.to_thread")
    async def test_returns_false_on_exception(self, mock_thread, mock_dtw):
        mock_thread.side_effect = Exception("write boom")

        writer = BaseDeltaWriter(table_path="abfss://ws@acct/table")
        mock_df = MagicMock()
        mock_df.is_empty.return_value = False

        result = await writer._async_append(mock_df)

        assert result is False

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    @patch("pipeline.common.writers.base.asyncio.to_thread")
    async def test_logs_success_details(self, mock_thread, mock_dtw):
        mock_thread.return_value = 10

        writer = BaseDeltaWriter(table_path="abfss://ws@acct/table")
        mock_df = MagicMock()
        mock_df.is_empty.return_value = False

        with patch.object(writer.logger, "info") as mock_log:
            await writer._async_append(mock_df, batch_id="b1")

        # The init also logs, so we check the second call
        calls = mock_log.call_args_list
        success_call = [c for c in calls if "Successfully appended" in str(c)]
        assert len(success_call) == 1

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    @patch("pipeline.common.writers.base.asyncio.to_thread")
    async def test_logs_error_details(self, mock_thread, mock_dtw):
        mock_thread.side_effect = Exception("Schema mismatch")

        writer = BaseDeltaWriter(table_path="abfss://ws@acct/table")
        mock_df = MagicMock()
        mock_df.is_empty.return_value = False
        mock_df.__len__ = lambda self: 5

        with patch.object(writer.logger, "error") as mock_log:
            await writer._async_append(mock_df, batch_id="b2")

        mock_log.assert_called_once()
        extra = mock_log.call_args[1]["extra"]
        assert extra["batch_id"] == "b2"
        assert extra["row_count"] == 5
        assert "Schema mismatch" in extra["error"]
        assert extra["table_path"] == "abfss://ws@acct/table"


# ---------------------------------------------------------------------------
# _async_merge
# ---------------------------------------------------------------------------

class TestBaseDeltaWriterAsyncMerge:

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    async def test_returns_true_for_empty_df(self, mock_dtw):
        writer = BaseDeltaWriter(table_path="abfss://ws@acct/table")
        mock_df = MagicMock()
        mock_df.is_empty.return_value = True

        result = await writer._async_merge(mock_df, merge_keys=["id"])

        assert result is True
        mock_dtw.return_value.merge.assert_not_called()

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    @patch("pipeline.common.writers.base.asyncio.to_thread")
    async def test_returns_true_on_success(self, mock_thread, mock_dtw):
        mock_thread.return_value = 3

        writer = BaseDeltaWriter(table_path="abfss://ws@acct/table")
        mock_df = MagicMock()
        mock_df.is_empty.return_value = False

        result = await writer._async_merge(mock_df, merge_keys=["id"])

        assert result is True

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    @patch("pipeline.common.writers.base.asyncio.to_thread")
    async def test_passes_merge_keys(self, mock_thread, mock_dtw):
        mock_thread.return_value = 1

        writer = BaseDeltaWriter(table_path="abfss://ws@acct/table")
        mock_df = MagicMock()
        mock_df.is_empty.return_value = False

        await writer._async_merge(mock_df, merge_keys=["id", "tenant_id"])

        call_kwargs = mock_thread.call_args[1]
        assert call_kwargs["merge_keys"] == ["id", "tenant_id"]

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    @patch("pipeline.common.writers.base.asyncio.to_thread")
    async def test_passes_preserve_columns(self, mock_thread, mock_dtw):
        mock_thread.return_value = 1

        writer = BaseDeltaWriter(table_path="abfss://ws@acct/table")
        mock_df = MagicMock()
        mock_df.is_empty.return_value = False

        await writer._async_merge(
            mock_df,
            merge_keys=["id"],
            preserve_columns=["created_at", "owner"],
        )

        call_kwargs = mock_thread.call_args[1]
        assert call_kwargs["preserve_columns"] == ["created_at", "owner"]

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    @patch("pipeline.common.writers.base.asyncio.to_thread")
    async def test_passes_update_condition(self, mock_thread, mock_dtw):
        mock_thread.return_value = 1

        writer = BaseDeltaWriter(table_path="abfss://ws@acct/table")
        mock_df = MagicMock()
        mock_df.is_empty.return_value = False

        await writer._async_merge(
            mock_df,
            merge_keys=["id"],
            update_condition="source.modified > target.modified",
        )

        call_kwargs = mock_thread.call_args[1]
        assert call_kwargs["update_condition"] == "source.modified > target.modified"

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    @patch("pipeline.common.writers.base.asyncio.to_thread")
    async def test_defaults_preserve_and_condition_to_none(self, mock_thread, mock_dtw):
        mock_thread.return_value = 1

        writer = BaseDeltaWriter(table_path="abfss://ws@acct/table")
        mock_df = MagicMock()
        mock_df.is_empty.return_value = False

        await writer._async_merge(mock_df, merge_keys=["id"])

        call_kwargs = mock_thread.call_args[1]
        assert call_kwargs["preserve_columns"] is None
        assert call_kwargs["update_condition"] is None

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    @patch("pipeline.common.writers.base.asyncio.to_thread")
    async def test_returns_false_on_exception(self, mock_thread, mock_dtw):
        mock_thread.side_effect = Exception("merge boom")

        writer = BaseDeltaWriter(table_path="abfss://ws@acct/table")
        mock_df = MagicMock()
        mock_df.is_empty.return_value = False

        result = await writer._async_merge(mock_df, merge_keys=["id"])

        assert result is False

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    @patch("pipeline.common.writers.base.asyncio.to_thread")
    async def test_logs_error_details(self, mock_thread, mock_dtw):
        mock_thread.side_effect = Exception("Key violation")

        writer = BaseDeltaWriter(table_path="abfss://ws@acct/table")
        mock_df = MagicMock()
        mock_df.is_empty.return_value = False
        mock_df.__len__ = lambda self: 7

        with patch.object(writer.logger, "error") as mock_log:
            await writer._async_merge(mock_df, merge_keys=["id"])

        mock_log.assert_called_once()
        extra = mock_log.call_args[1]["extra"]
        assert extra["merge_keys"] == ["id"]
        assert extra["row_count"] == 7
        assert "Key violation" in extra["error"]

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    @patch("pipeline.common.writers.base.asyncio.to_thread")
    async def test_logs_success_details(self, mock_thread, mock_dtw):
        mock_thread.return_value = 5

        writer = BaseDeltaWriter(table_path="abfss://ws@acct/table")
        mock_df = MagicMock()
        mock_df.is_empty.return_value = False

        with patch.object(writer.logger, "info") as mock_log:
            await writer._async_merge(mock_df, merge_keys=["id"])

        calls = mock_log.call_args_list
        success_call = [c for c in calls if "Successfully merged" in str(c)]
        assert len(success_call) == 1


# ---------------------------------------------------------------------------
# Subclass pattern
# ---------------------------------------------------------------------------

class TestBaseDeltaWriterSubclass:

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    async def test_subclass_can_call_async_append(self, mock_dtw):
        class CustomWriter(BaseDeltaWriter):
            async def write(self, data):
                mock_df = MagicMock()
                mock_df.is_empty.return_value = True
                return await self._async_append(mock_df)

        writer = CustomWriter(table_path="abfss://ws@acct/table")
        result = await writer.write({"key": "val"})
        assert result is True

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    async def test_subclass_can_call_async_merge(self, mock_dtw):
        class CustomWriter(BaseDeltaWriter):
            async def upsert(self, data):
                mock_df = MagicMock()
                mock_df.is_empty.return_value = True
                return await self._async_merge(mock_df, merge_keys=["id"])

        writer = CustomWriter(table_path="abfss://ws@acct/table")
        result = await writer.upsert({"key": "val"})
        assert result is True
