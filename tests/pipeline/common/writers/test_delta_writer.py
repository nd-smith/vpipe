"""
Unit tests for Delta table writers.

Test Coverage:
    - BaseDeltaWriter initialization and configuration
    - Async append operations (empty df, success, failure)
    - Async merge operations (empty df, success, failure)
    - DeltaWriter convenience methods (records, dataframes)
    - Error handling and logging
    - Batch ID correlation

Mocks underlying DeltaTableWriter - focuses on async wrapper behavior.
"""

from unittest.mock import patch

import polars as pl
import pytest

from pipeline.common.writers.base import BaseDeltaWriter
from pipeline.common.writers.delta_writer import DeltaWriter


class TestBaseDeltaWriterInitialization:
    """Test BaseDeltaWriter initialization."""

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    def test_initialization_with_defaults(self, mock_writer_class):
        """BaseDeltaWriter initializes with default configuration."""
        writer = BaseDeltaWriter(table_path="abfss://workspace@onelake/table")

        assert writer.table_path == "abfss://workspace@onelake/table"
        mock_writer_class.assert_called_once_with(
            table_path="abfss://workspace@onelake/table",
            timestamp_column="ingested_at",
            partition_column=None,
            z_order_columns=[],
        )

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    def test_initialization_with_custom_config(self, mock_writer_class):
        """BaseDeltaWriter initializes with custom configuration."""
        writer = BaseDeltaWriter(
            table_path="abfss://workspace@onelake/table",
            timestamp_column="created_at",
            partition_column="event_date",
            z_order_columns=["id", "created_at"],
        )

        assert writer.table_path == "abfss://workspace@onelake/table"
        mock_writer_class.assert_called_once_with(
            table_path="abfss://workspace@onelake/table",
            timestamp_column="created_at",
            partition_column="event_date",
            z_order_columns=["id", "created_at"],
        )

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    def test_initialization_creates_logger(self, mock_writer_class):
        """BaseDeltaWriter creates logger with class name."""
        writer = BaseDeltaWriter(table_path="abfss://workspace@onelake/table")

        assert writer.logger is not None
        assert writer.logger.name == "BaseDeltaWriter"


class TestBaseDeltaWriterAsyncAppend:
    """Test async append operations."""

    @pytest.mark.asyncio
    @patch("pipeline.common.writers.base.DeltaTableWriter")
    async def test_append_empty_dataframe_returns_true(self, mock_writer_class):
        """Async append with empty DataFrame returns True immediately."""
        writer = BaseDeltaWriter(table_path="abfss://workspace@onelake/table")
        empty_df = pl.DataFrame({"id": [], "name": []})

        result = await writer._async_append(empty_df)

        assert result is True
        # Should not call underlying writer for empty df
        mock_writer_class.return_value.append.assert_not_called()

    @pytest.mark.asyncio
    @patch("pipeline.common.writers.base.DeltaTableWriter")
    @patch("pipeline.common.writers.base.asyncio.to_thread")
    async def test_append_success(self, mock_to_thread, mock_writer_class):
        """Async append succeeds and returns True."""
        mock_to_thread.return_value = 2  # rows written

        writer = BaseDeltaWriter(table_path="abfss://workspace@onelake/table")
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        result = await writer._async_append(df)

        assert result is True
        mock_to_thread.assert_called_once_with(writer._delta_writer.append, df, batch_id=None)

    @pytest.mark.asyncio
    @patch("pipeline.common.writers.base.DeltaTableWriter")
    @patch("pipeline.common.writers.base.asyncio.to_thread")
    async def test_append_with_batch_id(self, mock_to_thread, mock_writer_class):
        """Async append passes batch_id for log correlation."""
        mock_to_thread.return_value = 2

        writer = BaseDeltaWriter(table_path="abfss://workspace@onelake/table")
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        result = await writer._async_append(df, batch_id="batch-123")

        assert result is True
        mock_to_thread.assert_called_once_with(
            writer._delta_writer.append, df, batch_id="batch-123"
        )

    @pytest.mark.asyncio
    @patch("pipeline.common.writers.base.DeltaTableWriter")
    @patch("pipeline.common.writers.base.asyncio.to_thread")
    async def test_append_failure_returns_false(self, mock_to_thread, mock_writer_class):
        """Async append handles errors and returns False."""
        mock_to_thread.side_effect = Exception("Delta write failed")

        writer = BaseDeltaWriter(table_path="abfss://workspace@onelake/table")
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        result = await writer._async_append(df)

        assert result is False

    @pytest.mark.asyncio
    @patch("pipeline.common.writers.base.DeltaTableWriter")
    @patch("pipeline.common.writers.base.asyncio.to_thread")
    async def test_append_logs_error_details(self, mock_to_thread, mock_writer_class):
        """Async append logs error details on failure."""
        mock_to_thread.side_effect = Exception("Schema mismatch")

        writer = BaseDeltaWriter(table_path="abfss://workspace@onelake/table")
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        with patch.object(writer.logger, "error") as mock_log:
            result = await writer._async_append(df, batch_id="batch-456")

            assert result is False
            mock_log.assert_called_once()
            log_extra = mock_log.call_args[1]["extra"]
            assert log_extra["batch_id"] == "batch-456"
            assert log_extra["row_count"] == 2
            assert "Schema mismatch" in log_extra["error"]


class TestBaseDeltaWriterAsyncMerge:
    """Test async merge operations."""

    @pytest.mark.asyncio
    @patch("pipeline.common.writers.base.DeltaTableWriter")
    async def test_merge_empty_dataframe_returns_true(self, mock_writer_class):
        """Async merge with empty DataFrame returns True immediately."""
        writer = BaseDeltaWriter(table_path="abfss://workspace@onelake/table")
        empty_df = pl.DataFrame({"id": [], "name": []})

        result = await writer._async_merge(empty_df, merge_keys=["id"])

        assert result is True
        mock_writer_class.return_value.merge.assert_not_called()

    @pytest.mark.asyncio
    @patch("pipeline.common.writers.base.DeltaTableWriter")
    @patch("pipeline.common.writers.base.asyncio.to_thread")
    async def test_merge_success(self, mock_to_thread, mock_writer_class):
        """Async merge succeeds and returns True."""
        mock_to_thread.return_value = 2  # rows affected

        writer = BaseDeltaWriter(table_path="abfss://workspace@onelake/table")
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        result = await writer._async_merge(df, merge_keys=["id"])

        assert result is True
        mock_to_thread.assert_called_once_with(
            writer._delta_writer.merge,
            df,
            merge_keys=["id"],
            preserve_columns=None,
            update_condition=None,
        )

    @pytest.mark.asyncio
    @patch("pipeline.common.writers.base.DeltaTableWriter")
    @patch("pipeline.common.writers.base.asyncio.to_thread")
    async def test_merge_with_preserve_columns(self, mock_to_thread, mock_writer_class):
        """Async merge passes preserve_columns to underlying writer."""
        mock_to_thread.return_value = 2

        writer = BaseDeltaWriter(table_path="abfss://workspace@onelake/table")
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"], "created_at": [None, None]})

        result = await writer._async_merge(
            df, merge_keys=["id"], preserve_columns=["created_at", "updated_at"]
        )

        assert result is True
        call_kwargs = mock_to_thread.call_args[1]
        assert call_kwargs["preserve_columns"] == ["created_at", "updated_at"]

    @pytest.mark.asyncio
    @patch("pipeline.common.writers.base.DeltaTableWriter")
    @patch("pipeline.common.writers.base.asyncio.to_thread")
    async def test_merge_with_update_condition(self, mock_to_thread, mock_writer_class):
        """Async merge passes update_condition to underlying writer."""
        mock_to_thread.return_value = 1

        writer = BaseDeltaWriter(table_path="abfss://workspace@onelake/table")
        df = pl.DataFrame({"id": [1], "modified_date": ["2024-01-01"]})

        result = await writer._async_merge(
            df,
            merge_keys=["id"],
            update_condition="source.modified_date > target.modified_date",
        )

        assert result is True
        call_kwargs = mock_to_thread.call_args[1]
        assert call_kwargs["update_condition"] == "source.modified_date > target.modified_date"

    @pytest.mark.asyncio
    @patch("pipeline.common.writers.base.DeltaTableWriter")
    @patch("pipeline.common.writers.base.asyncio.to_thread")
    async def test_merge_failure_returns_false(self, mock_to_thread, mock_writer_class):
        """Async merge handles errors and returns False."""
        mock_to_thread.side_effect = Exception("Merge conflict")

        writer = BaseDeltaWriter(table_path="abfss://workspace@onelake/table")
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        result = await writer._async_merge(df, merge_keys=["id"])

        assert result is False

    @pytest.mark.asyncio
    @patch("pipeline.common.writers.base.DeltaTableWriter")
    @patch("pipeline.common.writers.base.asyncio.to_thread")
    async def test_merge_logs_error_details(self, mock_to_thread, mock_writer_class):
        """Async merge logs error details on failure."""
        mock_to_thread.side_effect = Exception("Key not found")

        writer = BaseDeltaWriter(table_path="abfss://workspace@onelake/table")
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        with patch.object(writer.logger, "error") as mock_log:
            result = await writer._async_merge(df, merge_keys=["id"])

            assert result is False
            mock_log.assert_called_once()
            log_extra = mock_log.call_args[1]["extra"]
            assert log_extra["merge_keys"] == ["id"]
            assert log_extra["row_count"] == 2
            assert "Key not found" in log_extra["error"]


class TestDeltaWriterDataFrameMethods:
    """Test DeltaWriter DataFrame convenience methods."""

    @pytest.mark.asyncio
    @patch("pipeline.common.writers.base.DeltaTableWriter")
    async def test_write_dataframe_calls_async_append(self, mock_writer_class):
        """write_dataframe calls _async_append."""
        writer = DeltaWriter(table_path="abfss://workspace@onelake/table")
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        with patch.object(writer, "_async_append", return_value=True) as mock_append:
            result = await writer.write_dataframe(df)

            assert result is True
            mock_append.assert_called_once_with(df)

    @pytest.mark.asyncio
    @patch("pipeline.common.writers.base.DeltaTableWriter")
    async def test_merge_dataframe_calls_async_merge(self, mock_writer_class):
        """merge_dataframe calls _async_merge with correct parameters."""
        writer = DeltaWriter(table_path="abfss://workspace@onelake/table")
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        with patch.object(writer, "_async_merge", return_value=True) as mock_merge:
            result = await writer.merge_dataframe(df, merge_keys=["id"])

            assert result is True
            mock_merge.assert_called_once_with(df, merge_keys=["id"], preserve_columns=None)

    @pytest.mark.asyncio
    @patch("pipeline.common.writers.base.DeltaTableWriter")
    async def test_merge_dataframe_with_preserve_columns(self, mock_writer_class):
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
    @patch("pipeline.common.writers.base.DeltaTableWriter")
    async def test_write_records_empty_list_returns_true(self, mock_writer_class):
        """write_records with empty list returns True immediately."""
        writer = DeltaWriter(table_path="abfss://workspace@onelake/table")

        result = await writer.write_records([])

        assert result is True

    @pytest.mark.asyncio
    @patch("pipeline.common.writers.base.DeltaTableWriter")
    async def test_write_records_converts_to_dataframe(self, mock_writer_class):
        """write_records converts records to DataFrame."""
        writer = DeltaWriter(table_path="abfss://workspace@onelake/table")
        records = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]

        with patch.object(writer, "_async_append", return_value=True) as mock_append:
            result = await writer.write_records(records)

            assert result is True
            # Verify DataFrame was passed (check first arg is a DataFrame)
            call_args = mock_append.call_args[0]
            assert isinstance(call_args[0], pl.DataFrame)
            assert len(call_args[0]) == 2

    @pytest.mark.asyncio
    @patch("pipeline.common.writers.base.DeltaTableWriter")
    async def test_merge_records_empty_list_returns_true(self, mock_writer_class):
        """merge_records with empty list returns True immediately."""
        writer = DeltaWriter(table_path="abfss://workspace@onelake/table")

        result = await writer.merge_records([], merge_keys=["id"])

        assert result is True

    @pytest.mark.asyncio
    @patch("pipeline.common.writers.base.DeltaTableWriter")
    async def test_merge_records_converts_to_dataframe(self, mock_writer_class):
        """merge_records converts records to DataFrame."""
        writer = DeltaWriter(table_path="abfss://workspace@onelake/table")
        records = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]

        with patch.object(writer, "_async_merge", return_value=True) as mock_merge:
            result = await writer.merge_records(records, merge_keys=["id"])

            assert result is True
            # Verify DataFrame was passed with correct merge_keys
            call_args = mock_merge.call_args
            assert isinstance(call_args[0][0], pl.DataFrame)
            assert call_args[1]["merge_keys"] == ["id"]

    @pytest.mark.asyncio
    @patch("pipeline.common.writers.base.DeltaTableWriter")
    async def test_merge_records_with_preserve_columns(self, mock_writer_class):
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

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    def test_initialization_with_z_order(self, mock_writer_class):
        """DeltaWriter initializes with z_order_columns."""
        DeltaWriter(
            table_path="abfss://workspace@onelake/table",
            z_order_columns=["id", "created_at"],
        )

        mock_writer_class.assert_called_once()
        call_kwargs = mock_writer_class.call_args[1]
        assert call_kwargs["z_order_columns"] == ["id", "created_at"]

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    def test_initialization_with_partition_column(self, mock_writer_class):
        """DeltaWriter initializes with partition_column."""
        DeltaWriter(
            table_path="abfss://workspace@onelake/table",
            partition_column="event_date",
        )

        mock_writer_class.assert_called_once()
        call_kwargs = mock_writer_class.call_args[1]
        assert call_kwargs["partition_column"] == "event_date"

    @patch("pipeline.common.writers.base.DeltaTableWriter")
    def test_initialization_with_custom_timestamp_column(self, mock_writer_class):
        """DeltaWriter initializes with custom timestamp_column."""
        DeltaWriter(
            table_path="abfss://workspace@onelake/table",
            timestamp_column="modified_at",
        )

        mock_writer_class.assert_called_once()
        call_kwargs = mock_writer_class.call_args[1]
        assert call_kwargs["timestamp_column"] == "modified_at"
