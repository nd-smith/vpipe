"""Tests for delta writer enrichment handler.

polars is not installed; the root conftest.py mocks it.
delta and pyspark are also mocked here since we only test logic, not DataFrame ops.
"""

import sys
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

# Mock delta and pyspark before importing the module under test
_delta_mock = MagicMock()
_pyspark_mock = MagicMock()
sys.modules.setdefault("delta", _delta_mock)
sys.modules.setdefault("pyspark", _pyspark_mock)
sys.modules.setdefault("pyspark.sql", _pyspark_mock.sql)

from pipeline.plugins.shared.delta_writer import (  # noqa: E402
    DeltaTableBatchWriter,
    DeltaTableWriter,
)
from pipeline.plugins.shared.enrichment import EnrichmentContext  # noqa: E402

# =====================
# DeltaTableWriter tests
# =====================


class TestDeltaTableWriter:
    def test_requires_table_name(self):
        with pytest.raises(ValueError, match="table_name"):
            DeltaTableWriter(config={})

    def test_config_defaults(self):
        writer = DeltaTableWriter(config={"table_name": "my_table"})
        assert writer.table_name == "my_table"
        assert writer.mode == "append"
        assert writer.schema_evolution is True
        assert writer.column_mapping == {}
        assert writer.partition_by == []
        assert writer.table_location is None

    def test_config_overrides(self):
        writer = DeltaTableWriter(
            config={
                "table_name": "t",
                "mode": "overwrite",
                "schema_evolution": False,
                "column_mapping": {"a": "b"},
                "partition_by": ["year"],
                "table_location": "/data/tables/t",
            }
        )
        assert writer.mode == "overwrite"
        assert writer.schema_evolution is False
        assert writer.column_mapping == {"a": "b"}
        assert writer.partition_by == ["year"]
        assert writer.table_location == "/data/tables/t"

    def test_prepare_record_no_mapping(self):
        writer = DeltaTableWriter(config={"table_name": "t"})
        data = {"event_id": "1", "status": "done"}
        result = writer._prepare_record(data)
        assert result is data  # Returns same dict when no mapping

    def test_prepare_record_with_mapping(self):
        writer = DeltaTableWriter(
            config={
                "table_name": "t",
                "column_mapping": {
                    "col_event_id": "event_id",
                    "col_status": "status",
                },
            }
        )
        data = {"event_id": "1", "status": "done", "extra": "ignored"}
        result = writer._prepare_record(data)

        assert result["col_event_id"] == "1"
        assert result["col_status"] == "done"
        assert "extra" not in result

    def test_prepare_record_missing_field_is_null(self):
        writer = DeltaTableWriter(
            config={
                "table_name": "t",
                "column_mapping": {"col_a": "missing_field"},
            }
        )
        result = writer._prepare_record({"other": "data"})
        assert result["col_a"] is None

    def test_add_partition_columns_from_string(self):
        writer = DeltaTableWriter(
            config={
                "table_name": "t",
                "partition_by": ["year", "month", "day"],
            }
        )
        record = {"event_timestamp": "2024-06-15T10:30:00Z"}
        result = writer._add_partition_columns(record)

        assert result["year"] == 2024
        assert result["month"] == 6
        assert result["day"] == 15

    def test_add_partition_columns_from_datetime(self):
        writer = DeltaTableWriter(
            config={
                "table_name": "t",
                "partition_by": ["year", "month"],
            }
        )
        dt = datetime(2024, 3, 20, 14, 0, 0)
        record = {"event_timestamp": dt}
        result = writer._add_partition_columns(record)

        assert result["year"] == 2024
        assert result["month"] == 3

    def test_add_partition_columns_no_timestamp(self):
        writer = DeltaTableWriter(
            config={
                "table_name": "t",
                "partition_by": ["year"],
            }
        )
        record = {"other_field": "value"}
        result = writer._add_partition_columns(record)

        assert "year" not in result

    def test_add_partition_columns_unexpected_type(self):
        writer = DeltaTableWriter(
            config={
                "table_name": "t",
                "partition_by": ["year"],
            }
        )
        record = {"event_timestamp": 12345}
        result = writer._add_partition_columns(record)

        assert "year" not in result

    def test_add_partition_columns_only_configured_parts(self):
        writer = DeltaTableWriter(
            config={
                "table_name": "t",
                "partition_by": ["year"],
            }
        )
        record = {"event_timestamp": "2024-06-15T10:30:00+00:00"}
        result = writer._add_partition_columns(record)

        assert result["year"] == 2024
        assert "month" not in result
        assert "day" not in result

    async def test_enrich_success(self):
        writer = DeltaTableWriter(config={"table_name": "t"})
        writer.spark = MagicMock()
        writer._write_to_delta = MagicMock()

        ctx = EnrichmentContext(data={"event_id": "1"})
        result = await writer.enrich(ctx)

        assert result.success is True
        writer.spark.createDataFrame.assert_called_once()
        writer._write_to_delta.assert_called_once()

    async def test_enrich_adds_processed_timestamp(self):
        writer = DeltaTableWriter(config={"table_name": "t"})
        writer.spark = MagicMock()
        writer._write_to_delta = MagicMock()

        ctx = EnrichmentContext(data={"event_id": "1"})
        await writer.enrich(ctx)

        # The record passed to createDataFrame should have processed_timestamp
        call_args = writer.spark.createDataFrame.call_args[0][0]
        assert "processed_timestamp" in call_args[0]

    async def test_enrich_calls_add_partition_columns(self):
        writer = DeltaTableWriter(
            config={
                "table_name": "t",
                "partition_by": ["year"],
            }
        )
        writer.spark = MagicMock()
        writer._write_to_delta = MagicMock()

        ctx = EnrichmentContext(
            data={
                "event_id": "1",
                "event_timestamp": "2024-01-15T00:00:00Z",
            }
        )
        await writer.enrich(ctx)

        call_args = writer.spark.createDataFrame.call_args[0][0]
        assert call_args[0]["year"] == 2024

    async def test_enrich_failure_returns_failed(self):
        writer = DeltaTableWriter(config={"table_name": "t"})
        writer.spark = MagicMock()
        writer.spark.createDataFrame.side_effect = RuntimeError("spark error")

        ctx = EnrichmentContext(data={"event_id": "1"})
        result = await writer.enrich(ctx)

        assert result.success is False
        assert "spark error" in result.error

    def test_write_to_delta_append_mode(self):
        writer = DeltaTableWriter(config={"table_name": "t"})
        df = MagicMock()
        mock_writer = MagicMock()
        df.write.format.return_value.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer

        writer._write_to_delta(df)

        df.write.format.assert_called_with("delta")
        df.write.format.return_value.mode.assert_called_with("append")

    def test_write_to_delta_with_table_location(self):
        writer = DeltaTableWriter(
            config={
                "table_name": "t",
                "table_location": "/data/t",
            }
        )
        df = MagicMock()
        mock_writer = MagicMock()
        df.write.format.return_value.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer

        writer._write_to_delta(df)

        mock_writer.option.assert_any_call("path", "/data/t")

    async def test_initialize_with_existing_table(self):
        writer = DeltaTableWriter(config={"table_name": "t"})

        mock_spark = MagicMock()
        with patch("pipeline.plugins.shared.delta_writer.SparkSession") as mock_spark_cls:
            mock_spark_cls.builder.getOrCreate.return_value = mock_spark
            mock_spark.table.return_value = MagicMock()  # Table exists

            await writer.initialize()

            assert writer.spark is mock_spark

    async def test_initialize_with_nonexistent_table(self):
        writer = DeltaTableWriter(config={"table_name": "new_table"})

        mock_spark = MagicMock()
        mock_spark.table.side_effect = Exception("Table not found")

        with patch("pipeline.plugins.shared.delta_writer.SparkSession") as mock_spark_cls:
            mock_spark_cls.builder.getOrCreate.return_value = mock_spark
            await writer.initialize()

    async def test_cleanup(self):
        writer = DeltaTableWriter(config={"table_name": "t"})
        # Should not raise
        await writer.cleanup()

    def test_table_exists_true(self):
        writer = DeltaTableWriter(config={"table_name": "t"})
        writer.spark = MagicMock()
        writer.spark.table.return_value = MagicMock()

        assert writer._table_exists() is True

    def test_table_exists_false(self):
        writer = DeltaTableWriter(config={"table_name": "t"})
        writer.spark = MagicMock()
        writer.spark.table.side_effect = Exception("not found")

        assert writer._table_exists() is False

    def test_table_exists_at_location(self):
        writer = DeltaTableWriter(
            config={
                "table_name": "t",
                "table_location": "/data/t",
            }
        )
        writer.spark = MagicMock()
        writer.spark.table.side_effect = Exception("not found")

        with patch("pipeline.plugins.shared.delta_writer.DeltaTable") as mock_dt:
            mock_dt.forPath.return_value = MagicMock()
            assert writer._table_exists() is True


# =====================
# DeltaTableBatchWriter tests
# =====================


class TestDeltaTableBatchWriter:
    def test_config_defaults(self):
        writer = DeltaTableBatchWriter(config={"table_name": "t"})
        assert writer.batch_size == 100
        assert writer.batch_timeout == 60.0

    def test_config_overrides(self):
        writer = DeltaTableBatchWriter(
            config={
                "table_name": "t",
                "batch_size": 50,
                "batch_timeout_seconds": 30.0,
            }
        )
        assert writer.batch_size == 50
        assert writer.batch_timeout == 30.0

    async def test_accumulates_until_batch_size(self):
        writer = DeltaTableBatchWriter(
            config={
                "table_name": "t",
                "batch_size": 3,
            }
        )
        writer.spark = MagicMock()
        writer._write_to_delta = MagicMock()

        # First two should skip
        ctx1 = EnrichmentContext(data={"id": 1})
        result1 = await writer.enrich(ctx1)
        assert result1.skip is True

        ctx2 = EnrichmentContext(data={"id": 2})
        result2 = await writer.enrich(ctx2)
        assert result2.skip is True

        # Third should flush
        ctx3 = EnrichmentContext(data={"id": 3})
        result3 = await writer.enrich(ctx3)
        assert result3.success is True
        assert result3.skip is False

        writer.spark.createDataFrame.assert_called_once()
        writer._write_to_delta.assert_called_once()

    async def test_flush_writes_remaining(self):
        writer = DeltaTableBatchWriter(
            config={
                "table_name": "t",
                "batch_size": 100,
            }
        )
        writer.spark = MagicMock()
        writer._write_to_delta = MagicMock()

        ctx = EnrichmentContext(data={"id": 1})
        await writer.enrich(ctx)

        await writer.flush()

        writer.spark.createDataFrame.assert_called_once()
        writer._write_to_delta.assert_called_once()

    async def test_flush_empty_is_noop(self):
        writer = DeltaTableBatchWriter(config={"table_name": "t"})
        writer.spark = MagicMock()
        writer._write_to_delta = MagicMock()

        await writer.flush()

        writer.spark.createDataFrame.assert_not_called()

    async def test_enrich_failure_returns_failed(self):
        writer = DeltaTableBatchWriter(
            config={
                "table_name": "t",
                "batch_size": 1,
            }
        )
        writer.spark = MagicMock()
        writer.spark.createDataFrame.side_effect = RuntimeError("spark error")

        ctx = EnrichmentContext(data={"id": 1})
        result = await writer.enrich(ctx)

        assert result.success is False
        assert "spark error" in result.error

    async def test_cleanup_flushes_and_cleans(self):
        writer = DeltaTableBatchWriter(
            config={
                "table_name": "t",
                "batch_size": 100,
            }
        )
        writer.spark = MagicMock()
        writer._write_to_delta = MagicMock()

        ctx = EnrichmentContext(data={"id": 1})
        await writer.enrich(ctx)

        await writer.cleanup()

        # Should have flushed the batch
        writer.spark.createDataFrame.assert_called_once()

    async def test_flush_exception_propagates(self):
        writer = DeltaTableBatchWriter(
            config={
                "table_name": "t",
                "batch_size": 100,
            }
        )
        writer.spark = MagicMock()
        writer.spark.createDataFrame.side_effect = RuntimeError("fail")

        ctx = EnrichmentContext(data={"id": 1})
        await writer.enrich(ctx)

        with pytest.raises(RuntimeError, match="fail"):
            await writer.flush()
