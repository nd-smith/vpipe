"""
Tests for Delta table reader and writer.

Covers:
- get_open_file_descriptors on Linux and error paths
- DeltaTableReader: read, scan, read_filtered, read_as_polars, exists
- DeltaTableWriter: append, merge, merge_batched, write_rows
- DeltaTableWriter: _table_exists, _cast_null_columns, _align_schema_with_target
- DeltaTableWriter: _arrow_type_to_polars type mapping
- EventsTableReader: get_max_timestamp, read_after_watermark, read_by_status_subtypes
- Resource cleanup: context managers, close, __del__

All external dependencies (polars, deltalake, pyarrow, auth) are mocked.
Since polars is a MagicMock in sys.modules, we test control flow and logic
paths rather than actual DataFrame operations.
"""

import sys
import warnings
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

# Mock pyarrow if not installed (needed by _arrow_type_to_polars)
if "pyarrow" not in sys.modules:
    _pa = MagicMock()
    _pa.__name__ = "pyarrow"
    sys.modules["pyarrow"] = _pa

from pipeline.common.storage.delta import (
    DELTA_CIRCUIT_CONFIG,
    DELTA_RETRY_CONFIG,
    DeltaTableReader,
    DeltaTableWriter,
    EventsTableReader,
    _on_auth_error,
    get_open_file_descriptors,
)


class MockExpr:
    """Mock polars expression that supports all comparison and bitwise operators.

    MagicMock raises TypeError for comparison operators (<, >, <=, >=) when the
    other operand is a non-MagicMock type (like datetime.date or str). This class
    returns itself from every operation so chained expressions work.
    """

    def __getattr__(self, name):
        return MockExpr()

    def __call__(self, *args, **kwargs):
        return MockExpr()

    def __eq__(self, other):
        return MockExpr()

    def __ne__(self, other):
        return MockExpr()

    def __lt__(self, other):
        return MockExpr()

    def __gt__(self, other):
        return MockExpr()

    def __le__(self, other):
        return MockExpr()

    def __ge__(self, other):
        return MockExpr()

    def __and__(self, other):
        return MockExpr()

    def __rand__(self, other):
        return MockExpr()

    def __or__(self, other):
        return MockExpr()

    def __ror__(self, other):
        return MockExpr()

    def __invert__(self):
        return MockExpr()

    def __hash__(self):
        return id(self)


# ---------------------------------------------------------------------------
# get_open_file_descriptors
# ---------------------------------------------------------------------------


class TestGetOpenFileDescriptors:
    def test_returns_integer(self):
        result = get_open_file_descriptors()
        assert isinstance(result, int)

    def test_returns_positive_on_linux(self):
        import sys

        if not sys.platform.startswith("linux"):
            pytest.skip("Linux only")
        result = get_open_file_descriptors()
        assert result > 0

    @patch("os.path.exists", return_value=False)
    def test_returns_negative_one_when_no_proc(self, mock_exists):
        # When /proc/pid/fd doesn't exist, falls back to resource module
        result = get_open_file_descriptors()
        # Should return -1 (fallback path)
        assert result == -1

    @patch("os.listdir", side_effect=PermissionError("denied"))
    def test_returns_negative_one_on_error(self, mock_listdir):
        result = get_open_file_descriptors()
        assert result == -1


# ---------------------------------------------------------------------------
# _on_auth_error
# ---------------------------------------------------------------------------


class TestOnAuthError:
    @patch("pipeline.common.storage.delta._refresh_all_credentials")
    def test_calls_refresh_all_credentials(self, mock_refresh):
        _on_auth_error()
        mock_refresh.assert_called_once()


# ---------------------------------------------------------------------------
# DELTA_RETRY_CONFIG / DELTA_CIRCUIT_CONFIG
# ---------------------------------------------------------------------------


class TestDeltaConfigs:
    def test_retry_config_values(self):
        assert DELTA_RETRY_CONFIG.max_attempts == 3
        assert DELTA_RETRY_CONFIG.base_delay == 1.0
        assert DELTA_RETRY_CONFIG.max_delay == 10.0

    def test_circuit_config_values(self):
        assert DELTA_CIRCUIT_CONFIG.failure_threshold == 5
        assert DELTA_CIRCUIT_CONFIG.timeout_seconds == 30.0


# ---------------------------------------------------------------------------
# DeltaTableReader - initialization
# ---------------------------------------------------------------------------


class TestDeltaTableReaderInit:
    def test_initializes_with_path(self):
        reader = DeltaTableReader("abfss://ws@acct/table")
        assert reader.table_path == "abfss://ws@acct/table"
        assert reader.storage_options is None
        assert reader._delta_table is None
        assert not reader._closed

    def test_initializes_with_storage_options(self):
        opts = {"account_name": "test", "account_key": "key"}
        reader = DeltaTableReader("abfss://ws@acct/table", storage_options=opts)
        assert reader.storage_options == opts


# ---------------------------------------------------------------------------
# DeltaTableReader - context manager and close
# ---------------------------------------------------------------------------


class TestDeltaTableReaderLifecycle:
    async def test_async_context_manager(self):
        async with DeltaTableReader("test://table") as reader:
            assert not reader._closed
        assert reader._closed

    async def test_close_is_idempotent(self):
        reader = DeltaTableReader("test://table")
        await reader.close()
        await reader.close()
        assert reader._closed

    async def test_close_releases_delta_table(self):
        reader = DeltaTableReader("test://table")
        reader._delta_table = MagicMock()
        await reader.close()
        assert reader._delta_table is None

    def test_del_warns_when_not_closed(self):
        reader = DeltaTableReader("test://table")
        reader._closed = False
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", ResourceWarning)
            reader.__del__()
            assert len(w) == 1
            assert "was not properly closed" in str(w[0].message)

    async def test_del_no_warning_when_closed(self):
        reader = DeltaTableReader("test://table")
        await reader.close()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", ResourceWarning)
            reader.__del__()
            assert len(w) == 0


# ---------------------------------------------------------------------------
# DeltaTableReader - read
# ---------------------------------------------------------------------------


class TestDeltaTableReaderRead:
    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.read_delta")
    def test_read_calls_polars_read_delta(self, mock_read, mock_opts):
        mock_opts.return_value = {"key": "val"}
        mock_df = MagicMock()
        mock_read.return_value = mock_df

        reader = DeltaTableReader("abfss://ws@acct/table")
        result = reader.read()

        mock_read.assert_called_once_with(
            "abfss://ws@acct/table",
            storage_options={"key": "val"},
            columns=None,
        )
        assert result is mock_df

    @patch("pipeline.common.storage.delta.pl.read_delta")
    def test_read_uses_provided_storage_options(self, mock_read):
        mock_df = MagicMock()
        mock_read.return_value = mock_df
        opts = {"custom": "option"}

        reader = DeltaTableReader("abfss://ws@acct/table", storage_options=opts)
        reader.read()

        mock_read.assert_called_once_with(
            "abfss://ws@acct/table",
            storage_options=opts,
            columns=None,
        )

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.read_delta")
    def test_read_with_columns(self, mock_read, mock_opts):
        mock_opts.return_value = {}
        mock_read.return_value = MagicMock()

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read(columns=["id", "name"])

        mock_read.assert_called_once_with(
            "abfss://ws@acct/table",
            storage_options={},
            columns=["id", "name"],
        )


# ---------------------------------------------------------------------------
# DeltaTableReader - scan
# ---------------------------------------------------------------------------


class TestDeltaTableReaderScan:
    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_scan_returns_lazy_frame(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf

        reader = DeltaTableReader("abfss://ws@acct/table")
        result = reader.scan()

        assert result is mock_lf

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_scan_with_columns_selects(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.scan(columns=["id", "name"])

        mock_lf.select.assert_called_once_with(["id", "name"])


# ---------------------------------------------------------------------------
# DeltaTableReader - read_filtered
# ---------------------------------------------------------------------------


class TestDeltaTableReaderReadFiltered:
    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_read_filtered_applies_filter(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        # Chain all methods to return the same mock
        mock_lf.filter.return_value = mock_lf
        mock_lf.select.return_value = mock_lf
        mock_lf.sort.return_value = mock_lf
        mock_lf.head.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock()

        filter_expr = MagicMock()
        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read_filtered(filter_expr)

        mock_lf.filter.assert_called_once_with(filter_expr)

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_read_filtered_with_columns(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.filter.return_value = mock_lf
        mock_lf.select.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock()

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read_filtered(MagicMock(), columns=["id"])

        mock_lf.select.assert_called_once_with(["id"])

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_read_filtered_with_order_by(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.filter.return_value = mock_lf
        mock_lf.sort.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock()

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read_filtered(MagicMock(), order_by="created_at", descending=True)

        mock_lf.sort.assert_called_once_with("created_at", descending=True)

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_read_filtered_with_limit_and_no_order_by(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.filter.return_value = mock_lf
        mock_lf.head.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock()

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read_filtered(MagicMock(), limit=10)

        mock_lf.head.assert_called_once_with(10)

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_read_filtered_uses_streaming_without_sort(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.filter.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock()

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read_filtered(MagicMock())

        mock_lf.collect.assert_called_once_with(streaming=True)

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_read_filtered_disables_streaming_with_sort(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.filter.return_value = mock_lf
        mock_lf.sort.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock()

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read_filtered(MagicMock(), order_by="ts")

        mock_lf.collect.assert_called_once_with(streaming=False)

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_read_filtered_with_limit_and_order_by(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.filter.return_value = mock_lf
        mock_lf.sort.return_value = mock_lf
        mock_lf.head.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock()

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read_filtered(MagicMock(), limit=5, order_by="ts")

        mock_lf.sort.assert_called_once_with("ts", descending=False)
        mock_lf.head.assert_called_once_with(5)


# ---------------------------------------------------------------------------
# DeltaTableReader - read_as_polars
# ---------------------------------------------------------------------------


class TestDeltaTableReaderReadAsPolars:
    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_read_as_polars_no_filters(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock()

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read_as_polars()

        mock_lf.collect.assert_called_once_with(streaming=True)

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_read_as_polars_with_columns(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.select.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock()

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read_as_polars(columns=["id", "name"])

        mock_lf.select.assert_called_once_with(["id", "name"])

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_read_as_polars_raises_on_unsupported_operator(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf

        reader = DeltaTableReader("abfss://ws@acct/table")
        with pytest.raises(TypeError, match="Filter error"):
            reader.read_as_polars(filters=[("col", "LIKE", "val")])

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_read_as_polars_applies_eq_filter(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.filter.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock()

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read_as_polars(filters=[("status", "=", "active")])

        # filter should have been called for the "=" operation
        mock_lf.filter.assert_called_once()

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    @patch("pipeline.common.storage.delta.pl.col")
    def test_read_as_polars_applies_ne_filter(self, mock_col, mock_scan, mock_opts):
        """Test that != filter operator is applied."""
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.filter.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock()

        # Make pl.col() return a mock that supports all comparisons
        mock_col.return_value = MockExpr()

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read_as_polars(filters=[("status", "!=", "inactive")])

        mock_lf.filter.assert_called_once()

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    @patch("pipeline.common.storage.delta.pl.col")
    def test_read_as_polars_applies_comparison_filters(self, mock_col, mock_scan, mock_opts):
        """Test that <, >, <=, >= filter operators are applied."""
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.filter.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock()

        mock_col.return_value = MockExpr()

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read_as_polars(
            filters=[
                ("col1", "<", "val1"),
                ("col2", ">", "val2"),
                ("col3", "<=", "val3"),
                ("col4", ">=", "val4"),
            ]
        )

        assert mock_lf.filter.call_count == 4


# ---------------------------------------------------------------------------
# DeltaTableReader - exists
# ---------------------------------------------------------------------------


class TestDeltaTableReaderExists:
    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_exists_returns_true_when_table_readable(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf

        reader = DeltaTableReader("abfss://ws@acct/table")
        assert reader.exists() is True

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_exists_returns_false_on_error(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_scan.side_effect = Exception("table not found")

        reader = DeltaTableReader("abfss://ws@acct/table")
        assert reader.exists() is False


# ---------------------------------------------------------------------------
# DeltaTableWriter - initialization
# ---------------------------------------------------------------------------


class TestDeltaTableWriterInit:
    def test_initializes_with_defaults(self):
        writer = DeltaTableWriter("abfss://ws@acct/table")
        assert writer.table_path == "abfss://ws@acct/table"
        assert writer.timestamp_column == "ingested_at"
        assert writer.partition_column is None
        assert writer.z_order_columns == []
        assert not writer._closed

    def test_initializes_with_custom_params(self):
        writer = DeltaTableWriter(
            "abfss://ws@acct/table",
            timestamp_column="created_at",
            partition_column="event_date",
            z_order_columns=["id"],
        )
        assert writer.timestamp_column == "created_at"
        assert writer.partition_column == "event_date"
        assert writer.z_order_columns == ["id"]

    def test_creates_internal_reader(self):
        writer = DeltaTableWriter("abfss://ws@acct/table")
        assert writer._reader is not None
        assert writer._reader.table_path == "abfss://ws@acct/table"


# ---------------------------------------------------------------------------
# DeltaTableWriter - lifecycle
# ---------------------------------------------------------------------------


class TestDeltaTableWriterLifecycle:
    async def test_async_context_manager(self):
        async with DeltaTableWriter("test://table") as writer:
            assert not writer._closed
        assert writer._closed

    async def test_close_closes_reader(self):
        writer = DeltaTableWriter("test://table")
        await writer.close()
        assert writer._reader._closed

    async def test_close_releases_delta_table(self):
        writer = DeltaTableWriter("test://table")
        writer._delta_table = MagicMock()
        await writer.close()
        assert writer._delta_table is None

    def test_del_warns_when_not_closed(self):
        writer = DeltaTableWriter("test://table")
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", ResourceWarning)
            writer.__del__()
            assert len(w) == 1
            assert "was not properly closed" in str(w[0].message)


# ---------------------------------------------------------------------------
# DeltaTableWriter - _table_exists
# ---------------------------------------------------------------------------


class TestDeltaTableWriterTableExists:
    @patch("pipeline.common.storage.delta.DeltaTable")
    def test_returns_true_when_table_exists(self, mock_dt):
        writer = DeltaTableWriter("abfss://ws@acct/table")
        result = writer._table_exists({"key": "val"})
        assert result is True
        mock_dt.assert_called_once_with("abfss://ws@acct/table", storage_options={"key": "val"})

    @patch("pipeline.common.storage.delta.DeltaTable")
    def test_returns_false_when_table_missing(self, mock_dt):
        mock_dt.side_effect = Exception("table not found")
        writer = DeltaTableWriter("abfss://ws@acct/table")
        result = writer._table_exists({"key": "val"})
        assert result is False


# ---------------------------------------------------------------------------
# DeltaTableWriter - _cast_null_columns
# ---------------------------------------------------------------------------


class TestDeltaTableWriterCastNullColumns:
    def test_casts_null_columns(self):
        writer = DeltaTableWriter("abfss://ws@acct/table")
        mock_df = MagicMock()
        mock_df.columns = ["a", "b"]

        # Simulate column 'a' having Null dtype, column 'b' having Utf8
        col_a = MagicMock()
        col_a.dtype = pl.Null
        col_b = MagicMock()
        col_b.dtype = pl.Utf8

        mock_df.__getitem__ = lambda self, key: col_a if key == "a" else col_b
        mock_df.with_columns.return_value = mock_df

        writer._cast_null_columns(mock_df)
        # Should have called with_columns for column 'a' (Null type)
        mock_df.with_columns.assert_called_once()


# ---------------------------------------------------------------------------
# DeltaTableWriter - _arrow_type_to_polars
# ---------------------------------------------------------------------------


class TestArrowTypeToPolars:
    def setup_method(self):
        self.writer = DeltaTableWriter("abfss://ws@acct/table")

    def test_timestamp_maps_to_datetime_utc(self):
        mock_type = MagicMock()
        mock_type.__str__ = lambda self: "timestamp[us, tz=UTC]"
        result = self.writer._arrow_type_to_polars(mock_type)
        assert result == pl.Datetime("us", "UTC")

    def test_int64_maps_to_int64(self):
        # Mock pa.int64() equality check
        mock_type = MagicMock()
        mock_type.__str__ = lambda self: "int64"
        mock_type.__eq__ = lambda self, other: False  # won't match pa.xxx() calls
        result = self.writer._arrow_type_to_polars(mock_type)
        assert result == pl.Int64

    def test_float64_maps_to_float64(self):
        mock_type = MagicMock()
        mock_type.__str__ = lambda self: "float64"
        mock_type.__eq__ = lambda self, other: False
        result = self.writer._arrow_type_to_polars(mock_type)
        assert result == pl.Float64

    def test_double_maps_to_float64(self):
        mock_type = MagicMock()
        mock_type.__str__ = lambda self: "double"
        mock_type.__eq__ = lambda self, other: False
        result = self.writer._arrow_type_to_polars(mock_type)
        assert result == pl.Float64

    def test_bool_maps_to_boolean(self):
        mock_type = MagicMock()
        mock_type.__str__ = lambda self: "bool"
        mock_type.__eq__ = lambda self, other: False
        result = self.writer._arrow_type_to_polars(mock_type)
        assert result == pl.Boolean

    def test_date32_maps_to_date(self):
        mock_type = MagicMock()
        mock_type.__str__ = lambda self: "date32"
        mock_type.__eq__ = lambda self, other: False
        result = self.writer._arrow_type_to_polars(mock_type)
        assert result == pl.Date

    def test_binary_maps_to_binary(self):
        mock_type = MagicMock()
        mock_type.__str__ = lambda self: "binary"
        mock_type.__eq__ = lambda self, other: False
        result = self.writer._arrow_type_to_polars(mock_type)
        assert result == pl.Binary

    def test_decimal_maps_to_float64(self):
        mock_type = MagicMock()
        mock_type.__str__ = lambda self: "decimal(10,2)"
        mock_type.__eq__ = lambda self, other: False
        result = self.writer._arrow_type_to_polars(mock_type)
        assert result == pl.Float64

    def test_null_maps_to_null(self):
        mock_type = MagicMock()
        mock_type.__str__ = lambda self: "null"
        mock_type.__eq__ = lambda self, other: False
        result = self.writer._arrow_type_to_polars(mock_type)
        assert result == pl.Null

    def test_unknown_type_returns_none(self):
        mock_type = MagicMock()
        mock_type.__str__ = lambda self: "struct<x: y>"
        mock_type.__eq__ = lambda self, other: False
        result = self.writer._arrow_type_to_polars(mock_type)
        assert result is None

    def test_string_by_str_representation(self):
        mock_type = MagicMock()
        mock_type.__str__ = lambda self: "string"
        mock_type.__eq__ = lambda self, other: False
        result = self.writer._arrow_type_to_polars(mock_type)
        assert result == pl.Utf8

    def test_int32_by_str_representation(self):
        mock_type = MagicMock()
        mock_type.__str__ = lambda self: "int32"
        mock_type.__eq__ = lambda self, other: False
        result = self.writer._arrow_type_to_polars(mock_type)
        assert result == pl.Int32

    def test_float32_by_str_representation(self):
        mock_type = MagicMock()
        mock_type.__str__ = lambda self: "float32"
        mock_type.__eq__ = lambda self, other: False
        result = self.writer._arrow_type_to_polars(mock_type)
        assert result == pl.Float32

    def test_long_maps_to_int64(self):
        mock_type = MagicMock()
        mock_type.__str__ = lambda self: "long"
        mock_type.__eq__ = lambda self, other: False
        result = self.writer._arrow_type_to_polars(mock_type)
        assert result == pl.Int64


# ---------------------------------------------------------------------------
# DeltaTableWriter - append
# ---------------------------------------------------------------------------


class TestDeltaTableWriterAppend:
    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.get_auth")
    @patch("pipeline.common.storage.delta.DeltaTable")
    @patch("pipeline.common.storage.delta.write_deltalake")
    def test_append_empty_df_returns_zero(self, mock_write, mock_dt, mock_auth, mock_opts):
        mock_df = MagicMock()
        mock_df.is_empty.return_value = True

        writer = DeltaTableWriter("abfss://ws@acct/table")
        result = writer.append(mock_df, batch_id="b1")

        assert result == 0
        mock_write.assert_not_called()

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.get_auth")
    @patch("pipeline.common.storage.delta.DeltaTable")
    @patch("pipeline.common.storage.delta.write_deltalake")
    def test_append_writes_to_delta(self, mock_write, mock_dt, mock_auth, mock_opts):
        mock_opts.return_value = {"key": "val"}
        mock_auth.return_value = MagicMock(auth_mode="cli")

        mock_df = MagicMock()
        mock_df.is_empty.return_value = False
        mock_df.__len__ = lambda self: 5
        mock_df.columns = ["id", "name"]
        mock_df.dtypes = [pl.Int64, pl.Utf8]
        mock_df.head.return_value.to_dicts.return_value = [{"id": 1, "name": "a"}]
        mock_df.to_arrow.return_value = MagicMock()

        # Table exists
        mock_dt_instance = MagicMock()
        mock_dt_instance.metadata.return_value.partition_columns = []
        mock_dt.return_value = mock_dt_instance

        writer = DeltaTableWriter("abfss://ws@acct/table")
        with (
            patch.object(writer, "_table_exists", return_value=True),
            patch.object(writer, "_align_schema_with_target", return_value=mock_df),
        ):
            result = writer.append(mock_df, batch_id="b1")

        assert result == 5
        mock_write.assert_called_once()

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.get_auth")
    @patch("pipeline.common.storage.delta.DeltaTable")
    @patch("pipeline.common.storage.delta.write_deltalake")
    def test_append_casts_nulls_for_new_table(self, mock_write, mock_dt, mock_auth, mock_opts):
        mock_opts.return_value = {}
        mock_auth.return_value = MagicMock(auth_mode="cli")

        mock_df = MagicMock()
        mock_df.is_empty.return_value = False
        mock_df.__len__ = lambda self: 2
        mock_df.columns = ["id"]
        mock_df.dtypes = [pl.Int64]
        mock_df.head.return_value.to_dicts.return_value = [{"id": 1}]
        mock_df.to_arrow.return_value = MagicMock()

        writer = DeltaTableWriter("abfss://ws@acct/table", partition_column="event_date")
        with (
            patch.object(writer, "_table_exists", return_value=False),
            patch.object(writer, "_cast_null_columns", return_value=mock_df) as mock_cast,
        ):
            writer.append(mock_df)

        mock_cast.assert_called_once_with(mock_df)

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.get_auth")
    @patch("pipeline.common.storage.delta.DeltaTable")
    @patch("pipeline.common.storage.delta.write_deltalake")
    def test_append_uses_existing_table_partitions(self, mock_write, mock_dt, mock_auth, mock_opts):
        mock_opts.return_value = {}
        mock_auth.return_value = MagicMock(auth_mode="cli")

        mock_df = MagicMock()
        mock_df.is_empty.return_value = False
        mock_df.__len__ = lambda self: 1
        mock_df.columns = ["id"]
        mock_df.dtypes = [pl.Int64]
        mock_df.head.return_value.to_dicts.return_value = [{"id": 1}]
        mock_df.to_arrow.return_value = MagicMock()

        mock_dt_instance = MagicMock()
        mock_dt_instance.metadata.return_value.partition_columns = ["event_date"]
        mock_dt_instance.version.return_value = 1
        mock_dt.return_value = mock_dt_instance

        writer = DeltaTableWriter("abfss://ws@acct/table", partition_column="other_col")
        with (
            patch.object(writer, "_table_exists", return_value=True),
            patch.object(writer, "_align_schema_with_target", return_value=mock_df),
        ):
            writer.append(mock_df)

        # write_deltalake should use existing table's partitions
        call_kwargs = mock_write.call_args[1]
        assert call_kwargs["partition_by"] == ["event_date"]


# ---------------------------------------------------------------------------
# DeltaTableWriter - merge
# ---------------------------------------------------------------------------


class TestDeltaTableWriterMerge:
    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.get_auth")
    def test_merge_empty_df_returns_zero(self, mock_auth, mock_opts):
        mock_df = MagicMock()
        mock_df.is_empty.return_value = True

        writer = DeltaTableWriter("abfss://ws@acct/table")
        result = writer.merge(mock_df, merge_keys=["id"])

        assert result == 0

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.get_auth")
    @patch("pipeline.common.storage.delta.DeltaTable")
    @patch("pipeline.common.storage.delta.write_deltalake")
    def test_merge_creates_table_if_not_exists(self, mock_write, mock_dt, mock_auth, mock_opts):
        mock_opts.return_value = {}
        mock_auth.return_value = MagicMock(auth_mode="cli")

        mock_df = MagicMock()
        mock_df.is_empty.return_value = False
        mock_df.__len__ = lambda self: 2
        mock_df.columns = ["id", "name"]
        mock_df.to_dicts.return_value = [
            {"id": 1, "name": "a"},
            {"id": 2, "name": "b"},
        ]
        mock_df.to_arrow.return_value = MagicMock()

        writer = DeltaTableWriter("abfss://ws@acct/table")
        with (
            patch.object(writer, "_table_exists", return_value=False),
            patch.object(writer, "_cast_null_columns", return_value=mock_df),
            patch("pipeline.common.storage.delta.pl.DataFrame", return_value=mock_df),
        ):
            result = writer.merge(mock_df, merge_keys=["id"])

        assert result == 2
        mock_write.assert_called_once()
        call_kwargs = mock_write.call_args[1]
        assert call_kwargs["mode"] == "overwrite"

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.get_auth")
    @patch("pipeline.common.storage.delta.DeltaTable")
    def test_merge_performs_upsert_on_existing_table(self, mock_dt, mock_auth, mock_opts):
        mock_opts.return_value = {}
        mock_auth.return_value = MagicMock(auth_mode="cli")

        mock_df = MagicMock()
        mock_df.is_empty.return_value = False
        mock_df.__len__ = lambda self: 2
        mock_df.columns = ["id", "name", "created_at"]
        mock_df.to_dicts.return_value = [
            {"id": 1, "name": "a", "created_at": None},
            {"id": 2, "name": "b", "created_at": None},
        ]
        mock_df.to_arrow.return_value = MagicMock()

        mock_merge_builder = MagicMock()
        mock_merge_builder.when_matched_update.return_value = mock_merge_builder
        mock_merge_builder.when_not_matched_insert.return_value = mock_merge_builder
        mock_merge_builder.execute.return_value = {
            "num_target_rows_updated": 1,
            "num_target_rows_inserted": 1,
        }
        mock_dt.return_value.merge.return_value = mock_merge_builder

        writer = DeltaTableWriter("abfss://ws@acct/table")
        with (
            patch.object(writer, "_table_exists", return_value=True),
            patch.object(writer, "_align_schema_with_target", return_value=mock_df),
            patch("pipeline.common.storage.delta.pl.DataFrame", return_value=mock_df),
        ):
            result = writer.merge(mock_df, merge_keys=["id"])

        assert result == 2

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.get_auth")
    @patch("pipeline.common.storage.delta.DeltaTable")
    def test_merge_with_update_condition(self, mock_dt, mock_auth, mock_opts):
        mock_opts.return_value = {}
        mock_auth.return_value = MagicMock(auth_mode="cli")

        mock_df = MagicMock()
        mock_df.is_empty.return_value = False
        mock_df.__len__ = lambda self: 1
        mock_df.columns = ["id", "name"]
        mock_df.to_dicts.return_value = [{"id": 1, "name": "a"}]
        mock_df.to_arrow.return_value = MagicMock()

        mock_merge_builder = MagicMock()
        mock_merge_builder.when_matched_update.return_value = mock_merge_builder
        mock_merge_builder.when_not_matched_insert.return_value = mock_merge_builder
        mock_merge_builder.execute.return_value = {
            "num_target_rows_updated": 0,
            "num_target_rows_inserted": 1,
        }
        mock_dt.return_value.merge.return_value = mock_merge_builder

        writer = DeltaTableWriter("abfss://ws@acct/table")
        with (
            patch.object(writer, "_table_exists", return_value=True),
            patch.object(writer, "_align_schema_with_target", return_value=mock_df),
            patch("pipeline.common.storage.delta.pl.DataFrame", return_value=mock_df),
        ):
            writer.merge(
                mock_df,
                merge_keys=["id"],
                update_condition="source.ts > target.ts",
            )

        # when_matched_update should be called with predicate
        mock_merge_builder.when_matched_update.assert_called_once()
        call_args = mock_merge_builder.when_matched_update.call_args
        assert call_args[1].get("predicate") == "source.ts > target.ts"

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.get_auth")
    @patch("pipeline.common.storage.delta.DeltaTable")
    def test_merge_default_preserve_columns(self, mock_dt, mock_auth, mock_opts):
        mock_opts.return_value = {}
        mock_auth.return_value = MagicMock(auth_mode="cli")

        mock_df = MagicMock()
        mock_df.is_empty.return_value = False
        mock_df.__len__ = lambda self: 1
        mock_df.columns = ["id", "name", "created_at"]
        mock_df.to_dicts.return_value = [{"id": 1, "name": "a", "created_at": "2024-01-01"}]
        mock_df.to_arrow.return_value = MagicMock()

        mock_merge_builder = MagicMock()
        mock_merge_builder.when_matched_update.return_value = mock_merge_builder
        mock_merge_builder.when_not_matched_insert.return_value = mock_merge_builder
        mock_merge_builder.execute.return_value = {
            "num_target_rows_updated": 0,
            "num_target_rows_inserted": 1,
        }
        mock_dt.return_value.merge.return_value = mock_merge_builder

        writer = DeltaTableWriter("abfss://ws@acct/table")
        with (
            patch.object(writer, "_table_exists", return_value=True),
            patch.object(writer, "_align_schema_with_target", return_value=mock_df),
            patch("pipeline.common.storage.delta.pl.DataFrame", return_value=mock_df),
        ):
            writer.merge(mock_df, merge_keys=["id"])

        # update_dict should not include "id" (merge key) or "created_at" (default preserve)
        call_args = mock_merge_builder.when_matched_update.call_args
        update_dict = call_args[0][0]
        assert "id" not in update_dict
        assert "created_at" not in update_dict
        assert "name" in update_dict


# ---------------------------------------------------------------------------
# DeltaTableWriter - merge_batched
# ---------------------------------------------------------------------------


class TestDeltaTableWriterMergeBatched:
    def test_merge_batched_empty_returns_zero(self):
        mock_df = MagicMock()
        mock_df.is_empty.return_value = True

        writer = DeltaTableWriter("abfss://ws@acct/table")
        result = writer.merge_batched(mock_df, merge_keys=["id"])

        assert result == 0

    def test_merge_batched_single_batch(self):
        mock_df = MagicMock()
        mock_df.is_empty.return_value = False
        mock_df.__len__ = lambda self: 50

        writer = DeltaTableWriter("abfss://ws@acct/table")
        with patch.object(writer, "merge", return_value=50) as mock_merge:
            result = writer.merge_batched(mock_df, merge_keys=["id"], batch_size=100)

        assert result == 50
        mock_merge.assert_called_once()

    def test_merge_batched_multiple_batches(self):
        mock_df = MagicMock()
        mock_df.is_empty.return_value = False
        mock_df.__len__ = lambda self: 250
        mock_df.slice.return_value = mock_df

        writer = DeltaTableWriter("abfss://ws@acct/table")
        with patch.object(writer, "merge", return_value=100) as mock_merge:
            result = writer.merge_batched(mock_df, merge_keys=["id"], batch_size=100)

        # 250 rows / 100 batch_size = 3 batches
        assert mock_merge.call_count == 3
        assert result == 300

    def test_merge_batched_uses_default_batch_size(self):
        mock_df = MagicMock()
        mock_df.is_empty.return_value = False
        mock_df.__len__ = lambda self: 50

        writer = DeltaTableWriter("abfss://ws@acct/table")
        with patch.object(writer, "merge", return_value=50) as mock_merge:
            writer.merge_batched(mock_df, merge_keys=["id"])

        # 50 < 100000 default, so single batch
        mock_merge.assert_called_once()


# ---------------------------------------------------------------------------
# DeltaTableWriter - write_rows
# ---------------------------------------------------------------------------


class TestDeltaTableWriterWriteRows:
    def test_write_rows_empty_returns_zero(self):
        writer = DeltaTableWriter("abfss://ws@acct/table")
        result = writer.write_rows([])
        assert result == 0

    @patch("pipeline.common.storage.delta.pl.DataFrame")
    def test_write_rows_calls_append(self, mock_df_cls):
        mock_df = MagicMock()
        mock_df_cls.return_value = mock_df

        writer = DeltaTableWriter("abfss://ws@acct/table")
        with patch.object(writer, "append", return_value=2) as mock_append:
            result = writer.write_rows([{"id": 1}, {"id": 2}])

        assert result == 2
        mock_append.assert_called_once_with(mock_df)

    @patch("pipeline.common.storage.delta.pl.DataFrame")
    def test_write_rows_with_schema(self, mock_df_cls):
        mock_df = MagicMock()
        mock_df_cls.return_value = mock_df
        schema = {"id": pl.Int64, "name": pl.Utf8}

        writer = DeltaTableWriter("abfss://ws@acct/table")
        with patch.object(writer, "append", return_value=1):
            writer.write_rows([{"id": 1, "name": "a"}], schema=schema)

        mock_df_cls.assert_called_once_with([{"id": 1, "name": "a"}], schema=schema)


# ---------------------------------------------------------------------------
# DeltaTableWriter - _align_schema_with_target
# ---------------------------------------------------------------------------


class TestDeltaTableWriterAlignSchema:
    @patch("pipeline.common.storage.delta.DeltaTable")
    def test_returns_original_df_on_error(self, mock_dt):
        mock_dt.side_effect = Exception("table read failed")
        mock_df = MagicMock()

        writer = DeltaTableWriter("abfss://ws@acct/table")
        result = writer._align_schema_with_target(mock_df, {"key": "val"})

        assert result is mock_df

    @patch("pipeline.common.storage.delta.DeltaTable")
    def test_aligns_columns_calls_select(self, mock_dt):
        """Test that _align_schema_with_target calls df.select when schema is available."""
        # Use _arrow_type_to_polars by providing types that resolve to timestamp
        # (timestamp is checked first via string matching, avoiding pyarrow equality issues)
        mock_field1 = MagicMock()
        mock_field1.name = "id"
        mock_field1.type = MagicMock()
        mock_field1.type.__str__ = lambda self: "timestamp[us, tz=UTC]"
        mock_field1.type.__eq__ = lambda self, other: False

        mock_field2 = MagicMock()
        mock_field2.name = "name"
        mock_field2.type = MagicMock()
        mock_field2.type.__str__ = lambda self: "timestamp[us, tz=UTC]"
        mock_field2.type.__eq__ = lambda self, other: False

        mock_schema = MagicMock()
        mock_schema.fields = [mock_field1, mock_field2]
        mock_dt.return_value.schema.return_value = mock_schema

        mock_df = MagicMock()
        mock_df.columns = ["name", "id"]
        mock_col = MagicMock()
        mock_col.dtype = pl.Datetime("us", "UTC")
        mock_df.__getitem__ = lambda self, key: mock_col
        mock_df.select.return_value = mock_df

        writer = DeltaTableWriter("abfss://ws@acct/table")
        writer._align_schema_with_target(mock_df, {})

        mock_df.select.assert_called_once()


# ---------------------------------------------------------------------------
# EventsTableReader
# ---------------------------------------------------------------------------


class TestEventsTableReader:
    def test_initializes_with_path(self):
        reader = EventsTableReader("abfss://ws@acct/events")
        assert reader.table_path == "abfss://ws@acct/events"

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl")
    def test_get_max_timestamp_returns_none_on_empty(self, mock_pl, mock_opts):
        mock_opts.return_value = {}

        reader = EventsTableReader("abfss://ws@acct/events")
        with patch.object(reader, "read", return_value=MagicMock(is_empty=lambda: True)):
            result = reader.get_max_timestamp()
            # Returns None when empty
            assert result is None

    @patch("pipeline.common.storage.delta.get_storage_options")
    def test_get_max_timestamp_returns_none_on_exception(self, mock_opts):
        mock_opts.return_value = {}

        reader = EventsTableReader("abfss://ws@acct/events")
        with patch.object(reader, "read", side_effect=Exception("read error")):
            result = reader.get_max_timestamp()
            assert result is None

    @patch("pipeline.common.storage.delta.get_storage_options")
    def test_get_max_timestamp_returns_none_when_read_returns_none(self, mock_opts):
        mock_opts.return_value = {}

        reader = EventsTableReader("abfss://ws@acct/events")
        with patch.object(reader, "read", return_value=None):
            result = reader.get_max_timestamp()
            assert result is None

    @patch("pipeline.common.storage.delta.pl.lit")
    @patch("pipeline.common.storage.delta.pl.col")
    def test_read_after_watermark_calls_read_filtered(self, mock_col, mock_lit):
        reader = EventsTableReader("abfss://ws@acct/events")
        mock_result = MagicMock()

        watermark = datetime(2024, 1, 1, tzinfo=UTC)

        mock_col.return_value = MockExpr()

        with patch.object(reader, "read_filtered", return_value=mock_result) as mock_rf:
            result = reader.read_after_watermark(watermark, limit=10)

        mock_rf.assert_called_once()
        assert result is mock_result

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_read_by_status_subtypes_requires_watermark(self, mock_scan, mock_opts):
        reader = EventsTableReader("abfss://ws@acct/events")
        with pytest.raises(ValueError, match="Watermark required"):
            reader.read_by_status_subtypes(["subtype1"], watermark=None)

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    @patch("pipeline.common.storage.delta.pl.col")
    def test_read_by_status_subtypes_applies_filters(self, mock_col, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.select.return_value = mock_lf
        mock_lf.filter.return_value = mock_lf
        mock_lf.sort.return_value = mock_lf
        mock_lf.head.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock(__len__=lambda self: 3)

        mock_col.return_value = MockExpr()

        watermark = datetime(2024, 1, 1, tzinfo=UTC)
        reader = EventsTableReader("abfss://ws@acct/events")
        reader.read_by_status_subtypes(
            ["subtype1", "subtype2"],
            watermark=watermark,
        )

        # filter should have been called (partition filter and row filter)
        assert mock_lf.filter.call_count == 2

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    @patch("pipeline.common.storage.delta.pl.col")
    def test_read_by_status_subtypes_with_columns(self, mock_col, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.select.return_value = mock_lf
        mock_lf.filter.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock(__len__=lambda self: 0)

        mock_col.return_value = MockExpr()

        watermark = datetime(2024, 1, 1, tzinfo=UTC)
        reader = EventsTableReader("abfss://ws@acct/events")
        reader.read_by_status_subtypes(
            ["subtype1"],
            watermark=watermark,
            columns=["id", "status_subtype"],
        )

        # select should have been called with required columns merged in
        mock_lf.select.assert_called_once()
        selected_cols = mock_lf.select.call_args[0][0]
        assert "event_date" in selected_cols
        assert "status_subtype" in selected_cols
        assert "id" in selected_cols

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    @patch("pipeline.common.storage.delta.pl.col")
    def test_read_by_status_subtypes_with_order_and_limit(self, mock_col, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.filter.return_value = mock_lf
        mock_lf.sort.return_value = mock_lf
        mock_lf.head.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock(__len__=lambda self: 5)

        mock_col.return_value = MockExpr()

        watermark = datetime(2024, 1, 1, tzinfo=UTC)
        reader = EventsTableReader("abfss://ws@acct/events")
        reader.read_by_status_subtypes(
            ["subtype1"],
            watermark=watermark,
            order_by="ingested_at",
            limit=5,
        )

        mock_lf.sort.assert_called_once_with("ingested_at")
        mock_lf.head.assert_called_once_with(5)
        # streaming should be False when order_by is present
        mock_lf.collect.assert_called_once_with(streaming=False)

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    @patch("pipeline.common.storage.delta.pl.col")
    def test_read_by_status_subtypes_streaming_without_sort(self, mock_col, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.filter.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock(__len__=lambda self: 0)

        mock_col.return_value = MockExpr()

        watermark = datetime(2024, 1, 1, tzinfo=UTC)
        reader = EventsTableReader("abfss://ws@acct/events")
        reader.read_by_status_subtypes(
            ["subtype1"],
            watermark=watermark,
        )

        mock_lf.collect.assert_called_once_with(streaming=True)

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    @patch("pipeline.common.storage.delta.pl.col")
    def test_read_by_status_subtypes_limit_without_sort(self, mock_col, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.filter.return_value = mock_lf
        mock_lf.head.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock(__len__=lambda self: 0)

        mock_col.return_value = MockExpr()

        watermark = datetime(2024, 1, 1, tzinfo=UTC)
        reader = EventsTableReader("abfss://ws@acct/events")
        reader.read_by_status_subtypes(
            ["subtype1"],
            watermark=watermark,
            limit=10,
        )

        mock_lf.head.assert_called_once_with(10)

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    @patch("pipeline.common.storage.delta.pl.col")
    def test_read_by_status_subtypes_with_require_attachments(self, mock_col, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.filter.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock(__len__=lambda self: 0)

        mock_col.return_value = MockExpr()

        watermark = datetime(2024, 1, 1, tzinfo=UTC)
        reader = EventsTableReader("abfss://ws@acct/events")
        reader.read_by_status_subtypes(
            ["subtype1"],
            watermark=watermark,
            require_attachments=True,
        )

        # filter should still be called twice (partition + row)
        assert mock_lf.filter.call_count == 2

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    @patch("pipeline.common.storage.delta.pl.col")
    def test_read_by_status_subtypes_require_attachments_adds_column(
        self, mock_col, mock_scan, mock_opts
    ):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.select.return_value = mock_lf
        mock_lf.filter.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock(__len__=lambda self: 0)

        mock_col.return_value = MockExpr()

        watermark = datetime(2024, 1, 1, tzinfo=UTC)
        reader = EventsTableReader("abfss://ws@acct/events")
        reader.read_by_status_subtypes(
            ["subtype1"],
            watermark=watermark,
            columns=["id"],
            require_attachments=True,
        )

        selected_cols = mock_lf.select.call_args[0][0]
        assert "attachments" in selected_cols
