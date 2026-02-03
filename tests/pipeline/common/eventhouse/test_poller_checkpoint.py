"""Tests for KQLEventPoller pagination and boundary handling.

Verifies that the poller correctly paginates through rows using:
- Composite-key pagination (ingestion_time, trace_id) for tables with trace_id
- Timestamp-only pagination with boundary handling for tables without trace_id
"""

from datetime import UTC, datetime
from unittest.mock import patch

from pipeline.common.eventhouse.poller import KQLEventPoller, PollerConfig


def _make_config(**overrides) -> PollerConfig:
    """Create a minimal PollerConfig for testing."""
    from pipeline.common.eventhouse.kql_client import EventhouseConfig

    defaults = dict(
        eventhouse=EventhouseConfig(
            cluster_url="https://test.kusto.windows.net",
            database="testdb",
        ),
        kafka=None,
        event_schema_class=None,
        domain="test",
        source_table="TestTable",
        column_mapping={},
    )
    defaults.update(overrides)
    return PollerConfig(**defaults)


def _make_poller(
    trace_id_col=None, last_ingestion_time=None, last_trace_id="", batch_size=1000
):
    """Create a poller with mocked internals for unit testing."""
    config = _make_config(batch_size=batch_size)
    if trace_id_col:
        config.column_mapping = {"trace_id": trace_id_col}

    with patch.object(KQLEventPoller, "_load_checkpoint"):
        poller = KQLEventPoller(config)

    poller._last_ingestion_time = last_ingestion_time
    poller._last_trace_id = last_trace_id
    return poller


class TestTraceIdCol:
    """Test _trace_id_col property."""

    def test_returns_column_when_configured(self):
        poller = _make_poller(trace_id_col="traceId")
        assert poller._trace_id_col == "traceId"

    def test_returns_none_when_not_configured(self):
        poller = _make_poller()
        assert poller._trace_id_col is None

    def test_returns_none_when_set_to_none_string(self):
        """column_mapping with trace_id='None' should be treated as absent."""
        config = _make_config(column_mapping={"trace_id": "None"})
        with patch.object(KQLEventPoller, "_load_checkpoint"):
            poller = KQLEventPoller(config)
        assert poller._trace_id_col is None


class TestBuildQuery:
    """Test _build_query generates correct KQL."""

    def setup_method(self):
        self.t_from = datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC)
        self.t_to = datetime(2026, 2, 3, 0, 0, 0, tzinfo=UTC)

    def test_no_row_hash_without_trace_id(self):
        """Without trace_id, query must NOT contain _row_hash or pack_all."""
        poller = _make_poller()
        q = poller._build_query("T", self.t_from, self.t_to, 100, "")
        assert "_row_hash" not in q
        assert "pack_all" not in q
        assert "hash_sha256" not in q

    def test_no_strcmp_without_trace_id(self):
        """Without trace_id, query must not use strcmp even with checkpoint."""
        poller = _make_poller()
        q = poller._build_query("T", self.t_from, self.t_to, 100, "abc123")
        assert "strcmp" not in q

    def test_simple_order_without_trace_id(self):
        """Without trace_id, ORDER BY is just ingestion_time."""
        poller = _make_poller()
        q = poller._build_query("T", self.t_from, self.t_to, 100, "")
        assert "order by ingestion_time asc" in q
        # Ensure no secondary sort column
        order_idx = q.index("order by ingestion_time asc")
        after_order = q[order_idx + len("order by ingestion_time asc") :]
        assert not after_order.startswith(",")

    def test_composite_where_with_trace_id_and_checkpoint(self):
        """With trace_id and checkpoint, query uses strcmp composite WHERE."""
        poller = _make_poller(trace_id_col="traceId")
        q = poller._build_query("T", self.t_from, self.t_to, 100, "abc123")
        assert "strcmp" in q
        assert "abc123" in q
        assert "traceId" in q

    def test_simple_where_with_trace_id_no_checkpoint(self):
        """With trace_id but no checkpoint tid, no strcmp."""
        poller = _make_poller(trace_id_col="traceId")
        q = poller._build_query("T", self.t_from, self.t_to, 100, "")
        assert "strcmp" not in q

    def test_order_includes_trace_id_when_configured(self):
        """ORDER BY must include trace_id column when configured."""
        poller = _make_poller(trace_id_col="traceId")
        q = poller._build_query("T", self.t_from, self.t_to, 100, "")
        assert "order by ingestion_time asc, traceId asc" in q

    def test_uses_bin_microsecond_by_default(self):
        """Default should bin ingestion_time() to microsecond precision.

        KQL ingestion_time() has 100ns tick resolution but Python datetime
        only has microsecond precision.  Without bin(), rows at ticks T+1
        through T+9 satisfy '> T' in KQL but truncate back to T in Python,
        causing the checkpoint to never advance.
        """
        poller = _make_poller()
        q = poller._build_query("T", self.t_from, self.t_to, 100, "")
        assert "bin(ingestion_time(), 1microsecond)" in q

    def test_uses_configured_ingestion_time_column(self):
        """When ingestion_time_column is set, use column name."""
        config = _make_config(ingestion_time_column="IngestionTime")
        with patch.object(KQLEventPoller, "_load_checkpoint"):
            poller = KQLEventPoller(config)
        q = poller._build_query("T", self.t_from, self.t_to, 100, "")
        assert "extend ingestion_time = IngestionTime" in q

    def test_escapes_single_quotes_in_tid(self):
        """Single quotes in trace_id should be escaped in KQL."""
        poller = _make_poller(trace_id_col="traceId")
        q = poller._build_query("T", self.t_from, self.t_to, 100, "it's")
        assert "it\\'s" in q

    def test_take_limit(self):
        """Query must end with correct take limit."""
        poller = _make_poller()
        q = poller._build_query("T", self.t_from, self.t_to, 500, "")
        assert q.endswith("take 500")


class TestFilterCheckpointRows:
    """Test _filter_checkpoint_rows for trace_id composite-key filtering."""

    def test_no_filter_on_first_cycle(self):
        """First cycle (no checkpoint) should return all rows."""
        poller = _make_poller(trace_id_col="traceId", last_ingestion_time=None)
        rows = [
            {"ingestion_time": datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC), "traceId": "a"},
            {"ingestion_time": datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC), "traceId": "b"},
        ]
        assert poller._filter_checkpoint_rows(rows) == rows

    def test_filters_rows_before_checkpoint_time(self):
        """Rows with ingestion_time < checkpoint should be filtered out."""
        t = datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC)
        poller = _make_poller(
            trace_id_col="traceId", last_ingestion_time=t, last_trace_id="xyz"
        )
        rows = [
            {"ingestion_time": datetime(2026, 2, 2, 22, 59, 59, tzinfo=UTC), "traceId": "old"},
            {"ingestion_time": datetime(2026, 2, 2, 23, 0, 1, tzinfo=UTC), "traceId": "new"},
        ]
        result = poller._filter_checkpoint_rows(rows)
        assert len(result) == 1
        assert result[0]["traceId"] == "new"

    def test_filters_rows_at_checkpoint_time_with_lower_tid(self):
        """Rows at same time with trace_id <= checkpoint are filtered."""
        t = datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC)
        poller = _make_poller(
            trace_id_col="traceId", last_ingestion_time=t, last_trace_id="m"
        )
        rows = [
            {"ingestion_time": t, "traceId": "a"},  # a <= m  -> filtered
            {"ingestion_time": t, "traceId": "m"},  # m <= m  -> filtered
            {"ingestion_time": t, "traceId": "z"},  # z > m   -> kept
        ]
        result = poller._filter_checkpoint_rows(rows)
        assert len(result) == 1
        assert result[0]["traceId"] == "z"

    def test_keeps_rows_at_checkpoint_time_with_higher_tid(self):
        """Rows at same time with trace_id > checkpoint are kept."""
        t = datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC)
        poller = _make_poller(
            trace_id_col="traceId", last_ingestion_time=t, last_trace_id="abc"
        )
        rows = [
            {"ingestion_time": t, "traceId": "def"},
            {"ingestion_time": t, "traceId": "ghi"},
        ]
        result = poller._filter_checkpoint_rows(rows)
        assert len(result) == 2

    def test_no_secondary_filter_without_checkpoint_tid(self):
        """Without a checkpoint trace_id, only time-based filtering."""
        t = datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC)
        poller = _make_poller(
            trace_id_col="traceId", last_ingestion_time=t, last_trace_id=""
        )
        rows = [
            {"ingestion_time": t, "traceId": "a"},
            {"ingestion_time": t, "traceId": "z"},
        ]
        result = poller._filter_checkpoint_rows(rows)
        assert len(result) == 2

    def test_handles_string_timestamps(self):
        """Filter should handle rows with string timestamps (ISO format)."""
        t = datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC)
        poller = _make_poller(
            trace_id_col="traceId", last_ingestion_time=t, last_trace_id="m"
        )
        rows = [
            {"ingestion_time": "2026-02-02T23:00:00+00:00", "traceId": "z"},
        ]
        result = poller._filter_checkpoint_rows(rows)
        assert len(result) == 1


class TestResolveBatchBoundary:
    """Test _resolve_batch_boundary for timestamp-only pagination."""

    def test_batch_not_full_returns_all_rows(self):
        """When batch has fewer rows than batch_size, all rows are returned."""
        poller = _make_poller(batch_size=10)
        t1 = datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC)
        t2 = datetime(2026, 2, 2, 23, 0, 1, tzinfo=UTC)
        rows = [
            {"ingestion_time": t1},
            {"ingestion_time": t2},
        ]
        result_rows, cp_time = poller._resolve_batch_boundary(rows)
        assert result_rows == rows
        assert cp_time == t2

    def test_batch_full_mixed_timestamps_trims_boundary(self):
        """Full batch with mixed timestamps trims rows at the last timestamp."""
        poller = _make_poller(batch_size=5)
        t1 = datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC)
        t2 = datetime(2026, 2, 2, 23, 0, 1, tzinfo=UTC)
        rows = [
            {"ingestion_time": t1, "id": 1},
            {"ingestion_time": t1, "id": 2},
            {"ingestion_time": t1, "id": 3},
            {"ingestion_time": t2, "id": 4},
            {"ingestion_time": t2, "id": 5},
        ]
        result_rows, cp_time = poller._resolve_batch_boundary(rows)
        assert len(result_rows) == 3
        assert cp_time == t1
        assert all(r["ingestion_time"] == t1 for r in result_rows)

    def test_batch_full_single_timestamp_returns_none(self):
        """Full batch with single timestamp returns None for checkpoint."""
        poller = _make_poller(batch_size=3)
        t = datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC)
        rows = [
            {"ingestion_time": t, "id": 1},
            {"ingestion_time": t, "id": 2},
            {"ingestion_time": t, "id": 3},
        ]
        result_rows, cp_time = poller._resolve_batch_boundary(rows)
        assert result_rows == rows
        assert cp_time is None

    def test_boundary_trim_preserves_row_identity(self):
        """Trimmed rows should be the exact same dicts, not copies."""
        poller = _make_poller(batch_size=4)
        t1 = datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC)
        t2 = datetime(2026, 2, 2, 23, 0, 1, tzinfo=UTC)
        rows = [
            {"ingestion_time": t1, "data": "a"},
            {"ingestion_time": t1, "data": "b"},
            {"ingestion_time": t2, "data": "c"},
            {"ingestion_time": t2, "data": "d"},
        ]
        result_rows, _ = poller._resolve_batch_boundary(rows)
        assert len(result_rows) == 2
        assert result_rows[0] is rows[0]
        assert result_rows[1] is rows[1]

    def test_batch_full_three_timestamps_trims_only_last(self):
        """With three distinct timestamps, only the last group is trimmed."""
        poller = _make_poller(batch_size=6)
        t1 = datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC)
        t2 = datetime(2026, 2, 2, 23, 0, 1, tzinfo=UTC)
        t3 = datetime(2026, 2, 2, 23, 0, 2, tzinfo=UTC)
        rows = [
            {"ingestion_time": t1},
            {"ingestion_time": t1},
            {"ingestion_time": t2},
            {"ingestion_time": t2},
            {"ingestion_time": t3},
            {"ingestion_time": t3},
        ]
        result_rows, cp_time = poller._resolve_batch_boundary(rows)
        assert len(result_rows) == 4
        assert cp_time == t2

    def test_single_boundary_row_trimmed(self):
        """Even a single row at the last timestamp is trimmed."""
        poller = _make_poller(batch_size=3)
        t1 = datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC)
        t2 = datetime(2026, 2, 2, 23, 0, 1, tzinfo=UTC)
        rows = [
            {"ingestion_time": t1},
            {"ingestion_time": t1},
            {"ingestion_time": t2},
        ]
        result_rows, cp_time = poller._resolve_batch_boundary(rows)
        assert len(result_rows) == 2
        assert cp_time == t1


class TestParseRowTime:
    """Test _parse_row_time helper."""

    def test_datetime_passthrough(self):
        poller = _make_poller()
        t = datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC)
        assert poller._parse_row_time({"ingestion_time": t}) == t

    def test_string_iso_format(self):
        poller = _make_poller()
        t = datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC)
        assert poller._parse_row_time({"ingestion_time": "2026-02-02T23:00:00+00:00"}) == t

    def test_string_with_z_suffix(self):
        poller = _make_poller()
        t = datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC)
        assert poller._parse_row_time({"ingestion_time": "2026-02-02T23:00:00Z"}) == t

    def test_naive_datetime_gets_utc(self):
        poller = _make_poller()
        t_naive = datetime(2026, 2, 2, 23, 0, 0)
        result = poller._parse_row_time({"ingestion_time": t_naive})
        assert result.tzinfo == UTC

    def test_falls_back_to_dollar_ingestion_time(self):
        poller = _make_poller()
        t = datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC)
        assert poller._parse_row_time({"$IngestionTime": t}) == t
