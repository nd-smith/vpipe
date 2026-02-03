"""Tests for KQLEventPoller composite-key pagination.

Verifies that the poller correctly paginates through rows sharing the
same ``ingestion_time()`` by using a secondary key — either the
configured ``trace_id`` column or a synthetic ``_row_hash`` generated
via ``hash_sha256(tostring(pack_all()))``.
"""

from datetime import UTC, datetime
from unittest.mock import patch

import pytest

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


def _make_poller(trace_id_col=None, last_ingestion_time=None, last_trace_id=""):
    """Create a poller with mocked internals for unit testing."""
    config = _make_config()
    if trace_id_col:
        config.column_mapping = {"trace_id": trace_id_col}

    with patch.object(KQLEventPoller, "_load_checkpoint"):
        poller = KQLEventPoller(config)

    poller._last_ingestion_time = last_ingestion_time
    poller._last_trace_id = last_trace_id
    return poller


class TestPaginationCol:
    """Test _pagination_col property returns correct column."""

    def test_returns_trace_id_col_when_configured(self):
        poller = _make_poller(trace_id_col="traceId")
        assert poller._pagination_col == "traceId"

    def test_returns_row_hash_when_no_trace_id(self):
        poller = _make_poller()
        assert poller._pagination_col == "_row_hash"

    def test_returns_row_hash_when_trace_id_is_none_string(self):
        """column_mapping with trace_id='None' should be treated as absent."""
        config = _make_config(column_mapping={"trace_id": "None"})
        with patch.object(KQLEventPoller, "_load_checkpoint"):
            poller = KQLEventPoller(config)
        assert poller._pagination_col == "_row_hash"


class TestBuildQuery:
    """Test _build_query generates correct KQL with composite pagination."""

    def setup_method(self):
        self.t_from = datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC)
        self.t_to = datetime(2026, 2, 3, 0, 0, 0, tzinfo=UTC)

    def test_extends_row_hash_without_trace_id(self):
        """When no trace_id column, query must extend _row_hash."""
        poller = _make_poller()
        q = poller._build_query("T", self.t_from, self.t_to, 100, "")
        assert "_row_hash = hash_sha256(tostring(pack_all()))" in q

    def test_no_row_hash_with_trace_id(self):
        """When trace_id column is configured, _row_hash should not appear."""
        poller = _make_poller(trace_id_col="traceId")
        q = poller._build_query("T", self.t_from, self.t_to, 100, "")
        assert "_row_hash" not in q

    def test_composite_where_with_checkpoint_tid(self):
        """With a checkpoint trace_id, query should use composite WHERE."""
        poller = _make_poller()
        q = poller._build_query("T", self.t_from, self.t_to, 100, "abc123")
        assert "strcmp" in q
        assert "abc123" in q

    def test_simple_where_without_checkpoint_tid(self):
        """Without checkpoint trace_id, query uses simple > comparison."""
        poller = _make_poller()
        q = poller._build_query("T", self.t_from, self.t_to, 100, "")
        assert "strcmp" not in q

    def test_order_includes_pagination_col_row_hash(self):
        """ORDER BY must include _row_hash when no trace_id column."""
        poller = _make_poller()
        q = poller._build_query("T", self.t_from, self.t_to, 100, "")
        assert "order by ingestion_time asc, _row_hash asc" in q

    def test_order_includes_pagination_col_trace_id(self):
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
        poller = _make_poller()
        q = poller._build_query("T", self.t_from, self.t_to, 100, "it's")
        assert "it\\'s" in q

    def test_extend_row_hash_before_where(self):
        """extend _row_hash MUST appear before the WHERE that references it.

        KQL evaluates operators in pipeline order, so referencing _row_hash
        in a WHERE clause before the EXTEND that creates it causes a
        semantic error.
        """
        poller = _make_poller()
        q = poller._build_query("T", self.t_from, self.t_to, 100, "abc123")
        extend_pos = q.index("extend _row_hash")
        where_pos = q.index("strcmp")
        assert extend_pos < where_pos, (
            f"extend _row_hash (pos {extend_pos}) must come before "
            f"WHERE strcmp (pos {where_pos}) in the KQL pipeline"
        )

    def test_extend_ingestion_time_before_row_hash(self):
        """extend ingestion_time MUST appear before extend _row_hash.

        pack_all() includes all columns that exist at that point in the
        pipeline.  ingestion_time must be extended first so it is included
        in the hash, keeping _row_hash stable across query invocations.
        """
        poller = _make_poller()
        q = poller._build_query("T", self.t_from, self.t_to, 100, "")
        it_pos = q.index("extend ingestion_time")
        rh_pos = q.index("extend _row_hash")
        assert it_pos < rh_pos, (
            f"extend ingestion_time (pos {it_pos}) must come before "
            f"extend _row_hash (pos {rh_pos}) so pack_all() includes it"
        )

    def test_take_limit(self):
        """Query must end with correct take limit."""
        poller = _make_poller()
        q = poller._build_query("T", self.t_from, self.t_to, 500, "")
        assert q.endswith("take 500")


class TestFilterCheckpointRows:
    """Test _filter_checkpoint_rows with composite-key filtering."""

    def test_no_filter_on_first_cycle(self):
        """First cycle (no checkpoint) should return all rows."""
        poller = _make_poller(last_ingestion_time=None)
        rows = [
            {"ingestion_time": datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC), "_row_hash": "a"},
            {"ingestion_time": datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC), "_row_hash": "b"},
        ]
        assert poller._filter_checkpoint_rows(rows) == rows

    def test_filters_rows_before_checkpoint_time(self):
        """Rows with ingestion_time < checkpoint should be filtered out."""
        t = datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC)
        poller = _make_poller(last_ingestion_time=t, last_trace_id="xyz")
        rows = [
            {"ingestion_time": datetime(2026, 2, 2, 22, 59, 59, tzinfo=UTC), "_row_hash": "old"},
            {"ingestion_time": datetime(2026, 2, 2, 23, 0, 1, tzinfo=UTC), "_row_hash": "new"},
        ]
        result = poller._filter_checkpoint_rows(rows)
        assert len(result) == 1
        assert result[0]["_row_hash"] == "new"

    def test_filters_rows_at_checkpoint_time_with_lower_hash(self):
        """Rows at same time with hash <= checkpoint hash are filtered."""
        t = datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC)
        poller = _make_poller(last_ingestion_time=t, last_trace_id="m")
        rows = [
            {"ingestion_time": t, "_row_hash": "a"},  # a <= m  → filtered
            {"ingestion_time": t, "_row_hash": "m"},  # m <= m  → filtered
            {"ingestion_time": t, "_row_hash": "z"},  # z > m   → kept
        ]
        result = poller._filter_checkpoint_rows(rows)
        assert len(result) == 1
        assert result[0]["_row_hash"] == "z"

    def test_keeps_rows_at_checkpoint_time_with_higher_hash(self):
        """Rows at same time with hash > checkpoint hash are kept."""
        t = datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC)
        poller = _make_poller(last_ingestion_time=t, last_trace_id="abc")
        rows = [
            {"ingestion_time": t, "_row_hash": "def"},
            {"ingestion_time": t, "_row_hash": "ghi"},
        ]
        result = poller._filter_checkpoint_rows(rows)
        assert len(result) == 2

    def test_uses_trace_id_col_when_configured(self):
        """When trace_id is configured, filter uses that column."""
        t = datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC)
        poller = _make_poller(trace_id_col="traceId", last_ingestion_time=t, last_trace_id="tid-100")
        rows = [
            {"ingestion_time": t, "traceId": "tid-050"},  # < tid-100 → filtered
            {"ingestion_time": t, "traceId": "tid-100"},  # == tid-100 → filtered
            {"ingestion_time": t, "traceId": "tid-200"},  # > tid-100 → kept
        ]
        result = poller._filter_checkpoint_rows(rows)
        assert len(result) == 1
        assert result[0]["traceId"] == "tid-200"

    def test_no_secondary_filter_without_checkpoint_tid(self):
        """Without a checkpoint trace_id, only time-based filtering."""
        t = datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC)
        poller = _make_poller(last_ingestion_time=t, last_trace_id="")
        rows = [
            {"ingestion_time": t, "_row_hash": "a"},
            {"ingestion_time": t, "_row_hash": "z"},
        ]
        # Both at same time, no checkpoint tid → both kept
        result = poller._filter_checkpoint_rows(rows)
        assert len(result) == 2

    def test_handles_string_timestamps(self):
        """Filter should handle rows with string timestamps (ISO format)."""
        t = datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC)
        poller = _make_poller(last_ingestion_time=t, last_trace_id="m")
        rows = [
            {"ingestion_time": "2026-02-02T23:00:00+00:00", "_row_hash": "z"},
        ]
        result = poller._filter_checkpoint_rows(rows)
        assert len(result) == 1
