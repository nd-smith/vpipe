"""Tests for KQLEventPoller lifecycle, polling, and backfill logic.

The existing test_poller_checkpoint.py covers _build_query, _filter_checkpoint_rows,
_resolve_batch_boundary, and _parse_row_time. This file tests the async lifecycle
methods: start, stop, run, _poll_cycle, _bulk_backfill, checkpoint, and sink
interactions.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipeline.common.eventhouse.kql_client import EventhouseConfig, KQLQueryResult
from pipeline.common.eventhouse.poller import KQLEventPoller, PollerConfig


# =============================================================================
# Helpers
# =============================================================================


def _make_config(**overrides) -> PollerConfig:
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
        health_port=0,
    )
    defaults.update(overrides)
    return PollerConfig(**defaults)


def _make_poller(config=None, **config_overrides) -> KQLEventPoller:
    """Create a poller with stubbed-out HealthCheckServer."""
    if config is None:
        config = _make_config(**config_overrides)
    with patch("pipeline.common.eventhouse.poller.HealthCheckServer"):
        poller = KQLEventPoller(config)
    poller.health_server = AsyncMock()
    poller.health_server.actual_port = 0
    return poller


class FakeEvent:
    """Minimal event schema for testing."""

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    @classmethod
    def from_eventhouse_row(cls, row):
        return cls(**row)


# =============================================================================
# Initialization tests
# =============================================================================


class TestPollerInit:
    def test_defaults(self):
        poller = _make_poller()
        assert poller._running is False
        assert poller._kql_client is None
        assert poller._sink is None
        assert poller._total_events_fetched == 0
        assert poller._backfill_mode is True

    def test_uses_injected_sink(self):
        sink = AsyncMock()
        poller = _make_poller(sink=sink)
        assert poller._sink is sink
        assert poller._owns_sink is False

    def test_uses_injected_checkpoint_store(self):
        store = AsyncMock()
        poller = _make_poller(checkpoint_store=store)
        assert poller._checkpoint_store is store
        assert poller._owns_checkpoint_store is False

    def test_parses_backfill_timestamps(self):
        poller = _make_poller(
            backfill_start_stamp="2026-01-01T00:00:00Z",
            backfill_stop_stamp="2026-01-02T00:00:00Z",
        )
        assert poller._backfill_start_time == datetime(2026, 1, 1, tzinfo=UTC)
        assert poller._backfill_stop_time == datetime(2026, 1, 2, tzinfo=UTC)

    def test_custom_event_schema(self):
        poller = _make_poller(event_schema_class=FakeEvent)
        assert poller._event_schema_class is FakeEvent


# =============================================================================
# _parse_timestamp tests
# =============================================================================


class TestParseTimestamp:
    def test_iso_with_z(self):
        poller = _make_poller()
        dt = poller._parse_timestamp("2026-06-15T12:00:00Z")
        assert dt == datetime(2026, 6, 15, 12, 0, 0, tzinfo=UTC)

    def test_iso_with_offset(self):
        poller = _make_poller()
        dt = poller._parse_timestamp("2026-06-15T12:00:00+00:00")
        assert dt == datetime(2026, 6, 15, 12, 0, 0, tzinfo=UTC)

    def test_naive_gets_utc(self):
        poller = _make_poller()
        dt = poller._parse_timestamp("2026-06-15T12:00:00")
        assert dt.tzinfo == UTC


# =============================================================================
# _trace_id_col tests (supplemental)
# =============================================================================


class TestTraceIdColProperty:
    def test_returns_mapped_column(self):
        poller = _make_poller(column_mapping={"trace_id": "myTraceId"})
        assert poller._trace_id_col == "myTraceId"

    def test_returns_none_when_missing(self):
        poller = _make_poller(column_mapping={})
        assert poller._trace_id_col is None

    def test_returns_none_when_string_none(self):
        poller = _make_poller(column_mapping={"trace_id": "None"})
        assert poller._trace_id_col is None


# =============================================================================
# Checkpoint tests
# =============================================================================


class TestLoadCheckpoint:
    async def test_load_checkpoint_sets_state(self):
        store = AsyncMock()
        store.load = AsyncMock(
            return_value=MagicMock(
                to_datetime=MagicMock(
                    return_value=datetime(2026, 2, 1, tzinfo=UTC)
                ),
                last_trace_id="tid123",
            )
        )
        poller = _make_poller(checkpoint_store=store)
        await poller._load_checkpoint()
        assert poller._last_ingestion_time == datetime(2026, 2, 1, tzinfo=UTC)
        assert poller._last_trace_id == "tid123"

    async def test_load_checkpoint_sets_backfill_start_if_not_set(self):
        store = AsyncMock()
        store.load = AsyncMock(
            return_value=MagicMock(
                to_datetime=MagicMock(
                    return_value=datetime(2026, 2, 1, tzinfo=UTC)
                ),
                last_trace_id="",
            )
        )
        poller = _make_poller(checkpoint_store=store)
        assert poller._backfill_start_time is None
        await poller._load_checkpoint()
        assert poller._backfill_start_time == datetime(2026, 2, 1, tzinfo=UTC)

    async def test_load_checkpoint_noop_when_no_store(self):
        poller = _make_poller()
        poller._checkpoint_store = None
        await poller._load_checkpoint()
        assert poller._last_ingestion_time is None

    async def test_load_checkpoint_noop_when_no_checkpoint_saved(self):
        store = AsyncMock()
        store.load = AsyncMock(return_value=None)
        poller = _make_poller(checkpoint_store=store)
        await poller._load_checkpoint()
        assert poller._last_ingestion_time is None


class TestSaveCheckpoint:
    async def test_save_checkpoint_stores_state(self):
        store = AsyncMock()
        store.save = AsyncMock()
        poller = _make_poller(checkpoint_store=store)

        t = datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC)
        await poller._save_checkpoint(t, "trace_abc")
        store.save.assert_awaited_once()
        assert poller._last_ingestion_time == t
        assert poller._last_trace_id == "trace_abc"

    async def test_save_checkpoint_adds_utc_if_naive(self):
        store = AsyncMock()
        store.save = AsyncMock()
        poller = _make_poller(checkpoint_store=store)

        t_naive = datetime(2026, 3, 1, 12, 0, 0)
        await poller._save_checkpoint(t_naive, "t")
        # The stored time should have UTC
        assert poller._last_ingestion_time.tzinfo == UTC

    async def test_save_checkpoint_noop_when_no_store(self):
        poller = _make_poller()
        poller._checkpoint_store = None
        # Should not raise
        await poller._save_checkpoint(datetime(2026, 1, 1, tzinfo=UTC), "t")


# =============================================================================
# Start / Stop tests
# =============================================================================


class TestPollerStart:
    async def test_start_initializes_components(self):
        sink = AsyncMock()
        store = AsyncMock()
        store.load = AsyncMock(return_value=None)

        config = _make_config(sink=sink, checkpoint_store=store)
        poller = _make_poller(config=config)

        mock_kql_client = AsyncMock()
        mock_kql_client.execute_query = AsyncMock(
            return_value=KQLQueryResult(rows=[{"col": "val"}], row_count=1)
        )

        with patch(
            "pipeline.common.eventhouse.poller.KQLClient",
            return_value=mock_kql_client,
        ):
            await poller.start()

        assert poller._running is True
        sink.start.assert_awaited_once()
        mock_kql_client.connect.assert_awaited_once()

    async def test_start_raises_without_sink_or_kafka(self):
        config = _make_config(sink=None, kafka=None)
        poller = _make_poller(config=config)

        store = AsyncMock()
        store.load = AsyncMock(return_value=None)
        poller._checkpoint_store = store
        poller._owns_checkpoint_store = False

        mock_kql_client = AsyncMock()
        mock_kql_client.execute_query = AsyncMock(
            return_value=KQLQueryResult(rows=[{"col": "val"}], row_count=1)
        )

        with patch(
            "pipeline.common.eventhouse.poller.KQLClient",
            return_value=mock_kql_client,
        ):
            with pytest.raises(ValueError, match="Either sink or kafka"):
                await poller.start()

    async def test_start_creates_checkpoint_store_if_none(self):
        sink = AsyncMock()
        config = _make_config(sink=sink, checkpoint_store=None)
        poller = _make_poller(config=config)

        mock_store = AsyncMock()
        mock_store.load = AsyncMock(return_value=None)

        mock_kql_client = AsyncMock()
        mock_kql_client.execute_query = AsyncMock(
            return_value=KQLQueryResult(rows=[], row_count=0)
        )

        with patch(
            "pipeline.common.eventhouse.poller.KQLClient",
            return_value=mock_kql_client,
        ):
            with patch(
                "pipeline.common.eventhouse.poller.create_poller_checkpoint_store",
                new_callable=AsyncMock,
                return_value=mock_store,
            ):
                await poller.start()

        assert poller._checkpoint_store is mock_store
        assert poller._owns_checkpoint_store is True


class TestPollerStop:
    async def test_stop_cleans_up_owned_resources(self):
        sink = AsyncMock()
        store = AsyncMock()
        kql_client = AsyncMock()

        poller = _make_poller(sink=sink, checkpoint_store=store)
        poller._kql_client = kql_client
        poller._owns_sink = True
        poller._owns_checkpoint_store = True
        poller._running = True

        await poller.stop()

        assert poller._running is False
        sink.stop.assert_awaited_once()
        kql_client.close.assert_awaited_once()
        store.close.assert_awaited_once()

    async def test_stop_does_not_stop_injected_sink(self):
        sink = AsyncMock()
        poller = _make_poller(sink=sink)
        poller._owns_sink = False
        poller._running = True

        await poller.stop()

        sink.stop.assert_not_awaited()

    async def test_stop_does_not_close_injected_checkpoint_store(self):
        store = AsyncMock()
        poller = _make_poller(checkpoint_store=store)
        poller._owns_checkpoint_store = False
        poller._running = True

        await poller.stop()

        store.close.assert_not_awaited()


# =============================================================================
# Context manager
# =============================================================================


class TestPollerContextManager:
    async def test_async_context_manager(self):
        poller = _make_poller()
        poller.start = AsyncMock()
        poller.stop = AsyncMock()

        async with poller as p:
            assert p is poller
            poller.start.assert_awaited_once()

        poller.stop.assert_awaited_once()


# =============================================================================
# _process_filtered_results tests
# =============================================================================


class TestProcessFilteredResults:
    async def test_processes_rows_with_trace_id_col(self):
        sink = AsyncMock()
        poller = _make_poller(
            sink=sink,
            column_mapping={"trace_id": "traceId"},
            event_schema_class=FakeEvent,
        )
        poller._sink = sink

        rows = [
            {"traceId": "t1", "data": "a"},
            {"traceId": "t2", "data": "b"},
        ]
        count = await poller._process_filtered_results(rows)

        assert count == 2
        sink.write_batch.assert_awaited_once()
        messages = sink.write_batch.call_args[0][0]
        assert len(messages) == 2
        assert messages[0][0] == "t1"
        assert messages[1][0] == "t2"

    async def test_processes_rows_without_trace_id_col(self):
        sink = AsyncMock()
        poller = _make_poller(
            sink=sink,
            column_mapping={},
            event_schema_class=FakeEvent,
        )
        poller._sink = sink

        rows = [{"data": "a"}, {"data": "b"}]
        count = await poller._process_filtered_results(rows)

        assert count == 2
        sink.write_batch.assert_awaited_once()
        messages = sink.write_batch.call_args[0][0]
        assert len(messages) == 2

    async def test_uses_event_id_when_no_trace_id(self):
        sink = AsyncMock()

        class EventWithId:
            event_id = "ev-123"

            @classmethod
            def from_eventhouse_row(cls, row):
                return cls()

        poller = _make_poller(
            sink=sink,
            column_mapping={},
            event_schema_class=EventWithId,
        )
        poller._sink = sink

        rows = [{"data": "a"}]
        await poller._process_filtered_results(rows)

        messages = sink.write_batch.call_args[0][0]
        assert messages[0][0] == "ev-123"


# =============================================================================
# _poll_cycle tests
# =============================================================================


class TestPollCycle:
    async def test_poll_cycle_no_rows(self):
        store = AsyncMock()
        poller = _make_poller(checkpoint_store=store, event_schema_class=FakeEvent)
        poller._kql_client = AsyncMock()
        poller._kql_client.execute_query = AsyncMock(
            return_value=KQLQueryResult(rows=[], row_count=0)
        )
        poller._sink = AsyncMock()

        await poller._poll_cycle()
        # No checkpoint save when no rows
        store.save.assert_not_awaited()

    async def test_poll_cycle_with_trace_id(self):
        store = AsyncMock()
        store.save = AsyncMock()
        sink = AsyncMock()

        config = _make_config(
            column_mapping={"trace_id": "traceId"},
            event_schema_class=FakeEvent,
            sink=sink,
            checkpoint_store=store,
        )
        poller = _make_poller(config=config)
        poller._sink = sink
        poller._checkpoint_store = store

        t = datetime(2026, 2, 3, 12, 0, 0, tzinfo=UTC)
        rows = [
            {"ingestion_time": t, "traceId": "abc", "data": "x"},
            {"ingestion_time": t, "traceId": "def", "data": "y"},
        ]
        poller._kql_client = AsyncMock()
        poller._kql_client.execute_query = AsyncMock(
            return_value=KQLQueryResult(rows=rows, row_count=2)
        )

        await poller._poll_cycle()

        # Checkpoint should be saved with last row's time and trace_id
        store.save.assert_awaited_once()
        assert poller._last_trace_id == "def"

    async def test_poll_cycle_without_trace_id(self):
        store = AsyncMock()
        store.save = AsyncMock()
        sink = AsyncMock()

        config = _make_config(
            column_mapping={},
            event_schema_class=FakeEvent,
            sink=sink,
            checkpoint_store=store,
            batch_size=100,
        )
        poller = _make_poller(config=config)
        poller._sink = sink
        poller._checkpoint_store = store

        t1 = datetime(2026, 2, 3, 12, 0, 0, tzinfo=UTC)
        t2 = datetime(2026, 2, 3, 12, 0, 1, tzinfo=UTC)
        rows = [
            {"ingestion_time": t1, "data": "x"},
            {"ingestion_time": t2, "data": "y"},
        ]
        poller._kql_client = AsyncMock()
        poller._kql_client.execute_query = AsyncMock(
            return_value=KQLQueryResult(rows=rows, row_count=2)
        )

        await poller._poll_cycle()

        store.save.assert_awaited_once()

    async def test_poll_cycle_drains_when_stuck(self):
        """When all rows share the same timestamp at batch_size, drain is called."""
        store = AsyncMock()
        store.save = AsyncMock()
        sink = AsyncMock()

        config = _make_config(
            column_mapping={},
            event_schema_class=FakeEvent,
            sink=sink,
            checkpoint_store=store,
            batch_size=2,
        )
        poller = _make_poller(config=config)
        poller._sink = sink
        poller._checkpoint_store = store

        t = datetime(2026, 2, 3, 12, 0, 0, tzinfo=UTC)
        stuck_rows = [
            {"ingestion_time": t, "data": "a"},
            {"ingestion_time": t, "data": "b"},
        ]
        # First query returns stuck rows, drain returns all rows at that ts
        drained_rows = [
            {"ingestion_time": t, "data": "a"},
            {"ingestion_time": t, "data": "b"},
            {"ingestion_time": t, "data": "c"},
        ]

        poller._kql_client = AsyncMock()
        poller._kql_client.execute_query = AsyncMock(
            side_effect=[
                KQLQueryResult(rows=stuck_rows, row_count=2),
                KQLQueryResult(rows=drained_rows, row_count=3),
            ]
        )

        await poller._poll_cycle()

        # Two KQL queries: the poll + the drain
        assert poller._kql_client.execute_query.await_count == 2
        store.save.assert_awaited_once()


# =============================================================================
# _drain_timestamp tests
# =============================================================================


class TestDrainTimestamp:
    async def test_drain_returns_all_rows_at_timestamp(self):
        poller = _make_poller(event_schema_class=FakeEvent)
        t = datetime(2026, 2, 3, 12, 0, 0, tzinfo=UTC)
        upper = datetime(2026, 2, 3, 13, 0, 0, tzinfo=UTC)

        rows = [{"data": "a"}, {"data": "b"}, {"data": "c"}]
        poller._kql_client = AsyncMock()
        poller._kql_client.execute_query = AsyncMock(
            return_value=KQLQueryResult(rows=rows, row_count=3)
        )

        result = await poller._drain_timestamp(t, upper)
        assert result == rows
        # Verify the query targets the exact timestamp
        query_arg = poller._kql_client.execute_query.call_args[0][0]
        assert "ingestion_time ==" in query_arg

    async def test_drain_uses_ingestion_time_column_when_set(self):
        poller = _make_poller(
            event_schema_class=FakeEvent,
            ingestion_time_column="IngestionTime",
        )
        t = datetime(2026, 2, 3, 12, 0, 0, tzinfo=UTC)
        upper = datetime(2026, 2, 3, 13, 0, 0, tzinfo=UTC)

        poller._kql_client = AsyncMock()
        poller._kql_client.execute_query = AsyncMock(
            return_value=KQLQueryResult(rows=[], row_count=0)
        )

        await poller._drain_timestamp(t, upper)
        query = poller._kql_client.execute_query.call_args[0][0]
        assert "IngestionTime" in query
        assert "bin(ingestion_time()" not in query


# =============================================================================
# run() tests
# =============================================================================


class TestPollerRun:
    async def test_run_calls_poll_cycle(self):
        poller = _make_poller(poll_interval_seconds=0)
        poller._running = True

        call_count = 0

        async def fake_poll_cycle():
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                poller._running = False
                poller._shutdown_event.set()

        poller._poll_cycle = fake_poll_cycle
        await poller.run()
        assert call_count >= 2

    async def test_run_bulk_backfill_mode(self):
        poller = _make_poller(bulk_backfill=True)
        poller._backfill_mode = True
        poller._bulk_backfill = AsyncMock()

        await poller.run()
        poller._bulk_backfill.assert_awaited_once()

    async def test_run_handles_poll_cycle_error(self):
        poller = _make_poller(poll_interval_seconds=0)
        poller._running = True

        call_count = 0

        async def fail_then_stop():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("poll error")
            poller._running = False
            poller._shutdown_event.set()

        poller._poll_cycle = fail_then_stop
        # Should not raise, errors are caught in the loop
        await poller.run()
        assert call_count >= 2


# =============================================================================
# _bulk_backfill tests
# =============================================================================


class TestBulkBackfill:
    async def test_bulk_backfill_pages_through_results(self):
        store = AsyncMock()
        store.save = AsyncMock()
        sink = AsyncMock()

        config = _make_config(
            column_mapping={"trace_id": "traceId"},
            event_schema_class=FakeEvent,
            sink=sink,
            checkpoint_store=store,
            batch_size=2,
            backfill_start_stamp="2026-01-01T00:00:00Z",
            backfill_stop_stamp="2026-01-02T00:00:00Z",
        )
        poller = _make_poller(config=config)
        poller._sink = sink
        poller._checkpoint_store = store

        t1 = datetime(2026, 1, 1, 1, 0, 0, tzinfo=UTC)
        t2 = datetime(2026, 1, 1, 2, 0, 0, tzinfo=UTC)

        page1 = [
            {"ingestion_time": t1, "traceId": "a", "data": "1"},
            {"ingestion_time": t2, "traceId": "b", "data": "2"},
        ]
        page2 = [
            {"ingestion_time": t2, "traceId": "c", "data": "3"},
        ]

        poller._kql_client = AsyncMock()
        poller._kql_client.execute_query = AsyncMock(
            side_effect=[
                KQLQueryResult(rows=page1, row_count=2),
                KQLQueryResult(rows=page2, row_count=1),
            ]
        )

        await poller._bulk_backfill()

        # Two pages queried, both checkpointed
        assert poller._kql_client.execute_query.await_count == 2
        assert store.save.await_count == 2

    async def test_bulk_backfill_stops_on_empty_page(self):
        store = AsyncMock()
        store.save = AsyncMock()
        sink = AsyncMock()

        config = _make_config(
            column_mapping={"trace_id": "traceId"},
            event_schema_class=FakeEvent,
            sink=sink,
            checkpoint_store=store,
            batch_size=100,
            backfill_start_stamp="2026-01-01T00:00:00Z",
            backfill_stop_stamp="2026-01-02T00:00:00Z",
        )
        poller = _make_poller(config=config)
        poller._sink = sink
        poller._checkpoint_store = store

        poller._kql_client = AsyncMock()
        poller._kql_client.execute_query = AsyncMock(
            return_value=KQLQueryResult(rows=[], row_count=0)
        )

        await poller._bulk_backfill()

        poller._kql_client.execute_query.assert_awaited_once()
        store.save.assert_not_awaited()

    async def test_bulk_backfill_timestamp_only_path(self):
        """Test backfill for tables without trace_id (timestamp-only pagination)."""
        store = AsyncMock()
        store.save = AsyncMock()
        sink = AsyncMock()

        config = _make_config(
            column_mapping={},
            event_schema_class=FakeEvent,
            sink=sink,
            checkpoint_store=store,
            batch_size=100,
            backfill_start_stamp="2026-01-01T00:00:00Z",
            backfill_stop_stamp="2026-01-02T00:00:00Z",
        )
        poller = _make_poller(config=config)
        poller._sink = sink
        poller._checkpoint_store = store

        t1 = datetime(2026, 1, 1, 1, 0, 0, tzinfo=UTC)
        t2 = datetime(2026, 1, 1, 2, 0, 0, tzinfo=UTC)

        rows = [
            {"ingestion_time": t1, "data": "1"},
            {"ingestion_time": t2, "data": "2"},
        ]

        poller._kql_client = AsyncMock()
        poller._kql_client.execute_query = AsyncMock(
            return_value=KQLQueryResult(rows=rows, row_count=2)
        )

        await poller._bulk_backfill()

        store.save.assert_awaited_once()


# =============================================================================
# stats property test
# =============================================================================


class TestPollerStats:
    def test_stats_returns_total_fetched(self):
        poller = _make_poller()
        poller._total_events_fetched = 42
        assert poller.stats == {"total_fetched": 42}
