"""Tests for PeriodicStatsLogger."""

import asyncio
import logging
from unittest.mock import MagicMock, patch

import pytest

from core.logging.periodic_logger import PeriodicStatsLogger


def _make_stats_callback(records_succeeded=0, records_failed=0, records_skipped=0, records_deduplicated=0):
    """Return a stats callback that returns fixed values."""
    def get_stats(cycle_count):
        return (
            f"Cycle {cycle_count}",
            {
                "records_succeeded": records_succeeded,
                "records_failed": records_failed,
                "records_skipped": records_skipped,
                "records_deduplicated": records_deduplicated,
            },
        )
    return get_stats


class TestPeriodicStatsLoggerInit:

    def test_stores_configuration(self):
        callback = _make_stats_callback()
        psl = PeriodicStatsLogger(
            interval_seconds=30,
            get_stats=callback,
            stage="download",
            worker_id="w-0",
        )

        assert psl.interval_seconds == 30
        assert psl.get_stats is callback
        assert psl.stage == "download"
        assert psl.worker_id == "w-0"
        assert psl._task is None
        assert psl._cycle_count == 0
        assert psl._previous_stats == {}


class TestPeriodicStatsLoggerStart:

    def test_start_creates_task(self):
        psl = PeriodicStatsLogger(
            interval_seconds=30,
            get_stats=_make_stats_callback(),
            stage="download",
            worker_id="w-0",
        )

        with patch("core.logging.periodic_logger.asyncio.create_task") as mock_create:
            mock_create.return_value = MagicMock()
            psl.start()

            mock_create.assert_called_once()
            assert psl._task is not None

    def test_start_warns_if_already_running(self):
        psl = PeriodicStatsLogger(
            interval_seconds=30,
            get_stats=_make_stats_callback(),
            stage="download",
            worker_id="w-0",
        )
        psl._task = MagicMock()

        with patch("core.logging.periodic_logger.logger") as mock_logger:
            psl.start()
            mock_logger.warning.assert_called_once_with("Periodic logger already running")


class TestPeriodicStatsLoggerStop:

    @pytest.mark.asyncio
    async def test_stop_cancels_task(self):
        psl = PeriodicStatsLogger(
            interval_seconds=30,
            get_stats=_make_stats_callback(),
            stage="download",
            worker_id="w-0",
        )

        async def fake_run():
            await asyncio.sleep(100)

        psl._task = asyncio.create_task(fake_run())
        await psl.stop()

        assert psl._task is None

    @pytest.mark.asyncio
    async def test_stop_does_nothing_when_not_running(self):
        psl = PeriodicStatsLogger(
            interval_seconds=30,
            get_stats=_make_stats_callback(),
            stage="download",
            worker_id="w-0",
        )

        await psl.stop()  # Should not raise
        assert psl._task is None


class TestPeriodicStatsLoggerRun:

    @pytest.mark.asyncio
    async def test_logs_initial_cycle_zero(self):
        callback = _make_stats_callback(records_succeeded=10, records_failed=2)

        psl = PeriodicStatsLogger(
            interval_seconds=60,
            get_stats=callback,
            stage="download",
            worker_id="w-0",
        )

        with patch("core.logging.periodic_logger.logger") as mock_logger:
            # Start and stop quickly
            psl.start()
            # Give the task a moment to run the initial log
            await asyncio.sleep(0.05)
            await psl.stop()

            # Check that initial cycle 0 was logged
            info_calls = mock_logger.info.call_args_list
            assert len(info_calls) >= 1
            first_msg = info_calls[0][0][0]
            assert "Cycle 0:" in first_msg
            assert "cycle output every 60s" in first_msg

    @pytest.mark.asyncio
    async def test_logs_subsequent_cycles_with_deltas(self):
        cycle_data = {
            0: {"records_succeeded": 10, "records_failed": 0, "records_skipped": 0, "records_deduplicated": 0},
            1: {"records_succeeded": 25, "records_failed": 1, "records_skipped": 0, "records_deduplicated": 0},
        }

        def get_stats(cycle_count):
            data = cycle_data.get(cycle_count, cycle_data[0])
            return (f"Cycle {cycle_count}", data)

        psl = PeriodicStatsLogger(
            interval_seconds=0.05,  # Very short interval for testing
            get_stats=get_stats,
            stage="download",
            worker_id="w-0",
        )

        with patch("core.logging.periodic_logger.logger") as mock_logger:
            psl.start()
            # Wait long enough for at least one cycle after cycle 0
            await asyncio.sleep(0.15)
            await psl.stop()

            info_calls = mock_logger.info.call_args_list
            # Should have at least 2 calls (cycle 0 + cycle 1)
            assert len(info_calls) >= 2

            # Second call should have delta info
            second_call = info_calls[1]
            second_msg = second_call[0][0]
            assert "Cycle 1:" in second_msg
            assert "this cycle" in second_msg

    @pytest.mark.asyncio
    async def test_initial_log_includes_worker_metadata(self):
        callback = _make_stats_callback(records_succeeded=5)

        psl = PeriodicStatsLogger(
            interval_seconds=60,
            get_stats=callback,
            stage="enrich",
            worker_id="enricher-0",
        )

        with patch("core.logging.periodic_logger.logger") as mock_logger:
            psl.start()
            await asyncio.sleep(0.05)
            await psl.stop()

            first_call = mock_logger.info.call_args_list[0]
            extra = first_call[1]["extra"]
            assert extra["worker_id"] == "enricher-0"
            assert extra["stage"] == "enrich"
            assert extra["cycle"] == 0

    @pytest.mark.asyncio
    async def test_subsequent_cycle_includes_delta_metrics(self):
        call_count = 0

        def incrementing_stats(cycle_count):
            nonlocal call_count
            call_count += 1
            return (
                f"Cycle {cycle_count}",
                {
                    "records_succeeded": call_count * 10,
                    "records_failed": 0,
                    "records_skipped": 0,
                    "records_deduplicated": 0,
                },
            )

        psl = PeriodicStatsLogger(
            interval_seconds=0.05,
            get_stats=incrementing_stats,
            stage="download",
            worker_id="w-0",
        )

        with patch("core.logging.periodic_logger.logger") as mock_logger:
            psl.start()
            await asyncio.sleep(0.15)
            await psl.stop()

            # Find the first post-cycle-0 log call
            info_calls = mock_logger.info.call_args_list
            assert len(info_calls) >= 2
            cycle_1_extra = info_calls[1][1]["extra"]
            assert "delta_succeeded" in cycle_1_extra
            assert "delta_total" in cycle_1_extra
            assert "rate_msg_per_sec" in cycle_1_extra
            assert cycle_1_extra["cycle_id"] == "cycle-1"

    @pytest.mark.asyncio
    async def test_cancellation_is_handled_gracefully(self):
        psl = PeriodicStatsLogger(
            interval_seconds=60,
            get_stats=_make_stats_callback(),
            stage="download",
            worker_id="w-0",
        )

        with patch("core.logging.periodic_logger.logger"):
            psl.start()
            await asyncio.sleep(0.05)
            await psl.stop()  # Should not raise
            assert psl._task is None
