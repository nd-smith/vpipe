"""Tests for KQLEventPoller checkpoint advancement logic.

Verifies that the poller correctly detects and handles the case where
checkpoint ingestion_time does not advance due to sub-microsecond
precision mismatch between KQL (100ns ticks) and Python (microseconds).
"""

from datetime import UTC, datetime, timedelta
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


class TestEnsureCheckpointAdvances:
    """Test _ensure_checkpoint_advances handles stuck checkpoints."""

    def _make_poller(self, trace_id_col=None, last_ingestion_time=None):
        """Create a poller with mocked internals for unit testing."""
        config = _make_config()
        if trace_id_col:
            config.column_mapping = {"trace_id": trace_id_col}

        with patch.object(KQLEventPoller, "_load_checkpoint"):
            poller = KQLEventPoller(config)

        poller._last_ingestion_time = last_ingestion_time
        poller._last_trace_id = ""
        return poller

    def test_bumps_when_time_unchanged_no_trace_id(self):
        """Stuck checkpoint should bump by 1 microsecond."""
        t = datetime(2026, 2, 2, 23, 58, 37, 140843, tzinfo=UTC)
        poller = self._make_poller(last_ingestion_time=t)

        result = poller._ensure_checkpoint_advances(t, "")

        assert result == t + timedelta(microseconds=1)

    def test_bumps_when_time_goes_backward_no_trace_id(self):
        """If time goes backward (shouldn't happen, but defensive), still bump."""
        t = datetime(2026, 2, 2, 23, 58, 37, 140843, tzinfo=UTC)
        earlier = t - timedelta(microseconds=1)
        poller = self._make_poller(last_ingestion_time=t)

        result = poller._ensure_checkpoint_advances(earlier, "")

        assert result == t + timedelta(microseconds=1)

    def test_no_bump_when_time_advances(self):
        """Normal case: checkpoint advances, no bump needed."""
        t1 = datetime(2026, 2, 2, 23, 58, 37, 140843, tzinfo=UTC)
        t2 = datetime(2026, 2, 2, 23, 58, 37, 140844, tzinfo=UTC)
        poller = self._make_poller(last_ingestion_time=t1)

        result = poller._ensure_checkpoint_advances(t2, "")

        assert result == t2

    def test_no_bump_when_trace_id_col_configured(self):
        """With trace_id column, composite pagination handles boundary."""
        t = datetime(2026, 2, 2, 23, 58, 37, 140843, tzinfo=UTC)
        poller = self._make_poller(
            trace_id_col="traceId", last_ingestion_time=t
        )

        result = poller._ensure_checkpoint_advances(t, "abc-123")

        # Should NOT bump — trace_id provides secondary pagination
        assert result == t

    def test_no_bump_on_first_cycle(self):
        """First cycle has no previous checkpoint, no bump needed."""
        t = datetime(2026, 2, 2, 23, 58, 37, 140843, tzinfo=UTC)
        poller = self._make_poller(last_ingestion_time=None)

        result = poller._ensure_checkpoint_advances(t, "")

        assert result == t

    def test_bump_logs_warning(self):
        """Bump should emit a warning log."""
        t = datetime(2026, 2, 2, 23, 58, 37, 140843, tzinfo=UTC)
        poller = self._make_poller(last_ingestion_time=t)

        with patch("pipeline.common.eventhouse.poller.logger") as mock_logger:
            poller._ensure_checkpoint_advances(t, "")
            mock_logger.warning.assert_called_once()
            call_args = mock_logger.warning.call_args
            assert "sub-microsecond" in call_args[0][0]
