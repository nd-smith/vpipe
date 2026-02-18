"""
Tests for Delta events writer.

Tests cover:
- Initialization
- Async write operations via write_raw_events
- Error handling
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest


@pytest.fixture
def sample_raw_event():
    """Create a sample raw Eventhouse event for testing."""
    return {
        "type": "verisk.claims.property.xn.documentsReceived",
        "version": 1,
        "utcDateTime": "2024-01-01T12:00:00Z",
        "traceId": "test-trace-123",
        "data": '{"assignmentId": "A12345", "attachments": ["file1.pdf"]}',
    }


@pytest.fixture
def delta_writer():
    """Create a DeltaEventsWriter."""
    from pipeline.verisk.writers.delta_events import DeltaEventsWriter

    writer = DeltaEventsWriter(
        table_path="abfss://test@onelake/lakehouse/xact_events",
    )
    yield writer


class TestDeltaEventsWriter:
    """Test suite for DeltaEventsWriter."""

    def test_initialization(self, delta_writer):
        """Test writer initialization."""
        assert delta_writer.table_path == "abfss://test@onelake/lakehouse/xact_events"
        assert delta_writer._timestamp_column == "created_at"
        assert delta_writer._partition_column == "event_date"

    @pytest.mark.asyncio
    async def test_write_raw_events_success(self, delta_writer, sample_raw_event):
        """Test successful raw event write."""
        mock_df = pl.DataFrame(
            {
                "trace_id": ["test-trace-123"],
                "type": ["documentsReceived"],
                "ingested_at": [datetime.now(UTC)],
            }
        )

        with (
            patch("pipeline.verisk.writers.delta_events.flatten_events", return_value=mock_df),
            patch.object(delta_writer, "_async_append", new_callable=AsyncMock, return_value=True),
        ):
            result = await delta_writer.write_raw_events([sample_raw_event])

        assert result is True

    @pytest.mark.asyncio
    async def test_write_raw_events_multiple(self, delta_writer):
        """Test writing multiple raw events in batch."""
        events = [
            {
                "type": "verisk.claims.property.xn.documentsReceived",
                "version": 1,
                "utcDateTime": "2024-01-01T12:00:00Z",
                "traceId": f"trace-{i}",
                "data": f'{{"assignmentId": "A{i}"}}',
            }
            for i in range(3)
        ]

        mock_df = pl.DataFrame(
            {
                "trace_id": ["trace-0", "trace-1", "trace-2"],
                "type": ["documentsReceived"] * 3,
                "ingested_at": [datetime.now(UTC)] * 3,
            }
        )

        with (
            patch("pipeline.verisk.writers.delta_events.flatten_events", return_value=mock_df),
            patch.object(delta_writer, "_async_append", new_callable=AsyncMock, return_value=True) as mock_append,
        ):
            result = await delta_writer.write_raw_events(events)

        assert result is True
        mock_append.assert_called_once()

        # Verify DataFrame passed to _async_append has created_at column added
        df_written = mock_append.call_args[0][0]
        assert "created_at" in df_written.columns

    @pytest.mark.asyncio
    async def test_write_raw_events_empty_list(self, delta_writer):
        """Test writing empty event list returns True without calling Delta."""
        result = await delta_writer.write_raw_events([])

        assert result is True

    @pytest.mark.asyncio
    async def test_write_raw_events_failure(self, delta_writer, sample_raw_event):
        """Test write failure raises the underlying error."""
        mock_df = pl.DataFrame(
            {
                "trace_id": ["test-trace-123"],
                "type": ["documentsReceived"],
                "ingested_at": [datetime.now(UTC)],
            }
        )

        with (
            patch("pipeline.verisk.writers.delta_events.flatten_events", return_value=mock_df),
            patch.object(delta_writer, "_async_append", new_callable=AsyncMock, return_value=False),
        ):
            # write_raw_events raises when _async_append returns False
            # (it checks _last_append_error or raises RuntimeError)
            with pytest.raises(RuntimeError):
                await delta_writer.write_raw_events([sample_raw_event])

    @pytest.mark.asyncio
    async def test_created_at_timestamp_added(self, delta_writer, sample_raw_event):
        """Test that created_at timestamp is added to the DataFrame."""
        mock_df = pl.DataFrame(
            {
                "trace_id": ["test-trace-123"],
                "type": ["documentsReceived"],
                "ingested_at": [datetime.now(UTC)],
            }
        )

        before = datetime.now(UTC)

        with (
            patch("pipeline.verisk.writers.delta_events.flatten_events", return_value=mock_df),
            patch.object(delta_writer, "_async_append", new_callable=AsyncMock, return_value=True) as mock_append,
        ):
            await delta_writer.write_raw_events([sample_raw_event])

        after = datetime.now(UTC)

        # Verify created_at was added
        df_written = mock_append.call_args[0][0]
        assert "created_at" in df_written.columns

        created_at = df_written["created_at"][0]
        assert before <= created_at <= after


@pytest.mark.asyncio
async def test_delta_writer_integration():
    """Integration test verifying writer config."""
    from pipeline.verisk.writers.delta_events import DeltaEventsWriter

    writer = DeltaEventsWriter(
        table_path="abfss://test@onelake/lakehouse/xact_events",
    )

    # Verify writer was initialized with correct params
    assert writer._timestamp_column == "created_at"
    assert writer._partition_column == "event_date"
    assert writer._z_order_columns == ["event_date", "trace_id", "event_id", "type"]

    # Write a raw event
    raw_event = {
        "type": "verisk.claims.property.xn.documentsReceived",
        "version": 1,
        "utcDateTime": "2024-01-01T12:00:00Z",
        "traceId": "integration-test",
        "data": '{"assignmentId": "A12345"}',
    }

    mock_df = pl.DataFrame(
        {
            "trace_id": ["integration-test"],
            "type": ["documentsReceived"],
            "ingested_at": [datetime.now(UTC)],
        }
    )

    with (
        patch("pipeline.verisk.writers.delta_events.flatten_events", return_value=mock_df),
        patch.object(writer, "_async_append", new_callable=AsyncMock, return_value=True) as mock_append,
    ):
        result = await writer.write_raw_events([raw_event])

    assert result is True
    mock_append.assert_called_once()
