"""
Tests for Delta events writer.

Tests cover:
- Initialization
- Async write operations via write_raw_events
- Error handling
"""

import asyncio
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

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
    """Create a DeltaEventsWriter with mocked Delta backend."""
    with patch("kafka_pipeline.common.writers.base.DeltaTableWriter") as mock_delta_class:
        # Setup mock instance
        mock_instance = MagicMock()
        mock_instance.append = MagicMock(return_value=1)
        mock_delta_class.return_value = mock_instance

        from kafka_pipeline.xact.writers.delta_events import DeltaEventsWriter

        writer = DeltaEventsWriter(
            table_path="abfss://test@onelake/lakehouse/xact_events",
        )
        yield writer


class TestDeltaEventsWriter:
    """Test suite for DeltaEventsWriter."""

    def test_initialization(self, delta_writer):
        """Test writer initialization."""
        assert delta_writer.table_path == "abfss://test@onelake/lakehouse/xact_events"
        assert delta_writer._delta_writer is not None

    @pytest.mark.asyncio
    async def test_write_raw_events_success(self, delta_writer, sample_raw_event):
        """Test successful raw event write."""
        # Mock flatten_events to return a valid DataFrame
        mock_df = pl.DataFrame(
            {
                "trace_id": ["test-trace-123"],
                "type": ["documentsReceived"],
                "ingested_at": [datetime.now(timezone.utc)],
            }
        )

        with patch(
            "kafka_pipeline.xact.writers.delta_events.flatten_events", return_value=mock_df
        ):
            result = await delta_writer.write_raw_events([sample_raw_event])

        assert result is True
        delta_writer._delta_writer.append.assert_called_once()

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
                "ingested_at": [datetime.now(timezone.utc)] * 3,
            }
        )

        with patch(
            "kafka_pipeline.xact.writers.delta_events.flatten_events", return_value=mock_df
        ):
            result = await delta_writer.write_raw_events(events)

        assert result is True
        delta_writer._delta_writer.append.assert_called_once()

        # Verify DataFrame passed to append has created_at column added
        call_args = delta_writer._delta_writer.append.call_args
        df_written = call_args[0][0]
        assert "created_at" in df_written.columns

    @pytest.mark.asyncio
    async def test_write_raw_events_empty_list(self, delta_writer):
        """Test writing empty event list returns True without calling Delta."""
        result = await delta_writer.write_raw_events([])

        assert result is True
        delta_writer._delta_writer.append.assert_not_called()

    @pytest.mark.asyncio
    async def test_write_raw_events_failure(self, delta_writer, sample_raw_event):
        """Test write failure handling."""
        mock_df = pl.DataFrame(
            {
                "trace_id": ["test-trace-123"],
                "type": ["documentsReceived"],
                "ingested_at": [datetime.now(timezone.utc)],
            }
        )

        with patch(
            "kafka_pipeline.xact.writers.delta_events.flatten_events", return_value=mock_df
        ):
            # Mock append to raise an exception
            delta_writer._delta_writer.append = MagicMock(
                side_effect=Exception("Delta write failed")
            )

            result = await delta_writer.write_raw_events([sample_raw_event])

        assert result is False

    @pytest.mark.asyncio
    async def test_write_raw_events_async_execution(
        self, delta_writer, sample_raw_event
    ):
        """Test that write operations use asyncio.to_thread for non-blocking execution."""
        mock_df = pl.DataFrame(
            {
                "trace_id": ["test-trace-123"],
                "type": ["documentsReceived"],
                "ingested_at": [datetime.now(timezone.utc)],
            }
        )

        to_thread_call_count = [0]

        async def mock_to_thread_impl(func, *args, **kwargs):
            # Actually call the function to simulate to_thread behavior
            to_thread_call_count[0] += 1
            return func(*args, **kwargs)

        with patch(
            "kafka_pipeline.xact.writers.delta_events.flatten_events", return_value=mock_df
        ):
            with patch(
                "asyncio.to_thread",
                side_effect=mock_to_thread_impl,
            ):
                result = await delta_writer.write_raw_events([sample_raw_event])

                assert result is True
                # Verify to_thread was called (proving async execution)
                assert to_thread_call_count[0] >= 1

    @pytest.mark.asyncio
    async def test_created_at_timestamp_added(self, delta_writer, sample_raw_event):
        """Test that created_at timestamp is added to the DataFrame."""
        mock_df = pl.DataFrame(
            {
                "trace_id": ["test-trace-123"],
                "type": ["documentsReceived"],
                "ingested_at": [datetime.now(timezone.utc)],
            }
        )

        before = datetime.now(timezone.utc)

        with patch(
            "kafka_pipeline.xact.writers.delta_events.flatten_events", return_value=mock_df
        ):
            await delta_writer.write_raw_events([sample_raw_event])

        after = datetime.now(timezone.utc)

        # Verify created_at was added
        call_args = delta_writer._delta_writer.append.call_args
        df_written = call_args[0][0]
        assert "created_at" in df_written.columns

        created_at = df_written["created_at"][0]
        assert before <= created_at <= after


@pytest.mark.asyncio
async def test_delta_writer_integration():
    """Integration test with DeltaTableWriter mocked."""
    with patch(
        "kafka_pipeline.common.writers.base.DeltaTableWriter"
    ) as mock_delta_writer_class:
        # Setup mock
        mock_writer_instance = MagicMock()
        mock_writer_instance.append = MagicMock(return_value=1)
        mock_delta_writer_class.return_value = mock_writer_instance

        from kafka_pipeline.xact.writers.delta_events import DeltaEventsWriter

        # Create writer
        writer = DeltaEventsWriter(
            table_path="abfss://test@onelake/lakehouse/xact_events",
        )

        # Verify DeltaTableWriter was initialized with correct params
        mock_delta_writer_class.assert_called_once_with(
            table_path="abfss://test@onelake/lakehouse/xact_events",
            timestamp_column="created_at",
            partition_column="event_date",
            z_order_columns=["event_date", "trace_id", "event_id", "type"],
        )

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
                "ingested_at": [datetime.now(timezone.utc)],
            }
        )

        with patch(
            "kafka_pipeline.xact.writers.delta_events.flatten_events", return_value=mock_df
        ):
            result = await writer.write_raw_events([raw_event])

        assert result is True
        mock_writer_instance.append.assert_called_once()
