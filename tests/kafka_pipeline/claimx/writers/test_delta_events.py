"""
Tests for ClaimX Delta events writer.

Tests cover:
- Initialization
- Async write operations via write_events
- Schema validation
- Error handling
"""

import asyncio
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import polars as pl
import pytest


@pytest.fixture
def claimx_events_writer():
    """Create a ClaimXEventsDeltaWriter with mocked Delta backend."""
    with patch("kafka_pipeline.common.writers.base.DeltaTableWriter") as mock_delta_class:
        # Setup mock instance
        mock_instance = MagicMock()
        mock_instance.append = MagicMock(return_value=1)
        mock_delta_class.return_value = mock_instance

        from kafka_pipeline.claimx.writers.delta_events import ClaimXEventsDeltaWriter

        writer = ClaimXEventsDeltaWriter(
            table_path="abfss://test@onelake/lakehouse/claimx_events",
        )
        yield writer


class TestClaimXEventsDeltaWriter:
    """Test suite for ClaimXEventsDeltaWriter."""

    def test_initialization(self, claimx_events_writer):
        """Test writer initialization."""
        assert claimx_events_writer.table_path == "abfss://test@onelake/lakehouse/claimx_events"
        assert claimx_events_writer._delta_writer is not None

    @pytest.mark.asyncio
    async def test_write_events_success(self, claimx_events_writer, sample_claimx_event):
        """Test successful event write."""
        result = await claimx_events_writer.write_events([sample_claimx_event])

        assert result is True
        claimx_events_writer._delta_writer.append.assert_called_once()

    @pytest.mark.asyncio
    async def test_write_events_multiple(self, claimx_events_writer):
        """Test writing multiple events in batch."""
        now = datetime.now(timezone.utc)
        events = [
            {
                "event_id": f"evt-{i}",
                "event_type": "PROJECT_CREATED",
                "project_id": f"proj-{i}",
                "ingested_at": now,
            }
            for i in range(3)
        ]

        result = await claimx_events_writer.write_events(events)

        assert result is True
        claimx_events_writer._delta_writer.append.assert_called_once()

        # Verify DataFrame passed to append
        call_args = claimx_events_writer._delta_writer.append.call_args
        df_written = call_args[0][0]
        assert len(df_written) == 3
        assert "event_id" in df_written.columns
        assert "event_type" in df_written.columns
        assert "project_id" in df_written.columns
        assert "created_at" in df_written.columns
        assert "event_date" in df_written.columns

    @pytest.mark.asyncio
    async def test_write_events_empty_list(self, claimx_events_writer):
        """Test writing empty event list returns True without calling Delta."""
        result = await claimx_events_writer.write_events([])

        assert result is True
        claimx_events_writer._delta_writer.append.assert_not_called()

    @pytest.mark.asyncio
    async def test_write_events_failure(self, claimx_events_writer, sample_claimx_event):
        """Test write failure handling."""
        # Mock append to raise an exception
        claimx_events_writer._delta_writer.append = MagicMock(
            side_effect=Exception("Delta write failed")
        )

        result = await claimx_events_writer.write_events([sample_claimx_event])

        assert result is False

    @pytest.mark.asyncio
    async def test_write_events_with_string_timestamp(self, claimx_events_writer):
        """Test writing events with ISO format string timestamps."""
        event = {
            "event_id": "evt-str-ts",
            "event_type": "PROJECT_CREATED",
            "project_id": "123456",
            "ingested_at": "2024-01-01T12:00:00Z",
        }

        result = await claimx_events_writer.write_events([event])

        assert result is True
        claimx_events_writer._delta_writer.append.assert_called_once()

    @pytest.mark.asyncio
    async def test_write_events_derives_event_date(self, claimx_events_writer):
        """Test that event_date is derived from ingested_at."""
        event = {
            "event_id": "evt-date-test",
            "event_type": "PROJECT_CREATED",
            "project_id": "123456",
            "ingested_at": datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc),
        }

        result = await claimx_events_writer.write_events([event])

        assert result is True

        call_args = claimx_events_writer._delta_writer.append.call_args
        df_written = call_args[0][0]

        # Verify event_date was derived correctly
        from datetime import date
        event_date_value = df_written["event_date"][0]
        assert event_date_value == date(2024, 6, 15)

    @pytest.mark.asyncio
    async def test_write_events_fills_missing_ingested_at(self, claimx_events_writer):
        """Test that missing ingested_at is filled with current time."""
        event = {
            "event_id": "evt-no-ts",
            "event_type": "PROJECT_CREATED",
            "project_id": "123456",
            # ingested_at is intentionally missing
        }

        before = datetime.now(timezone.utc)
        result = await claimx_events_writer.write_events([event])
        after = datetime.now(timezone.utc)

        assert result is True

        call_args = claimx_events_writer._delta_writer.append.call_args
        df_written = call_args[0][0]

        ingested_at = df_written["ingested_at"][0]
        assert before <= ingested_at <= after

    @pytest.mark.asyncio
    async def test_write_events_drops_null_event_id(self, claimx_events_writer):
        """Test that events with null event_id are filtered out."""
        events = [
            {
                "event_id": "valid-evt",
                "event_type": "PROJECT_CREATED",
                "project_id": "123456",
                "ingested_at": datetime.now(timezone.utc),
            },
            {
                "event_id": None,  # This should be dropped
                "event_type": "PROJECT_CREATED",
                "project_id": "123456",
                "ingested_at": datetime.now(timezone.utc),
            },
        ]

        result = await claimx_events_writer.write_events(events)

        assert result is True

        call_args = claimx_events_writer._delta_writer.append.call_args
        df_written = call_args[0][0]

        # Only valid event should be written
        assert len(df_written) == 1
        assert df_written["event_id"][0] == "valid-evt"

    @pytest.mark.asyncio
    async def test_write_events_drops_null_event_type(self, claimx_events_writer):
        """Test that events with null event_type are filtered out."""
        events = [
            {
                "event_id": "evt-with-type",
                "event_type": "PROJECT_CREATED",
                "project_id": "123456",
                "ingested_at": datetime.now(timezone.utc),
            },
            {
                "event_id": "evt-no-type",
                "event_type": None,  # This should be dropped
                "project_id": "123456",
                "ingested_at": datetime.now(timezone.utc),
            },
        ]

        result = await claimx_events_writer.write_events(events)

        assert result is True

        call_args = claimx_events_writer._delta_writer.append.call_args
        df_written = call_args[0][0]

        # Only event with type should be written
        assert len(df_written) == 1
        assert df_written["event_id"][0] == "evt-with-type"

    @pytest.mark.asyncio
    async def test_write_events_async_execution(self, claimx_events_writer, sample_claimx_event):
        """Test that write operations use asyncio.to_thread for non-blocking execution."""
        to_thread_call_count = [0]

        async def mock_to_thread_impl(func, *args, **kwargs):
            to_thread_call_count[0] += 1
            return func(*args, **kwargs)

        with patch("asyncio.to_thread", side_effect=mock_to_thread_impl):
            result = await claimx_events_writer.write_events([sample_claimx_event])

            assert result is True
            # Verify to_thread was called (proving async execution)
            assert to_thread_call_count[0] >= 1


class TestClaimXEventsDeltaWriterSchema:
    """Test schema-related functionality."""

    def test_events_schema_fields(self):
        """Test that EVENTS_SCHEMA has all required fields."""
        from kafka_pipeline.claimx.writers.delta_events import EVENTS_SCHEMA

        required_fields = [
            "event_id",
            "event_type",
            "project_id",
            "media_id",
            "task_assignment_id",
            "video_collaboration_id",
            "master_file_name",
            "ingested_at",
            "created_at",
            "event_date",
        ]

        for field in required_fields:
            assert field in EVENTS_SCHEMA, f"Missing required field: {field}"

    def test_events_schema_types(self):
        """Test that EVENTS_SCHEMA has correct Polars types."""
        from kafka_pipeline.claimx.writers.delta_events import EVENTS_SCHEMA

        # String fields
        string_fields = [
            "event_id",
            "event_type",
            "project_id",
            "media_id",
            "task_assignment_id",
            "video_collaboration_id",
            "master_file_name",
        ]
        for field in string_fields:
            assert EVENTS_SCHEMA[field] == pl.Utf8, f"{field} should be Utf8"

        # Datetime fields
        assert EVENTS_SCHEMA["ingested_at"] == pl.Datetime("us", "UTC")
        assert EVENTS_SCHEMA["created_at"] == pl.Datetime("us", "UTC")

        # Date field
        assert EVENTS_SCHEMA["event_date"] == pl.Date


@pytest.mark.asyncio
async def test_claimx_events_writer_integration():
    """Integration test with DeltaTableWriter mocked."""
    with patch("kafka_pipeline.common.writers.base.DeltaTableWriter") as mock_delta_writer_class:
        # Setup mock
        mock_writer_instance = MagicMock()
        mock_writer_instance.append = MagicMock(return_value=1)
        mock_delta_writer_class.return_value = mock_writer_instance

        from kafka_pipeline.claimx.writers.delta_events import ClaimXEventsDeltaWriter

        # Create writer
        writer = ClaimXEventsDeltaWriter(
            table_path="abfss://test@onelake/lakehouse/claimx_events",
        )

        # Verify DeltaTableWriter was initialized with correct params
        mock_delta_writer_class.assert_called_once_with(
            table_path="abfss://test@onelake/lakehouse/claimx_events",
            timestamp_column="ingested_at",
            partition_column="event_date",
            z_order_columns=["project_id"],
        )

        # Write an event
        event = {
            "event_id": "integration-test-evt",
            "event_type": "PROJECT_FILE_ADDED",
            "project_id": "123456",
            "media_id": "media-789",
            "ingested_at": datetime.now(timezone.utc),
        }

        result = await writer.write_events([event])

        assert result is True
        mock_writer_instance.append.assert_called_once()
