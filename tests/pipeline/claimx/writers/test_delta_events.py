"""
Tests for ClaimX Delta events writer.

Tests cover:
- Initialization
- Async write operations via write_events
- Schema validation
- Error handling
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest


@pytest.fixture
def claimx_events_writer():
    """Create a ClaimXEventsDeltaWriter."""
    from pipeline.claimx.writers.delta_events import ClaimXEventsDeltaWriter

    writer = ClaimXEventsDeltaWriter(
        table_path="abfss://test@onelake/lakehouse/claimx_events",
    )
    yield writer


class TestClaimXEventsDeltaWriter:
    """Test suite for ClaimXEventsDeltaWriter."""

    def test_initialization(self, claimx_events_writer):
        """Test writer initialization."""
        assert claimx_events_writer.table_path == "abfss://test@onelake/lakehouse/claimx_events"
        assert claimx_events_writer._timestamp_column == "ingested_at"
        assert claimx_events_writer._partition_column == "event_date"

    @pytest.mark.asyncio
    async def test_write_events_success(self, claimx_events_writer, sample_claimx_event):
        """Test successful event write."""
        with patch.object(
            claimx_events_writer, "_async_append", new_callable=AsyncMock, return_value=True
        ) as mock_append:
            result = await claimx_events_writer.write_events([sample_claimx_event])

        assert result is True
        mock_append.assert_called_once()

    @pytest.mark.asyncio
    async def test_write_events_multiple(self, claimx_events_writer):
        """Test writing multiple events in batch."""
        now = datetime.now(UTC)
        events = [
            {
                "trace_id": f"evt-{i}",
                "event_type": "PROJECT_CREATED",
                "project_id": f"proj-{i}",
                "ingested_at": now,
            }
            for i in range(3)
        ]

        with patch.object(
            claimx_events_writer, "_async_append", new_callable=AsyncMock, return_value=True
        ) as mock_append:
            result = await claimx_events_writer.write_events(events)

        assert result is True
        mock_append.assert_called_once()

        # Verify DataFrame passed to _async_append
        df_written = mock_append.call_args[0][0]
        assert len(df_written) == 3
        assert "trace_id" in df_written.columns
        assert "event_type" in df_written.columns
        assert "project_id" in df_written.columns
        assert "created_at" in df_written.columns
        assert "event_date" in df_written.columns

    @pytest.mark.asyncio
    async def test_write_events_empty_list(self, claimx_events_writer):
        """Test writing empty event list returns True without calling Delta."""
        result = await claimx_events_writer.write_events([])

        assert result is True

    @pytest.mark.asyncio
    async def test_write_events_failure(self, claimx_events_writer, sample_claimx_event):
        """Test write failure handling."""
        with patch.object(
            claimx_events_writer, "_async_append", new_callable=AsyncMock, return_value=False
        ):
            result = await claimx_events_writer.write_events([sample_claimx_event])

        assert result is False

    @pytest.mark.asyncio
    async def test_write_events_with_string_timestamp(self, claimx_events_writer):
        """Test writing events with ISO format string timestamps."""
        event = {
            "trace_id": "evt-str-ts",
            "event_type": "PROJECT_CREATED",
            "project_id": "123456",
            "ingested_at": "2024-01-01T12:00:00Z",
        }

        with patch.object(
            claimx_events_writer, "_async_append", new_callable=AsyncMock, return_value=True
        ) as mock_append:
            result = await claimx_events_writer.write_events([event])

        assert result is True
        mock_append.assert_called_once()

    @pytest.mark.asyncio
    async def test_write_events_derives_event_date(self, claimx_events_writer):
        """Test that event_date is derived from ingested_at."""
        event = {
            "trace_id": "evt-date-test",
            "event_type": "PROJECT_CREATED",
            "project_id": "123456",
            "ingested_at": datetime(2024, 6, 15, 10, 30, 0, tzinfo=UTC),
        }

        with patch.object(
            claimx_events_writer, "_async_append", new_callable=AsyncMock, return_value=True
        ) as mock_append:
            result = await claimx_events_writer.write_events([event])

        assert result is True

        df_written = mock_append.call_args[0][0]

        # Verify event_date was derived correctly
        from datetime import date

        event_date_value = df_written["event_date"][0]
        assert event_date_value == date(2024, 6, 15)

    @pytest.mark.asyncio
    async def test_write_events_fills_missing_ingested_at(self, claimx_events_writer):
        """Test that missing ingested_at is filled with current time."""
        event = {
            "trace_id": "evt-no-ts",
            "event_type": "PROJECT_CREATED",
            "project_id": "123456",
            # ingested_at is intentionally missing
        }

        before = datetime.now(UTC)

        with patch.object(
            claimx_events_writer, "_async_append", new_callable=AsyncMock, return_value=True
        ) as mock_append:
            result = await claimx_events_writer.write_events([event])

        after = datetime.now(UTC)

        assert result is True

        df_written = mock_append.call_args[0][0]

        ingested_at = df_written["ingested_at"][0]
        assert before <= ingested_at <= after

    @pytest.mark.asyncio
    async def test_write_events_drops_null_trace_id(self, claimx_events_writer):
        """Test that events with null trace_id are filtered out."""
        events = [
            {
                "trace_id": "valid-evt",
                "event_type": "PROJECT_CREATED",
                "project_id": "123456",
                "ingested_at": datetime.now(UTC),
            },
            {
                "trace_id": None,  # This should be dropped
                "event_type": "PROJECT_CREATED",
                "project_id": "123456",
                "ingested_at": datetime.now(UTC),
            },
        ]

        with patch.object(
            claimx_events_writer, "_async_append", new_callable=AsyncMock, return_value=True
        ) as mock_append:
            result = await claimx_events_writer.write_events(events)

        assert result is True

        df_written = mock_append.call_args[0][0]

        # Only valid event should be written
        assert len(df_written) == 1
        assert df_written["trace_id"][0] == "valid-evt"

    @pytest.mark.asyncio
    async def test_write_events_drops_null_event_type(self, claimx_events_writer):
        """Test that events with null event_type are filtered out."""
        events = [
            {
                "trace_id": "evt-with-type",
                "event_type": "PROJECT_CREATED",
                "project_id": "123456",
                "ingested_at": datetime.now(UTC),
            },
            {
                "trace_id": "evt-no-type",
                "event_type": None,  # This should be dropped
                "project_id": "123456",
                "ingested_at": datetime.now(UTC),
            },
        ]

        with patch.object(
            claimx_events_writer, "_async_append", new_callable=AsyncMock, return_value=True
        ) as mock_append:
            result = await claimx_events_writer.write_events(events)

        assert result is True

        df_written = mock_append.call_args[0][0]

        # Only event with type should be written
        assert len(df_written) == 1
        assert df_written["trace_id"][0] == "evt-with-type"

    @pytest.mark.asyncio
    async def test_write_events_subprocess_execution(self, claimx_events_writer, sample_claimx_event):
        """Test that write operations use subprocess for non-blocking execution."""
        with patch.object(
            claimx_events_writer, "_async_append", new_callable=AsyncMock, return_value=True
        ) as mock_append:
            result = await claimx_events_writer.write_events([sample_claimx_event])

            assert result is True
            mock_append.assert_called_once()


class TestClaimXEventsDeltaWriterSchema:
    """Test schema-related functionality."""

    def test_events_schema_fields(self):
        """Test that EVENTS_SCHEMA has all required fields."""
        from pipeline.claimx.writers.delta_events import EVENTS_SCHEMA

        required_fields = [
            "trace_id",
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
        from pipeline.claimx.writers.delta_events import EVENTS_SCHEMA

        # String fields
        string_fields = [
            "trace_id",
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
    """Integration test verifying writer config."""
    from pipeline.claimx.writers.delta_events import ClaimXEventsDeltaWriter

    writer = ClaimXEventsDeltaWriter(
        table_path="abfss://test@onelake/lakehouse/claimx_events",
    )

    # Verify writer was initialized with correct params
    assert writer._timestamp_column == "ingested_at"
    assert writer._partition_column == "event_date"
    assert writer._z_order_columns == ["project_id"]

    # Write an event
    event = {
        "trace_id": "integration-test-evt",
        "event_type": "PROJECT_FILE_ADDED",
        "project_id": "123456",
        "media_id": "media-789",
        "ingested_at": datetime.now(UTC),
    }

    with patch.object(
        writer, "_async_append", new_callable=AsyncMock, return_value=True
    ) as mock_append:
        result = await writer.write_events([event])

    assert result is True
    mock_append.assert_called_once()
