"""
Tests for Delta inventory writer.

Tests cover:
- Result to DataFrame conversion
- Append-only write operations
- Async write operations
- Error handling
- Batch metrics tracking (size and latency)
"""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from kafka_pipeline.xact.schemas.results import DownloadResultMessage
from kafka_pipeline.xact.writers.delta_inventory import (
    DeltaInventoryWriter,
    DeltaFailedAttachmentsWriter,
)


@pytest.fixture
def sample_result():
    """Create a sample DownloadResultMessage for testing."""
    return DownloadResultMessage(
        trace_id="test-trace-123",
        media_id="media-123-abc",
        attachment_url="https://example.com/file1.pdf",
        blob_path="attachments/2024/01/test-trace-123/file1.pdf",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="A12345",
        status="completed",
        http_status=200,
        bytes_downloaded=12345,
        created_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    )


@pytest.fixture
def delta_writer():
    """Create a DeltaInventoryWriter with mocked Delta backend."""
    with patch("kafka_pipeline.common.writers.base.DeltaTableWriter") as mock_delta_class:
        # Setup mock instance
        mock_instance = MagicMock()
        mock_instance.append = MagicMock(return_value=1)
        mock_delta_class.return_value = mock_instance

        writer = DeltaInventoryWriter(
            table_path="abfss://test@onelake/lakehouse/xact_attachments",
        )
        yield writer


class TestDeltaInventoryWriter:
    """Test suite for DeltaInventoryWriter."""

    def test_initialization(self, delta_writer):
        """Test writer initialization."""
        assert delta_writer.table_path == "abfss://test@onelake/lakehouse/xact_attachments"
        assert delta_writer._delta_writer is not None

    def test_results_to_dataframe_single_result(self, delta_writer, sample_result):
        """Test converting a single result to DataFrame."""
        df = delta_writer._results_to_dataframe([sample_result])

        # Check DataFrame shape
        assert len(df) == 1

        # Check schema
        assert "media_id" in df.columns
        assert "trace_id" in df.columns
        assert "attachment_url" in df.columns
        assert "blob_path" in df.columns
        assert "status_subtype" in df.columns
        assert "file_type" in df.columns
        assert "assignment_id" in df.columns
        assert "bytes_downloaded" in df.columns
        assert "created_at" in df.columns

        # Check data types
        assert df.schema["media_id"] == pl.Utf8
        assert df.schema["trace_id"] == pl.Utf8
        assert df.schema["attachment_url"] == pl.Utf8
        assert df.schema["blob_path"] == pl.Utf8
        assert df.schema["status_subtype"] == pl.Utf8
        assert df.schema["file_type"] == pl.Utf8
        assert df.schema["assignment_id"] == pl.Utf8
        assert df.schema["bytes_downloaded"] == pl.Int64

        # Check values
        assert df["media_id"][0] == "media-123-abc"
        assert df["trace_id"][0] == "test-trace-123"
        assert df["attachment_url"][0] == "https://example.com/file1.pdf"
        assert df["blob_path"][0] == "attachments/2024/01/test-trace-123/file1.pdf"
        assert df["bytes_downloaded"][0] == 12345

    def test_results_to_dataframe_multiple_results(self, delta_writer):
        """Test converting multiple results to DataFrame."""
        results = [
            DownloadResultMessage(
                trace_id=f"trace-{i}",
                media_id=f"media-{i}",
                attachment_url=f"https://example.com/file{i}.pdf",
                blob_path=f"attachments/2024/01/trace-{i}/file{i}.pdf",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id=f"A{i}",
                status="completed",
                http_status=200,
                bytes_downloaded=1000 * i,
                created_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            )
            for i in range(3)
        ]

        df = delta_writer._results_to_dataframe(results)

        assert len(df) == 3
        assert df["trace_id"].to_list() == ["trace-0", "trace-1", "trace-2"]
        assert df["bytes_downloaded"].to_list() == [0, 1000, 2000]

    @pytest.mark.asyncio
    async def test_write_result_success(self, delta_writer, sample_result):
        """Test successful single result write."""
        # Mock the underlying Delta writer append
        delta_writer._delta_writer.append = MagicMock(return_value=1)

        result = await delta_writer.write_result(sample_result)

        assert result is True
        delta_writer._delta_writer.append.assert_called_once()

        # Verify DataFrame was created correctly
        call_args = delta_writer._delta_writer.append.call_args
        df = call_args[0][0]
        assert len(df) == 1
        assert df["trace_id"][0] == "test-trace-123"
        assert df["media_id"][0] == "media-123-abc"

    @pytest.mark.asyncio
    async def test_write_results_multiple(self, delta_writer):
        """Test writing multiple results in batch."""
        results = [
            DownloadResultMessage(
                trace_id=f"trace-{i}",
                media_id=f"media-{i}",
                attachment_url=f"https://example.com/file{i}.pdf",
                blob_path=f"attachments/2024/01/trace-{i}/file{i}.pdf",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id=f"A{i}",
                status="completed",
                http_status=200,
                bytes_downloaded=1000 * i,
                created_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            )
            for i in range(5)
        ]

        delta_writer._delta_writer.append = MagicMock(return_value=5)

        result = await delta_writer.write_results(results)

        assert result is True
        delta_writer._delta_writer.append.assert_called_once()

        # Verify DataFrame has all results
        call_args = delta_writer._delta_writer.append.call_args
        df = call_args[0][0]
        assert len(df) == 5

    @pytest.mark.asyncio
    async def test_write_results_empty_list(self, delta_writer):
        """Test writing empty result list returns True."""
        result = await delta_writer.write_results([])

        assert result is True
        delta_writer._delta_writer.append.assert_not_called()

    @pytest.mark.asyncio
    async def test_write_result_failure(self, delta_writer, sample_result):
        """Test write failure handling."""
        # Mock append to raise an exception
        delta_writer._delta_writer.append = MagicMock(
            side_effect=Exception("Delta append failed")
        )

        result = await delta_writer.write_result(sample_result)

        assert result is False

    @pytest.mark.asyncio
    async def test_write_results_async_execution(self, delta_writer, sample_result):
        """Test that write operations use asyncio.to_thread for non-blocking I/O."""
        # Track if asyncio.to_thread was called
        to_thread_called = []

        async def mock_to_thread(f, *args, **kwargs):
            to_thread_called.append(True)
            return f(*args, **kwargs)

        # Patch asyncio.to_thread to track calls
        with patch("asyncio.to_thread", side_effect=mock_to_thread):
            delta_writer._delta_writer.append = MagicMock(return_value=1)

            result = await delta_writer.write_result(sample_result)

            assert result is True
            # Verify asyncio.to_thread was called for non-blocking I/O
            assert len(to_thread_called) >= 1

    @pytest.mark.asyncio
    async def test_write_results_latency_metrics(self, delta_writer, sample_result):
        """Test that latency metrics are logged."""
        delta_writer._delta_writer.append = MagicMock(return_value=1)

        # Mock the logger to capture calls
        with patch.object(delta_writer, 'logger') as mock_logger:
            await delta_writer.write_result(sample_result)

            # Verify logger was called with latency_ms and batch_size
            assert mock_logger.info.called
            log_call = mock_logger.info.call_args
            extra = log_call[1]["extra"]
            assert "latency_ms" in extra
            assert "batch_size" in extra
            assert extra["batch_size"] == 1

    def test_created_at_timestamp(self, delta_writer, sample_result):
        """Test that created_at is set to current UTC time."""
        before = datetime.now(timezone.utc)
        df = delta_writer._results_to_dataframe([sample_result])
        after = datetime.now(timezone.utc)

        created_at = df["created_at"][0]

        # created_at should be between before and after
        assert before <= created_at <= after

    def test_event_date_field(self, delta_writer, sample_result):
        """Test that event_date is set to current UTC date."""
        before_date = datetime.now(timezone.utc).date()
        df = delta_writer._results_to_dataframe([sample_result])
        after_date = datetime.now(timezone.utc).date()

        event_date = df["event_date"][0]

        # event_date should be today's date
        assert event_date == before_date or event_date == after_date

    def test_timezone_handling(self, delta_writer, sample_result):
        """Test that all timestamps are timezone-aware (UTC)."""
        df = delta_writer._results_to_dataframe([sample_result])

        # Both downloaded_at and created_at should be UTC-aware
        assert df.schema["downloaded_at"] == pl.Datetime(time_zone="UTC")
        assert df.schema["created_at"] == pl.Datetime(time_zone="UTC")

    def test_blob_path_mapping(self, delta_writer, sample_result):
        """Test that blob_path is correctly mapped."""
        df = delta_writer._results_to_dataframe([sample_result])

        # blob_path from result should be in DataFrame
        assert df["blob_path"][0] == sample_result.blob_path


@pytest.mark.asyncio
async def test_delta_writer_integration():
    """Integration test with actual Delta writer (mocked storage)."""
    with patch(
        "kafka_pipeline.common.writers.base.DeltaTableWriter"
    ) as mock_delta_writer_class:
        # Setup mock
        mock_writer_instance = MagicMock()
        mock_writer_instance.append = MagicMock(return_value=1)
        mock_delta_writer_class.return_value = mock_writer_instance

        # Create writer and write result
        writer = DeltaInventoryWriter(
            table_path="abfss://test@onelake/lakehouse/xact_attachments",
        )

        result = DownloadResultMessage(
            trace_id="integration-test",
            media_id="media-integration-123",
            attachment_url="https://example.com/integration.pdf",
            blob_path="attachments/2024/01/integration-test/integration.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="A12345",
            status="completed",
            http_status=200,
            bytes_downloaded=54321,
            created_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )

        write_result = await writer.write_result(result)

        assert write_result is True
        mock_writer_instance.append.assert_called_once()

        # Verify DeltaTableWriter was initialized with correct params
        mock_delta_writer_class.assert_called_once_with(
            table_path="abfss://test@onelake/lakehouse/xact_attachments",
            timestamp_column="ingested_at",
            partition_column="event_date",
            z_order_columns=[],
        )


# =============================================================================
# DeltaFailedAttachmentsWriter Tests
# =============================================================================


@pytest.fixture
def sample_failed_result():
    """Create a sample failed DownloadResultMessage for testing."""
    return DownloadResultMessage(
        trace_id="test-trace-failed-123",
        media_id="media-failed-123",
        attachment_url="https://example.com/missing.pdf",
        blob_path="documentsReceived/A12345/pdf/missing.pdf",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="A12345",
        status="failed_permanent",
        http_status=404,
        bytes_downloaded=0,
        error_message="File not found: 404 response",
        created_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    )


@pytest.fixture
def failed_writer():
    """Create a DeltaFailedAttachmentsWriter with mocked Delta backend."""
    with patch("kafka_pipeline.common.writers.base.DeltaTableWriter") as mock_delta_class:
        # Setup mock instance
        mock_instance = MagicMock()
        mock_instance.merge = MagicMock(return_value=1)
        mock_delta_class.return_value = mock_instance

        writer = DeltaFailedAttachmentsWriter(
            table_path="abfss://test@onelake/lakehouse/xact_attachments_failed",
        )
        yield writer


class TestDeltaFailedAttachmentsWriter:
    """Test suite for DeltaFailedAttachmentsWriter."""

    def test_initialization(self, failed_writer):
        """Test writer initialization."""
        assert failed_writer.table_path == "abfss://test@onelake/lakehouse/xact_attachments_failed"
        assert failed_writer._delta_writer is not None

    def test_results_to_dataframe_single_result(self, failed_writer, sample_failed_result):
        """Test converting a single failed result to DataFrame."""
        df = failed_writer._results_to_dataframe([sample_failed_result])

        # Check DataFrame shape
        assert len(df) == 1

        # Check schema - matches the failed attachments schema from delta_inventory.py
        assert "media_id" in df.columns
        assert "trace_id" in df.columns
        assert "attachment_url" in df.columns
        assert "error_message" in df.columns
        assert "status" in df.columns
        assert "failed_at" in df.columns
        assert "retry_count" in df.columns
        assert "http_status" in df.columns
        assert "created_at" in df.columns

        # Check data types
        assert df.schema["media_id"] == pl.Utf8
        assert df.schema["trace_id"] == pl.Utf8
        assert df.schema["attachment_url"] == pl.Utf8
        assert df.schema["error_message"] == pl.Utf8

        # Check values
        assert df["media_id"][0] == "media-failed-123"
        assert df["trace_id"][0] == "test-trace-failed-123"
        assert df["attachment_url"][0] == "https://example.com/missing.pdf"
        assert df["error_message"][0] == "File not found: 404 response"
        assert df["status"][0] == "failed_permanent"

    def test_results_to_dataframe_multiple_results(self, failed_writer):
        """Test converting multiple failed results to DataFrame."""
        results = [
            DownloadResultMessage(
                trace_id=f"trace-failed-{i}",
                media_id=f"media-failed-{i}",
                attachment_url=f"https://example.com/missing{i}.pdf",
                blob_path=f"documentsReceived/A{i}/pdf/missing{i}.pdf",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id=f"A{i}",
                status="failed_permanent",
                http_status=404,
                bytes_downloaded=0,
                error_message=f"Error {i}",
                created_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            )
            for i in range(3)
        ]

        df = failed_writer._results_to_dataframe(results)

        assert len(df) == 3
        assert df["trace_id"].to_list() == ["trace-failed-0", "trace-failed-1", "trace-failed-2"]
        assert df["error_message"].to_list() == ["Error 0", "Error 1", "Error 2"]

    def test_results_to_dataframe_null_error_handling(self, failed_writer):
        """Test that null error_message is handled."""
        result = DownloadResultMessage(
            trace_id="trace-null-error",
            media_id="media-null-error",
            attachment_url="https://example.com/null.pdf",
            blob_path="documentsReceived/A12345/pdf/null.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="A12345",
            status="failed_permanent",
            http_status=500,
            bytes_downloaded=0,
            error_message=None,  # Null
            created_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )

        df = failed_writer._results_to_dataframe([result])

        # Should have default value for null error_message
        assert df["error_message"][0] is None or df["error_message"][0] == "Unknown error"

    @pytest.mark.asyncio
    async def test_write_result_success(self, failed_writer, sample_failed_result):
        """Test successful single failed result write."""
        failed_writer._delta_writer.merge = MagicMock(return_value=1)

        result = await failed_writer.write_result(sample_failed_result)

        assert result is True
        failed_writer._delta_writer.merge.assert_called_once()

        # Verify DataFrame was created correctly
        call_args = failed_writer._delta_writer.merge.call_args
        df = call_args[0][0]
        assert len(df) == 1
        assert df["media_id"][0] == "media-failed-123"
        assert df["trace_id"][0] == "test-trace-failed-123"
        assert df["error_message"][0] == "File not found: 404 response"

        # Verify merge keys (merge on media_id for idempotency)
        assert call_args[1]["merge_keys"] == ["media_id"]

    @pytest.mark.asyncio
    async def test_write_results_empty_list(self, failed_writer):
        """Test writing empty result list returns True."""
        result = await failed_writer.write_results([])

        assert result is True
        failed_writer._delta_writer.merge.assert_not_called()

    @pytest.mark.asyncio
    async def test_write_result_failure(self, failed_writer, sample_failed_result):
        """Test write failure handling."""
        failed_writer._delta_writer.merge = MagicMock(
            side_effect=Exception("Delta merge failed")
        )

        result = await failed_writer.write_result(sample_failed_result)

        assert result is False

    @pytest.mark.asyncio
    async def test_write_results_preserve_columns(self, failed_writer, sample_failed_result):
        """Test that created_at is preserved during merge updates."""
        failed_writer._delta_writer.merge = MagicMock(return_value=1)

        await failed_writer.write_result(sample_failed_result)

        call_args = failed_writer._delta_writer.merge.call_args
        preserve_columns = call_args[1]["preserve_columns"]
        assert preserve_columns == ["created_at"]


@pytest.mark.asyncio
async def test_failed_writer_integration():
    """Integration test for DeltaFailedAttachmentsWriter."""
    with patch(
        "kafka_pipeline.common.writers.base.DeltaTableWriter"
    ) as mock_delta_writer_class:
        mock_writer_instance = MagicMock()
        mock_writer_instance.merge = MagicMock(return_value=1)
        mock_delta_writer_class.return_value = mock_writer_instance

        writer = DeltaFailedAttachmentsWriter(
            table_path="abfss://test@onelake/lakehouse/xact_attachments_failed",
        )

        result = DownloadResultMessage(
            trace_id="integration-failed-test",
            media_id="media-integration-failed-123",
            attachment_url="https://example.com/integration-fail.pdf",
            blob_path="documentsReceived/A12345/pdf/integration-fail.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="A12345",
            status="failed_permanent",
            http_status=500,
            bytes_downloaded=0,
            error_message="Integration test error",
            created_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )

        write_result = await writer.write_result(result)

        assert write_result is True
        mock_writer_instance.merge.assert_called_once()

        # Verify DeltaTableWriter was initialized with correct params
        mock_delta_writer_class.assert_called_once_with(
            table_path="abfss://test@onelake/lakehouse/xact_attachments_failed",
            timestamp_column="ingested_at",
            partition_column=None,
            z_order_columns=["media_id", "failed_at"],
        )
