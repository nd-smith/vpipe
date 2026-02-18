"""
Tests for Delta inventory writer.

Tests cover:
- Result to DataFrame conversion
- Append-only write operations
- Async write operations
- Error handling
- Batch metrics tracking (size and latency)
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from pipeline.verisk.schemas.results import DownloadResultMessage
from pipeline.verisk.writers.delta_inventory import (
    DeltaFailedAttachmentsWriter,
    DeltaInventoryWriter,
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
        created_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
    )


@pytest.fixture
def delta_writer():
    """Create a DeltaInventoryWriter."""
    writer = DeltaInventoryWriter(
        table_path="abfss://test@onelake/lakehouse/xact_attachments",
    )
    yield writer


class TestDeltaInventoryWriter:
    """Test suite for DeltaInventoryWriter."""

    def test_initialization(self, delta_writer):
        """Test writer initialization."""
        assert delta_writer.table_path == "abfss://test@onelake/lakehouse/xact_attachments"
        assert delta_writer._partition_column == "event_date"

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
                created_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
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
        with patch.object(
            delta_writer, "_async_append", new_callable=AsyncMock, return_value=True
        ) as mock_append:
            result = await delta_writer.write_result(sample_result)

        assert result is True
        mock_append.assert_called_once()

        # Verify DataFrame was created correctly
        df = mock_append.call_args[0][0]
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
                created_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
            )
            for i in range(5)
        ]

        with patch.object(
            delta_writer, "_async_append", new_callable=AsyncMock, return_value=True
        ) as mock_append:
            result = await delta_writer.write_results(results)

        assert result is True
        mock_append.assert_called_once()

        # Verify DataFrame has all results
        df = mock_append.call_args[0][0]
        assert len(df) == 5

    @pytest.mark.asyncio
    async def test_write_results_empty_list(self, delta_writer):
        """Test writing empty result list returns True."""
        result = await delta_writer.write_results([])

        assert result is True

    @pytest.mark.asyncio
    async def test_write_result_failure(self, delta_writer, sample_result):
        """Test write failure handling."""
        with patch.object(
            delta_writer, "_async_append", new_callable=AsyncMock, return_value=False
        ):
            result = await delta_writer.write_result(sample_result)

        assert result is False

    @pytest.mark.asyncio
    async def test_write_results_latency_metrics(self, delta_writer, sample_result):
        """Test that latency metrics are logged."""
        with patch.object(
            delta_writer, "_async_append", new_callable=AsyncMock, return_value=True
        ):
            # Mock the logger to capture calls
            with patch.object(delta_writer, "logger") as mock_logger:
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
        before = datetime.now(UTC)
        df = delta_writer._results_to_dataframe([sample_result])
        after = datetime.now(UTC)

        created_at = df["created_at"][0]

        # created_at should be between before and after
        assert before <= created_at <= after

    def test_event_date_field(self, delta_writer, sample_result):
        """Test that event_date is set to current UTC date."""
        before_date = datetime.now(UTC).date()
        df = delta_writer._results_to_dataframe([sample_result])
        after_date = datetime.now(UTC).date()

        event_date = df["event_date"][0]

        # event_date should be today's date
        assert event_date in (before_date, after_date)

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
    """Integration test verifying writer config."""
    writer = DeltaInventoryWriter(
        table_path="abfss://test@onelake/lakehouse/xact_attachments",
    )

    # Verify writer was initialized with correct params
    assert writer._timestamp_column == "ingested_at"
    assert writer._partition_column == "event_date"
    assert writer._z_order_columns == []

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
        created_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
    )

    with patch.object(
        writer, "_async_append", new_callable=AsyncMock, return_value=True
    ) as mock_append:
        write_result = await writer.write_result(result)

    assert write_result is True
    mock_append.assert_called_once()


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
        created_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
    )


@pytest.fixture
def failed_writer():
    """Create a DeltaFailedAttachmentsWriter."""
    writer = DeltaFailedAttachmentsWriter(
        table_path="abfss://test@onelake/lakehouse/xact_attachments_failed",
    )
    yield writer


class TestDeltaFailedAttachmentsWriter:
    """Test suite for DeltaFailedAttachmentsWriter."""

    def test_initialization(self, failed_writer):
        """Test writer initialization."""
        assert failed_writer.table_path == "abfss://test@onelake/lakehouse/xact_attachments_failed"
        assert failed_writer._z_order_columns == ["media_id", "failed_at"]

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
                created_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
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
            created_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
        )

        df = failed_writer._results_to_dataframe([result])

        # Should have default value for null error_message
        assert df["error_message"][0] is None or df["error_message"][0] == "Unknown error"

    @pytest.mark.asyncio
    async def test_write_result_success(self, failed_writer, sample_failed_result):
        """Test successful single failed result write."""
        with patch.object(
            failed_writer, "_async_merge", new_callable=AsyncMock, return_value=True
        ) as mock_merge:
            result = await failed_writer.write_result(sample_failed_result)

        assert result is True
        mock_merge.assert_called_once()

        # Verify DataFrame was created correctly
        df = mock_merge.call_args[0][0]
        assert len(df) == 1
        assert df["media_id"][0] == "media-failed-123"
        assert df["trace_id"][0] == "test-trace-failed-123"
        assert df["error_message"][0] == "File not found: 404 response"

        # Verify merge keys (merge on media_id for idempotency)
        assert mock_merge.call_args[1]["merge_keys"] == ["media_id"]

    @pytest.mark.asyncio
    async def test_write_results_empty_list(self, failed_writer):
        """Test writing empty result list returns True."""
        result = await failed_writer.write_results([])

        assert result is True

    @pytest.mark.asyncio
    async def test_write_result_failure(self, failed_writer, sample_failed_result):
        """Test write failure handling."""
        with patch.object(
            failed_writer, "_async_merge", new_callable=AsyncMock, return_value=False
        ):
            result = await failed_writer.write_result(sample_failed_result)

        assert result is False

    @pytest.mark.asyncio
    async def test_write_results_preserve_columns(self, failed_writer, sample_failed_result):
        """Test that created_at is preserved during merge updates."""
        with patch.object(
            failed_writer, "_async_merge", new_callable=AsyncMock, return_value=True
        ) as mock_merge:
            await failed_writer.write_result(sample_failed_result)

        preserve_columns = mock_merge.call_args[1]["preserve_columns"]
        assert preserve_columns == ["created_at"]


@pytest.mark.asyncio
async def test_failed_writer_integration():
    """Integration test for DeltaFailedAttachmentsWriter."""
    writer = DeltaFailedAttachmentsWriter(
        table_path="abfss://test@onelake/lakehouse/xact_attachments_failed",
    )

    # Verify writer was initialized with correct params
    assert writer._timestamp_column == "ingested_at"
    assert writer._partition_column is None
    assert writer._z_order_columns == ["media_id", "failed_at"]

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
        created_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
    )

    with patch.object(
        writer, "_async_merge", new_callable=AsyncMock, return_value=True
    ) as mock_merge:
        write_result = await writer.write_result(result)

    assert write_result is True
    mock_merge.assert_called_once()
