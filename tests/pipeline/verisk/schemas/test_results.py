"""Tests for pipeline.verisk.schemas.results module."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from pipeline.verisk.schemas.results import DownloadResultMessage, FailedDownloadMessage
from pipeline.verisk.schemas.tasks import DownloadTaskMessage


def _make_result(**overrides):
    defaults = {
        "trace_id": "t1",
        "media_id": "m1",
        "attachment_url": "https://example.com/file.pdf",
        "blob_path": "docs/A1/pdf/file.pdf",
        "status_subtype": "documentsReceived",
        "file_type": "pdf",
        "assignment_id": "A1",
        "status": "completed",
        "created_at": datetime(2024, 1, 1, tzinfo=UTC),
    }
    defaults.update(overrides)
    return DownloadResultMessage(**defaults)


def _make_download_task():
    return DownloadTaskMessage(
        trace_id="t1",
        media_id="m1",
        attachment_url="https://example.com/file.pdf",
        blob_path="docs/A1/pdf/file.pdf",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="A1",
        event_type="xact",
        event_subtype="documentsReceived",
        original_timestamp=datetime(2024, 1, 1, tzinfo=UTC),
    )


class TestDownloadResultMessage:
    def test_valid_completed(self):
        result = _make_result(http_status=200, bytes_downloaded=1024)
        assert result.status == "completed"
        assert result.bytes_downloaded == 1024

    def test_valid_failed(self):
        result = _make_result(status="failed", error_message="timeout", http_status=503)
        assert result.status == "failed"
        assert result.error_message == "timeout"

    def test_valid_failed_permanent(self):
        result = _make_result(status="failed_permanent", error_message="404 not found")
        assert result.status == "failed_permanent"

    def test_invalid_status_rejected(self):
        with pytest.raises(ValidationError):
            _make_result(status="unknown_status")

    def test_empty_string_field_rejected(self):
        with pytest.raises(ValidationError):
            _make_result(trace_id="")

    def test_whitespace_only_rejected(self):
        with pytest.raises(ValidationError):
            _make_result(trace_id="   ")

    def test_strips_whitespace(self):
        result = _make_result(trace_id="  t1  ")
        assert result.trace_id == "t1"

    def test_error_message_truncation(self):
        long_error = "x" * 600
        result = _make_result(error_message=long_error)
        assert len(result.error_message) == 500
        assert result.error_message.endswith("...")

    def test_error_message_none(self):
        result = _make_result(error_message=None)
        assert result.error_message is None

    def test_to_tracking_row(self):
        result = _make_result(http_status=200, bytes_downloaded=512)
        row = result.to_tracking_row()
        assert row["trace_id"] == "t1"
        assert row["status"] == "completed"
        assert row["bytes_downloaded"] == 512
        assert row["http_status"] == 200
        assert "created_at" in row

    def test_timestamp_serialization(self):
        result = _make_result(expires_at=datetime(2024, 6, 1, tzinfo=UTC))
        data = result.model_dump()
        assert isinstance(data["created_at"], str)
        assert isinstance(data["expires_at"], str)

    def test_timestamp_serialization_none(self):
        result = _make_result()
        data = result.model_dump()
        assert data["expires_at"] is None

    def test_optional_fields_defaults(self):
        result = _make_result()
        assert result.http_status is None
        assert result.bytes_downloaded == 0
        assert result.retry_count == 0
        assert result.error_message is None
        assert result.expires_at is None
        assert result.expired_at_ingest is None


class TestFailedDownloadMessage:
    def test_valid_creation(self):
        task = _make_download_task()
        msg = FailedDownloadMessage(
            trace_id="t1",
            attachment_url="https://example.com/file.pdf",
            original_task=task,
            error_category="permanent",
            retry_count=3,
            failed_at=datetime(2024, 1, 1, tzinfo=UTC),
        )
        assert msg.trace_id == "t1"
        assert msg.retry_count == 3
        assert msg.original_task.trace_id == "t1"

    def test_empty_string_rejected(self):
        task = _make_download_task()
        with pytest.raises(ValidationError):
            FailedDownloadMessage(
                trace_id="",
                attachment_url="https://example.com/file.pdf",
                original_task=task,
                error_category="permanent",
                retry_count=3,
                failed_at=datetime(2024, 1, 1, tzinfo=UTC),
            )

    def test_timestamp_serialization(self):
        task = _make_download_task()
        msg = FailedDownloadMessage(
            trace_id="t1",
            attachment_url="https://example.com/file.pdf",
            original_task=task,
            error_category="permanent",
            retry_count=3,
            failed_at=datetime(2024, 1, 1, tzinfo=UTC),
        )
        data = msg.model_dump()
        assert isinstance(data["failed_at"], str)
