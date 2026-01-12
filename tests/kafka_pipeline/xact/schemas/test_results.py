"""
Tests for DownloadResultMessage and FailedDownloadMessage schemas.

Validates Pydantic model behavior, JSON serialization, status enum handling,
error truncation, and DLQ message structure.
"""

import json
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from kafka_pipeline.xact.schemas.results import DownloadResultMessage, FailedDownloadMessage
from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage


class TestDownloadResultMessageCreation:
    """Test DownloadResultMessage instantiation with valid data."""

    def test_create_completed_result(self):
        """DownloadResultMessage can be created for successful download."""
        result = DownloadResultMessage(
            trace_id="evt-123",
            media_id="media-123",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="claims/C-456/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-456",
            status="completed",
            http_status=200,
            bytes_downloaded=2048576,
            created_at=datetime(2024, 12, 25, 10, 31, 15, tzinfo=timezone.utc)
        )

        assert result.trace_id == "evt-123"
        assert result.status == "completed"
        assert result.blob_path == "claims/C-456/file.pdf"
        assert result.bytes_downloaded == 2048576
        assert result.error_message is None

    def test_create_failed_result(self):
        """DownloadResultMessage can be created for transient failure."""
        result = DownloadResultMessage(
            trace_id="evt-456",
            media_id="media-456",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/C-456/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-456",
            status="failed",
            bytes_downloaded=0,
            error_message="Connection timeout after 30 seconds",
            created_at=datetime.now(timezone.utc)
        )

        assert result.status == "failed"
        assert result.bytes_downloaded == 0
        assert result.error_message == "Connection timeout after 30 seconds"

    def test_create_failed_permanent_result(self):
        """DownloadResultMessage can be created for permanent failure."""
        result = DownloadResultMessage(
            trace_id="evt-789",
            media_id="media-789",
            attachment_url="https://storage.example.com/invalid.exe",
            blob_path="documentsReceived/C-789/exe/invalid.exe",
            status_subtype="documentsReceived",
            file_type="exe",
            assignment_id="C-789",
            status="failed_permanent",
            http_status=403,
            bytes_downloaded=0,
            error_message="File type .exe not allowed",
            created_at=datetime.now(timezone.utc)
        )

        assert result.status == "failed_permanent"
        assert result.error_message == "File type .exe not allowed"


class TestDownloadResultMessageValidation:
    """Test field validation rules for DownloadResultMessage."""

    def test_missing_required_fields_raises_error(self):
        """Missing required fields raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            DownloadResultMessage(
                trace_id="evt-123",
                attachment_url="https://storage.example.com/file.pdf",
                status="completed"
                # Missing media_id, blob_path, status_subtype, file_type, assignment_id, created_at
            )

        errors = exc_info.value.errors()
        assert any(e['loc'] == ('media_id',) for e in errors)
        assert any(e['loc'] == ('blob_path',) for e in errors)
        assert any(e['loc'] == ('status_subtype',) for e in errors)
        assert any(e['loc'] == ('file_type',) for e in errors)
        assert any(e['loc'] == ('assignment_id',) for e in errors)
        assert any(e['loc'] == ('created_at',) for e in errors)

    def test_empty_trace_id_raises_error(self):
        """Empty trace_id raises ValidationError."""
        with pytest.raises(ValidationError):
            DownloadResultMessage(
                trace_id="",
                media_id="media-123",
                attachment_url="https://storage.example.com/file.pdf",
                blob_path="claims/C-456/file.pdf",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id="C-456",
                status="completed",
                created_at=datetime.now(timezone.utc)
            )

    def test_whitespace_trace_id_raises_error(self):
        """Whitespace-only trace_id raises ValidationError."""
        with pytest.raises(ValidationError):
            DownloadResultMessage(
                trace_id="   ",
                media_id="media-123",
                attachment_url="https://storage.example.com/file.pdf",
                blob_path="claims/C-456/file.pdf",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id="C-456",
                status="completed",
                created_at=datetime.now(timezone.utc)
            )

    def test_invalid_status_raises_error(self):
        """Invalid status value raises ValidationError."""
        with pytest.raises(ValidationError):
            DownloadResultMessage(
                trace_id="evt-123",
                media_id="media-123",
                attachment_url="https://storage.example.com/file.pdf",
                blob_path="claims/C-456/file.pdf",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id="C-456",
                status="invalid_status",
                created_at=datetime.now(timezone.utc)
            )

    def test_negative_bytes_downloaded_raises_error(self):
        """Negative bytes_downloaded raises ValidationError."""
        with pytest.raises(ValidationError):
            DownloadResultMessage(
                trace_id="evt-123",
                media_id="media-123",
                attachment_url="https://storage.example.com/file.pdf",
                blob_path="claims/C-456/file.pdf",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id="C-456",
                status="completed",
                bytes_downloaded=-100,
                created_at=datetime.now(timezone.utc)
            )

    def test_whitespace_is_trimmed(self):
        """Leading/trailing whitespace is trimmed from string fields."""
        result = DownloadResultMessage(
            trace_id="  evt-123  ",
            media_id="  media-123  ",
            attachment_url="  https://storage.example.com/file.pdf  ",
            blob_path="  claims/C-456/file.pdf  ",
            status_subtype="  documentsReceived  ",
            file_type="  pdf  ",
            assignment_id="  C-456  ",
            status="completed",
            created_at=datetime.now(timezone.utc)
        )

        assert result.trace_id == "evt-123"
        assert result.attachment_url == "https://storage.example.com/file.pdf"
        assert result.blob_path == "claims/C-456/file.pdf"


class TestDownloadResultMessageStatusEnum:
    """Test status field values and validation."""

    def test_completed_status_is_valid(self):
        """Status 'completed' is accepted."""
        result = DownloadResultMessage(
            trace_id="evt-123",
            media_id="media-123",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="claims/C-456/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-456",
            status="completed",
            created_at=datetime.now(timezone.utc)
        )
        assert result.status == "completed"

    def test_failed_status_is_valid(self):
        """Status 'failed' is accepted."""
        result = DownloadResultMessage(
            trace_id="evt-123",
            media_id="media-123",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="claims/C-456/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-456",
            status="failed",
            created_at=datetime.now(timezone.utc)
        )
        assert result.status == "failed"

    def test_failed_permanent_status_is_valid(self):
        """Status 'failed_permanent' is accepted."""
        result = DownloadResultMessage(
            trace_id="evt-123",
            media_id="media-123",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="claims/C-456/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-456",
            status="failed_permanent",
            created_at=datetime.now(timezone.utc)
        )
        assert result.status == "failed_permanent"


class TestDownloadResultMessageSerialization:
    """Test JSON serialization and deserialization."""

    def test_serialize_completed_to_json(self):
        """DownloadResultMessage serializes to valid JSON."""
        result = DownloadResultMessage(
            trace_id="evt-123",
            media_id="media-123",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="claims/C-456/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-456",
            status="completed",
            http_status=200,
            bytes_downloaded=2048576,
            created_at=datetime(2024, 12, 25, 10, 31, 15, tzinfo=timezone.utc)
        )

        json_str = result.model_dump_json()
        parsed = json.loads(json_str)

        assert parsed["trace_id"] == "evt-123"
        assert parsed["status"] == "completed"
        assert parsed["blob_path"] == "claims/C-456/file.pdf"
        assert parsed["bytes_downloaded"] == 2048576
        assert parsed["created_at"] == "2024-12-25T10:31:15+00:00"

    def test_serialize_failure_to_json(self):
        """Failed result serializes with error fields."""
        result = DownloadResultMessage(
            trace_id="evt-456",
            media_id="media-456",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/C-456/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-456",
            status="failed",
            bytes_downloaded=0,
            error_message="Connection timeout",
            created_at=datetime(2024, 12, 25, 10, 31, 45, tzinfo=timezone.utc)
        )

        json_str = result.model_dump_json()
        parsed = json.loads(json_str)

        assert parsed["status"] == "failed"
        assert parsed["error_message"] == "Connection timeout"
        assert parsed["bytes_downloaded"] == 0

    def test_deserialize_from_json(self):
        """DownloadResultMessage can be created from JSON."""
        json_data = {
            "trace_id": "evt-789",
            "media_id": "media-789",
            "attachment_url": "https://storage.example.com/doc.pdf",
            "blob_path": "policies/P-001/doc.pdf",
            "status_subtype": "documentsReceived",
            "file_type": "pdf",
            "assignment_id": "P-001",
            "status": "completed",
            "http_status": 200,
            "bytes_downloaded": 1024,
            "retry_count": 0,
            "error_message": None,
            "created_at": "2024-12-25T15:45:00Z"
        }

        json_str = json.dumps(json_data)
        result = DownloadResultMessage.model_validate_json(json_str)

        assert result.trace_id == "evt-789"
        assert result.status == "completed"
        assert result.bytes_downloaded == 1024

    def test_round_trip_serialization(self):
        """Data survives JSON serialization round-trip."""
        original = DownloadResultMessage(
            trace_id="evt-round-trip",
            media_id="media-round-trip",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/C-456/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-456",
            status="failed_permanent",
            bytes_downloaded=0,
            error_message="Invalid file type",
            created_at=datetime(2024, 12, 25, 10, 30, 0, tzinfo=timezone.utc)
        )

        json_str = original.model_dump_json()
        restored = DownloadResultMessage.model_validate_json(json_str)

        assert restored.trace_id == original.trace_id
        assert restored.status == original.status
        assert restored.error_message == original.error_message


class TestErrorMessageTruncation:
    """Test error message truncation to prevent huge messages."""

    def test_error_message_under_limit_not_truncated(self):
        """Error messages under 500 chars are not truncated."""
        short_error = "Connection timeout after 30 seconds"
        result = DownloadResultMessage(
            trace_id="evt-123",
            media_id="media-123",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/C-456/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-456",
            status="failed",
            bytes_downloaded=0,
            error_message=short_error,
            created_at=datetime.now(timezone.utc)
        )

        assert result.error_message == short_error

    def test_error_message_over_limit_is_truncated(self):
        """Error messages over 500 chars are truncated."""
        long_error = "x" * 600
        result = DownloadResultMessage(
            trace_id="evt-123",
            media_id="media-123",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/C-456/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-456",
            status="failed",
            bytes_downloaded=0,
            error_message=long_error,
            created_at=datetime.now(timezone.utc)
        )

        assert len(result.error_message) == 500
        assert result.error_message.endswith("...")

    def test_error_message_exactly_500_chars_not_truncated(self):
        """Error message exactly 500 chars is not truncated."""
        exact_error = "x" * 500
        result = DownloadResultMessage(
            trace_id="evt-123",
            media_id="media-123",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/C-456/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-456",
            status="failed",
            bytes_downloaded=0,
            error_message=exact_error,
            created_at=datetime.now(timezone.utc)
        )

        assert len(result.error_message) == 500
        assert result.error_message == exact_error


class TestFailedDownloadMessageCreation:
    """Test FailedDownloadMessage instantiation for DLQ."""

    def test_create_dlq_message(self):
        """FailedDownloadMessage can be created with original task."""
        task = DownloadTaskMessage(
            trace_id="evt-456",
            media_id="media-456",
            attachment_url="https://storage.example.com/bad.pdf",
            blob_path="documentsReceived/C-789/pdf/bad.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-789",
            event_type="claim",
            event_subtype="created",
            retry_count=4,
            original_timestamp=datetime(2024, 12, 25, 10, 0, 0, tzinfo=timezone.utc)
        )

        dlq = FailedDownloadMessage(
            trace_id="evt-456",
            media_id="media-456",
            attachment_url="https://storage.example.com/bad.pdf",
            original_task=task,
            final_error="File not found after 4 retries",
            error_category="permanent",
            retry_count=4,
            failed_at=datetime(2024, 12, 25, 10, 45, 0, tzinfo=timezone.utc)
        )

        assert dlq.trace_id == "evt-456"
        assert dlq.original_task == task
        assert dlq.final_error == "File not found after 4 retries"
        assert dlq.error_category == "permanent"
        assert dlq.retry_count == 4

    def test_create_dlq_with_truncated_error(self):
        """FailedDownloadMessage truncates long error messages."""
        task = DownloadTaskMessage(
            trace_id="evt-error",
            media_id="media-error",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/C-001/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-001",
            event_type="claim",
            event_subtype="created",
            retry_count=4,
            original_timestamp=datetime.now(timezone.utc)
        )

        long_error = "Error: " + "x" * 600
        dlq = FailedDownloadMessage(
            trace_id="evt-error",
            media_id="media-error",
            attachment_url="https://storage.example.com/file.pdf",
            original_task=task,
            final_error=long_error,
            error_category="transient",
            retry_count=4,
            failed_at=datetime.now(timezone.utc)
        )

        assert len(dlq.final_error) == 500
        assert dlq.final_error.endswith("...")


class TestFailedDownloadMessageValidation:
    """Test field validation rules for FailedDownloadMessage."""

    def test_missing_required_fields_raises_error(self):
        """Missing required fields raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            FailedDownloadMessage(
                trace_id="evt-123",
                attachment_url="https://storage.example.com/file.pdf"
                # Missing media_id, original_task, final_error, error_category, retry_count, failed_at
            )

        errors = exc_info.value.errors()
        assert any(e['loc'] == ('media_id',) for e in errors)
        assert any(e['loc'] == ('original_task',) for e in errors)
        assert any(e['loc'] == ('final_error',) for e in errors)

    def test_empty_trace_id_raises_error(self):
        """Empty trace_id raises ValidationError."""
        task = DownloadTaskMessage(
            trace_id="evt-456",
            media_id="media-456",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/C-789/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-789",
            event_type="claim",
            event_subtype="created",
            original_timestamp=datetime.now(timezone.utc)
        )

        with pytest.raises(ValidationError):
            FailedDownloadMessage(
                trace_id="",
                media_id="media-456",
                attachment_url="https://storage.example.com/file.pdf",
                original_task=task,
                final_error="Error occurred",
                error_category="permanent",
                retry_count=4,
                failed_at=datetime.now(timezone.utc)
            )

    def test_empty_final_error_raises_error(self):
        """Empty final_error raises ValidationError."""
        task = DownloadTaskMessage(
            trace_id="evt-456",
            media_id="media-456",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/C-789/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-789",
            event_type="claim",
            event_subtype="created",
            original_timestamp=datetime.now(timezone.utc)
        )

        with pytest.raises(ValidationError):
            FailedDownloadMessage(
                trace_id="evt-456",
                media_id="media-456",
                attachment_url="https://storage.example.com/file.pdf",
                original_task=task,
                final_error="",
                error_category="permanent",
                retry_count=4,
                failed_at=datetime.now(timezone.utc)
            )

    def test_whitespace_final_error_raises_error(self):
        """Whitespace-only final_error raises ValidationError."""
        task = DownloadTaskMessage(
            trace_id="evt-456",
            media_id="media-456",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/C-789/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-789",
            event_type="claim",
            event_subtype="created",
            original_timestamp=datetime.now(timezone.utc)
        )

        with pytest.raises(ValidationError):
            FailedDownloadMessage(
                trace_id="evt-456",
                media_id="media-456",
                attachment_url="https://storage.example.com/file.pdf",
                original_task=task,
                final_error="   ",
                error_category="permanent",
                retry_count=4,
                failed_at=datetime.now(timezone.utc)
            )

    def test_whitespace_is_trimmed(self):
        """Leading/trailing whitespace is trimmed from string fields."""
        task = DownloadTaskMessage(
            trace_id="evt-456",
            media_id="media-456",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/C-789/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-789",
            event_type="claim",
            event_subtype="created",
            original_timestamp=datetime.now(timezone.utc)
        )

        dlq = FailedDownloadMessage(
            trace_id="  evt-456  ",
            media_id="  media-456  ",
            attachment_url="  https://storage.example.com/file.pdf  ",
            original_task=task,
            final_error="  Error occurred  ",
            error_category="  permanent  ",
            retry_count=4,
            failed_at=datetime.now(timezone.utc)
        )

        assert dlq.trace_id == "evt-456"
        assert dlq.attachment_url == "https://storage.example.com/file.pdf"
        assert dlq.final_error == "Error occurred"
        assert dlq.error_category == "permanent"


class TestFailedDownloadMessageSerialization:
    """Test JSON serialization and deserialization for DLQ messages."""

    def test_serialize_to_json(self):
        """FailedDownloadMessage serializes to valid JSON with nested task."""
        task = DownloadTaskMessage(
            trace_id="evt-dlq",
            media_id="media-dlq",
            attachment_url="https://storage.example.com/missing.pdf",
            blob_path="documentsReceived/C-999/pdf/missing.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-999",
            event_type="claim",
            event_subtype="created",
            retry_count=4,
            original_timestamp=datetime(2024, 12, 25, 10, 0, 0, tzinfo=timezone.utc),
            metadata={"last_error": "File not found (404)"}
        )

        dlq = FailedDownloadMessage(
            trace_id="evt-dlq",
            media_id="media-dlq",
            attachment_url="https://storage.example.com/missing.pdf",
            original_task=task,
            final_error="File not found (404) - exhausted retries",
            error_category="permanent",
            retry_count=4,
            failed_at=datetime(2024, 12, 25, 10, 45, 0, tzinfo=timezone.utc)
        )

        json_str = dlq.model_dump_json()
        parsed = json.loads(json_str)

        assert parsed["trace_id"] == "evt-dlq"
        assert parsed["final_error"] == "File not found (404) - exhausted retries"
        assert parsed["retry_count"] == 4
        assert "original_task" in parsed
        assert parsed["original_task"]["trace_id"] == "evt-dlq"
        assert parsed["original_task"]["retry_count"] == 4

    def test_deserialize_from_json(self):
        """FailedDownloadMessage can be created from JSON."""
        json_data = {
            "trace_id": "evt-dlq",
            "media_id": "media-dlq",
            "attachment_url": "https://storage.example.com/missing.pdf",
            "original_task": {
                "trace_id": "evt-dlq",
                "media_id": "media-dlq",
                "attachment_url": "https://storage.example.com/missing.pdf",
                "blob_path": "documentsReceived/C-999/pdf/missing.pdf",
                "status_subtype": "documentsReceived",
                "file_type": "pdf",
                "assignment_id": "C-999",
                "event_type": "claim",
                "event_subtype": "created",
                "retry_count": 4,
                "original_timestamp": "2024-12-25T10:00:00Z",
                "metadata": {}
            },
            "final_error": "File not found",
            "error_category": "permanent",
            "retry_count": 4,
            "failed_at": "2024-12-25T10:45:00Z"
        }

        json_str = json.dumps(json_data)
        dlq = FailedDownloadMessage.model_validate_json(json_str)

        assert dlq.trace_id == "evt-dlq"
        assert dlq.final_error == "File not found"
        assert dlq.retry_count == 4
        assert isinstance(dlq.original_task, DownloadTaskMessage)
        assert dlq.original_task.trace_id == "evt-dlq"

    def test_round_trip_serialization(self):
        """DLQ message survives JSON serialization round-trip."""
        task = DownloadTaskMessage(
            trace_id="evt-round",
            media_id="media-round",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/C-123/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-123",
            event_type="claim",
            event_subtype="created",
            retry_count=4,
            original_timestamp=datetime(2024, 12, 25, 10, 0, 0, tzinfo=timezone.utc)
        )

        original = FailedDownloadMessage(
            trace_id="evt-round",
            media_id="media-round",
            attachment_url="https://storage.example.com/file.pdf",
            original_task=task,
            final_error="Connection failed",
            error_category="transient",
            retry_count=4,
            failed_at=datetime(2024, 12, 25, 10, 30, 0, tzinfo=timezone.utc)
        )

        json_str = original.model_dump_json()
        restored = FailedDownloadMessage.model_validate_json(json_str)

        assert restored.trace_id == original.trace_id
        assert restored.final_error == original.final_error
        assert restored.retry_count == original.retry_count
        assert restored.original_task.trace_id == original.original_task.trace_id


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_zero_bytes_downloaded_is_valid(self):
        """Zero bytes downloaded is a valid value."""
        result = DownloadResultMessage(
            trace_id="evt-zero",
            media_id="media-zero",
            attachment_url="https://storage.example.com/empty.txt",
            blob_path="documentsReceived/C-456/txt/empty.txt",
            status_subtype="documentsReceived",
            file_type="txt",
            assignment_id="C-456",
            status="completed",
            bytes_downloaded=0,
            created_at=datetime.now(timezone.utc)
        )
        assert result.bytes_downloaded == 0

    def test_very_large_bytes_downloaded(self):
        """Very large bytes_downloaded values are accepted."""
        result = DownloadResultMessage(
            trace_id="evt-large",
            media_id="media-large",
            attachment_url="https://storage.example.com/huge.zip",
            blob_path="documentsReceived/C-456/zip/huge.zip",
            status_subtype="documentsReceived",
            file_type="zip",
            assignment_id="C-456",
            status="completed",
            bytes_downloaded=10_737_418_240,  # 10 GB
            created_at=datetime.now(timezone.utc)
        )
        assert result.bytes_downloaded == 10_737_418_240

    def test_unicode_in_error_message(self):
        """Error messages can contain Unicode characters."""
        result = DownloadResultMessage(
            trace_id="evt-unicode",
            media_id="media-unicode",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/C-456/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-456",
            status="failed_permanent",
            bytes_downloaded=0,
            error_message="Datei nicht gefunden: Schadenubersicht.pdf",
            created_at=datetime.now(timezone.utc)
        )
        assert "Schadenubersicht" in result.error_message

    def test_naive_datetime_is_accepted(self):
        """Naive datetime (without timezone) is accepted."""
        naive_dt = datetime(2024, 12, 25, 10, 30, 0)
        result = DownloadResultMessage(
            trace_id="evt-naive",
            media_id="media-naive",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/C-456/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-456",
            status="completed",
            created_at=naive_dt
        )
        assert result.created_at == naive_dt

    def test_to_tracking_row(self):
        """Test conversion to tracking table row format."""
        result = DownloadResultMessage(
            trace_id="evt-tracking",
            media_id="media-tracking",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/C-456/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-456",
            status="completed",
            http_status=200,
            bytes_downloaded=1024,
            created_at=datetime(2024, 12, 25, 10, 30, 0, tzinfo=timezone.utc)
        )

        row = result.to_tracking_row()

        assert row["trace_id"] == "evt-tracking"
        assert row["media_id"] == "media-tracking"
        assert row["blob_path"] == "documentsReceived/C-456/pdf/file.pdf"
        assert row["status"] == "completed"
        assert row["bytes_downloaded"] == 1024
