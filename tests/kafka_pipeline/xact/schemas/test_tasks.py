"""
Tests for DownloadTaskMessage schema.

Validates Pydantic model behavior, JSON serialization, retry tracking,
and metadata extensibility.
"""

import json
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage


class TestDownloadTaskMessageCreation:
    """Test DownloadTaskMessage instantiation with valid data."""

    def test_create_with_all_fields(self):
        """DownloadTaskMessage can be created with all fields populated."""
        task = DownloadTaskMessage(
            trace_id="evt-123",
            media_id="media-123",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/A12345/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="A12345",
            event_type="xact",
            event_subtype="documentsReceived",
            retry_count=0,
            original_timestamp=datetime(2024, 12, 25, 10, 30, 0, tzinfo=timezone.utc),
            metadata={"file_size": 1024, "content_type": "application/pdf"}
        )

        assert task.trace_id == "evt-123"
        assert task.attachment_url == "https://storage.example.com/file.pdf"
        assert task.blob_path == "documentsReceived/A12345/pdf/file.pdf"
        assert task.status_subtype == "documentsReceived"
        assert task.file_type == "pdf"
        assert task.assignment_id == "A12345"
        assert task.event_type == "xact"
        assert task.event_subtype == "documentsReceived"
        assert task.retry_count == 0
        assert task.metadata == {"file_size": 1024, "content_type": "application/pdf"}

    def test_create_with_defaults(self):
        """DownloadTaskMessage uses default values for optional fields."""
        task = DownloadTaskMessage(
            trace_id="evt-456",
            media_id="media-456",
            attachment_url="https://storage.example.com/image.jpg",
            blob_path="estimateCreated/B67890/jpg/image.jpg",
            status_subtype="estimateCreated",
            file_type="jpg",
            assignment_id="B67890",
            event_type="xact",
            event_subtype="estimateCreated",
            original_timestamp=datetime.now(timezone.utc)
        )

        assert task.retry_count == 0  # Default
        assert task.metadata == {}  # Default

    def test_create_with_retry_count(self):
        """DownloadTaskMessage correctly tracks retry attempts."""
        task = DownloadTaskMessage(
            trace_id="evt-retry",
            media_id="media-retry",
            attachment_url="https://storage.example.com/doc.pdf",
            blob_path="documentsReceived/C-001/pdf/doc.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-001",
            event_type="xact",
            event_subtype="documentsReceived",
            retry_count=3,
            original_timestamp=datetime.now(timezone.utc)
        )

        assert task.retry_count == 3

    def test_create_with_complex_metadata(self):
        """Metadata can contain nested structures."""
        complex_metadata = {
            "file_info": {
                "size": 2048576,
                "content_type": "application/pdf",
                "checksum": "abc123"
            },
            "retry_history": [
                {"attempt": 1, "error": "timeout"},
                {"attempt": 2, "error": "network"}
            ],
            "source_system": "claimx"
        }

        task = DownloadTaskMessage(
            trace_id="evt-complex",
            media_id="media-complex",
            attachment_url="https://storage.example.com/large.pdf",
            blob_path="documentsReceived/C-999/pdf/large.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-999",
            event_type="xact",
            event_subtype="documentsReceived",
            retry_count=2,
            original_timestamp=datetime.now(timezone.utc),
            metadata=complex_metadata
        )

        assert task.metadata == complex_metadata
        assert task.metadata["file_info"]["size"] == 2048576
        assert len(task.metadata["retry_history"]) == 2


class TestDownloadTaskMessageValidation:
    """Test field validation rules."""

    def test_missing_required_field_raises_error(self):
        """Missing required fields raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            DownloadTaskMessage(
                trace_id="evt-123",
                media_id="media-123",
                attachment_url="https://storage.example.com/file.pdf",
                event_type="xact",
                event_subtype="documentsReceived",
                original_timestamp=datetime.now(timezone.utc)
                # Missing blob_path, status_subtype, file_type, assignment_id
            )

        errors = exc_info.value.errors()
        assert any(e['loc'] == ('blob_path',) for e in errors)

    def test_empty_trace_id_raises_error(self):
        """Empty trace_id raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            DownloadTaskMessage(
                trace_id="",
                media_id="media-123",
                attachment_url="https://storage.example.com/file.pdf",
                blob_path="documentsReceived/A12345/pdf/file.pdf",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id="A12345",
                event_type="xact",
                event_subtype="documentsReceived",
                original_timestamp=datetime.now(timezone.utc)
            )

        errors = exc_info.value.errors()
        assert any('trace_id' in str(e) for e in errors)

    def test_whitespace_trace_id_raises_error(self):
        """Whitespace-only trace_id raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            DownloadTaskMessage(
                trace_id="   ",
                media_id="media-123",
                attachment_url="https://storage.example.com/file.pdf",
                blob_path="documentsReceived/A12345/pdf/file.pdf",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id="A12345",
                event_type="xact",
                event_subtype="documentsReceived",
                original_timestamp=datetime.now(timezone.utc)
            )

        errors = exc_info.value.errors()
        assert any('trace_id' in str(e) for e in errors)

    def test_empty_attachment_url_raises_error(self):
        """Empty attachment_url raises ValidationError."""
        with pytest.raises(ValidationError):
            DownloadTaskMessage(
                trace_id="evt-123",
                media_id="media-123",
                attachment_url="",
                blob_path="documentsReceived/A12345/pdf/file.pdf",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id="A12345",
                event_type="xact",
                event_subtype="documentsReceived",
                original_timestamp=datetime.now(timezone.utc)
            )

    def test_empty_blob_path_raises_error(self):
        """Empty blob_path raises ValidationError."""
        with pytest.raises(ValidationError):
            DownloadTaskMessage(
                trace_id="evt-123",
                media_id="media-123",
                attachment_url="https://storage.example.com/file.pdf",
                blob_path="",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id="A12345",
                event_type="xact",
                event_subtype="documentsReceived",
                original_timestamp=datetime.now(timezone.utc)
            )

    def test_whitespace_is_trimmed(self):
        """Leading/trailing whitespace is trimmed from string fields."""
        task = DownloadTaskMessage(
            trace_id="  evt-123  ",
            media_id="  media-123  ",
            attachment_url="  https://storage.example.com/file.pdf  ",
            blob_path="  documentsReceived/A12345/pdf/file.pdf  ",
            status_subtype="  documentsReceived  ",
            file_type="  pdf  ",
            assignment_id="  A12345  ",
            event_type="  xact  ",
            event_subtype="  documentsReceived  ",
            original_timestamp=datetime.now(timezone.utc)
        )

        assert task.trace_id == "evt-123"
        assert task.attachment_url == "https://storage.example.com/file.pdf"
        assert task.blob_path == "documentsReceived/A12345/pdf/file.pdf"
        assert task.event_type == "xact"
        assert task.event_subtype == "documentsReceived"

    def test_negative_retry_count_raises_error(self):
        """Negative retry_count raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            DownloadTaskMessage(
                trace_id="evt-123",
                media_id="media-123",
                attachment_url="https://storage.example.com/file.pdf",
                blob_path="documentsReceived/A12345/pdf/file.pdf",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id="A12345",
                event_type="xact",
                event_subtype="documentsReceived",
                retry_count=-1,
                original_timestamp=datetime.now(timezone.utc)
            )

        errors = exc_info.value.errors()
        assert any('retry_count' in str(e) for e in errors)

    def test_large_retry_count_is_valid(self):
        """Large retry_count values are accepted."""
        task = DownloadTaskMessage(
            trace_id="evt-many-retries",
            media_id="media-many-retries",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/A12345/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="A12345",
            event_type="xact",
            event_subtype="documentsReceived",
            retry_count=100,
            original_timestamp=datetime.now(timezone.utc)
        )

        assert task.retry_count == 100


class TestDownloadTaskMessageSerialization:
    """Test JSON serialization and deserialization."""

    def test_serialize_to_json(self):
        """DownloadTaskMessage serializes to valid JSON."""
        task = DownloadTaskMessage(
            trace_id="evt-123",
            media_id="media-123",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/A12345/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="A12345",
            event_type="xact",
            event_subtype="documentsReceived",
            retry_count=0,
            original_timestamp=datetime(2024, 12, 25, 10, 30, 0, tzinfo=timezone.utc),
            metadata={"file_size": 1024}
        )

        json_str = task.model_dump_json()
        parsed = json.loads(json_str)

        assert parsed["trace_id"] == "evt-123"
        assert parsed["attachment_url"] == "https://storage.example.com/file.pdf"
        assert parsed["blob_path"] == "documentsReceived/A12345/pdf/file.pdf"
        assert parsed["event_type"] == "xact"
        assert parsed["retry_count"] == 0
        assert parsed["original_timestamp"] == "2024-12-25T10:30:00+00:00"
        assert parsed["metadata"] == {"file_size": 1024}

    def test_deserialize_from_json(self):
        """DownloadTaskMessage can be created from JSON string."""
        json_data = {
            "trace_id": "evt-789",
            "media_id": "media-789",
            "attachment_url": "https://storage.example.com/doc.pdf",
            "blob_path": "estimateCreated/P-001/pdf/doc.pdf",
            "status_subtype": "estimateCreated",
            "file_type": "pdf",
            "assignment_id": "P-001",
            "event_type": "xact",
            "event_subtype": "estimateCreated",
            "retry_count": 2,
            "original_timestamp": "2024-12-25T15:45:00Z",
            "metadata": {"source": "policysys"}
        }

        json_str = json.dumps(json_data)
        task = DownloadTaskMessage.model_validate_json(json_str)

        assert task.trace_id == "evt-789"
        assert task.retry_count == 2
        assert task.metadata == {"source": "policysys"}

    def test_datetime_serialization_iso_format(self):
        """Datetime fields serialize to ISO 8601 format."""
        task = DownloadTaskMessage(
            trace_id="evt-123",
            media_id="media-123",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/A12345/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="A12345",
            event_type="xact",
            event_subtype="documentsReceived",
            original_timestamp=datetime(2024, 12, 25, 10, 30, 45, tzinfo=timezone.utc)
        )

        json_str = task.model_dump_json()
        parsed = json.loads(json_str)

        # Verify ISO 8601 format
        assert "2024-12-25" in parsed["original_timestamp"]
        assert "10:30:45" in parsed["original_timestamp"]

        # Verify it can be parsed back
        timestamp_str = parsed["original_timestamp"]
        datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))

    def test_round_trip_serialization(self):
        """Data survives JSON serialization round-trip."""
        original = DownloadTaskMessage(
            trace_id="evt-round-trip",
            media_id="media-round-trip",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/A12345/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="A12345",
            event_type="xact",
            event_subtype="documentsReceived",
            retry_count=3,
            original_timestamp=datetime(2024, 12, 25, 10, 30, 0, tzinfo=timezone.utc),
            metadata={"key": "value", "number": 42}
        )

        # Serialize to JSON and back
        json_str = original.model_dump_json()
        restored = DownloadTaskMessage.model_validate_json(json_str)

        assert restored.trace_id == original.trace_id
        assert restored.attachment_url == original.attachment_url
        assert restored.blob_path == original.blob_path
        assert restored.event_type == original.event_type
        assert restored.event_subtype == original.event_subtype
        assert restored.retry_count == original.retry_count
        assert restored.original_timestamp == original.original_timestamp
        assert restored.metadata == original.metadata

    def test_model_dump_dict(self):
        """model_dump() returns dict with serialized datetime."""
        task = DownloadTaskMessage(
            trace_id="evt-123",
            media_id="media-123",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/A12345/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="A12345",
            event_type="xact",
            event_subtype="documentsReceived",
            original_timestamp=datetime(2024, 12, 25, 10, 30, 0, tzinfo=timezone.utc)
        )

        data = task.model_dump()

        assert isinstance(data, dict)
        assert data["trace_id"] == "evt-123"
        # field_serializer applies even in regular model_dump()
        assert isinstance(data["original_timestamp"], str)
        assert "2024-12-25" in data["original_timestamp"]

    def test_model_dump_with_mode_json(self):
        """model_dump(mode='json') converts datetime to string."""
        task = DownloadTaskMessage(
            trace_id="evt-123",
            media_id="media-123",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/A12345/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="A12345",
            event_type="xact",
            event_subtype="documentsReceived",
            original_timestamp=datetime(2024, 12, 25, 10, 30, 0, tzinfo=timezone.utc)
        )

        data = task.model_dump(mode='json')

        assert isinstance(data, dict)
        assert isinstance(data["original_timestamp"], str)
        assert "2024-12-25" in data["original_timestamp"]


class TestDownloadTaskMessageRetryTracking:
    """Test retry count tracking and original timestamp preservation."""

    def test_original_timestamp_preserved_through_retries(self):
        """original_timestamp remains constant across retry attempts."""
        original_time = datetime(2024, 12, 25, 10, 0, 0, tzinfo=timezone.utc)

        # Initial task
        task_v1 = DownloadTaskMessage(
            trace_id="evt-retry-test",
            media_id="media-retry-test",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/A12345/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="A12345",
            event_type="xact",
            event_subtype="documentsReceived",
            retry_count=0,
            original_timestamp=original_time
        )

        # Simulate retry - same timestamp, incremented count
        task_v2 = DownloadTaskMessage(
            trace_id="evt-retry-test",
            media_id="media-retry-test",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/A12345/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="A12345",
            event_type="xact",
            event_subtype="documentsReceived",
            retry_count=1,
            original_timestamp=original_time  # Same as v1
        )

        assert task_v1.original_timestamp == task_v2.original_timestamp
        assert task_v2.retry_count == 1

    def test_retry_count_increments_correctly(self):
        """Retry count increases with each attempt."""
        for attempt in range(5):
            task = DownloadTaskMessage(
                trace_id="evt-retry-increment",
                media_id="media-retry-increment",
                attachment_url="https://storage.example.com/file.pdf",
                blob_path="documentsReceived/A12345/pdf/file.pdf",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id="A12345",
                event_type="xact",
                event_subtype="documentsReceived",
                retry_count=attempt,
                original_timestamp=datetime.now(timezone.utc)
            )

            assert task.retry_count == attempt


class TestDownloadTaskMessageMetadata:
    """Test metadata field extensibility."""

    def test_empty_metadata_is_default(self):
        """Metadata defaults to empty dict."""
        task = DownloadTaskMessage(
            trace_id="evt-123",
            media_id="media-123",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/A12345/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="A12345",
            event_type="xact",
            event_subtype="documentsReceived",
            original_timestamp=datetime.now(timezone.utc)
        )

        assert task.metadata == {}

    def test_metadata_extensibility(self):
        """Metadata can store arbitrary additional context."""
        task = DownloadTaskMessage(
            trace_id="evt-meta",
            media_id="media-meta",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/A12345/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="A12345",
            event_type="xact",
            event_subtype="documentsReceived",
            original_timestamp=datetime.now(timezone.utc),
            metadata={
                "correlation_id": "corr-123",
                "user_id": "user-456",
                "priority": "high",
                "custom_field": True,
                "tags": ["urgent", "important"]
            }
        )

        assert task.metadata["correlation_id"] == "corr-123"
        assert task.metadata["priority"] == "high"
        assert task.metadata["custom_field"] is True
        assert "urgent" in task.metadata["tags"]


class TestDownloadTaskMessageEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_very_long_url(self):
        """Very long URLs are accepted."""
        long_url = "https://storage.example.com/" + "x" * 2000 + ".pdf"
        task = DownloadTaskMessage(
            trace_id="evt-long-url",
            media_id="media-long-url",
            attachment_url=long_url,
            blob_path="documentsReceived/A12345/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="A12345",
            event_type="xact",
            event_subtype="documentsReceived",
            original_timestamp=datetime.now(timezone.utc)
        )

        assert task.attachment_url == long_url

    def test_very_long_blob_path(self):
        """Very long blob paths are accepted."""
        long_path = "claims/" + "/".join(["dir"] * 100) + "/file.pdf"
        task = DownloadTaskMessage(
            trace_id="evt-long-path",
            media_id="media-long-path",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path=long_path,
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="A12345",
            event_type="xact",
            event_subtype="documentsReceived",
            original_timestamp=datetime.now(timezone.utc)
        )

        assert task.blob_path == long_path

    def test_unicode_in_metadata(self):
        """Metadata can contain Unicode characters."""
        task = DownloadTaskMessage(
            trace_id="evt-unicode",
            media_id="media-unicode",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/A12345/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="A12345",
            event_type="xact",
            event_subtype="documentsReceived",
            original_timestamp=datetime.now(timezone.utc),
            metadata={
                "filename": "Schadenubersicht.pdf",
                "note": "Important document",
                "tags": ["urgent", "important"]
            }
        )

        assert task.metadata["filename"] == "Schadenubersicht.pdf"
        assert "urgent" in task.metadata["tags"]

    def test_naive_datetime_is_accepted(self):
        """Naive datetime (without timezone) is accepted."""
        naive_dt = datetime(2024, 12, 25, 10, 30, 0)
        task = DownloadTaskMessage(
            trace_id="evt-naive",
            media_id="media-naive",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="documentsReceived/A12345/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="A12345",
            event_type="xact",
            event_subtype="documentsReceived",
            original_timestamp=naive_dt
        )

        assert task.original_timestamp == naive_dt

    def test_special_characters_in_paths(self):
        """Special characters in paths are preserved."""
        task = DownloadTaskMessage(
            trace_id="evt-special-chars",
            media_id="media-special-chars",
            attachment_url="https://storage.example.com/file%20(1).pdf?token=abc123",
            blob_path="documentsReceived/A12345/pdf/file (1).pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="A12345",
            event_type="xact",
            event_subtype="documentsReceived",
            original_timestamp=datetime.now(timezone.utc)
        )

        assert "file%20(1).pdf" in task.attachment_url
        assert "file (1).pdf" in task.blob_path
