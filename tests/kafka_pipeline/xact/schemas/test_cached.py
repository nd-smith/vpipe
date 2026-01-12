"""
Tests for CachedDownloadMessage schema.

Validates Pydantic model behavior, JSON serialization, field validation,
and edge cases for the cached download message schema.

Created for WP-314: Upload Worker - Unit Tests.
"""

import json
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from kafka_pipeline.xact.schemas.cached import CachedDownloadMessage


class TestCachedDownloadMessageCreation:
    """Test CachedDownloadMessage instantiation with valid data."""

    def test_create_basic_message(self):
        """CachedDownloadMessage can be created with required fields."""
        cached = CachedDownloadMessage(
            trace_id="evt-123",
            media_id="media-123",
            attachment_url="https://storage.example.com/file.pdf",
            destination_path="claims/C-456/file.pdf",
            local_cache_path="/tmp/kafka_pipeline_cache/evt-123/file.pdf",
            bytes_downloaded=2048576,
            event_type="claim",
            event_subtype="created",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-456",
            original_timestamp=datetime(2024, 12, 25, 10, 30, 0, tzinfo=timezone.utc),
            downloaded_at=datetime(2024, 12, 25, 10, 30, 5, tzinfo=timezone.utc),
        )

        assert cached.trace_id == "evt-123"
        assert cached.attachment_url == "https://storage.example.com/file.pdf"
        assert cached.destination_path == "claims/C-456/file.pdf"
        assert cached.local_cache_path == "/tmp/kafka_pipeline_cache/evt-123/file.pdf"
        assert cached.bytes_downloaded == 2048576
        assert cached.event_type == "claim"
        assert cached.event_subtype == "created"

    def test_create_message_with_all_fields(self):
        """CachedDownloadMessage can be created with all optional fields."""
        cached = CachedDownloadMessage(
            trace_id="evt-456",
            media_id="media-456",
            attachment_url="https://claimxperience.com/files/document.pdf",
            destination_path="claims/C-789/document.pdf",
            local_cache_path="/tmp/kafka_pipeline_cache/evt-456/document.pdf",
            bytes_downloaded=4096,
            content_type="application/pdf",
            event_type="claim",
            event_subtype="documentsReceived",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="A-789",
            original_timestamp=datetime(2024, 12, 25, 10, 30, 0, tzinfo=timezone.utc),
            downloaded_at=datetime(2024, 12, 25, 10, 30, 5, tzinfo=timezone.utc),
            metadata={
                "source_partition": 3,
                "retry_count": 0
            }
        )

        assert cached.content_type == "application/pdf"
        assert cached.metadata["source_partition"] == 3
        assert cached.assignment_id == "A-789"

    def test_create_message_without_optional_fields(self):
        """CachedDownloadMessage can be created without optional fields."""
        cached = CachedDownloadMessage(
            trace_id="evt-789",
            media_id="media-789",
            attachment_url="https://storage.example.com/image.jpg",
            destination_path="policies/P-123/image.jpg",
            local_cache_path="/tmp/kafka_pipeline_cache/evt-789/image.jpg",
            bytes_downloaded=1024,
            event_type="policy",
            event_subtype="updated",
            status_subtype="documentsReceived",
            file_type="jpg",
            assignment_id="P-123",
            original_timestamp=datetime.now(timezone.utc),
            downloaded_at=datetime.now(timezone.utc),
        )

        assert cached.content_type is None
        assert cached.metadata == {}


class TestCachedDownloadMessageValidation:
    """Test field validation rules for CachedDownloadMessage."""

    def test_missing_required_fields_raises_error(self):
        """Missing required fields raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            CachedDownloadMessage(
                trace_id="evt-123",
                attachment_url="https://storage.example.com/file.pdf"
                # Missing destination_path, local_cache_path, bytes_downloaded,
                # event_type, event_subtype, original_timestamp, downloaded_at
            )

        errors = exc_info.value.errors()
        assert any(e['loc'] == ('destination_path',) for e in errors)
        assert any(e['loc'] == ('local_cache_path',) for e in errors)
        assert any(e['loc'] == ('bytes_downloaded',) for e in errors)

    def test_empty_trace_id_raises_error(self):
        """Empty trace_id raises ValidationError."""
        with pytest.raises(ValidationError):
            CachedDownloadMessage(
                trace_id="",
                attachment_url="https://storage.example.com/file.pdf",
                destination_path="claims/C-456/file.pdf",
                local_cache_path="/tmp/kafka_pipeline_cache/evt-123/file.pdf",
                bytes_downloaded=1024,
                event_type="claim",
                event_subtype="created",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id="C-456",
                original_timestamp=datetime.now(timezone.utc),
                downloaded_at=datetime.now(timezone.utc),
            )

    def test_whitespace_trace_id_raises_error(self):
        """Whitespace-only trace_id raises ValidationError."""
        with pytest.raises(ValidationError):
            CachedDownloadMessage(
                trace_id="   ",
                attachment_url="https://storage.example.com/file.pdf",
                destination_path="claims/C-456/file.pdf",
                local_cache_path="/tmp/kafka_pipeline_cache/evt-123/file.pdf",
                bytes_downloaded=1024,
                event_type="claim",
                event_subtype="created",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id="C-456",
                original_timestamp=datetime.now(timezone.utc),
                downloaded_at=datetime.now(timezone.utc),
            )

    def test_empty_attachment_url_raises_error(self):
        """Empty attachment_url raises ValidationError."""
        with pytest.raises(ValidationError):
            CachedDownloadMessage(
                trace_id="evt-123",
                attachment_url="",
                destination_path="claims/C-456/file.pdf",
                local_cache_path="/tmp/kafka_pipeline_cache/evt-123/file.pdf",
                bytes_downloaded=1024,
                event_type="claim",
                event_subtype="created",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id="C-456",
                original_timestamp=datetime.now(timezone.utc),
                downloaded_at=datetime.now(timezone.utc),
            )

    def test_empty_destination_path_raises_error(self):
        """Empty destination_path raises ValidationError."""
        with pytest.raises(ValidationError):
            CachedDownloadMessage(
                trace_id="evt-123",
                attachment_url="https://storage.example.com/file.pdf",
                destination_path="",
                local_cache_path="/tmp/kafka_pipeline_cache/evt-123/file.pdf",
                bytes_downloaded=1024,
                event_type="claim",
                event_subtype="created",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id="C-456",
                original_timestamp=datetime.now(timezone.utc),
                downloaded_at=datetime.now(timezone.utc),
            )

    def test_empty_local_cache_path_raises_error(self):
        """Empty local_cache_path raises ValidationError."""
        with pytest.raises(ValidationError):
            CachedDownloadMessage(
                trace_id="evt-123",
                attachment_url="https://storage.example.com/file.pdf",
                destination_path="claims/C-456/file.pdf",
                local_cache_path="",
                bytes_downloaded=1024,
                event_type="claim",
                event_subtype="created",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id="C-456",
                original_timestamp=datetime.now(timezone.utc),
                downloaded_at=datetime.now(timezone.utc),
            )

    def test_empty_event_type_raises_error(self):
        """Empty event_type raises ValidationError."""
        with pytest.raises(ValidationError):
            CachedDownloadMessage(
                trace_id="evt-123",
                attachment_url="https://storage.example.com/file.pdf",
                destination_path="claims/C-456/file.pdf",
                local_cache_path="/tmp/kafka_pipeline_cache/evt-123/file.pdf",
                bytes_downloaded=1024,
                event_type="",
                event_subtype="created",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id="C-456",
                original_timestamp=datetime.now(timezone.utc),
                downloaded_at=datetime.now(timezone.utc),
            )

    def test_empty_event_subtype_raises_error(self):
        """Empty event_subtype raises ValidationError."""
        with pytest.raises(ValidationError):
            CachedDownloadMessage(
                trace_id="evt-123",
                attachment_url="https://storage.example.com/file.pdf",
                destination_path="claims/C-456/file.pdf",
                local_cache_path="/tmp/kafka_pipeline_cache/evt-123/file.pdf",
                bytes_downloaded=1024,
                event_type="claim",
                event_subtype="",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id="C-456",
                original_timestamp=datetime.now(timezone.utc),
                downloaded_at=datetime.now(timezone.utc),
            )

    def test_negative_bytes_downloaded_raises_error(self):
        """Negative bytes_downloaded raises ValidationError."""
        with pytest.raises(ValidationError):
            CachedDownloadMessage(
                trace_id="evt-123",
                attachment_url="https://storage.example.com/file.pdf",
                destination_path="claims/C-456/file.pdf",
                local_cache_path="/tmp/kafka_pipeline_cache/evt-123/file.pdf",
                bytes_downloaded=-100,
                event_type="claim",
                event_subtype="created",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id="C-456",
                original_timestamp=datetime.now(timezone.utc),
                downloaded_at=datetime.now(timezone.utc),
            )

    def test_whitespace_is_trimmed(self):
        """Leading/trailing whitespace is trimmed from string fields."""
        cached = CachedDownloadMessage(
            trace_id="  evt-123  ",
            media_id="  media-123  ",
            attachment_url="  https://storage.example.com/file.pdf  ",
            destination_path="  claims/C-456/file.pdf  ",
            local_cache_path="  /tmp/kafka_pipeline_cache/evt-123/file.pdf  ",
            bytes_downloaded=1024,
            event_type="  claim  ",
            event_subtype="  created  ",
            status_subtype="  documentsReceived  ",
            file_type="  pdf  ",
            assignment_id="  C-456  ",
            original_timestamp=datetime.now(timezone.utc),
            downloaded_at=datetime.now(timezone.utc),
        )

        assert cached.trace_id == "evt-123"
        assert cached.attachment_url == "https://storage.example.com/file.pdf"
        assert cached.destination_path == "claims/C-456/file.pdf"
        assert cached.local_cache_path == "/tmp/kafka_pipeline_cache/evt-123/file.pdf"
        assert cached.event_type == "claim"
        assert cached.event_subtype == "created"
        assert cached.status_subtype == "documentsReceived"
        assert cached.file_type == "pdf"
        assert cached.assignment_id == "C-456"


class TestCachedDownloadMessageSerialization:
    """Test JSON serialization and deserialization."""

    def test_serialize_to_json(self):
        """CachedDownloadMessage serializes to valid JSON."""
        cached = CachedDownloadMessage(
            trace_id="evt-123",
            media_id="media-123",
            attachment_url="https://storage.example.com/file.pdf",
            destination_path="claims/C-456/file.pdf",
            local_cache_path="/tmp/kafka_pipeline_cache/evt-123/file.pdf",
            bytes_downloaded=2048576,
            content_type="application/pdf",
            event_type="claim",
            event_subtype="created",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-456",
            original_timestamp=datetime(2024, 12, 25, 10, 30, 0, tzinfo=timezone.utc),
            downloaded_at=datetime(2024, 12, 25, 10, 30, 5, tzinfo=timezone.utc),
            metadata={"source_partition": 3}
        )

        json_str = cached.model_dump_json()
        parsed = json.loads(json_str)

        assert parsed["trace_id"] == "evt-123"
        assert parsed["attachment_url"] == "https://storage.example.com/file.pdf"
        assert parsed["destination_path"] == "claims/C-456/file.pdf"
        assert parsed["local_cache_path"] == "/tmp/kafka_pipeline_cache/evt-123/file.pdf"
        assert parsed["bytes_downloaded"] == 2048576
        assert parsed["content_type"] == "application/pdf"
        assert parsed["event_type"] == "claim"
        assert parsed["event_subtype"] == "created"
        assert parsed["original_timestamp"] == "2024-12-25T10:30:00+00:00"
        assert parsed["downloaded_at"] == "2024-12-25T10:30:05+00:00"
        assert parsed["metadata"]["source_partition"] == 3

    def test_serialize_without_optional_fields(self):
        """CachedDownloadMessage serializes correctly without optional fields."""
        cached = CachedDownloadMessage(
            trace_id="evt-456",
            media_id="media-456",
            attachment_url="https://storage.example.com/image.jpg",
            destination_path="policies/P-123/image.jpg",
            local_cache_path="/tmp/kafka_pipeline_cache/evt-456/image.jpg",
            bytes_downloaded=1024,
            event_type="policy",
            event_subtype="updated",
            status_subtype="documentsReceived",
            file_type="jpg",
            assignment_id="P-123",
            original_timestamp=datetime(2024, 12, 25, 11, 0, 0, tzinfo=timezone.utc),
            downloaded_at=datetime(2024, 12, 25, 11, 0, 2, tzinfo=timezone.utc),
        )

        json_str = cached.model_dump_json()
        parsed = json.loads(json_str)

        assert parsed["content_type"] is None
        assert parsed["metadata"] == {}

    def test_deserialize_from_json(self):
        """CachedDownloadMessage can be created from JSON."""
        json_data = {
            "trace_id": "evt-789",
            "media_id": "media-789",
            "attachment_url": "https://storage.example.com/doc.pdf",
            "destination_path": "claims/C-999/doc.pdf",
            "local_cache_path": "/tmp/kafka_pipeline_cache/evt-789/doc.pdf",
            "bytes_downloaded": 4096,
            "content_type": "application/pdf",
            "event_type": "claim",
            "event_subtype": "documentsReceived",
            "status_subtype": "documentsReceived",
            "file_type": "pdf",
            "assignment_id": "C-999",
            "original_timestamp": "2024-12-25T15:45:00Z",
            "downloaded_at": "2024-12-25T15:45:03Z",
            "metadata": {"retry_count": 0}
        }

        json_str = json.dumps(json_data)
        cached = CachedDownloadMessage.model_validate_json(json_str)

        assert cached.trace_id == "evt-789"
        assert cached.bytes_downloaded == 4096
        assert cached.content_type == "application/pdf"
        assert cached.metadata["retry_count"] == 0

    def test_deserialize_without_optional_fields(self):
        """CachedDownloadMessage can be created from JSON without optional fields."""
        json_data = {
            "trace_id": "evt-minimal",
            "media_id": "media-minimal",
            "attachment_url": "https://storage.example.com/file.txt",
            "destination_path": "data/file.txt",
            "local_cache_path": "/tmp/cache/file.txt",
            "bytes_downloaded": 100,
            "event_type": "data",
            "event_subtype": "upload",
            "status_subtype": "documentsReceived",
            "file_type": "txt",
            "assignment_id": "D-123",
            "original_timestamp": "2024-12-25T12:00:00Z",
            "downloaded_at": "2024-12-25T12:00:01Z"
        }

        json_str = json.dumps(json_data)
        cached = CachedDownloadMessage.model_validate_json(json_str)

        assert cached.trace_id == "evt-minimal"
        assert cached.content_type is None
        assert cached.metadata == {}

    def test_round_trip_serialization(self):
        """Data survives JSON serialization round-trip."""
        original = CachedDownloadMessage(
            trace_id="evt-round-trip",
            media_id="media-round-trip",
            attachment_url="https://storage.example.com/round.pdf",
            destination_path="test/round.pdf",
            local_cache_path="/tmp/cache/round.pdf",
            bytes_downloaded=8192,
            content_type="application/pdf",
            event_type="test",
            event_subtype="roundtrip",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="T-123",
            original_timestamp=datetime(2024, 12, 25, 10, 30, 0, tzinfo=timezone.utc),
            downloaded_at=datetime(2024, 12, 25, 10, 30, 5, tzinfo=timezone.utc),
            metadata={"key": "value", "number": 42}
        )

        json_str = original.model_dump_json()
        restored = CachedDownloadMessage.model_validate_json(json_str)

        assert restored.trace_id == original.trace_id
        assert restored.attachment_url == original.attachment_url
        assert restored.destination_path == original.destination_path
        assert restored.local_cache_path == original.local_cache_path
        assert restored.bytes_downloaded == original.bytes_downloaded
        assert restored.content_type == original.content_type
        assert restored.event_type == original.event_type
        assert restored.event_subtype == original.event_subtype
        assert restored.metadata == original.metadata


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_zero_bytes_downloaded_is_valid(self):
        """Zero bytes downloaded is a valid value (empty file)."""
        cached = CachedDownloadMessage(
            trace_id="evt-empty",
            media_id="media-empty",
            attachment_url="https://storage.example.com/empty.txt",
            destination_path="test/empty.txt",
            local_cache_path="/tmp/cache/empty.txt",
            bytes_downloaded=0,
            event_type="test",
            event_subtype="empty",
            status_subtype="documentsReceived",
            file_type="txt",
            assignment_id="T-001",
            original_timestamp=datetime.now(timezone.utc),
            downloaded_at=datetime.now(timezone.utc),
        )
        assert cached.bytes_downloaded == 0

    def test_very_large_bytes_downloaded(self):
        """Very large bytes_downloaded values are accepted."""
        cached = CachedDownloadMessage(
            trace_id="evt-large",
            media_id="media-large",
            attachment_url="https://storage.example.com/huge.zip",
            destination_path="archives/huge.zip",
            local_cache_path="/tmp/cache/huge.zip",
            bytes_downloaded=10_737_418_240,  # 10 GB
            event_type="archive",
            event_subtype="backup",
            status_subtype="documentsReceived",
            file_type="zip",
            assignment_id="A-002",
            original_timestamp=datetime.now(timezone.utc),
            downloaded_at=datetime.now(timezone.utc),
        )
        assert cached.bytes_downloaded == 10_737_418_240

    def test_unicode_in_paths(self):
        """Paths can contain Unicode characters."""
        cached = CachedDownloadMessage(
            trace_id="evt-unicode",
            media_id="media-unicode",
            attachment_url="https://storage.example.com/Schäden/document.pdf",
            destination_path="claims/Schädenübersicht/document.pdf",
            local_cache_path="/tmp/cache/Schäden/document.pdf",
            bytes_downloaded=1024,
            event_type="claim",
            event_subtype="created",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-003",
            original_timestamp=datetime.now(timezone.utc),
            downloaded_at=datetime.now(timezone.utc),
        )
        assert "Schäden" in cached.destination_path

    def test_special_characters_in_trace_id(self):
        """Trace ID can contain special characters."""
        cached = CachedDownloadMessage(
            trace_id="evt-2024-001-abc_xyz",
            media_id="media-special",
            attachment_url="https://storage.example.com/file.pdf",
            destination_path="test/file.pdf",
            local_cache_path="/tmp/cache/file.pdf",
            bytes_downloaded=1024,
            event_type="test",
            event_subtype="special",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="T-004",
            original_timestamp=datetime.now(timezone.utc),
            downloaded_at=datetime.now(timezone.utc),
        )
        assert cached.trace_id == "evt-2024-001-abc_xyz"

    def test_naive_datetime_is_accepted(self):
        """Naive datetime (without timezone) is accepted."""
        naive_original = datetime(2024, 12, 25, 10, 30, 0)
        naive_downloaded = datetime(2024, 12, 25, 10, 30, 5)

        cached = CachedDownloadMessage(
            trace_id="evt-naive",
            media_id="media-naive",
            attachment_url="https://storage.example.com/file.pdf",
            destination_path="test/file.pdf",
            local_cache_path="/tmp/cache/file.pdf",
            bytes_downloaded=1024,
            event_type="test",
            event_subtype="naive",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="T-005",
            original_timestamp=naive_original,
            downloaded_at=naive_downloaded,
        )
        assert cached.original_timestamp == naive_original
        assert cached.downloaded_at == naive_downloaded

    def test_complex_metadata_structure(self):
        """Metadata can contain nested structures."""
        cached = CachedDownloadMessage(
            trace_id="evt-complex",
            media_id="media-complex",
            attachment_url="https://storage.example.com/file.pdf",
            destination_path="test/file.pdf",
            local_cache_path="/tmp/cache/file.pdf",
            bytes_downloaded=1024,
            event_type="test",
            event_subtype="complex",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="T-006",
            original_timestamp=datetime.now(timezone.utc),
            downloaded_at=datetime.now(timezone.utc),
            metadata={
                "source": {
                    "partition": 3,
                    "offset": 12345
                },
                "tags": ["important", "urgent"],
                "properties": {
                    "retries": 0,
                    "priority": "high"
                }
            }
        )

        assert cached.metadata["source"]["partition"] == 3
        assert "important" in cached.metadata["tags"]
        assert cached.metadata["properties"]["priority"] == "high"

    def test_various_content_types(self):
        """Various content types are accepted."""
        content_types = [
            "application/pdf",
            "image/jpeg",
            "image/png",
            "application/octet-stream",
            "text/plain; charset=utf-8",
        ]

        for ct in content_types:
            cached = CachedDownloadMessage(
                trace_id=f"evt-ct-{ct.replace('/', '-')}",
                media_id=f"media-ct-{ct.replace('/', '-')}",
                attachment_url="https://storage.example.com/file",
                destination_path="test/file",
                local_cache_path="/tmp/cache/file",
                bytes_downloaded=1024,
                content_type=ct,
                event_type="test",
                event_subtype="content_type",
                status_subtype="documentsReceived",
                file_type="bin",
                assignment_id="T-007",
                original_timestamp=datetime.now(timezone.utc),
                downloaded_at=datetime.now(timezone.utc),
            )
            assert cached.content_type == ct

    def test_long_paths_are_valid(self):
        """Long file paths are accepted."""
        long_dest = "/".join(["folder"] * 20 + ["file.pdf"])
        long_cache = "/tmp/" + "/".join(["dir"] * 20 + ["cached.pdf"])

        cached = CachedDownloadMessage(
            trace_id="evt-long-path",
            media_id="media-long-path",
            attachment_url="https://storage.example.com/" + long_dest,
            destination_path=long_dest,
            local_cache_path=long_cache,
            bytes_downloaded=1024,
            event_type="test",
            event_subtype="longpath",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="T-008",
            original_timestamp=datetime.now(timezone.utc),
            downloaded_at=datetime.now(timezone.utc),
        )
        assert len(cached.destination_path) > 100
