"""
Tests for ClaimXUploadResultMessage schema.

Validates Pydantic model behavior for upload result messages.
"""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from kafka_pipeline.claimx.schemas.results import ClaimXUploadResultMessage


class TestClaimXUploadResultMessageCreation:
    """Test ClaimXUploadResultMessage instantiation with valid data."""

    def test_create_successful_upload(self):
        """ClaimXUploadResultMessage for successful upload."""
        now = datetime.now(timezone.utc)
        result = ClaimXUploadResultMessage(
            media_id="media_111",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
            blob_path="claimx/proj_456/media/photo.jpg",
            status="completed",
            bytes_uploaded=2048576,
            created_at=now
        )

        assert result.media_id == "media_111"
        assert result.project_id == "proj_456"
        assert result.status == "completed"
        assert result.bytes_uploaded == 2048576
        assert result.error_message is None
        assert result.file_type == ""
        assert result.file_name == ""
        assert result.source_event_id == ""

    def test_create_failed_upload(self):
        """ClaimXUploadResultMessage for failed upload."""
        now = datetime.now(timezone.utc)
        result = ClaimXUploadResultMessage(
            media_id="media_222",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
            blob_path="claimx/proj_456/media/photo.jpg",
            status="failed",
            bytes_uploaded=0,
            error_message="Connection timeout",
            created_at=now
        )

        assert result.status == "failed"
        assert result.bytes_uploaded == 0
        assert result.error_message == "Connection timeout"

    def test_create_permanent_failure(self):
        """ClaimXUploadResultMessage for permanent failure."""
        now = datetime.now(timezone.utc)
        result = ClaimXUploadResultMessage(
            media_id="media_333",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
            blob_path="claimx/proj_456/media/photo.jpg",
            status="failed_permanent",
            bytes_uploaded=0,
            error_message="File not found in cache",
            created_at=now
        )

        assert result.status == "failed_permanent"
        assert result.error_message == "File not found in cache"

    def test_create_with_all_fields(self):
        """ClaimXUploadResultMessage with all optional fields."""
        now = datetime.now(timezone.utc)
        result = ClaimXUploadResultMessage(
            media_id="media_full",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
            blob_path="claimx/proj_456/media/photo.jpg",
            file_type="jpg",
            file_name="photo.jpg",
            source_event_id="evt_123",
            status="completed",
            bytes_uploaded=2048576,
            created_at=now
        )

        assert result.file_type == "jpg"
        assert result.file_name == "photo.jpg"
        assert result.source_event_id == "evt_123"


class TestClaimXUploadResultMessageValidation:
    """Test field validation rules for ClaimXUploadResultMessage."""

    def test_missing_media_id_raises_error(self):
        """Missing media_id raises ValidationError."""
        now = datetime.now(timezone.utc)
        with pytest.raises(ValidationError) as exc_info:
            ClaimXUploadResultMessage(
                project_id="proj_456",
                download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
                blob_path="claimx/proj_456/media/photo.jpg",
                status="completed",
                created_at=now
            )

        errors = exc_info.value.errors()
        assert any('media_id' in str(e) for e in errors)

    def test_empty_media_id_raises_error(self):
        """Empty media_id raises ValidationError."""
        now = datetime.now(timezone.utc)
        with pytest.raises(ValidationError) as exc_info:
            ClaimXUploadResultMessage(
                media_id="",
                project_id="proj_456",
                download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
                blob_path="claimx/proj_456/media/photo.jpg",
                status="completed",
                created_at=now
            )

        errors = exc_info.value.errors()
        assert any('media_id' in str(e) for e in errors)

    def test_invalid_status_raises_error(self):
        """Invalid status value raises ValidationError."""
        now = datetime.now(timezone.utc)
        with pytest.raises(ValidationError) as exc_info:
            ClaimXUploadResultMessage(
                media_id="media_111",
                project_id="proj_456",
                download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
                blob_path="claimx/proj_456/media/photo.jpg",
                status="unknown_status",
                created_at=now
            )

        errors = exc_info.value.errors()
        assert any('status' in str(e) for e in errors)

    def test_negative_bytes_uploaded_raises_error(self):
        """Negative bytes_uploaded raises ValidationError."""
        now = datetime.now(timezone.utc)
        with pytest.raises(ValidationError) as exc_info:
            ClaimXUploadResultMessage(
                media_id="media_111",
                project_id="proj_456",
                download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
                blob_path="claimx/proj_456/media/photo.jpg",
                status="completed",
                bytes_uploaded=-1,
                created_at=now
            )

        errors = exc_info.value.errors()
        assert any('bytes_uploaded' in str(e) for e in errors)

    def test_whitespace_is_trimmed(self):
        """Leading/trailing whitespace is trimmed from validated fields."""
        now = datetime.now(timezone.utc)
        result = ClaimXUploadResultMessage(
            media_id="  media_111  ",
            project_id="  proj_456  ",
            download_url="  https://s3.amazonaws.com/claimx/presigned/photo.jpg  ",
            blob_path="  claimx/proj_456/media/photo.jpg  ",
            status="completed",
            created_at=now
        )

        assert result.media_id == "media_111"
        assert result.project_id == "proj_456"
        assert result.download_url == "https://s3.amazonaws.com/claimx/presigned/photo.jpg"
        assert result.blob_path == "claimx/proj_456/media/photo.jpg"

    def test_all_status_values_valid(self):
        """All three status values are valid."""
        now = datetime.now(timezone.utc)
        statuses = ["completed", "failed", "failed_permanent"]

        for status in statuses:
            result = ClaimXUploadResultMessage(
                media_id="media_111",
                project_id="proj_456",
                download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
                blob_path="claimx/proj_456/media/photo.jpg",
                status=status,
                created_at=now
            )
            assert result.status == status


class TestClaimXUploadResultMessageSerialization:
    """Test JSON serialization for ClaimXUploadResultMessage."""

    def test_serialize_to_json(self):
        """ClaimXUploadResultMessage serializes to JSON correctly."""
        now = datetime.now(timezone.utc)
        result = ClaimXUploadResultMessage(
            media_id="media_111",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
            blob_path="claimx/proj_456/media/photo.jpg",
            file_type="jpg",
            file_name="photo.jpg",
            status="completed",
            bytes_uploaded=2048576,
            created_at=now
        )

        json_str = result.model_dump_json()
        assert "media_111" in json_str
        assert "proj_456" in json_str
        assert "completed" in json_str
        assert "photo.jpg" in json_str

    def test_created_at_serializes_as_iso(self):
        """created_at field serializes as ISO 8601 string."""
        now = datetime.now(timezone.utc)
        result = ClaimXUploadResultMessage(
            media_id="media_111",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
            blob_path="claimx/proj_456/media/photo.jpg",
            status="completed",
            created_at=now
        )

        json_str = result.model_dump_json()
        assert now.isoformat() in json_str or now.isoformat().replace('+00:00', 'Z') in json_str

    def test_deserialize_from_json(self):
        """ClaimXUploadResultMessage deserializes from JSON correctly."""
        now = datetime.now(timezone.utc)
        json_data = {
            "media_id": "media_111",
            "project_id": "proj_456",
            "download_url": "https://s3.amazonaws.com/claimx/presigned/photo.jpg",
            "blob_path": "claimx/proj_456/media/photo.jpg",
            "status": "completed",
            "bytes_uploaded": 2048576,
            "created_at": now.isoformat()
        }

        result = ClaimXUploadResultMessage.model_validate(json_data)
        assert result.media_id == "media_111"
        assert result.status == "completed"
        assert result.bytes_uploaded == 2048576

    def test_round_trip_serialization(self):
        """ClaimXUploadResultMessage round-trips through JSON correctly."""
        now = datetime.now(timezone.utc)
        original = ClaimXUploadResultMessage(
            media_id="media_111",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
            blob_path="claimx/proj_456/media/photo.jpg",
            file_type="jpg",
            file_name="photo.jpg",
            source_event_id="evt_123",
            status="failed",
            bytes_uploaded=0,
            error_message="Connection timeout",
            created_at=now
        )

        json_str = original.model_dump_json()
        restored = ClaimXUploadResultMessage.model_validate_json(json_str)

        assert restored.media_id == original.media_id
        assert restored.project_id == original.project_id
        assert restored.status == original.status
        assert restored.bytes_uploaded == original.bytes_uploaded
        assert restored.error_message == original.error_message
        assert restored.file_type == original.file_type
        assert restored.file_name == original.file_name


class TestClaimXUploadResultMessageEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_zero_bytes_uploaded(self):
        """ClaimXUploadResultMessage handles zero bytes (empty file)."""
        now = datetime.now(timezone.utc)
        result = ClaimXUploadResultMessage(
            media_id="media_empty",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/empty.txt",
            blob_path="claimx/proj_456/media/empty.txt",
            status="completed",
            bytes_uploaded=0,
            created_at=now
        )

        assert result.bytes_uploaded == 0
        assert result.status == "completed"

    def test_long_error_message(self):
        """ClaimXUploadResultMessage handles long error messages."""
        now = datetime.now(timezone.utc)
        long_error = "Error: " + "x" * 1000
        result = ClaimXUploadResultMessage(
            media_id="media_error",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
            blob_path="claimx/proj_456/media/photo.jpg",
            status="failed_permanent",
            bytes_uploaded=0,
            error_message=long_error,
            created_at=now
        )

        assert result.error_message == long_error

    def test_large_file_upload(self):
        """ClaimXUploadResultMessage handles large file sizes."""
        now = datetime.now(timezone.utc)
        large_size = 50 * 1024 * 1024 * 1024  # 50 GB
        result = ClaimXUploadResultMessage(
            media_id="media_large",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/large_video.mp4",
            blob_path="claimx/proj_456/media/large_video.mp4",
            status="completed",
            bytes_uploaded=large_size,
            created_at=now
        )

        assert result.bytes_uploaded == large_size

    def test_unicode_error_messages(self):
        """ClaimXUploadResultMessage handles Unicode in error messages."""
        now = datetime.now(timezone.utc)
        unicode_error = "Ошибка загрузки файла: 文件上传失败"
        result = ClaimXUploadResultMessage(
            media_id="media_unicode_error",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
            blob_path="claimx/proj_456/media/photo.jpg",
            status="failed",
            bytes_uploaded=0,
            error_message=unicode_error,
            created_at=now
        )

        assert result.error_message == unicode_error


class TestClaimXUploadResultMessageBusinessLogic:
    """Test business logic scenarios."""

    def test_completed_status_typically_has_bytes(self):
        """Completed uploads typically have non-zero bytes."""
        now = datetime.now(timezone.utc)
        result = ClaimXUploadResultMessage(
            media_id="media_111",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
            blob_path="claimx/proj_456/media/photo.jpg",
            status="completed",
            bytes_uploaded=2048576,
            created_at=now
        )

        assert result.status == "completed"
        assert result.bytes_uploaded > 0
        assert result.error_message is None

    def test_failed_status_typically_has_error_message(self):
        """Failed uploads typically have error messages."""
        now = datetime.now(timezone.utc)
        result = ClaimXUploadResultMessage(
            media_id="media_111",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
            blob_path="claimx/proj_456/media/photo.jpg",
            status="failed",
            bytes_uploaded=0,
            error_message="Transient network error",
            created_at=now
        )

        assert result.status == "failed"
        assert result.error_message is not None
        assert result.bytes_uploaded == 0

    def test_permanent_failure_has_descriptive_error(self):
        """Permanent failures have descriptive error messages."""
        now = datetime.now(timezone.utc)
        result = ClaimXUploadResultMessage(
            media_id="media_111",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
            blob_path="claimx/proj_456/media/photo.jpg",
            status="failed_permanent",
            bytes_uploaded=0,
            error_message="File not found in local cache - cache was cleared",
            created_at=now
        )

        assert result.status == "failed_permanent"
        assert result.error_message is not None
        assert "cache" in result.error_message.lower()
