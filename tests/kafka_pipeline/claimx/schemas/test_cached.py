"""
Tests for ClaimXCachedDownloadMessage schema.

Validates Pydantic model behavior for cached download messages.
"""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from kafka_pipeline.claimx.schemas.cached import ClaimXCachedDownloadMessage


class TestClaimXCachedDownloadMessageCreation:
    """Test ClaimXCachedDownloadMessage instantiation with valid data."""

    def test_create_with_required_fields(self):
        """ClaimXCachedDownloadMessage can be created with required fields."""
        now = datetime.now(timezone.utc)
        cached = ClaimXCachedDownloadMessage(
            media_id="media_111",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
            destination_path="claimx/proj_456/media/photo.jpg",
            local_cache_path="/tmp/cache/media_111/photo.jpg",
            bytes_downloaded=2048576,
            downloaded_at=now
        )

        assert cached.media_id == "media_111"
        assert cached.project_id == "proj_456"
        assert cached.download_url == "https://s3.amazonaws.com/claimx/presigned/photo.jpg"
        assert cached.destination_path == "claimx/proj_456/media/photo.jpg"
        assert cached.local_cache_path == "/tmp/cache/media_111/photo.jpg"
        assert cached.bytes_downloaded == 2048576
        assert cached.downloaded_at == now
        assert cached.content_type is None
        assert cached.file_type == ""
        assert cached.file_name == ""
        assert cached.source_event_id == ""

    def test_create_with_all_fields(self):
        """ClaimXCachedDownloadMessage can be created with all fields."""
        now = datetime.now(timezone.utc)
        cached = ClaimXCachedDownloadMessage(
            media_id="media_111",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
            destination_path="claimx/proj_456/media/photo.jpg",
            local_cache_path="/tmp/cache/media_111/photo.jpg",
            bytes_downloaded=2048576,
            content_type="image/jpeg",
            file_type="jpg",
            file_name="photo.jpg",
            source_event_id="evt_123",
            downloaded_at=now
        )

        assert cached.content_type == "image/jpeg"
        assert cached.file_type == "jpg"
        assert cached.file_name == "photo.jpg"
        assert cached.source_event_id == "evt_123"

    def test_create_with_zero_bytes(self):
        """ClaimXCachedDownloadMessage can handle zero-byte files."""
        now = datetime.now(timezone.utc)
        cached = ClaimXCachedDownloadMessage(
            media_id="media_empty",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/empty.txt",
            destination_path="claimx/proj_456/media/empty.txt",
            local_cache_path="/tmp/cache/media_empty/empty.txt",
            bytes_downloaded=0,
            downloaded_at=now
        )

        assert cached.bytes_downloaded == 0

    def test_create_with_various_content_types(self):
        """ClaimXCachedDownloadMessage can handle various content types."""
        now = datetime.now(timezone.utc)
        content_types = [
            ("image/jpeg", "jpg"),
            ("image/png", "png"),
            ("video/mp4", "mp4"),
            ("application/pdf", "pdf"),
            ("text/plain", "txt")
        ]

        for content_type, file_type in content_types:
            cached = ClaimXCachedDownloadMessage(
                media_id=f"media_{file_type}",
                project_id="proj_456",
                download_url=f"https://s3.amazonaws.com/claimx/file.{file_type}",
                destination_path=f"claimx/proj_456/media/file.{file_type}",
                local_cache_path=f"/tmp/cache/file.{file_type}",
                bytes_downloaded=1024,
                content_type=content_type,
                file_type=file_type,
                downloaded_at=now
            )

            assert cached.content_type == content_type
            assert cached.file_type == file_type


class TestClaimXCachedDownloadMessageValidation:
    """Test field validation rules for ClaimXCachedDownloadMessage."""

    def test_missing_media_id_raises_error(self):
        """Missing media_id raises ValidationError."""
        now = datetime.now(timezone.utc)
        with pytest.raises(ValidationError) as exc_info:
            ClaimXCachedDownloadMessage(
                project_id="proj_456",
                download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
                destination_path="claimx/proj_456/media/photo.jpg",
                local_cache_path="/tmp/cache/photo.jpg",
                bytes_downloaded=2048576,
                downloaded_at=now
            )

        errors = exc_info.value.errors()
        assert any('media_id' in str(e) for e in errors)

    def test_empty_media_id_raises_error(self):
        """Empty media_id raises ValidationError."""
        now = datetime.now(timezone.utc)
        with pytest.raises(ValidationError) as exc_info:
            ClaimXCachedDownloadMessage(
                media_id="",
                project_id="proj_456",
                download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
                destination_path="claimx/proj_456/media/photo.jpg",
                local_cache_path="/tmp/cache/photo.jpg",
                bytes_downloaded=2048576,
                downloaded_at=now
            )

        errors = exc_info.value.errors()
        assert any('media_id' in str(e) for e in errors)

    def test_negative_bytes_downloaded_raises_error(self):
        """Negative bytes_downloaded raises ValidationError."""
        now = datetime.now(timezone.utc)
        with pytest.raises(ValidationError) as exc_info:
            ClaimXCachedDownloadMessage(
                media_id="media_111",
                project_id="proj_456",
                download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
                destination_path="claimx/proj_456/media/photo.jpg",
                local_cache_path="/tmp/cache/photo.jpg",
                bytes_downloaded=-1,
                downloaded_at=now
            )

        errors = exc_info.value.errors()
        assert any('bytes_downloaded' in str(e) for e in errors)

    def test_empty_download_url_raises_error(self):
        """Empty download_url raises ValidationError."""
        now = datetime.now(timezone.utc)
        with pytest.raises(ValidationError) as exc_info:
            ClaimXCachedDownloadMessage(
                media_id="media_111",
                project_id="proj_456",
                download_url="",
                destination_path="claimx/proj_456/media/photo.jpg",
                local_cache_path="/tmp/cache/photo.jpg",
                bytes_downloaded=2048576,
                downloaded_at=now
            )

        errors = exc_info.value.errors()
        assert any('download_url' in str(e) for e in errors)

    def test_whitespace_is_trimmed(self):
        """Leading/trailing whitespace is trimmed from validated fields."""
        now = datetime.now(timezone.utc)
        cached = ClaimXCachedDownloadMessage(
            media_id="  media_111  ",
            project_id="  proj_456  ",
            download_url="  https://s3.amazonaws.com/claimx/presigned/photo.jpg  ",
            destination_path="  claimx/proj_456/media/photo.jpg  ",
            local_cache_path="  /tmp/cache/photo.jpg  ",
            bytes_downloaded=2048576,
            downloaded_at=now
        )

        assert cached.media_id == "media_111"
        assert cached.project_id == "proj_456"
        assert cached.download_url == "https://s3.amazonaws.com/claimx/presigned/photo.jpg"
        assert cached.destination_path == "claimx/proj_456/media/photo.jpg"
        assert cached.local_cache_path == "/tmp/cache/photo.jpg"


class TestClaimXCachedDownloadMessageSerialization:
    """Test JSON serialization for ClaimXCachedDownloadMessage."""

    def test_serialize_to_json(self):
        """ClaimXCachedDownloadMessage serializes to JSON correctly."""
        now = datetime.now(timezone.utc)
        cached = ClaimXCachedDownloadMessage(
            media_id="media_111",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
            destination_path="claimx/proj_456/media/photo.jpg",
            local_cache_path="/tmp/cache/photo.jpg",
            bytes_downloaded=2048576,
            content_type="image/jpeg",
            file_type="jpg",
            file_name="photo.jpg",
            downloaded_at=now
        )

        json_str = cached.model_dump_json()
        assert "media_111" in json_str
        assert "proj_456" in json_str
        assert "photo.jpg" in json_str
        assert "image/jpeg" in json_str

    def test_downloaded_at_serializes_as_iso(self):
        """downloaded_at field serializes as ISO 8601 string."""
        now = datetime.now(timezone.utc)
        cached = ClaimXCachedDownloadMessage(
            media_id="media_111",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
            destination_path="claimx/proj_456/media/photo.jpg",
            local_cache_path="/tmp/cache/photo.jpg",
            bytes_downloaded=2048576,
            downloaded_at=now
        )

        json_str = cached.model_dump_json()
        assert now.isoformat() in json_str or now.isoformat().replace('+00:00', 'Z') in json_str

    def test_deserialize_from_json(self):
        """ClaimXCachedDownloadMessage deserializes from JSON correctly."""
        now = datetime.now(timezone.utc)
        json_data = {
            "media_id": "media_111",
            "project_id": "proj_456",
            "download_url": "https://s3.amazonaws.com/claimx/presigned/photo.jpg",
            "destination_path": "claimx/proj_456/media/photo.jpg",
            "local_cache_path": "/tmp/cache/photo.jpg",
            "bytes_downloaded": 2048576,
            "content_type": "image/jpeg",
            "file_type": "jpg",
            "downloaded_at": now.isoformat()
        }

        cached = ClaimXCachedDownloadMessage.model_validate(json_data)
        assert cached.media_id == "media_111"
        assert cached.bytes_downloaded == 2048576
        assert cached.content_type == "image/jpeg"

    def test_round_trip_serialization(self):
        """ClaimXCachedDownloadMessage round-trips through JSON correctly."""
        now = datetime.now(timezone.utc)
        original = ClaimXCachedDownloadMessage(
            media_id="media_111",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
            destination_path="claimx/proj_456/media/photo.jpg",
            local_cache_path="/tmp/cache/photo.jpg",
            bytes_downloaded=2048576,
            content_type="image/jpeg",
            file_type="jpg",
            file_name="photo.jpg",
            source_event_id="evt_123",
            downloaded_at=now
        )

        json_str = original.model_dump_json()
        restored = ClaimXCachedDownloadMessage.model_validate_json(json_str)

        assert restored.media_id == original.media_id
        assert restored.project_id == original.project_id
        assert restored.download_url == original.download_url
        assert restored.destination_path == original.destination_path
        assert restored.local_cache_path == original.local_cache_path
        assert restored.bytes_downloaded == original.bytes_downloaded
        assert restored.content_type == original.content_type
        assert restored.file_type == original.file_type
        assert restored.file_name == original.file_name
        assert restored.source_event_id == original.source_event_id


class TestClaimXCachedDownloadMessageEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_large_file_size(self):
        """ClaimXCachedDownloadMessage handles large file sizes."""
        now = datetime.now(timezone.utc)
        large_size = 10 * 1024 * 1024 * 1024  # 10 GB
        cached = ClaimXCachedDownloadMessage(
            media_id="media_large",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/large_video.mp4",
            destination_path="claimx/proj_456/media/large_video.mp4",
            local_cache_path="/tmp/cache/large_video.mp4",
            bytes_downloaded=large_size,
            file_type="mp4",
            downloaded_at=now
        )

        assert cached.bytes_downloaded == large_size

    def test_unicode_file_names(self):
        """ClaimXCachedDownloadMessage handles Unicode file names."""
        now = datetime.now(timezone.utc)
        cached = ClaimXCachedDownloadMessage(
            media_id="media_unicode",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/файл.jpg",
            destination_path="claimx/proj_456/media/файл.jpg",
            local_cache_path="/tmp/cache/файл.jpg",
            bytes_downloaded=1024,
            file_name="файл.jpg",
            downloaded_at=now
        )

        assert cached.file_name == "файл.jpg"

    def test_long_paths(self):
        """ClaimXCachedDownloadMessage handles long file paths."""
        now = datetime.now(timezone.utc)
        long_path = "claimx/" + "/".join(["very"] * 50) + "/long/path/to/file.jpg"
        cached = ClaimXCachedDownloadMessage(
            media_id="media_long_path",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/file.jpg",
            destination_path=long_path,
            local_cache_path="/tmp/cache/file.jpg",
            bytes_downloaded=1024,
            downloaded_at=now
        )

        assert cached.destination_path == long_path
