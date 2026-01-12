"""
Tests for ClaimX task message schemas.

Validates Pydantic model behavior for ClaimXEnrichmentTask and ClaimXDownloadTask.
"""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from kafka_pipeline.claimx.schemas.tasks import (
    ClaimXEnrichmentTask,
    ClaimXDownloadTask,
)


class TestClaimXEnrichmentTaskCreation:
    """Test ClaimXEnrichmentTask instantiation with valid data."""

    def test_create_with_required_fields(self):
        """ClaimXEnrichmentTask can be created with only required fields."""
        now = datetime.now(timezone.utc)
        task = ClaimXEnrichmentTask(
            event_id="evt_123",
            event_type="PROJECT_CREATED",
            project_id="proj_456",
            created_at=now
        )

        assert task.event_id == "evt_123"
        assert task.event_type == "PROJECT_CREATED"
        assert task.project_id == "proj_456"
        assert task.retry_count == 0
        assert task.created_at == now
        assert task.media_id is None
        assert task.task_assignment_id is None
        assert task.video_collaboration_id is None
        assert task.master_file_name is None

    def test_create_with_media_id(self):
        """ClaimXEnrichmentTask with media_id for file events."""
        now = datetime.now(timezone.utc)
        task = ClaimXEnrichmentTask(
            event_id="evt_789",
            event_type="PROJECT_FILE_ADDED",
            project_id="proj_456",
            created_at=now,
            media_id="media_111"
        )

        assert task.media_id == "media_111"

    def test_create_with_retry_count(self):
        """ClaimXEnrichmentTask with non-zero retry_count."""
        now = datetime.now(timezone.utc)
        task = ClaimXEnrichmentTask(
            event_id="evt_retry",
            event_type="PROJECT_CREATED",
            project_id="proj_456",
            created_at=now,
            retry_count=3
        )

        assert task.retry_count == 3

    def test_create_with_all_optional_fields(self):
        """ClaimXEnrichmentTask with all optional fields populated."""
        now = datetime.now(timezone.utc)
        task = ClaimXEnrichmentTask(
            event_id="evt_full",
            event_type="PROJECT_FILE_ADDED",
            project_id="proj_456",
            created_at=now,
            retry_count=2,
            media_id="media_111",
            task_assignment_id="task_999",
            video_collaboration_id="video_777",
            master_file_name="CLAIM-2024-001"
        )

        assert task.media_id == "media_111"
        assert task.task_assignment_id == "task_999"
        assert task.video_collaboration_id == "video_777"
        assert task.master_file_name == "CLAIM-2024-001"


class TestClaimXEnrichmentTaskValidation:
    """Test field validation rules for ClaimXEnrichmentTask."""

    def test_missing_event_id_raises_error(self):
        """Missing event_id raises ValidationError."""
        now = datetime.now(timezone.utc)
        with pytest.raises(ValidationError) as exc_info:
            ClaimXEnrichmentTask(
                event_type="PROJECT_CREATED",
                project_id="proj_456",
                created_at=now
            )

        errors = exc_info.value.errors()
        assert any('event_id' in str(e) for e in errors)

    def test_empty_event_id_raises_error(self):
        """Empty event_id raises ValidationError."""
        now = datetime.now(timezone.utc)
        with pytest.raises(ValidationError) as exc_info:
            ClaimXEnrichmentTask(
                event_id="",
                event_type="PROJECT_CREATED",
                project_id="proj_456",
                created_at=now
            )

        errors = exc_info.value.errors()
        assert any('event_id' in str(e) for e in errors)

    def test_negative_retry_count_raises_error(self):
        """Negative retry_count raises ValidationError."""
        now = datetime.now(timezone.utc)
        with pytest.raises(ValidationError) as exc_info:
            ClaimXEnrichmentTask(
                event_id="evt_123",
                event_type="PROJECT_CREATED",
                project_id="proj_456",
                created_at=now,
                retry_count=-1
            )

        errors = exc_info.value.errors()
        assert any('retry_count' in str(e) for e in errors)

    def test_whitespace_is_trimmed(self):
        """Leading/trailing whitespace is trimmed from validated fields."""
        now = datetime.now(timezone.utc)
        task = ClaimXEnrichmentTask(
            event_id="  evt_123  ",
            event_type="  PROJECT_CREATED  ",
            project_id="  proj_456  ",
            created_at=now
        )

        assert task.event_id == "evt_123"
        assert task.event_type == "PROJECT_CREATED"
        assert task.project_id == "proj_456"


class TestClaimXEnrichmentTaskSerialization:
    """Test JSON serialization for ClaimXEnrichmentTask."""

    def test_serialize_to_json(self):
        """ClaimXEnrichmentTask serializes to JSON correctly."""
        now = datetime.now(timezone.utc)
        task = ClaimXEnrichmentTask(
            event_id="evt_123",
            event_type="PROJECT_FILE_ADDED",
            project_id="proj_456",
            created_at=now,
            media_id="media_111"
        )

        json_str = task.model_dump_json()
        assert "evt_123" in json_str
        assert "PROJECT_FILE_ADDED" in json_str
        assert "proj_456" in json_str
        assert "media_111" in json_str

    def test_created_at_serializes_as_iso(self):
        """created_at field serializes as ISO 8601 string."""
        now = datetime.now(timezone.utc)
        task = ClaimXEnrichmentTask(
            event_id="evt_123",
            event_type="PROJECT_CREATED",
            project_id="proj_456",
            created_at=now
        )

        json_str = task.model_dump_json()
        assert now.isoformat() in json_str or now.isoformat().replace('+00:00', 'Z') in json_str


class TestClaimXDownloadTaskCreation:
    """Test ClaimXDownloadTask instantiation with valid data."""

    def test_create_with_required_fields(self):
        """ClaimXDownloadTask can be created with only required fields."""
        task = ClaimXDownloadTask(
            media_id="media_111",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
            blob_path="claimx/proj_456/media/photo.jpg"
        )

        assert task.media_id == "media_111"
        assert task.project_id == "proj_456"
        assert task.download_url == "https://s3.amazonaws.com/claimx/presigned/photo.jpg"
        assert task.blob_path == "claimx/proj_456/media/photo.jpg"
        assert task.file_type == ""
        assert task.file_name == ""
        assert task.source_event_id == ""
        assert task.retry_count == 0
        assert task.expires_at is None
        assert task.refresh_count == 0

    def test_create_with_all_fields(self):
        """ClaimXDownloadTask can be created with all fields populated."""
        task = ClaimXDownloadTask(
            media_id="media_111",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
            blob_path="claimx/proj_456/media/photo.jpg",
            file_type="jpg",
            file_name="photo.jpg",
            source_event_id="evt_123",
            retry_count=2,
            expires_at="2024-12-26T10:30:00Z",
            refresh_count=1
        )

        assert task.file_type == "jpg"
        assert task.file_name == "photo.jpg"
        assert task.source_event_id == "evt_123"
        assert task.retry_count == 2
        assert task.expires_at == "2024-12-26T10:30:00Z"
        assert task.refresh_count == 1

    def test_create_with_video_file(self):
        """ClaimXDownloadTask can handle video file types."""
        task = ClaimXDownloadTask(
            media_id="media_222",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/video.mp4",
            blob_path="claimx/proj_456/media/video.mp4",
            file_type="mp4",
            file_name="damage_video.mp4"
        )

        assert task.file_type == "mp4"
        assert task.file_name == "damage_video.mp4"


class TestClaimXDownloadTaskValidation:
    """Test field validation rules for ClaimXDownloadTask."""

    def test_missing_media_id_raises_error(self):
        """Missing media_id raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            ClaimXDownloadTask(
                project_id="proj_456",
                download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
                blob_path="claimx/proj_456/media/photo.jpg"
            )

        errors = exc_info.value.errors()
        assert any('media_id' in str(e) for e in errors)

    def test_empty_media_id_raises_error(self):
        """Empty media_id raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            ClaimXDownloadTask(
                media_id="",
                project_id="proj_456",
                download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
                blob_path="claimx/proj_456/media/photo.jpg"
            )

        errors = exc_info.value.errors()
        assert any('media_id' in str(e) for e in errors)

    def test_empty_download_url_raises_error(self):
        """Empty download_url raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            ClaimXDownloadTask(
                media_id="media_111",
                project_id="proj_456",
                download_url="",
                blob_path="claimx/proj_456/media/photo.jpg"
            )

        errors = exc_info.value.errors()
        assert any('download_url' in str(e) for e in errors)

    def test_negative_retry_count_raises_error(self):
        """Negative retry_count raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            ClaimXDownloadTask(
                media_id="media_111",
                project_id="proj_456",
                download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
                blob_path="claimx/proj_456/media/photo.jpg",
                retry_count=-1
            )

        errors = exc_info.value.errors()
        assert any('retry_count' in str(e) for e in errors)

    def test_negative_refresh_count_raises_error(self):
        """Negative refresh_count raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            ClaimXDownloadTask(
                media_id="media_111",
                project_id="proj_456",
                download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
                blob_path="claimx/proj_456/media/photo.jpg",
                refresh_count=-1
            )

        errors = exc_info.value.errors()
        assert any('refresh_count' in str(e) for e in errors)

    def test_whitespace_is_trimmed(self):
        """Leading/trailing whitespace is trimmed from validated fields."""
        task = ClaimXDownloadTask(
            media_id="  media_111  ",
            project_id="  proj_456  ",
            download_url="  https://s3.amazonaws.com/claimx/presigned/photo.jpg  ",
            blob_path="  claimx/proj_456/media/photo.jpg  "
        )

        assert task.media_id == "media_111"
        assert task.project_id == "proj_456"
        assert task.download_url == "https://s3.amazonaws.com/claimx/presigned/photo.jpg"
        assert task.blob_path == "claimx/proj_456/media/photo.jpg"


class TestClaimXDownloadTaskSerialization:
    """Test JSON serialization for ClaimXDownloadTask."""

    def test_serialize_to_json(self):
        """ClaimXDownloadTask serializes to JSON correctly."""
        task = ClaimXDownloadTask(
            media_id="media_111",
            project_id="proj_456",
            download_url="https://s3.amazonaws.com/claimx/presigned/photo.jpg",
            blob_path="claimx/proj_456/media/photo.jpg",
            file_type="jpg",
            file_name="photo.jpg"
        )

        json_str = task.model_dump_json()
        assert "media_111" in json_str
        assert "proj_456" in json_str
        assert "photo.jpg" in json_str

    def test_deserialize_from_json(self):
        """ClaimXDownloadTask deserializes from JSON correctly."""
        json_data = {
            "media_id": "media_111",
            "project_id": "proj_456",
            "download_url": "https://s3.amazonaws.com/claimx/presigned/photo.jpg",
            "blob_path": "claimx/proj_456/media/photo.jpg",
            "file_type": "jpg",
            "file_name": "photo.jpg",
            "retry_count": 1
        }

        task = ClaimXDownloadTask.model_validate(json_data)
        assert task.media_id == "media_111"
        assert task.file_type == "jpg"
        assert task.retry_count == 1
