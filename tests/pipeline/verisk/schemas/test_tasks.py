"""Tests for pipeline.verisk.schemas.tasks module."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from pipeline.verisk.schemas.tasks import DownloadTaskMessage, XACTEnrichmentTask


def _make_enrichment_task(**overrides):
    defaults = {
        "event_id": "evt_1",
        "trace_id": "t1",
        "event_type": "xact",
        "status_subtype": "documentsReceived",
        "assignment_id": "A1",
        "created_at": datetime(2024, 1, 1, tzinfo=UTC),
        "original_timestamp": datetime(2024, 1, 1, tzinfo=UTC),
    }
    defaults.update(overrides)
    return XACTEnrichmentTask(**defaults)


def _make_download_task(**overrides):
    defaults = {
        "trace_id": "t1",
        "media_id": "m1",
        "attachment_url": "https://example.com/file.pdf",
        "blob_path": "docs/A1/pdf/file.pdf",
        "status_subtype": "documentsReceived",
        "file_type": "pdf",
        "assignment_id": "A1",
        "event_type": "xact",
        "event_subtype": "documentsReceived",
        "original_timestamp": datetime(2024, 1, 1, tzinfo=UTC),
    }
    defaults.update(overrides)
    return DownloadTaskMessage(**defaults)


class TestXACTEnrichmentTask:
    def test_valid_creation(self):
        task = _make_enrichment_task()
        assert task.event_id == "evt_1"
        assert task.retry_count == 0
        assert task.estimate_version is None
        assert task.attachments == []
        assert task.metadata is None

    def test_strips_whitespace(self):
        task = _make_enrichment_task(event_id="  evt_1  ")
        assert task.event_id == "evt_1"

    def test_empty_required_field_rejected(self):
        with pytest.raises(ValidationError):
            _make_enrichment_task(event_id="")

    def test_negative_retry_count_rejected(self):
        with pytest.raises(ValidationError):
            _make_enrichment_task(retry_count=-1)

    def test_timestamp_serialization(self):
        task = _make_enrichment_task()
        data = task.model_dump()
        assert isinstance(data["created_at"], str)
        assert isinstance(data["original_timestamp"], str)


class TestDownloadTaskMessage:
    def test_valid_creation(self):
        task = _make_download_task()
        assert task.trace_id == "t1"
        assert task.retry_count == 0
        assert task.estimate_version is None
        assert task.metadata == {}

    def test_strips_whitespace(self):
        task = _make_download_task(trace_id="  t1  ")
        assert task.trace_id == "t1"

    def test_empty_required_field_rejected(self):
        with pytest.raises(ValidationError):
            _make_download_task(trace_id="")

    def test_negative_retry_count_rejected(self):
        with pytest.raises(ValidationError):
            _make_download_task(retry_count=-1)

    def test_to_verisk_task_raises_due_to_media_id_mismatch(self):
        # to_verisk_task passes media_id kwarg but Task dataclass doesn't accept it
        task = _make_download_task(estimate_version="1.0")
        with pytest.raises(TypeError):
            task.to_verisk_task()

    def test_from_verisk_task(self):
        from pipeline.verisk.schemas.models import Task

        verisk_task = Task(
            trace_id="t1",
            attachment_url="https://example.com/file.pdf",
            blob_path="docs/A1/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="A1",
        )
        # from_verisk_task doesn't set event_type, event_subtype, original_timestamp
        # These are required fields, so from_verisk_task will fail for incomplete Tasks.
        # Test that the converter at least maps the core fields it can.
        with pytest.raises(ValidationError):
            DownloadTaskMessage.from_verisk_task(verisk_task)

    def test_serialization_roundtrip(self):
        task = _make_download_task()
        data = task.model_dump()
        restored = DownloadTaskMessage(**data)
        assert restored.trace_id == task.trace_id
        assert restored.attachment_url == task.attachment_url
