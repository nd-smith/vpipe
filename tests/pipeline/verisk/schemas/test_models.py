"""Tests for pipeline.verisk.schemas.models module."""

from pipeline.verisk.schemas.models import XACT_PRIMARY_KEYS, EventRecord, Task


class TestEventRecord:
    def test_basic_creation(self):
        record = EventRecord(
            type="status.completed",
            version="1.0",
            utc_datetime="2024-01-01T00:00:00Z",
            trace_id="t1",
            data='{"key": "value"}',
        )
        assert record.type == "status.completed"
        assert record.version == "1.0"
        assert record.trace_id == "t1"
        assert record.data == '{"key": "value"}'

    def test_status_subtype_with_dot(self):
        record = EventRecord(
            type="status.completed",
            version="1.0",
            utc_datetime="2024-01-01T00:00:00Z",
            trace_id="t1",
            data="{}",
        )
        assert record.status_subtype == "completed"

    def test_status_subtype_without_dot(self):
        record = EventRecord(
            type="completed",
            version="1.0",
            utc_datetime="2024-01-01T00:00:00Z",
            trace_id="t1",
            data="{}",
        )
        assert record.status_subtype == "completed"

    def test_status_subtype_multiple_dots(self):
        record = EventRecord(
            type="a.b.c",
            version="1.0",
            utc_datetime="2024-01-01T00:00:00Z",
            trace_id="t1",
            data="{}",
        )
        assert record.status_subtype == "c"


class TestTask:
    def test_required_fields(self):
        task = Task(
            trace_id="t1",
            attachment_url="https://example.com/file.pdf",
            blob_path="/path/to/blob",
            status_subtype="completed",
            file_type="pdf",
            assignment_id="a1",
        )
        assert task.trace_id == "t1"
        assert task.attachment_url == "https://example.com/file.pdf"
        assert task.retry_count == 0
        assert task.estimate_version is None

    def test_optional_fields(self):
        task = Task(
            trace_id="t1",
            attachment_url="https://example.com/file.pdf",
            blob_path="/path",
            status_subtype="completed",
            file_type="pdf",
            assignment_id="a1",
            estimate_version="v2",
            retry_count=3,
        )
        assert task.estimate_version == "v2"
        assert task.retry_count == 3


class TestConstants:
    def test_xact_primary_keys(self):
        assert XACT_PRIMARY_KEYS == ["trace_id", "attachment_url"]
