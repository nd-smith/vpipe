"""Tests for pipeline.claimx.workers.download_factory module."""

from pipeline.claimx.workers.download_factory import (
    _generate_blob_path,
    create_download_tasks_from_media,
)


class TestCreateDownloadTasksFromMedia:
    def test_creates_tasks_for_valid_rows(self):
        rows = [
            {
                "media_id": "m1",
                "project_id": "p1",
                "full_download_link": "https://s3.example.com/file1.pdf",
                "file_type": "pdf",
                "file_name": "file1.pdf",
                "trace_id": "t1",
            },
            {
                "media_id": "m2",
                "project_id": "p1",
                "full_download_link": "https://s3.example.com/file2.jpg",
                "file_type": "jpg",
                "file_name": "file2.jpg",
                "trace_id": "t1",
            },
        ]
        tasks = create_download_tasks_from_media(rows)
        assert len(tasks) == 2
        assert tasks[0].media_id == "m1"
        assert tasks[0].download_url == "https://s3.example.com/file1.pdf"
        assert tasks[1].media_id == "m2"

    def test_skips_rows_without_download_url(self):
        rows = [
            {"media_id": "m1", "project_id": "p1", "full_download_link": ""},
            {"media_id": "m2", "project_id": "p1"},
            {
                "media_id": "m3",
                "project_id": "p1",
                "full_download_link": "https://s3.example.com/file.pdf",
                "file_type": "pdf",
                "file_name": "file.pdf",
                "trace_id": "t1",
            },
        ]
        tasks = create_download_tasks_from_media(rows)
        assert len(tasks) == 1
        assert tasks[0].media_id == "m3"

    def test_empty_list(self):
        tasks = create_download_tasks_from_media([])
        assert tasks == []

    def test_task_fields(self):
        rows = [
            {
                "media_id": "m1",
                "project_id": "p1",
                "full_download_link": "https://s3.example.com/file.pdf",
                "file_type": "pdf",
                "file_name": "file.pdf",
                "trace_id": "t1",
                "expires_at": "2024-12-31T23:59:59Z",
            },
        ]
        tasks = create_download_tasks_from_media(rows)
        task = tasks[0]
        assert task.retry_count == 0
        assert task.refresh_count == 0
        assert task.blob_path == "p1/media/file.pdf"
        assert task.expires_at == "2024-12-31T23:59:59Z"


class TestGenerateBlobPath:
    def test_normal_path(self):
        row = {"project_id": "p1", "media_id": "m1", "file_name": "photo.jpg"}
        assert _generate_blob_path(row) == "p1/media/photo.jpg"

    def test_missing_fields_use_defaults(self):
        path = _generate_blob_path({})
        assert path == "unknown/media/media_unknown"

    def test_missing_file_name_uses_media_id(self):
        row = {"project_id": "p1", "media_id": "m42"}
        assert _generate_blob_path(row) == "p1/media/media_m42"
