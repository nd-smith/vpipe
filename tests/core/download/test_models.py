"""Tests for core.download.models module."""

from pathlib import Path

from core.download.models import DownloadOutcome, DownloadTask
from core.errors.exceptions import ErrorCategory


class TestDownloadTask:
    def test_defaults(self):
        task = DownloadTask(url="https://example.com/file.pdf", destination=Path("/tmp/file.pdf"))
        assert task.url == "https://example.com/file.pdf"
        assert task.destination == Path("/tmp/file.pdf")
        assert task.timeout == 60
        assert task.validate_url is True
        assert task.validate_file_type is True
        assert task.check_expiration is False
        assert task.allow_localhost is False
        assert task.allowed_domains is None
        assert task.allowed_extensions is None
        assert task.max_size is None
        assert task.skip_head is False

    def test_custom_values(self):
        task = DownloadTask(
            url="https://example.com/file.pdf",
            destination=Path("/tmp/file.pdf"),
            timeout=120,
            validate_url=False,
            allowed_domains={"example.com"},
            max_size=1024,
        )
        assert task.timeout == 120
        assert task.validate_url is False
        assert task.allowed_domains == {"example.com"}
        assert task.max_size == 1024


class TestDownloadOutcome:
    def test_success_outcome(self):
        outcome = DownloadOutcome.success_outcome(
            file_path=Path("/tmp/file.pdf"),
            bytes_downloaded=1024,
            content_type="application/pdf",
            status_code=200,
        )
        assert outcome.success is True
        assert outcome.file_path == Path("/tmp/file.pdf")
        assert outcome.bytes_downloaded == 1024
        assert outcome.content_type == "application/pdf"
        assert outcome.status_code == 200
        assert outcome.error_message is None

    def test_validation_failure(self):
        outcome = DownloadOutcome.validation_failure("Invalid URL")
        assert outcome.success is False
        assert outcome.error_message == "Validation failed: Invalid URL"
        assert outcome.error_category == ErrorCategory.PERMANENT
        assert outcome.validation_error == "Invalid URL"

    def test_validation_failure_custom_category(self):
        outcome = DownloadOutcome.validation_failure("Temp issue", ErrorCategory.TRANSIENT)
        assert outcome.error_category == ErrorCategory.TRANSIENT

    def test_download_failure(self):
        outcome = DownloadOutcome.download_failure(
            error_message="Connection timed out",
            error_category=ErrorCategory.TRANSIENT,
            status_code=503,
        )
        assert outcome.success is False
        assert outcome.error_message == "Connection timed out"
        assert outcome.error_category == ErrorCategory.TRANSIENT
        assert outcome.status_code == 503

    def test_download_failure_no_status_code(self):
        outcome = DownloadOutcome.download_failure(
            error_message="DNS error",
            error_category=ErrorCategory.TRANSIENT,
        )
        assert outcome.status_code is None
