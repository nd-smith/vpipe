"""
Data models for attachment download operations.

Defines clean input/output models for the AttachmentDownloader interface:
- DownloadTask: Input specification for what to download
- DownloadOutcome: Result of download operation with metadata
"""

from dataclasses import dataclass
from pathlib import Path

from core.errors.exceptions import ErrorCategory


@dataclass
class DownloadTask:
    """
    Input specification for attachment download.

    Security: allow_localhost is automatically blocked in production regardless
    of setting. Use only in development/testing with SIMULATION_MODE=true.
    """

    url: str
    destination: Path
    timeout: int = 60
    validate_url: bool = True
    validate_file_type: bool = True
    check_expiration: bool = False
    allow_localhost: bool = False
    allowed_domains: set[str] | None = None
    allowed_extensions: set[str] | None = None
    max_size: int | None = None
    skip_head: bool = False


@dataclass
class DownloadOutcome:
    """Result of attachment download operation with error classification."""

    success: bool
    file_path: Path | None = None
    bytes_downloaded: int = 0
    content_type: str | None = None
    status_code: int | None = None
    error_message: str | None = None
    error_category: ErrorCategory | None = None
    validation_error: str | None = None

    @classmethod
    def success_outcome(
        cls,
        file_path: Path,
        bytes_downloaded: int,
        content_type: str | None,
        status_code: int,
    ) -> "DownloadOutcome":
        return cls(
            success=True,
            file_path=file_path,
            bytes_downloaded=bytes_downloaded,
            content_type=content_type,
            status_code=status_code,
        )

    @classmethod
    def validation_failure(
        cls,
        validation_error: str,
        error_category: ErrorCategory = ErrorCategory.PERMANENT,
    ) -> "DownloadOutcome":
        return cls(
            success=False,
            error_message=f"Validation failed: {validation_error}",
            error_category=error_category,
            validation_error=validation_error,
        )

    @classmethod
    def download_failure(
        cls,
        error_message: str,
        error_category: ErrorCategory,
        status_code: int | None = None,
    ) -> "DownloadOutcome":
        return cls(
            success=False,
            error_message=error_message,
            error_category=error_category,
            status_code=status_code,
        )


__all__ = ["DownloadTask", "DownloadOutcome"]
