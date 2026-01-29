"""
Data models for attachment download operations.

Defines clean input/output models for the AttachmentDownloader interface:
- DownloadTask: Input specification for what to download
- DownloadOutcome: Result of download operation with metadata
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Set

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
    allowed_domains: Optional[Set[str]] = None
    allowed_extensions: Optional[Set[str]] = None
    max_size: Optional[int] = None


@dataclass
class DownloadOutcome:
    """Result of attachment download operation with error classification."""

    success: bool
    file_path: Optional[Path] = None
    bytes_downloaded: int = 0
    content_type: Optional[str] = None
    status_code: Optional[int] = None
    error_message: Optional[str] = None
    error_category: Optional[ErrorCategory] = None
    validation_error: Optional[str] = None

    @classmethod
    def success_outcome(
        cls,
        file_path: Path,
        bytes_downloaded: int,
        content_type: Optional[str],
        status_code: int,
    ) -> "DownloadOutcome":
        """Create successful download outcome."""
        return cls(
            success=True,
            file_path=file_path,
            bytes_downloaded=bytes_downloaded,
            content_type=content_type,
            status_code=status_code,
        )

    @classmethod
    def validation_failure(
        cls, validation_error: str, error_category: ErrorCategory = ErrorCategory.PERMANENT
    ) -> "DownloadOutcome":
        """Create validation failure outcome (URL or file type validation)."""
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
        status_code: Optional[int] = None,
    ) -> "DownloadOutcome":
        """Create download failure outcome (HTTP errors, timeouts, connection failures)."""
        return cls(
            success=False,
            error_message=error_message,
            error_category=error_category,
            status_code=status_code,
        )


__all__ = ["DownloadTask", "DownloadOutcome"]
