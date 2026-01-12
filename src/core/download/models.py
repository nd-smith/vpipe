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

    Attributes:
        url: URL to download from
        destination: Path where file should be saved
        timeout: Timeout in seconds (default: 60)
        validate_url: Whether to validate URL against domain allowlist (default: True)
        validate_file_type: Whether to validate file extension/content-type (default: True)
        check_expiration: Whether to check if presigned URLs are expired (default: False)
            When enabled, expired S3 presigned URLs (Xact) fail permanently.
        allowed_domains: Optional custom domain allowlist (None = use defaults)
        allowed_extensions: Optional custom file extension allowlist (None = use defaults)
        max_size: Optional maximum file size in bytes (None = no limit)
    """

    url: str
    destination: Path
    timeout: int = 60
    validate_url: bool = True
    validate_file_type: bool = True
    check_expiration: bool = False
    allowed_domains: Optional[Set[str]] = None
    allowed_extensions: Optional[Set[str]] = None
    max_size: Optional[int] = None


@dataclass
class DownloadOutcome:
    """
    Result of attachment download operation.

    Success case:
        success=True, file_path set, error fields None

    Failure case:
        success=False, error_message and error_category set, file_path None

    Attributes:
        success: Whether download succeeded
        file_path: Path to downloaded file (None on failure)
        bytes_downloaded: Number of bytes written to disk
        content_type: MIME type from Content-Type header
        status_code: HTTP status code (None for connection errors)
        error_message: Error description (None on success)
        error_category: Error classification for retry decisions (None on success)
        validation_error: Specific validation failure message (None if no validation error)
    """

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
        """
        Create successful download outcome.

        Args:
            file_path: Path to downloaded file
            bytes_downloaded: Number of bytes written
            content_type: MIME type from response
            status_code: HTTP status code (should be 200)

        Returns:
            DownloadOutcome with success=True
        """
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
        """
        Create validation failure outcome.

        Used for URL validation or file type validation failures.

        Args:
            validation_error: Specific validation error message
            error_category: Error classification (default: PERMANENT)

        Returns:
            DownloadOutcome with success=False and validation_error set
        """
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
        """
        Create download failure outcome.

        Used for HTTP errors, timeouts, connection failures, etc.

        Args:
            error_message: Error description
            error_category: Error classification for retry decisions
            status_code: HTTP status code if available

        Returns:
            DownloadOutcome with success=False
        """
        return cls(
            success=False,
            error_message=error_message,
            error_category=error_category,
            status_code=status_code,
        )


__all__ = ["DownloadTask", "DownloadOutcome"]
