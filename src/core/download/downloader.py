"""
Unified attachment downloader with clean interface.

Provides AttachmentDownloader class that orchestrates:
- URL validation (SSRF prevention)
- File type validation (extension and MIME type)
- HTTP download (in-memory or streaming based on size)
- Error classification and reporting

Clean interface: DownloadTask -> DownloadOutcome
"""

import asyncio
import logging
import time

import aiohttp

from core.download.http_client import create_session, download_url
from core.download.models import DownloadOutcome, DownloadTask
from core.download.streaming import download_to_file, should_stream
from core.errors.exceptions import ErrorCategory, classify_os_error
from core.security.exceptions import FileValidationError, URLValidationError
from core.security.file_validation import validate_file_type
from core.security.presigned_urls import check_presigned_url
from core.security.url_validation import validate_download_url

logger = logging.getLogger(__name__)


class AttachmentDownloader:
    """
    Unified downloader orchestrating validation, download, and error handling.

    Automatically handles URL/file validation, streaming/in-memory downloads based
    on size, and error classification. Use a shared session for batch downloads
    to reuse connections.
    """

    def __init__(
        self,
        session: aiohttp.ClientSession | None = None,
        max_connections: int = 100,
        max_connections_per_host: int = 10,
    ):
        self._session = session
        self._owns_session = session is None
        self._max_connections = max_connections
        self._max_connections_per_host = max_connections_per_host

    async def download(self, task: DownloadTask) -> DownloadOutcome:
        """Download attachment with validation and error handling."""
        # Step 1: Validate URL
        if task.validate_url:
            try:
                validate_download_url(
                    task.url,
                    allowed_domains=task.allowed_domains,
                    allow_localhost=task.allow_localhost,
                )
            except URLValidationError as e:
                return DownloadOutcome.validation_failure(
                    validation_error=f"URL validation failed: {str(e)}",
                    error_category=ErrorCategory.PERMANENT,
                )

        # Step 2: Check presigned URL expiration (Xact S3 URLs)
        if task.check_expiration:
            url_info = check_presigned_url(task.url)
            # S3 presigned URLs are used by Xact - no refresh capability
            if url_info.url_type == "s3" and url_info.is_expired:
                expires_at = url_info.expires_at.isoformat() if url_info.expires_at else "unknown"
                signed_at = url_info.signed_at.isoformat() if url_info.signed_at else "unknown"

                logger.debug(
                    "Presigned URL expired, sending to DLQ",
                    extra={
                        "url_type": url_info.url_type,
                        "signed_at": signed_at,
                        "expires_at": expires_at,
                        "ttl_seconds": url_info.ttl_seconds,
                        "seconds_expired": abs(url_info.seconds_remaining or 0),
                    },
                )
                return DownloadOutcome.validation_failure(
                    validation_error=f"Presigned URL expired at {expires_at} (signed at {signed_at})",
                    error_category=ErrorCategory.PERMANENT,
                )

        # Step 3: Validate file type from URL extension
        if task.validate_file_type:
            try:
                validate_file_type(task.url, allowed_extensions=task.allowed_extensions)
            except FileValidationError as e:
                return DownloadOutcome.validation_failure(
                    validation_error=f"File type validation failed: {str(e)}",
                    error_category=ErrorCategory.PERMANENT,
                )

        # Step 4: Perform HTTP download
        session = self._session
        should_close_session = False

        try:
            # Create session if needed
            if session is None:
                session = create_session(
                    max_connections=self._max_connections,
                    max_connections_per_host=self._max_connections_per_host,
                )
                should_close_session = True

            # HEAD request to check Content-Length for streaming decision
            t_head = time.perf_counter()
            content_length = await self._get_content_length(task.url, session, task.timeout)
            head_ms = int((time.perf_counter() - t_head) * 1000)
            logger.debug(
                f"HEAD request completed in {head_ms}ms: content_length={content_length}",
                extra={"download_url": task.url[:120]},
            )

            # Check max size if specified
            if task.max_size and content_length and content_length > task.max_size:
                return DownloadOutcome.validation_failure(
                    validation_error=f"File size {content_length} exceeds maximum {task.max_size}",
                    error_category=ErrorCategory.PERMANENT,
                )

            # Decide on streaming vs in-memory based on size
            use_streaming = should_stream(content_length)

            t_dl = time.perf_counter()
            if use_streaming:
                outcome = await self._download_streaming(task, session)
            else:
                outcome = await self._download_in_memory(task, session)
            dl_ms = int((time.perf_counter() - t_dl) * 1000)

            logger.debug(
                f"Download request completed in {dl_ms}ms: "
                f"streaming={use_streaming}, success={outcome.success}, "
                f"status_code={outcome.status_code}, error={outcome.error_message}",
                extra={"download_url": task.url[:120]},
            )

            # Step 5: Validate download integrity
            if outcome.success:
                integrity_error = None
                if outcome.bytes_downloaded == 0:
                    integrity_error = "Download produced zero bytes"
                elif content_length and outcome.bytes_downloaded != content_length:
                    integrity_error = (
                        f"Size mismatch: expected {content_length} bytes, "
                        f"got {outcome.bytes_downloaded}"
                    )

                if integrity_error:
                    if outcome.file_path and outcome.file_path.exists():
                        outcome.file_path.unlink()
                    return DownloadOutcome.download_failure(
                        error_message=integrity_error,
                        error_category=ErrorCategory.TRANSIENT,
                    )

            # Step 6: Validate Content-Type from response
            if outcome.success and task.validate_file_type and outcome.content_type:
                try:
                    validate_file_type(
                        task.url,
                        content_type=outcome.content_type,
                        allowed_extensions=task.allowed_extensions,
                    )
                except FileValidationError as e:
                    # Delete downloaded file on validation failure
                    if outcome.file_path and outcome.file_path.exists():
                        outcome.file_path.unlink()

                    # Content-Type mismatch likely means the server returned an
                    # error page (e.g. JSON 403) instead of the actual file.
                    # Classify as transient so it gets retried rather than DLQ'd.
                    return DownloadOutcome.validation_failure(
                        validation_error=f"Content-Type validation failed: {str(e)}",
                        error_category=ErrorCategory.TRANSIENT,
                    )

            return outcome

        finally:
            # Clean up session if we created it
            if should_close_session and session:
                await session.close()
                await asyncio.sleep(0)

    async def _get_content_length(
        self, url: str, session: aiohttp.ClientSession, timeout: int
    ) -> int | None:
        """Get Content-Length from HEAD request, or None if unavailable."""
        try:
            # HEAD requests should be fast - cap at 30s total, 10s socket read
            # This prevents hanging on slow servers when we're just checking size
            async with session.head(
                url,
                timeout=aiohttp.ClientTimeout(total=min(timeout, 30), sock_read=10),
                allow_redirects=True,
            ) as response:
                return response.content_length
        except Exception:
            # HEAD request failed or not supported - continue with download
            return None

    async def _download_in_memory(
        self, task: DownloadTask, session: aiohttp.ClientSession
    ) -> DownloadOutcome:
        """Download file in-memory (for files < 50MB)."""
        response, error = await download_url(
            url=task.url,
            session=session,
            timeout=task.timeout,
        )

        if error:
            # Log timeout errors with additional context
            if "timeout" in error.error_message.lower():
                logger.warning(
                    "Download timeout",
                    extra={
                        "url": task.url,
                        "timeout_seconds": task.timeout,
                        "error_message": error.error_message,
                    },
                )

            return DownloadOutcome.download_failure(
                error_message=error.error_message,
                error_category=error.error_category,
                status_code=error.status_code,
            )

        # Write content to file
        try:
            # Use asyncio.to_thread for mkdir to ensure proper synchronization
            # on Windows, where synchronous mkdir may not be immediately visible
            await asyncio.to_thread(task.destination.parent.mkdir, parents=True, exist_ok=True)
            await asyncio.to_thread(task.destination.write_bytes, response.content)

            return DownloadOutcome.success_outcome(
                file_path=task.destination,
                bytes_downloaded=len(response.content),
                content_type=response.content_type,
                status_code=response.status_code,
            )

        except OSError as e:
            return DownloadOutcome.download_failure(
                error_message=f"File write error: {str(e)}",
                error_category=classify_os_error(e),
            )

    async def _download_streaming(
        self, task: DownloadTask, session: aiohttp.ClientSession
    ) -> DownloadOutcome:
        """Download file using streaming (for files > 50MB)."""
        # Ensure parent directory exists
        # Use asyncio.to_thread for mkdir to ensure proper synchronization
        # on Windows, where synchronous mkdir may not be immediately visible
        await asyncio.to_thread(task.destination.parent.mkdir, parents=True, exist_ok=True)

        result, error = await download_to_file(
            url=task.url,
            output_path=task.destination,
            session=session,
        )

        if error:
            # Log timeout errors with additional context
            if "timeout" in error.error_message.lower():
                logger.warning(
                    "Download timeout (streaming)",
                    extra={
                        "url": task.url,
                        "timeout_seconds": task.timeout,
                        "error_message": error.error_message,
                    },
                )

            return DownloadOutcome.download_failure(
                error_message=error.error_message,
                error_category=error.error_category,
                status_code=error.status_code,
            )

        return DownloadOutcome.success_outcome(
            file_path=task.destination,
            bytes_downloaded=result.bytes_written,
            content_type=result.content_type,
            status_code=200,
        )


__all__ = ["AttachmentDownloader"]
