"""
Streaming download support for large files with memory bounds.

Provides chunked streaming functionality to handle files larger than
available memory. Extracts streaming logic from xact_download.py to be
reusable across pipeline components.
"""

import asyncio
import errno
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator, Optional

import aiohttp

from core.errors.exceptions import (
    ErrorCategory,
    TimeoutError as PipelineTimeoutError,
    classify_http_status,
)


# Download configuration constants
CHUNK_SIZE = 8 * 1024 * 1024  # 8MB chunks for streaming
STREAM_THRESHOLD = 50 * 1024 * 1024  # Stream files > 50MB


@dataclass
class StreamDownloadResponse:
    """
    Response from streaming HTTP download operation.

    Attributes:
        status_code: HTTP status code
        content_length: Size in bytes (from Content-Length header)
        content_type: MIME type (from Content-Type header)
        chunk_iterator: Async iterator yielding byte chunks
    """

    status_code: int
    content_length: Optional[int]
    content_type: Optional[str]
    chunk_iterator: AsyncIterator[bytes]


@dataclass
class StreamDownloadError:
    """
    Error result from failed streaming download.

    Attributes:
        status_code: HTTP status code if received
        error_message: Error description
        error_category: Classification for retry decisions
    """

    status_code: Optional[int]
    error_message: str
    error_category: ErrorCategory


async def stream_download_url(
    url: str,
    session: aiohttp.ClientSession,
    timeout: int = 60,
    chunk_size: int = CHUNK_SIZE,
    allow_redirects: bool = True,
    sock_read_timeout: int = 30,
) -> tuple[Optional[StreamDownloadResponse], Optional[StreamDownloadError]]:
    """
    Stream download content from URL using async HTTP with chunked reading.

    This function is optimized for large files (>50MB) and returns an async
    iterator for memory-efficient processing. The iterator MUST be consumed
    within the context manager scope.

    Does NOT perform:
    - URL validation (caller's responsibility)
    - Circuit breaker checks (higher-level concern)
    - Retry logic (higher-level concern)
    - Temp file management (caller's responsibility)

    Args:
        url: URL to download
        session: aiohttp ClientSession (caller manages lifecycle)
        timeout: Timeout in seconds (default: 60)
        chunk_size: Size of chunks in bytes (default: 8MB)
        allow_redirects: Whether to follow redirects (default: True for S3 presigned URLs)
        sock_read_timeout: Timeout for individual socket read operations in seconds
            (default: 30). Prevents hanging on stalled connections where the server
            stops sending data but keeps the connection open.

    Returns:
        Tuple of (StreamDownloadResponse, None) on success
        or (None, StreamDownloadError) on failure

    Example:
        async with aiohttp.ClientSession() as session:
            response, error = await stream_download_url(
                "https://example.com/largefile.pdf",
                session,
                timeout=120
            )
            if error:
                # Handle error
                print(f"Download failed: {error.error_message}")
            else:
                # Stream to file
                with open("output.pdf", "wb") as f:
                    async for chunk in response.chunk_iterator:
                        f.write(chunk)
    """
    try:
        # Create the HTTP request context
        # sock_read timeout ensures we don't hang on stalled connections
        # where the server stops sending data mid-stream
        response_ctx = session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=timeout, sock_read=sock_read_timeout),
            allow_redirects=allow_redirects,
        )

        # Get the response object
        response = await response_ctx.__aenter__()

        # Check status code
        if response.status != 200:
            error_category = classify_http_status(response.status)
            await response_ctx.__aexit__(None, None, None)

            return None, StreamDownloadError(
                status_code=response.status,
                error_message=f"HTTP {response.status}",
                error_category=error_category,
            )

        # Extract metadata
        content_length = response.content_length
        content_type = response.headers.get("Content-Type")

        # Create async iterator for chunks
        async def chunk_iterator() -> AsyncIterator[bytes]:
            """
            Async generator that yields chunks from the response.

            Note: This iterator manages the response context lifecycle.
            It will close the response when iteration completes or fails.
            """
            try:
                async for chunk in response.content.iter_chunked(chunk_size):
                    yield chunk
            finally:
                # Ensure response is closed after iteration
                await response_ctx.__aexit__(None, None, None)

        return (
            StreamDownloadResponse(
                status_code=response.status,
                content_length=content_length,
                content_type=content_type,
                chunk_iterator=chunk_iterator(),
            ),
            None,
        )

    except asyncio.TimeoutError as e:
        return None, StreamDownloadError(
            status_code=None,
            error_message=f"Download timeout after {timeout}s",
            error_category=ErrorCategory.TRANSIENT,
        )

    except aiohttp.ClientError as e:
        # Connection errors, DNS failures, etc.
        return None, StreamDownloadError(
            status_code=None,
            error_message=f"Connection error: {str(e)}",
            error_category=ErrorCategory.TRANSIENT,
        )


@dataclass
class DownloadToFileResult:
    """
    Result from download_to_file operation.

    Attributes:
        bytes_written: Number of bytes written to file
        content_type: MIME type from Content-Type header
    """

    bytes_written: int
    content_type: Optional[str]


async def download_to_file(
    url: str,
    output_path: Path,
    session: aiohttp.ClientSession,
    timeout: int = 120,
    chunk_size: int = CHUNK_SIZE,
    sock_read_timeout: int = 30,
) -> tuple[Optional[DownloadToFileResult], Optional[StreamDownloadError]]:
    """
    Download URL content directly to file using streaming.

    Convenience function that combines streaming download with file writing.
    Useful for simple file download scenarios without custom processing.

    Args:
        url: URL to download
        output_path: Path where file will be saved
        session: aiohttp ClientSession (caller manages lifecycle)
        timeout: Timeout in seconds (default: 120 for large files)
        chunk_size: Size of chunks in bytes (default: 8MB)
        sock_read_timeout: Timeout for individual socket read operations in seconds
            (default: 30). Prevents hanging on stalled connections.

    Returns:
        Tuple of (DownloadToFileResult, None) on success
        or (None, StreamDownloadError) on failure

    Example:
        async with aiohttp.ClientSession() as session:
            result, error = await download_to_file(
                "https://example.com/file.pdf",
                Path("output.pdf"),
                session
            )
            if error:
                print(f"Download failed: {error.error_message}")
            else:
                print(f"Downloaded {result.bytes_written} bytes, type: {result.content_type}")
    """
    response, error = await stream_download_url(
        url=url,
        session=session,
        timeout=timeout,
        chunk_size=chunk_size,
        sock_read_timeout=sock_read_timeout,
    )

    if error:
        return None, error

    # Get reference to chunk iterator for proper cleanup
    chunk_iterator = response.chunk_iterator

    try:
        bytes_written = 0
        # Ensure parent directory exists right before opening file
        # This handles potential timing issues on Windows where mkdir may not
        # be immediately visible to subsequent file operations
        await asyncio.to_thread(
            Path(output_path).parent.mkdir, parents=True, exist_ok=True
        )
        with open(output_path, "wb") as f:
            async for chunk in chunk_iterator:
                # Use asyncio.to_thread for disk I/O to avoid blocking event loop
                await asyncio.to_thread(f.write, chunk)
                bytes_written += len(chunk)

        return DownloadToFileResult(
            bytes_written=bytes_written,
            content_type=response.content_type,
        ), None

    except OSError as e:
        # Classify OSError - be conservative: only mark as PERMANENT if we're
        # certain it's not recoverable. Unknown errors should retry.
        # Permanent errors: disk full, read-only filesystem, permission denied
        permanent_errnos = (errno.ENOSPC, errno.EROFS, errno.EACCES, errno.EPERM)
        is_permanent = e.errno in permanent_errnos

        error_category = (
            ErrorCategory.PERMANENT if is_permanent else ErrorCategory.TRANSIENT
        )
        return None, StreamDownloadError(
            status_code=None,
            error_message=f"File write error: {str(e)}",
            error_category=error_category,
        )
    finally:
        # Ensure async generator is closed to release the HTTP connection
        # This prevents "Unclosed connection" warnings when iteration is
        # interrupted by errors or early exit
        await chunk_iterator.aclose()


def should_stream(content_length: Optional[int]) -> bool:
    """
    Determine if content should be streamed based on size.

    Uses STREAM_THRESHOLD (50MB) as the decision boundary.
    If content_length is unknown (None), defaults to streaming for safety.

    Args:
        content_length: Size in bytes from Content-Length header

    Returns:
        True if content should be streamed, False for in-memory download

    Example:
        if should_stream(content_length):
            # Use streaming download
            response, error = await stream_download_url(...)
        else:
            # Use in-memory download
            response, error = await download_url(...)
    """
    if content_length is None:
        # Unknown size - stream to be safe
        return True

    return content_length > STREAM_THRESHOLD


__all__ = [
    "CHUNK_SIZE",
    "STREAM_THRESHOLD",
    "StreamDownloadResponse",
    "StreamDownloadError",
    "DownloadToFileResult",
    "stream_download_url",
    "download_to_file",
    "should_stream",
]
