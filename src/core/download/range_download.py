"""
Range-based chunked downloads for large files.

Downloads large files using HTTP Range requests, enabling:
- Per-chunk timeouts (no single 60s wall clock for 200MB)
- Per-chunk retries (fail at chunk 7, retry chunk 7 — not byte 0)
- Resume from partial files on Kafka-level retry
"""

import asyncio
import logging
import random
from dataclasses import dataclass
from pathlib import Path

import aiohttp

from core.download.http_client import RETRYABLE_STATUSES
from core.errors.exceptions import ErrorCategory, classify_http_status, classify_os_error

logger = logging.getLogger(__name__)

# Range download constants
RANGE_CHUNK_SIZE = 20 * 1024 * 1024  # 20MB per range request
LARGE_FILE_THRESHOLD = 50 * 1024 * 1024  # 50MB triggers range-based download
CHUNK_TIMEOUT = 120  # 2 min total per chunk
CHUNK_SOCK_READ = 60  # 60s socket read per chunk
CHUNK_MAX_RETRIES = 3  # retries per chunk
DISK_WRITE_SIZE = 8 * 1024 * 1024  # 8MB iter_chunked for disk writes


@dataclass
class RangeDownloadResult:
    """Successful range download result."""

    bytes_written: int
    content_type: str | None
    chunks_completed: int
    total_chunks: int


@dataclass
class RangeDownloadError:
    """Failed range download result."""

    error_message: str
    error_category: ErrorCategory
    bytes_written_so_far: int
    status_code: int | None = None


async def download_file_in_ranges(
    url: str,
    output_path: Path,
    session: aiohttp.ClientSession,
    total_size: int,
    chunk_size: int = RANGE_CHUNK_SIZE,
    chunk_timeout: int = CHUNK_TIMEOUT,
    chunk_sock_read: int = CHUNK_SOCK_READ,
    resume_from_bytes: int = 0,
) -> tuple[RangeDownloadResult | None, RangeDownloadError | None]:
    """
    Download a file using HTTP Range requests.

    Each chunk is independently retryable. Writes in append mode so partial
    files can be resumed on subsequent Kafka-level retries.

    Args:
        url: Download URL (must support Range requests)
        output_path: Where to write the file
        session: aiohttp ClientSession (caller manages lifecycle)
        total_size: Total file size from HEAD Content-Length
        chunk_size: Bytes per range request (default 20MB)
        chunk_timeout: Total timeout per chunk request (default 120s)
        chunk_sock_read: Socket read timeout per chunk (default 60s)
        resume_from_bytes: Resume from this byte offset (0 = fresh download)

    Returns:
        (RangeDownloadResult, None) on success
        (None, RangeDownloadError) on failure
    """
    # Calculate chunks
    start_byte = resume_from_bytes
    total_chunks = _count_chunks(resume_from_bytes, total_size, chunk_size)
    chunks_completed = 0
    bytes_written = resume_from_bytes
    content_type = None

    # Ensure parent dir exists
    await asyncio.to_thread(output_path.parent.mkdir, parents=True, exist_ok=True)

    # Open in append mode if resuming, write mode if fresh
    file_mode = "ab" if resume_from_bytes > 0 else "wb"

    logger.info(
        "Starting range-based download",
        extra={
            "url": url[:120],
            "total_size": total_size,
            "chunk_size": chunk_size,
            "total_chunks": total_chunks,
            "resume_from_bytes": resume_from_bytes,
            "file_mode": file_mode,
        },
    )

    try:
        with open(output_path, file_mode) as f:
            while start_byte < total_size:
                end_byte = min(start_byte + chunk_size - 1, total_size - 1)

                chunk_data, chunk_ct, error = await _download_chunk(
                    url=url,
                    session=session,
                    start_byte=start_byte,
                    end_byte=end_byte,
                    timeout=chunk_timeout,
                    sock_read=chunk_sock_read,
                )

                if error is not None:
                    return None, RangeDownloadError(
                        error_message=error.error_message,
                        error_category=error.error_category,
                        bytes_written_so_far=bytes_written,
                        status_code=error.status_code,
                    )

                await asyncio.to_thread(f.write, chunk_data)
                await asyncio.to_thread(f.flush)

                chunk_len = len(chunk_data)
                bytes_written += chunk_len
                start_byte = end_byte + 1
                chunks_completed += 1
                if chunk_ct:
                    content_type = chunk_ct

                logger.debug(
                    "Range chunk completed",
                    extra={
                        "chunk": chunks_completed,
                        "total_chunks": total_chunks,
                        "chunk_bytes": chunk_len,
                        "total_bytes_written": bytes_written,
                    },
                )

    except OSError as e:
        return None, RangeDownloadError(
            error_message=f"File write error: {e}",
            error_category=classify_os_error(e),
            bytes_written_so_far=bytes_written,
        )

    logger.info(
        "Range-based download completed",
        extra={
            "bytes_written": bytes_written,
            "chunks_completed": chunks_completed,
            "total_chunks": total_chunks,
            "content_type": content_type,
        },
    )

    return (
        RangeDownloadResult(
            bytes_written=bytes_written,
            content_type=content_type,
            chunks_completed=chunks_completed,
            total_chunks=total_chunks,
        ),
        None,
    )


@dataclass
class _ChunkError:
    """Internal: error from a single chunk download attempt."""

    error_message: str
    error_category: ErrorCategory
    status_code: int | None = None


async def _download_chunk(
    url: str,
    session: aiohttp.ClientSession,
    start_byte: int,
    end_byte: int,
    timeout: int,
    sock_read: int,
) -> tuple[bytes, str | None, _ChunkError | None]:
    """
    Download a single byte range with retries.

    Returns (data, content_type, error). On success error is None.
    On failure data is b"" and error describes the problem.
    """
    range_header = f"bytes={start_byte}-{end_byte}"
    last_error = None

    for attempt in range(CHUNK_MAX_RETRIES):
        try:
            async with session.get(
                url,
                headers={"Range": range_header},
                timeout=aiohttp.ClientTimeout(total=timeout, sock_read=sock_read),
                allow_redirects=True,
            ) as response:
                if response.status == 200:
                    # Server ignores Range header — caller should fall back
                    return b"", None, _ChunkError(
                        error_message="Server returned 200 instead of 206 — Range not supported",
                        error_category=ErrorCategory.PERMANENT,
                        status_code=200,
                    )

                if response.status != 206:
                    category = classify_http_status(response.status)
                    last_error = _ChunkError(
                        error_message=f"HTTP {response.status} for range {range_header}",
                        error_category=category,
                        status_code=response.status,
                    )
                    if response.status not in RETRYABLE_STATUSES:
                        return b"", None, last_error
                    if attempt < CHUNK_MAX_RETRIES - 1:
                        await _backoff(attempt)
                        continue
                    return b"", None, last_error

                # Read chunk data
                content_type = response.headers.get("Content-Type")
                data = await response.read()
                return data, content_type, None

        except (TimeoutError, aiohttp.ServerTimeoutError) as e:
            last_error = _ChunkError(
                error_message=f"Timeout on range {range_header}: {e}",
                error_category=ErrorCategory.TRANSIENT,
            )
        except aiohttp.ClientError as e:
            last_error = _ChunkError(
                error_message=f"Connection error on range {range_header}: {e}",
                error_category=ErrorCategory.TRANSIENT,
            )

        if attempt < CHUNK_MAX_RETRIES - 1:
            await _backoff(attempt)

    return b"", None, last_error


async def _backoff(attempt: int) -> None:
    """Jittered exponential backoff — same pattern as http_client.py."""
    delay = min(2, 0.5 * 2**attempt) + random.random()
    await asyncio.sleep(delay)


def _count_chunks(resume_from: int, total_size: int, chunk_size: int) -> int:
    """Calculate number of chunks needed from resume_from to total_size."""
    remaining = total_size - resume_from
    if remaining <= 0:
        return 0
    return (remaining + chunk_size - 1) // chunk_size


__all__ = [
    "LARGE_FILE_THRESHOLD",
    "RANGE_CHUNK_SIZE",
    "RangeDownloadResult",
    "RangeDownloadError",
    "download_file_in_ranges",
]
