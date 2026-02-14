"""
Core HTTP download client using aiohttp.

Provides basic async HTTP download functionality without domain-specific
coupling. Handles timeouts, connection pooling, SSL verification, and
error classification.

This module is extracted from xact_download.py to be reusable across
different pipeline components.
"""

import asyncio
import random
from dataclasses import dataclass

import aiohttp

from core.errors.exceptions import (
    ErrorCategory,
    classify_http_status,
)

RETRYABLE_STATUSES = {429, 500, 502, 503, 504}


@dataclass
class DownloadResponse:
    """Response from HTTP download operation with content and metadata."""

    content: bytes
    status_code: int
    content_length: int | None = None
    content_type: str | None = None


@dataclass
class DownloadError:
    """Error result from failed HTTP download with retry classification."""

    status_code: int | None
    error_message: str
    error_category: ErrorCategory


async def download_url(
    url: str,
    session: aiohttp.ClientSession,
    timeout: int = 60,
    allow_redirects: bool = True,
    sock_read_timeout: int = 30,
) -> tuple[DownloadResponse | None, DownloadError | None]:
    """
    Low-level HTTP download without URL validation, circuit breaking, or retry.

    Args:
        url: URL to download
        session: aiohttp ClientSession (caller manages lifecycle)
        timeout: Total timeout in seconds
        allow_redirects: Whether to follow redirects (needed for S3 presigned URLs)
        sock_read_timeout: Socket read timeout to prevent hanging on stalled connections

    Returns:
        Tuple of (DownloadResponse, None) on success or (None, DownloadError)
            if error:
                # Handle error
                if error.error_category == ErrorCategory.TRANSIENT:
                    # Retry logic
                    pass
            else:
                # Process response.content
                pass
    """
    max_attempts = 3
    last_error = None

    for attempt in range(max_attempts):
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=timeout, sock_read=sock_read_timeout),
                allow_redirects=allow_redirects,
            ) as response:
                if response.status != 200:
                    error_category = classify_http_status(response.status)
                    last_error = DownloadError(
                        status_code=response.status,
                        error_message=f"HTTP {response.status}",
                        error_category=error_category,
                    )
                    if response.status not in RETRYABLE_STATUSES:
                        return None, last_error
                    if attempt < max_attempts - 1:
                        await asyncio.sleep(min(2, 0.5 * 2**attempt) + random.random())
                        continue
                    return None, last_error

                content = await response.read()
                content_length = response.content_length
                content_type = response.headers.get("Content-Type")

                return (
                    DownloadResponse(
                        content=content,
                        status_code=response.status,
                        content_length=content_length,
                        content_type=content_type,
                    ),
                    None,
                )

        except (TimeoutError, aiohttp.ServerTimeoutError, aiohttp.ClientError) as e:
            if isinstance(e, TimeoutError):
                last_error = DownloadError(
                    status_code=None,
                    error_message=f"Download timeout after {timeout}s",
                    error_category=ErrorCategory.TRANSIENT,
                )
            elif isinstance(e, aiohttp.ServerTimeoutError):
                last_error = DownloadError(
                    status_code=None,
                    error_message=f"Server timeout: {str(e)}",
                    error_category=ErrorCategory.TRANSIENT,
                )
            else:
                last_error = DownloadError(
                    status_code=None,
                    error_message=f"Connection error: {str(e)}",
                    error_category=ErrorCategory.TRANSIENT,
                )
            if attempt < max_attempts - 1:
                await asyncio.sleep(min(2, 0.5 * 2**attempt) + random.random())
                continue

    return None, last_error


def create_session(
    max_connections: int = 100,
    max_connections_per_host: int = 10,
    enable_ssl: bool = True,
    timeout_total: int = 300,
    timeout_connect: int = 30,
    timeout_sock_read: int = 60,
    timeout_sock_connect: int = 30,
) -> aiohttp.ClientSession:
    """
    Create aiohttp ClientSession with optimized connection pooling and timeouts.

    Connection pool configuration balances performance and resource usage:
    - max_connections: Total concurrent connections across all hosts
    - max_connections_per_host: Concurrent connections to single host
    - SSL verification: Always enabled for security (can disable for testing)

    Timeout configuration prevents indefinite hangs:
    - timeout_total: Total time for the entire request (default: 300s)
    - timeout_connect: Time to establish connection (default: 30s)
    - timeout_sock_read: Time between reads (default: 60s)
    - timeout_sock_connect: Socket connection timeout (default: 30s)

    Args:
        max_connections: Total connection pool size (default: 100)
        max_connections_per_host: Per-host connection limit (default: 10)
        enable_ssl: Enable SSL verification (default: True)
        timeout_total: Total timeout in seconds (default: 300)
        timeout_connect: Connection timeout in seconds (default: 30)
        timeout_sock_read: Socket read timeout in seconds (default: 60)
        timeout_sock_connect: Socket connection timeout in seconds (default: 30)

    Returns:
        Configured aiohttp.ClientSession

    Example:
        session = create_session(max_connections=50, timeout_total=180)
        try:
            response, error = await download_url(url, session)
            # ... handle response
        finally:
            await session.close()

    Note:
        Caller is responsible for session lifecycle management.
        Use async context manager for automatic cleanup:

        async with create_session() as session:
            response, error = await download_url(url, session)
    """
    connector = aiohttp.TCPConnector(
        limit=max_connections,
        limit_per_host=max_connections_per_host,
        ssl=enable_ssl,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )

    timeout = aiohttp.ClientTimeout(
        total=timeout_total,
        connect=timeout_connect,
        sock_read=timeout_sock_read,
        sock_connect=timeout_sock_connect,
    )

    return aiohttp.ClientSession(connector=connector, timeout=timeout)


__all__ = [
    "DownloadResponse",
    "DownloadError",
    "download_url",
    "create_session",
]
