"""
Core HTTP download client using aiohttp.

Provides basic async HTTP download functionality without domain-specific
coupling. Handles timeouts, connection pooling, SSL verification, and
error classification.

This module is extracted from xact_download.py to be reusable across
different pipeline components.
"""

import asyncio
from dataclasses import dataclass
from typing import Optional

import aiohttp

from core.errors.exceptions import (
    ConnectionError as PipelineConnectionError,
    ErrorCategory,
    TimeoutError as PipelineTimeoutError,
    classify_http_status,
)


@dataclass
class DownloadResponse:
    """
    Response from HTTP download operation.

    Attributes:
        content: Downloaded bytes
        status_code: HTTP status code
        content_length: Size in bytes (from Content-Length header)
        content_type: MIME type (from Content-Type header)
    """

    content: bytes
    status_code: int
    content_length: Optional[int] = None
    content_type: Optional[str] = None


@dataclass
class DownloadError:
    """
    Error result from failed HTTP download.

    Attributes:
        status_code: HTTP status code if received
        error_message: Error description
        error_category: Classification for retry decisions
    """

    status_code: Optional[int]
    error_message: str
    error_category: ErrorCategory


async def download_url(
    url: str,
    session: aiohttp.ClientSession,
    timeout: int = 60,
    allow_redirects: bool = True,
    sock_read_timeout: int = 30,
) -> tuple[Optional[DownloadResponse], Optional[DownloadError]]:
    """
    Download content from URL using async HTTP.

    This is a low-level HTTP download function that returns either success
    or error. It does NOT perform:
    - URL validation (caller's responsibility)
    - Circuit breaker checks (higher-level concern)
    - Retry logic (higher-level concern)

    Args:
        url: URL to download
        session: aiohttp ClientSession (caller manages lifecycle)
        timeout: Timeout in seconds (default: 60)
        allow_redirects: Whether to follow redirects (default: True for S3 presigned URLs)
        sock_read_timeout: Timeout for individual socket read operations in seconds
            (default: 30). Prevents hanging on stalled connections where the server
            stops sending data but keeps the connection open.

    Returns:
        Tuple of (DownloadResponse, None) on success
        or (None, DownloadError) on failure

    Example:
        async with aiohttp.ClientSession() as session:
            response, error = await download_url(
                "https://example.com/file.pdf",
                session,
                timeout=30
            )
            if error:
                # Handle error
                if error.error_category == ErrorCategory.TRANSIENT:
                    # Retry logic
                    pass
            else:
                # Process response.content
                pass
    """
    try:
        # sock_read timeout ensures we don't hang on stalled connections
        # where the server stops sending data mid-transfer
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=timeout, sock_read=sock_read_timeout),
            allow_redirects=allow_redirects,
        ) as response:
            # Check status code
            if response.status != 200:
                error_category = classify_http_status(response.status)

                return None, DownloadError(
                    status_code=response.status,
                    error_message=f"HTTP {response.status}",
                    error_category=error_category,
                )

            # Read content
            content = await response.read()

            # Extract metadata
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

    except asyncio.TimeoutError as e:
        return None, DownloadError(
            status_code=None,
            error_message=f"Download timeout after {timeout}s",
            error_category=ErrorCategory.TRANSIENT,
        )

    except aiohttp.ClientError as e:
        # Connection errors, DNS failures, etc.
        return None, DownloadError(
            status_code=None,
            error_message=f"Connection error: {str(e)}",
            error_category=ErrorCategory.TRANSIENT,
        )


def create_session(
    max_connections: int = 100,
    max_connections_per_host: int = 10,
    enable_ssl: bool = True,
) -> aiohttp.ClientSession:
    """
    Create aiohttp ClientSession with optimized connection pooling.

    Connection pool configuration balances performance and resource usage:
    - max_connections: Total concurrent connections across all hosts
    - max_connections_per_host: Concurrent connections to single host
    - SSL verification: Always enabled for security (can disable for testing)

    Args:
        max_connections: Total connection pool size (default: 100)
        max_connections_per_host: Per-host connection limit (default: 10)
        enable_ssl: Enable SSL verification (default: True)

    Returns:
        Configured aiohttp.ClientSession

    Example:
        session = create_session(max_connections=50)
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
    )

    return aiohttp.ClientSession(connector=connector)


__all__ = [
    "DownloadResponse",
    "DownloadError",
    "download_url",
    "create_session",
]
