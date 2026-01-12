"""
Tests for streaming download functionality.

Tests chunked streaming for large files, memory bounds, and file I/O.
"""

import asyncio
import errno
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import aiohttp
import pytest

from core.download.streaming import (
    CHUNK_SIZE,
    STREAM_THRESHOLD,
    StreamDownloadError,
    StreamDownloadResponse,
    download_to_file,
    should_stream,
    stream_download_url,
)
from core.errors.exceptions import ErrorCategory


@pytest.fixture
def mock_session():
    """Create mock aiohttp ClientSession."""
    return Mock(spec=aiohttp.ClientSession)


@pytest.fixture
def mock_response():
    """Create mock aiohttp ClientResponse."""
    response = Mock()
    response.status = 200
    response.content_length = 100 * 1024 * 1024  # 100MB
    response.headers = {"Content-Type": "application/pdf"}
    return response


@pytest.mark.asyncio
async def test_stream_download_url_success(mock_session, mock_response):
    """Test successful streaming download with chunks."""
    # Setup mock chunks
    chunks = [b"chunk1", b"chunk2", b"chunk3"]

    async def mock_iter_chunked(chunk_size):
        for chunk in chunks:
            yield chunk

    mock_response.content = Mock()
    mock_response.content.iter_chunked = mock_iter_chunked

    # Setup context manager
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_response)
    mock_ctx.__aexit__ = AsyncMock(return_value=None)
    mock_session.get = Mock(return_value=mock_ctx)

    # Execute
    result, error = await stream_download_url(
        "https://example.com/file.pdf",
        mock_session,
        timeout=60,
        chunk_size=CHUNK_SIZE,
    )

    # Verify
    assert error is None
    assert result is not None
    assert result.status_code == 200
    assert result.content_length == 100 * 1024 * 1024
    assert result.content_type == "application/pdf"

    # Consume iterator and verify chunks
    collected_chunks = []
    async for chunk in result.chunk_iterator:
        collected_chunks.append(chunk)

    assert collected_chunks == chunks

    # Verify session.get was called correctly
    mock_session.get.assert_called_once()
    call_args = mock_session.get.call_args
    assert call_args[0][0] == "https://example.com/file.pdf"
    assert call_args[1]["allow_redirects"] is True  # Default is True for S3 presigned URLs


@pytest.mark.asyncio
async def test_stream_download_url_http_error(mock_session, mock_response):
    """Test streaming download with HTTP error status."""
    mock_response.status = 404

    # Setup context manager
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_response)
    mock_ctx.__aexit__ = AsyncMock(return_value=None)
    mock_session.get = Mock(return_value=mock_ctx)

    # Execute
    result, error = await stream_download_url(
        "https://example.com/notfound.pdf",
        mock_session,
    )

    # Verify
    assert result is None
    assert error is not None
    assert error.status_code == 404
    assert error.error_message == "HTTP 404"
    assert error.error_category == ErrorCategory.PERMANENT

    # Verify context was cleaned up
    mock_ctx.__aexit__.assert_called_once()


@pytest.mark.asyncio
async def test_stream_download_url_timeout(mock_session):
    """Test streaming download with timeout."""
    # Setup timeout error
    mock_session.get.side_effect = asyncio.TimeoutError()

    # Execute
    result, error = await stream_download_url(
        "https://example.com/file.pdf",
        mock_session,
        timeout=30,
    )

    # Verify
    assert result is None
    assert error is not None
    assert error.status_code is None
    assert "timeout after 30s" in error.error_message.lower()
    assert error.error_category == ErrorCategory.TRANSIENT


@pytest.mark.asyncio
async def test_stream_download_url_connection_error(mock_session):
    """Test streaming download with connection error."""
    # Setup connection error
    mock_session.get.side_effect = aiohttp.ClientConnectionError("Connection refused")

    # Execute
    result, error = await stream_download_url(
        "https://example.com/file.pdf",
        mock_session,
    )

    # Verify
    assert result is None
    assert error is not None
    assert error.status_code is None
    assert "connection error" in error.error_message.lower()
    assert error.error_category == ErrorCategory.TRANSIENT


@pytest.mark.asyncio
async def test_stream_download_url_custom_chunk_size(mock_session, mock_response):
    """Test streaming download with custom chunk size."""
    chunks = [b"a" * 1024, b"b" * 1024]

    async def mock_iter_chunked(chunk_size):
        # Verify chunk size was passed correctly
        assert chunk_size == 1024
        for chunk in chunks:
            yield chunk

    mock_response.content = Mock()
    mock_response.content.iter_chunked = mock_iter_chunked

    # Setup context manager
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_response)
    mock_ctx.__aexit__ = AsyncMock(return_value=None)
    mock_session.get = Mock(return_value=mock_ctx)

    # Execute with custom chunk size
    result, error = await stream_download_url(
        "https://example.com/file.pdf",
        mock_session,
        chunk_size=1024,
    )

    # Verify
    assert error is None
    assert result is not None

    # Consume iterator
    collected_chunks = []
    async for chunk in result.chunk_iterator:
        collected_chunks.append(chunk)

    assert collected_chunks == chunks


@pytest.mark.asyncio
async def test_download_to_file_success(mock_session, mock_response, tmp_path):
    """Test download directly to file."""
    output_path = tmp_path / "output.pdf"
    chunks = [b"chunk1", b"chunk2", b"chunk3"]
    expected_content = b"".join(chunks)

    async def mock_iter_chunked(chunk_size):
        for chunk in chunks:
            yield chunk

    mock_response.content = Mock()
    mock_response.content.iter_chunked = mock_iter_chunked

    # Setup context manager
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_response)
    mock_ctx.__aexit__ = AsyncMock(return_value=None)
    mock_session.get = Mock(return_value=mock_ctx)

    # Execute
    result, error = await download_to_file(
        "https://example.com/file.pdf",
        output_path,
        mock_session,
    )

    # Verify
    assert error is None
    assert result.bytes_written == len(expected_content)
    assert result.content_type == "application/pdf"
    assert output_path.exists()
    assert output_path.read_bytes() == expected_content


@pytest.mark.asyncio
async def test_download_to_file_stream_error(mock_session, tmp_path):
    """Test download to file with stream error."""
    output_path = tmp_path / "output.pdf"

    # Setup error
    mock_session.get.side_effect = aiohttp.ClientConnectionError("Connection failed")

    # Execute
    bytes_written, error = await download_to_file(
        "https://example.com/file.pdf",
        output_path,
        mock_session,
    )

    # Verify
    assert bytes_written is None
    assert error is not None
    assert error.error_category == ErrorCategory.TRANSIENT
    assert not output_path.exists()


@pytest.mark.asyncio
async def test_download_to_file_write_error(mock_session, mock_response, tmp_path):
    """Test download to file with disk write error."""
    output_path = tmp_path / "output.pdf"

    chunks = [b"chunk1"]

    async def mock_iter_chunked(chunk_size):
        for chunk in chunks:
            yield chunk

    mock_response.content = Mock()
    mock_response.content.iter_chunked = mock_iter_chunked

    # Setup context manager
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_response)
    mock_ctx.__aexit__ = AsyncMock(return_value=None)
    mock_session.get = Mock(return_value=mock_ctx)

    # Mock open to raise OSError when writing
    import builtins
    original_open = builtins.open

    def mock_open_error(*args, **kwargs):
        if args[0] == output_path and 'wb' in args:
            raise OSError(errno.ENOSPC, "No space left on device")
        return original_open(*args, **kwargs)

    with patch("builtins.open", side_effect=mock_open_error):
        # Execute
        result, error = await download_to_file(
            "https://example.com/file.pdf",
            output_path,
            mock_session,
        )

    # Verify
    assert result is None
    assert error is not None
    assert "write error" in error.error_message.lower()
    assert error.error_category == ErrorCategory.PERMANENT


@pytest.mark.asyncio
async def test_download_to_file_socket_timeout_error(mock_session, mock_response, tmp_path):
    """Test that socket timeout errors during streaming are classified as transient."""
    output_path = tmp_path / "output.pdf"

    chunks = [b"chunk1"]

    async def mock_iter_chunked(chunk_size):
        for chunk in chunks:
            yield chunk

    mock_response.content = Mock()
    mock_response.content.iter_chunked = mock_iter_chunked

    # Setup context manager
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_response)
    mock_ctx.__aexit__ = AsyncMock(return_value=None)
    mock_session.get = Mock(return_value=mock_ctx)

    # Mock open to raise OSError with socket timeout message
    import builtins
    original_open = builtins.open

    def mock_open_error(*args, **kwargs):
        if args[0] == output_path and 'wb' in args:
            raise OSError("Timeout on reading data from socket")
        return original_open(*args, **kwargs)

    with patch("builtins.open", side_effect=mock_open_error):
        # Execute
        result, error = await download_to_file(
            "https://example.com/file.pdf",
            output_path,
            mock_session,
        )

    # Verify
    assert result is None
    assert error is not None
    assert "write error" in error.error_message.lower()
    assert "timeout" in error.error_message.lower()
    # Socket timeout should be classified as TRANSIENT, not PERMANENT
    assert error.error_category == ErrorCategory.TRANSIENT


@pytest.mark.asyncio
async def test_download_to_file_unknown_error(mock_session, mock_response, tmp_path):
    """Test that unknown errors during streaming are classified as transient (conservative)."""
    output_path = tmp_path / "output.pdf"

    chunks = [b"chunk1"]

    async def mock_iter_chunked(chunk_size):
        for chunk in chunks:
            yield chunk

    mock_response.content = Mock()
    mock_response.content.iter_chunked = mock_iter_chunked

    # Setup context manager
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_response)
    mock_ctx.__aexit__ = AsyncMock(return_value=None)
    mock_session.get = Mock(return_value=mock_ctx)

    # Mock open to raise OSError with no errno (unknown error)
    import builtins
    original_open = builtins.open

    def mock_open_error(*args, **kwargs):
        if args[0] == output_path and 'wb' in args:
            raise OSError()  # No errno, no message
        return original_open(*args, **kwargs)

    with patch("builtins.open", side_effect=mock_open_error):
        # Execute
        result, error = await download_to_file(
            "https://example.com/file.pdf",
            output_path,
            mock_session,
        )

    # Verify
    assert result is None
    assert error is not None
    assert "write error" in error.error_message.lower()
    # Unknown errors should be classified as TRANSIENT to allow retry
    assert error.error_category == ErrorCategory.TRANSIENT


def test_should_stream_large_file():
    """Test should_stream returns True for files > 50MB."""
    assert should_stream(100 * 1024 * 1024) is True  # 100MB
    assert should_stream(51 * 1024 * 1024) is True  # 51MB


def test_should_stream_small_file():
    """Test should_stream returns False for files < 50MB."""
    assert should_stream(10 * 1024 * 1024) is False  # 10MB
    assert should_stream(1024) is False  # 1KB


def test_should_stream_threshold_boundary():
    """Test should_stream at exact threshold."""
    # Exactly at threshold - should not stream
    assert should_stream(STREAM_THRESHOLD) is False

    # Just above threshold - should stream
    assert should_stream(STREAM_THRESHOLD + 1) is True


def test_should_stream_unknown_size():
    """Test should_stream with unknown content length."""
    # Unknown size - should stream to be safe
    assert should_stream(None) is True


@pytest.mark.asyncio
async def test_stream_download_url_no_content_length(mock_session, mock_response):
    """Test streaming download with missing Content-Length header."""
    mock_response.content_length = None
    chunks = [b"data"]

    async def mock_iter_chunked(chunk_size):
        for chunk in chunks:
            yield chunk

    mock_response.content = Mock()
    mock_response.content.iter_chunked = mock_iter_chunked

    # Setup context manager
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_response)
    mock_ctx.__aexit__ = AsyncMock(return_value=None)
    mock_session.get = Mock(return_value=mock_ctx)

    # Execute
    result, error = await stream_download_url(
        "https://example.com/file.pdf",
        mock_session,
    )

    # Verify
    assert error is None
    assert result is not None
    assert result.content_length is None


@pytest.mark.asyncio
async def test_stream_download_url_no_content_type(mock_session, mock_response):
    """Test streaming download with missing Content-Type header."""
    mock_response.headers = {}
    chunks = [b"data"]

    async def mock_iter_chunked(chunk_size):
        for chunk in chunks:
            yield chunk

    mock_response.content = Mock()
    mock_response.content.iter_chunked = mock_iter_chunked

    # Setup context manager
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_response)
    mock_ctx.__aexit__ = AsyncMock(return_value=None)
    mock_session.get = Mock(return_value=mock_ctx)

    # Execute
    result, error = await stream_download_url(
        "https://example.com/file.pdf",
        mock_session,
    )

    # Verify
    assert error is None
    assert result is not None
    assert result.content_type is None


@pytest.mark.asyncio
async def test_stream_download_url_redirects_disabled(mock_session, mock_response):
    """Test that redirects are disabled by default."""
    chunks = [b"data"]

    async def mock_iter_chunked(chunk_size):
        for chunk in chunks:
            yield chunk

    mock_response.content = Mock()
    mock_response.content.iter_chunked = mock_iter_chunked

    # Setup context manager
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_response)
    mock_ctx.__aexit__ = AsyncMock(return_value=None)
    mock_session.get = Mock(return_value=mock_ctx)

    # Execute
    result, error = await stream_download_url(
        "https://example.com/file.pdf",
        mock_session,
        allow_redirects=False,
    )

    # Verify redirects setting was passed
    call_args = mock_session.get.call_args
    assert call_args[1]["allow_redirects"] is False
