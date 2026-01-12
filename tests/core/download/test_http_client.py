"""
Tests for core.download.http_client module.

Tests cover:
- Successful HTTP downloads
- HTTP error status codes (4xx, 5xx)
- Timeout scenarios
- Connection errors
- Session creation and configuration
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from core.download.http_client import (
    DownloadError,
    DownloadResponse,
    create_session,
    download_url,
)
from core.errors.exceptions import ErrorCategory


class TestDownloadUrl:
    """Tests for download_url function."""

    @pytest.mark.asyncio
    async def test_successful_download(self):
        """Test successful file download returns content and metadata."""
        # Mock aiohttp response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.content_length = 1024
        mock_response.headers = {"Content-Type": "application/pdf"}
        mock_response.read = AsyncMock(return_value=b"file content here")
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        # Mock session
        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)

        # Execute download
        response, error = await download_url(
            "https://example.com/file.pdf", mock_session, timeout=30
        )

        # Verify success
        assert error is None
        assert response is not None
        assert response.content == b"file content here"
        assert response.status_code == 200
        assert response.content_length == 1024
        assert response.content_type == "application/pdf"

        # Verify session.get called correctly
        mock_session.get.assert_called_once()
        call_args = mock_session.get.call_args
        assert call_args[0][0] == "https://example.com/file.pdf"
        assert call_args[1]["allow_redirects"] is True  # Default is True for S3 presigned URLs

    @pytest.mark.asyncio
    async def test_http_404_error(self):
        """Test 404 response is classified as PERMANENT."""
        mock_response = AsyncMock()
        mock_response.status = 404
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)

        response, error = await download_url(
            "https://example.com/missing.pdf", mock_session
        )

        assert response is None
        assert error is not None
        assert error.status_code == 404
        assert error.error_category == ErrorCategory.PERMANENT
        assert "404" in error.error_message

    @pytest.mark.asyncio
    async def test_http_500_error(self):
        """Test 500 response is classified as TRANSIENT."""
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)

        response, error = await download_url(
            "https://example.com/error.pdf", mock_session
        )

        assert response is None
        assert error is not None
        assert error.status_code == 500
        assert error.error_category == ErrorCategory.TRANSIENT
        assert "500" in error.error_message

    @pytest.mark.asyncio
    async def test_http_429_rate_limit(self):
        """Test 429 response is classified as TRANSIENT."""
        mock_response = AsyncMock()
        mock_response.status = 429
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)

        response, error = await download_url(
            "https://example.com/rate-limited.pdf", mock_session
        )

        assert response is None
        assert error is not None
        assert error.status_code == 429
        assert error.error_category == ErrorCategory.TRANSIENT

    @pytest.mark.asyncio
    async def test_http_401_auth_error(self):
        """Test 401 response is classified as AUTH."""
        mock_response = AsyncMock()
        mock_response.status = 401
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)

        response, error = await download_url(
            "https://example.com/protected.pdf", mock_session
        )

        assert response is None
        assert error is not None
        assert error.status_code == 401
        assert error.error_category == ErrorCategory.AUTH

    @pytest.mark.asyncio
    async def test_timeout_error(self):
        """Test timeout is classified as TRANSIENT."""
        mock_session = AsyncMock()
        mock_session.get = MagicMock(side_effect=asyncio.TimeoutError("Connection timeout"))

        response, error = await download_url(
            "https://example.com/slow.pdf", mock_session, timeout=5
        )

        assert response is None
        assert error is not None
        assert error.status_code is None
        assert error.error_category == ErrorCategory.TRANSIENT
        assert "timeout" in error.error_message.lower()

    @pytest.mark.asyncio
    async def test_connection_error(self):
        """Test connection error is classified as TRANSIENT."""
        mock_session = AsyncMock()
        mock_session.get = MagicMock(side_effect=aiohttp.ClientConnectionError("Connection refused"))

        response, error = await download_url(
            "https://unreachable.com/file.pdf", mock_session
        )

        assert response is None
        assert error is not None
        assert error.status_code is None
        assert error.error_category == ErrorCategory.TRANSIENT
        assert "connection error" in error.error_message.lower()

    @pytest.mark.asyncio
    async def test_allow_redirects_parameter(self):
        """Test allow_redirects parameter is passed correctly."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.read = AsyncMock(return_value=b"content")
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)

        # Test with redirects enabled
        await download_url(
            "https://example.com/file.pdf", mock_session, allow_redirects=True
        )

        call_args = mock_session.get.call_args
        assert call_args[1]["allow_redirects"] is True

    @pytest.mark.asyncio
    async def test_timeout_parameter(self):
        """Test timeout parameter is passed to aiohttp."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.read = AsyncMock(return_value=b"content")
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)

        await download_url("https://example.com/file.pdf", mock_session, timeout=120)

        call_args = mock_session.get.call_args
        timeout_arg = call_args[1]["timeout"]
        assert isinstance(timeout_arg, aiohttp.ClientTimeout)


class TestCreateSession:
    """Tests for create_session function."""

    @pytest.mark.asyncio
    async def test_create_session_default_config(self):
        """Test session created with default configuration."""
        session = create_session()

        assert isinstance(session, aiohttp.ClientSession)
        assert session.connector is not None
        assert isinstance(session.connector, aiohttp.TCPConnector)

        # Verify connector settings
        connector = session.connector
        assert connector._limit == 100  # max_connections
        assert connector._limit_per_host == 10  # max_connections_per_host

        # Cleanup
        await session.close()

    @pytest.mark.asyncio
    async def test_create_session_custom_config(self):
        """Test session created with custom configuration."""
        session = create_session(
            max_connections=50, max_connections_per_host=5, enable_ssl=True
        )

        assert isinstance(session, aiohttp.ClientSession)

        connector = session.connector
        assert connector._limit == 50
        assert connector._limit_per_host == 5

        # Cleanup
        await session.close()

    @pytest.mark.asyncio
    async def test_create_session_ssl_disabled(self):
        """Test session can disable SSL verification (for testing)."""
        session = create_session(enable_ssl=False)

        assert isinstance(session, aiohttp.ClientSession)

        # SSL disabled means connector.ssl should be False
        connector = session.connector
        assert connector._ssl is False

        # Cleanup
        await session.close()


class TestIntegration:
    """Integration tests with real aiohttp session (no mocks)."""

    @pytest.mark.asyncio
    async def test_session_lifecycle(self):
        """Test proper session lifecycle management."""
        session = create_session(max_connections=10)

        try:
            assert not session.closed
        finally:
            await session.close()
            assert session.closed

    @pytest.mark.asyncio
    async def test_session_context_manager(self):
        """Test session works with async context manager."""
        async with create_session() as session:
            assert not session.closed
            assert isinstance(session, aiohttp.ClientSession)

        # Session should be closed after context exit
        assert session.closed
