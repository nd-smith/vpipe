"""
Unit tests for ClaimX API Client.

Tests the ClaimXApiClient with mocked HTTP responses including:
- Error classification
- Circuit breaker integration
- Rate limiting
- All endpoint methods
- Session management
- Retry and error handling
"""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock
from typing import Any, Dict

import aiohttp
import pytest

from kafka_pipeline.claimx.api_client import (
    ClaimXApiClient,
    ClaimXApiError,
    classify_api_error,
)
from kafka_pipeline.common.exceptions import ErrorCategory


# ============================================================================
# Test classify_api_error function
# ============================================================================


class TestClassifyApiError:
    """Test error classification logic."""

    def test_classify_401_unauthorized(self):
        """Test 401 returns AUTH error, not retryable (static credentials)."""
        error = classify_api_error(401, "https://api.test/project/123")
        assert error.status_code == 401
        assert error.category == ErrorCategory.AUTH
        assert error.is_retryable is False  # Static credentials - not retryable without changes
        assert error.should_refresh_auth is False  # No credential refresh for static tokens
        assert "Unauthorized" in str(error)

    def test_classify_403_forbidden(self):
        """Test 403 returns PERMANENT error, not retryable."""
        error = classify_api_error(403, "https://api.test/project/123")
        assert error.status_code == 403
        assert error.category == ErrorCategory.PERMANENT
        assert error.is_retryable is False
        assert "Forbidden" in str(error)

    def test_classify_404_not_found(self):
        """Test 404 returns PERMANENT error, not retryable."""
        error = classify_api_error(404, "https://api.test/project/123")
        assert error.status_code == 404
        assert error.category == ErrorCategory.PERMANENT
        assert error.is_retryable is False
        assert "Not found" in str(error)

    def test_classify_429_rate_limit(self):
        """Test 429 returns TRANSIENT error, retryable."""
        error = classify_api_error(429, "https://api.test/project/123")
        assert error.status_code == 429
        assert error.category == ErrorCategory.TRANSIENT
        assert error.is_retryable is True
        assert "Rate limited" in str(error)

    @pytest.mark.parametrize("status", [500, 502, 503, 504])
    def test_classify_5xx_server_errors(self, status):
        """Test 5xx errors return TRANSIENT error, retryable."""
        error = classify_api_error(status, "https://api.test/project/123")
        assert error.status_code == status
        assert error.category == ErrorCategory.TRANSIENT
        assert error.is_retryable is True
        assert "Server error" in str(error)

    @pytest.mark.parametrize("status", [400, 405, 410, 422])
    def test_classify_4xx_client_errors(self, status):
        """Test 4xx (except 401, 403, 404, 429) return PERMANENT error, not retryable."""
        error = classify_api_error(status, "https://api.test/project/123")
        assert error.status_code == status
        assert error.category == ErrorCategory.PERMANENT
        assert error.is_retryable is False
        assert "Client error" in str(error)

    def test_classify_unknown_error(self):
        """Test unknown status codes default to TRANSIENT, retryable."""
        error = classify_api_error(999, "https://api.test/project/123")
        assert error.status_code == 999
        assert error.category == ErrorCategory.TRANSIENT
        assert error.is_retryable is True
        assert "HTTP error" in str(error)


# ============================================================================
# Test ClaimXApiClient
# ============================================================================


@pytest.fixture
def mock_circuit_breaker():
    """Create mock circuit breaker."""
    circuit = MagicMock()
    circuit.is_open = False
    circuit.record_success = MagicMock()
    circuit.record_failure = MagicMock()
    circuit.get_diagnostics = MagicMock(return_value={"state": "closed", "failures": 0})
    circuit._get_retry_after = MagicMock(return_value=60.0)
    return circuit


@pytest.fixture
def api_client(mock_circuit_breaker):
    """Create API client with mocked circuit breaker."""
    with patch(
        "kafka_pipeline.claimx.api_client.get_circuit_breaker",
        return_value=mock_circuit_breaker,
    ):
        client = ClaimXApiClient(
            base_url="https://api.claimx.test/service/cxedirest",
            token="test-token-123",
            timeout_seconds=30,
            max_concurrent=20,
            sender_username="test@example.com",
        )
        return client


class TestClaimXApiClientInit:
    """Test API client initialization."""

    def test_init_sets_config(self, api_client):
        """Test initialization sets configuration properly."""
        assert api_client.base_url == "https://api.claimx.test/service/cxedirest"
        # Note: token is stored in _auth_header as "Basic <token>", not as raw token
        assert api_client._auth_header == "Basic test-token-123"
        assert api_client.timeout_seconds == 30
        assert api_client.max_concurrent == 20
        assert api_client.sender_username == "test@example.com"

    def test_init_strips_trailing_slash(self, mock_circuit_breaker):
        """Test initialization strips trailing slash from base_url."""
        with patch(
            "kafka_pipeline.claimx.api_client.get_circuit_breaker",
            return_value=mock_circuit_breaker,
        ):
            client = ClaimXApiClient(
                base_url="https://api.claimx.test/service/cxedirest/",
                token="token",
            )
            assert client.base_url == "https://api.claimx.test/service/cxedirest"

    def test_init_creates_circuit_breaker(self, api_client):
        """Test initialization creates circuit breaker."""
        assert api_client._circuit is not None


class TestClaimXApiClientSessionManagement:
    """Test session lifecycle management."""

    @pytest.mark.asyncio
    async def test_context_manager_creates_session(self, api_client):
        """Test async context manager creates session."""
        assert api_client._session is None

        async with api_client as client:
            assert client._session is not None
            assert not client._session.closed
            assert client._semaphore is not None

        # Session should be closed after context exit
        assert api_client._session is None or api_client._session.closed

    @pytest.mark.asyncio
    async def test_ensure_session_creates_session_once(self, api_client):
        """Test _ensure_session creates session only once."""
        assert api_client._session is None

        await api_client._ensure_session()
        session1 = api_client._session
        assert session1 is not None

        await api_client._ensure_session()
        session2 = api_client._session
        assert session2 is session1  # Same instance

        await api_client.close()

    @pytest.mark.asyncio
    async def test_ensure_session_sets_headers(self, api_client):
        """Test _ensure_session configures session headers (auth per-request)."""
        await api_client._ensure_session()

        assert api_client._session is not None
        headers = api_client._session.headers
        # Note: Authorization header is now set per-request for refresh support
        # Session only has Content-Type and Accept
        assert headers["Content-Type"] == "application/json"
        assert headers["Accept"] == "application/json"
        # Auth header is stored separately for per-request use
        assert api_client._auth_header == "Basic test-token-123"

        await api_client.close()

    @pytest.mark.asyncio
    async def test_close_closes_session(self, api_client):
        """Test close() closes active session."""
        await api_client._ensure_session()
        assert api_client._session is not None

        await api_client.close()
        assert api_client._session is None

    @pytest.mark.asyncio
    async def test_close_handles_no_session(self, api_client):
        """Test close() handles case when no session exists."""
        assert api_client._session is None
        await api_client.close()  # Should not raise
        assert api_client._session is None


class TestClaimXApiClientRequest:
    """Test _request method with various scenarios."""

    @pytest.mark.asyncio
    async def test_request_success(self, api_client, mock_circuit_breaker):
        """Test successful API request."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"data": "success"})
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession.request", return_value=mock_response):
            async with api_client:
                result = await api_client._request("GET", "/export/project/123")

                assert result == {"data": "success"}
                mock_circuit_breaker.record_success.assert_called_once()

    @pytest.mark.asyncio
    async def test_request_circuit_open(self, api_client, mock_circuit_breaker):
        """Test request fails when circuit is open."""
        mock_circuit_breaker.is_open = True

        async with api_client:
            with pytest.raises(ClaimXApiError) as exc_info:
                await api_client._request("GET", "/export/project/123")

            error = exc_info.value
            assert error.category == ErrorCategory.CIRCUIT_OPEN
            assert error.is_retryable is True
            assert "Circuit open" in str(error)

    @pytest.mark.asyncio
    async def test_request_404_not_retryable(self, api_client, mock_circuit_breaker):
        """Test 404 error is classified as not retryable."""
        mock_response = AsyncMock()
        mock_response.status = 404

        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession.request", return_value=mock_response):
            async with api_client:
                with pytest.raises(ClaimXApiError) as exc_info:
                    await api_client._request("GET", "/export/project/999")

                error = exc_info.value
                assert error.status_code == 404
                assert error.category == ErrorCategory.PERMANENT
                assert error.is_retryable is False
                # Should NOT record failure for non-retryable errors
                mock_circuit_breaker.record_failure.assert_not_called()

    @pytest.mark.asyncio
    async def test_request_500_retryable(self, api_client, mock_circuit_breaker):
        """Test 500 error is classified as retryable and records failure."""
        mock_response = AsyncMock()
        mock_response.status = 500

        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession.request", return_value=mock_response):
            async with api_client:
                with pytest.raises(ClaimXApiError) as exc_info:
                    await api_client._request("GET", "/export/project/123")

                error = exc_info.value
                assert error.status_code == 500
                assert error.category == ErrorCategory.TRANSIENT
                assert error.is_retryable is True
                # Should record failure for retryable errors
                mock_circuit_breaker.record_failure.assert_called_once()

    @pytest.mark.asyncio
    async def test_request_timeout(self, api_client, mock_circuit_breaker):
        """Test timeout error is classified as transient."""
        mock_response = AsyncMock()
        mock_response.__aenter__ = AsyncMock(side_effect=asyncio.TimeoutError())
        mock_response.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession.request", return_value=mock_response):
            async with api_client:
                with pytest.raises(ClaimXApiError) as exc_info:
                    await api_client._request("GET", "/export/project/123")

                error = exc_info.value
                assert "Timeout" in str(error)
                assert error.category == ErrorCategory.TRANSIENT
                assert error.is_retryable is True
                mock_circuit_breaker.record_failure.assert_called_once()

    @pytest.mark.asyncio
    async def test_request_connection_error(self, api_client, mock_circuit_breaker):
        """Test connection error is classified as transient."""
        mock_response = AsyncMock()
        mock_response.__aenter__ = AsyncMock(
            side_effect=aiohttp.ClientConnectionError("Connection refused")
        )
        mock_response.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession.request", return_value=mock_response):
            async with api_client:
                with pytest.raises(ClaimXApiError) as exc_info:
                    await api_client._request("GET", "/export/project/123")

                error = exc_info.value
                assert "Connection error" in str(error)
                assert error.category == ErrorCategory.TRANSIENT
                assert error.is_retryable is True
                mock_circuit_breaker.record_failure.assert_called_once()

    @pytest.mark.asyncio
    async def test_request_url_construction(self, api_client):
        """Test request constructs URL correctly."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={})
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession.request", return_value=mock_response) as mock_req:
            async with api_client:
                await api_client._request("GET", "/export/project/123")

                # Check URL was constructed correctly
                call_args = mock_req.call_args
                assert call_args[0][0] == "GET"  # method
                assert (
                    call_args[0][1]
                    == "https://api.claimx.test/service/cxedirest/export/project/123"
                )

    @pytest.mark.asyncio
    async def test_request_with_params(self, api_client):
        """Test request includes query parameters."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={})
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession.request", return_value=mock_response) as mock_req:
            async with api_client:
                await api_client._request(
                    "GET", "/export/project/123/media", params={"mediaIds": "1,2,3"}
                )

                call_kwargs = mock_req.call_args[1]
                assert call_kwargs["params"] == {"mediaIds": "1,2,3"}

    @pytest.mark.asyncio
    async def test_request_with_json_body(self, api_client):
        """Test request includes JSON body for POST."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={})
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession.request", return_value=mock_response) as mock_req:
            async with api_client:
                body = {"reportType": "VIDEO_COLLABORATION", "projectId": 123}
                await api_client._request("POST", "/data", json_body=body)

                call_kwargs = mock_req.call_args[1]
                assert call_kwargs["json"] == body


class TestClaimXApiClientEndpoints:
    """Test all API endpoint methods."""

    @pytest.mark.asyncio
    async def test_get_project(self, api_client):
        """Test get_project endpoint."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={"projectId": 123, "claimNumber": "CLM-123"}
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession.request", return_value=mock_response):
            async with api_client:
                result = await api_client.get_project(123)

                assert result["projectId"] == 123
                assert result["claimNumber"] == "CLM-123"

    @pytest.mark.asyncio
    async def test_get_project_media_list_response(self, api_client):
        """Test get_project_media handles list response."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value=[{"mediaId": 1}, {"mediaId": 2}]
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession.request", return_value=mock_response):
            async with api_client:
                result = await api_client.get_project_media(123)

                assert isinstance(result, list)
                assert len(result) == 2
                assert result[0]["mediaId"] == 1

    @pytest.mark.asyncio
    async def test_get_project_media_dict_with_data_key(self, api_client):
        """Test get_project_media handles dict response with 'data' key."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={"data": [{"mediaId": 1}, {"mediaId": 2}]}
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession.request", return_value=mock_response):
            async with api_client:
                result = await api_client.get_project_media(123)

                assert isinstance(result, list)
                assert len(result) == 2

    @pytest.mark.asyncio
    async def test_get_project_media_dict_with_media_key(self, api_client):
        """Test get_project_media handles dict response with 'media' key."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={"media": [{"mediaId": 1}]}
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession.request", return_value=mock_response):
            async with api_client:
                result = await api_client.get_project_media(123)

                assert isinstance(result, list)
                assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_project_media_single_dict(self, api_client):
        """Test get_project_media wraps single dict in list."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"mediaId": 1})
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession.request", return_value=mock_response):
            async with api_client:
                result = await api_client.get_project_media(123)

                assert isinstance(result, list)
                assert len(result) == 1
                assert result[0]["mediaId"] == 1

    @pytest.mark.asyncio
    async def test_get_project_media_with_media_ids(self, api_client):
        """Test get_project_media includes mediaIds param."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=[])
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession.request", return_value=mock_response) as mock_req:
            async with api_client:
                await api_client.get_project_media(123, media_ids=[456, 789])

                call_kwargs = mock_req.call_args[1]
                assert call_kwargs["params"] == {"mediaIds": "456,789"}

    @pytest.mark.asyncio
    async def test_get_project_contacts(self, api_client):
        """Test get_project_contacts normalizes response to list."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={"contacts": [{"contactId": 1}]}
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession.request", return_value=mock_response):
            async with api_client:
                result = await api_client.get_project_contacts(123)

                assert isinstance(result, list)
                assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_custom_task(self, api_client):
        """Test get_custom_task with full=true param."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={"assignmentId": 456, "customTask": {}}
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession.request", return_value=mock_response) as mock_req:
            async with api_client:
                result = await api_client.get_custom_task(456)

                assert result["assignmentId"] == 456
                # Check that full=true param was included
                call_kwargs = mock_req.call_args[1]
                assert call_kwargs["params"] == {"full": "true"}

    @pytest.mark.asyncio
    async def test_get_project_tasks(self, api_client):
        """Test get_project_tasks POST request with body."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={
                "success": True,
                "data": [{"customTaskAssignmentId": 1, "taskName": "Test Task"}],
            }
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession.request", return_value=mock_response) as mock_req:
            async with api_client:
                result = await api_client.get_project_tasks(123)

                # Check POST was made to /data
                assert mock_req.call_args[0][0] == "POST"
                # Check body includes reportType, projectId, and senderUsername
                body = mock_req.call_args[1]["json"]
                assert body["reportType"] == "CUSTOM_TASK_HIGH_LEVEL"
                assert body["projectId"] == 123
                assert body["senderUsername"] == "test@example.com"
                # Check result is the data list
                assert isinstance(result, list)
                assert len(result) == 1
                assert result[0]["customTaskAssignmentId"] == 1

    @pytest.mark.asyncio
    async def test_get_video_collaboration(self, api_client):
        """Test get_video_collaboration POST request with body."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"data": []})
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession.request", return_value=mock_response) as mock_req:
            async with api_client:
                result = await api_client.get_video_collaboration("123")

                # Check POST was made to /data
                assert mock_req.call_args[0][0] == "POST"
                # Check body includes reportType and projectId
                body = mock_req.call_args[1]["json"]
                assert body["reportType"] == "VIDEO_COLLABORATION"
                assert body["projectId"] == 123
                assert body["senderUsername"] == "test@example.com"

    @pytest.mark.asyncio
    async def test_get_video_collaboration_with_dates(self, api_client):
        """Test get_video_collaboration includes date filters."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={})
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 12, 31, tzinfo=timezone.utc)

        with patch("aiohttp.ClientSession.request", return_value=mock_response) as mock_req:
            async with api_client:
                await api_client.get_video_collaboration(
                    "123", start_date=start, end_date=end
                )

                body = mock_req.call_args[1]["json"]
                assert body["startDate"] == start.isoformat()
                assert body["endDate"] == end.isoformat()

    @pytest.mark.asyncio
    async def test_get_project_conversations(self, api_client):
        """Test get_project_conversations normalizes response to list."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={"conversations": [{"conversationId": 1}]}
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession.request", return_value=mock_response):
            async with api_client:
                result = await api_client.get_project_conversations(123)

                assert isinstance(result, list)
                assert len(result) == 1


class TestClaimXApiClientUtilities:
    """Test utility methods."""

    def test_get_circuit_status(self, api_client, mock_circuit_breaker):
        """Test get_circuit_status returns diagnostics."""
        diagnostics = api_client.get_circuit_status()

        assert diagnostics == {"state": "closed", "failures": 0}
        mock_circuit_breaker.get_diagnostics.assert_called_once()

    def test_is_circuit_open_property(self, api_client, mock_circuit_breaker):
        """Test is_circuit_open property."""
        mock_circuit_breaker.is_open = False
        assert api_client.is_circuit_open is False

        mock_circuit_breaker.is_open = True
        assert api_client.is_circuit_open is True
