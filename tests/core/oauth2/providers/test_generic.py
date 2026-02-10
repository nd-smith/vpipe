"""Tests for GenericOAuth2Provider - standard OAuth2 client credentials flow."""

from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from core.oauth2.exceptions import InvalidConfigurationError, TokenAcquisitionError
from core.oauth2.models import OAuth2Config, OAuth2Token
from core.oauth2.providers.generic import GenericOAuth2Provider


def _make_config(**overrides):
    defaults = {
        "provider_name": "test_provider",
        "client_id": "test_client",
        "client_secret": "test-cs",
        "token_url": "https://auth.example.com/token",
    }
    defaults.update(overrides)
    return OAuth2Config(**defaults)


def _mock_session_with_response(response_mock):
    """Create a mock session where post() returns the given response context manager."""
    mock_session = MagicMock()
    mock_session.closed = False
    mock_session.post = MagicMock(return_value=response_mock)
    return mock_session


def _ok_response(data):
    """Create a mock async context manager for a 200 response."""
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json = AsyncMock(return_value=data)
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=False)
    return mock_resp


def _error_response(status, text="error"):
    """Create a mock async context manager for an error response."""
    mock_resp = AsyncMock()
    mock_resp.status = status
    mock_resp.text = AsyncMock(return_value=text)
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=False)
    return mock_resp


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------


class TestGenericProviderInit:
    def test_creates_provider_with_valid_config(self):
        config = _make_config()
        provider = GenericOAuth2Provider(config)
        assert provider.provider_name == "test_provider"
        assert provider.config is config

    def test_raises_when_client_id_missing(self):
        config = _make_config(client_id="")
        with pytest.raises(InvalidConfigurationError, match="client_id"):
            GenericOAuth2Provider(config)

    def test_raises_when_client_secret_missing(self):
        config = _make_config(client_secret="")
        with pytest.raises(InvalidConfigurationError, match="client_secret"):
            GenericOAuth2Provider(config)

    def test_raises_when_token_url_missing(self):
        config = _make_config(token_url="")
        with pytest.raises(InvalidConfigurationError, match="token_url"):
            GenericOAuth2Provider(config)

    def test_session_starts_none(self):
        config = _make_config()
        provider = GenericOAuth2Provider(config)
        assert provider._session is None


# ---------------------------------------------------------------------------
# _ensure_session
# ---------------------------------------------------------------------------


class TestEnsureSession:
    async def test_creates_session_when_none(self):
        provider = GenericOAuth2Provider(_make_config())
        session = await provider._ensure_session()
        assert isinstance(session, aiohttp.ClientSession)
        await provider.close()

    async def test_reuses_existing_session(self):
        provider = GenericOAuth2Provider(_make_config())
        session1 = await provider._ensure_session()
        session2 = await provider._ensure_session()
        assert session1 is session2
        await provider.close()

    async def test_creates_new_session_if_closed(self):
        provider = GenericOAuth2Provider(_make_config())
        session1 = await provider._ensure_session()
        await session1.close()
        session2 = await provider._ensure_session()
        assert session2 is not session1
        assert not session2.closed
        await provider.close()


# ---------------------------------------------------------------------------
# acquire_token
# ---------------------------------------------------------------------------


class TestAcquireToken:
    async def test_returns_token_on_success(self):
        provider = GenericOAuth2Provider(_make_config())

        resp = _ok_response(
            {
                "access_token": "test-acquired-tok",
                "token_type": "Bearer",
                "expires_in": 3600,
            }
        )
        session = _mock_session_with_response(resp)
        provider._session = session

        token = await provider.acquire_token()
        assert isinstance(token, OAuth2Token)
        assert token.access_token == "test-acquired-tok"

    async def test_sends_correct_request_data(self):
        provider = GenericOAuth2Provider(_make_config(scope="read write"))

        resp = _ok_response({"access_token": "tok", "expires_in": 3600})
        session = _mock_session_with_response(resp)
        provider._session = session

        await provider.acquire_token()

        call_kwargs = session.post.call_args
        assert call_kwargs[0][0] == "https://auth.example.com/token"
        data = call_kwargs[1]["data"]
        assert data["grant_type"] == "client_credentials"
        assert data["client_id"] == "test_client"
        assert data["client_secret"] == "test-cs"
        assert data["scope"] == "read write"

    async def test_includes_additional_params(self):
        provider = GenericOAuth2Provider(
            _make_config(additional_params={"audience": "https://api.example.com"})
        )

        resp = _ok_response({"access_token": "tok", "expires_in": 3600})
        session = _mock_session_with_response(resp)
        provider._session = session

        await provider.acquire_token()

        data = session.post.call_args[1]["data"]
        assert data["audience"] == "https://api.example.com"

    async def test_omits_scope_when_empty(self):
        provider = GenericOAuth2Provider(_make_config(scope=None))

        resp = _ok_response({"access_token": "tok", "expires_in": 3600})
        session = _mock_session_with_response(resp)
        provider._session = session

        await provider.acquire_token()

        data = session.post.call_args[1]["data"]
        assert "scope" not in data

    async def test_raises_on_http_error_status(self):
        provider = GenericOAuth2Provider(_make_config())

        resp = _error_response(401, "Unauthorized")
        session = _mock_session_with_response(resp)
        provider._session = session

        with pytest.raises(TokenAcquisitionError, match="HTTP 401"):
            await provider.acquire_token()

    async def test_raises_on_aiohttp_client_error(self):
        provider = GenericOAuth2Provider(_make_config())

        session = MagicMock()
        session.closed = False
        session.post = MagicMock(side_effect=aiohttp.ClientError("Connection failed"))
        provider._session = session

        with pytest.raises(TokenAcquisitionError, match="HTTP error"):
            await provider.acquire_token()

    async def test_raises_on_unexpected_error(self):
        provider = GenericOAuth2Provider(_make_config())

        session = MagicMock()
        session.closed = False
        session.post = MagicMock(side_effect=ValueError("bad json"))
        provider._session = session

        with pytest.raises(TokenAcquisitionError, match="Token acquisition failed"):
            await provider.acquire_token()

    async def test_truncates_long_error_text(self):
        provider = GenericOAuth2Provider(_make_config())

        long_error = "x" * 500
        resp = _error_response(500, long_error)
        session = _mock_session_with_response(resp)
        provider._session = session

        with pytest.raises(TokenAcquisitionError) as exc_info:
            await provider.acquire_token()
        # Error text should be truncated to 200 chars
        assert len(str(exc_info.value)) < 300


# ---------------------------------------------------------------------------
# refresh_token
# ---------------------------------------------------------------------------


class TestRefreshToken:
    def _make_token(self, refresh_token=None):
        from datetime import UTC, datetime, timedelta

        return OAuth2Token(
            access_token="test-old-tok",
            token_type="Bearer",
            expires_at=datetime.now(UTC) + timedelta(hours=1),
            refresh_token=refresh_token,
        )

    async def test_acquires_new_token_when_no_refresh_token(self):
        provider = GenericOAuth2Provider(_make_config())

        resp = _ok_response({"access_token": "test-new-tok", "expires_in": 3600})
        session = _mock_session_with_response(resp)
        provider._session = session

        token = self._make_token(refresh_token=None)
        result = await provider.refresh_token(token)
        assert result.access_token == "test-new-tok"

        data = session.post.call_args[1]["data"]
        assert data["grant_type"] == "client_credentials"

    async def test_uses_refresh_token_when_available(self):
        provider = GenericOAuth2Provider(_make_config())

        resp = _ok_response({"access_token": "test-refreshed-tok", "expires_in": 3600})
        session = _mock_session_with_response(resp)
        provider._session = session

        token = self._make_token(refresh_token="test-refresh-tok")
        result = await provider.refresh_token(token)
        assert result.access_token == "test-refreshed-tok"

        data = session.post.call_args[1]["data"]
        assert data["grant_type"] == "refresh_token"
        assert data["refresh_token"] == "test-refresh-tok"

    async def test_falls_back_to_acquire_on_refresh_failure(self):
        provider = GenericOAuth2Provider(_make_config())

        fail_resp = _error_response(401, "invalid refresh token")
        success_resp = _ok_response({"access_token": "test-fallback-tok", "expires_in": 3600})

        session = MagicMock()
        session.closed = False
        session.post = MagicMock(side_effect=[fail_resp, success_resp])
        provider._session = session

        token = self._make_token(refresh_token="test-expired-ref")
        result = await provider.refresh_token(token)
        assert result.access_token == "test-fallback-tok"

    async def test_falls_back_to_acquire_on_exception(self):
        provider = GenericOAuth2Provider(_make_config())

        success_resp = _ok_response({"access_token": "test-fallback-tok", "expires_in": 3600})

        session = MagicMock()
        session.closed = False
        session.post = MagicMock(side_effect=[aiohttp.ClientError("conn reset"), success_resp])
        provider._session = session

        token = self._make_token(refresh_token="test-some-ref")
        result = await provider.refresh_token(token)
        assert result.access_token == "test-fallback-tok"


# ---------------------------------------------------------------------------
# close
# ---------------------------------------------------------------------------


class TestClose:
    async def test_closes_open_session(self):
        provider = GenericOAuth2Provider(_make_config())
        session = await provider._ensure_session()
        assert not session.closed

        await provider.close()
        assert session.closed

    async def test_no_error_when_no_session(self):
        provider = GenericOAuth2Provider(_make_config())
        await provider.close()  # Should not raise

    async def test_no_error_when_session_already_closed(self):
        provider = GenericOAuth2Provider(_make_config())
        session = await provider._ensure_session()
        await session.close()

        await provider.close()  # Should not raise
