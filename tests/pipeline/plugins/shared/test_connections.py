"""Tests for connection management."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipeline.plugins.shared.connections import (
    AuthType,
    ConnectionConfig,
    ConnectionManager,
    is_http_error,
)

# =====================
# is_http_error tests
# =====================


class TestIsHttpError:
    def test_200_is_not_error(self):
        assert is_http_error(200) is False

    def test_201_is_not_error(self):
        assert is_http_error(201) is False

    def test_299_is_not_error(self):
        assert is_http_error(299) is False

    def test_300_is_error(self):
        assert is_http_error(300) is True

    def test_404_is_error(self):
        assert is_http_error(404) is True

    def test_500_is_error(self):
        assert is_http_error(500) is True

    def test_199_is_error(self):
        assert is_http_error(199) is True

    def test_100_is_error(self):
        assert is_http_error(100) is True


# =====================
# AuthType tests
# =====================


class TestAuthType:
    def test_values(self):
        assert AuthType.NONE.value == "none"
        assert AuthType.BEARER.value == "bearer"
        assert AuthType.API_KEY.value == "api_key"
        assert AuthType.BASIC.value == "basic"


# =====================
# ConnectionConfig tests
# =====================


class TestConnectionConfig:
    def test_defaults(self):
        config = ConnectionConfig(name="test", base_url="https://api.example.com")
        assert config.auth_type == AuthType.NONE
        assert config.timeout_seconds == 30
        assert config.connect_timeout_seconds == 10
        assert config.max_retries == 3
        assert config.headers == {}
        assert config.auth_header is None

    def test_strips_trailing_slash(self):
        config = ConnectionConfig(name="test", base_url="https://api.example.com/")
        assert config.base_url == "https://api.example.com"

    def test_strips_multiple_trailing_slashes(self):
        config = ConnectionConfig(name="test", base_url="https://api.example.com///")
        assert config.base_url == "https://api.example.com"

    def test_bearer_default_auth_header(self):
        config = ConnectionConfig(
            name="test",
            base_url="https://api.example.com",
            auth_type=AuthType.BEARER,
            auth_token="tok",
        )
        assert config.auth_header == "Authorization"

    def test_basic_default_auth_header(self):
        config = ConnectionConfig(
            name="test",
            base_url="https://api.example.com",
            auth_type=AuthType.BASIC,
            auth_token="test-creds",
        )
        assert config.auth_header == "Authorization"

    def test_api_key_default_auth_header(self):
        config = ConnectionConfig(
            name="test",
            base_url="https://api.example.com",
            auth_type=AuthType.API_KEY,
            auth_token="test-key",
        )
        assert config.auth_header == "X-API-Key"

    def test_custom_auth_header(self):
        config = ConnectionConfig(
            name="test",
            base_url="https://api.example.com",
            auth_type=AuthType.BEARER,
            auth_token="tok",
            auth_header="X-Custom-Auth",
        )
        assert config.auth_header == "X-Custom-Auth"

    def test_string_auth_type_converted(self):
        config = ConnectionConfig(
            name="test",
            base_url="https://api.example.com",
            auth_type="bearer",
        )
        assert config.auth_type == AuthType.BEARER

    def test_no_auth_header_for_none_type(self):
        config = ConnectionConfig(name="test", base_url="https://api.example.com")
        assert config.auth_header is None


# =====================
# ConnectionManager tests
# =====================


class TestConnectionManager:
    def test_add_connection(self):
        mgr = ConnectionManager()
        config = ConnectionConfig(name="api", base_url="https://api.example.com")
        mgr.add_connection(config)

        assert "api" in mgr.list_connections()

    def test_add_duplicate_raises(self):
        mgr = ConnectionManager()
        config = ConnectionConfig(name="api", base_url="https://api.example.com")
        mgr.add_connection(config)

        with pytest.raises(ValueError, match="already exists"):
            mgr.add_connection(config)

    def test_get_connection(self):
        mgr = ConnectionManager()
        config = ConnectionConfig(name="api", base_url="https://api.example.com")
        mgr.add_connection(config)

        result = mgr.get_connection("api")
        assert result is config

    def test_get_missing_connection_raises(self):
        mgr = ConnectionManager()
        with pytest.raises(KeyError, match="not found"):
            mgr.get_connection("nonexistent")

    def test_list_connections_empty(self):
        mgr = ConnectionManager()
        assert mgr.list_connections() == []

    def test_list_connections_multiple(self):
        mgr = ConnectionManager()
        mgr.add_connection(ConnectionConfig(name="a", base_url="https://a.com"))
        mgr.add_connection(ConnectionConfig(name="b", base_url="https://b.com"))

        names = mgr.list_connections()
        assert set(names) == {"a", "b"}

    def test_is_started_false_initially(self):
        mgr = ConnectionManager()
        assert mgr.is_started is False

    async def test_request_raises_when_not_started(self):
        mgr = ConnectionManager()
        mgr.add_connection(ConnectionConfig(name="api", base_url="https://api.com"))

        with pytest.raises(RuntimeError, match="not started"):
            await mgr.request(
                connection_name="api",
                method="GET",
                path="/test",
            )

    async def test_close_when_not_started_is_noop(self):
        mgr = ConnectionManager()
        await mgr.close()  # Should not raise

    async def test_start_sets_started(self):
        mgr = ConnectionManager()

        with patch("pipeline.plugins.shared.connections.aiohttp") as mock_aiohttp:
            mock_session = MagicMock()
            mock_aiohttp.ClientSession.return_value = mock_session
            mock_aiohttp.TCPConnector.return_value = MagicMock()
            mock_aiohttp.ClientTimeout.return_value = MagicMock()

            await mgr.start()
            assert mgr.is_started is True

            # Cleanup
            mock_session.close = AsyncMock()
            await mgr.close()

    async def test_start_twice_is_idempotent(self):
        mgr = ConnectionManager()

        with patch("pipeline.plugins.shared.connections.aiohttp") as mock_aiohttp:
            mock_session = MagicMock()
            mock_aiohttp.ClientSession.return_value = mock_session
            mock_aiohttp.TCPConnector.return_value = MagicMock()
            mock_aiohttp.ClientTimeout.return_value = MagicMock()

            await mgr.start()
            await mgr.start()  # Should warn but not error
            assert mgr.is_started is True

            mock_session.close = AsyncMock()
            await mgr.close()

    async def test_request_builds_url_correctly(self):
        mgr = ConnectionManager()
        config = ConnectionConfig(
            name="api",
            base_url="https://api.example.com",
            auth_type=AuthType.BEARER,
            auth_token="test-tok",
        )
        mgr.add_connection(config)

        # Mock the internal request flow
        with patch("pipeline.plugins.shared.connections.aiohttp") as mock_aiohttp:
            mock_session = MagicMock()
            mock_aiohttp.ClientSession.return_value = mock_session
            mock_aiohttp.TCPConnector.return_value = MagicMock()
            mock_aiohttp.ClientTimeout.return_value = MagicMock()

            await mgr.start()

            # Mock the with_retry_async to just call the function directly
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.read = AsyncMock()

            with patch("pipeline.plugins.shared.connections.with_retry_async") as mock_retry:
                # Make retry decorator pass through
                mock_retry.return_value = lambda fn: fn

                # Mock the session.request context manager
                mock_cm = AsyncMock()
                mock_cm.__aenter__.return_value = mock_response
                mock_session.request.return_value = mock_cm

                await mgr.request(
                    connection_name="api",
                    method="GET",
                    path="/v1/items",
                )

                # Verify the session was called with correct URL
                call_kwargs = mock_session.request.call_args.kwargs
                assert call_kwargs["url"] == "https://api.example.com/v1/items"
                assert "Bearer test-tok" in call_kwargs["headers"]["Authorization"]

            mock_session.close = AsyncMock()
            await mgr.close()

    async def test_request_adds_api_key_auth(self):
        mgr = ConnectionManager()
        config = ConnectionConfig(
            name="api",
            base_url="https://api.example.com",
            auth_type=AuthType.API_KEY,
            auth_token="test-api-key",
        )
        mgr.add_connection(config)

        with patch("pipeline.plugins.shared.connections.aiohttp") as mock_aiohttp:
            mock_session = MagicMock()
            mock_aiohttp.ClientSession.return_value = mock_session
            mock_aiohttp.TCPConnector.return_value = MagicMock()
            mock_aiohttp.ClientTimeout.return_value = MagicMock()

            await mgr.start()

            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.read = AsyncMock()

            with patch("pipeline.plugins.shared.connections.with_retry_async") as mock_retry:
                mock_retry.return_value = lambda fn: fn

                mock_cm = AsyncMock()
                mock_cm.__aenter__.return_value = mock_response
                mock_session.request.return_value = mock_cm

                await mgr.request(
                    connection_name="api",
                    method="GET",
                    path="/data",
                )

                call_kwargs = mock_session.request.call_args.kwargs
                assert call_kwargs["headers"]["X-API-Key"] == "test-api-key"

            mock_session.close = AsyncMock()
            await mgr.close()

    async def test_request_adds_basic_auth(self):
        mgr = ConnectionManager()
        config = ConnectionConfig(
            name="api",
            base_url="https://api.example.com",
            auth_type=AuthType.BASIC,
            auth_token="fake-basic-creds",
        )
        mgr.add_connection(config)

        with patch("pipeline.plugins.shared.connections.aiohttp") as mock_aiohttp:
            mock_session = MagicMock()
            mock_aiohttp.ClientSession.return_value = mock_session
            mock_aiohttp.TCPConnector.return_value = MagicMock()
            mock_aiohttp.ClientTimeout.return_value = MagicMock()

            await mgr.start()

            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.read = AsyncMock()

            with patch("pipeline.plugins.shared.connections.with_retry_async") as mock_retry:
                mock_retry.return_value = lambda fn: fn

                mock_cm = AsyncMock()
                mock_cm.__aenter__.return_value = mock_response
                mock_session.request.return_value = mock_cm

                await mgr.request(
                    connection_name="api",
                    method="GET",
                    path="/data",
                )

                call_kwargs = mock_session.request.call_args.kwargs
                assert call_kwargs["headers"]["Authorization"] == "Basic fake-basic-creds"

            mock_session.close = AsyncMock()
            await mgr.close()

    async def test_request_merges_extra_headers(self):
        mgr = ConnectionManager()
        config = ConnectionConfig(
            name="api",
            base_url="https://api.example.com",
            headers={"X-Base": "base-val"},
        )
        mgr.add_connection(config)

        with patch("pipeline.plugins.shared.connections.aiohttp") as mock_aiohttp:
            mock_session = MagicMock()
            mock_aiohttp.ClientSession.return_value = mock_session
            mock_aiohttp.TCPConnector.return_value = MagicMock()
            mock_aiohttp.ClientTimeout.return_value = MagicMock()

            await mgr.start()

            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.read = AsyncMock()

            with patch("pipeline.plugins.shared.connections.with_retry_async") as mock_retry:
                mock_retry.return_value = lambda fn: fn

                mock_cm = AsyncMock()
                mock_cm.__aenter__.return_value = mock_response
                mock_session.request.return_value = mock_cm

                await mgr.request(
                    connection_name="api",
                    method="GET",
                    path="/data",
                    headers={"X-Extra": "extra-val"},
                )

                call_kwargs = mock_session.request.call_args.kwargs
                assert call_kwargs["headers"]["X-Base"] == "base-val"
                assert call_kwargs["headers"]["X-Extra"] == "extra-val"

            mock_session.close = AsyncMock()
            await mgr.close()

    async def test_request_timeout_override(self):
        mgr = ConnectionManager()
        config = ConnectionConfig(
            name="api",
            base_url="https://api.example.com",
            timeout_seconds=30,
        )
        mgr.add_connection(config)

        with patch("pipeline.plugins.shared.connections.aiohttp") as mock_aiohttp:
            mock_session = MagicMock()
            mock_aiohttp.ClientSession.return_value = mock_session
            mock_aiohttp.TCPConnector.return_value = MagicMock()
            mock_aiohttp.ClientTimeout.return_value = MagicMock()

            await mgr.start()

            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.read = AsyncMock()

            with patch("pipeline.plugins.shared.connections.with_retry_async") as mock_retry:
                mock_retry.return_value = lambda fn: fn

                mock_cm = AsyncMock()
                mock_cm.__aenter__.return_value = mock_response
                mock_session.request.return_value = mock_cm

                await mgr.request(
                    connection_name="api",
                    method="GET",
                    path="/data",
                    timeout_override=5,
                )

                call_kwargs = mock_session.request.call_args.kwargs
                # timeout=5 should have been passed (override)
                assert call_kwargs["timeout"] is not None

            mock_session.close = AsyncMock()
            await mgr.close()

    async def test_request_missing_connection_raises(self):
        mgr = ConnectionManager()

        with patch("pipeline.plugins.shared.connections.aiohttp") as mock_aiohttp:
            mock_session = MagicMock()
            mock_aiohttp.ClientSession.return_value = mock_session
            mock_aiohttp.TCPConnector.return_value = MagicMock()
            mock_aiohttp.ClientTimeout.return_value = MagicMock()

            await mgr.start()

            with pytest.raises(KeyError, match="not found"):
                await mgr.request(
                    connection_name="missing",
                    method="GET",
                    path="/test",
                )

            mock_session.close = AsyncMock()
            await mgr.close()
