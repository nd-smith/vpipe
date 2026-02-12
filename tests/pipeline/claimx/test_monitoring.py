"""Tests for ClaimX HealthCheckServer.

Tests health check initialization, readiness logic, HTTP endpoint handlers,
server lifecycle (start/stop), and edge cases like port conflicts and
disabled mode.
"""

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request

from pipeline.claimx.monitoring import HealthCheckServer

# ============================================================================
# Initialization
# ============================================================================


class TestHealthCheckServerInit:
    """Tests for HealthCheckServer initialization."""

    def test_default_initialization(self):
        server = HealthCheckServer()

        assert server.port == 8080
        assert server.worker_name == "claimx-worker"
        assert server._enabled is True
        assert server._ready is False
        assert server._transport_connected is False
        assert server._api_reachable is True
        assert server._circuit_open is False
        assert server._actual_port is None

    def test_custom_port_and_worker_name(self):
        server = HealthCheckServer(port=9090, worker_name="my-worker")

        assert server.port == 9090
        assert server.worker_name == "my-worker"
        assert server._enabled is True

    def test_port_zero_for_dynamic_assignment(self):
        server = HealthCheckServer(port=0)

        assert server.port == 0
        assert server._enabled is True

    def test_port_none_disables_server(self):
        server = HealthCheckServer(port=None)

        assert server._enabled is False

    def test_enabled_false_disables_server(self):
        server = HealthCheckServer(enabled=False)

        assert server._enabled is False

    def test_port_none_overrides_enabled_true(self):
        """Server is disabled when port is None, even if enabled=True."""
        server = HealthCheckServer(port=None, enabled=True)

        assert server._enabled is False

    def test_started_at_is_set(self):
        before = datetime.now(UTC)
        server = HealthCheckServer()
        after = datetime.now(UTC)

        assert before <= server._started_at <= after

    def test_aiohttp_components_initially_none(self):
        server = HealthCheckServer()

        assert server._app is None
        assert server._runner is None
        assert server._site is None


# ============================================================================
# Properties
# ============================================================================


class TestHealthCheckServerProperties:
    """Tests for HealthCheckServer properties."""

    def test_is_ready_initially_false(self):
        server = HealthCheckServer()
        assert server.is_ready is False

    def test_is_ready_reflects_internal_state(self):
        server = HealthCheckServer()
        server._ready = True
        assert server.is_ready is True

    def test_actual_port_initially_none(self):
        server = HealthCheckServer()
        assert server.actual_port is None

    def test_actual_port_reflects_internal_state(self):
        server = HealthCheckServer()
        server._actual_port = 12345
        assert server.actual_port == 12345

    def test_is_enabled_when_enabled(self):
        server = HealthCheckServer()
        assert server.is_enabled is True

    def test_is_enabled_when_disabled(self):
        server = HealthCheckServer(enabled=False)
        assert server.is_enabled is False


# ============================================================================
# set_ready
# ============================================================================


class TestSetReady:
    """Tests for the set_ready method."""

    def test_transport_connected_makes_ready(self):
        server = HealthCheckServer()
        server.set_ready(transport_connected=True)

        assert server._ready is True
        assert server._transport_connected is True

    def test_transport_disconnected_makes_not_ready(self):
        server = HealthCheckServer()
        server.set_ready(transport_connected=False)

        assert server._ready is False
        assert server._transport_connected is False

    def test_api_unreachable_makes_not_ready(self):
        server = HealthCheckServer()
        server.set_ready(transport_connected=True, api_reachable=False)

        assert server._ready is False
        assert server._api_reachable is False

    def test_circuit_open_makes_not_ready(self):
        server = HealthCheckServer()
        server.set_ready(transport_connected=True, circuit_open=True)

        assert server._ready is False
        assert server._circuit_open is True

    def test_api_reachable_none_preserves_default(self):
        """When api_reachable is None, the default True is preserved."""
        server = HealthCheckServer()
        server.set_ready(transport_connected=True, api_reachable=None)

        assert server._api_reachable is True
        assert server._ready is True

    def test_api_reachable_none_preserves_previous_value(self):
        """When api_reachable is None, previous value is preserved."""
        server = HealthCheckServer()
        server.set_ready(transport_connected=True, api_reachable=False)
        assert server._api_reachable is False

        server.set_ready(transport_connected=True, api_reachable=None)
        assert server._api_reachable is False
        assert server._ready is False

    def test_all_healthy_makes_ready(self):
        server = HealthCheckServer()
        server.set_ready(transport_connected=True, api_reachable=True, circuit_open=False)

        assert server._ready is True

    def test_all_unhealthy_makes_not_ready(self):
        server = HealthCheckServer()
        server.set_ready(transport_connected=False, api_reachable=False, circuit_open=True)

        assert server._ready is False

    def test_readiness_change_is_logged(self):
        server = HealthCheckServer()
        # First call: False -> True
        with patch("pipeline.claimx.monitoring.logger") as mock_logger:
            server.set_ready(transport_connected=True)
            mock_logger.info.assert_called()

    def test_no_log_when_readiness_unchanged(self):
        server = HealthCheckServer()
        server.set_ready(transport_connected=True)
        # Second call: True -> True (no change)
        with patch("pipeline.claimx.monitoring.logger") as mock_logger:
            server.set_ready(transport_connected=True)
            # info might be called for other reasons, but the readiness-change
            # log should not fire. Check that the specific message was not logged.
            for call in mock_logger.info.call_args_list:
                assert "Readiness status changed" not in str(call)


# ============================================================================
# Liveness handler
# ============================================================================


class TestHandleLiveness:
    """Tests for the /health/live endpoint handler."""

    async def test_liveness_returns_200(self):
        server = HealthCheckServer()
        request = make_mocked_request("GET", "/health/live")

        response = await server.handle_liveness(request)

        assert response.status == 200

    async def test_liveness_body_contains_status_alive(self):
        server = HealthCheckServer()
        request = make_mocked_request("GET", "/health/live")

        response = await server.handle_liveness(request)
        body = json.loads(response.body)

        assert body["status"] == "alive"

    async def test_liveness_body_contains_worker_name(self):
        server = HealthCheckServer(worker_name="test-worker")
        request = make_mocked_request("GET", "/health/live")

        response = await server.handle_liveness(request)
        body = json.loads(response.body)

        assert body["worker"] == "test-worker"

    async def test_liveness_body_contains_uptime(self):
        server = HealthCheckServer()
        request = make_mocked_request("GET", "/health/live")

        response = await server.handle_liveness(request)
        body = json.loads(response.body)

        assert "uptime_seconds" in body
        assert isinstance(body["uptime_seconds"], int)
        assert body["uptime_seconds"] >= 0

    async def test_liveness_body_contains_timestamp(self):
        server = HealthCheckServer()
        request = make_mocked_request("GET", "/health/live")

        response = await server.handle_liveness(request)
        body = json.loads(response.body)

        assert "timestamp" in body
        # Should be a valid ISO format timestamp
        datetime.fromisoformat(body["timestamp"])


# ============================================================================
# Readiness handler
# ============================================================================


class TestHandleReadiness:
    """Tests for the /health/ready endpoint handler."""

    async def test_readiness_returns_503_when_not_ready(self):
        server = HealthCheckServer()
        request = make_mocked_request("GET", "/health/ready")

        response = await server.handle_readiness(request)

        assert response.status == 503

    async def test_readiness_returns_200_when_ready(self):
        server = HealthCheckServer()
        server.set_ready(transport_connected=True)
        request = make_mocked_request("GET", "/health/ready")

        response = await server.handle_readiness(request)

        assert response.status == 200

    async def test_readiness_ready_body_has_correct_status(self):
        server = HealthCheckServer()
        server.set_ready(transport_connected=True)
        request = make_mocked_request("GET", "/health/ready")

        response = await server.handle_readiness(request)
        body = json.loads(response.body)

        assert body["status"] == "ready"

    async def test_readiness_not_ready_body_has_correct_status(self):
        server = HealthCheckServer()
        request = make_mocked_request("GET", "/health/ready")

        response = await server.handle_readiness(request)
        body = json.loads(response.body)

        assert body["status"] == "not_ready"

    async def test_readiness_ready_body_contains_checks(self):
        server = HealthCheckServer()
        server.set_ready(transport_connected=True, api_reachable=True, circuit_open=False)
        request = make_mocked_request("GET", "/health/ready")

        response = await server.handle_readiness(request)
        body = json.loads(response.body)

        assert body["checks"]["transport_connected"] is True
        assert body["checks"]["api_reachable"] is True
        assert body["checks"]["circuit_closed"] is True

    async def test_readiness_not_ready_body_contains_reasons(self):
        server = HealthCheckServer()
        server.set_ready(transport_connected=False)
        request = make_mocked_request("GET", "/health/ready")

        response = await server.handle_readiness(request)
        body = json.loads(response.body)

        assert "reasons" in body
        assert "transport_disconnected" in body["reasons"]

    async def test_readiness_not_ready_transport_disconnected_reason(self):
        server = HealthCheckServer()
        server.set_ready(transport_connected=False)
        request = make_mocked_request("GET", "/health/ready")

        response = await server.handle_readiness(request)
        body = json.loads(response.body)

        assert "transport_disconnected" in body["reasons"]

    async def test_readiness_not_ready_api_unreachable_reason(self):
        server = HealthCheckServer()
        server.set_ready(transport_connected=True, api_reachable=False)
        request = make_mocked_request("GET", "/health/ready")

        response = await server.handle_readiness(request)
        body = json.loads(response.body)

        assert "api_unreachable" in body["reasons"]

    async def test_readiness_not_ready_circuit_open_reason(self):
        server = HealthCheckServer()
        server.set_ready(transport_connected=True, circuit_open=True)
        request = make_mocked_request("GET", "/health/ready")

        response = await server.handle_readiness(request)
        body = json.loads(response.body)

        assert "circuit_open" in body["reasons"]

    async def test_readiness_not_ready_multiple_reasons(self):
        server = HealthCheckServer()
        server.set_ready(transport_connected=False, api_reachable=False, circuit_open=True)
        request = make_mocked_request("GET", "/health/ready")

        response = await server.handle_readiness(request)
        body = json.loads(response.body)

        assert "transport_disconnected" in body["reasons"]
        assert "api_unreachable" in body["reasons"]
        assert "circuit_open" in body["reasons"]

    async def test_readiness_body_contains_worker_name(self):
        server = HealthCheckServer(worker_name="my-worker")
        server.set_ready(transport_connected=True)
        request = make_mocked_request("GET", "/health/ready")

        response = await server.handle_readiness(request)
        body = json.loads(response.body)

        assert body["worker"] == "my-worker"

    async def test_readiness_body_contains_timestamp(self):
        server = HealthCheckServer()
        request = make_mocked_request("GET", "/health/ready")

        response = await server.handle_readiness(request)
        body = json.loads(response.body)

        assert "timestamp" in body
        datetime.fromisoformat(body["timestamp"])


# ============================================================================
# create_app
# ============================================================================


class TestCreateApp:
    """Tests for the create_app method."""

    def test_create_app_returns_application(self):
        server = HealthCheckServer()
        app = server.create_app()

        assert isinstance(app, web.Application)

    def test_create_app_registers_liveness_route(self):
        server = HealthCheckServer()
        app = server.create_app()

        # Check that the route exists
        routes = [r.resource.canonical for r in app.router.routes()]
        assert "/health/live" in routes

    def test_create_app_registers_readiness_route(self):
        server = HealthCheckServer()
        app = server.create_app()

        routes = [r.resource.canonical for r in app.router.routes()]
        assert "/health/ready" in routes


# ============================================================================
# start / stop lifecycle
# ============================================================================


class TestStartStop:
    """Tests for server start and stop lifecycle."""

    async def test_start_disabled_is_noop(self):
        server = HealthCheckServer(enabled=False)

        await server.start()

        assert server._app is None
        assert server._runner is None

    async def test_stop_disabled_is_noop(self):
        server = HealthCheckServer(enabled=False)

        await server.stop()

        assert server._runner is None

    async def test_stop_without_start_is_safe(self):
        server = HealthCheckServer()
        server._enabled = True

        # No runner set, should not raise
        await server.stop()

    async def test_stop_cleans_up_runner(self):
        server = HealthCheckServer()
        mock_runner = AsyncMock()
        server._runner = mock_runner
        server._site = Mock()
        server._app = Mock()
        server._actual_port = 8080

        await server.stop()

        mock_runner.cleanup.assert_called_once()
        assert server._runner is None
        assert server._site is None
        assert server._app is None
        assert server._actual_port is None

    async def test_stop_handles_cleanup_error(self):
        """stop() logs error but still cleans up references on failure."""
        server = HealthCheckServer()
        mock_runner = AsyncMock()
        mock_runner.cleanup.side_effect = Exception("cleanup failed")
        server._runner = mock_runner
        server._site = Mock()
        server._app = Mock()
        server._actual_port = 8080

        await server.stop()

        # References should still be cleaned up via finally block
        assert server._runner is None
        assert server._site is None
        assert server._app is None
        assert server._actual_port is None

    async def test_start_success_on_configured_port(self):
        server = HealthCheckServer(port=8080)

        with patch.object(server, "_try_start_on_port", new_callable=AsyncMock) as mock_try:
            mock_try.return_value = True
            await server.start()

            mock_try.assert_called_once_with(8080)

    async def test_start_falls_back_to_dynamic_port_when_configured_port_in_use(self):
        server = HealthCheckServer(port=8080)

        call_count = 0

        async def try_start_side_effect(port):
            nonlocal call_count
            call_count += 1
            return port != 8080

        with patch.object(server, "_try_start_on_port", side_effect=try_start_side_effect):
            await server.start()

        assert call_count == 2

    async def test_start_disables_when_all_ports_fail(self):
        server = HealthCheckServer(port=8080)

        with patch.object(server, "_try_start_on_port", new_callable=AsyncMock) as mock_try:
            mock_try.return_value = False
            await server.start()

        assert server._enabled is False

    async def test_start_disables_when_dynamic_port_zero_fails(self):
        """When port=0 and it fails, server disables without retry."""
        server = HealthCheckServer(port=0)

        with patch.object(server, "_try_start_on_port", new_callable=AsyncMock) as mock_try:
            mock_try.return_value = False
            await server.start()

        # port == 0, so no fallback attempted, just disables
        assert server._enabled is False
        mock_try.assert_called_once_with(0)

    async def test_start_handles_unexpected_exception(self):
        """Unexpected exceptions during start disable the server gracefully."""
        server = HealthCheckServer(port=8080)

        with patch.object(server, "_try_start_on_port", side_effect=RuntimeError("unexpected")):
            await server.start()

        assert server._enabled is False


# ============================================================================
# _try_start_on_port
# ============================================================================


class TestTryStartOnPort:
    """Tests for the _try_start_on_port method."""

    async def test_try_start_on_port_success(self):
        server = HealthCheckServer()

        mock_runner = AsyncMock()
        mock_site = AsyncMock()
        mock_socket = Mock()
        mock_socket.getsockname.return_value = ("0.0.0.0", 8080)
        mock_site._server = Mock()
        mock_site._server.sockets = [mock_socket]

        with (
            patch("pipeline.claimx.monitoring.web.Application"),
            patch("pipeline.claimx.monitoring.web.AppRunner", return_value=mock_runner),
            patch("pipeline.claimx.monitoring.web.TCPSite", return_value=mock_site),
        ):
            result = await server._try_start_on_port(8080)

        assert result is True
        assert server._actual_port == 8080

    async def test_try_start_on_port_returns_false_on_port_in_use(self):
        server = HealthCheckServer()

        mock_runner = AsyncMock()
        mock_site = AsyncMock()
        os_error = OSError()
        os_error.errno = 98  # Address already in use (Linux)
        mock_site.start.side_effect = os_error

        with (
            patch("pipeline.claimx.monitoring.web.Application"),
            patch("pipeline.claimx.monitoring.web.AppRunner", return_value=mock_runner),
            patch("pipeline.claimx.monitoring.web.TCPSite", return_value=mock_site),
        ):
            result = await server._try_start_on_port(8080)

        assert result is False
        mock_runner.cleanup.assert_called_once()
        assert server._runner is None
        assert server._site is None
        assert server._app is None

    async def test_try_start_on_port_reraises_non_port_os_error(self):
        server = HealthCheckServer()

        mock_runner = AsyncMock()
        mock_site = AsyncMock()
        os_error = OSError()
        os_error.errno = 13  # Permission denied
        mock_site.start.side_effect = os_error

        with (
            patch("pipeline.claimx.monitoring.web.Application"),
            patch("pipeline.claimx.monitoring.web.AppRunner", return_value=mock_runner),
            patch("pipeline.claimx.monitoring.web.TCPSite", return_value=mock_site),
        ):
            with pytest.raises(OSError) as exc_info:
                await server._try_start_on_port(8080)

            assert exc_info.value.errno == 13

    async def test_try_start_on_port_captures_dynamic_port(self):
        """When port=0, actual port is read from socket."""
        server = HealthCheckServer()

        mock_runner = AsyncMock()
        mock_site = AsyncMock()
        mock_socket = Mock()
        mock_socket.getsockname.return_value = ("0.0.0.0", 54321)
        mock_site._server = Mock()
        mock_site._server.sockets = [mock_socket]

        with (
            patch("pipeline.claimx.monitoring.web.Application"),
            patch("pipeline.claimx.monitoring.web.AppRunner", return_value=mock_runner),
            patch("pipeline.claimx.monitoring.web.TCPSite", return_value=mock_site),
        ):
            result = await server._try_start_on_port(0)

        assert result is True
        assert server._actual_port == 54321

    async def test_try_start_on_port_fallback_when_no_sockets(self):
        """Falls back to configured port when server.sockets is empty."""
        server = HealthCheckServer()

        mock_runner = AsyncMock()
        mock_site = AsyncMock()
        mock_site._server = Mock()
        mock_site._server.sockets = []

        with (
            patch("pipeline.claimx.monitoring.web.Application"),
            patch("pipeline.claimx.monitoring.web.AppRunner", return_value=mock_runner),
            patch("pipeline.claimx.monitoring.web.TCPSite", return_value=mock_site),
        ):
            result = await server._try_start_on_port(9999)

        assert result is True
        assert server._actual_port == 9999

    async def test_try_start_on_port_fallback_when_no_server(self):
        """Falls back to configured port when _server is None."""
        server = HealthCheckServer()

        mock_runner = AsyncMock()
        mock_site = AsyncMock()
        mock_site._server = None

        with (
            patch("pipeline.claimx.monitoring.web.Application"),
            patch("pipeline.claimx.monitoring.web.AppRunner", return_value=mock_runner),
            patch("pipeline.claimx.monitoring.web.TCPSite", return_value=mock_site),
        ):
            result = await server._try_start_on_port(7777)

        assert result is True
        assert server._actual_port == 7777

    async def test_try_start_on_port_windows_errno(self):
        """Port in use on Windows (errno 10048) returns False."""
        server = HealthCheckServer()

        mock_runner = AsyncMock()
        mock_site = AsyncMock()
        os_error = OSError()
        os_error.errno = 10048
        mock_site.start.side_effect = os_error

        with (
            patch("pipeline.claimx.monitoring.web.Application"),
            patch("pipeline.claimx.monitoring.web.AppRunner", return_value=mock_runner),
            patch("pipeline.claimx.monitoring.web.TCPSite", return_value=mock_site),
        ):
            result = await server._try_start_on_port(8080)

        assert result is False
