"""
Unit tests for HealthCheckServer.

Test Coverage:
    - Server initialization and configuration
    - Lifecycle management (start/stop)
    - Liveness endpoint behavior
    - Readiness endpoint behavior
    - Status management (set_ready, set_error)
    - Port conflict handling and fallback
    - Dynamic port assignment
    - Error state handling
    - Properties (is_ready, actual_port, is_enabled)

No infrastructure required - all tests use real HTTP endpoints.
"""

import aiohttp
import pytest

from pipeline.common.health import HealthCheckServer


class TestHealthCheckServerInitialization:
    """Test server initialization and configuration."""

    def test_initialization_with_default_config(self):
        """Server initializes with default configuration."""
        server = HealthCheckServer(port=8080, worker_name="test-worker")

        assert server.port == 8080
        assert server.worker_name == "test-worker"
        assert server.is_enabled is True
        assert server.is_ready is False
        assert server.actual_port is None
        assert server.error_message is None

    def test_initialization_with_port_none_disables_server(self):
        """Server is disabled when port=None."""
        server = HealthCheckServer(port=None, worker_name="test-worker")

        assert server.port is None
        assert server.is_enabled is False

    def test_initialization_with_enabled_false(self):
        """Server is disabled when enabled=False."""
        server = HealthCheckServer(port=8080, worker_name="test-worker", enabled=False)

        assert server.port == 8080
        assert server.is_enabled is False

    def test_initialization_with_dynamic_port(self):
        """Server initializes with port=0 for dynamic assignment."""
        server = HealthCheckServer(port=0, worker_name="test-worker")

        assert server.port == 0
        assert server.is_enabled is True


class TestHealthCheckServerLifecycle:
    """Test server lifecycle (start/stop)."""

    @pytest.mark.asyncio
    async def test_start_server_successfully(self):
        """Server starts successfully on available port."""
        server = HealthCheckServer(port=0, worker_name="test-worker")

        await server.start()

        try:
            # Verify server is running
            assert server.actual_port is not None
            assert server.actual_port > 0

            # Verify we can hit the endpoints
            async with (
                aiohttp.ClientSession() as session,
                session.get(f"http://localhost:{server.actual_port}/health/live") as resp,
            ):
                assert resp.status == 200

        finally:
            await server.stop()

    @pytest.mark.asyncio
    async def test_start_with_port_conflict_falls_back(self):
        """Server falls back to dynamic port on conflict."""
        # Start first server on dynamic port
        server1 = HealthCheckServer(port=0, worker_name="server1")
        await server1.start()

        try:
            port1 = server1.actual_port

            # Try to start second server on same port (should fall back)
            server2 = HealthCheckServer(port=port1, worker_name="server2")
            await server2.start()

            try:
                # Should have fallen back to different port
                assert server2.actual_port is not None
                assert server2.actual_port != port1
                assert server2.is_enabled is True

            finally:
                await server2.stop()

        finally:
            await server1.stop()

    @pytest.mark.asyncio
    async def test_start_disabled_server_is_noop(self):
        """Starting disabled server does nothing."""
        server = HealthCheckServer(port=8080, worker_name="test-worker", enabled=False)

        await server.start()

        # Should still be disabled
        assert server.is_enabled is False
        assert server.actual_port is None

        await server.stop()

    @pytest.mark.asyncio
    async def test_stop_server_cleanly(self):
        """Server stops cleanly."""
        server = HealthCheckServer(port=0, worker_name="test-worker")
        await server.start()

        port = server.actual_port
        assert port is not None

        # Stop server
        await server.stop()

        # Verify cleanup
        assert server.actual_port is None

        # Verify endpoints are no longer accessible
        async with aiohttp.ClientSession() as session:
            with pytest.raises(aiohttp.ClientError):
                async with session.get(f"http://localhost:{port}/health/live"):
                    pass

    @pytest.mark.asyncio
    async def test_stop_disabled_server_is_noop(self):
        """Stopping disabled server does nothing."""
        server = HealthCheckServer(port=8080, worker_name="test-worker", enabled=False)

        # Should not raise
        await server.stop()


class TestHealthCheckServerLivenessEndpoint:
    """Test liveness endpoint behavior."""

    @pytest.mark.asyncio
    async def test_liveness_always_returns_200(self):
        """Liveness endpoint always returns 200 OK."""
        server = HealthCheckServer(port=0, worker_name="test-worker")
        await server.start()

        try:
            async with (
                aiohttp.ClientSession() as session,
                session.get(f"http://localhost:{server.actual_port}/health/live") as resp,
            ):
                assert resp.status == 200

                data = await resp.json()
                assert data["status"] == "alive"
                assert data["worker"] == "test-worker"
                assert "uptime_seconds" in data
                assert "timestamp" in data
                assert data["uptime_seconds"] >= 0

        finally:
            await server.stop()

    @pytest.mark.asyncio
    async def test_liveness_returns_200_even_when_not_ready(self):
        """Liveness endpoint returns 200 even when worker is not ready."""
        server = HealthCheckServer(port=0, worker_name="test-worker")
        await server.start()

        try:
            # Set not ready
            server.set_ready(kafka_connected=False)

            async with (
                aiohttp.ClientSession() as session,
                session.get(f"http://localhost:{server.actual_port}/health/live") as resp,
            ):
                assert resp.status == 200

        finally:
            await server.stop()


class TestHealthCheckServerReadinessEndpoint:
    """Test readiness endpoint behavior."""

    @pytest.mark.asyncio
    async def test_readiness_returns_503_when_not_ready(self):
        """Readiness endpoint returns 503 when not ready."""
        server = HealthCheckServer(port=0, worker_name="test-worker")
        await server.start()

        try:
            # Default state is not ready
            async with (
                aiohttp.ClientSession() as session,
                session.get(f"http://localhost:{server.actual_port}/health/ready") as resp,
            ):
                assert resp.status == 503

                data = await resp.json()
                assert data["status"] == "not_ready"
                assert data["worker"] == "test-worker"
                assert "kafka_disconnected" in data["reasons"]

        finally:
            await server.stop()

    @pytest.mark.asyncio
    async def test_readiness_returns_200_when_ready(self):
        """Readiness endpoint returns 200 when ready."""
        server = HealthCheckServer(port=0, worker_name="test-worker")
        await server.start()

        try:
            # Set ready
            server.set_ready(kafka_connected=True)

            async with (
                aiohttp.ClientSession() as session,
                session.get(f"http://localhost:{server.actual_port}/health/ready") as resp,
            ):
                assert resp.status == 200

                data = await resp.json()
                assert data["status"] == "ready"
                assert data["worker"] == "test-worker"
                assert data["checks"]["kafka_connected"] is True
                assert data["checks"]["api_reachable"] is True
                assert data["checks"]["circuit_closed"] is True

        finally:
            await server.stop()

    @pytest.mark.asyncio
    async def test_readiness_not_ready_due_to_kafka(self):
        """Readiness is not ready when Kafka is disconnected."""
        server = HealthCheckServer(port=0, worker_name="test-worker")
        await server.start()

        try:
            server.set_ready(kafka_connected=False)

            async with (
                aiohttp.ClientSession() as session,
                session.get(f"http://localhost:{server.actual_port}/health/ready") as resp,
            ):
                assert resp.status == 503

                data = await resp.json()
                assert "kafka_disconnected" in data["reasons"]
                assert data["checks"]["kafka_connected"] is False

        finally:
            await server.stop()

    @pytest.mark.asyncio
    async def test_readiness_not_ready_due_to_api(self):
        """Readiness is not ready when API is unreachable."""
        server = HealthCheckServer(port=0, worker_name="test-worker")
        await server.start()

        try:
            server.set_ready(kafka_connected=True, api_reachable=False)

            async with (
                aiohttp.ClientSession() as session,
                session.get(f"http://localhost:{server.actual_port}/health/ready") as resp,
            ):
                assert resp.status == 503

                data = await resp.json()
                assert "api_unreachable" in data["reasons"]
                assert data["checks"]["api_reachable"] is False

        finally:
            await server.stop()

    @pytest.mark.asyncio
    async def test_readiness_not_ready_due_to_circuit_breaker(self):
        """Readiness is not ready when circuit breaker is open."""
        server = HealthCheckServer(port=0, worker_name="test-worker")
        await server.start()

        try:
            server.set_ready(kafka_connected=True, circuit_open=True)

            async with (
                aiohttp.ClientSession() as session,
                session.get(f"http://localhost:{server.actual_port}/health/ready") as resp,
            ):
                assert resp.status == 503

                data = await resp.json()
                assert "circuit_open" in data["reasons"]
                assert data["checks"]["circuit_closed"] is False

        finally:
            await server.stop()

    @pytest.mark.asyncio
    async def test_readiness_with_error_state_returns_200(self):
        """Readiness with error state returns 200 to allow deployment."""
        server = HealthCheckServer(port=0, worker_name="test-worker")
        await server.start()

        try:
            server.set_error("Configuration error")

            async with (
                aiohttp.ClientSession() as session,
                session.get(f"http://localhost:{server.actual_port}/health/ready") as resp,
            ):
                # Returns 200 to allow deployment to complete
                assert resp.status == 200

                data = await resp.json()
                assert data["status"] == "error"
                assert data["error"] == "Configuration error"
                assert "configuration_error" in data["reasons"]

        finally:
            await server.stop()


class TestHealthCheckServerStatusManagement:
    """Test status management methods."""

    def test_set_ready_updates_status(self):
        """set_ready updates readiness status."""
        server = HealthCheckServer(port=8080, worker_name="test-worker")

        # Initially not ready
        assert server.is_ready is False

        # Set ready
        server.set_ready(kafka_connected=True)
        assert server.is_ready is True

        # Set not ready
        server.set_ready(kafka_connected=False)
        assert server.is_ready is False

    def test_set_ready_with_api_reachable(self):
        """set_ready handles api_reachable parameter."""
        server = HealthCheckServer(port=8080, worker_name="test-worker")

        # Set ready with API unreachable
        server.set_ready(kafka_connected=True, api_reachable=False)
        assert server.is_ready is False

        # Set ready with API reachable
        server.set_ready(kafka_connected=True, api_reachable=True)
        assert server.is_ready is True

    def test_set_ready_with_circuit_breaker(self):
        """set_ready handles circuit_open parameter."""
        server = HealthCheckServer(port=8080, worker_name="test-worker")

        # Set ready with circuit open
        server.set_ready(kafka_connected=True, circuit_open=True)
        assert server.is_ready is False

        # Set ready with circuit closed
        server.set_ready(kafka_connected=True, circuit_open=False)
        assert server.is_ready is True

    def test_set_ready_api_none_preserves_previous_value(self):
        """set_ready with api_reachable=None preserves previous value."""
        server = HealthCheckServer(port=8080, worker_name="test-worker")

        # Set API unreachable
        server.set_ready(kafka_connected=True, api_reachable=False)
        assert server.is_ready is False

        # Update without changing API status
        server.set_ready(kafka_connected=True, api_reachable=None)
        # Should still be False from previous call
        assert server.is_ready is False

    def test_set_error_updates_error_state(self):
        """set_error sets error message and marks not ready."""
        server = HealthCheckServer(port=8080, worker_name="test-worker")

        server.set_ready(kafka_connected=True)
        assert server.is_ready is True

        server.set_error("Test error")
        assert server.is_ready is False
        assert server.error_message == "Test error"

    def test_clear_error_clears_error_state(self):
        """clear_error clears the error message."""
        server = HealthCheckServer(port=8080, worker_name="test-worker")

        server.set_error("Test error")
        assert server.error_message == "Test error"

        server.clear_error()
        assert server.error_message is None

    def test_clear_error_when_no_error_is_noop(self):
        """clear_error when no error is a no-op."""
        server = HealthCheckServer(port=8080, worker_name="test-worker")

        # Should not raise
        server.clear_error()
        assert server.error_message is None


class TestHealthCheckServerProperties:
    """Test server properties."""

    def test_is_ready_property(self):
        """is_ready property reflects readiness status."""
        server = HealthCheckServer(port=8080, worker_name="test-worker")

        assert server.is_ready is False

        server.set_ready(kafka_connected=True)
        assert server.is_ready is True

    def test_is_enabled_property(self):
        """is_enabled property reflects server state."""
        enabled_server = HealthCheckServer(port=8080, worker_name="test-worker")
        assert enabled_server.is_enabled is True

        disabled_server = HealthCheckServer(port=8080, worker_name="test-worker", enabled=False)
        assert disabled_server.is_enabled is False

    def test_error_message_property(self):
        """error_message property returns current error."""
        server = HealthCheckServer(port=8080, worker_name="test-worker")

        assert server.error_message is None

        server.set_error("Test error")
        assert server.error_message == "Test error"

        server.clear_error()
        assert server.error_message is None

    @pytest.mark.asyncio
    async def test_actual_port_property_after_start(self):
        """actual_port property returns assigned port after start."""
        server = HealthCheckServer(port=0, worker_name="test-worker")

        assert server.actual_port is None

        await server.start()

        try:
            assert server.actual_port is not None
            assert server.actual_port > 0

        finally:
            await server.stop()

        assert server.actual_port is None

    def test_actual_port_property_when_disabled(self):
        """actual_port property returns None when disabled."""
        server = HealthCheckServer(port=8080, worker_name="test-worker", enabled=False)

        assert server.actual_port is None
