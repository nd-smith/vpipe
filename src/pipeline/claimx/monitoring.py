"""Health check endpoints for ClaimX workers.

Provides /health/live and /health/ready endpoints for Kubernetes probes.
"""

import logging
from datetime import UTC, datetime

from aiohttp import web

logger = logging.getLogger(__name__)


class HealthCheckServer:
    """HTTP server for Kubernetes health check endpoints.

    Provides /health/live and /health/ready endpoints for container orchestration.
    """

    def __init__(
        self,
        port: int | None = 8080,
        worker_name: str = "claimx-worker",
        enabled: bool = True,
    ):
        """
        Initialize health check server.

        Args:
            port: HTTP port to listen on. Use 0 for dynamic port assignment,
                  or None to disable the server (default: 8080)
            worker_name: Name of the worker for logging
            enabled: Whether to enable the health check server (default: True).
                     If False, start() and stop() become no-ops.
        """
        self.port = port
        self.worker_name = worker_name
        self._enabled = enabled and port is not None
        self._ready = False
        self._transport_connected = False
        self._api_reachable = True  # Default true for workers without API dependency
        self._circuit_open = False
        self._started_at = datetime.now(UTC)
        self._actual_port: int | None = None

        # aiohttp components
        self._app: web.Application | None = None
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None

        if self._enabled:
            logger.info(
                f"Initialized HealthCheckServer for {worker_name}",
                extra={"port": port, "worker_name": worker_name},
            )
        else:
            logger.info(
                f"HealthCheckServer disabled for {worker_name}",
                extra={"worker_name": worker_name},
            )

    def set_ready(
        self,
        transport_connected: bool,
        api_reachable: bool | None = None,
        circuit_open: bool = False,
    ) -> None:
        """
        Update readiness status.

        Worker should call this method to update the readiness status
        based on its current state.

        Args:
            transport_connected: Whether transport connection is healthy
            api_reachable: Whether external API is reachable (None = not applicable)
            circuit_open: Whether circuit breaker is open
        """
        self._transport_connected = transport_connected

        if api_reachable is not None:
            self._api_reachable = api_reachable

        self._circuit_open = circuit_open

        # Ready if all dependencies are healthy and circuit is closed
        old_ready = self._ready
        self._ready = self._transport_connected and self._api_reachable and not self._circuit_open

        if old_ready != self._ready:
            logger.info(
                f"Readiness status changed: {old_ready} -> {self._ready}",
                extra={
                    "worker_name": self.worker_name,
                    "transport_connected": transport_connected,
                    "api_reachable": self._api_reachable,
                    "circuit_open": circuit_open,
                },
            )

    async def handle_liveness(self, request: web.Request) -> web.Response:
        """
        Handle GET /health/live - Liveness probe.

        Always returns 200 OK if the server is running.
        Kubernetes will restart the pod if this endpoint becomes unavailable.

        Returns:
            200 OK with status and uptime
        """
        uptime_seconds = (datetime.now(UTC) - self._started_at).total_seconds()

        return web.json_response(
            {
                "status": "alive",
                "worker": self.worker_name,
                "uptime_seconds": int(uptime_seconds),
                "timestamp": datetime.now(UTC).isoformat(),
            },
            status=200,
        )

    async def handle_readiness(self, request: web.Request) -> web.Response:
        """
        Handle GET /health/ready - Readiness probe.

        Returns 200 OK if worker is ready to process work, 503 otherwise.
        Kubernetes uses this to determine if the pod should receive traffic.

        Returns:
            200 OK if ready, 503 Service Unavailable if not ready
        """
        if self._ready:
            return web.json_response(
                {
                    "status": "ready",
                    "worker": self.worker_name,
                    "checks": {
                        "transport_connected": self._transport_connected,
                        "api_reachable": self._api_reachable,
                        "circuit_closed": not self._circuit_open,
                    },
                    "timestamp": datetime.now(UTC).isoformat(),
                },
                status=200,
            )
        else:
            # Determine reason for not being ready
            reasons = []
            if not self._transport_connected:
                reasons.append("transport_disconnected")
            if not self._api_reachable:
                reasons.append("api_unreachable")
            if self._circuit_open:
                reasons.append("circuit_open")

            return web.json_response(
                {
                    "status": "not_ready",
                    "worker": self.worker_name,
                    "reasons": reasons,
                    "checks": {
                        "transport_connected": self._transport_connected,
                        "api_reachable": self._api_reachable,
                        "circuit_closed": not self._circuit_open,
                    },
                    "timestamp": datetime.now(UTC).isoformat(),
                },
                status=503,
            )

    def create_app(self) -> web.Application:
        """Create aiohttp application with health endpoints."""
        app = web.Application()
        app.router.add_get("/health/live", self.handle_liveness)
        app.router.add_get("/health/ready", self.handle_readiness)
        return app

    async def _try_start_on_port(self, port: int) -> bool:
        """Try to start the health server on a specific port.

        Args:
            port: Port to bind to (0 for dynamic assignment)

        Returns:
            True if successful, False if port is in use
        """
        try:
            self._app = self.create_app()
            self._runner = web.AppRunner(self._app)
            await self._runner.setup()
            self._site = web.TCPSite(self._runner, "0.0.0.0", port)
            await self._site.start()

            # Capture actual port (important when using port=0 for dynamic assignment)
            if self._site._server and self._site._server.sockets:
                self._actual_port = self._site._server.sockets[0].getsockname()[1]
            else:
                self._actual_port = port

            return True
        except OSError as e:
            # Port in use: errno 98 (Linux) or 10048 (Windows)
            if e.errno in (98, 10048):
                # Clean up partial setup
                if self._runner:
                    await self._runner.cleanup()
                    self._runner = None
                    self._site = None
                    self._app = None
                return False
            raise

    async def start(self) -> None:
        """
        Start the health check HTTP server.

        Starts listening on the configured port for health check requests.
        If port is 0, an available port will be dynamically assigned.
        If the configured port is in use, falls back to dynamic port assignment.
        If all attempts fail, logs a warning and continues without health checks.
        """
        if not self._enabled:
            logger.debug(
                "Health check server is disabled, skipping start",
                extra={"worker_name": self.worker_name},
            )
            return

        try:
            # Try the configured port first
            if await self._try_start_on_port(self.port):
                logger.info(
                    "Health check server started",
                    extra={
                        "worker_name": self.worker_name,
                        "port": self._actual_port,
                        "liveness_endpoint": f"http://localhost:{self._actual_port}/health/live",
                        "readiness_endpoint": f"http://localhost:{self._actual_port}/health/ready",
                    },
                )
                return

            # Port was in use - try dynamic port if we weren't already using it
            if self.port != 0:
                logger.warning(
                    f"Port {self.port} in use, falling back to dynamic port assignment",
                    extra={"worker_name": self.worker_name, "original_port": self.port},
                )
                if await self._try_start_on_port(0):
                    logger.info(
                        "Health check server started on dynamic port",
                        extra={
                            "worker_name": self.worker_name,
                            "port": self._actual_port,
                            "liveness_endpoint": f"http://localhost:{self._actual_port}/health/live",
                            "readiness_endpoint": f"http://localhost:{self._actual_port}/health/ready",
                        },
                    )
                    return

            # All attempts failed - disable health checks but don't crash
            logger.warning(
                "Could not start health check server, continuing without health checks",
                extra={"worker_name": self.worker_name},
            )
            self._enabled = False

        except Exception as e:
            logger.error(
                f"Failed to start health check server: {e}",
                extra={"worker_name": self.worker_name, "port": self.port},
                exc_info=True,
            )
            # Don't crash the worker - just disable health checks
            logger.warning(
                "Continuing without health checks due to startup error",
                extra={"worker_name": self.worker_name},
            )
            self._enabled = False

    async def stop(self) -> None:
        """
        Stop the health check HTTP server.

        Cleans up resources and stops listening for requests.
        """
        if not self._enabled:
            return

        if self._runner:
            try:
                await self._runner.cleanup()
                logger.info(
                    "Health check server stopped",
                    extra={"worker_name": self.worker_name},
                )
            except Exception as e:
                logger.error(
                    f"Error stopping health check server: {e}",
                    extra={"worker_name": self.worker_name},
                    exc_info=True,
                )
            finally:
                self._runner = None
                self._site = None
                self._app = None
                self._actual_port = None

    @property
    def is_ready(self) -> bool:
        return self._ready

    @property
    def actual_port(self) -> int | None:
        """Get the actual port the server is listening on.

        This is particularly useful when using port=0 for dynamic assignment.
        Returns None if the server is not running or is disabled.
        """
        return self._actual_port

    @property
    def is_enabled(self) -> bool:
        return self._enabled


__all__ = ["HealthCheckServer"]
