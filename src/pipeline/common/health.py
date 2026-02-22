"""
Health check endpoints for pipeline workers.

Provides Kubernetes-compatible health check endpoints:
- /health/live - Liveness probe (is the worker running?)
- /health/ready - Readiness probe (is the worker ready to process work?)

This module is domain-agnostic and can be used by any worker (xact, claimx, etc.).

Usage:
    from pipeline.common.health import HealthCheckServer

    # In worker __init__
    self.health_server = HealthCheckServer(port=8080, worker_name="xact-downloader")

    # In worker start()
    await self.health_server.start()

    # Update readiness status
    self.health_server.set_ready(transport_connected=True, api_reachable=True)

    # In worker stop()
    await self.health_server.stop()
"""

import asyncio
import logging
import threading
from datetime import UTC, datetime

from aiohttp import web

logger = logging.getLogger(__name__)


class HealthCheckServer:
    """
    HTTP server for Kubernetes health check endpoints.

    Provides two endpoints compatible with Kubernetes probes:
    - /health/live: Returns 200 if worker is running
    - /health/ready: Returns 200 if worker is ready to process work

    Liveness Check:
        Always returns 200 OK if the server is running.
        Kubernetes uses this to detect if the pod needs to be restarted.

    Readiness Check:
        Returns 200 OK only if:
        - Transport connection is established
        - External dependencies are reachable (API, etc.)
        - No critical errors prevent processing

        Returns 503 Service Unavailable otherwise.
        Kubernetes uses this to control traffic routing.

    Example:
        >>> health_server = HealthCheckServer(port=8080)
        >>> await health_server.start()
        >>> health_server.set_ready(transport_connected=True, api_reachable=True)
        >>> # Later...
        >>> health_server.set_ready(transport_connected=False)
        >>> await health_server.stop()
    """

    def __init__(
        self,
        port: int | None = 8080,
        worker_name: str = "worker",
        enabled: bool = True,
        heartbeat_timeout_seconds: float = 60.0,
    ):
        """
        Initialize health check server.

        Args:
            port: HTTP port to listen on. Use 0 for dynamic port assignment,
                  or None to disable the server (default: 8080)
            worker_name: Name of the worker for logging
            enabled: Whether to enable the health check server (default: True).
                     If False, start() and stop() become no-ops.
            heartbeat_timeout_seconds: Max seconds since last heartbeat before
                liveness probe returns 503.  Workers call record_heartbeat()
                from their event loop; if the loop stalls, the heartbeat goes
                stale and Kubernetes restarts the pod.  Set to 0 to disable
                heartbeat checking (original always-200 behavior).
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
        self._error_message: str | None = None  # Configuration/startup error

        # Event-loop heartbeat: updated by workers via record_heartbeat()
        self._heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self._last_heartbeat: float | None = None  # None until first heartbeat

        # aiohttp components
        self._app: web.Application | None = None
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None

        # Thread management for isolated event loop
        self._thread: threading.Thread | None = None
        self._thread_loop: asyncio.AbstractEventLoop | None = None
        self._thread_ready = threading.Event()
        self._server_started = threading.Event()  # Signals when server is listening
        self._shutdown_event = threading.Event()
        self._state_lock = threading.Lock()  # Protects state variables

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
        with self._state_lock:
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

    def set_error(self, error_message: str) -> None:
        """
        Set an error state that prevents readiness.

        This is used when the worker encounters a configuration or startup
        error that prevents it from operating normally. The worker will
        remain alive (for debugging/inspection) but report as not ready.

        Args:
            error_message: Human-readable description of the error
        """
        with self._state_lock:
            self._error_message = error_message
            self._ready = False
            logger.error(
                f"Health check error state set: {error_message}",
                extra={
                    "worker_name": self.worker_name,
                    "error": error_message,
                },
            )

    def clear_error(self) -> None:
        """Clear the error state."""
        with self._state_lock:
            if self._error_message:
                logger.info(
                    "Health check error state cleared",
                    extra={"worker_name": self.worker_name},
                )
            self._error_message = None

    @property
    def error_message(self) -> str | None:
        """Get the current error message, if any."""
        with self._state_lock:
            return self._error_message

    def record_heartbeat(self) -> None:
        """Record a heartbeat from the main event loop.

        Workers should call this periodically (e.g. on every on_event callback,
        including idle wakeups) to prove the event loop is responsive.
        If the heartbeat goes stale, the liveness probe returns 503 so
        Kubernetes can restart the pod.
        """
        import time as _time

        with self._state_lock:
            self._last_heartbeat = _time.monotonic()

    async def handle_liveness(self, request: web.Request) -> web.Response:
        """
        Handle GET /health/live - Liveness probe.

        Returns 200 OK if the event loop is responsive (heartbeat is fresh).
        Returns 503 if the heartbeat is stale, signalling Kubernetes to
        restart the pod.

        If heartbeat checking is disabled (timeout=0) or no heartbeat has been
        recorded yet, always returns 200 (original behavior).

        Returns:
            200 OK with status and uptime, or 503 if heartbeat is stale
        """
        import time as _time

        with self._state_lock:
            started_at = self._started_at
            last_hb = self._last_heartbeat
            hb_timeout = self._heartbeat_timeout_seconds

        uptime_seconds = (datetime.now(UTC) - started_at).total_seconds()

        # Check heartbeat staleness (only if enabled and at least one heartbeat recorded)
        if hb_timeout > 0 and last_hb is not None:
            staleness = _time.monotonic() - last_hb
            if staleness > hb_timeout:
                logger.warning(
                    "Liveness check failed: event loop heartbeat stale",
                    extra={
                        "worker_name": self.worker_name,
                        "staleness_seconds": round(staleness, 1),
                        "timeout_seconds": hb_timeout,
                    },
                )
                return web.json_response(
                    {
                        "status": "unhealthy",
                        "reason": "event_loop_stale",
                        "worker": self.worker_name,
                        "heartbeat_staleness_seconds": round(staleness, 1),
                        "heartbeat_timeout_seconds": hb_timeout,
                        "uptime_seconds": int(uptime_seconds),
                        "timestamp": datetime.now(UTC).isoformat(),
                    },
                    status=503,
                )

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
        # Read state with lock to ensure consistency
        with self._state_lock:
            error_message = self._error_message
            ready = self._ready
            transport_connected = self._transport_connected
            api_reachable = self._api_reachable
            circuit_open = self._circuit_open

        # Check for error state first - return 200 to allow deployment to complete
        # The error details are visible in the response body for debugging
        if error_message:
            return web.json_response(
                {
                    "status": "error",
                    "worker": self.worker_name,
                    "error": error_message,
                    "reasons": ["configuration_error"],
                    "timestamp": datetime.now(UTC).isoformat(),
                },
                status=200,  # Return 200 so K8s deployment completes
            )

        if ready:
            return web.json_response(
                {
                    "status": "ready",
                    "worker": self.worker_name,
                    "checks": {
                        "transport_connected": transport_connected,
                        "api_reachable": api_reachable,
                        "circuit_closed": not circuit_open,
                    },
                    "timestamp": datetime.now(UTC).isoformat(),
                },
                status=200,
            )
        else:
            # Determine reason for not being ready
            reasons = []
            if not transport_connected:
                reasons.append("transport_disconnected")
            if not api_reachable:
                reasons.append("api_unreachable")
            if circuit_open:
                reasons.append("circuit_open")

            return web.json_response(
                {
                    "status": "not_ready",
                    "worker": self.worker_name,
                    "reasons": reasons,
                    "checks": {
                        "transport_connected": transport_connected,
                        "api_reachable": api_reachable,
                        "circuit_closed": not circuit_open,
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

    def _run_server_thread(self) -> None:
        """Entry point for health server thread."""
        try:
            # Create new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._thread_loop = loop

            # Signal that thread is ready
            self._thread_ready.set()

            # Run the server until shutdown
            loop.run_until_complete(self._start_server_async())

        except Exception as e:
            logger.error(
                f"Health server thread error: {e}",
                extra={"worker_name": self.worker_name},
                exc_info=True,
            )
        finally:
            if self._thread_loop:
                self._thread_loop.close()

    async def _start_server_async(self) -> None:
        """Start aiohttp server in thread's event loop."""
        try:
            # Try configured port
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
                # Signal that server is now listening
                self._server_started.set()
            elif self.port != 0 and await self._try_start_on_port(0):
                logger.warning(
                    f"Port {self.port} in use, falling back to dynamic port assignment",
                    extra={"worker_name": self.worker_name, "original_port": self.port},
                )
                logger.info(
                    "Health check server started on dynamic port",
                    extra={
                        "worker_name": self.worker_name,
                        "port": self._actual_port,
                        "liveness_endpoint": f"http://localhost:{self._actual_port}/health/live",
                        "readiness_endpoint": f"http://localhost:{self._actual_port}/health/ready",
                    },
                )
                # Signal that server is now listening
                self._server_started.set()
            else:
                logger.warning(
                    "Could not start health check server",
                    extra={"worker_name": self.worker_name},
                )
                # Signal startup complete even on failure (prevents deadlock)
                self._server_started.set()
                return

            # Wait for shutdown signal
            while not self._shutdown_event.is_set():
                await asyncio.sleep(0.5)

        finally:
            if self._runner:
                await self._runner.cleanup()

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
            self._site = web.TCPSite(self._runner, "0.0.0.0", port, reuse_address=True)
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

        Starts listening on the configured port for health check requests in a dedicated thread.
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

        # Already running - no-op for idempotent calls
        if self._thread is not None and self._thread.is_alive():
            return

        try:
            # Start health server in dedicated thread
            self._thread = threading.Thread(
                target=self._run_server_thread,
                name=f"health-server-{self.worker_name}",
                daemon=True,  # Don't prevent process exit
            )
            self._thread.start()

            # Wait for thread to be ready (with timeout)
            if not self._thread_ready.wait(timeout=5.0):
                logger.error(
                    "Health server thread failed to start",
                    extra={"worker_name": self.worker_name},
                )
                self._enabled = False
                return

            # Wait for server to actually start listening (with timeout)
            if not self._server_started.wait(timeout=5.0):
                logger.error(
                    "Health server failed to start listening",
                    extra={"worker_name": self.worker_name},
                )
                self._enabled = False
                return

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
        if not self._enabled or not self._thread:
            return

        try:
            # Signal thread to shutdown
            self._shutdown_event.set()

            # Wait for thread to finish (with timeout)
            self._thread.join(timeout=5.0)

            if self._thread.is_alive():
                logger.warning(
                    "Health server thread did not stop cleanly",
                    extra={"worker_name": self.worker_name},
                )
            else:
                logger.info(
                    "Health check server stopped",
                    extra={"worker_name": self.worker_name},
                )

            # Reset state for potential restart
            self._actual_port = None
            self._thread_ready.clear()
            self._server_started.clear()
            self._shutdown_event.clear()

        except Exception as e:
            logger.error(
                f"Error stopping health check server: {e}",
                extra={"worker_name": self.worker_name},
                exc_info=True,
            )

    @property
    def is_ready(self) -> bool:
        """Check if worker is currently ready."""
        with self._state_lock:
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
        """Check if health check server is enabled."""
        return self._enabled


__all__ = ["HealthCheckServer"]
