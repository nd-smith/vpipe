# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
#
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""
Telemetry initialization and configuration using Prometheus + Jaeger/OpenTracing.

Provides centralized telemetry setup for distributed tracing and metrics:
- Tracer using Jaeger client (OpenTracing API)
- Metrics using Prometheus client → Prometheus
- W3C Trace Context propagation for Kafka headers
- 100% sampling (ALWAYS_ON)
- Resource attributes (service.name, deployment.environment)

Telemetry is fully optional:
- Set ENABLE_TELEMETRY=false to disable
- If prometheus-client or jaeger-client not available, gracefully degrades to no-op
"""

import logging
import os
from typing import Optional, Any

logger = logging.getLogger(__name__)

_initialized = False
_telemetry_available = False
_tracer: Optional[Any] = None
_prometheus_registry: Optional[Any] = None


class SpanKind:
    """Span kind constants for OpenTracing compatibility."""

    INTERNAL = 0
    SERVER = 1
    CLIENT = 2
    PRODUCER = 3
    CONSUMER = 4


class NoOpTracer:
    """No-op tracer when telemetry is disabled or unavailable."""

    def start_span(self, operation_name: str, **kwargs):
        """Return a no-op span."""
        return NoOpSpan()

    def start_active_span(self, operation_name: str, **kwargs):
        """Return a no-op context manager."""
        return NoOpSpanContext()

    def start_as_current_span(self, operation_name: str, **kwargs):
        """Return a no-op context manager (OpenTracing API compatibility)."""
        return NoOpSpanContext()


class NoOpSpan:
    """No-op span when telemetry is disabled or unavailable."""

    def set_tag(self, key: str, value: Any):
        """No-op set_tag (OpenTracing API)."""
        pass

    def set_attribute(self, key: str, value: Any):
        """No-op set_attribute (OpenTracing API)."""
        pass

    def set_attributes(self, attributes: dict):
        """No-op set_attributes (OpenTracing API)."""
        pass

    def add_event(self, name: str, attributes: dict = None):
        """No-op add_event (OpenTracing API)."""
        pass

    def record_exception(self, exception: Exception, attributes: dict = None):
        """No-op record_exception (OpenTracing API)."""
        pass

    def set_status(self, status: Any, description: str = None):
        """No-op set_status (OpenTracing API)."""
        pass

    def log_kv(self, kv: dict):
        """No-op log_kv (OpenTracing API)."""
        pass

    def finish(self):
        """No-op finish."""
        pass

    def is_recording(self) -> bool:
        """Return False for no-op span."""
        return False

    def get_span_context(self):
        """Return None for no-op span context."""
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class NoOpSpanContext:
    """No-op span context manager when telemetry is disabled or unavailable."""

    def __enter__(self):
        return NoOpSpan()

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


def initialize_telemetry(
    service_name: str,
    environment: str = "development",
    jaeger_endpoint: Optional[str] = None,
    enable_traces: bool = True,
    enable_metrics: bool = True,
) -> None:
    """
    Initialize Prometheus and Jaeger telemetry with 100% sampling.

    Sets up distributed tracing and metrics export to Jaeger and Prometheus.
    This should be called once at application startup.

    Args:
        service_name: Name of the service (e.g., "xact-event-ingester")
        environment: Deployment environment (development, staging, production)
        jaeger_endpoint: Jaeger agent endpoint (default: localhost:6831)
        enable_traces: Enable trace export to Jaeger
        enable_metrics: Enable metric export to Prometheus

    Example:
        >>> initialize_telemetry(
        ...     service_name="xact-download-worker",
        ...     environment="production",
        ...     jaeger_endpoint="localhost:6831"
        ... )

    Environment Variables:
        ENABLE_TELEMETRY: Set to "false" or "0" to disable telemetry completely
    """
    global _initialized, _telemetry_available, _tracer, _prometheus_registry

    if _initialized:
        logger.warning("Telemetry already initialized, skipping")
        return

    # Check if telemetry is disabled via environment variable
    telemetry_enabled = os.getenv("ENABLE_TELEMETRY", "true").lower() not in ("false", "0", "no")
    if not telemetry_enabled:
        logger.info("Telemetry disabled via ENABLE_TELEMETRY environment variable")
        _initialized = True
        return

    # Get endpoint from environment if not provided
    if jaeger_endpoint is None:
        jaeger_endpoint = os.getenv("JAEGER_ENDPOINT", "localhost:6831")

    logger.info(
        "Initializing telemetry",
        extra={
            "service_name": service_name,
            "environment": environment,
            "jaeger_endpoint": jaeger_endpoint,
            "enable_traces": enable_traces,
            "enable_metrics": enable_metrics,
        },
    )

    # Try to import telemetry libraries
    try:
        if enable_metrics:
            import prometheus_client

            _prometheus_registry = prometheus_client.CollectorRegistry()
            _telemetry_available = True
            logger.info("Prometheus client loaded successfully")
    except ImportError as e:
        logger.warning(f"prometheus-client not available: {e}. Metrics will be disabled.")
        enable_metrics = False

    try:
        if enable_traces:
            from jaeger_client import Config

            _telemetry_available = True

            # Initialize Jaeger tracer with 100% sampling
            config = Config(
                config={
                    "sampler": {
                        "type": "const",
                        "param": 1,  # 100% sampling (ALWAYS_ON)
                    },
                    "local_agent": {
                        "reporting_host": jaeger_endpoint.split(":")[0],
                        "reporting_port": (
                            int(jaeger_endpoint.split(":")[1]) if ":" in jaeger_endpoint else 6831
                        ),
                    },
                    "logging": True,
                },
                service_name=service_name,
                validate=True,
            )
            _tracer = config.initialize_tracer()
            logger.info(
                "Tracing initialized",
                extra={
                    "endpoint": jaeger_endpoint,
                    "sampler": "const=1 (ALWAYS_ON)",
                },
            )
    except ImportError as e:
        logger.warning(f"jaeger-client not available: {e}. Tracing will be disabled.")
        enable_traces = False
    except Exception as e:
        logger.error("Failed to initialize tracing", exc_info=True)
        enable_traces = False

    _initialized = True

    if not enable_traces and not enable_metrics:
        logger.warning("Telemetry initialized but both traces and metrics are disabled")
    else:
        logger.info("Telemetry initialization complete")


def get_tracer(name: str) -> Any:
    """
    Get a tracer instance for creating spans.

    Args:
        name: Name of the tracer (typically __name__ of the module)

    Returns:
        Tracer instance for creating spans (or no-op tracer if unavailable)

    Example:
        >>> tracer = get_tracer(__name__)
        >>> with tracer.start_active_span("operation"):
        ...     # Do work
        ...     pass
    """
    if _tracer is not None:
        return _tracer
    return NoOpTracer()


def get_prometheus_registry() -> Optional[Any]:
    """
    Get the Prometheus registry for creating metrics.

    Returns:
        Prometheus CollectorRegistry instance (or None if unavailable)

    Example:
        >>> registry = get_prometheus_registry()
        >>> if registry:
        ...     counter = Counter('requests_total', 'Total requests', registry=registry)
    """
    return _prometheus_registry


def is_initialized() -> bool:
    """Check if telemetry has been initialized."""
    return _initialized


def is_available() -> bool:
    """Check if telemetry libraries are available and loaded."""
    return _telemetry_available


def shutdown_telemetry() -> None:
    """
    Shutdown telemetry providers gracefully.

    This should be called on application shutdown to flush any pending spans/metrics.
    """
    global _initialized, _tracer

    if not _initialized:
        return

    logger.info("Shutting down telemetry")

    # Close Jaeger tracer
    if _tracer is not None:
        try:
            _tracer.close()
            logger.info("Jaeger tracer closed")
        except Exception as e:
            logger.error(f"Error closing Jaeger tracer: {e}")

    _initialized = False
    logger.info("Telemetry shutdown complete")
