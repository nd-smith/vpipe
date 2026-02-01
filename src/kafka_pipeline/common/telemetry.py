"""
Telemetry initialization and configuration using Prometheus.

Provides centralized telemetry setup for metrics:
- Metrics using Prometheus client → Prometheus
- Resource attributes (service.name, deployment.environment)

Telemetry is fully optional:
- Set ENABLE_TELEMETRY=false to disable
- If prometheus-client not available, gracefully degrades to no-op
"""

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

_initialized = False
_telemetry_available = False
_prometheus_registry: Any | None = None


def initialize_telemetry(
    service_name: str,
    environment: str = "development",
    enable_metrics: bool = True,
) -> None:
    """
    Initialize Prometheus telemetry.

    Sets up metrics export to Prometheus.
    This should be called once at application startup.

    Args:
        service_name: Name of the service (e.g., "xact-event-ingester")
        environment: Deployment environment (development, staging, production)
        enable_metrics: Enable metric export to Prometheus

    Example:
        >>> initialize_telemetry(
        ...     service_name="xact-download-worker",
        ...     environment="production"
        ... )

    Environment Variables:
        ENABLE_TELEMETRY: Set to "false" or "0" to disable telemetry completely
    """
    global _initialized, _telemetry_available, _prometheus_registry

    if _initialized:
        logger.warning("Telemetry already initialized, skipping")
        return

    # Check if telemetry is disabled via environment variable
    telemetry_enabled = os.getenv("ENABLE_TELEMETRY", "true").lower() not in (
        "false",
        "0",
        "no",
    )
    if not telemetry_enabled:
        logger.info("Telemetry disabled via ENABLE_TELEMETRY environment variable")
        _initialized = True
        return

    logger.info(
        "Initializing telemetry",
        extra={
            "service_name": service_name,
            "environment": environment,
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
        logger.warning(
            f"prometheus-client not available: {e}. Metrics will be disabled."
        )
        enable_metrics = False

    _initialized = True

    if not enable_metrics:
        logger.warning("Telemetry initialized but metrics are disabled")
    else:
        logger.info("Telemetry initialization complete")


def initialize_worker_telemetry(domain: str, worker_name: str) -> None:
    """Initialize telemetry for a worker with standard naming convention.

    Args:
        domain: Pipeline domain (e.g., "verisk", "claimx")
        worker_name: Worker type (e.g., "download-worker", "enrichment-worker")
    """
    environment = os.getenv("ENVIRONMENT", "development")
    service_name = f"{domain}-{worker_name}"
    initialize_telemetry(service_name=service_name, environment=environment)


def get_prometheus_registry() -> Any | None:
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

    This should be called on application shutdown to flush any pending metrics.
    """
    global _initialized

    if not _initialized:
        return

    logger.info("Shutting down telemetry")
    _initialized = False
    logger.info("Telemetry shutdown complete")
