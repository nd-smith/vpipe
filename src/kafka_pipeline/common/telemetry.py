"""
OpenTelemetry initialization and configuration.

Provides centralized telemetry setup for distributed tracing and metrics:
- Tracer provider with Jaeger exporter (OTLP/gRPC)
- Meter provider with OTLP exporter → Prometheus via OTel Collector
- W3C Trace Context propagation for Kafka headers
- 100% sampling (ALWAYS_ON)
- Resource attributes (service.name, deployment.environment)
"""

import logging
import os
from typing import Optional

from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.sampling import ALWAYS_ON
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, DEPLOYMENT_ENVIRONMENT
from opentelemetry.propagate import set_global_textmap
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

logger = logging.getLogger(__name__)

_initialized = False


def initialize_telemetry(
    service_name: str,
    environment: str = "development",
    jaeger_endpoint: Optional[str] = None,
    enable_traces: bool = True,
    enable_metrics: bool = True,
) -> None:
    """
    Initialize OpenTelemetry with 100% sampling.

    Sets up distributed tracing and metrics export to Jaeger and OTel Collector.
    This should be called once at application startup.

    Args:
        service_name: Name of the service (e.g., "xact-event-ingester")
        environment: Deployment environment (development, staging, production)
        jaeger_endpoint: Jaeger OTLP endpoint (default: http://localhost:4317)
        enable_traces: Enable trace export to Jaeger
        enable_metrics: Enable metric export to OTel Collector

    Example:
        >>> initialize_telemetry(
        ...     service_name="xact-download-worker",
        ...     environment="production",
        ...     jaeger_endpoint="http://jaeger:4317"
        ... )

    Environment Variables:
        ENABLE_TELEMETRY: Set to "false" or "0" to disable telemetry completely
    """
    global _initialized

    if _initialized:
        logger.warning("OpenTelemetry already initialized, skipping")
        return

    # Check if telemetry is disabled via environment variable
    telemetry_enabled = os.getenv("ENABLE_TELEMETRY", "true").lower() not in ("false", "0", "no")
    if not telemetry_enabled:
        logger.info("OpenTelemetry disabled via ENABLE_TELEMETRY environment variable")
        _initialized = True
        return

    # Get endpoint from environment if not provided
    if jaeger_endpoint is None:
        jaeger_endpoint = os.getenv("JAEGER_ENDPOINT", "http://localhost:4317")

    logger.info(
        "Initializing OpenTelemetry",
        extra={
            "service_name": service_name,
            "environment": environment,
            "jaeger_endpoint": jaeger_endpoint,
            "enable_traces": enable_traces,
            "enable_metrics": enable_metrics,
        },
    )

    # Resource attributes for all telemetry signals
    resource = Resource.create(
        {
            SERVICE_NAME: service_name,
            DEPLOYMENT_ENVIRONMENT: environment,
            "service.version": "0.1.0",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.language": "python",
        }
    )

    # Initialize tracing
    if enable_traces:
        _initialize_tracing(jaeger_endpoint, resource)

    # Initialize metrics
    if enable_metrics:
        _initialize_metrics(jaeger_endpoint, resource)

    # Set W3C Trace Context as global propagator
    set_global_textmap(TraceContextTextMapPropagator())

    _initialized = True
    logger.info("OpenTelemetry initialization complete")


def _initialize_tracing(endpoint: str, resource: Resource) -> None:
    """Initialize tracing with Jaeger exporter."""
    try:
        # Create tracer provider with ALWAYS_ON (100% sampling)
        tracer_provider = TracerProvider(
            resource=resource,
            sampler=ALWAYS_ON,
        )

        # Configure OTLP span exporter to Jaeger
        span_exporter = OTLPSpanExporter(
            endpoint=endpoint,
            insecure=True,  # For local development; use TLS in production
        )

        # BatchSpanProcessor for efficient batching
        # Exports in batches of 512 spans or every 5 seconds
        span_processor = BatchSpanProcessor(
            span_exporter,
            max_queue_size=2048,
            max_export_batch_size=512,
            schedule_delay_millis=5000,
        )

        tracer_provider.add_span_processor(span_processor)

        # Set as global tracer provider
        trace.set_tracer_provider(tracer_provider)

        logger.info(
            "Tracing initialized",
            extra={
                "endpoint": endpoint,
                "sampler": "ALWAYS_ON",
                "batch_size": 512,
            },
        )
    except Exception as e:
        logger.error("Failed to initialize tracing", exc_info=True)
        raise


def _initialize_metrics(endpoint: str, resource: Resource) -> None:
    """Initialize metrics with OTel Collector exporter."""
    try:
        # Configure OTLP metric exporter
        metric_exporter = OTLPMetricExporter(
            endpoint=endpoint,
            insecure=True,  # For local development; use TLS in production
        )

        # Periodic exporter - exports metrics every 60 seconds
        metric_reader = PeriodicExportingMetricReader(
            exporter=metric_exporter,
            export_interval_millis=60000,  # 60 seconds
        )

        # Create meter provider
        meter_provider = MeterProvider(
            resource=resource,
            metric_readers=[metric_reader],
        )

        # Set as global meter provider
        metrics.set_meter_provider(meter_provider)

        logger.info(
            "Metrics initialized",
            extra={
                "endpoint": endpoint,
                "export_interval_seconds": 60,
            },
        )
    except Exception as e:
        logger.error("Failed to initialize metrics", exc_info=True)
        raise


def get_tracer(name: str) -> trace.Tracer:
    """
    Get a tracer instance for creating spans.

    Args:
        name: Name of the tracer (typically __name__ of the module)

    Returns:
        Tracer instance for creating spans

    Example:
        >>> tracer = get_tracer(__name__)
        >>> with tracer.start_as_current_span("operation"):
        ...     # Do work
        ...     pass
    """
    return trace.get_tracer(name)


def get_meter(name: str) -> metrics.Meter:
    """
    Get a meter instance for creating metrics.

    Args:
        name: Name of the meter (typically __name__ of the module)

    Returns:
        Meter instance for creating counters, gauges, histograms

    Example:
        >>> meter = get_meter(__name__)
        >>> counter = meter.create_counter("requests.count")
        >>> counter.add(1, attributes={"status": "success"})
    """
    return metrics.get_meter(name)


def is_initialized() -> bool:
    """Check if OpenTelemetry has been initialized."""
    return _initialized


def shutdown_telemetry() -> None:
    """
    Shutdown telemetry providers gracefully.

    This should be called on application shutdown to flush any pending spans/metrics.
    """
    global _initialized

    if not _initialized:
        return

    logger.info("Shutting down OpenTelemetry")

    # Get providers and shut them down
    tracer_provider = trace.get_tracer_provider()
    if hasattr(tracer_provider, "shutdown"):
        tracer_provider.shutdown()

    meter_provider = metrics.get_meter_provider()
    if hasattr(meter_provider, "shutdown"):
        meter_provider.shutdown()

    _initialized = False
    logger.info("OpenTelemetry shutdown complete")
