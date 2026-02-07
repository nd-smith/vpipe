"""EventHub pipeline worker orchestration. Use --help for usage."""

import argparse
import asyncio
import logging
import os
import signal
import sys
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from prometheus_client import REGISTRY, start_http_server

from core.logging.eventhub_config import prepare_eventhub_logging_config
from core.logging.setup import (
    setup_logging,
    setup_multi_worker_logging,
    upload_crash_logs,
)
from core.logging.utilities import get_log_output_mode
from pipeline.common.health import HealthCheckServer
from pipeline.runners.registry import WORKER_REGISTRY, run_worker_from_registry

# Project root directory (where .env file is located)
# __main__.py is at src/pipeline/__main__.py, so root is 3 levels up
PROJECT_ROOT = Path(__file__).parent.parent.parent

# Worker stages for multi-worker logging
WORKER_STAGES = list(WORKER_REGISTRY.keys())

# Placeholder logger until setup_logging() is called in main()
# This allows module-level logging before full initialization
logger = logging.getLogger(__name__)

# Global shutdown event for graceful batch completion
# Set by signal handlers, checked by workers to finish current batch before exiting
_shutdown_event: asyncio.Event | None = None


def get_shutdown_event() -> asyncio.Event:
    global _shutdown_event
    if _shutdown_event is None:
        _shutdown_event = asyncio.Event()
    return _shutdown_event


async def run_error_mode(worker_name: str, error_msg: str) -> None:
    """Run health server in error state until shutdown signal.

    This is a fallback for errors that occur before worker health servers exist:
    - Configuration errors (before worker instantiation)
    - Worker instantiation errors (before health server created)
    - Errors from workers without health servers (legacy/plugin workers)

    Workers with integrated health servers handle their own error mode via
    the runner functions in runners/common.py.
    """
    shutdown_event = get_shutdown_event()

    health_server = HealthCheckServer(
        port=8080,
        worker_name=worker_name,
        enabled=True,
    )
    health_server.set_error(error_msg)

    await health_server.start()
    logger.info(
        "Health server running in error mode (top-level fallback)",
        extra={
            "port": health_server.actual_port,
            "worker": worker_name,
            "error": error_msg,
        },
    )

    await asyncio.to_thread(upload_crash_logs, error_msg)

    await shutdown_event.wait()
    logger.info("Shutdown signal received in error mode")
    await health_server.stop()


def enter_error_mode(
    loop: asyncio.AbstractEventLoop, worker_name: str, error_msg: str
) -> None:
    """Enter error mode with health server running until shutdown.

    Fallback error mode for cases where worker health server doesn't exist:
    - Configuration errors (before worker creation)
    - Worker instantiation errors (before health server initialized)
    - Workers without health servers (will reach here via runner re-raise)

    Most workers now handle their own error mode via runners/common.py, so
    this primarily handles pre-worker and configuration errors.
    """
    logger.warning("Entering ERROR MODE - health endpoint will remain alive")
    try:
        loop.run_until_complete(run_error_mode(worker_name, error_msg))
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received in error mode, shutting down...")


def load_dev_config():
    """Load configuration for development mode (EventHub only)."""
    logger.info("Running in DEVELOPMENT mode (EventHub only)")
    from config import get_config
    from config.pipeline_config import (
        EventSourceType,
        PipelineConfig,
    )

    kafka_config = get_config()

    pipeline_config = PipelineConfig(
        event_source=EventSourceType.EVENTHUB,
    )

    return pipeline_config, kafka_config, kafka_config


def load_production_config():
    """Load configuration for production mode (Event Hub/Eventhouse).

    Returns:
        Tuple of (pipeline_config, eventhub_config, kafka_config)

    Raises:
        ValueError: If configuration is invalid
    """
    from config import get_config
    from config.pipeline_config import EventSourceType, get_pipeline_config

    pipeline_config = get_pipeline_config()
    kafka_config = get_config()

    if pipeline_config.event_source == EventSourceType.EVENTHOUSE:
        logger.info("Running in PRODUCTION mode (Eventhouse → EventHub pipeline)")
        eventhub_config = None
    else:
        logger.info("Running in PRODUCTION mode (Event Hub → EventHub pipeline)")
        eventhub_config = pipeline_config.eventhub.to_kafka_config()

    return pipeline_config, eventhub_config, kafka_config


async def run_worker_pool(
    worker_fn: Callable[..., Coroutine[Any, Any, None]],
    count: int,
    worker_name: str,
    *args: Any,
    **kwargs: Any,
) -> None:
    """Run multiple instances of a worker concurrently.
    Each instance joins the same consumer group for automatic partition distribution.
    Each instance gets a unique instance_id for distinct logging and identity.
    """
    logger.info(
        "Starting worker instances", extra={"count": count, "worker_name": worker_name}
    )

    tasks = []
    for i in range(count):
        instance_id = str(i)

        # Pass instance_id to worker for distinct logging and Kafka client_id
        instance_kwargs = kwargs.copy()
        instance_kwargs["instance_id"] = instance_id

        task = asyncio.create_task(
            worker_fn(*args, **instance_kwargs),
            name=f"{worker_name}-{instance_id}",
        )
        tasks.append(task)

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info(
            "Worker pool cancelled, shutting down", extra={"worker_name": worker_name}
        )
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run EventHub pipeline workers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run all workers (Event Hub → EventHub pipeline)
    python -m pipeline

    # Run specific xact worker
    python -m pipeline --worker xact-download

    # Run specific claimx worker
    python -m pipeline --worker claimx-enricher

    # Run in development mode (EventHub transport)
    python -m pipeline --dev

    # Run with custom metrics port
    python -m pipeline --metrics-port 9090
        """,
    )

    parser.add_argument(
        "--worker",
        choices=WORKER_STAGES + ["all"],
        default="all",
        help="Which worker(s) to run (default: all)",
    )

    parser.add_argument(
        "--metrics-port",
        type=int,
        default=8000,
        help="Port for Prometheus metrics server (default: 8000)",
    )

    parser.add_argument(
        "--dev",
        action="store_true",
        help="Development mode: use EventHub transport (no Eventhouse credentials required)",
    )

    parser.add_argument(
        "--no-delta",
        action="store_true",
        help="Disable Delta Lake writes (for testing)",
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )

    parser.add_argument(
        "--log-dir",
        type=str,
        default=None,
        help="Log directory path (default: from LOG_DIR env var or ./logs)",
    )

    parser.add_argument(
        "--count",
        "-c",
        type=int,
        default=1,
        help="Number of worker instances to run concurrently (default: 1). "
        "Multiple instances share the same consumer group for automatic partition distribution.",
    )

    parser.add_argument(
        "--log-to-stdout",
        action="store_true",
        help="Send all log output to stdout only, skipping file handlers. "
        "Useful for containerized deployments where logs are captured from stdout. "
        "Can also be set via LOG_TO_STDOUT environment variable.",
    )

    return parser.parse_args()


async def run_all_workers(
    pipeline_config,
    enable_delta_writes: bool = True,
):
    """Run all pipeline workers concurrently.
    Architecture: events.raw → EventIngester → downloads.pending → DownloadWorker → ...
                  events.raw → DeltaEventsWorker → Delta table (parallel)"""
    from config import get_config
    from config.pipeline_config import EventSourceType
    from pipeline.runners import verisk_runners

    logger.info("Starting all pipeline workers...")

    kafka_config = get_config()
    shutdown_event = get_shutdown_event()

    tasks = []

    if pipeline_config.event_source == EventSourceType.EVENTHOUSE:
        events_table_path = (
            pipeline_config.verisk_eventhouse.verisk_events_table_path
            or pipeline_config.events_table_path
        )
    else:
        events_table_path = pipeline_config.events_table_path

    if pipeline_config.event_source == EventSourceType.EVENTHOUSE:
        tasks.append(
            asyncio.create_task(
                verisk_runners.run_eventhouse_poller(pipeline_config, shutdown_event),
                name="eventhouse-poller",
            )
        )
        tasks.append(
            asyncio.create_task(
                verisk_runners.run_local_event_ingester(
                    kafka_config,
                    shutdown_event,
                    domain=pipeline_config.domain,
                ),
                name="xact-event-ingester",
            )
        )
        logger.info("Using Eventhouse as event source")
    else:
        eventhub_config = pipeline_config.eventhub.to_kafka_config()
        tasks.append(
            asyncio.create_task(
                verisk_runners.run_event_ingester(
                    eventhub_config,
                    kafka_config,
                    shutdown_event,
                    domain=pipeline_config.domain,
                ),
                name="xact-event-ingester",
            )
        )
        logger.info("Using Event Hub as event source")

    if enable_delta_writes and events_table_path:
        tasks.append(
            asyncio.create_task(
                verisk_runners.run_delta_events_worker(
                    kafka_config, events_table_path, shutdown_event
                ),
                name="xact-delta-writer",
            )
        )
        logger.info("Delta events writer enabled")

        tasks.append(
            asyncio.create_task(
                verisk_runners.run_xact_retry_scheduler(kafka_config, shutdown_event),
                name="xact-retry-scheduler",
            )
        )
        logger.info("XACT unified retry scheduler enabled")

    tasks.extend(
        [
            asyncio.create_task(
                verisk_runners.run_download_worker(kafka_config, shutdown_event),
                name="xact-download",
            ),
            asyncio.create_task(
                verisk_runners.run_upload_worker(kafka_config, shutdown_event),
                name="xact-upload",
            ),
            asyncio.create_task(
                verisk_runners.run_result_processor(
                    kafka_config,
                    shutdown_event,
                    enable_delta_writes,
                    inventory_table_path=pipeline_config.inventory_table_path,
                    failed_table_path=pipeline_config.failed_table_path,
                ),
                name="xact-result-processor",
            ),
        ]
    )

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Workers cancelled, shutting down...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def start_metrics_server(preferred_port: int) -> int:
    """Start Prometheus metrics server with automatic port fallback.
    Returns actual port number that the server is listening on."""
    import socket

    # Try to get custom registry from telemetry, fallback to default
    try:
        from pipeline.common.telemetry import get_prometheus_registry

        registry = get_prometheus_registry()
        if registry is None:
            registry = REGISTRY
    except ImportError:
        registry = REGISTRY

    try:
        start_http_server(preferred_port, registry=registry)
        return preferred_port
    except OSError as e:
        if e.errno == 98:
            logger.info(
                "Port already in use, finding available port",
                extra={"preferred_port": preferred_port},
            )

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("", 0))
                s.listen(1)
                available_port = s.getsockname()[1]

            start_http_server(available_port, registry=registry)
            return available_port
        else:
            raise


def setup_signal_handlers(loop: asyncio.AbstractEventLoop):
    """Set up signal handlers for graceful shutdown.

    First CTRL+C: Sets shutdown event - workers finish current batch, flush data, commit offsets.
    Second CTRL+C: Forces immediate shutdown by cancelling all tasks.
    Note: Signal handlers not supported on Windows - KeyboardInterrupt used instead."""

    def handle_signal(sig):
        logger.info(
            "Received signal, initiating graceful shutdown", extra={"signal": sig.name}
        )
        shutdown_event = get_shutdown_event()
        if not shutdown_event.is_set():
            shutdown_event.set()
        else:
            logger.warning("Received second signal, forcing immediate shutdown...")
            for task in asyncio.all_tasks(loop):
                task.cancel()

    if sys.platform == "win32":
        logger.debug(
            "Signal handlers not supported on Windows, using KeyboardInterrupt"
        )
        return

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: handle_signal(s))


def main():
    load_dotenv(PROJECT_ROOT / ".env")

    disable_ssl = os.getenv("DISABLE_SSL_VERIFY", "false").lower() in (
        "true",
        "1",
        "yes",
    )
    if disable_ssl:
        print("[SSL] DISABLE_SSL_VERIFY is set - applying SSL bypass patches...")
        from core.security.ssl_dev_bypass import apply_ssl_dev_bypass

        apply_ssl_dev_bypass()
        print("[SSL] SSL verification disabled for all HTTP clients (urllib3, requests, Kusto SDK)")
    else:
        print(
            "[SSL] SSL verification ENABLED (set DISABLE_SSL_VERIFY=true to disable)"
        )

    global logger
    args = parse_args()

    log_level = getattr(logging, args.log_level)

    json_logs = os.getenv("JSON_LOGS", "true").lower() in ("true", "1", "yes")

    log_dir_str = args.log_dir or os.getenv("LOG_DIR", "logs")
    log_dir = Path(log_dir_str)

    log_to_stdout = args.log_to_stdout or os.getenv(
        "LOG_TO_STDOUT", "false"
    ).lower() in (
        "true",
        "1",
        "yes",
    )

    # Generate unique worker ID with hostname/pod info
    import socket
    hostname = socket.gethostname()
    worker_id = os.getenv("WORKER_ID", f"{args.worker}@{hostname}")

    domain = "kafka"
    if args.worker != "all" and "-" in args.worker:
        domain_prefix = args.worker.split("-")[0]
        if domain_prefix in ("xact", "claimx"):
            domain = domain_prefix

    # Load config early to get logging configuration
    from config import load_config

    try:
        config = load_config()
    except Exception as e:
        # If config loading fails, setup basic logging without EventHub
        logger = logging.getLogger(__name__)
        logger.warning(
            "Failed to load config for logging setup, continuing without EventHub logging",
            extra={"error": str(e)},
        )
        config = None

    # Prepare EventHub logging config
    eventhub_config = (
        prepare_eventhub_logging_config(config.logging_config) if config else None
    )

    # Get toggle flags
    file_enabled = True
    eventhub_enabled = True
    if config:
        file_enabled = config.logging_config.get("file_logging", {}).get("enabled", True)
        eventhub_enabled = config.logging_config.get("eventhub_logging", {}).get(
            "enabled", True
        )

    if args.worker == "all":
        setup_multi_worker_logging(
            workers=WORKER_STAGES,
            domain="kafka",
            log_dir=log_dir,
            json_format=json_logs,
            console_level=log_level,
            log_to_stdout=log_to_stdout,
            eventhub_config=eventhub_config,
            enable_file_logging=file_enabled,
            enable_eventhub_logging=eventhub_enabled,
        )
    else:
        setup_logging(
            name="pipeline",
            stage=args.worker,
            domain=domain,
            log_dir=log_dir,
            json_format=json_logs,
            console_level=log_level,
            worker_id=worker_id,
            log_to_stdout=log_to_stdout,
            eventhub_config=eventhub_config,
            enable_file_logging=file_enabled,
            enable_eventhub_logging=eventhub_enabled,
        )

    logger = logging.getLogger(__name__)

    # Determine log output mode and print startup info
    log_output_mode = get_log_output_mode(
        log_to_stdout=log_to_stdout,
        enable_file_logging=file_enabled,
        enable_eventhub_logging=eventhub_enabled,
    )

    # Print to stdout for immediate visibility
    print(f"[STARTUP] Log output mode: {log_output_mode}")
    print(f"[STARTUP] Worker ID: {worker_id}")

    # Display EventHub log destination if EventHub logging is enabled
    if eventhub_config and eventhub_enabled:
        # Parse EventHub namespace from connection string
        conn_str = eventhub_config.get("connection_string", "")
        eventhub_name = eventhub_config.get("eventhub_name", "unknown")

        # Extract endpoint from connection string (format: Endpoint=sb://namespace.servicebus.windows.net/;...)
        eventhub_namespace = "unknown"
        if "Endpoint=sb://" in conn_str:
            try:
                endpoint_part = conn_str.split("Endpoint=sb://")[1].split("/")[0]
                eventhub_namespace = endpoint_part
            except (IndexError, AttributeError):
                pass

        print(f"[STARTUP] EventHub logs destination: {eventhub_namespace}/{eventhub_name}")

    # Create event loop early so we can start health server immediately
    print("[STARTUP] Creating event loop...")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    setup_signal_handlers(loop)

    # Start health server FIRST, before any other initialization
    # This ensures the health endpoint is available immediately for Kubernetes probes
    print("[STARTUP] Initializing health server (first priority)...")
    early_health_server = HealthCheckServer(
        port=8080,
        worker_name=args.worker if args.worker != "all" else "all-workers",
        enabled=True,
    )
    loop.run_until_complete(early_health_server.start())
    print(
        f"[STARTUP] Health server started on port {early_health_server.actual_port} "
        "(available for Kubernetes probes)"
    )
    logger.info(
        "Health server started early (before main initialization)",
        extra={"port": early_health_server.actual_port, "worker": args.worker},
    )

    _debug_token_file = os.getenv("AZURE_TOKEN_FILE")
    if _debug_token_file:
        _debug_token_exists = Path(_debug_token_file).exists()
        logger.debug(
            "Auth configuration detected",
            extra={
                "project_root": str(PROJECT_ROOT),
                "token_file": _debug_token_file,
                "token_file_exists": _debug_token_exists,
            },
        )
        if not _debug_token_exists:
            _resolved = PROJECT_ROOT / _debug_token_file
            logger.debug(
                "Attempting to resolve token file path relative to project root",
                extra={
                    "resolved_path": str(_resolved),
                    "resolved_exists": _resolved.exists(),
                },
            )

    # Initialize telemetry before starting metrics server so metrics are registered
    from pipeline.common.telemetry import initialize_telemetry

    worker_name = args.worker if args.worker != "all" else "all-workers"
    print("[STARTUP] Initializing telemetry...")
    initialize_telemetry(
        service_name=f"{domain}-{worker_name}",
        environment=os.getenv("ENVIRONMENT", "development"),
    )

    print("[STARTUP] Starting metrics server...")
    actual_port = start_metrics_server(args.metrics_port)
    if actual_port != args.metrics_port:
        logger.info(
            "Metrics server started on fallback port",
            extra={"actual_port": actual_port, "preferred_port": args.metrics_port},
        )
    else:
        logger.info("Metrics server started", extra={"port": actual_port})

    # Load configuration based on mode
    print("[STARTUP] Loading configuration...")
    if args.dev:
        pipeline_config, eventhub_config, local_kafka_config = load_dev_config()
    else:
        try:
            pipeline_config, eventhub_config, local_kafka_config = (
                load_production_config()
            )
        except ValueError as e:
            error_msg = str(e)
            logger.exception("Configuration error", extra={"error": error_msg})
            logger.error("Use --dev flag for local development without Eventhouse")

            # Set error on early health server and wait for shutdown
            early_health_server.set_error(f"Configuration error: {error_msg}")
            logger.info("Health server set to error state, waiting for shutdown...")
            shutdown_event = get_shutdown_event()
            loop.run_until_complete(shutdown_event.wait())
            loop.run_until_complete(early_health_server.stop())
            loop.close()
            logger.info("Error mode shutdown complete")
            return

    enable_delta_writes = not args.no_delta

    shutdown_event = get_shutdown_event()

    try:
        print("[STARTUP] Starting worker(s)...")
        if args.worker == "all":
            loop.run_until_complete(
                run_all_workers(pipeline_config, enable_delta_writes)
            )
        else:
            # Use registry to run specific worker
            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_worker_from_registry,
                        args.count,
                        args.worker,
                        args.worker,
                        pipeline_config,
                        shutdown_event,
                        enable_delta_writes,
                        eventhub_config,
                        local_kafka_config,
                    )
                )
            else:
                loop.run_until_complete(
                    run_worker_from_registry(
                        args.worker,
                        pipeline_config,
                        shutdown_event,
                        enable_delta_writes,
                        eventhub_config,
                        local_kafka_config,
                    )
                )
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
    except Exception as e:
        # Fatal error handling
        # Note: Workers with health servers handle their own error mode in
        # runners/common.py and won't reach here. This catches:
        # - Worker instantiation errors (before health server exists)
        # - Errors from workers without health servers
        # - Errors from run_all_workers orchestration
        error_msg = str(e)
        logger.error("Fatal error", extra={"error": error_msg})

        # Set error on early health server and wait for shutdown
        early_health_server.set_error(f"Fatal error: {error_msg}")
        logger.info("Early health server set to error state, waiting for shutdown...")
        loop.run_until_complete(asyncio.to_thread(upload_crash_logs, error_msg))
        loop.run_until_complete(shutdown_event.wait())
    finally:
        # Stop early health server
        print("[SHUTDOWN] Stopping early health server...")
        loop.run_until_complete(early_health_server.stop())
        loop.close()
        logger.info("Pipeline shutdown complete")


if __name__ == "__main__":
    main()
