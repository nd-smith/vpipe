"""EventHub pipeline worker orchestration. Use --help for usage."""

import argparse
import asyncio
import logging
import os
import signal
import sys
import uuid
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


def enter_error_mode(loop: asyncio.AbstractEventLoop, worker_name: str, error_msg: str) -> None:
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
    from config.pipeline_config import PipelineConfig

    kafka_config = get_config()
    pipeline_config = PipelineConfig()

    return pipeline_config, kafka_config, kafka_config


def load_production_config():
    """Load configuration for production mode (Event Hub).

    Returns:
        Tuple of (pipeline_config, eventhub_config, kafka_config)

    Raises:
        ValueError: If configuration is invalid
    """
    from config import get_config
    from config.pipeline_config import get_pipeline_config

    pipeline_config = get_pipeline_config()
    kafka_config = get_config()

    logger.info("Running in PRODUCTION mode (Event Hub pipeline)")
    eventhub_config = pipeline_config.eventhub.to_message_config()

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
    logger.info("Starting worker instances", extra={"count": count, "worker_name": worker_name})

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
        logger.info("Worker pool cancelled, shutting down", extra={"worker_name": worker_name})
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


def _create_event_ingester_task(verisk_runners, eventhub_config, kafka_config, shutdown_event, domain):
    return asyncio.create_task(
        verisk_runners.run_event_ingester(
            eventhub_config,
            kafka_config,
            shutdown_event,
            domain=domain,
        ),
        name="xact-event-ingester",
    )


def _create_delta_writer_tasks(verisk_runners, kafka_config, events_table_path, shutdown_event):
    tasks = []
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

    return tasks


def _create_core_xact_tasks(verisk_runners, kafka_config, shutdown_event, pipeline_config, enable_delta_writes):
    return [
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


async def run_all_workers(
    pipeline_config,
    enable_delta_writes: bool = True,
):
    """Run all pipeline wcoorkers concurrently.
    Architecture: events.raw → EventIngester → downloads.pending → DownloadWorker → ...
                  events.raw → DeltaEventsWorker → Delta table (parallel)"""
    from config import get_config
    from pipeline.runners import verisk_runners

    logger.info("Starting all pipeline workers...")

    kafka_config = get_config()
    shutdown_event = get_shutdown_event()

    tasks = []

    eventhub_config = pipeline_config.eventhub.to_message_config()
    tasks.append(
        _create_event_ingester_task(
            verisk_runners, eventhub_config, kafka_config, shutdown_event, pipeline_config.domain
        )
    )
    logger.info("Using Event Hub as event source")

    events_table_path = pipeline_config.events_table_path
    if enable_delta_writes and events_table_path:
        tasks.extend(
            _create_delta_writer_tasks(verisk_runners, kafka_config, events_table_path, shutdown_event)
        )

    tasks.extend(
        _create_core_xact_tasks(
            verisk_runners, kafka_config, shutdown_event, pipeline_config, enable_delta_writes
        )
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
        logger.info("Received signal, initiating graceful shutdown", extra={"signal": sig.name})
        shutdown_event = get_shutdown_event()
        if not shutdown_event.is_set():
            shutdown_event.set()
        else:
            logger.warning("Received second signal, forcing immediate shutdown...")
            for task in asyncio.all_tasks(loop):
                task.cancel()

    if sys.platform == "win32":
        logger.debug("Signal handlers not supported on Windows, using KeyboardInterrupt")
        return

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: handle_signal(s))


def verify_storage_permissions(storage_path: Path) -> bool:
    """Verify read/write permissions on persistent storage at startup.

    Writes a temporary file, reads it back, and cleans up.
    Returns True if all operations succeed, False otherwise.
    """
    test_file = storage_path / f".vpipe_permission_check_{uuid.uuid4().hex[:8]}"
    test_content = "vpipe storage permission check"

    try:
        if not storage_path.exists():
            print(f"[STARTUP] Storage check FAILED: {storage_path} does not exist", flush=True)
            return False

        test_file.write_text(test_content)
        read_back = test_file.read_text()

        if read_back != test_content:
            print(
                f"[STARTUP] Storage check FAILED: read-back mismatch at {storage_path}",
                flush=True,
            )
            return False

        test_file.unlink()
        print(f"[STARTUP] Storage check PASSED: read/write OK at {storage_path}", flush=True)
        return True
    except PermissionError:
        print(
            f"[STARTUP] Storage check FAILED: permission denied at {storage_path}", flush=True
        )
        return False
    except OSError as e:
        print(f"[STARTUP] Storage check FAILED: {e}", flush=True)
        return False
    finally:
        if test_file.exists():
            test_file.unlink(missing_ok=True)


def _setup_environment():
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
        print("[SSL] SSL verification ENABLED (set DISABLE_SSL_VERIFY=true to disable)")

    args = parse_args()

    domain = "kafka"
    if args.worker != "all" and "-" in args.worker:
        domain_prefix = args.worker.split("-")[0]
        if domain_prefix in ("xact", "claimx", "itel"):
            domain = domain_prefix

    from core.utils import generate_worker_id

    worker_id = os.getenv("WORKER_ID") or generate_worker_id(args.worker)

    return args, domain, worker_id


def _setup_logging(args, domain, worker_id):
    log_level = getattr(logging, args.log_level)

    json_logs = os.getenv("JSON_LOGS", "true").lower() in ("true", "1", "yes")

    log_to_stdout = args.log_to_stdout or os.getenv("LOG_TO_STDOUT", "false").lower() in (
        "true",
        "1",
        "yes",
    )

    # Load config early to get logging configuration
    from config import load_config

    try:
        config = load_config()
    except Exception as e:
        # If config loading fails, setup basic logging without EventHub
        logging.getLogger(__name__).warning(
            "Failed to load config for logging setup, continuing without EventHub logging",
            extra={"error": str(e)},
        )
        config = None

    log_dir_str = args.log_dir or os.getenv("LOG_DIR") or (
        config.logging_config.get("log_dir") if config else None
    ) or "logs"
    log_dir = Path(log_dir_str)

    # Prepare EventHub logging config
    eventhub_config = prepare_eventhub_logging_config(config.logging_config) if config else None

    eventhub_enabled = True
    if config:
        eventhub_enabled = config.logging_config.get("eventhub_logging", {}).get("enabled", True)

    if args.worker == "all":
        setup_multi_worker_logging(
            workers=WORKER_STAGES,
            domain="kafka",
            log_dir=log_dir,
            json_format=json_logs,
            console_level=log_level,
            log_to_stdout=log_to_stdout,
            eventhub_config=eventhub_config,
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
            enable_eventhub_logging=eventhub_enabled,
        )

    return config, log_to_stdout, eventhub_enabled, eventhub_config


def _extract_eventhub_namespace(conn_str: str) -> str:
    """Extract namespace from an EventHub connection string."""
    if "Endpoint=sb://" not in conn_str:
        return "unknown"
    try:
        return conn_str.split("Endpoint=sb://")[1].split("/")[0]
    except (IndexError, AttributeError):
        return "unknown"


def _display_eventhub_status(eventhub_enabled, eventhub_config):
    """Print EventHub logging status during startup."""
    if not eventhub_enabled:
        print("[STARTUP] EventHub logging: DISABLED")
        return

    if not eventhub_config:
        print(
            "[STARTUP] EventHub logging: ✗ NOT CONFIGURED (missing EVENTHUB_NAMESPACE_CONNECTION_STRING)"
        )
        return

    conn_str = eventhub_config.get("connection_string", "")
    eventhub_name = eventhub_config.get("eventhub_name", "unknown")
    eventhub_namespace = _extract_eventhub_namespace(conn_str)
    print(f"[STARTUP] EventHub logs configured: {eventhub_namespace}/{eventhub_name}")

    root_logger = logging.getLogger()
    has_eventhub_handler = any("EventHub" in type(h).__name__ for h in root_logger.handlers)
    if has_eventhub_handler:
        print("[STARTUP] EventHub log handler: ✓ ACTIVE")
    else:
        print("[STARTUP] EventHub log handler: ✗ FAILED TO CREATE (check logs for errors)")


def _display_startup_info(worker_id, log_to_stdout, eventhub_enabled, eventhub_config):
    log_output_mode = get_log_output_mode(
        log_to_stdout=log_to_stdout,
        enable_eventhub_logging=eventhub_enabled,
    )

    print(f"[STARTUP] Log output mode: {log_output_mode}", flush=True)
    print(f"[STARTUP] Worker ID: {worker_id}", flush=True)
    _display_eventhub_status(eventhub_enabled, eventhub_config)


def _initialize_infrastructure(args, domain):
    print("[STARTUP] Creating event loop...", flush=True)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    setup_signal_handlers(loop)

    # Start health server FIRST, before any other initialization
    # This ensures the health endpoint is available immediately for Kubernetes probes
    print("[STARTUP] Initializing health server (first priority)...", flush=True)
    early_health_server = HealthCheckServer(
        port=8080,
        worker_name=args.worker if args.worker != "all" else "all-workers",
        enabled=True,
    )
    loop.run_until_complete(early_health_server.start())
    print(
        f"[STARTUP] Health server started on port {early_health_server.actual_port} "
        "(available for Kubernetes probes)",
        flush=True,
    )
    logger.info(
        "Health server started early (before main initialization)",
        extra={"port": early_health_server.actual_port, "worker": args.worker},
    )

    # Verify persistent storage permissions
    verify_storage_permissions(Path("/mnt/pcesdopodapp"))

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
    print("[STARTUP] Initializing telemetry...", flush=True)
    initialize_telemetry(
        service_name=f"{domain}-{worker_name}",
        environment=os.getenv("ENVIRONMENT", "development"),
    )

    print("[STARTUP] Starting metrics server...", flush=True)
    actual_port = start_metrics_server(args.metrics_port)
    if actual_port != args.metrics_port:
        logger.info(
            "Metrics server started on fallback port",
            extra={"actual_port": actual_port, "preferred_port": args.metrics_port},
        )
    else:
        logger.info("Metrics server started", extra={"port": actual_port})

    return loop, early_health_server


def _load_pipeline_config(args, loop, early_health_server):
    print("[STARTUP] Loading configuration...", flush=True)
    if args.dev:
        pipeline_config, eventhub_config, local_kafka_config = load_dev_config()
        return pipeline_config, eventhub_config, local_kafka_config

    try:
        pipeline_config, eventhub_config, local_kafka_config = load_production_config()
        return pipeline_config, eventhub_config, local_kafka_config
    except (ValueError, FileNotFoundError, KeyError) as e:
        error_msg = str(e)
        logger.exception("Configuration error", extra={"error": error_msg})
        logger.error("Use --dev flag for local development")

        # Set error on early health server and wait for shutdown
        early_health_server.set_error(f"Configuration error: {error_msg}")
        logger.info("Health server set to error state, waiting for shutdown...")
        shutdown_event = get_shutdown_event()
        loop.run_until_complete(shutdown_event.wait())
        loop.run_until_complete(early_health_server.stop())
        loop.close()
        logger.info("Error mode shutdown complete")
        return None


def _start_workers(args, loop, pipeline_config, enable_delta_writes, eventhub_config, local_kafka_config):
    shutdown_event = get_shutdown_event()

    print("[STARTUP] Starting worker(s)...", flush=True)
    if args.worker == "all":
        loop.run_until_complete(run_all_workers(pipeline_config, enable_delta_writes))
    elif args.count > 1:
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


def main():
    global logger

    args, domain, worker_id = _setup_environment()

    config, log_to_stdout, eventhub_enabled, eventhub_config_logging = _setup_logging(
        args, domain, worker_id
    )
    logger = logging.getLogger(__name__)

    _display_startup_info(worker_id, log_to_stdout, eventhub_enabled, eventhub_config_logging)

    loop, early_health_server = _initialize_infrastructure(args, domain)

    config_result = _load_pipeline_config(args, loop, early_health_server)
    if config_result is None:
        return
    pipeline_config, eventhub_config, local_kafka_config = config_result

    enable_delta_writes = not args.no_delta
    shutdown_event = get_shutdown_event()

    try:
        _start_workers(args, loop, pipeline_config, enable_delta_writes, eventhub_config, local_kafka_config)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
    except asyncio.CancelledError:
        logger.info("Tasks cancelled, shutting down...")
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
        print("[SHUTDOWN] Stopping early health server...", flush=True)
        loop.run_until_complete(early_health_server.stop())
        loop.close()
        logger.info("Pipeline shutdown complete")


if __name__ == "__main__":
    main()
