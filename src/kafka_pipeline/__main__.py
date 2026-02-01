"""
Entry point for running Kafka pipeline workers.

Usage:
    # Run all workers
    python -m kafka_pipeline

    # Run specific xact worker
    python -m kafka_pipeline --worker xact-poller
    python -m kafka_pipeline --worker xact-event-ingester
    python -m kafka_pipeline --worker xact-enricher
    python -m kafka_pipeline --worker xact-delta-writer
    python -m kafka_pipeline --worker xact-retry-scheduler
    python -m kafka_pipeline --worker xact-download
    python -m kafka_pipeline --worker xact-upload
    python -m kafka_pipeline --worker xact-result-processor

    # Run specific claimx worker
    python -m kafka_pipeline --worker claimx-poller
    python -m kafka_pipeline --worker claimx-ingester
    python -m kafka_pipeline --worker claimx-enricher
    python -m kafka_pipeline --worker claimx-downloader
    python -m kafka_pipeline --worker claimx-uploader
    python -m kafka_pipeline --worker claimx-result-processor
    python -m kafka_pipeline --worker claimx-delta-writer
    python -m kafka_pipeline --worker claimx-retry-scheduler
    python -m kafka_pipeline --worker claimx-entity-writer

    # Run dummy data source for testing (generates synthetic events)
    python -m kafka_pipeline --worker dummy-source --dev

    # Run multiple worker instances (for horizontal scaling)
    python -m kafka_pipeline --worker xact-download --count 4
    python -m kafka_pipeline --worker xact-upload -c 3

    # Run with metrics server
    python -m kafka_pipeline --metrics-port 8000

    # Run in development mode (local Kafka only, no Event Hub/Eventhouse)
    python -m kafka_pipeline --dev

    # Run in simulation mode (with mock dependencies)
    python -m kafka_pipeline --simulation-mode --worker claimx-enricher
    python -m kafka_pipeline --simulation-mode --worker xact-uploader

Event Source Configuration:
    Set EVENT_SOURCE environment variable:
    - eventhub (default): Use Azure Event Hub via Kafka protocol
    - eventhouse: Poll Microsoft Fabric Eventhouse
    - dummy: Generate synthetic test data (use --worker dummy-source)

Architecture:
    xact Domain:
        Event Source → events.raw topic
            → xact-event-ingester → enrichment.pending → xact-enricher (plugins) →
              downloads.pending → download worker → downloads.cached → upload worker →
              downloads.results → result processor
            → xact-delta-writer → xact_events Delta table (parallel)
              ↓ (on failure)
            → xact.retry topic → xact-retry-scheduler → retry write or DLQ (header-based routing)

    claimx Domain:
        Eventhouse → claimx-poller → claimx.events.raw → claimx-ingester →
        enrichment.pending → enrichment worker → entity tables + downloads.pending →
        download worker → downloads.cached → upload worker → downloads.results
            → claimx-delta-writer → claimx_events Delta table (parallel)
              ↓ (on failure)
            → claimx.retry topic → claimx-retry-scheduler → retry write or DLQ (header-based routing)
"""

import argparse
import asyncio
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Any, Callable, Coroutine, Optional

import coolname
from dotenv import load_dotenv
from prometheus_client import start_http_server, REGISTRY

from core.logging.context import set_log_context
from core.logging.setup import (
    get_logger,
    setup_logging,
    setup_multi_worker_logging,
    upload_crash_logs,
)
from kafka_pipeline.common.health import HealthCheckServer
from kafka_pipeline.runners.registry import WORKER_REGISTRY, run_worker_from_registry

# Project root directory (where .env file is located)
# __main__.py is at src/kafka_pipeline/__main__.py, so root is 3 levels up
PROJECT_ROOT = Path(__file__).parent.parent.parent

# Worker stages for multi-worker logging
WORKER_STAGES = list(WORKER_REGISTRY.keys())

# Placeholder logger until setup_logging() is called in main()
# This allows module-level logging before full initialization
logger = logging.getLogger(__name__)

# Global shutdown event for graceful batch completion
# Set by signal handlers, checked by workers to finish current batch before exiting
_shutdown_event: Optional[asyncio.Event] = None


def get_shutdown_event() -> asyncio.Event:
    global _shutdown_event
    if _shutdown_event is None:
        _shutdown_event = asyncio.Event()
    return _shutdown_event


async def run_worker_pool(
    worker_fn: Callable[..., Coroutine[Any, Any, None]],
    count: int,
    worker_name: str,
    *args: Any,
    **kwargs: Any,
) -> None:
    """Run multiple instances of a worker concurrently.
    Each instance joins the same consumer group for automatic partition distribution.
    Each instance gets a unique instance_id (coolname) for distinct logging and identity.
    """
    logger.info(
        "Starting worker instances", extra={"count": count, "worker_name": worker_name}
    )

    tasks = []
    for i in range(count):
        # Generate unique coolname for this worker instance (e.g., "happy-tiger")
        instance_id = coolname.generate_slug(2)

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
        description="Run Kafka pipeline workers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run all workers (Event Hub → Local Kafka pipeline)
    python -m kafka_pipeline

    # Run specific xact worker
    python -m kafka_pipeline --worker xact-download

    # Run specific claimx worker
    python -m kafka_pipeline --worker claimx-enricher

    # Run in development mode (local Kafka only)
    python -m kafka_pipeline --dev

    # Run with custom metrics port
    python -m kafka_pipeline --metrics-port 9090
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
        help="Development mode: use local Kafka only (no Event Hub/Eventhouse credentials required)",
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

    parser.add_argument(
        "--simulation-mode",
        action="store_true",
        help="Enable simulation mode with mock dependencies for local testing. "
        "Uses mock API clients and local filesystem storage instead of production services. "
        "CANNOT run in production environments.",
    )

    return parser.parse_args()


async def run_all_workers(
    pipeline_config,
    enable_delta_writes: bool = True,
):
    """Run all pipeline workers concurrently.
    Architecture: events.raw → EventIngester → downloads.pending → DownloadWorker → ...
                  events.raw → DeltaEventsWorker → Delta table (parallel)"""
    from config.pipeline_config import EventSourceType
    from kafka_pipeline.runners import verisk_runners

    logger.info("Starting all pipeline workers...")

    local_kafka_config = pipeline_config.local_kafka.to_kafka_config()
    shutdown_event = get_shutdown_event()

    tasks = []

    if pipeline_config.event_source == EventSourceType.EVENTHOUSE:
        events_table_path = (
            pipeline_config.verisk_eventhouse.xact_events_table_path
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
                    local_kafka_config,
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
                    local_kafka_config,
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
                    local_kafka_config, events_table_path, shutdown_event
                ),
                name="xact-delta-writer",
            )
        )
        logger.info("Delta events writer enabled")

        tasks.append(
            asyncio.create_task(
                verisk_runners.run_xact_retry_scheduler(
                    local_kafka_config, shutdown_event
                ),
                name="xact-retry-scheduler",
            )
        )
        logger.info("XACT unified retry scheduler enabled")

    tasks.extend(
        [
            asyncio.create_task(
                verisk_runners.run_download_worker(local_kafka_config, shutdown_event),
                name="xact-download",
            ),
            asyncio.create_task(
                verisk_runners.run_upload_worker(local_kafka_config, shutdown_event),
                name="xact-upload",
            ),
            asyncio.create_task(
                verisk_runners.run_result_processor(
                    local_kafka_config,
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
        from kafka_pipeline.common.telemetry import get_prometheus_registry

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

    from core.security.ssl_dev_bypass import apply_ssl_dev_bypass

    apply_ssl_dev_bypass()

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

    worker_id = os.getenv("WORKER_ID", f"kafka-{args.worker}")

    domain = "kafka"
    if args.worker != "all" and "-" in args.worker:
        domain_prefix = args.worker.split("-")[0]
        if domain_prefix in ("xact", "claimx"):
            domain = domain_prefix

    if args.worker == "all":
        setup_multi_worker_logging(
            workers=WORKER_STAGES,
            domain="kafka",
            log_dir=log_dir,
            json_format=json_logs,
            console_level=log_level,
            log_to_stdout=log_to_stdout,
        )
    else:
        setup_logging(
            name="kafka_pipeline",
            stage=args.worker,
            domain=domain,
            log_dir=log_dir,
            json_format=json_logs,
            console_level=log_level,
            worker_id=worker_id,
            log_to_stdout=log_to_stdout,
        )

    logger = get_logger(__name__)

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
    from kafka_pipeline.common.telemetry import initialize_telemetry

    worker_name = args.worker if args.worker != "all" else "all-workers"
    initialize_telemetry(
        service_name=f"{domain}-{worker_name}",
        environment=os.getenv("ENVIRONMENT", "development"),
    )

    actual_port = start_metrics_server(args.metrics_port)
    if actual_port != args.metrics_port:
        logger.info(
            "Metrics server started on fallback port",
            extra={"actual_port": actual_port, "preferred_port": args.metrics_port},
        )
    else:
        logger.info("Metrics server started", extra={"port": actual_port})

    # Workers that only use local Kafka (don't need Event Hub/Eventhouse credentials)
    local_only_workers = [
        "xact-enricher",
        "xact-download",
        "xact-upload",
        "xact-delta-writer",
        "xact-retry-scheduler",
        "claimx-ingester",
        "claimx-enricher",
        "claimx-downloader",
        "claimx-uploader",
        "claimx-result-processor",
        "claimx-delta-writer",
        "claimx-entity-writer",
        "claimx-retry-scheduler",
    ]

    # Auto-enable dev mode for local-only workers
    if args.worker in local_only_workers and not args.dev:
        logger.info(
            "Auto-enabling development mode for local-only worker",
            extra={"worker": args.worker},
        )
        args.dev = True

    if args.dev:
        logger.info("Running in DEVELOPMENT mode (local Kafka only)")
        from config.pipeline_config import (
            EventSourceType,
            LocalKafkaConfig,
            PipelineConfig,
        )

        local_config = LocalKafkaConfig.load_config()
        kafka_config = local_config.to_kafka_config()

        pipeline_config = PipelineConfig(
            event_source=EventSourceType.EVENTHUB,
            local_kafka=local_config,
        )

        eventhub_config = kafka_config
        local_kafka_config = kafka_config
    else:
        from config.pipeline_config import EventSourceType, get_pipeline_config

        try:
            pipeline_config = get_pipeline_config()
            local_kafka_config = pipeline_config.local_kafka.to_kafka_config()

            if pipeline_config.event_source == EventSourceType.EVENTHOUSE:
                logger.info("Running in PRODUCTION mode (Eventhouse + local Kafka)")
                eventhub_config = None
            else:
                logger.info("Running in PRODUCTION mode (Event Hub + local Kafka)")
                eventhub_config = pipeline_config.eventhub.to_kafka_config()
        except ValueError as e:
            logger.error("Configuration error", extra={"error": str(e)})
            logger.error(
                "Use --dev flag for local development without Event Hub/Eventhouse"
            )
            logger.warning("Running in ERROR MODE - health endpoint will remain alive")

            # Run in error mode: keep health endpoint alive but report not ready
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            setup_signal_handlers(loop)

            async def run_error_mode():
                """Run health server in error state until shutdown signal."""
                shutdown_event = get_shutdown_event()

                # Start health server in error state on port 8080 (matches Jenkinsfile probes)
                health_server = HealthCheckServer(
                    port=8080,
                    worker_name=args.worker,
                    enabled=True,
                )
                health_server.set_error(f"Configuration error: {e}")

                await health_server.start()
                logger.info(
                    "Health server running in error mode",
                    extra={
                        "port": health_server.actual_port,
                        "worker": args.worker,
                        "error": str(e),
                    },
                )

                # Upload crash logs in background thread so health server stays responsive
                await asyncio.to_thread(upload_crash_logs, f"Configuration error: {e}")

                # Wait for shutdown signal
                await shutdown_event.wait()
                logger.info("Shutdown signal received in error mode")
                await health_server.stop()

            try:
                loop.run_until_complete(run_error_mode())
            except KeyboardInterrupt:
                logger.info(
                    "Keyboard interrupt received in error mode, shutting down..."
                )
            finally:
                loop.close()
                logger.info("Error mode shutdown complete")
            return  # Exit main() without sys.exit(1)

    enable_delta_writes = not args.no_delta

    # Check for simulation mode
    simulation_mode = args.simulation_mode or os.getenv(
        "SIMULATION_MODE", "false"
    ).lower() in (
        "true",
        "1",
        "yes",
    )

    # Initialize simulation config at startup if enabled
    # Validation happens here - fail fast if config is invalid
    if simulation_mode:
        from kafka_pipeline.simulation.config import (
            SimulationConfig,
            set_simulation_config,
        )

        logger.warning("=" * 80)
        logger.warning("SIMULATION MODE ENABLED")
        logger.warning("Using mock dependencies and local storage for testing")
        logger.warning("Production APIs and cloud storage will NOT be accessed")
        logger.warning("=" * 80)

        # Initialize and validate simulation config
        # ValidationErrors will propagate - fail fast if config is invalid
        sim_config = SimulationConfig.from_env(enabled=True)
        set_simulation_config(sim_config)
        sim_config.ensure_directories()

        logger.info(
            "Simulation config initialized",
            extra={
                "local_storage_path": str(sim_config.local_storage_path),
                "local_delta_path": str(sim_config.local_delta_path),
                "fixtures_dir": str(sim_config.fixtures_dir),
                "delta_write_mode": sim_config.delta_write_mode,
            },
        )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    setup_signal_handlers(loop)

    shutdown_event = get_shutdown_event()

    # Check for deprecated dummy-source worker
    if args.worker in ["dummy-source", "dummy_source"]:
        logger.error("=" * 80)
        logger.error("DEPRECATED: 'dummy-source' has been moved to simulation module")
        logger.error("=" * 80)
        logger.error("")
        logger.error("The dummy data producer is now a simulation-only tool.")
        logger.error("It has been moved to prevent accidental use in production.")
        logger.error("")
        logger.error("New usage:")
        logger.error("  export SIMULATION_MODE=true")
        logger.error(
            "  python -m kafka_pipeline.simulation.dummy_producer --domains claimx --max-events 100"
        )
        logger.error("")
        logger.error("Or use the convenience script:")
        logger.error("  ./scripts/generate_test_data.sh")
        logger.error("")
        logger.error("See: kafka_pipeline/simulation/README.md")
        logger.error("=" * 80)
        sys.exit(1)

    try:
        if args.worker == "all":
            if simulation_mode:
                logger.error("Simulation mode does not support running all workers")
                logger.error("Please specify a specific worker with --worker flag")
                sys.exit(1)
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
                        simulation_mode=simulation_mode,
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
                        simulation_mode=simulation_mode,
                    )
                )
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
    except Exception as e:
        logger.error("Fatal error", extra={"error": str(e)}, exc_info=True)
        logger.warning("Entering ERROR MODE - health endpoint will remain alive")

        # Run in error mode: keep health endpoint alive but report not ready
        async def run_fatal_error_mode():
            """Run health server in error state after fatal error."""
            shutdown_event = get_shutdown_event()

            # Start health server in error state on port 8080 (matches Jenkinsfile probes)
            health_server = HealthCheckServer(
                port=8080,
                worker_name=args.worker,
                enabled=True,
            )
            health_server.set_error(f"Fatal error: {e}")

            await health_server.start()
            logger.info(
                "Health server running in error mode after fatal error",
                extra={
                    "port": health_server.actual_port,
                    "worker": args.worker,
                    "error": str(e),
                },
            )

            # Upload crash logs in background thread so health server stays responsive
            await asyncio.to_thread(upload_crash_logs, f"Fatal error: {e}")

            # Wait for shutdown signal
            await shutdown_event.wait()
            logger.info("Shutdown signal received in error mode")
            await health_server.stop()

        try:
            loop.run_until_complete(run_fatal_error_mode())
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received in error mode, shutting down...")
    finally:
        loop.close()
        logger.info("Pipeline shutdown complete")


if __name__ == "__main__":
    main()
