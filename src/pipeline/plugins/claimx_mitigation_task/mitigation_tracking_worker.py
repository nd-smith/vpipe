"""
ClaimX Mitigation Task Tracking Worker

Consumes mitigation task completion events via transport layer (Event Hub or Kafka),
enriches them with ClaimX data, and publishes to success topic. Simple, explicit flow.

Usage:
    python -m pipeline.plugins.claimx_mitigation_task.mitigation_tracking_worker

Configuration:
    config/plugins/claimx/claimx_mitigation_task/workers.yaml
    config/config.yaml (eventhub.plugins section)

Environment Variables:
    CLAIMX_API_URL: ClaimX API base URL
    CLAIMX_API_TOKEN: Bearer token for ClaimX API
    PIPELINE_TRANSPORT: Transport type (eventhub or kafka, default: eventhub)
    EVENTHUB_NAMESPACE_CONNECTION_STRING: Event Hub namespace connection string
    KAFKA_BOOTSTRAP_SERVERS: Kafka broker addresses (only used if PIPELINE_TRANSPORT=kafka)
"""

import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

from config.config import MessageConfig
from core.logging import log_worker_startup, setup_logging
from core.logging.context import set_log_context
from core.logging.eventhub_config import prepare_eventhub_logging_config
from core.logging.periodic_logger import PeriodicStatsLogger
from pipeline.common.health import HealthCheckServer
from pipeline.common.metrics import (
    message_processing_duration_seconds,
    record_message_consumed,
    record_processing_error,
)
from pipeline.common.signals import setup_shutdown_signal_handlers
from pipeline.common.transport import create_consumer, create_producer
from pipeline.common.types import PipelineMessage
from pipeline.plugins.shared.config import load_connections, load_yaml_config
from pipeline.plugins.shared.connections import ConnectionManager

from .pipeline import MitigationTaskPipeline

# Project root directory (where .env file is located)
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent

logger = logging.getLogger(__name__)

# Configuration paths
CONFIG_DIR = Path(__file__).parent.parent.parent.parent / "config"
WORKERS_CONFIG_PATH = CONFIG_DIR / "plugins" / "claimx" / "claimx_mitigation_task" / "workers.yaml"
CONNECTIONS_CONFIG_PATH = CONFIG_DIR / "plugins" / "shared" / "connections" / "claimx.yaml"

CONSUMER_GROUP = "claimx-mitigation-tracking"
TOPIC_KEY = "ghrn_mitigation_pending"


class MitigationTrackingWorker:
    """
    Worker for processing ClaimX mitigation task events.

    Uses transport layer abstraction for Kafka/EventHub compatibility.
    Processes messages one-at-a-time through the pipeline.

    Transport Layer Contract:
    - Consumes from configured topic via create_consumer()
    - Message handler processes each message individually
    - Commit handled automatically by transport layer
    """

    def __init__(
        self,
        transport_config: dict,
        connection_manager: ConnectionManager,
        pipeline: MitigationTaskPipeline,
        health_server: HealthCheckServer | None = None,
        health_port: int = 8098,
        health_enabled: bool = True,
    ):
        self.transport_config = transport_config
        self.connections = connection_manager
        self.pipeline = pipeline

        self.consumer = None
        self.running = False

        # Health server for Kubernetes liveness/readiness probes
        self.health_server = health_server or HealthCheckServer(
            port=health_port,
            worker_name="mitigation-tracking",
            enabled=health_enabled,
        )

        # Shutdown handling
        self._shutdown_event = asyncio.Event()

        # Cycle output counters
        self._records_processed = 0
        self._records_succeeded = 0
        self._records_failed = 0
        self._stats_logger: PeriodicStatsLogger | None = None
        self._cycle_offset_start_ts = None
        self._cycle_offset_end_ts = None

    def _update_cycle_offsets(self, timestamp):
        if self._cycle_offset_start_ts is None or timestamp < self._cycle_offset_start_ts:
            self._cycle_offset_start_ts = timestamp
        if self._cycle_offset_end_ts is None or timestamp > self._cycle_offset_end_ts:
            self._cycle_offset_end_ts = timestamp

    async def _handle_message(self, record: PipelineMessage) -> None:
        """Process a single message from the transport layer."""
        start_time = time.perf_counter()
        topic = self.transport_config["input_topic"]

        try:
            message_data = json.loads(record.value.decode("utf-8"))
            event_id = message_data.get("eventId", message_data.get("event_id", ""))
            set_log_context(trace_id=event_id)

            result = await self.pipeline.process(message_data)

            self._records_processed += 1
            self._records_succeeded += 1
            self._update_cycle_offsets(record.timestamp)
            record_message_consumed(
                topic=topic,
                consumer_group=CONSUMER_GROUP,
                message_bytes=len(record.value),
            )

            logger.info(
                "Message processed successfully",
                extra={
                    "event_id": result.event.event_id,
                    "assignment_id": result.event.assignment_id,
                    "task_id": result.event.task_id,
                    "was_enriched": result.was_enriched(),
                },
            )

        except ValueError as e:
            self._records_processed += 1
            self._records_failed += 1
            self._update_cycle_offsets(record.timestamp)
            record_processing_error(
                topic=topic,
                consumer_group=CONSUMER_GROUP,
                error_category="validation",
            )
            logger.error(
                "Validation error: %s",
                e,
                extra={"offset": getattr(record, "offset", "unknown")},
            )

        except Exception as e:
            self._records_processed += 1
            self._records_failed += 1
            self._update_cycle_offsets(record.timestamp)
            record_processing_error(
                topic=topic,
                consumer_group=CONSUMER_GROUP,
                error_category="processing",
            )
            logger.exception(
                "Failed to process message: %s",
                e,
                extra={"offset": getattr(record, "offset", "unknown")},
            )
            raise

        finally:
            duration = time.perf_counter() - start_time
            message_processing_duration_seconds.labels(
                topic=topic,
                consumer_group=CONSUMER_GROUP,
            ).observe(duration)

    async def start(self):
        """Start the worker using transport layer."""
        logger.info("Starting Mitigation Task Tracking Worker")

        from pipeline.common.telemetry import initialize_worker_telemetry

        initialize_worker_telemetry("plugins", "mitigation-tracking")
        set_log_context(stage="tracking", worker_id="mitigation-tracking")

        config = MessageConfig(bootstrap_servers=self.transport_config["bootstrap_servers"])

        self.consumer = await create_consumer(
            config=config,
            domain="plugins",
            worker_name="mitigation_tracking_worker",
            topics=[self.transport_config["input_topic"]],
            message_handler=self._handle_message,
            topic_key=TOPIC_KEY,
        )

        await self.health_server.start()
        self._stats_logger = PeriodicStatsLogger(
            interval_seconds=30,
            get_stats=self._get_cycle_stats,
            stage="tracking",
            worker_id="mitigation-tracking",
        )
        self._stats_logger.start()
        await self.consumer.start()

        logger.info(
            "Consumer started via transport layer",
            extra={
                "topic": self.transport_config["input_topic"],
                "group": self.transport_config["consumer_group"],
                "transport_layer": "enabled",
            },
        )

        self.running = True
        self.health_server.set_ready(consumer_connected=True)

    async def run(self):
        """
        Main run loop - transport layer handles message consumption.

        The transport layer's consumer processes messages via _handle_message callback.
        This method just waits for shutdown signal.
        """
        logger.info("Worker running - transport layer consuming messages")

        try:
            await self._shutdown_event.wait()
            logger.info("Shutdown signal received")

        except Exception as e:
            logger.exception("Worker run loop error: %s", e)
            raise

    def _get_cycle_stats(self, cycle_count: int) -> tuple[str, dict]:
        extra = {
            "records_processed": self._records_processed,
            "records_succeeded": self._records_succeeded,
            "records_failed": self._records_failed,
            "records_skipped": 0,
            "records_deduplicated": 0,
            "cycle_offset_start_ts": self._cycle_offset_start_ts,
            "cycle_offset_end_ts": self._cycle_offset_end_ts,
        }
        self._cycle_offset_start_ts = None
        self._cycle_offset_end_ts = None
        return "", extra

    async def stop(self):
        """Stop the worker gracefully."""
        logger.info("Stopping worker")
        self.running = False
        self._shutdown_event.set()

        try:
            if self._stats_logger:
                await self._stats_logger.stop()
        except Exception as e:
            logger.error("Error stopping stats logger", extra={"error": str(e)})

        try:
            if self.consumer:
                await self.consumer.stop()
                logger.info("Consumer stopped")
        except Exception as e:
            logger.error("Error stopping consumer", extra={"error": str(e)})

        try:
            await self.health_server.stop()
        except Exception as e:
            logger.error("Error stopping health server", extra={"error": str(e)})


def load_worker_config() -> dict:
    """Load worker configuration from YAML."""
    config_data = load_yaml_config(WORKERS_CONFIG_PATH)
    workers = config_data.get("workers", {})

    if "mitigation_tracking" not in workers:
        raise ValueError(f"Worker 'mitigation_tracking' not found in {WORKERS_CONFIG_PATH}")

    return workers["mitigation_tracking"]


async def build_tracking_worker(
    worker_config: dict,
    health_server: HealthCheckServer,
) -> tuple:
    """Build a configured tracking worker and its resources.

    Args:
        worker_config: Parsed worker configuration from workers.yaml.
        health_server: Pre-started health server for the worker to use.

    Returns:
        Tuple of (worker, connection_manager, producer) for lifecycle management.
    """
    connections_list = load_connections(CONNECTIONS_CONFIG_PATH)

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    transport_config = {
        "bootstrap_servers": bootstrap_servers,
        "input_topic": worker_config["kafka"]["input_topic"],
        "consumer_group": worker_config["kafka"]["consumer_group"],
    }

    connection_manager = ConnectionManager()
    for conn in connections_list:
        connection_manager.add_connection(conn)

    config = MessageConfig(bootstrap_servers=bootstrap_servers)
    producer = create_producer(
        config=config,
        domain="plugins",
        worker_name="mitigation_tracking_worker",
        topic_key="ghrn_mitigation_completed",
    )
    await producer.start()

    pipeline = MitigationTaskPipeline(
        connection_manager=connection_manager,
        kafka_producer=producer,
        config=worker_config.get("pipeline", {}),
    )

    worker = MitigationTrackingWorker(
        transport_config=transport_config,
        connection_manager=connection_manager,
        pipeline=pipeline,
        health_server=health_server,
    )

    return worker, connection_manager, producer


async def main():
    """Main entry point."""
    load_dotenv(PROJECT_ROOT / ".env")

    from config import load_config

    try:
        config = load_config()
    except Exception:
        config = None

    log_dir = Path(config.logging_config.get("log_dir", "logs")) if config else Path("logs")
    eventhub_config = prepare_eventhub_logging_config(config.logging_config) if config else None
    eventhub_enabled = (
        config.logging_config.get("eventhub_logging", {}).get("enabled", True)
        if config
        else True
    )

    setup_logging(
        name="mitigation_tracking",
        domain="claimx_mitigation_task",
        stage="tracking",
        log_dir=log_dir,
        json_format=True,
        console_level=logging.INFO,
        file_level=logging.DEBUG,
        eventhub_config=eventhub_config,
        enable_eventhub_logging=eventhub_enabled,
    )

    # Load worker config early to get health port
    try:
        worker_config = load_worker_config()
    except (FileNotFoundError, ValueError) as e:
        logger.error("Configuration error: %s", e)
        sys.exit(1)

    processing_config = worker_config.get("processing", {})
    health_port = processing_config.get("health_port", 8098)
    health_enabled = processing_config.get("health_enabled", True)

    # Start health server before building the worker so it survives build failures
    health_server = HealthCheckServer(
        port=health_port,
        worker_name="mitigation-tracking",
        enabled=health_enabled,
    )
    await health_server.start()

    shutdown_event = asyncio.Event()
    setup_shutdown_signal_handlers(shutdown_event.set)

    try:
        worker, connection_manager, producer = await build_tracking_worker(
            worker_config=worker_config,
            health_server=health_server,
        )
    except (FileNotFoundError, ValueError) as e:
        logger.error("Configuration error: %s", e)
        health_server.set_error(f"Configuration error: {e}")
        logger.warning("Entering error mode - health server will remain active")
        await shutdown_event.wait()
        await health_server.stop()
        logger.info("Pipeline shutdown complete")
        return

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    log_worker_startup(
        logger=logger,
        worker_name="Mitigation Task Tracking Worker",
        kafka_bootstrap_servers=bootstrap_servers,
        input_topic=worker.transport_config["input_topic"],
        output_topic=worker_config.get("pipeline", {}).get("output_topic"),
        consumer_group=worker.transport_config["consumer_group"],
    )

    try:
        await connection_manager.start()

        from pipeline.runners.common import execute_worker_with_shutdown

        await execute_worker_with_shutdown(worker, "mitigation-tracking", shutdown_event)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.exception("Worker failed: %s", e)
        sys.exit(1)
    finally:
        await worker.stop()
        await connection_manager.close()
        await producer.stop()
        logger.info("Worker shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
