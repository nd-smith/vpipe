"""
iTel Cabinet Tracking Worker

Consumes iTel cabinet task events via transport layer (Event Hub or Kafka),
enriches them with ClaimX data, and writes to Delta tables. Simple, explicit flow.

Usage:
    python -m pipeline.plugins.itel_cabinet_api.itel_cabinet_tracking_worker

Configuration:
    config/plugins/claimx/itel_cabinet_api/workers.yaml
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

from .delta import ItelAttachmentsDeltaWriter, ItelSubmissionsDeltaWriter
from .pipeline import ItelCabinetPipeline

# Project root directory (where .env file is located)
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent

logger = logging.getLogger(__name__)

# Configuration paths
CONFIG_DIR = Path(__file__).parent.parent.parent.parent / "config"
WORKERS_CONFIG_PATH = CONFIG_DIR / "plugins" / "claimx" / "itel_cabinet_api" / "workers.yaml"
CONNECTIONS_CONFIG_PATH = CONFIG_DIR / "plugins" / "shared" / "connections" / "claimx.yaml"

CONSUMER_GROUP = "itel-cabinet-tracking"
TOPIC_KEY = "itel_cabinet_pending"


class ItelCabinetTrackingWorker:
    """
    Worker for processing iTel cabinet task events.

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
        pipeline: ItelCabinetPipeline,
        health_port: int = 8096,
        health_enabled: bool = True,
    ):
        self.transport_config = transport_config
        self.connections = connection_manager
        self.pipeline = pipeline

        self.consumer = None
        self.running = False

        # Health server for Kubernetes liveness/readiness probes
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name="itel-cabinet-tracking",
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
        logger.info("Starting iTel Cabinet Tracking Worker")

        from pipeline.common.telemetry import initialize_worker_telemetry

        initialize_worker_telemetry("plugins", "itel-cabinet-tracking")
        set_log_context(stage="tracking", worker_id="itel-cabinet-tracking")

        config = MessageConfig(bootstrap_servers=self.transport_config["bootstrap_servers"])

        self.consumer = await create_consumer(
            config=config,
            domain="plugins",
            worker_name="itel_cabinet_tracking_worker",
            topics=[self.transport_config["input_topic"]],
            message_handler=self._handle_message,
            enable_message_commit=True,
            topic_key=TOPIC_KEY,
        )

        await self.health_server.start()
        self._stats_logger = PeriodicStatsLogger(
            interval_seconds=30,
            get_stats=self._get_cycle_stats,
            stage="tracking",
            worker_id="itel-cabinet-tracking",
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

        if self._stats_logger:
            await self._stats_logger.stop()

        if self.consumer:
            await self.consumer.stop()
            logger.info("Consumer stopped")

        await self.health_server.stop()


def load_worker_config() -> dict:
    """Load worker configuration from YAML."""
    config_data = load_yaml_config(WORKERS_CONFIG_PATH)
    workers = config_data.get("workers", {})

    if "itel_cabinet_tracking" not in workers:
        raise ValueError(f"Worker 'itel_cabinet_tracking' not found in {WORKERS_CONFIG_PATH}")

    return workers["itel_cabinet_tracking"]


async def build_tracking_worker() -> tuple:
    """Build a configured tracking worker and its resources.

    Returns:
        Tuple of (worker, connection_manager, producer) for lifecycle management.
    """
    worker_config = load_worker_config()
    connections_list = load_connections(CONNECTIONS_CONFIG_PATH)

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    transport_config = {
        "bootstrap_servers": bootstrap_servers,
        "input_topic": worker_config["transport"]["input_topic"],
        "consumer_group": worker_config["transport"]["consumer_group"],
    }

    connection_manager = ConnectionManager()
    for conn in connections_list:
        connection_manager.add_connection(conn)

    config = MessageConfig(bootstrap_servers=bootstrap_servers)
    producer = create_producer(
        config=config,
        domain="plugins",
        worker_name="itel_cabinet_tracking_worker",
        topic_key="itel_cabinet_completed",
    )
    await producer.start()

    submissions_path = os.environ.get("ITEL_DELTA_FORMS_TABLE")
    attachments_path = os.environ.get("ITEL_DELTA_ATTACHMENTS_TABLE")

    if not submissions_path or not attachments_path:
        logger.error(
            "Missing required delta table environment variables. "
            "Please set ITEL_DELTA_FORMS_TABLE and ITEL_DELTA_ATTACHMENTS_TABLE"
        )
        sys.exit(1)

    logger.info("Delta submissions table: %s", submissions_path)
    logger.info("Delta attachments table: %s", attachments_path)

    submissions_writer = ItelSubmissionsDeltaWriter(submissions_path)
    attachments_writer = ItelAttachmentsDeltaWriter(attachments_path)

    pipeline = ItelCabinetPipeline(
        connection_manager=connection_manager,
        submissions_writer=submissions_writer,
        attachments_writer=attachments_writer,
        producer=producer,
        config=worker_config.get("pipeline", {}),
    )

    processing_config = worker_config.get("processing", {})
    worker = ItelCabinetTrackingWorker(
        transport_config=transport_config,
        connection_manager=connection_manager,
        pipeline=pipeline,
        health_port=processing_config.get("health_port", 8096),
        health_enabled=processing_config.get("health_enabled", True),
    )

    return worker, connection_manager, producer


async def main():
    """Main entry point for standalone execution."""
    load_dotenv(PROJECT_ROOT / ".env")

    setup_logging(
        name="itel_cabinet_tracking",
        domain="itel_cabinet_api",
        stage="tracking",
        log_dir=Path("logs"),
        json_format=True,
        console_level=logging.INFO,
        file_level=logging.DEBUG,
    )

    try:
        worker, connection_manager, producer = await build_tracking_worker()
    except (FileNotFoundError, ValueError) as e:
        logger.error("Configuration error: %s", e)
        sys.exit(1)

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    log_worker_startup(
        logger=logger,
        worker_name="iTel Cabinet Tracking Worker",
        kafka_bootstrap_servers=bootstrap_servers,
        input_topic=worker.transport_config["input_topic"],
        output_topic=worker.pipeline.output_topic,
        consumer_group=worker.transport_config["consumer_group"],
    )

    shutdown_event = asyncio.Event()
    setup_shutdown_signal_handlers(shutdown_event.set)

    try:
        await connection_manager.start()

        from pipeline.runners.common import execute_worker_with_shutdown

        await execute_worker_with_shutdown(worker, "itel-cabinet-tracking", shutdown_event)
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
