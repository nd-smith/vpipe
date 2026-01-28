"""
ClaimX Mitigation Task Tracking Worker

Consumes mitigation task completion events from Kafka, enriches them with ClaimX data,
and publishes to success topic. Simple, explicit flow.

Usage:
    python -m kafka_pipeline.plugins.claimx_mitigation_task.mitigation_tracking_worker

Configuration:
    config/plugins/claimx/claimx_mitigation_task/workers.yaml

Environment Variables:
    CLAIMX_API_BASE_URL: ClaimX API base URL
    CLAIMX_API_TOKEN: Bearer token for ClaimX API
    KAFKA_BOOTSTRAP_SERVERS: Kafka broker addresses (default: localhost:9094)
"""

import asyncio
import json
import logging
import os
import signal
import sys
from pathlib import Path

import yaml
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError
from dotenv import load_dotenv

from core.logging import setup_logging, get_logger, log_worker_startup

# Project root directory (where .env file is located)
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent

from kafka_pipeline.plugins.shared.connections import (
    AuthType,
    ConnectionConfig,
    ConnectionManager,
)

from .pipeline import MitigationTaskPipeline

logger = get_logger(__name__)

# Configuration paths
CONFIG_DIR = Path(__file__).parent.parent.parent.parent / "config"
WORKERS_CONFIG_PATH = CONFIG_DIR / "plugins" / "claimx" / "claimx_mitigation_task" / "workers.yaml"
CONNECTIONS_CONFIG_PATH = CONFIG_DIR / "plugins" / "shared" / "connections" / "claimx.yaml"


class MitigationTrackingWorker:
    """
    Worker for processing ClaimX mitigation task events.

    Simple, explicit consumer loop - no framework magic.
    Easy to understand, easy to debug.
    """

    def __init__(
        self,
        kafka_config: dict,
        connection_manager: ConnectionManager,
        pipeline: MitigationTaskPipeline,
    ):
        """
        Initialize worker.

        Args:
            kafka_config: Kafka consumer configuration
            connection_manager: For API connections
            pipeline: Processing pipeline
        """
        self.kafka_config = kafka_config
        self.connections = connection_manager
        self.pipeline = pipeline

        self.consumer: AIOKafkaConsumer = None
        self.running = False

        # Shutdown handling
        self._shutdown_event = asyncio.Event()

    async def start(self):
        """Start the worker (connect to Kafka)."""
        logger.info("Starting Mitigation Task Tracking Worker")

        # Initialize telemetry
        from kafka_pipeline.common.telemetry import initialize_telemetry

        initialize_telemetry(
            service_name="mitigation-tracking-worker",
            environment=os.getenv("ENVIRONMENT", "development"),
        )

        # Create Kafka consumer
        self.consumer = AIOKafkaConsumer(
            self.kafka_config["input_topic"],
            bootstrap_servers=self.kafka_config["bootstrap_servers"],
            group_id=self.kafka_config["consumer_group"],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            enable_auto_commit=False,  # Manual commit for exactly-once
            auto_offset_reset="earliest",  # Process existing messages when no committed offset
            metadata_max_age_ms=30000,  # Refresh metadata every 30s
        )

        await self.consumer.start()
        logger.info(
            "Consumer started",
            extra={
                "topic": self.kafka_config["input_topic"],
                "group": self.kafka_config["consumer_group"],
            },
        )

        self.running = True

    async def run(self):
        """
        Main processing loop.

        Clear flow:
        1. Consume message from Kafka
        2. Process through pipeline
        3. Commit offset
        4. Handle errors
        """
        logger.info("Worker running - waiting for messages")

        try:
            async for message in self.consumer:
                # Check for shutdown
                if self._shutdown_event.is_set():
                    logger.info("Shutdown signal received, stopping consumer")
                    break

                try:
                    # Process message through pipeline
                    result = await self.pipeline.process(message.value)

                    # Commit offset (exactly-once semantics)
                    await self.consumer.commit()

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
                    # Validation error - log and skip message
                    logger.error(f"Validation error: {e}", extra={"offset": message.offset})
                    await self.consumer.commit()  # Skip bad message

                except Exception as e:
                    # Processing error - log, commit to skip, and continue
                    logger.exception(
                        f"Failed to process message: {e}", extra={"offset": message.offset}
                    )
                    # Commit to skip failed message and continue polling
                    await self.consumer.commit()
                    continue

        except KafkaError as e:
            logger.exception(f"Kafka error: {e}")
            raise

    async def stop(self):
        """Stop the worker gracefully."""
        logger.info("Stopping worker")
        self.running = False
        self._shutdown_event.set()

        if self.consumer:
            await self.consumer.stop()
            logger.info("Consumer stopped")

    def signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}")
        self._shutdown_event.set()


def load_yaml_config(path: Path) -> dict:
    """Load YAML configuration file."""
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    logger.info(f"Loading configuration from {path}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_worker_config() -> dict:
    """Load worker configuration from YAML."""
    config_data = load_yaml_config(WORKERS_CONFIG_PATH)
    workers = config_data.get("workers", {})

    if "mitigation_tracking" not in workers:
        raise ValueError(f"Worker 'mitigation_tracking' not found in {WORKERS_CONFIG_PATH}")

    return workers["mitigation_tracking"]


def load_connections() -> list[ConnectionConfig]:
    """Load connection configurations."""
    config_data = load_yaml_config(CONNECTIONS_CONFIG_PATH)

    connections = []
    for conn_name, conn_data in config_data.get("connections", {}).items():
        if not conn_data or not conn_data.get("base_url"):
            continue

        # Expand environment variables
        base_url = os.path.expandvars(conn_data["base_url"])
        auth_token = os.path.expandvars(conn_data.get("auth_token", ""))

        # Validate that environment variables were actually expanded
        if "${" in base_url:
            raise ValueError(
                f"Environment variable not expanded in base_url for connection '{conn_name}': {base_url}. "
                f"Check that all required environment variables are set in .env file."
            )
        if auth_token and "${" in auth_token:
            raise ValueError(
                f"Environment variable not expanded in auth_token for connection '{conn_name}'. "
                f"Check that all required environment variables are set in .env file."
            )

        auth_type = conn_data.get("auth_type", "none")
        if isinstance(auth_type, str):
            auth_type = AuthType(auth_type)

        conn = ConnectionConfig(
            name=conn_data.get("name", conn_name),
            base_url=base_url,
            auth_type=auth_type,
            auth_token=auth_token,
            auth_header=conn_data.get("auth_header"),
            timeout_seconds=conn_data.get("timeout_seconds", 30),
            max_retries=conn_data.get("max_retries", 3),
            retry_backoff_base=conn_data.get("retry_backoff_base", 2),
            retry_backoff_max=conn_data.get("retry_backoff_max", 60),
            headers=conn_data.get("headers", {}),
        )
        connections.append(conn)
        logger.info(f"Loaded connection: {conn.name} -> {conn.base_url}")

    return connections


async def main():
    """Main entry point."""
    # Load environment variables from .env file before any config access
    load_dotenv(PROJECT_ROOT / ".env")

    # Setup logging
    setup_logging(
        name="mitigation_tracking",
        domain="claimx_mitigation_task",
        stage="tracking",
        log_dir=Path("logs"),
        json_format=True,
        console_level=logging.INFO,
        file_level=logging.DEBUG,
    )

    # Load configuration
    try:
        worker_config = load_worker_config()
        connections_list = load_connections()
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)

    # Setup Kafka configuration
    kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    kafka_config = {
        "bootstrap_servers": kafka_servers,
        "input_topic": worker_config["kafka"]["input_topic"],
        "consumer_group": worker_config["kafka"]["consumer_group"],
    }

    # Log startup with Kafka config
    log_worker_startup(
        logger=logger,
        worker_name="Mitigation Task Tracking Worker",
        kafka_bootstrap_servers=kafka_servers,
        input_topic=kafka_config["input_topic"],
        output_topic=worker_config.get("pipeline", {}).get("output_topic"),
        consumer_group=kafka_config["consumer_group"],
    )

    # Setup connection manager
    connection_manager = ConnectionManager()
    for conn in connections_list:
        connection_manager.add_connection(conn)

    # Setup Kafka producer
    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_servers,
        value_serializer=lambda v: v,  # Already serialized in pipeline
    )
    await producer.start()

    # Create pipeline
    pipeline = MitigationTaskPipeline(
        connection_manager=connection_manager,
        kafka_producer=producer,
        config=worker_config.get("pipeline", {}),
    )

    # Create and run worker
    worker = MitigationTrackingWorker(
        kafka_config=kafka_config,
        connection_manager=connection_manager,
        pipeline=pipeline,
    )

    # Setup signal handlers (Windows-compatible)
    loop = asyncio.get_event_loop()
    try:
        # Unix signal handling
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(worker.stop()))
    except NotImplementedError:
        # Windows doesn't support add_signal_handler
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating shutdown")
            asyncio.create_task(worker.stop())

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    try:
        await connection_manager.start()
        await worker.start()
        await worker.run()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.exception(f"Worker failed: {e}")
        sys.exit(1)
    finally:
        await worker.stop()
        await connection_manager.close()
        await producer.stop()
        logger.info("Worker shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
