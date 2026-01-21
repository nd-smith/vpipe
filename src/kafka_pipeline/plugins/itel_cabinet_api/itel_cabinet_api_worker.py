"""
iTel Cabinet API Worker

Consumes completed task payloads from tracking worker and sends to iTel API.
Simple, focused worker - no enrichment, just transformation and API call.

Usage:
    python -m kafka_pipeline.plugins.itel_cabinet_api.itel_cabinet_api_worker

Dev Mode (writes to files):
    python -m kafka_pipeline.plugins.itel_cabinet_api.itel_cabinet_api_worker --dev

Configuration:
    config/plugins/claimx/itel_cabinet_api/workers.yaml

Environment Variables:
    ITEL_CABINET_API_BASE_URL: iTel API base URL
    ITEL_CABINET_API_TOKEN: Bearer token for iTel API
    KAFKA_BOOTSTRAP_SERVERS: Kafka broker addresses (default: localhost:9094)
"""

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from dotenv import load_dotenv

from core.logging import setup_logging, get_logger

# Project root directory (where .env file is located)
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent

from kafka_pipeline.plugins.shared.connections import (
    AuthType,
    ConnectionConfig,
    ConnectionManager,
)

logger = get_logger(__name__)

# Configuration paths
CONFIG_DIR = Path(__file__).parent.parent.parent.parent / "config"
WORKERS_CONFIG_PATH = CONFIG_DIR / "plugins" / "itel_cabinet_api" / "workers.yaml"
CONNECTIONS_CONFIG_PATH = CONFIG_DIR / "plugins" / "shared" / "connections" / "app.itel.yaml"


class ItelCabinetApiWorker:
    """
    Worker that sends completed task data to iTel Cabinet API.

    Responsibilities:
    1. Consume from itel.cabinet.completed topic
    2. Transform submission/attachments into iTel API format
    3. Send to iTel API (or write to file in test mode)

    Simple, explicit flow - easy to trace and debug.
    """

    def __init__(
        self,
        kafka_config: dict,
        api_config: dict,
        connection_manager: ConnectionManager,
    ):
        """
        Initialize API worker.

        Args:
            kafka_config: Kafka consumer configuration
            api_config: API configuration (connection, endpoint, test mode)
            connection_manager: For API connections
        """
        self.kafka_config = kafka_config
        self.api_config = api_config
        self.connections = connection_manager

        self.consumer: AIOKafkaConsumer = None
        self.running = False
        self._shutdown_event = asyncio.Event()

        logger.info(
            "ItelCabinetApiWorker initialized",
            extra={
                'test_mode': api_config.get('test_mode', False),
                'endpoint': api_config.get('endpoint'),
            }
        )

    async def start(self):
        """Start the worker (connect to Kafka)."""
        logger.info("Starting iTel Cabinet API Worker")

        # Initialize OpenTracing
        from kafka_pipeline.common.telemetry import initialize_telemetry
        import os

        initialize_telemetry(
            service_name="itel-cabinet-api-worker",
            environment=os.getenv("ENVIRONMENT", "development"),
        )

        self.consumer = AIOKafkaConsumer(
            self.kafka_config['input_topic'],
            bootstrap_servers=self.kafka_config['bootstrap_servers'],
            group_id=self.kafka_config['consumer_group'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            enable_auto_commit=False,
            # Consumer group membership timeout settings
            session_timeout_ms=self.kafka_config.get('session_timeout_ms', 45000),
            max_poll_interval_ms=self.kafka_config.get('max_poll_interval_ms', 600000),
            heartbeat_interval_ms=self.kafka_config.get('heartbeat_interval_ms', 15000),
        )

        await self.consumer.start()
        logger.info(
            "Consumer started",
            extra={
                'topic': self.kafka_config['input_topic'],
                'group': self.kafka_config['consumer_group'],
            }
        )

        self.running = True

    async def run(self):
        """
        Main processing loop.

        Clear flow:
        1. Consume message
        2. Transform to iTel API format
        3. Send to API (or write to file)
        4. Commit offset
        """
        logger.info("Worker running - waiting for messages")

        try:
            async for message in self.consumer:
                if self._shutdown_event.is_set():
                    logger.info("Shutdown signal received")
                    break

                try:
                    payload = message.value

                    # Transform to iTel API format
                    api_payload = self._transform_to_api_format(payload)

                    # Send to API or write to file
                    if self.api_config.get('test_mode', False):
                        await self._write_test_payload(api_payload, payload)
                    else:
                        await self._send_to_api(api_payload)

                    # Commit offset
                    await self.consumer.commit()

                    logger.info(
                        "Message processed successfully",
                        extra={
                            'assignment_id': payload.get('assignment_id'),
                            'test_mode': self.api_config.get('test_mode', False),
                        }
                    )

                except Exception as e:
                    logger.exception(
                        f"Failed to process message: {e}",
                        extra={'offset': message.offset}
                    )
                    # Don't commit - will retry

        except KafkaError as e:
            logger.exception(f"Kafka error: {e}")
            raise

    def _transform_to_api_format(self, payload: dict) -> dict:
        """
        Transform tracking worker payload into iTel API format.

        Now uses the readable_report format which organizes data by topic
        for easier consumption.

        Args:
            payload: Message from tracking worker with submission, attachments, and readable_report

        Returns:
            iTel API formatted payload
        """
        submission = payload.get('submission', {})
        attachments = payload.get('attachments', [])
        readable_report = payload.get('readable_report', {})

        logger.info(
            f"Processing payload with readable_report",
            extra={
                'assignment_id': submission.get('assignment_id'),
                'topics_count': len(readable_report.get('topics', {})),
            }
        )

        # Build iTel API payload using readable_report
        # The readable_report already has all form data organized by topic
        api_payload = {
            # Traceability
            "traceId": payload.get("event_id"),  # For end-to-end tracing

            # Metadata from readable_report.meta
            "meta": readable_report.get("meta", {}),

            # Topic-organized data from readable_report.topics
            "data": readable_report.get("topics", {}),

            # Customer info from submission
            "customer": {
                "firstName": submission.get("customer_first_name"),
                "lastName": submission.get("customer_last_name"),
                "email": submission.get("customer_email"),
                "phone": submission.get("customer_phone"),
            },
        }

        return api_payload

    async def _send_to_api(self, api_payload: dict):
        """Send payload to iTel Cabinet API."""
        logger.info(
            "Sending to iTel API",
            extra={
                'endpoint': self.api_config['endpoint'],
                'assignment_id': api_payload.get('assignmentId'),
            }
        )

        # Send to API with tracing span
        from kafka_pipeline.common.telemetry import get_tracer

        tracer = get_tracer(__name__)
        with tracer.start_active_span("itel.api.submit") as scope:
            span = scope.span if hasattr(scope, 'span') else scope
            span.set_tag("span.kind", "client")
            # Set span attributes
            span.set_tag("http.method", self.api_config.get('method', 'POST'))
            span.set_tag("http.url", self.api_config['endpoint'])
            assignment_id = api_payload.get('meta', {}).get('assignmentId') or api_payload.get('assignmentId')
            if assignment_id:
                span.set_tag("assignment_id", assignment_id)

            trace_id = api_payload.get('traceId')
            if trace_id:
                span.set_tag("trace_id", trace_id)

            status, response = await self.connections.request_json(
                connection_name=self.api_config['connection'],
                method=self.api_config.get('method', 'POST'),
                path=self.api_config['endpoint'],
                json=api_payload,
            )

            # Set response attributes
            span.set_tag("http.status_code", status)
            span.set_tag("success", 200 <= status < 300)

            if status < 200 or status >= 300:
                raise Exception(f"iTel API returned error status {status}: {response}")

        logger.info(
            "iTel API request successful",
            extra={
                'status': status,
                'assignment_id': assignment_id,
            }
        )

    async def _write_test_payload(self, api_payload: dict, original_payload: dict):
        """Write payload to file instead of sending to API (test mode)."""
        output_dir = Path(self.api_config.get('test_output_dir', 'test_output'))
        output_dir.mkdir(parents=True, exist_ok=True)

        assignment_id = api_payload.get('assignmentId', 'unknown')
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        # Write the transformed API payload
        api_filename = f"payload_{assignment_id}_{timestamp}.json"
        api_output_path = output_dir / api_filename

        with open(api_output_path, 'w') as f:
            json.dump(api_payload, f, indent=2, default=str)

        # Also write the original Kafka payload for debugging
        original_filename = f"original_{assignment_id}_{timestamp}.json"
        original_output_path = output_dir / original_filename

        with open(original_output_path, 'w') as f:
            json.dump(original_payload, f, indent=2, default=str)

        # Log details about attachments for debugging
        attachments = original_payload.get('attachments', [])
        if attachments:
            sample_att = attachments[0]
            logger.info(
                f"[TEST MODE] Sample attachment keys: {list(sample_att.keys()) if isinstance(sample_att, dict) else 'not a dict'}",
                extra={
                    'sample_attachment': sample_att,
                    'total_attachments': len(attachments),
                }
            )

        logger.info(
            "[TEST MODE] Payloads written to files",
            extra={
                'assignment_id': assignment_id,
                'api_payload_file': str(api_output_path),
                'original_payload_file': str(original_output_path),
            }
        )

    async def stop(self):
        """Stop the worker gracefully."""
        logger.info("Stopping worker")
        self.running = False
        self._shutdown_event.set()

        if self.consumer:
            await self.consumer.stop()
            logger.info("Consumer stopped")


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

    if "itel_cabinet_api" not in workers:
        raise ValueError(
            f"Worker 'itel_cabinet_api' not found in {WORKERS_CONFIG_PATH}"
        )

    return workers["itel_cabinet_api"]


def load_connections() -> list[ConnectionConfig]:
    """Load connection configurations."""
    config_data = load_yaml_config(CONNECTIONS_CONFIG_PATH)

    connections = []
    for conn_name, conn_data in config_data.get("connections", {}).items():
        if not conn_data or not conn_data.get("base_url"):
            continue

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

    # Parse arguments
    parser = argparse.ArgumentParser(
        description="iTel Cabinet API Worker - Sends completed tasks to iTel Cabinet API"
    )
    parser.add_argument(
        "--dev",
        action="store_true",
        help="Enable dev mode (writes API payloads to test directory instead of sending)"
    )
    args = parser.parse_args()

    # Setup logging
    setup_logging(
        name="itel_cabinet_api",
        domain="itel_cabinet_api",
        stage="api",
        log_dir=Path("logs"),
        json_format=True,
        console_level=logging.INFO,
        file_level=logging.DEBUG,
    )

    logger.info("=" * 70)
    logger.info("Starting iTel Cabinet API Worker")
    if args.dev:
        logger.info("DEV MODE: Payloads will be written to test directory")
    logger.info("=" * 70)

    # Load configuration
    try:
        worker_config = load_worker_config()
        connections_list = load_connections()
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)

    # Override test mode if --dev flag
    if args.dev:
        worker_config['api']['test_mode'] = True

    # Setup connection manager
    connection_manager = ConnectionManager()
    for conn in connections_list:
        connection_manager.add_connection(conn)

    # Kafka configuration
    kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    kafka_config = {
        'bootstrap_servers': kafka_servers,
        'input_topic': worker_config['kafka']['input_topic'],
        'consumer_group': worker_config['kafka']['consumer_group'],
    }

    logger.info(f"Kafka bootstrap servers: {kafka_servers}")
    logger.info(f"Input topic: {kafka_config['input_topic']}")
    logger.info(f"Consumer group: {kafka_config['consumer_group']}")

    # Create and run worker
    worker = ItelCabinetApiWorker(
        kafka_config=kafka_config,
        api_config=worker_config['api'],
        connection_manager=connection_manager,
    )

    # Setup signal handlers (Windows-compatible)
    loop = asyncio.get_event_loop()
    try:
        # Unix signal handling
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(
                sig,
                lambda: asyncio.create_task(worker.stop())
            )
    except NotImplementedError:
        # Windows doesn't support add_signal_handler
        # Use signal.signal instead (less graceful but works)
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
        logger.info("Worker shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
