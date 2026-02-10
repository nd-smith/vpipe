"""
iTel Cabinet API Worker

Consumes completed task payloads from tracking worker and sends to iTel API.
Simple, focused worker - no enrichment, just transformation and API call.

Usage:
    python -m pipeline.plugins.itel_cabinet_api.itel_cabinet_api_worker

Dev Mode (writes to files):
    python -m pipeline.plugins.itel_cabinet_api.itel_cabinet_api_worker --dev

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
from typing import Any

import yaml
from dotenv import load_dotenv

from config.config import MessageConfig
from core.logging import setup_logging
from pipeline.common.transport import create_consumer
from pipeline.common.types import PipelineMessage
from pipeline.plugins.shared.connections import (
    AuthType,
    ConnectionConfig,
    ConnectionManager,
    is_http_error,
)

# Project root directory (where .env file is located)
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent

logger = logging.getLogger(__name__)

# Image type mapping: question_key -> vendor image_type
IMAGE_TYPE_MAP = {
    "overview_photos": "overview",
    # Lower Cabinet
    "lower_cabinet_box": "low_overview",
    "lower_face_frames_doors_drawers": "low_face",
    "lower_cabinet_end_panels": "low_end",
    # Upper Cabinet
    "upper_cabinet_box": "upp_overview",
    "upper_face_frames_doors_drawers": "upp_face",
    "upper_cabinet_end_panels": "upp_end",
    # Full Height Cabinet
    "full_height_cabinet_box": "fh_overview",
    "full_height_face_frames_doors_drawers": "fh_face",
    "full_height_end_panels": "fh_end",
    # Island Cabinet
    "island_cabinet_box": "isl_overview",
    "island_face_frames_doors_drawers": "isl_face",
    "island_cabinet_end_panels": "isl_end",
}

# Configuration paths
CONFIG_DIR = Path(__file__).parent.parent.parent.parent / "config"
WORKERS_CONFIG_PATH = CONFIG_DIR / "plugins" / "claimx" / "itel_cabinet_api" / "workers.yaml"
CONNECTIONS_CONFIG_PATH = CONFIG_DIR / "plugins" / "shared" / "connections" / "app.itel.yaml"


class ItelCabinetApiWorker:
    """
    Worker that sends completed task data to iTel Cabinet API.

    Uses transport layer abstraction for Kafka/EventHub compatibility.

    Responsibilities:
    1. Consume from itel.cabinet.completed topic via transport layer
    2. Transform submission/attachments into iTel API format
    3. Send to iTel API (or write to file in test mode)

    Transport Layer Contract:
    - Consumes via create_consumer() with message handler
    - Single-message processing pattern
    - Commit handled automatically by transport layer
    """

    def __init__(
        self,
        kafka_config: dict,
        api_config: dict,
        connection_manager: ConnectionManager,
        simulation_config: Any | None = None,
    ):
        """
        Initialize API worker.

        Args:
            kafka_config: Kafka consumer configuration
            api_config: API configuration (connection, endpoint, test mode)
            connection_manager: For API connections
            simulation_config: Optional simulation configuration (if enabled at startup)
        """
        self.kafka_config = kafka_config
        self.api_config = api_config
        self.connections = connection_manager
        self.simulation_config = simulation_config

        self.consumer = None
        self.running = False
        self._shutdown_event = asyncio.Event()

        # Setup simulation mode if config provided
        if self.simulation_config:
            self.output_dir = self.simulation_config.local_storage_path / "itel_submissions"
            self.output_dir.mkdir(parents=True, exist_ok=True)
            logger.info(
                "iTel Cabinet worker running in simulation mode",
                extra={
                    "output_dir": str(self.output_dir),
                    "api_calls_disabled": True,
                    "simulation_mode": True,
                },
            )
        else:
            self.output_dir = None

        logger.info(
            "ItelCabinetApiWorker initialized",
            extra={
                "test_mode": api_config.get("test_mode", False),
                "simulation_mode": bool(self.simulation_config),
                "endpoint": api_config.get("endpoint"),
            },
        )

    async def _handle_message(self, record: PipelineMessage) -> None:
        """
        Process a single message from the transport layer.

        Args:
            record: PipelineMessage from consumer
        """
        try:
            payload = record.value

            # Transform to iTel API format
            api_payload = self._transform_to_api_format(payload)

            # Send to API or write to file
            if self.simulation_config:
                await self._write_simulation_payload(api_payload, payload)
            elif self.api_config.get("test_mode", False):
                await self._write_test_payload(api_payload, payload)
            else:
                await self._send_to_api(api_payload)

            logger.info(
                "Message processed successfully",
                extra={
                    "assignment_id": payload.get("assignment_id"),
                    "simulation_mode": bool(self.simulation_config),
                    "test_mode": self.api_config.get("test_mode", False),
                },
            )

        except Exception as e:
            logger.exception(
                f"Failed to process message: {e}",
                extra={"offset": getattr(record, "offset", "unknown")},
            )
            # Re-raise to let transport layer handle error
            raise

    async def start(self):
        """Start the worker using transport layer."""
        logger.info("Starting iTel Cabinet API Worker")

        # Initialize telemetry
        import os

        from pipeline.common.telemetry import initialize_telemetry

        initialize_telemetry(
            service_name="itel-cabinet-api-worker",
            environment=os.getenv("ENVIRONMENT", "development"),
        )

        # Create minimal KafkaConfig for transport layer
        config = MessageConfig(bootstrap_servers=self.kafka_config["bootstrap_servers"])

        # Create consumer via transport layer
        self.consumer = await create_consumer(
            config=config,
            domain="plugins",
            worker_name="itel_cabinet_api_worker",
            topics=[self.kafka_config["input_topic"]],
            message_handler=self._handle_message,
            enable_message_commit=True,
        )

        await self.consumer.start()

        logger.info(
            "Consumer started via transport layer",
            extra={
                "topic": self.kafka_config["input_topic"],
                "group": self.kafka_config["consumer_group"],
                "transport_layer": "enabled",
            },
        )

        self.running = True

    async def run(self):
        """
        Main run loop - transport layer handles message consumption.

        The transport layer's consumer processes messages via _handle_message callback.
        This method just waits for shutdown signal.
        """
        logger.info("Worker running - transport layer consuming messages")

        try:
            # Wait for shutdown signal
            await self._shutdown_event.wait()
            logger.info("Shutdown signal received")

        except Exception as e:
            logger.exception("Worker run loop error: %s", e)
            raise

    def _transform_to_api_format(self, payload: dict) -> dict:
        """
        Transform tracking worker payload into iTel vendor API format.

        Transforms our internal format to match the vendor's required schema.

        Args:
            payload: Message from tracking worker with submission, attachments, and readable_report

        Returns:
            iTel API formatted payload matching vendor schema
        """
        submission = payload.get("submission", {})
        attachments = payload.get("attachments", [])
        readable_report = payload.get("readable_report", {})
        meta = readable_report.get("meta", {})

        logger.info(
            "Transforming payload to vendor format",
            extra={
                "assignment_id": submission.get("assignment_id"),
                "attachment_count": len(attachments),
            },
        )

        # Build images array from attachments
        images = self._build_images_array(attachments)

        # Build cabinet repair specs from submission
        cabinet_specs = self._build_cabinet_repair_specs(submission)

        # Build opinion replacement value specs (linear feet)
        linear_feet_specs = self._build_linear_feet_specs(submission)

        # Build iTel vendor API payload
        api_payload = {
            # Integration & Claim IDs
            "integration_test_id": str(submission.get("assignment_id", "")),
            "claim_number": submission.get("project_id", ""),
            "external_claim_id": submission.get("project_id", ""),
            # Claim metadata
            "cat_code": "",
            "claim_type": "Other",
            "claim_type_other_description": submission.get("damage_description", ""),
            "loss_type": None,
            "loss_date": meta.get("dates", {}).get("assigned", submission.get("date_assigned")),
            "service_level": "one_hour",
            # Insured information
            "insured": {
                "name": self._build_full_name(
                    submission.get("customer_first_name"),
                    submission.get("customer_last_name"),
                ),
                "street_number": "",
                "street_name": "",
                "city": "",
                "state": "",
                "zip_code": "",
                "country": None,
            },
            # Adjuster information
            "adjuster": {
                "carrier_id": "",  # TODO: Configure iTel carrier GUID
                "adjuster_id": "",
                "first_name": "",
                "last_name": "",
                "phone": "",
                "email": submission.get("assignor_email", ""),
            },
            # Images
            "images": images,
            # Cabinet damage specifications
            "cabinet_repair_specs": cabinet_specs,
            # Linear feet measurements
            "opinion_replacement_value_specs": linear_feet_specs,
        }

        return api_payload

    def _build_full_name(self, first_name: str | None, last_name: str | None) -> str:
        """Build full name from first and last name."""
        parts = []
        if first_name:
            parts.append(first_name)
        if last_name:
            parts.append(last_name)
        return " ".join(parts) if parts else ""

    def _build_images_array(self, attachments: list) -> list[dict]:
        """
        Build images array from attachments.

        Maps our question_key to vendor's image_type values.

        Args:
            attachments: List of attachment dicts with question_key and url

        Returns:
            List of image objects with image_type and url
        """
        images = []
        for attachment in attachments:
            question_key = attachment.get("question_key", "")
            url = attachment.get("url")

            # Map question_key to vendor image_type
            image_type = IMAGE_TYPE_MAP.get(question_key)

            if image_type and url:
                images.append({"image_type": image_type, "url": url})
            elif url:
                # Log unmapped image types for debugging
                logger.warning(
                    "Unmapped question_key for image",
                    extra={
                        "question_key": question_key,
                        "question_text": attachment.get("question_text"),
                    },
                )

        logger.debug("Built images array", extra={"image_count": len(images)})
        return images

    def _build_cabinet_repair_specs(self, submission: dict) -> dict:
        """
        Build cabinet_repair_specs from submission data.

        Maps our field names to vendor's field names.

        Args:
            submission: Submission dict with cabinet damage data

        Returns:
            Cabinet repair specs dict matching vendor schema
        """
        return {
            "damage_description": submission.get("damage_description", ""),
            # Upper cabinets
            "upper_cabinets_damaged": submission.get("upper_cabinets_damaged", False),
            "upper_cabinets_damaged_count": submission.get("num_damaged_upper_boxes", 0),
            "upper_cabinets_detached": submission.get("upper_cabinets_detached", False),
            "upper_faces_frames_doors_drawers_available": self._normalize_yes_no(
                submission.get("upper_face_frames_doors_drawers_available")
            ),
            "upper_faces_frames_doors_drawers_damaged": submission.get(
                "upper_face_frames_doors_drawers_damaged", False
            ),
            "upper_end_panels_damaged": submission.get("upper_finished_end_panels_damaged", False),
            # Lower cabinets
            "lower_cabinets_damaged": submission.get("lower_cabinets_damaged", False),
            "lower_cabinets_damaged_count": submission.get("num_damaged_lower_boxes", 0),
            "lower_cabinets_detached": submission.get("lower_cabinets_detached", False),
            "lower_faces_frames_doors_drawers_available": self._normalize_yes_no(
                submission.get("lower_face_frames_doors_drawers_available")
            ),
            "lower_faces_frames_doors_drawers_damaged": submission.get(
                "lower_face_frames_doors_drawers_damaged", False
            ),
            "lower_end_panels_damaged": submission.get("lower_finished_end_panels_damaged", False),
            "lower_cabinets_counter_top_type": submission.get("lower_counter_type", ""),
            # Full height cabinets
            "full_height_cabinets_damaged": submission.get("full_height_cabinets_damaged", False),
            "full_height_pantry_cabinets_damaged_count": submission.get(
                "num_damaged_full_height_boxes", 0
            ),
            "full_height_pantry_cabinets_detached": submission.get(
                "full_height_cabinets_detached", False
            ),
            "full_height_frames_doors_drawers_available": self._normalize_yes_no(
                submission.get("full_height_face_frames_doors_drawers_available")
            ),
            "full_height_frames_doors_drawers_damaged": submission.get(
                "full_height_face_frames_doors_drawers_damaged", False
            ),
            "full_height_end_panels_damaged": submission.get(
                "full_height_finished_end_panels_damaged", False
            ),
            # Island cabinets
            "island_cabinets_damaged": submission.get("island_cabinets_damaged", False),
            "island_cabinets_damaged_count": submission.get("num_damaged_island_boxes", 0),
            "island_cabinets_detached": submission.get("island_cabinets_detached", False),
            "island_frames_doors_drawers_available": self._normalize_yes_no(
                submission.get("island_face_frames_doors_drawers_available")
            ),
            "island_frames_doors_drawers_damaged": submission.get(
                "island_face_frames_doors_drawers_damaged", False
            ),
            "island_end_panels_damaged": submission.get(
                "island_finished_end_panels_damaged", False
            ),
            "island_cabinets_counter_top_type": submission.get("island_counter_type", ""),
            # Other details
            "other_details_and_instructions": submission.get("additional_notes", ""),
        }

    def _build_linear_feet_specs(self, submission: dict) -> dict:
        """
        Build opinion_replacement_value_specs with linear feet measurements.

        Args:
            submission: Submission dict with linear feet data

        Returns:
            Linear feet specs dict matching vendor schema
        """
        return {
            "upper_cabinets_linear_ft": submission.get("upper_cabinets_lf"),
            "lower_cabinets_linear_ft": submission.get("lower_cabinets_lf"),
            "full_height_cabinets_linear_ft": submission.get("full_height_cabinets_lf"),
            "island_cabinets_linear_ft": submission.get("island_cabinets_lf"),
            "counter_top_linear_ft": submission.get("countertops_lf"),
        }

    def _normalize_yes_no(self, value: str | bool | None) -> str:
        """
        Normalize yes/no values to lowercase string.

        Args:
            value: Boolean or string value

        Returns:
            "yes", "no", or "No" (default)
        """
        if value is None:
            return "No"
        if isinstance(value, bool):
            return "yes" if value else "no"
        if isinstance(value, str):
            cleaned = value.strip().lower()
            if cleaned in ("yes", "true", "1"):
                return "yes"
            return "no"
        return "No"

    async def _send_to_api(self, api_payload: dict):
        """Send payload to iTel Cabinet API."""
        assignment_id = api_payload.get("integration_test_id", "unknown")

        logger.info(
            "Sending to iTel API",
            extra={
                "endpoint": self.api_config["endpoint"],
                "assignment_id": assignment_id,
            },
        )

        status, response = await self.connections.request_json(
            connection_name=self.api_config["connection"],
            method=self.api_config.get("method", "POST"),
            path=self.api_config["endpoint"],
            json=api_payload,
        )

        if is_http_error(status):
            raise Exception(f"iTel API returned error status {status}: {response}")

        logger.info(
            "iTel API request successful",
            extra={
                "status": status,
                "assignment_id": assignment_id,
            },
        )

    async def _write_test_payload(self, api_payload: dict, original_payload: dict):
        """Write payload to file instead of sending to API (test mode)."""
        output_dir = Path(self.api_config.get("test_output_dir", "test_output"))
        output_dir.mkdir(parents=True, exist_ok=True)

        assignment_id = api_payload.get("integration_test_id", "unknown")
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        # Write the transformed API payload
        api_filename = f"payload_{assignment_id}_{timestamp}.json"
        api_output_path = output_dir / api_filename

        with open(api_output_path, "w") as f:
            json.dump(api_payload, f, indent=2, default=str)

        # Also write the original Kafka payload for debugging
        original_filename = f"original_{assignment_id}_{timestamp}.json"
        original_output_path = output_dir / original_filename

        with open(original_output_path, "w") as f:
            json.dump(original_payload, f, indent=2, default=str)

        # Log details about images for debugging
        images = api_payload.get("images", [])
        logger.info(
            "[TEST MODE] Vendor schema payload written",
            extra={
                "assignment_id": assignment_id,
                "api_payload_file": str(api_output_path),
                "original_payload_file": str(original_output_path),
                "image_count": len(images),
                "image_types": [img.get("image_type") for img in images],
            },
        )

    async def _write_simulation_payload(self, api_payload: dict, original_payload: dict):
        """Write payload to simulation directory (simulation mode).

        In simulation mode, submissions are written to /tmp/pcesdopodappv1_simulation/itel_submissions/
        instead of being sent to the real iTel API. This enables end-to-end testing
        without external dependencies.

        Args:
            api_payload: Transformed payload for iTel API (vendor schema format)
            original_payload: Original Kafka message payload
        """
        # Extract assignment_id from new vendor schema
        assignment_id = api_payload.get("integration_test_id", "unknown")

        # Create filename with timestamp for uniqueness
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        filename = f"itel_submission_{assignment_id}_{timestamp}.json"
        filepath = self.output_dir / filename

        # Build submission data in vendor format with simulation metadata
        submission_data = {
            **api_payload,  # Include full vendor payload
            # Add simulation metadata
            "submitted_at": datetime.utcnow().isoformat(),
            "simulation_mode": True,
            "source": "itel_cabinet_api_worker",
        }

        # Write to file
        filepath.write_text(json.dumps(submission_data, indent=2, default=str))

        logger.info(
            "[SIMULATION MODE] iTel submission written to file (vendor schema)",
            extra={
                "assignment_id": assignment_id,
                "filepath": str(filepath),
                "claim_number": api_payload.get("claim_number"),
                "image_count": len(api_payload.get("images", [])),
                "simulation_mode": True,
            },
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

    logger.info("Loading configuration from %s", path)
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_worker_config() -> dict:
    """Load worker configuration from YAML."""
    config_data = load_yaml_config(WORKERS_CONFIG_PATH)
    workers = config_data.get("workers", {})

    if "itel_cabinet_api" not in workers:
        raise ValueError(f"Worker 'itel_cabinet_api' not found in {WORKERS_CONFIG_PATH}")

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
        logger.info("Loaded connection: %s -> %s", conn.name, conn.base_url)

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
        help="Enable dev mode (writes API payloads to test directory instead of sending)",
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
        logger.error("Configuration error: %s", e)
        sys.exit(1)

    # Override test mode if --dev flag
    if args.dev:
        worker_config["api"]["test_mode"] = True

    # Setup connection manager
    connection_manager = ConnectionManager()
    for conn in connections_list:
        connection_manager.add_connection(conn)

    # Kafka configuration
    kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    kafka_config = {
        "bootstrap_servers": kafka_servers,
        "input_topic": worker_config["kafka"]["input_topic"],
        "consumer_group": worker_config["kafka"]["consumer_group"],
    }

    logger.info("Kafka bootstrap servers: %s", kafka_servers)
    logger.info("Input topic: %s", kafka_config["input_topic"])
    logger.info("Consumer group: %s", kafka_config["consumer_group"])

    # Check for simulation mode
    simulation_config = None
    try:
        from pipeline.simulation import get_simulation_config, is_simulation_mode

        if is_simulation_mode():
            simulation_config = get_simulation_config()
            logger.info("Simulation mode detected - worker will write to local files")
    except ImportError:
        # Simulation module not available
        pass

    # Create and run worker
    worker = ItelCabinetApiWorker(
        kafka_config=kafka_config,
        api_config=worker_config["api"],
        connection_manager=connection_manager,
        simulation_config=simulation_config,
    )

    # Setup signal handlers (Windows-compatible)
    loop = asyncio.get_event_loop()
    try:
        # Unix signal handling
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(worker.stop()))
    except NotImplementedError:
        # Windows doesn't support add_signal_handler
        # Use signal.signal instead (less graceful but works)
        def signal_handler(signum, frame):
            logger.info("Received signal %s, initiating shutdown", signum)
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
        logger.exception("Worker failed: %s", e)
        sys.exit(1)
    finally:
        await worker.stop()
        await connection_manager.close()
        logger.info("Worker shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
