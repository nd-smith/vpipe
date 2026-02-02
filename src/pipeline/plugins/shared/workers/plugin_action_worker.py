"""
Generic Plugin Action Worker

A configurable worker that consumes messages from plugin-triggered Kafka topics,
enriches the data through a pipeline of handlers, and sends results to external APIs.

Designed to be declaratively configured via YAML without requiring custom code
for each plugin integration.
"""

import asyncio
import json
import logging
import signal
from dataclasses import dataclass
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from pipeline.common.producer import BaseKafkaProducer
from pipeline.common.types import PipelineMessage, from_consumer_record
from pipeline.plugins.shared.connections import (
    ConnectionConfig,
    ConnectionManager,
)
from pipeline.plugins.shared.enrichment import (
    BatchingHandler,
    EnrichmentPipeline,
    create_handler_from_config,
)

logger = logging.getLogger(__name__)


@dataclass
class WorkerConfig:
    """Configuration for plugin action worker.

    Attributes:
        name: Worker name for logging/metrics
        input_topic: Kafka topic to consume from
        consumer_group: Kafka consumer group ID
        destination_connection: Name of connection to send enriched data to
        destination_path: API endpoint path
        destination_method: HTTP method (POST, PUT, etc.)
        enrichment_handlers: List of enrichment handler configs
        batch_size: Message batch size for Kafka consumer
        error_topic: Optional topic to publish failed messages to
        success_topic: Optional topic to publish successful responses to
        max_retries: Max retries for API requests
        enable_auto_commit: Whether to auto-commit Kafka offsets
    """

    name: str
    input_topic: str
    consumer_group: str
    destination_connection: str
    destination_path: str
    destination_method: str = "POST"
    enrichment_handlers: list[dict[str, Any]] = None
    batch_size: int = 100
    error_topic: str | None = None
    success_topic: str | None = None
    max_retries: int = 3
    enable_auto_commit: bool = False


class PluginActionWorker:
    """Generic worker for processing plugin-triggered messages.

    This worker provides a complete pipeline for:
    1. Consuming messages from a Kafka topic
    2. Enriching data through a configurable pipeline (transform, lookup, validate, batch)
    3. Sending enriched data to external APIs
    4. Publishing results to success/error topics
    5. Handling graceful shutdown with batch flushing

    Example usage:
        config = WorkerConfig(
            name="photo_task_worker",
            input_topic="plugin.photo_tasks",
            consumer_group="photo_task_worker",
            destination_connection="external_api",
            destination_path="/v1/photo_tasks",
            enrichment_handlers=[
                {"type": "transform", "config": {"mappings": {...}}},
                {"type": "validation", "config": {"required_fields": [...]}}
            ]
        )

        worker = PluginActionWorker(
            config=config,
            kafka_config=kafka_config,
            connection_manager=connection_manager
        )

        await worker.start()
        await worker.run()
    """

    def __init__(
        self,
        config: WorkerConfig,
        kafka_config: dict[str, Any],
        connection_manager: ConnectionManager,
        producer: BaseKafkaProducer | None = None,
    ):
        """Initialize worker.

        Args:
            config: Worker configuration
            kafka_config: Kafka connection configuration (bootstrap_servers, etc.)
            connection_manager: Connection manager for API requests
            producer: Optional Kafka producer for success/error topics
        """
        self.config = config
        self.kafka_config = kafka_config
        self.connection_manager = connection_manager
        self.producer = producer

        # State
        self.consumer: AIOKafkaConsumer | None = None
        self.enrichment_pipeline: EnrichmentPipeline | None = None
        self.running = False
        self.shutdown_event = asyncio.Event()

        # Metrics
        self.messages_processed = 0
        self.messages_succeeded = 0
        self.messages_failed = 0
        self.messages_skipped = 0

        # Batching handlers (need special handling for flush)
        self.batching_handlers: list[BatchingHandler] = []

    async def start(self) -> None:
        """Initialize worker components."""
        logger.info("Starting PluginActionWorker: %s", self.config.name)

        # Build enrichment pipeline
        await self._build_enrichment_pipeline()

        # Create Kafka consumer
        await self._create_consumer()

        # Setup signal handlers
        self._setup_signal_handlers()

        logger.info(
            f"Worker '{self.config.name}' started "
            f"(topic: {self.config.input_topic}, "
            f"destination: {self.config.destination_connection}:{self.config.destination_path})"
        )

    async def _build_enrichment_pipeline(self) -> None:
        """Build enrichment pipeline from config."""
        if not self.config.enrichment_handlers:
            logger.warning("No enrichment handlers configured, using passthrough")
            self.enrichment_pipeline = EnrichmentPipeline(handlers=[])
            return

        handlers = []
        for handler_config in self.config.enrichment_handlers:
            handler = create_handler_from_config(handler_config)
            handlers.append(handler)

            # Track batching handlers for flush on shutdown
            if isinstance(handler, BatchingHandler):
                self.batching_handlers.append(handler)

        self.enrichment_pipeline = EnrichmentPipeline(handlers=handlers)
        await self.enrichment_pipeline.initialize()

        logger.info("Built enrichment pipeline with %s handlers", len(handlers))

    async def _create_consumer(self) -> None:
        """Create and configure Kafka consumer."""
        self.consumer = AIOKafkaConsumer(
            self.config.input_topic,
            bootstrap_servers=self.kafka_config["bootstrap_servers"],
            group_id=self.config.consumer_group,
            enable_auto_commit=self.config.enable_auto_commit,
            auto_offset_reset="earliest",
            max_poll_records=self.config.batch_size,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            # Consumer group membership timeout settings
            session_timeout_ms=self.kafka_config.get("session_timeout_ms", 45000),
            max_poll_interval_ms=self.kafka_config.get("max_poll_interval_ms", 600000),
            heartbeat_interval_ms=self.kafka_config.get("heartbeat_interval_ms", 15000),
        )

        try:
            await self.consumer.start()
        except Exception as e:
            # Clean up consumer on startup failure to prevent resource leak
            if self.consumer:
                try:
                    await self.consumer.stop()
                except Exception:
                    pass  # Ignore errors during cleanup
                self.consumer = None
            raise RuntimeError(
                f"Failed to start Kafka consumer for topic '{self.config.input_topic}': {e}. "
                f"Ensure Kafka is running and the topic exists."
            ) from e

        logger.info(
            f"Consumer started for topic '{self.config.input_topic}' "
            f"(group: {self.config.consumer_group})"
        )

    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""

        def signal_handler(sig, frame):
            logger.info("Received signal %s, initiating shutdown", sig)
            self.shutdown_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def run(self) -> None:
        """Main processing loop."""
        self.running = True

        try:
            while self.running and not self.shutdown_event.is_set():
                await self._process_batch()

        except Exception as e:
            logger.exception("Worker crashed: %s", e)
            raise
        finally:
            await self.stop()

    async def _process_batch(self) -> None:
        """Fetch and process a batch of messages."""
        try:
            # Fetch batch of messages
            data = await self.consumer.getmany(
                timeout_ms=1000, max_records=self.config.batch_size
            )

            if not data:
                # No messages, check for batch timeout flush
                await self._flush_batching_handlers()
                return

            # Process messages
            for _topic_partition, messages in data.items():
                for message in messages:
                    # Convert ConsumerRecord to PipelineMessage
                    pipeline_message = from_consumer_record(message)
                    await self._process_message(pipeline_message)

            # Commit offsets if not auto-committing
            if not self.config.enable_auto_commit:
                await self.consumer.commit()

        except KafkaError as e:
            logger.error("Kafka error in process batch: %s", e)
            await asyncio.sleep(1)  # Backoff on error

    async def _process_message(self, message: PipelineMessage) -> None:
        """Process a single message.

        Args:
            message: PipelineMessage from Kafka
        """
        self.messages_processed += 1

        try:
            # Parse message value
            message_data = message.value

            if self.messages_processed % 100 == 0:
                logger.info(
                    f"Processed {self.messages_processed} messages "
                    f"(succeeded: {self.messages_succeeded}, "
                    f"failed: {self.messages_failed}, "
                    f"skipped: {self.messages_skipped})"
                )

            # Run enrichment pipeline
            result = await self.enrichment_pipeline.execute(
                message=message_data, connection_manager=self.connection_manager
            )

            # Handle enrichment result
            if not result.success:
                logger.error(
                    f"Enrichment failed for message offset {message.offset}: {result.error}"
                )
                await self._handle_error(message_data, result.error)
                self.messages_failed += 1
                return

            if result.skip:
                # Message was added to batch or filtered out
                self.messages_skipped += 1
                return

            # Send enriched data to destination API
            await self._send_to_api(result.data, message_data)

        except Exception as e:
            logger.exception(
                f"Unexpected error processing message offset {message.offset}: {e}"
            )
            await self._handle_error(message.value, str(e))
            self.messages_failed += 1

    async def _send_to_api(
        self, enriched_data: dict[str, Any], original_message: dict[str, Any]
    ) -> None:
        """Send enriched data to destination API.

        Args:
            enriched_data: Data after enrichment pipeline
            original_message: Original message from Kafka
        """
        try:
            response = await self.connection_manager.request(
                connection_name=self.config.destination_connection,
                method=self.config.destination_method,
                path=self.config.destination_path,
                json=enriched_data,
                retry_override=self.config.max_retries,
            )

            status = response.status
            response_body = await response.text()

            if status >= 400:
                logger.error(
                    f"API request failed with status {status}: {response_body[:200]}"
                )
                await self._handle_error(
                    original_message,
                    f"API error {status}: {response_body[:200]}",
                )
                self.messages_failed += 1
                return

            # Success!
            logger.debug("Successfully sent to API: %s", status)
            await self._handle_success(enriched_data, response_body, status)
            self.messages_succeeded += 1

        except Exception as e:
            logger.exception("Failed to send to API: %s", e)
            await self._handle_error(original_message, f"API exception: {str(e)}")
            self.messages_failed += 1

    async def _handle_success(
        self, enriched_data: dict[str, Any], response_body: str, status_code: int
    ) -> None:
        """Handle successful API request.

        Args:
            enriched_data: Data sent to API
            response_body: API response body
            status_code: HTTP status code
        """
        if not self.config.success_topic or not self.producer:
            return

        success_message = {
            "enriched_data": enriched_data,
            "response": response_body,
            "status_code": status_code,
            "worker_name": self.config.name,
        }

        try:
            await self.producer.send_one(self.config.success_topic, success_message)
        except Exception as e:
            logger.error("Failed to publish success message: %s", e)

    async def _handle_error(self, original_message: dict[str, Any], error: str) -> None:
        """Handle failed message.

        Args:
            original_message: Original message from Kafka
            error: Error description
        """
        if not self.config.error_topic or not self.producer:
            logger.error("No error topic configured, dropping failed message: %s", error)
            return

        error_message = {
            "original_message": original_message,
            "error": error,
            "worker_name": self.config.name,
        }

        try:
            await self.producer.send_one(self.config.error_topic, error_message)
            logger.info("Published error message to %s", self.config.error_topic)
        except Exception as e:
            logger.error("Failed to publish error message: %s", e)

    async def _flush_batching_handlers(self) -> None:
        """Flush any batching handlers that have timed out."""
        for handler in self.batching_handlers:
            batch_data = await handler.flush()
            if batch_data:
                logger.info("Flushing timed-out batch")
                # Send batched data to API
                await self._send_to_api(batch_data, original_message={"batch": True})

    async def stop(self) -> None:
        """Stop worker and cleanup resources."""
        logger.info("Stopping worker '%s'", self.config.name)
        self.running = False

        # Flush any remaining batches
        logger.info("Flushing remaining batches...")
        await self._flush_batching_handlers()

        # Cleanup enrichment pipeline
        if self.enrichment_pipeline:
            await self.enrichment_pipeline.cleanup()

        # Close consumer
        if self.consumer:
            await self.consumer.stop()

        logger.info(
            f"Worker '{self.config.name}' stopped. "
            f"Total processed: {self.messages_processed}, "
            f"succeeded: {self.messages_succeeded}, "
            f"failed: {self.messages_failed}, "
            f"skipped: {self.messages_skipped}"
        )


async def main():
    """Example main function for running worker standalone."""
    # This would normally be loaded from YAML config
    worker_config = WorkerConfig(
        name="example_worker",
        input_topic="plugin.example",
        consumer_group="example_worker",
        destination_connection="external_api",
        destination_path="/v1/events",
        enrichment_handlers=[
            {
                "type": "transform",
                "config": {
                    "mappings": {
                        "event_id": "event_id",
                        "project_id": "project_id",
                    }
                },
            },
            {
                "type": "validation",
                "config": {
                    "required_fields": ["event_id", "project_id"],
                },
            },
        ],
    )

    kafka_config = {
        "bootstrap_servers": "localhost:9092",
    }

    # Setup connection manager
    connection_manager = ConnectionManager()
    connection_manager.add_connection(
        ConnectionConfig(
            name="external_api",
            base_url="https://api.example.com",
            auth_type="bearer",
            auth_token="your-token-here",
        )
    )
    await connection_manager.start()

    # Create and run worker
    worker = PluginActionWorker(
        config=worker_config,
        kafka_config=kafka_config,
        connection_manager=connection_manager,
    )

    try:
        await worker.start()
        await worker.run()
    finally:
        await connection_manager.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    asyncio.run(main())
