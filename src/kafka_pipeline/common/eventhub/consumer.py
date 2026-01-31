"""Azure Event Hub consumer adapter.

Implements the same interface as BaseKafkaConsumer but uses azure-eventhub SDK
with AMQP over WebSocket transport for compatibility with Azure Private Link.

Architecture notes:
- Namespace connection string (no EntityPath) + eventhub_name parameter
- Event Hub names and consumer groups are resolved per-topic from config.yaml
- Event Hub uses checkpoint store for offset management (vs Kafka consumer groups)
- Partition assignment is automatic (no manual partition assignment like Kafka)
- Uses async iteration instead of poll-based consumption
"""

import asyncio
import json
import logging
import socket
import time
from typing import Awaitable, Callable, List, Optional

from azure.eventhub import EventData, TransportType
from azure.eventhub.aio import EventHubConsumerClient
from aiokafka import AIOKafkaProducer
from aiokafka.structs import ConsumerRecord

from core.logging import get_logger, log_with_context, log_exception, KafkaLogContext
from core.errors.exceptions import ErrorCategory
from core.errors.kafka_classifier import KafkaErrorClassifier
from kafka_pipeline.common.metrics import (
    record_message_consumed,
    record_processing_error,
    update_connection_status,
    update_assigned_partitions,
    message_processing_duration_seconds,
)

logger = get_logger(__name__)


class EventHubConsumerRecord:
    """Adapts EventData to look like ConsumerRecord for compatibility."""

    def __init__(self, event_data: EventData, eventhub_name: str, partition: str):
        self.topic = eventhub_name
        self.partition = int(partition) if partition else 0
        self.offset = event_data.offset if hasattr(event_data, 'offset') else 0
        self.timestamp = int(event_data.enqueued_time.timestamp() * 1000) if event_data.enqueued_time else 0
        self.key = event_data.properties.get("_key", "").encode("utf-8") if event_data.properties else None
        self.value = event_data.body_as_bytes()

        # Convert properties back to headers format
        self.headers = []
        if event_data.properties:
            for k, v in event_data.properties.items():
                if k != "_key":  # Skip internal key property
                    self.headers.append((k, str(v).encode("utf-8")))


class EventHubConsumer:
    """Event Hub consumer with BaseKafkaConsumer-compatible interface.

    Uses azure-eventhub SDK with TransportType.AmqpOverWebsocket for
    compatibility with Azure Private Link endpoints.

    Note: Event Hub checkpointing is handled automatically after successful
    message processing (when commit() is called).
    """

    def __init__(
        self,
        connection_string: str,
        domain: str,
        worker_name: str,
        eventhub_name: str,
        consumer_group: str,
        message_handler: Callable[[ConsumerRecord], Awaitable[None]],
        enable_message_commit: bool = True,
        instance_id: Optional[str] = None,
    ):
        """Initialize Event Hub consumer.

        Args:
            connection_string: Namespace-level connection string (no EntityPath)
            domain: Pipeline domain (e.g., "xact", "claimx")
            worker_name: Worker name for logging
            eventhub_name: Event Hub name (resolved from config.yaml by transport layer)
            consumer_group: Consumer group name (resolved from config.yaml by transport layer)
            message_handler: Async function to process each message
            enable_message_commit: Whether to commit offsets after processing
            instance_id: Optional instance identifier for parallel consumers
        """
        self.connection_string = connection_string
        self.domain = domain
        self.worker_name = worker_name
        self.instance_id = instance_id
        self.eventhub_name = eventhub_name
        self.consumer_group = consumer_group
        self.message_handler = message_handler
        self._consumer: Optional[EventHubConsumerClient] = None
        self._running = False
        self._enable_message_commit = enable_message_commit
        self._dlq_producer: Optional[AIOKafkaProducer] = None
        self._current_partition_context = {}  # Track partition contexts for checkpointing

        log_with_context(
            logger,
            logging.INFO,
            "Initialized Event Hub consumer",
            domain=domain,
            worker_name=worker_name,
            entity=eventhub_name,
            consumer_group=consumer_group,
            enable_message_commit=enable_message_commit,
        )

    async def start(self) -> None:
        if self._running:
            logger.warning("Consumer already running, ignoring duplicate start call")
            return

        log_with_context(
            logger,
            logging.INFO,
            "Starting Event Hub consumer",
            entity=self.eventhub_name,
            consumer_group=self.consumer_group,
        )

        try:
            # Apply SSL dev bypass if configured
            # This must be done before creating the client
            from core.security.ssl_dev_bypass import apply_ssl_dev_bypass
            apply_ssl_dev_bypass()

            # Create consumer with AMQP over WebSocket transport
            # Namespace connection string + eventhub_name parameter
            self._consumer = EventHubConsumerClient.from_connection_string(
                conn_str=self.connection_string,
                consumer_group=self.consumer_group,
                eventhub_name=self.eventhub_name,
                transport_type=TransportType.AmqpOverWebsocket,
            )

            self._running = True
            update_connection_status("consumer", connected=True)

            log_with_context(
                logger,
                logging.INFO,
                "Event Hub consumer started successfully",
                entity=self.eventhub_name,
                consumer_group=self.consumer_group,
            )

            # Start consuming
            await self._consume_loop()

        except asyncio.CancelledError:
            logger.info("Consumer loop cancelled, shutting down")
            raise
        except Exception as e:
            log_exception(logger, e, "Consumer loop terminated with error")
            raise
        finally:
            self._running = False

    async def stop(self) -> None:
        if not self._running or self._consumer is None:
            logger.debug("Consumer not running or already stopped")
            return

        logger.info("Stopping Event Hub consumer")
        self._running = False

        try:
            if self._consumer:
                await self._consumer.close()

            if self._dlq_producer is not None:
                try:
                    await self._dlq_producer.flush()
                    await self._dlq_producer.stop()
                    logger.info("DLQ producer stopped successfully")
                except Exception as dlq_error:
                    log_exception(logger, dlq_error, "Error stopping DLQ producer")
                finally:
                    self._dlq_producer = None

            logger.info("Event Hub consumer stopped successfully")
        except Exception as e:
            log_exception(logger, e, "Error stopping Event Hub consumer")
            raise
        finally:
            update_connection_status("consumer", connected=False)
            update_assigned_partitions(self.consumer_group, 0)
            self._consumer = None

    async def commit(self) -> None:
        """Commit offsets for processed messages.

        For Event Hub, this is handled by checkpointing in the partition context.
        This method is called after successful batch processing.
        """
        if self._consumer is None:
            logger.warning("Cannot commit: consumer not started")
            return

        log_with_context(
            logger,
            logging.DEBUG,
            "Committed checkpoints",
            consumer_group=self.consumer_group,
        )

    async def _consume_loop(self) -> None:
        """Main consumption loop using Event Hub async receive."""
        log_with_context(
            logger,
            logging.INFO,
            "Starting message consumption loop",
            entity=self.eventhub_name,
            consumer_group=self.consumer_group,
        )

        # Define event handler for each partition
        async def on_event(partition_context, event):
            """Process single event from Event Hub partition."""
            if not self._running:
                return

            # Store partition context for checkpointing
            partition_id = partition_context.partition_id
            self._current_partition_context[partition_id] = partition_context

            # Convert EventData to ConsumerRecord for compatibility
            record = EventHubConsumerRecord(event, self.eventhub_name, partition_id)

            # Process the message
            await self._process_message(record)

            # Checkpoint after successful processing
            if self._enable_message_commit:
                await partition_context.update_checkpoint(event)

        async def on_partition_initialize(partition_context):
            """Called when partition is assigned to this consumer."""
            partition_id = partition_context.partition_id
            log_with_context(
                logger,
                logging.INFO,
                "Partition assigned",
                entity=self.eventhub_name,
                consumer_group=self.consumer_group,
                partition_id=partition_id,
            )
            # Update metrics
            current_count = len(self._current_partition_context)
            update_assigned_partitions(self.consumer_group, current_count + 1)

        async def on_partition_close(partition_context, reason):
            """Called when partition is revoked from this consumer."""
            partition_id = partition_context.partition_id
            log_with_context(
                logger,
                logging.INFO,
                "Partition revoked",
                entity=self.eventhub_name,
                consumer_group=self.consumer_group,
                partition_id=partition_id,
                reason=reason,
            )
            # Clean up partition context
            self._current_partition_context.pop(partition_id, None)
            # Update metrics
            update_assigned_partitions(self.consumer_group, len(self._current_partition_context))

        async def on_error(partition_context, error):
            """Called when error occurs during consumption."""
            partition_id = partition_context.partition_id if partition_context else "unknown"
            log_exception(
                logger,
                error,
                "Error in Event Hub consumption",
                entity=self.eventhub_name,
                consumer_group=self.consumer_group,
                partition_id=partition_id,
            )

        # Start receiving events
        # This runs until stop() is called
        try:
            async with self._consumer:
                await self._consumer.receive(
                    on_event=on_event,
                    on_partition_initialize=on_partition_initialize,
                    on_partition_close=on_partition_close,
                    on_error=on_error,
                    starting_position="-1",  # Start from beginning (like earliest)
                )
        except Exception as e:
            log_exception(logger, e, "Error in Event Hub receive loop")
            raise

    async def _process_message(self, message: ConsumerRecord) -> None:
        """Process a single message (same logic as BaseKafkaConsumer)."""
        from kafka_pipeline.common.telemetry import get_tracer

        tracer = get_tracer(__name__)
        with tracer.start_active_span("eventhub.message.process") as scope:
            span = scope.span if hasattr(scope, "span") else scope
            span.set_tag("messaging.system", "eventhub")
            span.set_tag("messaging.destination", message.topic)
            span.set_tag("messaging.kafka.partition", message.partition)
            span.set_tag("messaging.kafka.offset", message.offset)
            span.set_tag("messaging.kafka.consumer_group", self.consumer_group)
            span.set_tag(
                "messaging.message.id", message.key.decode("utf-8") if message.key else None
            )
            span.set_tag("span.kind", "consumer")

            with KafkaLogContext(
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
                key=message.key.decode("utf-8") if message.key else None,
                consumer_group=self.consumer_group,
            ):
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "Processing message",
                    message_size=len(message.value) if message.value else 0,
                )

                start_time = time.perf_counter()
                message_size = len(message.value) if message.value else 0

                try:
                    await self.message_handler(message)

                    duration = time.perf_counter() - start_time
                    message_processing_duration_seconds.labels(
                        topic=message.topic, consumer_group=self.consumer_group
                    ).observe(duration)

                    record_message_consumed(
                        message.topic, self.consumer_group, message_size, success=True
                    )

                    log_with_context(
                        logger,
                        logging.DEBUG,
                        "Message processed successfully",
                        duration_ms=round(duration * 1000, 2),
                    )

                except Exception as e:
                    duration = time.perf_counter() - start_time
                    message_processing_duration_seconds.labels(
                        topic=message.topic, consumer_group=self.consumer_group
                    ).observe(duration)

                    record_message_consumed(
                        message.topic, self.consumer_group, message_size, success=False
                    )

                    span.set_tag("error", True)
                    span.log_kv({"event": "error", "error.object": str(e)})

                    await self._handle_processing_error(message, e, duration)

    async def _handle_processing_error(
        self, message: ConsumerRecord, error: Exception, duration: float
    ) -> None:
        """Error classification with DLQ routing (same as BaseKafkaConsumer)."""
        classified_error = KafkaErrorClassifier.classify_consumer_error(
            error,
            context={
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "group_id": self.consumer_group,
            },
        )

        error_category = classified_error.category
        record_processing_error(message.topic, self.consumer_group, error_category.value)

        common_context = {
            "error_category": error_category.value,
            "classified_as": type(classified_error).__name__,
            "duration_ms": round(duration * 1000, 2),
        }

        if error_category == ErrorCategory.PERMANENT:
            log_exception(
                logger,
                error,
                "Permanent error processing message - routing to DLQ",
                **common_context,
            )
            # DLQ routing would go here if needed
            # For now, just log and continue

        elif error_category == ErrorCategory.TRANSIENT:
            log_exception(
                logger,
                error,
                "Transient error - will reprocess message",
                level=logging.WARNING,
                **common_context,
            )

        else:
            log_exception(
                logger,
                error,
                "Unknown error category - applying conservative retry",
                **common_context,
            )

    @property
    def is_running(self):
        return self._running and self._consumer is not None


__all__ = [
    "EventHubConsumer",
    "EventHubConsumerRecord",
]
