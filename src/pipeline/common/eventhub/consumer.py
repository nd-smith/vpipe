"""Azure Event Hub consumer adapter.

Implements the same interface as MessageConsumer but uses azure-eventhub SDK
with AMQP over WebSocket transport for compatibility with Azure Private Link.

Architecture notes:
- Namespace connection string (no EntityPath) + eventhub_name parameter
- Event Hub names and consumer groups are resolved per-topic from config.yaml
- Event Hub uses checkpoint store for offset management (vs Kafka consumer groups)
- Partition assignment is automatic (no manual partition assignment like Kafka)
- Uses async iteration instead of poll-based consumption

Checkpoint persistence:
- If checkpoint_store is provided: offsets are persisted to Azure Blob Storage,
  enabling durable progress tracking across restarts and partition rebalancing
- If checkpoint_store is None: offsets are stored in-memory only and lost on restart,
  consumer will restart from starting_position (default "@latest")
"""

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from azure.eventhub import EventData, TransportType
from azure.eventhub.aio import EventHubConsumerClient

from core.errors.exceptions import ErrorCategory
from core.errors.transport_classifier import TransportErrorClassifier
from core.logging import MessageLogContext
from core.security.ssl_utils import get_ca_bundle_kwargs
from core.utils import generate_worker_id
from pipeline.common.eventhub.diagnostics import (
    log_connection_attempt_details,
    log_connection_diagnostics,
    mask_connection_string,
)
from pipeline.common.eventhub.producer import EventHubProducer
from pipeline.common.metrics import (
    message_processing_duration_seconds,
    record_dlq_message,
    record_message_consumed,
    record_processing_error,
    update_assigned_partitions,
    update_connection_status,
)
from pipeline.common.types import PipelineMessage

logger = logging.getLogger(__name__)


class EventHubConsumerRecord:
    """Adapts EventData to PipelineMessage for transport-agnostic processing.

    Converts Azure Event Hub EventData to the transport-agnostic PipelineMessage type,
    enabling the same message handlers to work with both Kafka and Event Hub.

    Conversion details:
    - EventHub entity name -> PipelineMessage.topic
    - Partition ID (string) -> PipelineMessage.partition (int)
    - EventData.offset -> PipelineMessage.offset
    - EventData.enqueued_time (datetime) -> PipelineMessage.timestamp (int milliseconds)
    - EventData.properties["_key"] -> PipelineMessage.key (bytes)
    - EventData.body -> PipelineMessage.value (bytes)
    - EventData.properties (dict) -> PipelineMessage.headers (List[Tuple[str, bytes]])
    """

    @staticmethod
    def _extract_key(properties: dict | None) -> bytes | None:
        """Extract message key from EventData properties."""
        if not properties or "_key" not in properties:
            return None
        key_prop = properties.get("_key", "")
        if not key_prop:
            return None
        return key_prop if isinstance(key_prop, bytes) else str(key_prop).encode("utf-8")

    @staticmethod
    def _to_bytes(value) -> bytes:
        """Convert a property value to bytes."""
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            return value.encode("utf-8")
        return str(value).encode("utf-8")

    @staticmethod
    def _convert_headers(properties: dict | None) -> list[tuple[str, bytes]] | None:
        """Convert EventData properties to header tuples, excluding internal _key."""
        if not properties:
            return None
        headers = []
        for k, v in properties.items():
            key_name = k.decode("utf-8", errors="replace") if isinstance(k, bytes) else str(k)
            if key_name != "_key":
                headers.append((key_name, EventHubConsumerRecord._to_bytes(v)))
        return headers if headers else None

    def __init__(self, event_data: EventData, eventhub_name: str, partition: str) -> None:
        """Convert EventData to PipelineMessage.

        Args:
            event_data: Azure Event Hub EventData object
            eventhub_name: Name of the Event Hub entity (used as topic)
            partition: Partition ID as string (converted to int)
        """
        timestamp_ms = 0
        if event_data.enqueued_time:
            timestamp_ms = int(event_data.enqueued_time.timestamp() * 1000)

        self._message = PipelineMessage(
            topic=eventhub_name,
            partition=int(partition) if partition else 0,
            offset=event_data.offset if hasattr(event_data, "offset") else 0,
            timestamp=timestamp_ms,
            key=self._extract_key(event_data.properties),
            value=event_data.body if isinstance(event_data.body, bytes) else b"".join(event_data.body),
            headers=self._convert_headers(event_data.properties),
        )

    def to_pipeline_message(self) -> PipelineMessage:
        """Get the underlying PipelineMessage for handlers that accept it directly."""
        return self._message


@dataclass
class EventHubConsumerOptions:
    """Optional configuration for EventHubConsumer."""

    enable_message_commit: bool = True
    instance_id: str | None = None
    checkpoint_store: Any = None
    prefetch: int = 300
    starting_position: Any = field(default="@latest")
    starting_position_inclusive: bool = False
    checkpoint_interval: int = 1
    owner_level: int = 0


class EventHubConsumer:
    """Event Hub consumer with MessageConsumer-compatible interface.

    Uses azure-eventhub SDK with TransportType.AmqpOverWebsocket for
    compatibility with Azure Private Link endpoints.

    Checkpoint persistence behavior:
    - With checkpoint_store: offsets persisted to Azure Blob Storage (durable)
    - Without checkpoint_store: offsets stored in-memory only (lost on restart)
    """

    def __init__(
        self,
        connection_string: str,
        domain: str,
        worker_name: str,
        eventhub_name: str,
        consumer_group: str,
        message_handler: Callable[[PipelineMessage], Awaitable[None]],
        options: EventHubConsumerOptions | None = None,
        # Legacy keyword args — forwarded to options if provided
        **kwargs,
    ):
        """Initialize Event Hub consumer.

        Args:
            connection_string: Namespace-level connection string (no EntityPath)
            domain: Pipeline domain (e.g., "verisk", "claimx")
            worker_name: Worker name for logging
            eventhub_name: Event Hub name (resolved from config.yaml by transport layer)
            consumer_group: Consumer group name (resolved from config.yaml by transport layer)
            message_handler: Async function to process each PipelineMessage
            options: Optional EventHubConsumerOptions for advanced settings
        """
        # Support both options object and legacy kwargs
        if options is None:
            options = EventHubConsumerOptions(**kwargs)

        self.connection_string = connection_string
        self.domain = domain
        self.worker_name = worker_name
        self.instance_id = options.instance_id
        self.eventhub_name = eventhub_name
        self.consumer_group = consumer_group
        self.message_handler = message_handler
        self.checkpoint_store = options.checkpoint_store
        self.prefetch = options.prefetch
        self.starting_position = options.starting_position
        self.starting_position_inclusive = options.starting_position_inclusive
        self._consumer: EventHubConsumerClient | None = None
        self._running = False
        self._enable_message_commit = options.enable_message_commit
        self._dlq_producer: EventHubProducer | None = None
        self._current_partition_context = {}
        self._last_partition_event = {}
        self._checkpoint_count = 0
        self._checkpoint_interval = options.checkpoint_interval
        self._partition_since_checkpoint = {}
        self._owner_level = options.owner_level or None

        prefix = f"{domain}-{worker_name}"
        if options.instance_id:
            prefix = f"{prefix}-{options.instance_id}"
        self.worker_id = generate_worker_id(prefix)

        self._dlq_entity_map = self._build_dlq_entity_map()

        logger.info(
            "Initialized Event Hub consumer",
            extra={
                "domain": domain,
                "worker_name": worker_name,
                "entity": eventhub_name,
                "consumer_group": consumer_group,
                "enable_message_commit": options.enable_message_commit,
                "prefetch": options.prefetch,
                "checkpoint_interval": options.checkpoint_interval,
                "checkpoint_persistence": (
                    "blob_storage" if options.checkpoint_store else "in_memory"
                ),
                "owner_level": self._owner_level,
            },
        )

    async def start(self) -> None:
        """Start the Event Hub consumer.

        Creates EventHubConsumerClient and begins consuming messages.
        If checkpoint_store was provided during initialization, it will be
        passed to the client for durable offset persistence in Azure Blob Storage.

        Raises:
            Exception: If consumer initialization or connection fails
        """
        if self._running:
            logger.warning("Consumer already running, ignoring duplicate start call")
            return

        checkpoint_mode = (
            "with blob storage checkpoint persistence"
            if self.checkpoint_store
            else "with in-memory checkpoints only"
        )
        logger.info(
            f"Starting Event Hub consumer {checkpoint_mode}",
            extra={
                "entity": self.eventhub_name,
                "consumer_group": self.consumer_group,
                "checkpoint_persistence": (
                    "blob_storage" if self.checkpoint_store else "in_memory"
                ),
            },
        )

        try:
            # Log comprehensive connection diagnostics
            log_connection_diagnostics(self.connection_string, self.eventhub_name)

            ssl_kwargs = get_ca_bundle_kwargs()

            # Log connection attempt details
            log_connection_attempt_details(
                eventhub_name=self.eventhub_name,
                transport_type="AmqpOverWebsocket",
                ssl_kwargs=ssl_kwargs,
            )

            # Create consumer with AMQP over WebSocket transport
            # Namespace connection string + eventhub_name parameter
            # Pass checkpoint_store if provided for durable offset persistence
            self._consumer = EventHubConsumerClient.from_connection_string(
                conn_str=self.connection_string,
                consumer_group=self.consumer_group,
                eventhub_name=self.eventhub_name,
                transport_type=TransportType.AmqpOverWebsocket,
                checkpoint_store=self.checkpoint_store,
                **ssl_kwargs,
            )

            self._running = True
            update_connection_status("consumer", connected=True)

            logger.info(
                "Event Hub consumer started successfully",
                extra={
                    "entity": self.eventhub_name,
                    "consumer_group": self.consumer_group,
                    "checkpoint_persistence": (
                        "blob_storage" if self.checkpoint_store else "in_memory"
                    ),
                },
            )

            # Start consuming
            await self._consume_loop()

        except asyncio.CancelledError:
            logger.info("Consumer loop cancelled, shutting down")
            raise
        except Exception as e:
            masked_conn = mask_connection_string(self.connection_string)
            logger.error(
                "Consumer loop terminated with error",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "eventhub_name": self.eventhub_name,
                    "consumer_group": self.consumer_group,
                    "connection_string_masked": masked_conn,
                },
                exc_info=True,
            )
            raise
        finally:
            self._running = False

    async def stop(self) -> None:
        if self._consumer is None:
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
                except Exception:
                    logger.error("Error stopping DLQ producer", exc_info=True)
                finally:
                    self._dlq_producer = None

            logger.info("Event Hub consumer stopped successfully")
        except Exception:
            logger.error("Error stopping Event Hub consumer", exc_info=True)
            raise
        finally:
            update_connection_status("consumer", connected=False)
            update_assigned_partitions(self.consumer_group, 0)
            self._consumer = None

            # Allow time for aiohttp sessions to close properly
            # EventHubConsumerClient uses aiohttp internally with AmqpOverWebsocket
            # and doesn't always close sessions cleanly on exit
            await asyncio.sleep(0.250)

    async def commit(self) -> None:
        """Checkpoint each partition using its last-received event.

        Called by batch workers after successful batch processing to persist
        offsets to the checkpoint store (blob storage).
        """
        if self._consumer is None:
            logger.warning("Cannot commit: consumer not started")
            return

        if not self._last_partition_event:
            logger.debug("No events to checkpoint")
            return

        # Snapshot and clear — events arriving during commit go into next batch
        events = dict(self._last_partition_event)
        self._last_partition_event.clear()

        for partition_id, event in events.items():
            context = self._current_partition_context.get(partition_id)
            if context is None:
                continue
            await context.update_checkpoint(event)
            self._checkpoint_count += 1

        logger.debug(
            "Committed checkpoints",
            extra={
                "consumer_group": self.consumer_group,
                "partitions_checkpointed": len(events),
            },
        )

    async def _maybe_checkpoint(self, partition_context, event, partition_id: str, offset) -> None:
        """Checkpoint if commit is enabled and interval is reached."""
        if not self._enable_message_commit:
            return
        count = self._partition_since_checkpoint.get(partition_id, 0) + 1
        if count < self._checkpoint_interval:
            self._partition_since_checkpoint[partition_id] = count
            return
        await partition_context.update_checkpoint(event)
        self._checkpoint_count += 1
        self._partition_since_checkpoint[partition_id] = 0
        if self._checkpoint_count % 1000 == 0:
            logger.info(
                "Checkpoint heartbeat",
                extra={
                    "partition_id": partition_id,
                    "offset": offset,
                    "total_checkpoints": self._checkpoint_count,
                    "consumer_group": self.consumer_group,
                },
            )

    async def _on_event(self, partition_context, event):
        """Process single event from Event Hub partition."""
        if not self._running or event is None:
            return

        partition_id = partition_context.partition_id
        self._current_partition_context[partition_id] = partition_context
        self._last_partition_event[partition_id] = event

        record_adapter = EventHubConsumerRecord(event, self.eventhub_name, partition_id)
        message = record_adapter.to_pipeline_message()

        try:
            await self._process_message(message)
            await self._maybe_checkpoint(partition_context, event, partition_id, message.offset)
        except Exception:
            logger.error(
                "Message processing failed - will not checkpoint",
                extra={
                    "entity": self.eventhub_name,
                    "partition_id": partition_id,
                    "offset": message.offset,
                },
                exc_info=True,
            )

    async def _on_partition_initialize(self, partition_context):
        """Called when partition is assigned to this consumer."""
        partition_id = partition_context.partition_id
        checkpoint_type = "blob_storage" if self.checkpoint_store else "in_memory"
        logger.info(
            "Partition assigned",
            extra={
                "entity": self.eventhub_name,
                "consumer_group": self.consumer_group,
                "partition_id": partition_id,
                "checkpoint_type": checkpoint_type,
            },
        )
        current_count = len(self._current_partition_context)
        update_assigned_partitions(self.consumer_group, current_count + 1)

    async def _on_partition_close(self, partition_context, reason):
        """Called when partition is revoked from this consumer."""
        partition_id = partition_context.partition_id

        # Flush uncheckpointed events before releasing partition
        uncheckpointed = self._partition_since_checkpoint.get(partition_id, 0)
        if self._enable_message_commit and uncheckpointed > 0:
            last_event = self._last_partition_event.get(partition_id)
            if last_event:
                await partition_context.update_checkpoint(last_event)
                self._checkpoint_count += 1
                logger.info(
                    "Flushed checkpoint on partition close",
                    extra={
                        "partition_id": partition_id,
                        "uncheckpointed_events": uncheckpointed,
                    },
                )
        self._partition_since_checkpoint.pop(partition_id, None)

        logger.info(
            "Partition revoked",
            extra={
                "entity": self.eventhub_name,
                "consumer_group": self.consumer_group,
                "partition_id": partition_id,
                "reason": reason,
            },
        )
        self._current_partition_context.pop(partition_id, None)
        self._last_partition_event.pop(partition_id, None)
        update_assigned_partitions(self.consumer_group, len(self._current_partition_context))

    async def _on_error(self, partition_context, error):
        """Called when error occurs during consumption."""
        partition_id = partition_context.partition_id if partition_context else "unknown"
        logger.error(
            "Event Hub consumer error on partition %s: %s: %s",
            partition_id,
            type(error).__name__,
            error,
            extra={
                "partition_id": partition_id,
                "error_type": type(error).__name__,
                "error": str(error),
            },
        )

    async def _consume_loop(self) -> None:
        """Main consumption loop using Event Hub async receive."""
        logger.info(
            "Starting message consumption loop",
            extra={
                "entity": self.eventhub_name,
                "consumer_group": self.consumer_group,
            },
        )

        # NOTE: Do not use `async with self._consumer:` here. The stop() method
        # handles closing the consumer with a grace period for aiohttp session
        # cleanup. Using the context manager causes a double-close race where
        # __aexit__ closes after the grace period, leaking an aiohttp session.
        try:
            await self._consumer.receive(
                on_event=self._on_event,
                on_partition_initialize=self._on_partition_initialize,
                on_partition_close=self._on_partition_close,
                on_error=self._on_error,
                starting_position=self.starting_position,
                starting_position_inclusive=self.starting_position_inclusive,
                max_wait_time=5,
                prefetch=self.prefetch,
                owner_level=self._owner_level,
            )
        except Exception:
            logger.error("Error in Event Hub receive loop", exc_info=True)
            raise

    async def _process_message(self, message: PipelineMessage) -> None:
        """Process a single message using transport-agnostic PipelineMessage type."""
        with MessageLogContext(
            topic=message.topic,
            partition=message.partition,
            offset=message.offset,
            key=message.key.decode("utf-8") if message.key else None,
            consumer_group=self.consumer_group,
        ):
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

            except Exception as e:
                duration = time.perf_counter() - start_time
                message_processing_duration_seconds.labels(
                    topic=message.topic, consumer_group=self.consumer_group
                ).observe(duration)

                record_message_consumed(
                    message.topic, self.consumer_group, message_size, success=False
                )

                await self._handle_processing_error(message, e, duration)

    def _build_dlq_entity_map(self) -> dict:
        """Build mapping from source topic names to DLQ entity names.

        Maps:
        - verisk topics (verisk_events) -> verisk-dlq
        - claimx topics (claimx_events) -> claimx-dlq
        """
        return {
            # Verisk domain
            "verisk_events": "verisk-dlq",
            # ClaimX domain
            "claimx_events": "claimx-dlq",
        }

    def _get_dlq_entity_name(self, source_topic: str) -> str | None:
        """Get DLQ entity name for a source topic.

        Args:
            source_topic: Source Event Hub entity name

        Returns:
            DLQ entity name or None if no DLQ configured
        """
        dlq_entity = self._dlq_entity_map.get(source_topic)
        if dlq_entity is None:
            logger.warning(
                f"No DLQ entity mapping found for topic: {source_topic}. "
                f"Available mappings: {list(self._dlq_entity_map.keys())}"
            )
        return dlq_entity

    async def _ensure_dlq_producer(self, dlq_entity_name: str) -> None:
        """Lazy-initialize DLQ producer to avoid unnecessary connections.

        Args:
            dlq_entity_name: Name of the DLQ Event Hub entity
        """
        if self._dlq_producer is not None:
            # Check if we need to recreate for a different entity
            if self._dlq_producer.eventhub_name == dlq_entity_name:
                return
            else:
                # Need to close old producer and create new one
                logger.info(
                    f"Closing existing DLQ producer for {self._dlq_producer.eventhub_name} "
                    f"to create new one for {dlq_entity_name}"
                )
                await self._dlq_producer.stop()
                self._dlq_producer = None

        logger.info(
            "Initializing DLQ producer for permanent error routing",
            extra={
                "domain": self.domain,
                "worker_name": self.worker_name,
                "dlq_entity": dlq_entity_name,
            },
        )

        # Create EventHub producer for DLQ entity
        self._dlq_producer = EventHubProducer(
            connection_string=self.connection_string,
            domain=self.domain,
            worker_name=self.worker_name,
            eventhub_name=dlq_entity_name,
        )
        await self._dlq_producer.start()

        logger.info(
            "DLQ producer started successfully",
            extra={"dlq_entity": dlq_entity_name},
        )

    async def _send_to_dlq(
        self, message: PipelineMessage, error: Exception, error_category: ErrorCategory
    ) -> bool:
        """Send failed message to DLQ Event Hub with full context.

        Args:
            message: Original PipelineMessage that failed processing
            error: Exception that occurred during processing
            error_category: Classification of the error (PERMANENT or TRANSIENT)

        Returns:
            True if successfully sent to DLQ, False otherwise
        """
        # Determine DLQ entity name
        dlq_entity_name = self._get_dlq_entity_name(message.topic)
        if dlq_entity_name is None:
            logger.error(
                "Cannot route to DLQ - no DLQ entity configured for topic",
                extra={
                    "original_topic": message.topic,
                    "error_category": error_category.value,
                },
                exc_info=True,
            )
            return False

        # Ensure DLQ producer is initialized
        try:
            await self._ensure_dlq_producer(dlq_entity_name)
        except Exception:
            logger.error(
                "Failed to initialize DLQ producer",
                extra={"dlq_entity": dlq_entity_name},
                exc_info=True,
            )
            return False

        # Construct DLQ message with full context
        dlq_message = {
            "original_topic": message.topic,
            "original_partition": message.partition,
            "original_offset": message.offset,
            "original_key": message.key.decode("utf-8") if message.key else None,
            "original_value": message.value.decode("utf-8") if message.value else None,
            "original_headers": {
                k: v.decode("utf-8") if isinstance(v, bytes) else v
                for k, v in (message.headers or [])
            },
            "original_timestamp": message.timestamp,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "error_category": error_category.value,
            "consumer_group": self.consumer_group,
            "worker_id": self.worker_id,
            "domain": self.domain,
            "worker_name": self.worker_name,
            "dlq_timestamp": time.time(),
        }

        dlq_value = json.dumps(dlq_message).encode("utf-8")
        dlq_key = message.key or f"dlq-{message.offset}".encode()

        # Construct DLQ headers
        dlq_headers = {
            "dlq_source_topic": message.topic,
            "dlq_error_category": error_category.value,
            "dlq_consumer_group": self.consumer_group,
        }

        try:
            # Send to DLQ Event Hub
            metadata = await self._dlq_producer.send(
                topic=dlq_entity_name,
                key=dlq_key,
                value=dlq_value,
                headers=dlq_headers,
            )

            logger.info(
                "Message sent to DLQ successfully",
                extra={
                    "dlq_entity": dlq_entity_name,
                    "dlq_partition": metadata.partition,
                    "dlq_offset": metadata.offset,
                    "original_topic": message.topic,
                    "original_partition": message.partition,
                    "original_offset": message.offset,
                    "error_category": error_category.value,
                    "error_type": type(error).__name__,
                },
            )

            # Record metrics
            record_dlq_message(self.domain, error_category.value)

            return True

        except Exception:
            logger.error(
                "Failed to send message to DLQ - message will be retried",
                extra={
                    "dlq_entity": dlq_entity_name,
                    "original_topic": message.topic,
                    "original_partition": message.partition,
                    "original_offset": message.offset,
                    "error_category": error_category.value,
                },
                exc_info=True,
            )
            return False

    async def _route_to_dlq_or_raise(
        self, message: PipelineMessage, error: Exception, error_category: ErrorCategory
    ) -> None:
        """Send to DLQ; if DLQ write fails, re-raise to prevent checkpoint."""
        dlq_success = await self._send_to_dlq(message, error, error_category)
        if dlq_success:
            logger.info(
                "Message sent to DLQ successfully - will checkpoint to skip",
                extra={
                    "original_topic": message.topic,
                    "original_partition": message.partition,
                    "original_offset": message.offset,
                },
            )
            return
        logger.error(
            "DLQ write failed - preventing checkpoint, message will be retried",
            extra={
                "original_topic": message.topic,
                "original_partition": message.partition,
                "original_offset": message.offset,
                "error_category": error_category.value,
            },
        )
        raise error

    # Maps transient error categories to their log messages
    _TRANSIENT_ERROR_MESSAGES = {
        ErrorCategory.TRANSIENT: "Transient error - will reprocess message",
        ErrorCategory.AUTH: "Authentication error - will reprocess after token refresh",
        ErrorCategory.CIRCUIT_OPEN: "Circuit breaker open - will reprocess when circuit closes",
    }

    async def _handle_processing_error(
        self, message: PipelineMessage, error: Exception, duration: float
    ) -> None:
        """Error classification with DLQ routing using transport-agnostic PipelineMessage."""
        classified_error = TransportErrorClassifier.classify_consumer_error(
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
            logger.error(
                "Permanent error processing message - routing to DLQ",
                extra=common_context,
                exc_info=True,
            )
            await self._route_to_dlq_or_raise(message, error, error_category)
            return

        transient_msg = self._TRANSIENT_ERROR_MESSAGES.get(error_category)
        if transient_msg:
            logger.warning(transient_msg, extra=common_context, exc_info=True)
            return

        # Unknown category — route to DLQ to prevent silent data loss
        logger.error(
            f"Unhandled error category '{error_category.value}' - "
            f"routing to DLQ: {type(error).__name__}: {error}",
            extra=common_context,
            exc_info=True,
        )
        await self._route_to_dlq_or_raise(message, error, error_category)

    @property
    def is_running(self) -> bool:
        return self._running and self._consumer is not None


__all__ = [
    "EventHubConsumer",
    "EventHubConsumerOptions",
    "EventHubConsumerRecord",
]
