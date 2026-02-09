"""Azure Event Hub producer adapter.

Implements the same interface as MessageProducer but uses azure-eventhub SDK
with AMQP over WebSocket transport for compatibility with Azure Private Link.

Architecture notes:
- Event Hub uses AMQP protocol (port 5671 or WebSocket on 443)
- Namespace connection string (no EntityPath) + eventhub_name parameter
- Each Event Hub entity is specified separately via the SDK's eventhub_name arg
- Event Hub names are resolved per-topic from config.yaml by the transport layer
"""

import json
import logging
import os
import time
from typing import Any

from azure.eventhub import EventData, EventHubProducerClient, TransportType
from pydantic import BaseModel

from core.utils.json_serializers import json_serializer
from pipeline.common.eventhub.diagnostics import (
    log_connection_attempt_details,
    log_connection_diagnostics,
    mask_connection_string,
)
from pipeline.common.metrics import (
    message_processing_duration_seconds,
    record_message_produced,
    record_producer_error,
    update_connection_status,
)
from pipeline.common.types import ProduceResult

logger = logging.getLogger(__name__)


class EventHubRecordMetadata:
    """Adapts EventHub send result to ProduceResult for transport-agnostic processing.

    Converts EventHub producer confirmation to the transport-agnostic ProduceResult type,
    enabling the same code to work with both Kafka and Event Hub producer results.

    Note: Event Hub SDK does not provide partition/offset info synchronously,
    so we return placeholder values (partition=0, offset=0) for compatibility.
    """

    def __init__(self, topic: str, partition: int = 0, offset: int = 0) -> None:
        """Create ProduceResult from EventHub send confirmation.

        Args:
            topic: Name of the Event Hub entity the message was sent to
            partition: Partition number (0 for EventHub, assigned automatically)
            offset: Offset (0 for EventHub, not available in SDK)
        """
        self._result = ProduceResult(
            topic=topic,
            partition=partition,
            offset=offset,
        )

    def to_produce_result(self) -> ProduceResult:
        """Get the underlying ProduceResult for code that accepts it directly."""
        return self._result


class EventHubProducer:
    """Event Hub producer with MessageProducer-compatible interface.

    Uses azure-eventhub SDK with TransportType.AmqpOverWebsocket for
    compatibility with Azure Private Link endpoints.

    Note: Event Hub does not support multiple topics per connection.
    The entity name is passed via the SDK's `eventhub_name` parameter,
    separate from the namespace connection string.
    """

    def __init__(
        self,
        connection_string: str,
        domain: str,
        worker_name: str,
        eventhub_name: str,
    ):
        """Initialize Event Hub producer.

        Args:
            connection_string: Namespace-level connection string (no EntityPath)
            domain: Pipeline domain (e.g., "verisk", "claimx")
            worker_name: Worker name for logging
            eventhub_name: Event Hub name (resolved from config.yaml by transport layer)
        """
        self.connection_string = connection_string
        self.domain = domain
        self.worker_name = worker_name
        self.eventhub_name = eventhub_name
        self._producer: EventHubProducerClient | None = None
        self._started = False

        logger.info(
            "Initialized Event Hub producer",
            extra={
                "domain": domain,
                "worker_name": worker_name,
                "eventhub_name": self.eventhub_name,
                "transport": "AmqpOverWebsocket",
            },
        )

    async def start(self) -> None:
        if self._started:
            logger.warning("Producer already started, ignoring duplicate start call")
            return

        logger.info("Starting Event Hub producer")

        try:
            # Log comprehensive connection diagnostics
            log_connection_diagnostics(self.connection_string, self.eventhub_name)

            # Apply SSL configuration for production CA bundle
            # Check for custom CA bundle (production TLS-intercepting proxy)
            ssl_kwargs = {}
            ca_bundle = (
                os.getenv("SSL_CERT_FILE")
                or os.getenv("REQUESTS_CA_BUNDLE")
                or os.getenv("CURL_CA_BUNDLE")
            )
            if ca_bundle:
                ssl_kwargs = {"connection_verify": ca_bundle}
            else:
                logger.info("[DEBUG] Using system default CA certificates")

            # Log connection attempt details
            log_connection_attempt_details(
                eventhub_name=self.eventhub_name,
                transport_type="AmqpOverWebsocket",
                ssl_kwargs=ssl_kwargs,
            )

            # Create producer with AMQP over WebSocket transport
            # Namespace connection string + eventhub_name parameter
            # This is required for Azure Private Link endpoints
            logger.info("[DEBUG] Calling EventHubProducerClient.from_connection_string...")
            self._producer = EventHubProducerClient.from_connection_string(
                conn_str=self.connection_string,
                eventhub_name=self.eventhub_name,
                transport_type=TransportType.AmqpOverWebsocket,
                uamqp_transport=True,
                **ssl_kwargs,
            )
            logger.info("[DEBUG] Producer client object created successfully")

            # Test connection by getting properties
            logger.info("[DEBUG] Attempting to get Event Hub properties (triggers actual connection)...")
            props = self._producer.get_eventhub_properties()
            logger.info(f"[DEBUG] Successfully retrieved Event Hub properties: {props}")
            logger.info(
                f"Connected to Event Hub: {props.get('name', 'unknown')}, "
                f"partitions: {len(props.get('partition_ids', []))}"
            )

            self._started = True
            update_connection_status("producer", connected=True)

            logger.info(
                "Event Hub producer started successfully",
                extra={
                    "eventhub_name": self.eventhub_name,
                    "transport": "AmqpOverWebsocket",
                },
            )

        except Exception as e:
            # Log masked connection string for troubleshooting
            masked_conn = mask_connection_string(self.connection_string)

            logger.error(
                "[DEBUG] Failed to start Event Hub producer",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "eventhub_name": self.eventhub_name,
                    "ca_bundle": ca_bundle if ca_bundle else "system default",
                    "connection_string_masked": masked_conn,
                },
                exc_info=True,
            )
            logger.error(f"[DEBUG] Exception details: {repr(e)}")
            logger.error(f"[DEBUG] Connection string (masked): {masked_conn}")
            raise

    async def stop(self) -> None:
        if not self._started or self._producer is None:
            logger.debug("Producer not started or already stopped")
            return

        logger.info("Stopping Event Hub producer")

        try:
            self._producer.close()
            logger.info("Event Hub producer stopped successfully")
        except Exception as e:
            logger.error(
                "Error stopping Event Hub producer",
                extra={"error": str(e)},
                exc_info=True,
            )
        finally:
            update_connection_status("producer", connected=False)
            self._producer = None
            self._started = False

            # Allow time for aiohttp sessions to close properly
            # EventHubProducerClient uses aiohttp internally with AmqpOverWebsocket
            # and doesn't always close sessions cleanly on exit
            import asyncio
            await asyncio.sleep(0.250)

    async def send(
        self,
        value: BaseModel | dict[str, Any] | bytes,
        topic: str | None = None,
        key: str | bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> ProduceResult:
        """Send a single message to Event Hub.

        Args:
            value: Message value (Pydantic model, dict, or bytes)
            topic: Optional Event Hub entity name. If not provided, uses the producer's
                   configured eventhub_name. For compatibility with Kafka-style interfaces.
            key: Message key (stored in Event Hub properties)
            headers: Optional message headers (stored in Event Hub properties)

        Returns:
            ProduceResult with transport-agnostic confirmation metadata
        """
        if not self._started or self._producer is None:
            raise RuntimeError("Producer not started. Call start() first.")

        # Default to the producer's configured entity name
        if topic is None:
            topic = self.eventhub_name

        # Validate topic matches entity
        if topic != self.eventhub_name:
            logger.warning(
                f"Topic mismatch: requested '{topic}', producer connected to '{self.eventhub_name}'. "
                f"Event Hub does not support multiple topics. Using entity '{self.eventhub_name}'."
            )

        # Serialize value
        if isinstance(value, bytes):
            value_bytes = value
        elif isinstance(value, BaseModel):
            value_bytes = value.model_dump_json().encode("utf-8")
        else:
            value_bytes = json.dumps(value, default=json_serializer).encode("utf-8")

        # Create EventData with properties
        event_data = EventData(value_bytes)

        # Add key as property if provided
        if key is not None:
            key_str = key.decode("utf-8") if isinstance(key, bytes) else str(key)
            event_data.properties["_key"] = key_str

        # Add headers as properties if provided
        if headers:
            for k, v in headers.items():
                event_data.properties[k] = v

        logger.debug(
            "Sending message to Event Hub",
            extra={
                "entity": self.eventhub_name,
                "key": key,
                "headers": headers,
                "value_size": len(value_bytes),
            },
        )

        try:
            # Create and send batch
            # Note: EventHubProducerClient uses sync methods, not async
            batch = self._producer.create_batch()
            batch.add(event_data)
            self._producer.send_batch(batch)

            record_message_produced(self.eventhub_name, len(value_bytes), success=True)

            logger.debug(
                "Message sent successfully",
                extra={"entity": self.eventhub_name},
            )

            # Return ProduceResult for transport-agnostic interface
            # Event Hub doesn't provide partition/offset info synchronously
            metadata = EventHubRecordMetadata(
                topic=self.eventhub_name,
                partition=0,  # Partition assignment is automatic
                offset=0,  # Offset not available in Event Hub SDK
            )
            return metadata.to_produce_result()

        except Exception as e:
            record_message_produced(self.eventhub_name, len(value_bytes), success=False)
            record_producer_error(self.eventhub_name, type(e).__name__)
            logger.error(
                "Failed to send message",
                extra={"entity": self.eventhub_name, "key": key, "error": str(e)},
                exc_info=True,
            )
            raise

    async def send_batch(
        self,
        messages: list[tuple[str, BaseModel]],
        topic: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> list[ProduceResult]:
        """Send a batch of messages to Event Hub.

        Args:
            messages: List of (key, value) tuples
            topic: Optional Event Hub entity name. If not provided, uses the producer's
                   configured eventhub_name. For compatibility with Kafka-style interfaces.
            headers: Optional headers applied to all messages

        Returns:
            List of ProduceResult with transport-agnostic confirmation metadata
        """
        if not self._started or self._producer is None:
            raise RuntimeError("Producer not started. Call start() first.")

        if not messages:
            logger.warning("send_batch called with empty message list")
            return []

        # Default to the producer's configured entity name
        if topic is None:
            topic = self.eventhub_name

        # Validate topic
        if topic != self.eventhub_name:
            logger.warning(
                f"Topic mismatch: requested '{topic}', using '{self.eventhub_name}'"
            )

        logger.info(
            "Sending batch to Event Hub",
            extra={
                "entity": self.eventhub_name,
                "message_count": len(messages),
                "headers": headers,
            },
        )

        start_time = time.perf_counter()
        total_bytes = 0

        try:
            # Create batch and add all messages
            batch = self._producer.create_batch()

            for key, value in messages:
                value_bytes = value.model_dump_json().encode("utf-8")
                total_bytes += len(value_bytes)

                event_data = EventData(value_bytes)
                event_data.properties["_key"] = key

                if headers:
                    for k, v in headers.items():
                        event_data.properties[k] = v

                batch.add(event_data)

            # Send batch (synchronous operation)
            self._producer.send_batch(batch)

            duration = time.perf_counter() - start_time
            message_processing_duration_seconds.labels(
                topic=self.eventhub_name
            ).observe(duration)

            for _ in messages:
                record_message_produced(
                    self.eventhub_name, total_bytes // len(messages), success=True
                )

            logger.info(
                "Batch sent successfully",
                extra={
                    "entity": self.eventhub_name,
                    "message_count": len(messages),
                    "duration_ms": round(duration * 1000, 2),
                },
            )

            # Return ProduceResult list for transport-agnostic interface
            return [
                EventHubRecordMetadata(
                    topic=self.eventhub_name, partition=0, offset=i
                ).to_produce_result()
                for i in range(len(messages))
            ]

        except Exception as e:
            duration = time.perf_counter() - start_time
            message_processing_duration_seconds.labels(
                topic=self.eventhub_name
            ).observe(duration)

            for _ in messages:
                record_message_produced(
                    self.eventhub_name, total_bytes // len(messages), success=False
                )
            record_producer_error(self.eventhub_name, type(e).__name__)

            logger.error(
                "Failed to send batch",
                extra={
                    "entity": self.eventhub_name,
                    "message_count": len(messages),
                    "duration_ms": round(duration * 1000, 2),
                    "error": str(e),
                },
                exc_info=True,
            )
            raise

    async def flush(self) -> None:
        """Flush pending messages.

        Note: Event Hub SDK sends synchronously, so this is a no-op.
        Included for interface compatibility with MessageProducer.
        """
        if not self._started or self._producer is None:
            raise RuntimeError("Producer not started. Call start() first.")
        logger.debug("Flush called (no-op for Event Hub)")

    @property
    def is_started(self) -> bool:
        return self._started and self._producer is not None


__all__ = [
    "EventHubProducer",
    "EventHubRecordMetadata",
]
