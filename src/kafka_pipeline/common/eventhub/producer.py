"""Azure Event Hub producer adapter.

Implements the same interface as BaseKafkaProducer but uses azure-eventhub SDK
with AMQP over WebSocket transport for compatibility with Azure Private Link.

Architecture notes:
- Event Hub uses AMQP protocol (port 5671 or WebSocket on 443)
- Connection string format: Endpoint=sb://<namespace>.servicebus.windows.net/;...;EntityPath=<entity>
- EntityPath in connection string is the Event Hub name (maps to Kafka topic)
- Each Event Hub is a single entity (no topic multiplexing like Kafka)
"""

import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple, Union

from azure.eventhub import EventData, EventHubProducerClient, TransportType
from aiokafka.structs import RecordMetadata
from pydantic import BaseModel

from core.logging import get_logger, log_with_context, log_exception
from core.utils.json_serializers import json_serializer
from kafka_pipeline.common.metrics import (
    record_message_produced,
    record_producer_error,
    update_connection_status,
    message_processing_duration_seconds,
)

logger = get_logger(__name__)


class EventHubRecordMetadata:
    """Mimics aiokafka RecordMetadata for compatibility."""

    def __init__(self, topic: str, partition: int = 0, offset: int = 0):
        self.topic = topic
        self.partition = partition
        self.offset = offset


class EventHubProducer:
    """Event Hub producer with BaseKafkaProducer-compatible interface.

    Uses azure-eventhub SDK with TransportType.AmqpOverWebsocket for
    compatibility with Azure Private Link endpoints.

    Note: Event Hub does not support multiple topics per connection.
    The entity name (topic) is embedded in the connection string.
    """

    def __init__(
        self,
        connection_string: str,
        domain: str,
        worker_name: str,
        entity_name: Optional[str] = None,
    ):
        """Initialize Event Hub producer.

        Args:
            connection_string: Event Hub connection string (includes EntityPath)
            domain: Pipeline domain (e.g., "xact", "claimx")
            worker_name: Worker name for logging
            entity_name: Optional override for entity name (if not in connection string)
        """
        self.connection_string = connection_string
        self.domain = domain
        self.worker_name = worker_name
        self._producer: Optional[EventHubProducerClient] = None
        self._started = False

        # Extract entity name from connection string or use override
        self.entity_name = entity_name or self._extract_entity_name(connection_string)

        log_with_context(
            logger,
            logging.INFO,
            "Initialized Event Hub producer",
            domain=domain,
            worker_name=worker_name,
            entity_name=self.entity_name,
            transport="AmqpOverWebsocket",
        )

    def _extract_entity_name(self, connection_string: str) -> str:
        """Extract EntityPath from connection string."""
        for part in connection_string.split(";"):
            if part.startswith("EntityPath="):
                return part.split("=", 1)[1]
        raise ValueError("EntityPath not found in Event Hub connection string")

    async def start(self) -> None:
        if self._started:
            logger.warning("Producer already started, ignoring duplicate start call")
            return

        logger.info("Starting Event Hub producer")

        try:
            # Apply SSL dev bypass if configured
            # This must be done before creating the client
            from core.security.ssl_dev_bypass import apply_ssl_dev_bypass
            apply_ssl_dev_bypass()

            # Create producer with AMQP over WebSocket transport
            # This is required for Azure Private Link endpoints
            self._producer = EventHubProducerClient.from_connection_string(
                conn_str=self.connection_string,
                transport_type=TransportType.AmqpOverWebsocket,
            )

            # Test connection by getting properties
            props = self._producer.get_eventhub_properties()
            logger.info(
                f"Connected to Event Hub: {props.get('name', 'unknown')}, "
                f"partitions: {len(props.get('partition_ids', []))}"
            )

            self._started = True
            update_connection_status("producer", connected=True)

            log_with_context(
                logger,
                logging.INFO,
                "Event Hub producer started successfully",
                entity_name=self.entity_name,
                transport="AmqpOverWebsocket",
            )

        except Exception as e:
            log_exception(logger, e, "Failed to start Event Hub producer")
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
            log_exception(logger, e, "Error stopping Event Hub producer")
        finally:
            update_connection_status("producer", connected=False)
            self._producer = None
            self._started = False

    async def send(
        self,
        topic: str,
        key: Optional[Union[str, bytes]],
        value: Union[BaseModel, Dict[str, Any], bytes],
        headers: Optional[Dict[str, str]] = None,
    ) -> RecordMetadata:
        """Send a single message to Event Hub.

        Args:
            topic: Event Hub entity name (must match self.entity_name)
            key: Message key (stored in Event Hub properties)
            value: Message value (Pydantic model, dict, or bytes)
            headers: Optional message headers (stored in Event Hub properties)

        Returns:
            EventHubRecordMetadata for compatibility with Kafka interface
        """
        if not self._started or self._producer is None:
            raise RuntimeError("Producer not started. Call start() first.")

        # Validate topic matches entity
        if topic != self.entity_name:
            logger.warning(
                f"Topic mismatch: requested '{topic}', producer connected to '{self.entity_name}'. "
                f"Event Hub does not support multiple topics. Using entity '{self.entity_name}'."
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

        log_with_context(
            logger,
            logging.DEBUG,
            "Sending message to Event Hub",
            entity=self.entity_name,
            key=key,
            headers=headers,
            value_size=len(value_bytes),
        )

        try:
            # Create and send batch
            # Note: EventHubProducerClient uses sync methods, not async
            batch = self._producer.create_batch()
            batch.add(event_data)
            self._producer.send_batch(batch)

            record_message_produced(self.entity_name, len(value_bytes), success=True)

            log_with_context(
                logger,
                logging.DEBUG,
                "Message sent successfully",
                entity=self.entity_name,
            )

            # Return metadata for compatibility
            # Event Hub doesn't provide partition/offset info synchronously
            return EventHubRecordMetadata(
                topic=self.entity_name,
                partition=0,  # Partition assignment is automatic
                offset=0,  # Offset not available in Event Hub SDK
            )

        except Exception as e:
            record_message_produced(self.entity_name, len(value_bytes), success=False)
            record_producer_error(self.entity_name, type(e).__name__)
            log_exception(logger, e, "Failed to send message", entity=self.entity_name, key=key)
            raise

    async def send_batch(
        self,
        topic: str,
        messages: List[Tuple[str, BaseModel]],
        headers: Optional[Dict[str, str]] = None,
    ) -> List[RecordMetadata]:
        """Send a batch of messages to Event Hub.

        Args:
            topic: Event Hub entity name (must match self.entity_name)
            messages: List of (key, value) tuples
            headers: Optional headers applied to all messages

        Returns:
            List of EventHubRecordMetadata for compatibility
        """
        if not self._started or self._producer is None:
            raise RuntimeError("Producer not started. Call start() first.")

        if not messages:
            logger.warning("send_batch called with empty message list")
            return []

        # Validate topic
        if topic != self.entity_name:
            logger.warning(
                f"Topic mismatch: requested '{topic}', using '{self.entity_name}'"
            )

        log_with_context(
            logger,
            logging.INFO,
            "Sending batch to Event Hub",
            entity=self.entity_name,
            message_count=len(messages),
            headers=headers,
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
            message_processing_duration_seconds.labels(topic=self.entity_name).observe(duration)

            for _ in messages:
                record_message_produced(self.entity_name, total_bytes // len(messages), success=True)

            log_with_context(
                logger,
                logging.INFO,
                "Batch sent successfully",
                entity=self.entity_name,
                message_count=len(messages),
                duration_ms=round(duration * 1000, 2),
            )

            # Return metadata list for compatibility
            return [
                EventHubRecordMetadata(topic=self.entity_name, partition=0, offset=i)
                for i in range(len(messages))
            ]

        except Exception as e:
            duration = time.perf_counter() - start_time
            message_processing_duration_seconds.labels(topic=self.entity_name).observe(duration)

            for _ in messages:
                record_message_produced(self.entity_name, total_bytes // len(messages), success=False)
            record_producer_error(self.entity_name, type(e).__name__)

            log_exception(
                logger,
                e,
                "Failed to send batch",
                entity=self.entity_name,
                message_count=len(messages),
                duration_ms=round(duration * 1000, 2),
            )
            raise

    async def flush(self) -> None:
        """Flush pending messages.

        Note: Event Hub SDK sends synchronously, so this is a no-op.
        Included for interface compatibility with BaseKafkaProducer.
        """
        if not self._started or self._producer is None:
            raise RuntimeError("Producer not started. Call start() first.")
        logger.debug("Flush called (no-op for Event Hub)")

    @property
    def is_started(self):
        return self._started and self._producer is not None


__all__ = [
    "EventHubProducer",
    "EventHubRecordMetadata",
]
