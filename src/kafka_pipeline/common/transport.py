"""Transport layer abstraction for Kafka and Event Hub.

Provides factory functions to create producer/consumer instances based on
configuration. Supports both aiokafka (Kafka protocol on port 9093) and
azure-eventhub (AMQP over WebSocket on port 443).

Architecture:
- PIPELINE_TRANSPORT env var selects transport: "eventhub" (default) or "kafka"
- Event Hub is the default for Azure Private Link compatibility
- Kafka transport remains available as fallback
"""

import logging
import os
from enum import Enum
from typing import Awaitable, Callable, List, Optional, Union

from aiokafka.structs import ConsumerRecord

from core.logging import get_logger
from config.config import KafkaConfig

logger = get_logger(__name__)


class TransportType(str, Enum):
    """Transport protocol type."""
    EVENTHUB = "eventhub"
    KAFKA = "kafka"


def get_transport_type() -> TransportType:
    """Get configured transport type from environment.

    Returns:
        TransportType.EVENTHUB (default) or TransportType.KAFKA

    The default is Event Hub because corporate Azure Private Link endpoints
    expose AMQP (port 443) but NOT Kafka protocol (port 9093).
    """
    transport_str = os.getenv("PIPELINE_TRANSPORT", "eventhub").lower()
    try:
        return TransportType(transport_str)
    except ValueError:
        logger.warning(
            f"Invalid PIPELINE_TRANSPORT value '{transport_str}'. "
            f"Must be 'eventhub' or 'kafka'. Defaulting to 'eventhub'."
        )
        return TransportType.EVENTHUB


def create_producer(
    config: KafkaConfig,
    domain: str,
    worker_name: str,
    transport_type: Optional[TransportType] = None,
    topic: Optional[str] = None,
):
    """Create a producer instance based on transport configuration.

    Args:
        config: KafkaConfig with connection details
        domain: Pipeline domain (e.g., "xact", "claimx")
        worker_name: Worker name for logging
        transport_type: Optional override for transport type (defaults to env var)
        topic: Optional explicit topic/entity name (overrides config-based detection)

    Returns:
        BaseKafkaProducer or EventHubProducer instance

    Note for Event Hub:
        Since Event Hub only supports one entity per connection, the entity name
        must be specified. This can be done via:
        1. Explicit 'topic' parameter
        2. Worker-specific env var: EVENTHUB_ENTITY_<WORKER_NAME>
        3. Default entity from EVENTHUB_ENTITY_NAME env var
        4. EntityPath in EVENTHUB_CONNECTION_STRING
    """
    transport = transport_type or get_transport_type()

    if transport == TransportType.EVENTHUB:
        from kafka_pipeline.common.eventhub.producer import EventHubProducer

        # Get base Event Hub connection string
        base_connection_string = os.getenv("EVENTHUB_CONNECTION_STRING")
        if not base_connection_string:
            raise ValueError(
                "EVENTHUB_CONNECTION_STRING environment variable is required "
                "when PIPELINE_TRANSPORT=eventhub"
            )

        # Determine entity name (topic equivalent)
        entity_name = topic or _get_entity_name(domain, worker_name, base_connection_string)

        # Build connection string with correct EntityPath
        connection_string = _build_eventhub_connection_string(
            base_connection_string, entity_name
        )

        logger.info(
            f"Creating Event Hub producer: domain={domain}, worker={worker_name}, "
            f"entity={entity_name}"
        )

        return EventHubProducer(
            connection_string=connection_string,
            domain=domain,
            worker_name=worker_name,
            entity_name=entity_name,
        )

    else:  # TransportType.KAFKA
        from kafka_pipeline.common.producer import BaseKafkaProducer

        logger.info(
            f"Creating Kafka producer: domain={domain}, worker={worker_name}, "
            f"servers={config.bootstrap_servers}"
        )

        return BaseKafkaProducer(
            config=config,
            domain=domain,
            worker_name=worker_name,
        )


def create_consumer(
    config: KafkaConfig,
    domain: str,
    worker_name: str,
    topics: List[str],
    message_handler: Callable[[ConsumerRecord], Awaitable[None]],
    enable_message_commit: bool = True,
    instance_id: Optional[str] = None,
    transport_type: Optional[TransportType] = None,
):
    """Create a consumer instance based on transport configuration.

    Args:
        config: KafkaConfig with connection details
        domain: Pipeline domain (e.g., "xact", "claimx")
        worker_name: Worker name for logging
        topics: List of topics to consume from
        message_handler: Async function to process each message
        enable_message_commit: Whether to commit offsets after processing
        instance_id: Optional instance identifier for parallel consumers
        transport_type: Optional override for transport type (defaults to env var)

    Returns:
        BaseKafkaConsumer or EventHubConsumer instance
    """
    transport = transport_type or get_transport_type()

    if len(topics) != 1 and transport == TransportType.EVENTHUB:
        raise ValueError(
            f"Event Hub transport only supports consuming from a single topic/entity. "
            f"Got {len(topics)} topics: {topics}"
        )

    if transport == TransportType.EVENTHUB:
        from kafka_pipeline.common.eventhub.consumer import EventHubConsumer

        # Get base Event Hub connection string
        base_connection_string = os.getenv("EVENTHUB_CONNECTION_STRING")
        if not base_connection_string:
            raise ValueError(
                "EVENTHUB_CONNECTION_STRING environment variable is required "
                "when PIPELINE_TRANSPORT=eventhub"
            )

        # Use first topic as entity name (Event Hub only supports one entity per consumer)
        # The topic name is the actual Kafka topic we're consuming from
        entity_name = topics[0]

        # Build connection string with correct EntityPath
        connection_string = _build_eventhub_connection_string(
            base_connection_string, entity_name
        )

        consumer_group = config.get_consumer_group(domain, worker_name)

        logger.info(
            f"Creating Event Hub consumer: domain={domain}, worker={worker_name}, "
            f"entity={entity_name}, group={consumer_group}"
        )

        return EventHubConsumer(
            connection_string=connection_string,
            domain=domain,
            worker_name=worker_name,
            entity_name=entity_name,
            consumer_group=consumer_group,
            message_handler=message_handler,
            enable_message_commit=enable_message_commit,
            instance_id=instance_id,
        )

    else:  # TransportType.KAFKA
        from kafka_pipeline.common.consumer import BaseKafkaConsumer

        logger.info(
            f"Creating Kafka consumer: domain={domain}, worker={worker_name}, "
            f"topics={topics}, servers={config.bootstrap_servers}"
        )

        return BaseKafkaConsumer(
            config=config,
            domain=domain,
            worker_name=worker_name,
            topics=topics,
            message_handler=message_handler,
            enable_message_commit=enable_message_commit,
            instance_id=instance_id,
        )


def _get_entity_name(domain: str, worker_name: str, connection_string: str) -> str:
    """Determine Event Hub entity name for a worker.

    Priority order:
    1. Worker-specific env var: EVENTHUB_ENTITY_{WORKER_NAME}
    2. Default entity env var: EVENTHUB_ENTITY_NAME
    3. EntityPath in connection string
    4. Fallback: construct from domain

    Args:
        domain: Pipeline domain
        worker_name: Worker name
        connection_string: Event Hub connection string

    Returns:
        Entity name (Event Hub name)
    """
    # 1. Check worker-specific env var
    worker_env_var = f"EVENTHUB_ENTITY_{worker_name.upper().replace('-', '_')}"
    entity_name = os.getenv(worker_env_var)
    if entity_name:
        logger.debug(f"Using entity name from {worker_env_var}: {entity_name}")
        return entity_name

    # 2. Check default entity env var
    entity_name = os.getenv("EVENTHUB_ENTITY_NAME")
    if entity_name:
        logger.debug(f"Using entity name from EVENTHUB_ENTITY_NAME: {entity_name}")
        return entity_name

    # 3. Try to extract from connection string
    for part in connection_string.split(";"):
        if part.startswith("EntityPath="):
            entity_name = part.split("=", 1)[1]
            logger.debug(f"Using entity name from connection string: {entity_name}")
            return entity_name

    # 4. Fallback: construct from domain
    entity_name = f"com.allstate.pcesdopodappv1.{domain}.events.raw"
    logger.warning(
        f"No entity name configured for worker '{worker_name}'. "
        f"Using fallback: {entity_name}. "
        f"Set {worker_env_var} or EVENTHUB_ENTITY_NAME to configure."
    )
    return entity_name


def _build_eventhub_connection_string(base_connection_string: str, entity_name: str) -> str:
    """Build Event Hub connection string with correct EntityPath.

    Args:
        base_connection_string: Base connection string (may or may not have EntityPath)
        entity_name: Entity name to use

    Returns:
        Connection string with EntityPath set to entity_name
    """
    # Parse connection string and replace/add EntityPath
    parts = []
    has_entity_path = False

    for part in base_connection_string.split(";"):
        if part.startswith("EntityPath="):
            parts.append(f"EntityPath={entity_name}")
            has_entity_path = True
        elif part.strip():  # Skip empty parts
            parts.append(part)

    # Add EntityPath if not present
    if not has_entity_path:
        parts.append(f"EntityPath={entity_name}")

    return ";".join(parts)


__all__ = [
    "TransportType",
    "get_transport_type",
    "create_producer",
    "create_consumer",
]
