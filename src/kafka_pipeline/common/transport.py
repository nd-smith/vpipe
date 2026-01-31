"""Transport layer abstraction for Kafka and Event Hub.

Provides factory functions to create producer/consumer instances based on
configuration. Supports both aiokafka (Kafka protocol on port 9093) and
azure-eventhub (AMQP over WebSocket on port 443).

Architecture:
- PIPELINE_TRANSPORT env var selects transport: "eventhub" (default) or "kafka"
- Event Hub is the default for Azure Private Link compatibility
- Kafka transport remains available as fallback

Event Hub resolution:
- One namespace connection string provides access to all Event Hubs
- Event Hub names and consumer groups are defined per-topic in config.yaml
  under eventhub.{domain}.{topic_key}.eventhub_name / consumer_group
- The Azure SDK `eventhub_name` parameter is used instead of
  baking EntityPath into the connection string
"""

import logging
import os
from enum import Enum
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Union

from aiokafka.structs import ConsumerRecord

from core.logging import get_logger
from config.config import KafkaConfig
from kafka_pipeline.common.eventhub.checkpoint_store import get_checkpoint_store

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


# =============================================================================
# Event Hub configuration loading (cached)
# =============================================================================

_eventhub_config: Optional[Dict[str, Any]] = None


def _load_eventhub_config() -> Dict[str, Any]:
    """Load and cache the eventhub section from config.yaml.

    Returns the expanded eventhub config dict with Event Hub mappings.
    """
    global _eventhub_config
    if _eventhub_config is not None:
        return _eventhub_config

    from config.config import load_yaml, _expand_env_vars, DEFAULT_CONFIG_FILE

    if DEFAULT_CONFIG_FILE.exists():
        data = load_yaml(DEFAULT_CONFIG_FILE)
        data = _expand_env_vars(data)
        _eventhub_config = data.get("eventhub", {})
    else:
        _eventhub_config = {}

    return _eventhub_config


def _get_namespace_connection_string() -> str:
    """Get Event Hub namespace-level connection string.

    Priority:
    1. EVENTHUB_NAMESPACE_CONNECTION_STRING env var
    2. EVENTHUB_CONNECTION_STRING env var (backward compat, EntityPath stripped)
    3. eventhub.namespace_connection_string from config.yaml

    Returns:
        Namespace connection string with EntityPath removed (if present).

    Raises:
        ValueError: If no connection string is configured.
    """
    # 1. New env var (preferred)
    conn = os.getenv("EVENTHUB_NAMESPACE_CONNECTION_STRING")
    if conn:
        return _strip_entity_path(conn)

    # 2. Legacy env var (backward compat)
    conn = os.getenv("EVENTHUB_CONNECTION_STRING")
    if conn:
        logger.debug(
            "Using legacy EVENTHUB_CONNECTION_STRING. "
            "Consider migrating to EVENTHUB_NAMESPACE_CONNECTION_STRING."
        )
        return _strip_entity_path(conn)

    # 3. Config file
    config = _load_eventhub_config()
    conn = config.get("namespace_connection_string", "")
    if conn:
        return _strip_entity_path(conn)

    raise ValueError(
        "Event Hub namespace connection string is required. "
        "Set EVENTHUB_NAMESPACE_CONNECTION_STRING environment variable."
    )


def _strip_entity_path(connection_string: str) -> str:
    """Remove EntityPath from a connection string if present.

    This normalizes entity-level connection strings to namespace-level
    so the SDK's `eventhub_name` parameter can be used instead.
    """
    parts = [
        part for part in connection_string.split(";")
        if part.strip() and not part.startswith("EntityPath=")
    ]
    return ";".join(parts)


def _resolve_eventhub_name(
    domain: str,
    topic_key: Optional[str],
    worker_name: str,
) -> str:
    """Resolve Event Hub name for a given domain/topic.

    Priority:
    1. config.yaml: eventhub.{domain}.{topic_key}.eventhub_name
    2. Worker-specific env var: EVENTHUB_NAME_{WORKER_NAME}
    3. Default env var: EVENTHUB_ENTITY_NAME (legacy)
    4. Fallback: construct from domain

    Args:
        domain: Pipeline domain (e.g., "verisk", "claimx")
        topic_key: Topic key matching config.yaml (e.g., "events", "downloads_pending")
        worker_name: Worker name for env var lookup

    Returns:
        Event Hub name
    """
    # 1. Config file lookup (preferred)
    if topic_key:
        config = _load_eventhub_config()
        domain_config = config.get(domain, {})
        topic_config = domain_config.get(topic_key, {})
        eventhub_name = topic_config.get("eventhub_name")
        if eventhub_name:
            logger.debug(
                f"Resolved Event Hub from config: "
                f"eventhub.{domain}.{topic_key}.eventhub_name={eventhub_name}"
            )
            return eventhub_name

    # 2. Worker-specific env var
    worker_env_var = f"EVENTHUB_NAME_{worker_name.upper().replace('-', '_')}"
    eventhub_name = os.getenv(worker_env_var)
    if eventhub_name:
        logger.debug(f"Using Event Hub name from {worker_env_var}: {eventhub_name}")
        return eventhub_name

    # 3. Legacy default env var
    eventhub_name = os.getenv("EVENTHUB_ENTITY_NAME")
    if eventhub_name:
        logger.debug(f"Using Event Hub name from EVENTHUB_ENTITY_NAME: {eventhub_name}")
        return eventhub_name

    # 4. Fallback
    eventhub_name = f"com.allstate.pcesdopodappv1.{domain}.events.raw"
    logger.warning(
        f"No Event Hub name configured for domain='{domain}', topic_key='{topic_key}', "
        f"worker='{worker_name}'. Using fallback: {eventhub_name}. "
        f"Configure in config.yaml under eventhub.{domain}.{topic_key}.eventhub_name."
    )
    return eventhub_name


def _resolve_eventhub_consumer_group(
    domain: str,
    topic_key: Optional[str],
    worker_name: str,
    kafka_config: KafkaConfig,
) -> str:
    """Resolve Event Hub consumer group for a given domain/topic.

    Priority:
    1. config.yaml: eventhub.{domain}.{topic_key}.consumer_group
    2. KafkaConfig.get_consumer_group (existing Kafka consumer group logic)
    3. eventhub.default_consumer_group from config.yaml
    4. Fallback: $Default

    Args:
        domain: Pipeline domain
        topic_key: Topic key matching config.yaml
        worker_name: Worker name
        kafka_config: KafkaConfig for fallback consumer group resolution

    Returns:
        Consumer group name
    """
    # 1. Config file lookup (preferred)
    if topic_key:
        config = _load_eventhub_config()
        domain_config = config.get(domain, {})
        topic_config = domain_config.get(topic_key, {})
        consumer_group = topic_config.get("consumer_group")
        if consumer_group:
            logger.debug(
                f"Resolved consumer group from config: "
                f"eventhub.{domain}.{topic_key}.consumer_group={consumer_group}"
            )
            return consumer_group

    # 2. KafkaConfig consumer group (existing logic)
    try:
        return kafka_config.get_consumer_group(domain, worker_name)
    except (ValueError, KeyError):
        pass

    # 3. Default from config
    config = _load_eventhub_config()
    default_group = config.get("default_consumer_group")
    if default_group:
        return default_group

    # 4. Fallback
    return "$Default"


# =============================================================================
# Factory functions
# =============================================================================

def create_producer(
    config: KafkaConfig,
    domain: str,
    worker_name: str,
    transport_type: Optional[TransportType] = None,
    topic: Optional[str] = None,
    topic_key: Optional[str] = None,
):
    """Create a producer instance based on transport configuration.

    Args:
        config: KafkaConfig with connection details
        domain: Pipeline domain (e.g., "verisk", "claimx")
        worker_name: Worker name for logging
        transport_type: Optional override for transport type (defaults to env var)
        topic: Optional explicit Event Hub name (overrides config-based detection)
        topic_key: Optional topic key for Event Hub resolution from config.yaml
                   (e.g., "events", "downloads_pending"). When provided, the
                   Event Hub name is looked up from
                   eventhub.{domain}.{topic_key}.eventhub_name.

    Returns:
        BaseKafkaProducer or EventHubProducer instance

    Note for Event Hub:
        Name resolution priority:
        1. Explicit 'topic' parameter
        2. config.yaml lookup via topic_key
        3. Worker-specific env var: EVENTHUB_NAME_{WORKER_NAME}
        4. Default from EVENTHUB_ENTITY_NAME env var
        5. Fallback: construct from domain
    """
    transport = transport_type or get_transport_type()

    if transport == TransportType.EVENTHUB:
        from kafka_pipeline.common.eventhub.producer import EventHubProducer

        # Get namespace connection string
        namespace_connection_string = _get_namespace_connection_string()

        # Determine Event Hub name
        eventhub_name = topic or _resolve_eventhub_name(domain, topic_key, worker_name)

        logger.info(
            f"Creating Event Hub producer: domain={domain}, worker={worker_name}, "
            f"eventhub={eventhub_name}"
        )

        return EventHubProducer(
            connection_string=namespace_connection_string,
            domain=domain,
            worker_name=worker_name,
            eventhub_name=eventhub_name,
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


async def create_consumer(
    config: KafkaConfig,
    domain: str,
    worker_name: str,
    topics: List[str],
    message_handler: Callable[[ConsumerRecord], Awaitable[None]],
    enable_message_commit: bool = True,
    instance_id: Optional[str] = None,
    transport_type: Optional[TransportType] = None,
    topic_key: Optional[str] = None,
):
    """Create a consumer instance based on transport configuration.

    Args:
        config: KafkaConfig with connection details
        domain: Pipeline domain (e.g., "verisk", "claimx")
        worker_name: Worker name for logging
        topics: List of topics to consume from
        message_handler: Async function to process each message
        enable_message_commit: Whether to commit offsets after processing
        instance_id: Optional instance identifier for parallel consumers
        transport_type: Optional override for transport type (defaults to env var)
        topic_key: Optional topic key for Event Hub / consumer group resolution
                   from config.yaml (e.g., "events", "downloads_pending").

    Returns:
        BaseKafkaConsumer or EventHubConsumer instance
    """
    transport = transport_type or get_transport_type()

    if len(topics) != 1 and transport == TransportType.EVENTHUB:
        raise ValueError(
            f"Event Hub transport only supports consuming from a single topic. "
            f"Got {len(topics)} topics: {topics}"
        )

    if transport == TransportType.EVENTHUB:
        from kafka_pipeline.common.eventhub.consumer import EventHubConsumer

        # Get namespace connection string
        namespace_connection_string = _get_namespace_connection_string()

        # Resolve Event Hub name: use topic_key config lookup, or fall back to topics[0]
        if topic_key:
            eventhub_name = _resolve_eventhub_name(domain, topic_key, worker_name)
        else:
            # Backward compat: use the Kafka topic name as Event Hub name
            eventhub_name = topics[0]

        # Resolve consumer group from config
        consumer_group = _resolve_eventhub_consumer_group(
            domain, topic_key, worker_name, config
        )

        # Get checkpoint store for durable offset persistence
        checkpoint_store = None
        try:
            checkpoint_store = await get_checkpoint_store()
            if checkpoint_store is None:
                logger.info(
                    f"Event Hub consumer will use in-memory checkpointing: "
                    f"domain={domain}, worker={worker_name}, eventhub={eventhub_name}. "
                    f"Configure checkpoint store in config.yaml for durable offset persistence."
                )
            else:
                logger.info(
                    f"Event Hub consumer initialized with blob checkpoint store: "
                    f"domain={domain}, worker={worker_name}, eventhub={eventhub_name}"
                )
        except Exception as e:
            logger.error(
                f"Failed to initialize checkpoint store for Event Hub consumer: "
                f"domain={domain}, worker={worker_name}, eventhub={eventhub_name}. "
                f"Falling back to in-memory checkpointing. Error: {e}"
            )
            # Continue with None checkpoint_store (in-memory mode)

        logger.info(
            f"Creating Event Hub consumer: domain={domain}, worker={worker_name}, "
            f"eventhub={eventhub_name}, group={consumer_group}"
        )

        return EventHubConsumer(
            connection_string=namespace_connection_string,
            domain=domain,
            worker_name=worker_name,
            eventhub_name=eventhub_name,
            consumer_group=consumer_group,
            message_handler=message_handler,
            enable_message_commit=enable_message_commit,
            instance_id=instance_id,
            checkpoint_store=checkpoint_store,
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


def reset_eventhub_config() -> None:
    """Reset the cached eventhub config (for testing)."""
    global _eventhub_config
    _eventhub_config = None


__all__ = [
    "TransportType",
    "get_transport_type",
    "create_producer",
    "create_consumer",
    "reset_eventhub_config",
]
