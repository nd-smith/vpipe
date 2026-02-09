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
from collections.abc import Awaitable, Callable
from enum import Enum
from typing import Any

from aiokafka.structs import ConsumerRecord

from config.config import MessageConfig
from pipeline.common.eventhub.checkpoint_store import get_checkpoint_store

logger = logging.getLogger(__name__)


# Event Hub configuration keys
EVENTHUB_CONFIG_KEY = "eventhub"
NAMESPACE_CONNECTION_STRING_KEY = "namespace_connection_string"
EVENTHUB_NAME_KEY = "eventhub_name"
CONSUMER_GROUP_KEY = "consumer_group"
DEFAULT_CONSUMER_GROUP_KEY = "default_consumer_group"


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
# Event Hub configuration loading
# =============================================================================


def _load_eventhub_config() -> dict[str, Any]:
    """Load the eventhub section from config.yaml.

    Returns the expanded eventhub config dict with Event Hub mappings.
    """
    from config.config import DEFAULT_CONFIG_FILE, _expand_env_vars, load_yaml

    if not DEFAULT_CONFIG_FILE.exists():
        return {}

    data = load_yaml(DEFAULT_CONFIG_FILE)
    data = _expand_env_vars(data)
    return data.get(EVENTHUB_CONFIG_KEY, {})


def _get_namespace_connection_string() -> str:
    """Get Event Hub namespace-level connection string.

    Priority:
    1. EVENTHUB_NAMESPACE_CONNECTION_STRING env var
    2. eventhub.namespace_connection_string from config.yaml

    Returns:
        Namespace connection string with EntityPath removed (if present).

    Raises:
        ValueError: If no connection string is configured or if it's empty/invalid.
    """
    # 1. New env var (preferred)
    conn = os.getenv("EVENTHUB_NAMESPACE_CONNECTION_STRING")
    if conn:
        stripped = _strip_entity_path(conn)
        if not stripped or not stripped.strip():
            raise ValueError(
                "EVENTHUB_NAMESPACE_CONNECTION_STRING is set but empty or contains only whitespace. "
                "Ensure the environment variable contains a valid Event Hub connection string."
            )
        return stripped

    # 2. Config file
    config = _load_eventhub_config()
    conn = config.get(NAMESPACE_CONNECTION_STRING_KEY, "")
    if conn:
        stripped = _strip_entity_path(conn)
        if not stripped or not stripped.strip():
            raise ValueError(
                "eventhub.namespace_connection_string in config.yaml is set but empty or contains only whitespace. "
                "Ensure the configuration contains a valid Event Hub connection string."
            )
        return stripped

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
        part
        for part in connection_string.split(";")
        if part.strip() and not part.startswith("EntityPath=")
    ]
    return ";".join(parts)


def _resolve_eventhub_name(
    domain: str,
    topic_key: str | None,
    worker_name: str,
) -> str:
    """Resolve Event Hub name for a given domain/topic.

    Priority:
    1. config.yaml: eventhub.{domain}.{topic_key}.eventhub_name
    2. Worker-specific env var: EVENTHUB_NAME_{WORKER_NAME}

    Args:
        domain: Pipeline domain (e.g., "verisk", "claimx")
        topic_key: Topic key matching config.yaml (e.g., "events", "downloads_pending")
        worker_name: Worker name for env var lookup

    Returns:
        Event Hub name

    Raises:
        ValueError: If Event Hub name cannot be resolved
    """
    # 1. Config file lookup (preferred)
    if topic_key:
        config = _load_eventhub_config()
        domain_config = config.get(domain, {})
        topic_config = domain_config.get(topic_key, {})
        eventhub_name = topic_config.get(EVENTHUB_NAME_KEY)
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
        logger.debug("Using Event Hub name from %s: %s", worker_env_var, eventhub_name)
        return eventhub_name

    raise ValueError(
        f"Event Hub name not configured for domain='{domain}', topic_key='{topic_key}', "
        f"worker='{worker_name}'. Configure in config.yaml under "
        f"eventhub.{domain}.{topic_key}.eventhub_name or set {worker_env_var} environment variable."
    )


def _resolve_eventhub_consumer_group(
    domain: str,
    topic_key: str | None,
    worker_name: str,
    message_config: MessageConfig,
) -> str:
    """Resolve Event Hub consumer group for a given domain/topic.

    Priority:
    1. config.yaml: eventhub.{domain}.{topic_key}.consumer_group
    2. MessageConfig.get_consumer_group (existing consumer group logic)
    3. eventhub.default_consumer_group from config.yaml

    Args:
        domain: Pipeline domain
        topic_key: Topic key matching config.yaml
        worker_name: Worker name
        message_config: MessageConfig for fallback consumer group resolution

    Returns:
        Consumer group name

    Raises:
        ValueError: If consumer group cannot be resolved
    """
    # 1. Config file lookup (preferred)
    if topic_key:
        config = _load_eventhub_config()
        domain_config = config.get(domain, {})
        topic_config = domain_config.get(topic_key, {})
        consumer_group = topic_config.get(CONSUMER_GROUP_KEY)
        if consumer_group:
            logger.debug(
                f"Resolved consumer group from config: "
                f"eventhub.{domain}.{topic_key}.consumer_group={consumer_group}"
            )
            return consumer_group

    # 2. MessageConfig consumer group (existing logic)
    try:
        return message_config.get_consumer_group(domain, worker_name)
    except (ValueError, KeyError):
        pass

    # 3. Default from config
    config = _load_eventhub_config()
    default_group = config.get(DEFAULT_CONSUMER_GROUP_KEY)
    if default_group:
        return default_group

    raise ValueError(
        f"Event Hub consumer group not configured for domain='{domain}', "
        f"topic_key='{topic_key}', worker='{worker_name}'. Configure in config.yaml under "
        f"eventhub.{domain}.{topic_key}.consumer_group or eventhub.default_consumer_group."
    )


# =============================================================================
# Factory functions
# =============================================================================


def create_producer(
    config: MessageConfig,
    domain: str,
    worker_name: str,
    transport_type: TransportType | None = None,
    topic: str | None = None,
    topic_key: str | None = None,
):
    """Create a producer instance based on transport configuration.

    Args:
        config: MessageConfig with connection details
        domain: Pipeline domain (e.g., "verisk", "claimx")
        worker_name: Worker name for logging
        transport_type: Optional override for transport type (defaults to env var)
        topic: Optional explicit Event Hub name (overrides config-based detection)
        topic_key: Optional topic key for Event Hub resolution from config.yaml
                   (e.g., "events", "downloads_pending"). When provided, the
                   Event Hub name is looked up from
                   eventhub.{domain}.{topic_key}.eventhub_name.

    Returns:
        MessageProducer or EventHubProducer instance

    Note for Event Hub:
        Name resolution priority:
        1. Explicit 'topic' parameter
        2. config.yaml lookup via topic_key
        3. Worker-specific env var: EVENTHUB_NAME_{WORKER_NAME}
        4. Default from EVENTHUB_ENTITY_NAME env var
        5. Fallback: construct from domain
    """
    transport = transport_type or get_transport_type()

    logger.debug(
        f"Creating producer: transport={transport.value}, domain={domain}, worker={worker_name}"
    )

    if transport == TransportType.EVENTHUB:
        from pipeline.common.eventhub.producer import EventHubProducer

        # Get namespace connection string
        logger.debug(
            "Attempting to load Event Hub namespace connection string",
            extra={
                "EVENTHUB_NAMESPACE_CONNECTION_STRING": "***SET***" if os.getenv("EVENTHUB_NAMESPACE_CONNECTION_STRING") else "NOT SET",
                "EVENTHUB_CONNECTION_STRING": "***SET***" if os.getenv("EVENTHUB_CONNECTION_STRING") else "NOT SET",
            }
        )
        namespace_connection_string = _get_namespace_connection_string()

        # Determine Event Hub name
        # When topic_key is provided, always resolve from config.yaml
        # (topic may be a Kafka topic name which differs from the Event Hub entity name)
        if topic_key:
            eventhub_name = _resolve_eventhub_name(domain, topic_key, worker_name)
        else:
            eventhub_name = topic or _resolve_eventhub_name(
                domain, topic_key, worker_name
            )

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
        from pipeline.common.producer import MessageProducer

        logger.info(
            f"Creating message producer (Kafka protocol): domain={domain}, worker={worker_name}, "
            f"servers={config.bootstrap_servers}"
        )

        return MessageProducer(
            config=config,
            domain=domain,
            worker_name=worker_name,
        )


async def create_consumer(
    config: MessageConfig,
    domain: str,
    worker_name: str,
    topics: list[str],
    message_handler: Callable[[ConsumerRecord], Awaitable[None]],
    enable_message_commit: bool = True,
    instance_id: str | None = None,
    transport_type: TransportType | None = None,
    topic_key: str | None = None,
):
    """Create a consumer instance based on transport configuration.

    Args:
        config: MessageConfig with connection details
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
        MessageConsumer or EventHubConsumer instance
    """
    transport = transport_type or get_transport_type()

    if len(topics) != 1 and transport == TransportType.EVENTHUB:
        raise ValueError(
            f"Event Hub transport only supports consuming from a single topic. "
            f"Got {len(topics)} topics: {topics}"
        )

    if transport == TransportType.EVENTHUB:
        from pipeline.common.eventhub.consumer import EventHubConsumer

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
        from pipeline.common.consumer import MessageConsumer

        logger.info(
            f"Creating message consumer (Kafka protocol): domain={domain}, worker={worker_name}, "
            f"topics={topics}, servers={config.bootstrap_servers}"
        )

        return MessageConsumer(
            config=config,
            domain=domain,
            worker_name=worker_name,
            topics=topics,
            message_handler=message_handler,
            enable_message_commit=enable_message_commit,
            instance_id=instance_id,
        )


async def create_batch_consumer(
    config: MessageConfig,
    domain: str,
    worker_name: str,
    topics: list[str],
    batch_handler: Callable[[list[ConsumerRecord]], Awaitable[bool]],
    batch_size: int = 20,
    batch_timeout_ms: int = 1000,
    enable_message_commit: bool = True,
    instance_id: str | None = None,
    transport_type: TransportType | None = None,
    topic_key: str | None = None,
):
    """Create a batch consumer for concurrent message processing.

    Args:
        config: MessageConfig with connection details
        domain: Pipeline domain (e.g., "verisk", "claimx")
        worker_name: Worker name for logging and metrics
        topics: List of topics to consume (EventHub requires single topic)
        batch_handler: Async function that processes message batches
        batch_size: Target batch size (default: 20)
        batch_timeout_ms: Max wait time to accumulate batch (default: 1000ms)
        enable_message_commit: Whether to commit after successful batch processing
        instance_id: Optional instance identifier for parallel consumers
        transport_type: Optional transport override (defaults to PIPELINE_TRANSPORT env)
        topic_key: Optional topic key for EventHub resolution from config.yaml

    Returns:
        EventHubBatchConsumer or MessageBatchConsumer based on transport

    Batch Handler Contract:
        The batch_handler receives a list of PipelineMessage objects and must:
        - Return True to commit/checkpoint the batch
        - Return False to skip commit (messages will be redelivered)
        - Raise an exception to skip commit (messages will be redelivered)

        The handler is responsible for concurrent processing (e.g., using
        asyncio.gather with a semaphore to control concurrency).

    Example:
        async def process_batch(messages: list[PipelineMessage]) -> bool:
            # Process concurrently with semaphore
            async def bounded_process(msg):
                async with semaphore:
                    return await process_message(msg)

            results = await asyncio.gather(*[bounded_process(m) for m in messages])

            # Check for transient errors (circuit breaker)
            if any(isinstance(r, CircuitOpenError) for r in results):
                return False  # Don't commit - retry batch

            return True  # Commit batch
    """
    transport = transport_type or get_transport_type()

    if len(topics) != 1 and transport == TransportType.EVENTHUB:
        raise ValueError(
            f"Event Hub transport only supports consuming from a single topic. "
            f"Got {len(topics)} topics: {topics}"
        )

    if transport == TransportType.EVENTHUB:
        from pipeline.common.eventhub.batch_consumer import EventHubBatchConsumer

        # Get namespace connection string
        namespace_connection_string = _get_namespace_connection_string()

        # Resolve Event Hub name
        if topic_key:
            eventhub_name = _resolve_eventhub_name(domain, topic_key, worker_name)
        else:
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
                    f"Event Hub batch consumer will use in-memory checkpointing: "
                    f"domain={domain}, worker={worker_name}, eventhub={eventhub_name}. "
                    f"Configure checkpoint store in config.yaml for durable offset persistence."
                )
            else:
                logger.info(
                    f"Event Hub batch consumer initialized with blob checkpoint store: "
                    f"domain={domain}, worker={worker_name}, eventhub={eventhub_name}"
                )
        except Exception as e:
            logger.error(
                f"Failed to initialize checkpoint store for Event Hub batch consumer: "
                f"domain={domain}, worker={worker_name}, eventhub={eventhub_name}. "
                f"Falling back to in-memory checkpointing. Error: {e}"
            )
            # Continue with None checkpoint_store (in-memory mode)

        logger.info(
            f"Creating Event Hub batch consumer: domain={domain}, worker={worker_name}, "
            f"eventhub={eventhub_name}, group={consumer_group}, "
            f"batch_size={batch_size}, timeout_ms={batch_timeout_ms}"
        )

        return EventHubBatchConsumer(
            connection_string=namespace_connection_string,
            domain=domain,
            worker_name=worker_name,
            eventhub_name=eventhub_name,
            consumer_group=consumer_group,
            batch_handler=batch_handler,
            batch_size=batch_size,
            batch_timeout_ms=batch_timeout_ms,
            enable_message_commit=enable_message_commit,
            instance_id=instance_id,
            checkpoint_store=checkpoint_store,
        )

    else:  # TransportType.KAFKA
        from pipeline.common.batch_consumer import MessageBatchConsumer

        logger.info(
            f"Creating message batch consumer (Kafka protocol): domain={domain}, worker={worker_name}, "
            f"topics={topics}, servers={config.bootstrap_servers}, "
            f"batch_size={batch_size}, timeout_ms={batch_timeout_ms}"
        )

        return MessageBatchConsumer(
            config=config,
            domain=domain,
            worker_name=worker_name,
            topics=topics,
            batch_handler=batch_handler,
            batch_size=batch_size,
            batch_timeout_ms=batch_timeout_ms,
            enable_message_commit=enable_message_commit,
            instance_id=instance_id,
        )


__all__ = [
    "TransportType",
    "get_transport_type",
    "create_producer",
    "create_consumer",
    "create_batch_consumer",
]
