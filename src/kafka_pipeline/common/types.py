"""Transport-agnostic message types for Kafka and Event Hub.

This module defines common message types that decouple the pipeline from
transport-specific implementations (aiokafka and azure-eventhub). These types
enable a clean abstraction layer that allows the codebase to work seamlessly
with either Kafka or Event Hub without coupling to vendor-specific types.

Why These Types Exist
---------------------
The pipeline currently uses aiokafka types (ConsumerRecord, RecordMetadata,
TopicPartition) throughout the codebase, even when using EventHub transport.
This creates several problems:

1. **Tight Coupling**: Direct dependency on aiokafka types in business logic
2. **Type Mismatch**: EventHub adapters must convert to/from aiokafka types
3. **Maintenance Burden**: Changes to transport layer affect entire codebase
4. **Testing Complexity**: Hard to test without importing aiokafka

Transport Abstraction Strategy
------------------------------
These types form the foundation of a transport-agnostic architecture:

- **PipelineMessage**: Replaces aiokafka.structs.ConsumerRecord
  - Used by message handlers and business logic
  - Adapters convert from transport-specific types to PipelineMessage

- **ProduceResult**: Replaces aiokafka.structs.RecordMetadata
  - Returned by producer.send() operations
  - Contains confirmation metadata (topic, partition, offset)

- **PartitionInfo**: Replaces aiokafka.structs.TopicPartition
  - Used for partition management and offset tracking
  - Simple topic + partition identifier

Migration Strategy
------------------
The migration follows a gradual, backwards-compatible approach:

Phase 1: Foundation (Current)
    - Define transport-agnostic types in this module
    - Add conversion helpers for aiokafka and EventHub types
    - Update EventHub adapters to use new types

Phase 2: Incremental Adoption
    - Update worker message handlers to accept PipelineMessage
    - Update consumer/producer interfaces to use new types
    - Maintain compatibility with existing code during transition

Phase 3: Complete Migration
    - Replace all aiokafka type references with transport-agnostic types
    - Remove aiokafka type dependencies from business logic
    - Only transport adapters interact with vendor-specific types

Benefits
--------
- **Clean Separation**: Business logic independent of transport details
- **Type Safety**: Clear, explicit types for pipeline operations
- **Testability**: Easy to create test fixtures without transport dependencies
- **Flexibility**: Swap transports without changing business logic
- **Documentation**: Self-documenting message structure

Example Usage
-------------
```python
# Worker message handler
async def handle_message(message: PipelineMessage) -> None:
    # Business logic uses transport-agnostic type
    payload = json.loads(message.value)
    process_data(payload)

# Producer
result: ProduceResult = await producer.send(
    topic="events",
    value=data,
    key=entity_id
)
print(f"Published to partition {result.partition} at offset {result.offset}")

# Partition management
partition = PartitionInfo(topic="events", partition=0)
await consumer.seek(partition, offset=12345)
```
"""

from dataclasses import dataclass

__all__ = [
    "PipelineMessage",
    "ProduceResult",
    "PartitionInfo",
    "from_consumer_record",
]


@dataclass(frozen=True)
class PipelineMessage:
    """Transport-agnostic message received from Kafka or Event Hub.

    Replaces aiokafka.structs.ConsumerRecord with a transport-agnostic
    representation that works for both Kafka and Event Hub messages.

    All fields are immutable (frozen=True) to prevent accidental modification
    and to ensure thread-safety when messages are passed between async tasks.

    Attributes:
        topic: Name of the topic/Event Hub the message was received from
        partition: Partition number the message was received from
        offset: Message offset within the partition (for ordering and checkpointing)
        timestamp: Message timestamp in milliseconds since Unix epoch
        key: Optional message key as raw bytes (used for partitioning/routing)
        value: Message payload as raw bytes (typically JSON-encoded data)
        headers: Optional list of (name, value) header tuples for metadata

    Note:
        - Both key and value are bytes to avoid encoding assumptions
        - Deserialize value based on your message format (typically JSON)
        - Headers store arbitrary metadata (e.g., correlation ID, trace context)
        - Timestamp comes from broker (producer-set or broker-assigned)
    """

    topic: str
    partition: int
    offset: int
    timestamp: int
    key: bytes | None = None
    value: bytes | None = None
    headers: list[tuple[str, bytes]] | None = None


@dataclass(frozen=True)
class ProduceResult:
    """Transport-agnostic confirmation of a published message.

    Replaces aiokafka.structs.RecordMetadata with a transport-agnostic
    representation returned after successfully publishing a message.

    All fields are immutable (frozen=True) for consistency with message types.

    Attributes:
        topic: Name of the topic/Event Hub the message was published to
        partition: Partition number the message was assigned to
        offset: Offset assigned to the message within the partition

    Note:
        - offset can be used to verify message was persisted
        - partition indicates which partition the broker selected
        - Combine topic + partition + offset for unique message identifier
    """

    topic: str
    partition: int
    offset: int


@dataclass(frozen=True)
class PartitionInfo:
    """Transport-agnostic partition identifier for Kafka or Event Hub.

    Replaces aiokafka.structs.TopicPartition with a simple, transport-agnostic
    representation used for partition management and offset tracking.

    All fields are immutable (frozen=True) for use as dictionary keys and
    to prevent accidental modification.

    Attributes:
        topic: Name of the topic/Event Hub
        partition: Partition number (0-based index)

    Note:
        - Used for seeking to specific offsets: consumer.seek(partition, offset)
        - Can be used as dictionary key due to immutability
        - Event Hubs use same partition concept as Kafka
    """

    topic: str
    partition: int


def from_consumer_record(record) -> PipelineMessage:
    """Convert aiokafka ConsumerRecord to transport-agnostic PipelineMessage.

    This helper enables gradual migration from aiokafka ConsumerRecord to
    PipelineMessage. Workers can convert records at consumption time without
    importing aiokafka types in their handler methods.

    Args:
        record: aiokafka ConsumerRecord from consumer.getmany()

    Returns:
        PipelineMessage with same data in transport-agnostic format

    Example:
        ```python
        # In worker consume loop
        msg_dict = await self.consumer.getmany(timeout_ms=1000)
        for partition, messages in msg_dict.items():
            for msg in messages:
                pipeline_msg = from_consumer_record(msg)
                await self._handle_message(pipeline_msg)
        ```

    Note:
        - Avoids importing aiokafka in this module (accepts Any type)
        - Headers conversion: aiokafka uses List[Tuple[str, bytes]]
        - Timestamp is already in milliseconds for Kafka
        - Type hint uses 'Any' to avoid circular aiokafka dependency
    """
    # Convert headers from aiokafka format to PipelineMessage format
    # aiokafka: List[Tuple[str, bytes]] or None
    headers = None
    if hasattr(record, "headers") and record.headers:
        headers = [(k, v) for k, v in record.headers]

    return PipelineMessage(
        topic=record.topic,
        partition=record.partition,
        offset=record.offset,
        timestamp=record.timestamp,
        key=record.key,
        value=record.value,
        headers=headers,
    )
