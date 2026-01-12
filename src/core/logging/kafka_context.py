"""Kafka-specific context variables for structured logging."""

from contextvars import ContextVar
from typing import Dict, Optional


# Kafka-specific context variables
_kafka_topic: ContextVar[str] = ContextVar("kafka_topic", default="")
_kafka_partition: ContextVar[int] = ContextVar("kafka_partition", default=-1)
_kafka_offset: ContextVar[int] = ContextVar("kafka_offset", default=-1)
_kafka_key: ContextVar[str] = ContextVar("kafka_key", default="")
_kafka_consumer_group: ContextVar[str] = ContextVar("kafka_consumer_group", default="")


def set_kafka_context(
    topic: Optional[str] = None,
    partition: Optional[int] = None,
    offset: Optional[int] = None,
    key: Optional[str] = None,
    consumer_group: Optional[str] = None,
) -> None:
    """
    Set Kafka-specific context variables for structured logging.

    Args:
        topic: Kafka topic name
        partition: Kafka partition number
        offset: Message offset within partition
        key: Message key (if any)
        consumer_group: Consumer group ID
    """
    if topic is not None:
        _kafka_topic.set(topic)
    if partition is not None:
        _kafka_partition.set(partition)
    if offset is not None:
        _kafka_offset.set(offset)
    if key is not None:
        _kafka_key.set(key)
    if consumer_group is not None:
        _kafka_consumer_group.set(consumer_group)


def get_kafka_context() -> Dict[str, any]:
    """
    Get current Kafka logging context.

    Returns:
        Dictionary with topic, partition, offset, key, and consumer_group
    """
    context = {
        "kafka_topic": _kafka_topic.get(),
        "kafka_partition": _kafka_partition.get(),
        "kafka_offset": _kafka_offset.get(),
    }

    # Only include optional fields if they're set
    key = _kafka_key.get()
    if key:
        context["kafka_key"] = key

    consumer_group = _kafka_consumer_group.get()
    if consumer_group:
        context["kafka_consumer_group"] = consumer_group

    return context


def clear_kafka_context() -> None:
    """Clear all Kafka logging context variables."""
    _kafka_topic.set("")
    _kafka_partition.set(-1)
    _kafka_offset.set(-1)
    _kafka_key.set("")
    _kafka_consumer_group.set("")


class KafkaLogContext:
    """
    Context manager for Kafka message processing with automatic context setting.

    Usage:
        with KafkaLogContext(topic="events", partition=0, offset=12345):
            # All logs in this block will include Kafka context
            process_message()
    """

    def __init__(
        self,
        topic: Optional[str] = None,
        partition: Optional[int] = None,
        offset: Optional[int] = None,
        key: Optional[str] = None,
        consumer_group: Optional[str] = None,
    ):
        self.new_context = {
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "key": key,
            "consumer_group": consumer_group,
        }
        self.old_context: Dict[str, any] = {}

    def __enter__(self) -> "KafkaLogContext":
        # Save current context
        self.old_context = {
            "topic": _kafka_topic.get(),
            "partition": _kafka_partition.get(),
            "offset": _kafka_offset.get(),
            "key": _kafka_key.get(),
            "consumer_group": _kafka_consumer_group.get(),
        }

        # Set new context
        for key, value in self.new_context.items():
            if value is not None:
                set_kafka_context(**{key: value})

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore old context
        set_kafka_context(
            topic=self.old_context.get("topic", ""),
            partition=self.old_context.get("partition", -1),
            offset=self.old_context.get("offset", -1),
            key=self.old_context.get("key", ""),
            consumer_group=self.old_context.get("consumer_group", ""),
        )
        return False
