"""Message transport context variables for structured logging."""

from contextvars import ContextVar
from typing import Dict, Optional

# Message transport context variables
_message_topic: ContextVar[str] = ContextVar("message_topic", default="")
_message_partition: ContextVar[int] = ContextVar("message_partition", default=-1)
_message_offset: ContextVar[int] = ContextVar("message_offset", default=-1)
_message_key: ContextVar[str] = ContextVar("message_key", default="")
_message_consumer_group: ContextVar[str] = ContextVar("message_consumer_group", default="")


def set_message_context(
    topic: Optional[str] = None,
    partition: Optional[int] = None,
    offset: Optional[int] = None,
    key: Optional[str] = None,
    consumer_group: Optional[str] = None,
) -> None:
    """
    Set message transport context variables for structured logging.

    Args:
        topic: Message topic/stream name
        partition: Partition number
        offset: Message offset within partition
        key: Message key (if any)
        consumer_group: Consumer group ID
    """
    if topic is not None:
        _message_topic.set(topic)
    if partition is not None:
        _message_partition.set(partition)
    if offset is not None:
        _message_offset.set(offset)
    if key is not None:
        _message_key.set(key)
    if consumer_group is not None:
        _message_consumer_group.set(consumer_group)


def get_message_context() -> Dict[str, any]:
    """
    Get current message transport logging context.

    Returns:
        Dictionary with topic, partition, offset, key, and consumer_group
    """
    context = {
        "message_topic": _message_topic.get(),
        "message_partition": _message_partition.get(),
        "message_offset": _message_offset.get(),
    }

    # Only include optional fields if they're set
    key = _message_key.get()
    if key:
        context["message_key"] = key

    consumer_group = _message_consumer_group.get()
    if consumer_group:
        context["message_consumer_group"] = consumer_group

    return context


def clear_message_context() -> None:
    """Clear all message transport logging context variables."""
    _message_topic.set("")
    _message_partition.set(-1)
    _message_offset.set(-1)
    _message_key.set("")
    _message_consumer_group.set("")


class MessageLogContext:
    """
    Context manager for message processing with automatic context setting.

    Usage:
        with MessageLogContext(topic="events", partition=0, offset=12345):
            # All logs in this block will include message context
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

    def __enter__(self) -> "MessageLogContext":
        # Save current context
        self.old_context = {
            "topic": _message_topic.get(),
            "partition": _message_partition.get(),
            "offset": _message_offset.get(),
            "key": _message_key.get(),
            "consumer_group": _message_consumer_group.get(),
        }

        for key, value in self.new_context.items():
            if value is not None:
                set_message_context(**{key: value})

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore old context
        set_message_context(
            topic=self.old_context.get("topic", ""),
            partition=self.old_context.get("partition", -1),
            offset=self.old_context.get("offset", -1),
            key=self.old_context.get("key", ""),
            consumer_group=self.old_context.get("consumer_group", ""),
        )
        return False
