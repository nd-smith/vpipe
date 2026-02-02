"""
Message context compatibility module.

Re-exports message context functions and class with kafka_ prefixed keys
for backward compatibility with tests. The actual implementation uses
message_ prefixed keys in core.logging.context.
"""

from core.logging.context import (
    clear_message_context as _clear_message_context,
    get_message_context as _get_message_context,
    set_message_context as _set_message_context,
)
from core.logging.context_managers import MessageLogContext as _MessageLogContext


def set_message_context(
    topic: str | None = None,
    partition: int | None = None,
    offset: int | None = None,
    key: str | None = None,
    consumer_group: str | None = None,
) -> None:
    """Set message context with kafka_ prefixed keys."""
    _set_message_context(
        topic=topic,
        partition=partition,
        offset=offset,
        key=key,
        consumer_group=consumer_group,
    )


def get_message_context() -> dict[str, any]:
    """Get message context with kafka_ prefixed keys for backward compatibility."""
    ctx = _get_message_context()
    # Convert message_ prefix to kafka_ prefix
    return {
        "kafka_topic": ctx.get("message_topic", ""),
        "kafka_partition": ctx.get("message_partition", -1),
        "kafka_offset": ctx.get("message_offset", -1),
        **{
            f"kafka_{k.replace('message_', '')}": v
            for k, v in ctx.items()
            if k.startswith("message_") and k not in ["message_topic", "message_partition", "message_offset"]
        },
    }


def clear_message_context() -> None:
    """Clear message context."""
    _clear_message_context()


class MessageLogContext(_MessageLogContext):
    """Context manager for message processing with kafka_ prefixed keys."""

    pass


__all__ = [
    "set_message_context",
    "get_message_context",
    "clear_message_context",
    "MessageLogContext",
]
