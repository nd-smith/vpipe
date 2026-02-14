"""Context variables for structured logging."""

from contextvars import ContextVar

# Pipeline execution context
_cycle_id: ContextVar[str] = ContextVar("cycle_id", default="")
_stage_name: ContextVar[str] = ContextVar("stage_name", default="")
_worker_id: ContextVar[str] = ContextVar("worker_id", default="")
_domain: ContextVar[str] = ContextVar("domain", default="")
_trace_id: ContextVar[str] = ContextVar("trace_id", default="")
_event_id: ContextVar[str] = ContextVar("event_id", default="")
_media_id: ContextVar[str] = ContextVar("media_id", default="")

# Message transport context
_message_topic: ContextVar[str] = ContextVar("message_topic", default="")
_message_partition: ContextVar[int] = ContextVar("message_partition", default=-1)
_message_offset: ContextVar[int] = ContextVar("message_offset", default=-1)
_message_key: ContextVar[str] = ContextVar("message_key", default="")
_message_consumer_group: ContextVar[str] = ContextVar("message_consumer_group", default="")


def set_log_context(
    cycle_id: str | None = None,
    stage: str | None = None,
    worker_id: str | None = None,
    domain: str | None = None,
    trace_id: str | None = None,
    event_id: str | None = None,
    media_id: str | None = None,
) -> None:
    if cycle_id is not None:
        _cycle_id.set(cycle_id)
    if stage is not None:
        _stage_name.set(stage)
    if worker_id is not None:
        _worker_id.set(worker_id)
    if domain is not None:
        _domain.set(domain)
    if trace_id is not None:
        _trace_id.set(trace_id)
    if event_id is not None:
        _event_id.set(event_id)
    if media_id is not None:
        _media_id.set(media_id)


def get_log_context() -> dict[str, str]:
    context = {
        "cycle_id": _cycle_id.get(),
        "stage": _stage_name.get(),
        "worker_id": _worker_id.get(),
        "domain": _domain.get(),
        "trace_id": _trace_id.get(),
        "event_id": _event_id.get(),
        "media_id": _media_id.get(),
    }

    # Note: Distributed tracing (OpenTracing) has been removed

    return context


def clear_log_context() -> None:
    _cycle_id.set("")
    _stage_name.set("")
    _worker_id.set("")
    _domain.set("")
    _trace_id.set("")
    _event_id.set("")
    _media_id.set("")


def set_message_context(
    topic: str | None = None,
    partition: int | None = None,
    offset: int | None = None,
    key: str | None = None,
    consumer_group: str | None = None,
) -> None:
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


def get_message_context() -> dict[str, any]:
    context = {
        "message_topic": _message_topic.get(),
        "message_partition": _message_partition.get(),
        "message_offset": _message_offset.get(),
    }

    key = _message_key.get()
    if key:
        context["message_key"] = key

    consumer_group = _message_consumer_group.get()
    if consumer_group:
        context["message_consumer_group"] = consumer_group

    return context


def clear_message_context() -> None:
    _message_topic.set("")
    _message_partition.set(-1)
    _message_offset.set(-1)
    _message_key.set("")
    _message_consumer_group.set("")
