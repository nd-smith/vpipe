"""Context variables for structured logging."""

from contextvars import ContextVar

_cycle_id: ContextVar[str] = ContextVar("cycle_id", default="")
_stage_name: ContextVar[str] = ContextVar("stage_name", default="")
_worker_id: ContextVar[str] = ContextVar("worker_id", default="")
_domain: ContextVar[str] = ContextVar("domain", default="")
_trace_id: ContextVar[str] = ContextVar("trace_id", default="")


def set_log_context(
    cycle_id: str | None = None,
    stage: str | None = None,
    worker_id: str | None = None,
    domain: str | None = None,
    trace_id: str | None = None,
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


def get_log_context() -> dict[str, str]:
    context = {
        "cycle_id": _cycle_id.get(),
        "stage": _stage_name.get(),
        "worker_id": _worker_id.get(),
        "domain": _domain.get(),
        "trace_id": _trace_id.get(),
    }

    # Note: Distributed tracing (OpenTracing) has been removed

    return context


def clear_log_context() -> None:
    _cycle_id.set("")
    _stage_name.set("")
    _worker_id.set("")
    _domain.set("")
    _trace_id.set("")
