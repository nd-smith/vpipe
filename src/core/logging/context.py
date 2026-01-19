"""Context variables for structured logging."""

from contextvars import ContextVar
from typing import Dict, Optional

_cycle_id: ContextVar[str] = ContextVar("cycle_id", default="")
_stage_name: ContextVar[str] = ContextVar("stage_name", default="")
_worker_id: ContextVar[str] = ContextVar("worker_id", default="")
_domain: ContextVar[str] = ContextVar("domain", default="")
_trace_id: ContextVar[str] = ContextVar("trace_id", default="")


def set_log_context(
    cycle_id: Optional[str] = None,
    stage: Optional[str] = None,
    worker_id: Optional[str] = None,
    domain: Optional[str] = None,
    trace_id: Optional[str] = None,
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


def get_log_context() -> Dict[str, str]:
    context = {
        "cycle_id": _cycle_id.get(),
        "stage": _stage_name.get(),
        "worker_id": _worker_id.get(),
        "domain": _domain.get(),
        "trace_id": _trace_id.get(),
    }

    # Add OpenTelemetry trace context if available
    try:
        from opentelemetry import trace

        span = trace.get_current_span()
        span_ctx = span.get_span_context()

        if span_ctx.is_valid:
            context["otel_trace_id"] = format(span_ctx.trace_id, '032x')
            context["otel_span_id"] = format(span_ctx.span_id, '016x')
    except Exception:
        # OTel not initialized or not available, skip
        pass

    return context


def clear_log_context() -> None:
    _cycle_id.set("")
    _stage_name.set("")
    _worker_id.set("")
    _domain.set("")
    _trace_id.set("")
