"""Context variables for structured logging."""

from contextvars import ContextVar
from typing import Dict, Optional

# Context variables for structured logging
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
    """
    Set context variables for structured logging.

    Args:
        cycle_id: Current cycle identifier
        stage: Current stage name
        worker_id: Worker identifier
        domain: Pipeline domain (xact, claimx, kafka)
        trace_id: Trace identifier for request/event tracking
    """
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
    """
    Get current logging context.

    Returns:
        Dictionary with cycle_id, stage, worker_id, domain, and trace_id
    """
    return {
        "cycle_id": _cycle_id.get(),
        "stage": _stage_name.get(),
        "worker_id": _worker_id.get(),
        "domain": _domain.get(),
        "trace_id": _trace_id.get(),
    }


def clear_log_context() -> None:
    """Clear all logging context variables."""
    _cycle_id.set("")
    _stage_name.set("")
    _worker_id.set("")
    _domain.set("")
    _trace_id.set("")
