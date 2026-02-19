"""
Pipeline-specific logging utilities.

Provides pipeline-specific abstractions built on top of core.logging:
- extract_log_context: Extract identifiers from pipeline objects for logging
- with_api_error_handling: Decorator for API error handling

For core logging functions, import directly from core.logging:
    from core.logging import get_logger, log_with_context, log_exception
"""

import asyncio
import functools
from collections.abc import Callable
from typing import Any, TypeVar

from core.logging import get_logger, log_exception

__all__ = [
    "extract_log_context",
    "with_api_error_handling",
]


_UNWRAP_ATTRS = [
    ("task", ("error_category", "http_status")),
    ("event", ("error_category", "api_calls")),
]


def _unwrap_result_object(obj: Any, ctx: dict[str, Any]) -> Any:
    """Unwrap result objects to get underlying task/event, adding error context."""
    for inner_attr, extra_fields in _UNWRAP_ATTRS:
        inner = getattr(obj, inner_attr, None)
        if inner is None:
            continue
        for field in extra_fields:
            value = getattr(obj, field, None)
            if value:
                if field == "error_category":
                    ctx[field] = value.value if hasattr(value, "value") else str(value)
                else:
                    ctx[field] = value
        return inner
    return obj


_CONTEXT_ATTRS = [
    "trace_id",
    "assignment_id",
    "status_subtype",
    "event_type",
    "project_id",
    "media_id",
]


def extract_log_context(obj: Any) -> dict[str, Any]:
    """
    Extract loggable context from any pipeline object.

    Handles ClaimX and Xact objects by extracting identifier fields.

    Args:
        obj: Pipeline model object (Task, Event, Result, etc.)

    Returns:
        Dict with identifier fields suitable for logging

    Example:
        try:
            result = download(task)
        except Exception as e:
            log_exception(logger, e, "Download failed", **extract_log_context(task))
    """
    ctx: dict[str, Any] = {}

    if obj is None:
        return ctx

    inner = _unwrap_result_object(obj, ctx)

    for attr in _CONTEXT_ATTRS:
        if hasattr(inner, attr):
            value = getattr(inner, attr)
            if value is not None:
                ctx[attr] = value

    return ctx


F = TypeVar("F", bound=Callable[..., Any])


def with_api_error_handling(func: F) -> F:
    """
    Decorator for API error handling with logging.

    Logs exceptions and re-raises them.

    Example:
        @with_api_error_handling
        async def fetch_projects(self):
            ...
    """

    if asyncio.iscoroutinefunction(func):

        @functools.wraps(func)
        async def async_wrapper(self, *args, **kwargs):
            _logger = getattr(self, "_logger", None) or get_logger(self.__class__.__module__)
            try:
                return await func(self, *args, **kwargs)
            except Exception as e:
                log_exception(_logger, e, f"{func.__name__} failed")
                raise

        return async_wrapper  # type: ignore
    else:

        @functools.wraps(func)
        def sync_wrapper(self, *args, **kwargs):
            _logger = getattr(self, "_logger", None) or get_logger(self.__class__.__module__)
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                log_exception(_logger, e, f"{func.__name__} failed")
                raise

        return sync_wrapper  # type: ignore
