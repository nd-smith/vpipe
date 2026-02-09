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

    # Unwrap result objects to get underlying task/event
    inner = obj
    if hasattr(obj, "task") and obj.task is not None:
        inner = obj.task
        # Capture error info from result
        if hasattr(obj, "error_category") and obj.error_category:
            cat = obj.error_category
            ctx["error_category"] = cat.value if hasattr(cat, "value") else str(cat)
        if hasattr(obj, "http_status") and obj.http_status:
            ctx["http_status"] = obj.http_status
    elif hasattr(obj, "event") and obj.event is not None:
        inner = obj.event
        # Capture error info from result
        if hasattr(obj, "error_category") and obj.error_category:
            cat = obj.error_category
            ctx["error_category"] = cat.value if hasattr(cat, "value") else str(cat)
        if hasattr(obj, "api_calls") and obj.api_calls:
            ctx["api_calls"] = obj.api_calls

    # Extract common identifiers
    for attr in [
        "trace_id",
        "assignment_id",
        "status_subtype",  # Xact
        "event_id",
        "event_type",
        "project_id",
        "media_id",  # ClaimX
    ]:
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
            _logger = getattr(self, "_logger", None) or get_logger(
                self.__class__.__module__
            )
            try:
                return await func(self, *args, **kwargs)
            except Exception as e:
                log_exception(_logger, e, f"{func.__name__} failed")
                raise

        return async_wrapper  # type: ignore
    else:

        @functools.wraps(func)
        def sync_wrapper(self, *args, **kwargs):
            _logger = getattr(self, "_logger", None) or get_logger(
                self.__class__.__module__
            )
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                log_exception(_logger, e, f"{func.__name__} failed")
                raise

        return sync_wrapper  # type: ignore
