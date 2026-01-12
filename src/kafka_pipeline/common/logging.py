"""
kafka_pipeline-specific logging utilities.

Provides pipeline-specific abstractions built on top of core.logging:
- LoggedClass: Mixin for classes with logging infrastructure
- logged_operation: Decorator for automatic operation logging
- extract_log_context: Extract identifiers from pipeline objects for logging
- with_api_error_handling: Decorator for API error handling

For core logging functions, import directly from core.logging:
    from core.logging import get_logger, log_with_context, log_exception
"""

import functools
import logging
from typing import Any, Callable, Dict, Optional, TypeVar

from core.logging import get_logger, log_exception, log_with_context

F = TypeVar("F", bound=Callable[..., Any])

__all__ = [
    "LoggedClass",
    "logged_operation",
    "extract_log_context",
    "with_api_error_handling",
]


def _is_coroutine_function(func: Callable) -> bool:
    """Check if function is a coroutine function."""
    import asyncio

    return asyncio.iscoroutinefunction(func)


def _extract_instance_context(obj: Any) -> Dict[str, Any]:
    """
    Extract loggable context from instance attributes.

    Looks for common identifier fields.

    Args:
        obj: Object instance

    Returns:
        Dict with identifier fields
    """
    ctx: Dict[str, Any] = {}

    # Common identifiers
    for attr in ["table_path", "primary_keys", "circuit_name", "api_url"]:
        if hasattr(obj, attr):
            value = getattr(obj, attr)
            if value is not None:
                # Convert table_path -> table for cleaner logging
                key = "table" if attr == "table_path" else attr
                ctx[key] = value

    return ctx


def logged_operation(
    level: int = logging.DEBUG,
    slow_threshold_ms: Optional[float] = None,
    log_start: bool = False,
    operation_name: Optional[str] = None,
) -> Callable[[F], F]:
    """
    Decorator for automatic operation logging on class methods.

    Simplified version without full OperationContext infrastructure.

    Args:
        level: Log level for completion message
        slow_threshold_ms: Not implemented in simplified version
        log_start: Also log when operation starts
        operation_name: Override operation name (default: method_name)

    Example:
        class ApiClient(LoggedClass):
            @logged_operation(level=logging.DEBUG)
            def fetch_data(self):
                ...
    """

    def decorator(func: F) -> F:
        if _is_coroutine_function(func):

            @functools.wraps(func)
            async def async_wrapper(self, *args, **kwargs):
                _logger = getattr(self, "_logger", None) or get_logger(
                    self.__class__.__module__
                )
                op_name = operation_name or func.__name__
                full_op = f"{self.__class__.__name__}.{op_name}"

                if log_start:
                    log_with_context(_logger, level, f"{full_op} starting")

                try:
                    result = await func(self, *args, **kwargs)
                    log_with_context(_logger, level, f"{full_op} completed")
                    return result
                except Exception as e:
                    log_exception(_logger, e, f"{full_op} failed")
                    raise

            return async_wrapper  # type: ignore
        else:

            @functools.wraps(func)
            def sync_wrapper(self, *args, **kwargs):
                _logger = getattr(self, "_logger", None) or get_logger(
                    self.__class__.__module__
                )
                op_name = operation_name or func.__name__
                full_op = f"{self.__class__.__name__}.{op_name}"

                if log_start:
                    log_with_context(_logger, level, f"{full_op} starting")

                try:
                    result = func(self, *args, **kwargs)
                    log_with_context(_logger, level, f"{full_op} completed")
                    return result
                except Exception as e:
                    log_exception(_logger, e, f"{full_op} failed")
                    raise

            return sync_wrapper  # type: ignore

    return decorator


class LoggedClass:
    """
    Mixin providing logging infrastructure for classes.

    Provides:
    - self._logger: Logger instance
    - self._log(): Log with auto-extracted context
    - self._log_exception(): Exception logging with context

    Example:
        class ApiClient(LoggedClass):
            def __init__(self, api_url: str):
                self.api_url = api_url
                super().__init__()

            def fetch(self):
                self._log(logging.DEBUG, "Fetching data")
                ...
    """

    log_component: Optional[str] = None  # Optional logger name suffix

    def __init__(self, *args, **kwargs):
        logger_name = self.__class__.__module__
        if self.log_component:
            logger_name = f"{logger_name}.{self.log_component}"
        self._logger = get_logger(logger_name)
        super().__init__(*args, **kwargs)

    def _log(self, level: int, msg: str, **extra: Any) -> None:
        """
        Log with automatic context extraction from instance.

        Args:
            level: Log level
            msg: Log message
            **extra: Additional context fields
        """
        context = _extract_instance_context(self)
        context.update(extra)
        log_with_context(self._logger, level, msg, **context)

    def _log_exception(
        self,
        exc: Exception,
        msg: str,
        level: int = logging.ERROR,
        **extra: Any,
    ) -> None:
        """
        Log exception with automatic context extraction from instance.

        Args:
            exc: Exception to log
            msg: Context message
            level: Log level (default: ERROR)
            **extra: Additional context fields
        """
        context = _extract_instance_context(self)
        context.update(extra)
        log_exception(self._logger, exc, msg, level=level, **context)


def extract_log_context(obj: Any) -> Dict[str, Any]:
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
    ctx: Dict[str, Any] = {}

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


def with_api_error_handling(func: F) -> F:
    """
    Decorator for API error handling with logging.

    Logs exceptions and re-raises them.

    Example:
        @with_api_error_handling
        async def fetch_projects(self):
            ...
    """

    if _is_coroutine_function(func):

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
