"""Logging utility functions."""

import logging
from typing import Any, Optional


# Reserved LogRecord attribute names that cannot be used in extra dict
_RESERVED_LOG_KEYS = frozenset({
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "lineno", "funcName", "created", "asctime", "msecs",
    "relativeCreated", "thread", "threadName", "processName", "process",
    "message", "exc_info", "exc_text", "stack_info",
})


def log_with_context(
    logger: logging.Logger,
    level: int,
    msg: str,
    **kwargs: Any,
) -> None:
    """
    Log with structured context fields.

    Args:
        logger: Logger instance
        level: Log level (logging.INFO, etc.)
        msg: Log message
        **kwargs: Additional context fields (trace_id, duration_ms, etc.)
                  Note: exc_info=True is supported and handled specially.

    Example:
        log_with_context(
            logger, logging.INFO, "Download complete",
            trace_id=task.trace_id,
            duration_ms=elapsed,
            http_status=200,
        )
    """
    # Handle exc_info specially - it's a direct parameter to log(), not extra
    exc_info = kwargs.pop("exc_info", None)

    # Filter out reserved keys to prevent LogRecord conflicts
    extra = {k: v for k, v in kwargs.items() if k not in _RESERVED_LOG_KEYS}

    logger.log(level, msg, exc_info=exc_info, extra=extra)


def log_exception(
    logger: logging.Logger,
    exc: Exception,
    msg: str,
    level: int = logging.ERROR,
    include_traceback: bool = True,
    **kwargs: Any,
) -> None:
    """
    Log exception with context and optional traceback.

    Automatically extracts error_category from PipelineError subclasses.
    Sanitizes error messages to remove sensitive data.

    Args:
        logger: Logger instance
        exc: Exception to log
        msg: Context message
        level: Log level (default: ERROR)
        include_traceback: Include full traceback (default: True)
        **kwargs: Additional context fields

    Example:
        try:
            download_file(url)
        except Exception as e:
            log_exception(logger, e, "Download failed", trace_id=task.trace_id)
    """
    # Extract error category if available
    error_category = kwargs.get("error_category")
    if error_category is None and hasattr(exc, "category"):
        cat = exc.category
        error_category = cat.value if hasattr(cat, "value") else str(cat)
        kwargs["error_category"] = error_category

    # Sanitize error message
    error_msg = str(exc)
    if len(error_msg) > 500:
        error_msg = error_msg[:500] + "..."
    kwargs["error_message"] = error_msg

    if include_traceback:
        logger.log(level, msg, exc_info=exc, extra=kwargs)
    else:
        logger.log(level, msg, extra=kwargs)


def format_cycle_output(
    cycle_count: int,
    succeeded: int,
    failed: int,
    skipped: int = 0,
    in_flight: Optional[int] = None,
    error_breakdown: Optional[dict[str, int]] = None,
) -> str:
    """
    Format standardized cycle output for workers.

    Args:
        cycle_count: Current cycle number
        succeeded: Number of records succeeded
        failed: Number of records failed
        skipped: Number of records skipped (default: 0)
        in_flight: Number of records currently being processed (optional)
        error_breakdown: Error category breakdown, e.g., {"transient": 20, "permanent": 14}

    Returns:
        Formatted cycle output string

    Example:
        >>> format_cycle_output(1, 1200, 34, 0, 12, {"transient": 20, "permanent": 14})
        '[CYCLE 1] Processing | Succeeded: 1200 | Failed: 34 (transient: 20, permanent: 14) | Skipped: 0 | In-flight: 12'
    """
    parts = [f"[CYCLE {cycle_count}] Processing"]
    parts.append(f"Succeeded: {succeeded}")

    # Format failed with optional breakdown
    if error_breakdown and failed > 0:
        breakdown_str = ", ".join(f"{k}: {v}" for k, v in sorted(error_breakdown.items()))
        parts.append(f"Failed: {failed} ({breakdown_str})")
    else:
        parts.append(f"Failed: {failed}")

    parts.append(f"Skipped: {skipped}")

    if in_flight is not None:
        parts.append(f"In-flight: {in_flight}")

    return " | ".join(parts)


def log_worker_error(
    logger: logging.Logger,
    error_message: str,
    event_id: Optional[str] = None,
    error_category: Optional[str] = None,
    exc: Optional[Exception] = None,
    **context: Any,
) -> None:
    """
    Log worker error with standardized context.

    This function ensures all worker errors are logged consistently with
    required context fields like event_id, error_category, etc.

    Args:
        logger: Logger instance
        error_message: Human-readable error description
        event_id: Event/trace ID for correlation (if available)
        error_category: Error category (TRANSIENT, PERMANENT, AUTH, etc.)
        exc: Exception object (will include traceback if provided)
        **context: Additional context fields (media_id, status_code, etc.)

    Example:
        log_worker_error(
            logger,
            "Download failed",
            event_id="evt_123",
            error_category="TRANSIENT",
            media_id="media_456",
            status_code=500,
            retry_count=2,
        )
    """
    # Build structured context
    extra = dict(context)

    if event_id:
        extra["event_id"] = event_id
        extra["correlation_id"] = event_id  # Also set correlation_id for consistency

    if error_category:
        extra["error_category"] = error_category

    # Log with or without traceback
    if exc:
        logger.error(error_message, extra=extra, exc_info=exc)
    else:
        logger.error(error_message, extra=extra)
