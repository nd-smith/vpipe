"""Logging utility functions."""

import logging
from datetime import UTC, datetime
from typing import Any

# Reserved LogRecord attribute names that cannot be used in extra dict
_RESERVED_LOG_KEYS = frozenset(
    {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "lineno",
        "funcName",
        "created",
        "asctime",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "message",
        "exc_info",
        "exc_text",
        "stack_info",
    }
)


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
    deduplicated: int = 0,
    since_last: dict[str, int] | None = None,
    interval_seconds: int = 30,
    **kwargs,
) -> str:
    """
    Format standardized cycle output for workers with delta tracking.

    Args:
        cycle_count: Current cycle number
        succeeded: Total count of successfully processed records
        failed: Total count of failed records
        skipped: Total count of skipped records (default: 0)
        deduplicated: Total count of deduplicated records (default: 0)
        since_last: Optional delta counts since last cycle (keys: succeeded, failed, skipped, deduplicated)
        interval_seconds: Cycle interval in seconds (default: 30)

    Returns:
        Formatted cycle output string

    Example:
        >>> format_cycle_output(1, 1200, 34, 50, 100)
        'Cycle 1: processed=1200 (succeeded=1200, failed=34, skipped=50, deduped=100)'
        >>> format_cycle_output(5, 1200, 34, 50, 0, {"succeeded": 240, "failed": 0, "skipped": 0}, 30)
        'Cycle 5: +240 this cycle | total: 1200 succeeded, 34 failed, 50 skipped | 8.0 msg/s'
    """
    offset_start_ts: int | None = kwargs.get("offset_start_ts")
    offset_end_ts: int | None = kwargs.get("offset_end_ts")

    total_processed = succeeded + failed + skipped

    # Build offset suffix if both timestamps are provided
    offset_suffix = ""
    if offset_start_ts is not None and offset_end_ts is not None:
        start = datetime.fromtimestamp(offset_start_ts / 1000, tz=UTC)
        end = datetime.fromtimestamp(offset_end_ts / 1000, tz=UTC)
        offset_suffix = f" | offsets: {start:%Y-%m-%d %H:%M:%SZ} .. {end:%Y-%m-%d %H:%M:%SZ}"

    # If deltas are provided, format with delta and rate
    if since_last is not None:
        delta_total = (
            since_last.get("succeeded", 0)
            + since_last.get("failed", 0)
            + since_last.get("skipped", 0)
        )
        rate = delta_total / interval_seconds if interval_seconds > 0 else 0

        parts = [f"+{delta_total} this cycle"]

        # Total section
        total_parts = [f"{succeeded} succeeded"]
        if failed > 0:
            total_parts.append(f"{failed} failed")
        if skipped > 0:
            total_parts.append(f"{skipped} skipped")
        if deduplicated > 0:
            total_parts.append(f"{deduplicated} deduped")

        parts.append(f"total: {', '.join(total_parts)}")
        parts.append(f"{rate:.1f} msg/s")

        return f"Cycle {cycle_count}: {' | '.join(parts)}{offset_suffix}"

    # Legacy format without deltas
    parts = [f"processed={total_processed}"]
    parts.append(f"succeeded={succeeded}")
    parts.append(f"failed={failed}")

    if skipped > 0:
        parts.append(f"skipped={skipped}")

    if deduplicated > 0:
        parts.append(f"deduped={deduplicated}")

    return f"Cycle {cycle_count}: {', '.join(parts)}{offset_suffix}"


def get_log_output_mode(
    log_to_stdout: bool,
    enable_eventhub_logging: bool,
) -> str:
    """
    Determine log output mode based on configuration flags.

    File logging is always active. log_to_stdout adds verbose console output.

    Returns:
        String describing where logs are being sent: "file+stdout",
        "file+eventhub+stdout", "file", "file+eventhub", etc.
    """
    modes = ["file"]
    if enable_eventhub_logging:
        modes.append("eventhub")
    if log_to_stdout:
        modes.append("stdout")

    return "+".join(modes)


def _classify_handler_modes(handlers: list[logging.Handler]) -> list[str]:
    """Classify logging handlers into mode strings (file, eventhub)."""
    modes: list[str] = []
    if any(isinstance(h, logging.FileHandler) for h in handlers):
        modes.append("file")
    if any("EventHub" in type(h).__name__ for h in handlers):
        modes.append("eventhub")
    return modes


def detect_log_output_mode() -> str:
    """
    Detect current log output mode by inspecting active logging handlers.

    Returns:
        String describing where logs are being sent: "stdout", "file",
        "eventhub", "file+eventhub", etc.
    """
    handlers = logging.getLogger().handlers
    modes = _classify_handler_modes(handlers)

    if not modes:
        # Single StreamHandler = stdout-only mode; no handlers = console fallback
        return "stdout" if len(handlers) == 1 else "console"

    return "+".join(modes)


_BANNER_FIELDS: list[tuple[str, str]] = [
    ("instance_id", "Instance:     {}"),
    ("domain", "Domain:       {}"),
    ("input_topic", "Input Topic:  {}"),
    ("output_topic", "Output Topic: {}"),
    ("health_port", "Health:       http://localhost:{}"),
    ("log_output_mode", "Log Output:   {}"),
]


def log_startup_banner(
    logger: logging.Logger,
    worker_name: str,
    **kwargs: Any,
) -> None:
    """
    Log startup banner with worker configuration.

    Args:
        logger: Logger instance
        worker_name: Worker name (e.g., "ClaimX Enrichment Worker")
        **kwargs: Optional fields: instance_id, domain, input_topic, output_topic,
            health_port, version, log_output_mode

    Example:
        log_startup_banner(
            logger,
            worker_name="ClaimX Enrichment Worker",
            instance_id="enrichment_worker-0",
            domain="claimx",
            input_topic="claimx.enrichment.pending",
            output_topic="claimx.downloads.pending",
            health_port=8081,
        )
    """
    separator = "=" * 50

    lines = ["", separator, worker_name]

    version = kwargs.get("version")
    if version:
        lines.append(f"Version: {version}")

    lines.append(separator)

    for field_name, fmt in _BANNER_FIELDS:
        value = kwargs.get(field_name)
        if value:
            lines.append(fmt.format(value))

    lines.append(separator)
    lines.append("")

    logger.info("\n".join(lines))


def log_worker_error(
    logger: logging.Logger,
    error_message: str,
    error_category: str | None = None,
    exc: Exception | None = None,
    **context: Any,
) -> None:
    """
    Log worker error with standardized context.

    This function ensures all worker errors are logged consistently with
    required context fields like error_category, etc.

    Args:
        logger: Logger instance
        error_message: Human-readable error description
        error_category: Error category (TRANSIENT, PERMANENT, AUTH, etc.)
        exc: Exception object (will include traceback if provided)
        **context: Additional context fields (trace_id, media_id, status_code, etc.)

    Example:
        log_worker_error(
            logger,
            "Download failed",
            error_category="TRANSIENT",
            trace_id="evt_123",
            media_id="media_456",
            status_code=500,
            retry_count=2,
        )
    """
    # Build structured context
    extra = dict(context)

    if error_category:
        extra["error_category"] = error_category

    extra["error_message"] = error_message

    # Log with or without traceback
    if exc:
        logger.error(error_message, extra=extra, exc_info=exc)
    else:
        logger.error(error_message, extra=extra)
