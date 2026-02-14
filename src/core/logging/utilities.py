"""Logging utility functions."""

import logging
from datetime import datetime, timezone
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
    offset_start_ts: int | None = None,
    offset_end_ts: int | None = None,
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
    total_processed = succeeded + failed + skipped

    # Build offset suffix if both timestamps are provided
    offset_suffix = ""
    if offset_start_ts is not None and offset_end_ts is not None:
        start = datetime.fromtimestamp(offset_start_ts / 1000, tz=timezone.utc)
        end = datetime.fromtimestamp(offset_end_ts / 1000, tz=timezone.utc)
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
    enable_file_logging: bool,
    enable_eventhub_logging: bool,
) -> str:
    """
    Determine log output mode based on configuration flags.

    Returns:
        String describing where logs are being sent: "stdout", "file",
        "eventhub", "file+eventhub", etc.
    """
    if log_to_stdout:
        return "stdout"

    modes = []
    if enable_file_logging:
        modes.append("file")
    if enable_eventhub_logging:
        modes.append("eventhub")

    return "+".join(modes) if modes else "none"


def detect_log_output_mode() -> str:
    """
    Detect current log output mode by inspecting active logging handlers.

    Returns:
        String describing where logs are being sent: "stdout", "file",
        "eventhub", "file+eventhub", etc.
    """
    import logging

    root_logger = logging.getLogger()
    modes = []

    has_file = False
    has_eventhub = False
    has_console_only = False

    for handler in root_logger.handlers:
        if isinstance(handler, logging.FileHandler):
            has_file = True
        elif hasattr(handler, "__class__") and "EventHub" in handler.__class__.__name__:
            has_eventhub = True
        elif isinstance(handler, logging.StreamHandler) and len(root_logger.handlers) == 1:
            has_console_only = True

    if has_console_only:
        return "stdout"

    if has_file:
        modes.append("file")
    if has_eventhub:
        modes.append("eventhub")

    return "+".join(modes) if modes else "console"


def log_startup_banner(
    logger: logging.Logger,
    worker_name: str,
    instance_id: str | None = None,
    domain: str | None = None,
    input_topic: str | None = None,
    output_topic: str | None = None,
    health_port: int | None = None,
    version: str | None = None,
    log_output_mode: str | None = None,
) -> None:
    """
    Log startup banner with worker configuration.

    Args:
        logger: Logger instance
        worker_name: Worker name (e.g., "ClaimX Enrichment Worker")
        instance_id: Instance identifier (e.g., "enrichment_worker-0")
        domain: Domain (e.g., "claimx")
        input_topic: Input topic name
        output_topic: Output topic name
        health_port: Health check server port
        version: Optional version string
        log_output_mode: Log output mode (e.g., "file+eventhub", "stdout")

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

    lines = [
        "",
        separator,
        worker_name,
    ]

    if version:
        lines.append(f"Version: {version}")

    lines.append(separator)

    if instance_id:
        lines.append(f"Instance:     {instance_id}")
    if domain:
        lines.append(f"Domain:       {domain}")
    if input_topic:
        lines.append(f"Input Topic:  {input_topic}")
    if output_topic:
        lines.append(f"Output Topic: {output_topic}")
    if health_port:
        lines.append(f"Health:       http://localhost:{health_port}")
    if log_output_mode:
        lines.append(f"Log Output:   {log_output_mode}")

    lines.append(separator)
    lines.append("")

    banner = "\n".join(lines)
    logger.info(banner)


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
