"""Log formatters for JSON and console output."""

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict

from core.logging.context import get_log_context


class JSONFormatter(logging.Formatter):
    """
    JSON log formatter with context injection.

    Produces one JSON object per line for easy parsing with jq/grep.
    Sanitizes URLs to remove sensitive tokens before logging.
    """

    # Fields to extract from LogRecord extras
    EXTRA_FIELDS = [
        # Correlation and tracing
        "trace_id",
        "correlation_id",
        "batch_id",
        "duration_ms",
        # HTTP
        "http_status",
        "http_method",
        "http_url",
        # Errors
        "error_category",
        "error_message",
        "error_code",
        "error",
        "error_type",
        "api_errors",
        # Processing metrics
        "records_processed",
        "records_succeeded",
        "records_failed",
        "records_skipped",
        "batch_size",
        "retry_count",
        # Resilience
        "circuit_state",
        # Storage
        "download_url",
        "blob_path",
        "blob_size",
        # Operation tracking
        "operation",
        "table",
        "primary_keys",
        "rows_read",
        "rows_written",
        "rows_merged",
        "rows_inserted",
        "rows_updated",
        "columns",
        "limit",
        # Identifiers
        "event_id",
        "event_type",
        "project_id",
        "media_id",
        "assignment_id",
        "status_subtype",
        "resource",
        # Memory tracking
        "checkpoint",
        "memory_mb",
        "df_rows",
        "df_cols",
        # API tracking
        "api_endpoint",
        "api_method",
        "api_calls",
        # Kafka (to be expanded in WP-109)
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        # KQL/Kusto
        "database",
        "query",
        "query_length",
    ]

    # Fields that contain URLs and should be sanitized
    URL_FIELDS = ["download_url", "blob_path", "url", "http_url"]

    # Pattern to match sensitive query parameters
    SENSITIVE_PARAMS_PATTERN = re.compile(
        r"([?&])(sig|token|key|secret|password|auth)=[^&]*",
        re.IGNORECASE,
    )

    def _sanitize_url(self, url: str) -> str:
        """
        Basic URL sanitization to remove sensitive query parameters.

        This is a simple implementation. Full sanitization is in core.security
        when available (WP-113).

        Args:
            url: URL to sanitize

        Returns:
            Sanitized URL with sensitive params replaced with [REDACTED]
        """
        return self.SENSITIVE_PARAMS_PATTERN.sub(r"\1\2=[REDACTED]", url)

    def _sanitize_value(self, key: str, value: Any) -> Any:
        """Sanitize value if it's a URL field."""
        if key in self.URL_FIELDS and isinstance(value, str):
            return self._sanitize_url(value)
        return value

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON with sanitized URLs."""
        log_entry: Dict[str, Any] = {
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            + "Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }

        # Inject context variables
        ctx = get_log_context()
        if ctx["domain"]:
            log_entry["domain"] = ctx["domain"]
        if ctx["stage"]:
            log_entry["stage"] = ctx["stage"]
        if ctx["cycle_id"]:
            log_entry["cycle_id"] = ctx["cycle_id"]
        if ctx["worker_id"]:
            log_entry["worker_id"] = ctx["worker_id"]
        # Inject trace_id from context if available (can be overridden by extra)
        if ctx["trace_id"]:
            log_entry["trace_id"] = ctx["trace_id"]

        # Add source location for DEBUG/ERROR
        if record.levelno in (logging.DEBUG, logging.ERROR, logging.CRITICAL):
            log_entry["file"] = f"{record.filename}:{record.lineno}"

        # Extract extra fields with sanitization
        for field in self.EXTRA_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                log_entry[field] = self._sanitize_value(field, value)

        # Include exception info
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str, ensure_ascii=False)


class ConsoleFormatter(logging.Formatter):
    """
    Human-readable console formatter.

    Includes context when available.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format log record for console output."""
        ctx = get_log_context()

        # Build prefix
        parts = [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            record.levelname,
        ]

        if ctx["domain"]:
            parts.append(f"[{ctx['domain']}]")
        if ctx["stage"]:
            parts.append(f"[{ctx['stage']}]")

        prefix = " - ".join(parts)

        # Add batch_id and/or trace_id if present
        batch_id = getattr(record, "batch_id", None)
        trace_id = getattr(record, "trace_id", None) or ctx.get("trace_id")

        if batch_id and trace_id:
            return f"{prefix} - [batch:{batch_id}] [{trace_id[:8]}] {record.getMessage()}"
        elif batch_id:
            return f"{prefix} - [batch:{batch_id}] {record.getMessage()}"
        elif trace_id:
            return f"{prefix} - [{trace_id[:8]}] {record.getMessage()}"

        return f"{prefix} - {record.getMessage()}"
