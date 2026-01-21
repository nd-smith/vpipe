"""Log formatters for JSON and console output."""

import json
import logging
import re
from datetime import datetime, date, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict

from core.logging.context import get_log_context


def _json_serializer(obj: Any) -> Any:
    """
    Type-safe JSON serializer for ADX compatibility.

    Ensures proper types for ADX schema instead of converting everything to strings:
    - datetime/date → ISO 8601 string
    - Decimal → float (for precise numeric fields)
    - Path → string
    - Enums → value
    - Everything else → string (fallback)

    This prevents numeric fields from becoming strings, which would break
    ADX aggregations and cause schema drift.

    Args:
        obj: Object to serialize

    Returns:
        JSON-serializable representation with proper types
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, Path):
        return str(obj)
    elif hasattr(obj, 'value'):
        # For enums
        return obj.value
    elif hasattr(obj, '__dict__'):
        # For objects with __dict__, serialize as dict
        return obj.__dict__
    else:
        # Fallback to string
        return str(obj)


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
        "processing_time_ms",
        "bytes_downloaded",
        "bytes_uploaded",
        "content_type",
        "status_code",
        # Resilience
        "circuit_state",
        # Storage
        "download_url",
        "blob_path",
        "blob_size",
        "destination_path",
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

    # Type mapping for numeric fields to ensure ADX compatibility
    # This prevents numeric fields from being serialized as strings
    NUMERIC_FIELDS = {
        # Timing fields (milliseconds as float/real for precision)
        "processing_time_ms": float,
        "duration_ms": float,
        "memory_mb": float,

        # Count fields (as int for smaller counts)
        "retry_count": int,
        "http_status": int,
        "status_code": int,
        "kafka_partition": int,
        "df_cols": int,
        "api_calls": int,

        # Large count fields (as int, ADX can handle large ints)
        "batch_size": int,
        "records_processed": int,
        "records_succeeded": int,
        "records_failed": int,
        "records_skipped": int,
        "rows_read": int,
        "rows_written": int,
        "rows_merged": int,
        "rows_inserted": int,
        "rows_updated": int,
        "df_rows": int,
        "query_length": int,
        "limit": int,

        # Byte counts (as int for large values)
        "blob_size": int,
        "bytes_uploaded": int,
        "bytes_downloaded": int,
        "kafka_offset": int,
    }

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

    def _ensure_type(self, field: str, value: Any) -> Any:
        """
        Ensure field has correct type for ADX compatibility.

        Converts values to their expected types (int, float) to prevent
        string serialization that would break ADX aggregations.

        Args:
            field: Field name
            value: Value to type-check

        Returns:
            Value with correct type, or None if conversion fails
        """
        if field not in self.NUMERIC_FIELDS or value is None:
            return value

        expected_type = self.NUMERIC_FIELDS[field]
        try:
            # Convert to expected type
            return expected_type(value)
        except (ValueError, TypeError):
            # If conversion fails, return None (ADX prefers NULL over invalid data)
            # Log this at DEBUG level to avoid log spam
            return None

    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON with type safety for ADX.

        Ensures numeric fields maintain proper types (int/float) instead of
        being converted to strings, enabling efficient ADX aggregations.
        """
        log_entry: Dict[str, Any] = {
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            + "Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }

        # Inject context variables
        log_context = get_log_context()
        if log_context["domain"]:
            log_entry["domain"] = log_context["domain"]
        if log_context["stage"]:
            log_entry["stage"] = log_context["stage"]
        if log_context["cycle_id"]:
            log_entry["cycle_id"] = log_context["cycle_id"]
        if log_context["worker_id"]:
            log_entry["worker_id"] = log_context["worker_id"]
        # Inject trace_id from context if available (can be overridden by extra)
        if log_context["trace_id"]:
            log_entry["trace_id"] = log_context["trace_id"]

        # Inject OpenTracing trace context if available
        if log_context.get("opentracing_trace_id"):
            log_entry["opentracing_trace_id"] = log_context["opentracing_trace_id"]
        if log_context.get("opentracing_span_id"):
            log_entry["opentracing_span_id"] = log_context["opentracing_span_id"]

        # Add source location for DEBUG/ERROR
        if record.levelno in (logging.DEBUG, logging.ERROR, logging.CRITICAL):
            log_entry["file"] = f"{record.filename}:{record.lineno}"

        # Extract extra fields with type validation AND sanitization
        # Type validation must happen first to ensure proper types for ADX
        for field in self.EXTRA_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                # First ensure correct type (prevents string coercion)
                typed_value = self._ensure_type(field, value)
                # Then sanitize URLs if needed
                log_entry[field] = self._sanitize_value(field, typed_value)

        # Include structured exception info for ADX querying
        if record.exc_info:
            exc_type, exc_value, exc_tb = record.exc_info
            log_entry["exception"] = {
                "type": exc_type.__name__ if exc_type else None,
                "message": str(exc_value) if exc_value else None,
                "stacktrace": self.formatException(record.exc_info)
            }

        # Use type-safe serializer instead of default=str
        # This prevents numeric fields from becoming strings
        return json.dumps(log_entry, default=_json_serializer, ensure_ascii=False)


class ConsoleFormatter(logging.Formatter):
    """
    Human-readable console formatter.

    Includes context when available.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format log record for console output."""
        log_context = get_log_context()

        # Build prefix
        parts = [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            record.levelname,
        ]

        if log_context["domain"]:
            parts.append(f"[{log_context['domain']}]")
        if log_context["stage"]:
            parts.append(f"[{log_context['stage']}]")

        prefix = " - ".join(parts)

        # Add batch_id and/or trace_id if present
        batch_id = getattr(record, "batch_id", None)
        trace_id = getattr(record, "trace_id", None) or log_context.get("trace_id")

        if batch_id and trace_id:
            return f"{prefix} - [batch:{batch_id}] [{trace_id[:8]}] {record.getMessage()}"
        elif batch_id:
            return f"{prefix} - [batch:{batch_id}] {record.getMessage()}"
        elif trace_id:
            return f"{prefix} - [{trace_id[:8]}] {record.getMessage()}"

        return f"{prefix} - {record.getMessage()}"
