"""
Shared utilities for ClaimX event handlers.

Consolidates type conversion, timestamp handling, and timing utilities
used across handler modules.
"""

from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from core.logging import get_logger

logger = get_logger(__name__)


def safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        logger.warning(
            "Type conversion failed",
            extra={"value": str(value)[:100], "target_type": "int"},
        )
        return None


def safe_str(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def safe_str_id(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return str(int(value))
    s = str(value).strip()
    return s if s else None


def safe_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    return bool(value)


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        logger.warning(
            "Type conversion failed",
            extra={"value": str(value)[:100], "target_type": "float"},
        )
        return None


def safe_decimal_str(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return str(Decimal(str(value)))
    except (InvalidOperation, ValueError, TypeError):
        logger.warning(
            "Type conversion failed",
            extra={"value": str(value)[:100], "target_type": "decimal"},
        )
        return None


def parse_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        s = value.strip()
        return s.replace("Z", "+00:00") if s else None
    return None


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def now_datetime() -> datetime:
    return datetime.now(UTC)


def today_date() -> date:
    return datetime.now(UTC).date()


def parse_timestamp_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            # Handle Z suffix
            s = s.replace("Z", "+00:00")
            return datetime.fromisoformat(s)
        except ValueError:
            return None
    return None


def elapsed_ms(start: datetime) -> int:
    return int((datetime.now(UTC) - start).total_seconds() * 1000)


class BaseTransformer:
    """
    Base transformer class providing common metadata injection.

    Eliminates boilerplate timestamp injection across all transformers.
    """

    @staticmethod
    def inject_metadata(
        row: dict[str, Any],
        event_id: str,
        include_last_enriched: bool = True,
    ) -> dict[str, Any]:
        """
        Inject common metadata fields into a row dictionary.

        Args:
            row: The row dictionary to inject metadata into
            event_id: Event ID for traceability
            include_last_enriched: Whether to include last_enriched_at field

        Returns:
            The row dictionary with metadata fields injected
        """
        now = now_datetime()
        row["event_id"] = event_id
        row["created_at"] = now
        row["updated_at"] = now
        if include_last_enriched:
            row["last_enriched_at"] = now
        return row
