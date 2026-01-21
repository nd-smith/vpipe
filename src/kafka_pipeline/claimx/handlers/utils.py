"""
Shared utilities for ClaimX event handlers.

Consolidates type conversion, timestamp handling, and timing utilities
used across handler modules.
"""

from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional, Union


def safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def safe_int32(value: Any) -> Optional[int]:
    # Returns None for values outside int32 range (-2147483648 to 2147483647)
    if value is None:
        return None
    try:
        v = int(value)
        # Check int32 bounds
        if -2147483648 <= v <= 2147483647:
            return v
        return None
    except (ValueError, TypeError):
        return None


def safe_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def safe_str_id(value: Any) -> Optional[str]:
    if value is None:
        return None
    # Handle numeric values
    if isinstance(value, (int, float)):
        return str(int(value))
    s = str(value).strip()
    return s if s else None


def safe_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    return bool(value)


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def safe_decimal_str(value: Any) -> Optional[str]:
    # Returns decimal string to avoid float precision issues
    if value is None:
        return None
    try:
        return str(Decimal(str(value)))
    except (InvalidOperation, ValueError, TypeError):
        return None


def parse_timestamp(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        s = value.strip()
        return s.replace("Z", "+00:00") if s else None
    return None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def now_datetime() -> datetime:
    return datetime.now(timezone.utc)


def today_date() -> date:
    return datetime.now(timezone.utc).date()


def parse_timestamp_dt(value: Any) -> Optional[datetime]:
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
    return int((datetime.now(timezone.utc) - start).total_seconds() * 1000)


class BaseTransformer:
    """
    Base transformer class providing common metadata injection.

    Eliminates boilerplate timestamp injection across all transformers.
    """

    @staticmethod
    def inject_metadata(
        row: Dict[str, Any],
        event_id: str,
        include_last_enriched: bool = True,
    ) -> Dict[str, Any]:
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
