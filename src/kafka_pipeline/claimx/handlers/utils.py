"""
Shared utilities for ClaimX event handlers.

Consolidates type conversion, timestamp handling, and timing utilities
used across handler modules.
"""

from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional, Union


def safe_int(value: Any) -> Optional[int]:
    """
    Safely convert to int, return None on failure.

    Args:
        value: Value to convert

    Returns:
        Integer value or None
    """
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def safe_int32(value: Any) -> Optional[int]:
    """
    Safely convert to 32-bit int, return None on failure or overflow.

    Use for columns that require Int32 in the Delta table schema.
    Values outside int32 range (-2147483648 to 2147483647) return None.

    Args:
        value: Value to convert

    Returns:
        Integer value within int32 range or None
    """
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
    """
    Safely convert to string, return None for empty.

    Args:
        value: Value to convert

    Returns:
        String value or None if empty
    """
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def safe_str_id(value: Any) -> Optional[str]:
    """
    Safely convert ID value to string.

    Handles integers and strings. Use for ID columns
    that are StringType in Delta schema (e.g., media_id, project_id).

    Args:
        value: ID value to convert

    Returns:
        String ID or None
    """
    if value is None:
        return None
    # Handle numeric values
    if isinstance(value, (int, float)):
        return str(int(value))
    s = str(value).strip()
    return s if s else None


def safe_bool(value: Any) -> Optional[bool]:
    """
    Safely convert to boolean.

    Args:
        value: Value to convert

    Returns:
        Boolean value or None
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    return bool(value)


def safe_float(value: Any) -> Optional[float]:
    """
    Safely convert to float, return None on failure.

    Args:
        value: Value to convert

    Returns:
        Float value or None
    """
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def safe_decimal_str(value: Any) -> Optional[str]:
    """
    Safely convert to decimal string for precise storage.

    Returns string representation to avoid float precision issues
    when storing in Delta tables.

    Args:
        value: Value to convert

    Returns:
        Decimal string or None
    """
    if value is None:
        return None
    try:
        return str(Decimal(str(value)))
    except (InvalidOperation, ValueError, TypeError):
        return None


def parse_timestamp(value: Any) -> Optional[str]:
    """
    Parse timestamp to ISO format string.

    Handles datetime objects, ISO strings, and Z-suffix normalization.

    Args:
        value: Timestamp value to parse

    Returns:
        ISO format string or None
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        s = value.strip()
        return s.replace("Z", "+00:00") if s else None
    return None


def now_iso() -> str:
    """
    Return current UTC time as ISO format string.

    Use for columns with StringType in Delta schema.

    Returns:
        Current UTC time in ISO format
    """
    return datetime.now(timezone.utc).isoformat()


def now_datetime() -> datetime:
    """
    Return current UTC time as datetime object.

    Use for columns with TimestampType in Delta schema
    (e.g., created_at, updated_at, last_enriched_at).

    Returns:
        Current UTC datetime object
    """
    return datetime.now(timezone.utc)


def today_date() -> date:
    """
    Return current UTC date as date object.

    Use for columns with DateType in Delta schema
    (e.g., created_date).

    Returns:
        Current UTC date object
    """
    return datetime.now(timezone.utc).date()


def parse_timestamp_dt(value: Any) -> Optional[datetime]:
    """
    Parse timestamp to datetime object.

    Handles datetime objects, ISO strings with timezone.
    Use for columns with TimestampType in Delta schema.

    Args:
        value: Timestamp value to parse

    Returns:
        datetime object or None
    """
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
    """
    Calculate milliseconds elapsed since start time.

    Args:
        start: Start time

    Returns:
        Elapsed milliseconds as integer
    """
    return int((datetime.now(timezone.utc) - start).total_seconds() * 1000)
