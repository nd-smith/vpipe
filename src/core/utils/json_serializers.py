# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
#
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""Shared JSON serialization utilities for type-safe JSON encoding."""

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


def _serialize_known_type(obj: Any) -> tuple[bool, Any]:
    """Try to serialize by known type. Returns (handled, result)."""
    if isinstance(obj, (datetime, date)):
        return True, obj.isoformat()
    if isinstance(obj, Decimal):
        return True, float(obj)
    if isinstance(obj, Path):
        return True, str(obj)
    return False, None


def json_serializer(obj: Any) -> Any:
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
    handled, result = _serialize_known_type(obj)
    if handled:
        return result
    if hasattr(obj, "value"):
        return obj.value
    if hasattr(obj, "__dict__"):
        return obj.__dict__
    return str(obj)


__all__ = ["json_serializer"]
