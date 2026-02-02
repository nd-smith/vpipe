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
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, Path):
        return str(obj)
    elif hasattr(obj, "value"):
        # For enums
        return obj.value
    elif hasattr(obj, "__dict__"):
        # For objects with __dict__, serialize as dict
        return obj.__dict__
    else:
        # Fallback to string
        return str(obj)


__all__ = ["json_serializer"]
