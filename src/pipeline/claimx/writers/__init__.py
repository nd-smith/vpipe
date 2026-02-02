"""ClaimX Delta table writers.

This module contains Delta table writers for:
- ClaimX events table
- ClaimX entity tables (7 entity types)
"""

from pipeline.claimx.writers.delta_entities import (
    MERGE_KEYS,
    ClaimXEntityWriter,
)
from pipeline.claimx.writers.delta_events import ClaimXEventsDeltaWriter

__all__ = [
    "ClaimXEventsDeltaWriter",
    "ClaimXEntityWriter",
    "MERGE_KEYS",
]
