"""
Xact writers package.

Contains Delta table writers for xact domain:
- delta_events: Writer for xact events table
- delta_inventory: Writer for xact inventory table
"""

from kafka_pipeline.xact.writers.delta_events import DeltaEventsWriter
from kafka_pipeline.xact.writers.delta_inventory import (
    DeltaFailedAttachmentsWriter,
    DeltaInventoryWriter,
)

__all__ = [
    "DeltaEventsWriter",
    "DeltaInventoryWriter",
    "DeltaFailedAttachmentsWriter",
]
