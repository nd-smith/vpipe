"""
Xact writers package.

Contains Delta table writers for verisk domain:
- delta_events: Writer for verisk events table
- delta_inventory: Writer for xact inventory table
"""

from kafka_pipeline.verisk.writers.delta_events import DeltaEventsWriter
from kafka_pipeline.verisk.writers.delta_inventory import (
    DeltaFailedAttachmentsWriter,
    DeltaInventoryWriter,
)

__all__ = [
    "DeltaEventsWriter",
    "DeltaInventoryWriter",
    "DeltaFailedAttachmentsWriter",
]
