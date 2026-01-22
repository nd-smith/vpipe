# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
# 
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

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
