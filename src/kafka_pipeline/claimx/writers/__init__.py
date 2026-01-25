# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
#
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""ClaimX Delta table writers.

This module contains Delta table writers for:
- ClaimX events table
- ClaimX entity tables (7 entity types)
"""

from kafka_pipeline.claimx.writers.delta_entities import (
    ClaimXEntityWriter,
    MERGE_KEYS,
)
from kafka_pipeline.claimx.writers.delta_events import ClaimXEventsDeltaWriter

__all__ = [
    "ClaimXEventsDeltaWriter",
    "ClaimXEntityWriter",
    "MERGE_KEYS",
]
