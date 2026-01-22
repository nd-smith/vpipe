# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
# 
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""
XACT dead-letter queue (DLQ) handling.

Provides DLQ message management for XACT pipeline:
- DLQHandler: Manual DLQ review and replay handler
"""

from kafka_pipeline.xact.dlq.handler import DLQHandler

__all__ = [
    "DLQHandler",
]
