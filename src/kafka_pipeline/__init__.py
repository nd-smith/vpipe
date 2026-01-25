# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
#
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""
Kafka Pipeline: Event-driven data processing.

This package implements the new Kafka-centric architecture for real-time
event processing, replacing the legacy polling-based Delta/Kusto pipeline.

Subpackages:
    kafka    - Kafka infrastructure (producers, consumers, retry handling)
    workers  - Consumer workers (event ingester, download worker, result processor)
    schemas  - Message schemas (Pydantic models for Kafka messages)

Architecture:
    events.raw → EventIngester → downloads.pending → DownloadWorker → downloads.results → ResultProcessor → Delta
                                        ↓ (on failure)
                                 downloads.retry.* → (delayed) → DownloadWorker
                                        ↓ (exhausted)
                                 downloads.dlq → DLQHandler

Dependencies:
    - core.*: Reusable components (auth, resilience, logging, etc.)
    - aiokafka: Async Kafka client
    - pydantic: Message schema validation
"""

from config.config import KafkaConfig

__version__ = "0.1.0"
__all__ = ["KafkaConfig"]
