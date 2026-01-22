# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
# 
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""
XACT retry handling.

Provides retry routing for failed tasks:
- RetryHandler: Download task retry handler (domain-specific)

Note: Unified retry scheduler is now in kafka_pipeline.common.retry.unified_scheduler
Note: DeltaRetryHandler is in kafka_pipeline.common.retry.delta_handler
"""

from kafka_pipeline.xact.retry.download_handler import RetryHandler

# Alias for backward compatibility with enrichment_worker imports
DownloadRetryHandler = RetryHandler

__all__ = [
    "RetryHandler",
    "DownloadRetryHandler",
]
