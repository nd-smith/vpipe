# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
# 
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""
Plugin workers module.

Contains worker implementations for processing plugin-triggered messages.
"""

from kafka_pipeline.plugins.shared.workers.plugin_action_worker import (
    PluginActionWorker,
    WorkerConfig,
)

__all__ = [
    "PluginActionWorker",
    "WorkerConfig",
]
