# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
# 
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""
Shared plugin framework components.

Core functionality used across all plugins.
"""

from kafka_pipeline.plugins.shared.base import (
    Plugin,
    PluginContext,
    PluginResult,
    PluginAction,
    ActionType,
    Domain,
    PipelineStage,
)
from kafka_pipeline.plugins.shared.registry import (
    PluginRegistry,
    get_plugin_registry,
    register_plugin,
)
from kafka_pipeline.plugins.shared.loader import load_plugins_from_directory
from kafka_pipeline.plugins.shared.connections import ConnectionManager, ConnectionConfig
from kafka_pipeline.plugins.shared.enrichment import (
    EnrichmentHandler,
    EnrichmentContext,
    EnrichmentResult,
    EnrichmentPipeline,
)
from kafka_pipeline.plugins.shared.task_trigger import TaskTriggerPlugin

__all__ = [
    "Plugin",
    "PluginContext",
    "PluginResult",
    "PluginAction",
    "ActionType",
    "Domain",
    "PipelineStage",
    "PluginRegistry",
    "get_plugin_registry",
    "register_plugin",
    "load_plugins_from_directory",
    "ConnectionManager",
    "ConnectionConfig",
    "EnrichmentHandler",
    "EnrichmentContext",
    "EnrichmentResult",
    "EnrichmentPipeline",
    "TaskTriggerPlugin",
]
