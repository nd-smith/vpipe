"""
Shared plugin framework components.

Core functionality used across all plugins.
"""

from kafka_pipeline.plugins.shared.base import (
    ActionType,
    Domain,
    PipelineStage,
    Plugin,
    PluginAction,
    PluginContext,
    PluginResult,
)
from kafka_pipeline.plugins.shared.connections import (
    ConnectionConfig,
    ConnectionManager,
)
from kafka_pipeline.plugins.shared.enrichment import (
    EnrichmentContext,
    EnrichmentHandler,
    EnrichmentPipeline,
    EnrichmentResult,
)
from kafka_pipeline.plugins.shared.loader import load_plugins_from_directory
from kafka_pipeline.plugins.shared.registry import PluginRegistry
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
    "load_plugins_from_directory",
    "ConnectionManager",
    "ConnectionConfig",
    "EnrichmentHandler",
    "EnrichmentContext",
    "EnrichmentResult",
    "EnrichmentPipeline",
    "TaskTriggerPlugin",
]
