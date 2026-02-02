"""
Shared plugin framework components.

Core functionality used across all plugins.
"""

from pipeline.plugins.shared.base import (
    ActionType,
    Domain,
    PipelineStage,
    Plugin,
    PluginAction,
    PluginContext,
    PluginResult,
)
from pipeline.plugins.shared.connections import (
    ConnectionConfig,
    ConnectionManager,
)
from pipeline.plugins.shared.enrichment import (
    EnrichmentContext,
    EnrichmentHandler,
    EnrichmentPipeline,
    EnrichmentResult,
)
from pipeline.plugins.shared.loader import load_plugins_from_directory
from pipeline.plugins.shared.registry import PluginRegistry
from pipeline.plugins.shared.task_trigger import TaskTriggerPlugin

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
