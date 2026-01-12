"""
Plugin framework for injecting business logic into the pipeline.

Provides a lightweight mechanism for reacting to pipeline events
based on configurable conditions and executing actions.

For backward compatibility, this module re-exports everything from
kafka_pipeline.plugins.shared. New code should import directly from
kafka_pipeline.plugins.shared or from plugin-specific modules.
"""

# Re-export from shared for backward compatibility
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
]
