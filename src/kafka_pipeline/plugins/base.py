"""Re-export plugins.shared.base for backward compatibility."""

from kafka_pipeline.plugins.shared.base import (
    ActionType,
    Domain,
    PipelineStage,
    Plugin,
    PluginAction,
    PluginContext,
    PluginResult,
)

__all__ = [
    "ActionType",
    "Domain",
    "PipelineStage",
    "Plugin",
    "PluginAction",
    "PluginContext",
    "PluginResult",
]
