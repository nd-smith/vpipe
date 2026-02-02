"""
Plugin registry compatibility module.

Re-exports plugin registry classes from shared.registry for backward compatibility.
"""

from kafka_pipeline.plugins.shared.registry import (
    ActionExecutor,
    OrchestratorResult,
    PluginOrchestrator,
    PluginRegistry,
    reset_plugin_registry,
)

__all__ = [
    "ActionExecutor",
    "OrchestratorResult",
    "PluginOrchestrator",
    "PluginRegistry",
    "reset_plugin_registry",
]
