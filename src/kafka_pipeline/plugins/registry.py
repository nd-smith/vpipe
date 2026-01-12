"""Re-export plugins.shared.registry for backward compatibility."""

from kafka_pipeline.plugins.shared.registry import (
    ActionExecutor,
    OrchestratorResult,
    PluginOrchestrator,
    PluginRegistry,
    get_plugin_registry,
    register_plugin,
    reset_plugin_registry,
)

__all__ = [
    "ActionExecutor",
    "OrchestratorResult",
    "PluginOrchestrator",
    "PluginRegistry",
    "get_plugin_registry",
    "register_plugin",
    "reset_plugin_registry",
]
