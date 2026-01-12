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
