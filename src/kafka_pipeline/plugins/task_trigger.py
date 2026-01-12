"""Re-export plugins.shared.task_trigger for backward compatibility."""

from kafka_pipeline.plugins.shared.task_trigger import (
    TaskTriggerPlugin,
    create_task_trigger_plugin,
)

__all__ = [
    "TaskTriggerPlugin",
    "create_task_trigger_plugin",
]
