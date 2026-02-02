"""
Task trigger plugin compatibility module.

Re-exports task trigger classes from shared.task_trigger for backward compatibility.
"""

from pipeline.plugins.shared.task_trigger import (
    TaskTriggerPlugin,
    create_task_trigger_plugin,
)

__all__ = [
    "TaskTriggerPlugin",
    "create_task_trigger_plugin",
]
