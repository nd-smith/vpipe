"""
Task-based trigger plugin.

POC plugin that triggers actions when tasks with specific task_ids are processed.

Example use case:
    - When a specific task template (task_id) is assigned or completed
    - Publish to a topic, call a webhook, or perform other actions

Usage:
    from kafka_pipeline.plugins import register_plugin
    from kafka_pipeline.plugins.shared.task_trigger import TaskTriggerPlugin

    # Register with specific task_id triggers
    register_plugin(TaskTriggerPlugin(config={
        "triggers": {
            456: {  # task_id (template ID)
                "name": "Photo Documentation Task",
                "on_assigned": {
                    "publish_to_topic": "task-456-assigned",
                },
                "on_completed": {
                    "publish_to_topic": "task-456-completed",
                    "webhook": "https://api.example.com/task-complete",
                },
            },
            789: {
                "name": "Damage Assessment Task",
                "on_completed": {
                    "publish_to_topic": "damage-assessment-complete",
                },
            },
        }
    }))
"""

import logging
from typing import Any, Dict, List, Optional

from kafka_pipeline.plugins.shared.base import (
    Plugin,
    PluginContext,
    PluginResult,
    PluginAction,
    ActionType,
    Domain,
    PipelineStage,
)


class TaskTriggerPlugin(Plugin):
    """
    Plugin that triggers actions based on task_id.

    Watches for CUSTOM_TASK_ASSIGNED and CUSTOM_TASK_COMPLETED events,
    checks if the task_id matches configured triggers, and executes
    the configured actions.
    """

    name = "task_trigger"
    description = "Trigger actions when specific tasks are assigned or completed"
    version = "1.0.0"

    # Only ClaimX domain, after enrichment (when we have task data)
    domains = [Domain.CLAIMX]
    stages = [PipelineStage.ENRICHMENT_COMPLETE]
    # event_types set dynamically in __init__ based on trigger config

    # Run early so other plugins see the results
    priority = 50

    default_config = {
        # Map of task_id -> trigger config
        # Can be loaded from YAML or set programmatically
        "triggers": {},
        # Whether to include full task data in published messages
        "include_task_data": True,
        # Whether to include project data in published messages
        "include_project_data": False,
    }

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize plugin with config and compute event_types dynamically.

        Only subscribes to event types that have actions configured:
        - CUSTOM_TASK_ASSIGNED if any trigger has on_assigned
        - CUSTOM_TASK_COMPLETED if any trigger has on_completed
        - Both if any trigger has on_any
        """
        super().__init__(config)

        # Determine which event types we actually need based on trigger config
        triggers = self.config.get("triggers", {})
        needs_assigned = False
        needs_completed = False

        for trigger_config in triggers.values():
            if trigger_config.get("on_any"):
                needs_assigned = True
                needs_completed = True
                break
            if trigger_config.get("on_assigned"):
                needs_assigned = True
            if trigger_config.get("on_completed"):
                needs_completed = True

        # Build event_types list based on what's configured
        self.event_types: List[str] = []
        if needs_assigned:
            self.event_types.append("CUSTOM_TASK_ASSIGNED")
        if needs_completed:
            self.event_types.append("CUSTOM_TASK_COMPLETED")

    async def execute(self, context: PluginContext) -> PluginResult:
        """
        Check if task matches a trigger and execute actions.

        Args:
v
        Returns:
            PluginResult with actions to execute
        """
        # Get task data from enriched entities
        task = context.get_first_task()
        if not task:
            return PluginResult.skip("No task data in context")

        task_id = task.get("task_id")
        if task_id is None:
            return PluginResult.skip("Task has no task_id")

        # Check if this task_id has a trigger configured
        triggers = self.config.get("triggers", {})
        trigger_config = triggers.get(task_id) or triggers.get(str(task_id))

        if not trigger_config:
            return PluginResult.skip(f"No trigger configured for task_id={task_id}")

        # Determine which trigger to use based on event type
        # First try specific event type config, then fall back to on_any
        event_type = context.event_type
        if event_type == "CUSTOM_TASK_ASSIGNED":
            action_config = trigger_config.get("on_assigned")
        elif event_type == "CUSTOM_TASK_COMPLETED":
            action_config = trigger_config.get("on_completed")
        else:
            action_config = None

        # Fall back to on_any if no specific config found
        if not action_config:
            action_config = trigger_config.get("on_any")

        if not action_config:
            return PluginResult.skip(
                f"No action configured for task_id={task_id}, event={event_type}"
            )

        # Build actions
        actions = self._build_actions(action_config, task, context, trigger_config)

        trigger_name = trigger_config.get("name", f"task_{task_id}")
        self._log(
            logging.INFO,
            "Task trigger matched",
            plugin_name=self.name,
            trigger_name=trigger_name,
            task_id=task_id,
            assignment_id=task.get("assignment_id"),
            task_name=task.get("task_name"),
            task_status=task.get("status"),
            event_id=context.event_id,
            event_type=event_type,
            project_id=context.project_id,
            action_count=len(actions),
        )

        return PluginResult(
            success=True,
            actions=actions,
            message=f"Triggered: {trigger_name} ({event_type})",
        )

    def _build_actions(
        self,
        action_config: Dict[str, Any],
        task: Dict[str, Any],
        context: PluginContext,
        trigger_config: Dict[str, Any],
    ) -> List[PluginAction]:
        """
        Build action list from trigger configuration.

        Args:
            action_config: Action configuration (on_assigned/on_completed)
            task: Task data from enrichment
            context: Plugin context
            trigger_config: Full trigger configuration

        Returns:
            List of PluginAction to execute
        """
        actions = []

        # Build payload for publish actions
        payload = self._build_payload(task, context, trigger_config)

        # Publish to topic
        if "publish_to_topic" in action_config:
            topic = action_config["publish_to_topic"]
            actions.append(PluginAction(
                action_type=ActionType.PUBLISH_TO_TOPIC,
                params={
                    "topic": topic,
                    "payload": payload,
                    "headers": {
                        "x-trigger-name": trigger_config.get("name", ""),
                        "x-task-id": str(task.get("task_id", "")),
                        "x-event-type": context.event_type or "",
                    },
                }
            ))

        # HTTP webhook
        if "webhook" in action_config:
            webhook_config = action_config["webhook"]
            if isinstance(webhook_config, str):
                webhook_config = {"url": webhook_config}

            actions.append(PluginAction(
                action_type=ActionType.HTTP_WEBHOOK,
                params={
                    "url": webhook_config.get("url"),
                    "method": webhook_config.get("method", "POST"),
                    "body": payload,
                    "headers": webhook_config.get("headers", {}),
                }
            ))

        # Log action
        if "log" in action_config:
            log_config = action_config["log"]
            if isinstance(log_config, str):
                log_config = {"message": log_config}

            actions.append(PluginAction(
                action_type=ActionType.LOG,
                params={
                    "level": log_config.get("level", "info"),
                    "message": log_config.get("message", f"Task trigger: {task.get('task_id')}"),
                }
            ))

        # Custom actions (for extension)
        if "custom" in action_config:
            for custom_action in action_config.get("custom", []):
                actions.append(PluginAction(
                    action_type=ActionType(custom_action.get("type", "log")),
                    params=custom_action.get("params", {}),
                ))

        return actions

    def _build_payload(
        self,
        task: Dict[str, Any],
        context: PluginContext,
        trigger_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Build payload for publish/webhook actions.

        Args:
            task: Task data
            context: Plugin context
            trigger_config: Trigger configuration

        Returns:
            Payload dict
        """
        payload = {
            "trigger_name": trigger_config.get("name"),
            "event_id": context.event_id,
            "event_type": context.event_type,
            "project_id": context.project_id,
            "task_id": task.get("task_id"),
            "assignment_id": task.get("assignment_id"),
            "task_name": task.get("task_name"),
            "task_status": task.get("status"),
            "timestamp": context.timestamp.isoformat(),
        }

        # Optionally include full task data
        if self.config.get("include_task_data"):
            payload["task"] = task

        # Optionally include project data
        if self.config.get("include_project_data"):
            entities = context.get_claimx_entities()
            if entities and entities.projects:
                payload["project"] = entities.projects[0]

        return payload


# Convenience function to create and configure the plugin
def create_task_trigger_plugin(
    triggers: Dict[int, Dict[str, Any]],
    include_task_data: bool = True,
    include_project_data: bool = False,
) -> TaskTriggerPlugin:
    """
    Create a TaskTriggerPlugin with the given triggers.

    Args:
        triggers: Map of task_id -> trigger config
        include_task_data: Include full task data in payloads
        include_project_data: Include project data in payloads

    Returns:
        Configured TaskTriggerPlugin instance

    Example:
        plugin = create_task_trigger_plugin({
            456: {
                "name": "Photo Documentation",
                "on_completed": {
                    "publish_to_topic": "photo-tasks-completed",
                    "webhook": "https://api.example.com/notify",
                }
            }
        })
    """
    return TaskTriggerPlugin(config={
        "triggers": triggers,
        "include_task_data": include_task_data,
        "include_project_data": include_project_data,
    })
