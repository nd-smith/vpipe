"""
XACT Status Trigger Plugin

Plugin that triggers actions based on XACT event status changes.
Supports multiple action types: Kafka publishing, HTTP webhooks, and logging.

Use Cases:
- Publish to Kafka when assignment reaches "paymentProcessorAssigned" status
- Call webhook when "estimateCreated"
- Log important status transitions
- Combine multiple actions for complex workflows

Configuration in config.yaml:
  triggers:
    - status: "paymentProcessorAssigned"
      publish_to_topic: "xact.notifications.payment-processor-assigned"
      webhook: "https://api.example.com/payment-assigned"
      log:
        level: info
        message: "Payment processor assigned"
"""

from typing import Any, Dict, List
from datetime import datetime
import logging

from kafka_pipeline.plugins.shared.base import (
    Plugin,
    PluginContext,
    PluginResult,
    PluginAction,
    ActionType,
    PipelineStage,
    Domain,
)
from core.logging.setup import get_logger

logger = get_logger(__name__)


class XACTStatusTriggerPlugin(Plugin):
    """
    Plugin that triggers multiple action types based on XACT event status.

    Supports:
    - Kafka topic publishing
    - HTTP webhooks
    - Log actions

    Executes during ENRICHMENT_COMPLETE stage for XACT domain events.
    """

    name = "xact_status_trigger"
    version = "1.0.0"
    description = "Trigger actions (Kafka, webhooks, logs) based on XACT status changes"

    # Plugin metadata - which domains and stages this plugin applies to
    domains = [Domain.XACT]
    stages = [PipelineStage.ENRICHMENT_COMPLETE]
    event_types = ["*"]  # All event types
    priority = 100  # Standard priority

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the plugin with configuration.

        Args:
            config: Dictionary containing trigger mappings
                Format: {
                    "triggers": [
                        {
                            "status": "paymentProcessorAssigned",
                            "publish_to_topic": "xact.status.ppa",
                            "webhook": {
                                "url": "https://api.example.com/notify",
                                "method": "POST"
                            },
                            "log": {
                                "level": "info",
                                "message": "Payment processor assigned"
                            }
                        }
                    ]
                }
        """
        super().__init__(config)

        # Build lookup map: status -> trigger config for fast matching
        self.triggers_by_status = {}
        triggers = self.config.get("triggers", [])

        for trigger in triggers:
            status = trigger.get("status")
            if status:
                self.triggers_by_status[status] = trigger
                logger.info(
                    f"Registered XACT status trigger: {status}",
                    extra={"plugin": self.name, "status": status},
                )

        logger.info(
            f"XACTStatusTriggerPlugin initialized with {len(self.triggers_by_status)} trigger(s)",
            extra={"plugin": self.name, "trigger_count": len(self.triggers_by_status)},
        )

    async def execute(self, context: PluginContext) -> PluginResult:
        """
        Execute the plugin logic - check status and trigger actions if matched.

        Args:
            context: Plugin execution context containing event data

        Returns:
            PluginResult with actions to execute (if any)
        """
        # Extract status from context data
        status_subtype = context.data.get("status_subtype")

        if not status_subtype:
            logger.debug(
                "No status_subtype in context, skipping",
                extra={"plugin": self.name, "event_id": context.event_id},
            )
            return PluginResult(success=True, actions=[])

        # Check if this status has a trigger configured
        trigger_config = self.triggers_by_status.get(status_subtype)

        if not trigger_config:
            logger.debug(
                f"No trigger configured for status: {status_subtype}",
                extra={"plugin": self.name, "event_id": context.event_id, "status": status_subtype},
            )
            return PluginResult(success=True, actions=[])

        # Build actions based on trigger configuration
        actions = self._build_actions(trigger_config, context)

        logger.info(
            f"Status trigger matched: {status_subtype}",
            extra={
                "plugin": self.name,
                "event_id": context.event_id,
                "status": status_subtype,
                "trace_id": context.data.get("trace_id"),
                "assignment_id": context.data.get("assignment_id"),
                "action_count": len(actions),
            },
        )

        return PluginResult(
            success=True, actions=actions, message=f"Triggered actions for status {status_subtype}"
        )

    def _build_actions(
        self,
        trigger_config: Dict[str, Any],
        context: PluginContext,
    ) -> List[PluginAction]:
        """
        Build action list from trigger configuration.

        Supports multiple action types:
        - publish_to_topic: Publish to Kafka topic
        - webhook: Call HTTP endpoint
        - log: Write log message

        Args:
            trigger_config: Trigger configuration with action definitions
            context: Plugin execution context

        Returns:
            List of PluginAction to execute
        """
        actions = []

        # Build payload for publish/webhook actions
        payload = self._build_notification_payload(context)

        # 1. Kafka Topic Publishing
        if "publish_to_topic" in trigger_config:
            topic = trigger_config["publish_to_topic"]
            actions.append(
                PluginAction(
                    action_type=ActionType.PUBLISH_TO_TOPIC,
                    params={
                        "topic": topic,
                        "key": context.event_id,
                        "payload": payload,
                        "headers": {
                            "x-plugin-name": self.name,
                            "x-status": context.data.get("status_subtype", ""),
                            "x-event-type": context.event_type or "",
                        },
                    },
                )
            )
            logger.debug(f"Added Kafka publish action: {topic}")

        # 2. HTTP Webhook
        if "webhook" in trigger_config:
            webhook_config = trigger_config["webhook"]

            # Support both string URL and dict config
            if isinstance(webhook_config, str):
                webhook_config = {"url": webhook_config}

            actions.append(
                PluginAction(
                    action_type=ActionType.HTTP_WEBHOOK,
                    params={
                        "url": webhook_config.get("url"),
                        "method": webhook_config.get("method", "POST"),
                        "body": payload,
                        "headers": webhook_config.get("headers", {}),
                        "timeout": webhook_config.get("timeout", 30),
                    },
                )
            )
            logger.debug(f"Added webhook action: {webhook_config.get('url')}")

        # 3. Log Action
        if "log" in trigger_config:
            log_config = trigger_config["log"]

            # Support both string message and dict config
            if isinstance(log_config, str):
                log_config = {"message": log_config}

            # Format message with context variables
            message = log_config.get("message", "Status trigger: {status}")
            message = message.format(
                status=context.data.get("status_subtype"),
                event_id=context.event_id,
                trace_id=context.data.get("trace_id"),
                assignment_id=context.data.get("assignment_id"),
                task_status=context.data.get("status_subtype"),
            )

            actions.append(
                PluginAction(
                    action_type=ActionType.LOG,
                    params={
                        "level": log_config.get("level", "info"),
                        "message": message,
                    },
                )
            )
            logger.debug(f"Added log action: {message[:50]}")

        return actions

    def _build_notification_payload(self, context: PluginContext) -> Dict[str, Any]:
        """
        Build the notification payload for Kafka/webhook actions.

        Args:
            context: Plugin execution context

        Returns:
            Dictionary containing notification data
        """
        return {
            "event_id": context.event_id,
            "trace_id": context.data.get("trace_id"),
            "event_type": context.event_type,
            "status_subtype": context.data.get("status_subtype"),
            "assignment_id": context.data.get("assignment_id"),
            "estimate_version": context.data.get("estimate_version"),
            "attachment_count": context.data.get("attachment_count", 0),
            "timestamp": context.timestamp.isoformat(),
            "domain": context.domain.value,
            "stage": context.stage.value,
            "metadata": {
                "plugin_name": self.name,
                "plugin_version": self.version,
                "triggered_at": datetime.utcnow().isoformat(),
            },
        }

    def should_run(self, context: PluginContext) -> bool:
        """
        Determine if this plugin should run for the given context.

        The base class already checks domain, stage, and event_type.
        We add an additional check for configured triggers.

        Args:
            context: Plugin execution context

        Returns:
            True if plugin should run, False otherwise
        """
        # Use base class logic first (domain, stage, event_type)
        if not super().should_run(context):
            return False

        # Only run if we have at least one trigger configured
        if not self.triggers_by_status:
            logger.debug("No triggers configured, plugin will not run", extra={"plugin": self.name})
            return False

        return True


# Export the plugin class
__all__ = ["XACTStatusTriggerPlugin"]
