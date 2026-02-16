"""
Plugin registry and orchestration.

Manages plugin registration, lookup, and execution.
"""

import logging
from collections import defaultdict
from typing import Callable, Optional

from pipeline.plugins.shared.base import (
    ActionType,
    PipelineStage,
    Plugin,
    PluginAction,
    PluginContext,
    PluginResult,
)

logger = logging.getLogger(__name__)


class PluginRegistry:
    """
    Registry for managing plugins.

    Plugins are indexed by stage for efficient lookup during execution.
    """

    def __init__(self):
        self._plugins: dict[str, Plugin] = {}
        self._plugins_by_stage: dict[PipelineStage, list[Plugin]] = defaultdict(list)

        logger.debug("PluginRegistry initialized")

    def register(self, plugin: Plugin) -> None:
        """
        Register a plugin.

        Args:
            plugin: Plugin instance to register
        """
        if plugin.name in self._plugins:
            logger.warning(
                "Overwriting plugin registration",
                extra={"plugin_name": plugin.name},
            )

        self._plugins[plugin.name] = plugin

        # Index by stage for efficient lookup
        if plugin.stages:
            for stage in plugin.stages:
                self._plugins_by_stage[stage].append(plugin)
                # Keep sorted by priority
                self._plugins_by_stage[stage].sort(key=lambda p: p.priority)
        else:
            # No stage filter = runs at all stages
            for stage in PipelineStage:
                self._plugins_by_stage[stage].append(plugin)
                self._plugins_by_stage[stage].sort(key=lambda p: p.priority)

        logger.info(
            "Registered plugin",
            extra={
                "plugin_name": plugin.name,
                "domains": ([d.value for d in plugin.domains] if plugin.domains else ["all"]),
                "stages": ([s.value for s in plugin.stages] if plugin.stages else ["all"]),
                "event_types": plugin.event_types or ["all"],
                "priority": plugin.priority,
            },
        )

    def unregister(self, plugin_name: str) -> Plugin | None:
        """
        Unregister a plugin by name.

        Args:
            plugin_name: Name of plugin to remove

        Returns:
            Removed plugin or None if not found
        """
        plugin = self._plugins.pop(plugin_name, None)
        if plugin:
            for stage_plugins in self._plugins_by_stage.values():
                if plugin in stage_plugins:
                    stage_plugins.remove(plugin)
            logger.info(
                "Unregistered plugin",
                extra={"plugin_name": plugin_name},
            )
        return plugin

    def get_plugin(self, name: str) -> Plugin | None:
        """Get plugin by name."""
        return self._plugins.get(name)

    def get_plugins_for_stage(self, stage: PipelineStage) -> list[Plugin]:
        """Get all plugins that run at a given stage."""
        return self._plugins_by_stage.get(stage, [])

    def list_plugins(self) -> list[Plugin]:
        """Get all registered plugins."""
        return list(self._plugins.values())

    def clear(self) -> None:
        """Remove all plugins."""
        self._plugins.clear()
        self._plugins_by_stage.clear()


class PluginOrchestrator:
    """
    Orchestrates plugin execution.

    Handles running plugins for a stage and executing their actions.
    """

    def __init__(
        self,
        registry: PluginRegistry,
        action_executor: Optional["ActionExecutor"] = None,
    ):
        """
        Initialize orchestrator.

        Args:
            registry: Plugin registry to use
            action_executor: Optional action executor (created if not provided)
        """
        self.registry = registry
        self.action_executor = action_executor or ActionExecutor()

    async def execute(
        self,
        context: PluginContext,
    ) -> "OrchestratorResult":
        """
        Execute all applicable plugins for a context.

        Args:
            context: Plugin execution context

        Returns:
            OrchestratorResult with all plugin results
        """
        plugins = self.registry.get_plugins_for_stage(context.stage)
        results: list[tuple[str, PluginResult]] = []
        actions_executed = 0
        terminated = False
        termination_reason = None

        for plugin in plugins:
            if not plugin.should_run(context):
                continue

            try:
                result = await plugin.execute(context)
                results.append((plugin.name, result))

                # Use DEBUG for skipped plugins (no actions), INFO when actually executing
                is_skipped = len(result.actions) == 0
                log_level = logging.DEBUG if is_skipped else logging.INFO
                log_message = "Plugin skipped" if is_skipped else "Plugin executed"

                logger.log(
                    log_level,
                    log_message,
                    extra={
                        "plugin_name": plugin.name,
                        "success": result.success,
                        "result_message": result.message,
                        "actions_count": len(result.actions),
                        "event_id": context.event_id,
                        "event_type": context.event_type,
                        "project_id": context.project_id,
                    },
                )

                # Execute actions
                for action in result.actions:
                    logger.info(
                        "Executing plugin action",
                        extra={
                            "plugin_name": plugin.name,
                            "action_type": action.action_type.value,
                            "event_id": context.event_id,
                            "event_type": context.event_type,
                            "project_id": context.project_id,
                        },
                    )
                    await self.action_executor.execute(action, context)
                    actions_executed += 1
                    logger.info(
                        "Plugin action executed successfully",
                        extra={
                            "plugin_name": plugin.name,
                            "action_type": action.action_type.value,
                            "event_id": context.event_id,
                            "event_type": context.event_type,
                            "project_id": context.project_id,
                        },
                    )

                # Check for termination
                if result.terminate_pipeline:
                    terminated = True
                    termination_reason = result.message
                    logger.info(
                        "Pipeline terminated by plugin",
                        extra={
                            "plugin_name": plugin.name,
                            "reason": termination_reason,
                        },
                    )
                    break

            except Exception as e:
                logger.error(
                    "Plugin execution failed",
                    extra={
                        "plugin_name": plugin.name,
                        "error": str(e),
                        "stage": context.stage.value,
                        "event_id": context.event_id,
                        "event_type": context.event_type,
                        "project_id": context.project_id,
                    },
                    exc_info=True,
                )
                results.append(
                    (
                        plugin.name,
                        PluginResult(success=False, message=f"Error: {str(e)}"),
                    )
                )

        return OrchestratorResult(
            results=results,
            actions_executed=actions_executed,
            terminated=terminated,
            termination_reason=termination_reason,
        )


class OrchestratorResult:
    """Result of executing plugins for a stage."""

    def __init__(
        self,
        results: list[tuple[str, PluginResult]],
        actions_executed: int = 0,
        terminated: bool = False,
        termination_reason: str | None = None,
    ):
        self.results = results
        self.actions_executed = actions_executed
        self.terminated = terminated
        self.termination_reason = termination_reason

    @property
    def success_count(self) -> int:
        return sum(1 for _, r in self.results if r.success)

    @property
    def failure_count(self) -> int:
        return sum(1 for _, r in self.results if not r.success)


class ActionExecutor:
    """
    Executes plugin actions.

    Override or extend to add custom action implementations.
    """

    def __init__(
        self,
        producer=None,
        http_client=None,
        connection_manager=None,
        producer_factory: Callable[[str], object] | None = None,
    ):
        """
        Initialize action executor.

        Args:
            producer: Default producer for publish actions (used when no factory provided,
                      or as fallback for Kafka where a single producer handles all topics)
            http_client: HTTP client for webhook actions (optional, legacy)
            connection_manager: ConnectionManager for named connection webhooks (optional)
            producer_factory: Callable that creates a producer for a given topic name.
                              Required for Event Hub where each topic needs its own connection.
                              Producers are cached and reused for subsequent publishes to the
                              same topic.
        """
        self.producer = producer
        self.http_client = http_client
        self.connection_manager = connection_manager
        self._producer_factory = producer_factory
        self._producer_cache: dict[str, object] = {}

    async def execute(self, action: PluginAction, context: PluginContext) -> None:
        """
        Execute a single action.

        Args:
            action: Action to execute
            context: Plugin context
        """
        if action.action_type == ActionType.PUBLISH_TO_TOPIC:
            await self._publish_to_topic(action.params, context)
        elif action.action_type == ActionType.HTTP_WEBHOOK:
            await self._http_webhook(action.params, context)
        elif action.action_type == ActionType.SEND_EMAIL:
            await self._send_email(action.params, context)
        elif action.action_type == ActionType.CREATE_CLAIMX_TASK:
            await self._create_claimx_task(action.params, context)
        elif action.action_type == ActionType.LOG:
            self._log(action.params, context)
        elif action.action_type == ActionType.ADD_HEADER:
            self._add_header(action.params, context)
        elif action.action_type == ActionType.METRIC:
            self._emit_metric(action.params, context)
        elif action.action_type == ActionType.FILTER:
            pass  # Handled by orchestrator via terminate_pipeline

    async def _get_producer_for_topic(self, topic: str):
        """Get or create a producer for the given topic.

        When a producer_factory is configured, producers are created per-topic
        and cached (required for Event Hub where each entity needs its own connection).
        Falls back to the default producer (sufficient for Kafka).
        """
        if self._producer_factory:
            if topic not in self._producer_cache:
                producer = self._producer_factory(topic)
                await producer.start()
                self._producer_cache[topic] = producer
                logger.info(
                    "Created plugin producer for topic",
                    extra={"topic": topic},
                )
            return self._producer_cache[topic]
        return self.producer

    async def close(self) -> None:
        """Stop all cached producers. Call during worker shutdown."""
        for topic, producer in self._producer_cache.items():
            try:
                await producer.stop()
            except Exception as e:
                logger.error(
                    "Error stopping plugin producer",
                    extra={"topic": topic, "error": str(e)},
                )
        self._producer_cache.clear()

    async def _publish_to_topic(
        self,
        params: dict,
        context: PluginContext,
    ) -> None:
        """Publish message to a Kafka/Event Hub topic."""
        topic = params["topic"]
        payload = params.get("payload", {})
        headers = {**context.headers, **params.get("headers", {})}
        # Use event_id as key (project standard), allow override via params
        key = params.get("key", context.event_id)

        logger.info(
            "Plugin action: publish to topic",
            extra={
                "topic": topic,
                "event_id": context.event_id,
                "project_id": context.project_id,
                "payload_keys": (list(payload.keys()) if isinstance(payload, dict) else None),
            },
        )

        producer = await self._get_producer_for_topic(topic)
        if producer:
            await producer.send(
                value=payload,
                key=key,
                headers=headers,
            )
            logger.info(
                "Plugin message published to topic successfully",
                extra={
                    "topic": topic,
                    "event_id": context.event_id,
                    "event_type": context.event_type,
                    "project_id": context.project_id,
                },
            )
        else:
            # Log only mode when no producer configured
            logger.warning(
                "No producer configured - publish action logged only",
                extra={
                    "topic": topic,
                    "event_id": context.event_id,
                    "event_type": context.event_type,
                    "project_id": context.project_id,
                    "payload_keys": (list(payload.keys()) if isinstance(payload, dict) else None),
                },
            )

    async def _http_webhook(
        self,
        params: dict,
        context: PluginContext,
    ) -> None:
        """Call HTTP webhook.

        Supports two modes:
        1. Named connection (recommended): Use 'connection' and 'path' params
        2. Direct URL (legacy): Use 'url' param with http_client
        """
        # Check if using named connection (new approach)
        connection_name = params.get("connection")

        if connection_name:
            # Use ConnectionManager for named connection
            if not self.connection_manager:
                logger.error(
                    "Named connection specified but no connection manager configured",
                    extra={"connection": connection_name},
                )
                return

            path = params.get("path", "/")
            method = params.get("method", "POST")
            body = params.get("body", {})
            headers = params.get("headers", {})

            logger.info(
                "Plugin action: HTTP webhook (named connection)",
                extra={
                    "connection": connection_name,
                    "path": path,
                    "method": method,
                    "event_id": context.event_id,
                },
            )

            try:
                response = await self.connection_manager.request(
                    connection_name=connection_name,
                    method=method,
                    path=path,
                    json=body,
                    headers=headers,
                )

                logger.debug(
                    "Webhook response",
                    extra={
                        "status": response.status,
                        "connection": connection_name,
                        "path": path,
                    },
                )

                # Log error responses
                if response.status >= 400:
                    response_body = await response.text()
                    logger.warning(
                        "Webhook returned error status",
                        extra={
                            "status": response.status,
                            "response": response_body[:200],
                        },
                    )
            except Exception as e:
                logger.error(
                    "Webhook request failed",
                    extra={
                        "connection": connection_name,
                        "error": str(e),
                    },
                )
        else:
            # Legacy mode: direct URL with http_client
            url = params.get("url")
            if not url:
                logger.error("HTTP webhook action requires either 'connection' or 'url' parameter")
                return

            method = params.get("method", "POST")
            body = params.get("body", {})
            headers = params.get("headers", {})

            logger.info(
                "Plugin action: HTTP webhook (direct URL)",
                extra={
                    "url": url,
                    "method": method,
                    "event_id": context.event_id,
                },
            )

            if self.http_client:
                async with self.http_client.request(
                    method=method,
                    url=url,
                    json=body,
                    headers=headers,
                ) as response:
                    logger.debug(
                        "Webhook response",
                        extra={
                            "status": response.status,
                            "url": url,
                        },
                    )
            else:
                logger.warning(
                    "No HTTP client configured - webhook action logged only",
                    extra={
                        "url": url,
                        "method": method,
                    },
                )

    async def _send_email(
        self,
        params: dict,
        context: PluginContext,
    ) -> None:
        """
        Send an email via configured email service.

        Uses ConnectionManager to call an email service API (e.g., SendGrid, Mailgun,
        AWS SES, or custom SMTP relay API).

        Params:
            connection: Named connection for email service (default: "email_service")
            to: List of recipient email addresses
            subject: Email subject line
            body: Email body content
            html: If True, body is HTML content (default: False)
            cc: Optional list of CC recipients
            bcc: Optional list of BCC recipients
            reply_to: Optional reply-to address
            template_id: Optional template ID for templated emails
            template_data: Optional dict of template variables
        """
        connection_name = params.get("connection", "email_service")

        if not self.connection_manager:
            logger.error(
                "Send email action requires connection manager but none configured",
                extra={
                    "connection": connection_name,
                    "event_id": context.event_id,
                },
            )
            return

        # Extract email parameters
        to_addresses = params.get("to", [])
        subject = params.get("subject", "")
        body = params.get("body", "")
        is_html = params.get("html", False)
        cc_addresses = params.get("cc", [])
        bcc_addresses = params.get("bcc", [])
        reply_to = params.get("reply_to")
        template_id = params.get("template_id")
        template_data = params.get("template_data", {})

        # Build email payload - generic format that works with most email APIs
        # Specific email services may need adapter configuration in the connection
        email_payload = {
            "to": to_addresses,
            "subject": subject,
            "content": body,
            "content_type": "text/html" if is_html else "text/plain",
        }

        if cc_addresses:
            email_payload["cc"] = cc_addresses
        if bcc_addresses:
            email_payload["bcc"] = bcc_addresses
        if reply_to:
            email_payload["reply_to"] = reply_to
        if template_id:
            email_payload["template_id"] = template_id
            email_payload["template_data"] = template_data

        # Add context metadata for tracking
        email_payload["metadata"] = {
            "event_id": context.event_id,
            "event_type": context.event_type,
            "project_id": context.project_id,
            "domain": context.domain.value,
            "stage": context.stage.value,
        }

        logger.info(
            "Plugin action: send email",
            extra={
                "connection": connection_name,
                "to_count": len(to_addresses),
                "subject": subject[:50] + "..." if len(subject) > 50 else subject,
                "has_template": template_id is not None,
                "event_id": context.event_id,
                "project_id": context.project_id,
            },
        )

        try:
            response = await self.connection_manager.request(
                connection_name=connection_name,
                method="POST",
                path="/send",
                json=email_payload,
            )

            if response.status >= 400:
                response_body = await response.text()
                logger.error(
                    "Email send failed with error status",
                    extra={
                        "status": response.status,
                        "response": response_body[:200],
                        "connection": connection_name,
                        "event_id": context.event_id,
                    },
                )
            else:
                logger.info(
                    "Email sent successfully",
                    extra={
                        "status": response.status,
                        "connection": connection_name,
                        "to_count": len(to_addresses),
                        "event_id": context.event_id,
                        "project_id": context.project_id,
                    },
                )
        except Exception as e:
            logger.error(
                "Email send request failed",
                extra={
                    "connection": connection_name,
                    "error": str(e),
                    "event_id": context.event_id,
                },
                exc_info=True,
            )
            raise

    async def _create_claimx_task(
        self,
        params: dict,
        context: PluginContext,
    ) -> None:
        """
        Create a ClaimX task via the ClaimX API.

        Uses ConnectionManager to call the ClaimX API endpoint /import/project/actions
        to create a new task in the specified project.

        Supports cross-domain usage by accepting either project_id (for ClaimX events)
        or claim_number (for XACT events). If claim_number is provided, will first
        fetch the ClaimX project ID via GET /export/project/projectId.

        Required API payload structure:
        {
            "projectId": 0,
            "usePrimaryContactAsSender": true,
            "senderUserName": "string",
            "type": "CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
            "data": {
                "customTaskName": "string",
                "customTaskId": 0,
                "notificationType": "COPY_EXTERNAL_LINK_URL",
                ...
            }
        }

        Params:
            connection: Named connection for ClaimX API (default: "claimx_api")
            project_id: ClaimX project ID (optional if claim_number provided)
            claim_number: Claim number (optional if project_id provided)
            task_type: Action type (e.g., "CUSTOM_TASK_ASSIGN_EXTERNAL_LINK")
            task_data: Task-specific data payload
            use_primary_contact_as_sender: Use primary contact as sender (default: True)
            sender_username: Sender username (optional)
        """
        connection_name = params.get("connection", "claimx_api")

        if not self.connection_manager:
            logger.error(
                "Create ClaimX task action requires connection manager but none configured",
                extra={
                    "connection": connection_name,
                    "event_id": context.event_id,
                },
            )
            return

        # Resolve project_id: use explicit value or look up from claim_number
        project_id = params.get("project_id")
        if not project_id and params.get("claim_number"):
            project_id = await self._resolve_claimx_project_id(
                connection_name, params["claim_number"], context
            )

        # Build and validate the API payload
        payload = self._build_claimx_task_payload(params, project_id, context)
        if payload is None:
            return

        # Submit the task to ClaimX
        await self._submit_claimx_task(connection_name, payload, context)

    async def _resolve_claimx_project_id(
        self,
        connection_name: str,
        claim_number: str,
        context: PluginContext,
    ):
        """Look up a ClaimX project ID from a claim number. Returns the ID or None."""
        logger.info(
            "Resolving ClaimX project ID from claim number",
            extra={
                "connection": connection_name,
                "claim_number": claim_number,
                "event_id": context.event_id,
            },
        )

        try:
            response = await self.connection_manager.request(
                connection_name=connection_name,
                method="GET",
                path="/export/project/projectId",
                params={"projectNumber": claim_number},  # API uses projectNumber param
            )

            if response.status >= 400:
                response_body = await response.text()
                logger.warning(
                    "Failed to resolve ClaimX project ID from claim number",
                    extra={
                        "status": response.status,
                        "response": response_body[:200],
                        "connection": connection_name,
                        "claim_number": claim_number,
                        "event_id": context.event_id,
                    },
                )
                return None

            # Parse the response - may be just a number or a dict
            response_data = await response.json()
            if isinstance(response_data, int):
                project_id = response_data
            elif isinstance(response_data, dict):
                project_id = response_data.get("projectId") or response_data.get("id")
            else:
                project_id = None

            if not project_id:
                logger.warning(
                    "ClaimX project ID not found in API response",
                    extra={
                        "connection": connection_name,
                        "claim_number": claim_number,
                        "response": str(response_data)[:200],
                        "event_id": context.event_id,
                    },
                )
                return None

            logger.info(
                "Resolved ClaimX project ID from claim number",
                extra={
                    "connection": connection_name,
                    "claim_number": claim_number,
                    "project_id": project_id,
                    "event_id": context.event_id,
                },
            )
            return project_id

        except Exception as e:
            logger.error(
                "Failed to resolve ClaimX project ID from claim number",
                extra={
                    "connection": connection_name,
                    "claim_number": claim_number,
                    "error": str(e),
                    "event_id": context.event_id,
                },
                exc_info=True,
            )
            raise

    def _build_claimx_task_payload(
        self,
        params: dict,
        project_id,
        context: PluginContext,
    ) -> dict | None:
        """Validate parameters and build the ClaimX task API payload. Returns None on validation failure."""
        task_type = params.get("task_type")
        task_data = params.get("task_data", {})

        if not project_id:
            logger.error(
                "Create ClaimX task action requires project_id or claim_number parameter",
                extra={"event_id": context.event_id},
            )
            return None

        if not task_type:
            logger.error(
                "Create ClaimX task action requires task_type parameter",
                extra={
                    "event_id": context.event_id,
                    "project_id": project_id,
                },
            )
            return None

        if not task_data:
            logger.error(
                "Create ClaimX task action requires task_data parameter",
                extra={
                    "event_id": context.event_id,
                    "project_id": project_id,
                },
            )
            return None

        payload = {
            "projectId": project_id,
            "usePrimaryContactAsSender": params.get("use_primary_contact_as_sender", True),
            "type": task_type,
            "data": task_data,
        }

        sender_username = params.get("sender_username")
        if sender_username:
            payload["senderUserName"] = sender_username

        return payload

    async def _submit_claimx_task(
        self,
        connection_name: str,
        payload: dict,
        context: PluginContext,
    ) -> None:
        """POST the task payload to the ClaimX API."""
        project_id = payload["projectId"]
        task_type = payload["type"]

        logger.info(
            "Plugin action: create ClaimX task",
            extra={
                "connection": connection_name,
                "project_id": project_id,
                "task_type": task_type,
                "event_id": context.event_id,
            },
        )

        try:
            response = await self.connection_manager.request(
                connection_name=connection_name,
                method="POST",
                path="/import/project/actions",
                json=payload,
            )

            if response.status >= 400:
                response_body = await response.text()
                logger.error(
                    "ClaimX task creation failed with error status",
                    extra={
                        "status": response.status,
                        "response": response_body[:500],
                        "connection": connection_name,
                        "project_id": project_id,
                        "task_type": task_type,
                        "event_id": context.event_id,
                    },
                )
            else:
                logger.info(
                    "ClaimX task created successfully",
                    extra={
                        "status": response.status,
                        "connection": connection_name,
                        "project_id": project_id,
                        "task_type": task_type,
                        "event_id": context.event_id,
                    },
                )
        except Exception as e:
            logger.error(
                "ClaimX task creation request failed",
                extra={
                    "connection": connection_name,
                    "project_id": project_id,
                    "task_type": task_type,
                    "error": str(e),
                    "event_id": context.event_id,
                },
                exc_info=True,
            )
            raise

    def _log(self, params: dict, context: PluginContext) -> None:
        """Log a message."""
        level = params.get("level", "info").upper()
        message = params.get("message", "")

        log_level = getattr(logging, level, logging.INFO)
        logger.log(
            log_level,
            f"Plugin log: {message}",
            extra={
                "event_id": context.event_id,
                "project_id": context.project_id,
            },
        )

    def _add_header(self, params: dict, context: PluginContext) -> None:
        """Add header to context."""
        key = params.get("key")
        value = params.get("value")
        if key and value:
            context.headers[key] = value

    def _emit_metric(self, params: dict, context: PluginContext) -> None:
        """Emit a metric (placeholder - implement with your metrics system)."""
        name = params.get("name")
        labels = params.get("labels", {})

        logger.debug(
            "Plugin action: emit metric",
            extra={
                "metric_name": name,
                "labels": labels,
            },
        )


# Global registry instance for testing
_global_registry: PluginRegistry | None = None


def get_global_registry() -> PluginRegistry:
    """Get or create the global plugin registry instance."""
    global _global_registry
    if _global_registry is None:
        _global_registry = PluginRegistry()
    return _global_registry


def reset_plugin_registry() -> None:
    """Reset the global plugin registry (for testing)."""
    global _global_registry
    _global_registry = None
