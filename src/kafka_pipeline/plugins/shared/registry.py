"""
Plugin registry and orchestration.

Manages plugin registration, lookup, and execution.
"""

import logging
from collections import defaultdict
from typing import Optional

from core.logging import log_with_context
from kafka_pipeline.plugins.shared.base import (
    ActionType,
    PipelineStage,
    Plugin,
    PluginAction,
    PluginContext,
    PluginResult,
)

logger = logging.getLogger(__name__)

# Global registry instance
_plugin_registry: Optional["PluginRegistry"] = None


class PluginRegistry:
    """
    Registry for managing plugins.

    Plugins are indexed by stage for efficient lookup during execution.
    """

    def __init__(self):
        self._plugins: dict[str, Plugin] = {}
        self._plugins_by_stage: dict[PipelineStage, list[Plugin]] = defaultdict(list)

        log_with_context(
            logger,
            logging.DEBUG,
            "PluginRegistry initialized",
        )

    def register(self, plugin: Plugin) -> None:
        """
        Register a plugin.

        Args:
            plugin: Plugin instance to register
        """
        if plugin.name in self._plugins:
            log_with_context(
                logger,
                logging.WARNING,
                "Overwriting plugin registration",
                plugin_name=plugin.name,
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

        log_with_context(
            logger,
            logging.INFO,
            "Registered plugin",
            plugin_name=plugin.name,
            plugin_version=plugin.version,
            domains=[d.value for d in plugin.domains] if plugin.domains else ["all"],
            stages=[s.value for s in plugin.stages] if plugin.stages else ["all"],
            event_types=plugin.event_types or ["all"],
            priority=plugin.priority,
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
            log_with_context(
                logger,
                logging.INFO,
                "Unregistered plugin",
                plugin_name=plugin_name,
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

                log_with_context(
                    logger,
                    log_level,
                    log_message,
                    plugin_name=plugin.name,
                    success=result.success,
                    result_message=result.message,
                    actions_count=len(result.actions),
                    event_id=context.event_id,
                    event_type=context.event_type,
                    project_id=context.project_id,
                )

                # Execute actions
                for action in result.actions:
                    log_with_context(
                        logger,
                        logging.INFO,
                        "Executing plugin action",
                        plugin_name=plugin.name,
                        action_type=action.action_type.value,
                        event_id=context.event_id,
                        event_type=context.event_type,
                        project_id=context.project_id,
                    )
                    await self.action_executor.execute(action, context)
                    actions_executed += 1
                    log_with_context(
                        logger,
                        logging.INFO,
                        "Plugin action executed successfully",
                        plugin_name=plugin.name,
                        action_type=action.action_type.value,
                        event_id=context.event_id,
                        event_type=context.event_type,
                        project_id=context.project_id,
                    )

                # Check for termination
                if result.terminate_pipeline:
                    terminated = True
                    termination_reason = result.message
                    log_with_context(
                        logger,
                        logging.INFO,
                        "Pipeline terminated by plugin",
                        plugin_name=plugin.name,
                        reason=termination_reason,
                    )
                    break

            except Exception as e:
                log_with_context(
                    logger,
                    logging.ERROR,
                    "Plugin execution failed",
                    plugin_name=plugin.name,
                    error=str(e),
                    stage=context.stage.value,
                    event_id=context.event_id,
                    event_type=context.event_type,
                    project_id=context.project_id,
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

    def __init__(self, producer=None, http_client=None, connection_manager=None):
        """
        Initialize action executor.

        Args:
            producer: Kafka producer for publish actions (optional)
            http_client: HTTP client for webhook actions (optional, legacy)
            connection_manager: ConnectionManager for named connection webhooks (optional)
        """
        self.producer = producer
        self.http_client = http_client
        self.connection_manager = connection_manager

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

    async def _publish_to_topic(
        self,
        params: dict,
        context: PluginContext,
    ) -> None:
        """Publish message to Kafka topic."""
        topic = params["topic"]
        payload = params.get("payload", {})
        headers = {**context.headers, **params.get("headers", {})}
        # Use event_id as key (project standard), allow override via params
        key = params.get("key", context.event_id)

        log_with_context(
            logger,
            logging.INFO,
            "Plugin action: publish to topic",
            topic=topic,
            event_id=context.event_id,
            project_id=context.project_id,
            payload_keys=list(payload.keys()) if isinstance(payload, dict) else None,
        )

        if self.producer:
            await self.producer.send(
                topic=topic,
                key=key,
                value=payload,
                headers=headers,
            )
            log_with_context(
                logger,
                logging.INFO,
                "Plugin message published to topic successfully",
                topic=topic,
                event_id=context.event_id,
                event_type=context.event_type,
                project_id=context.project_id,
            )
        else:
            # Log only mode when no producer configured
            log_with_context(
                logger,
                logging.WARNING,
                "No producer configured - publish action logged only",
                topic=topic,
                event_id=context.event_id,
                event_type=context.event_type,
                project_id=context.project_id,
                payload_keys=(
                    list(payload.keys()) if isinstance(payload, dict) else None
                ),
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
                log_with_context(
                    logger,
                    logging.ERROR,
                    "Named connection specified but no connection manager configured",
                    connection=connection_name,
                )
                return

            path = params.get("path", "/")
            method = params.get("method", "POST")
            body = params.get("body", {})
            headers = params.get("headers", {})

            log_with_context(
                logger,
                logging.INFO,
                "Plugin action: HTTP webhook (named connection)",
                connection=connection_name,
                path=path,
                method=method,
                event_id=context.event_id,
            )

            try:
                response = await self.connection_manager.request(
                    connection_name=connection_name,
                    method=method,
                    path=path,
                    json=body,
                    headers=headers,
                )

                log_with_context(
                    logger,
                    logging.DEBUG,
                    "Webhook response",
                    status=response.status,
                    connection=connection_name,
                    path=path,
                )

                # Log error responses
                if response.status >= 400:
                    response_body = await response.text()
                    log_with_context(
                        logger,
                        logging.WARNING,
                        "Webhook returned error status",
                        status=response.status,
                        response=response_body[:200],
                    )
            except Exception as e:
                log_with_context(
                    logger,
                    logging.ERROR,
                    "Webhook request failed",
                    connection=connection_name,
                    error=str(e),
                )
        else:
            # Legacy mode: direct URL with http_client
            url = params.get("url")
            if not url:
                log_with_context(
                    logger,
                    logging.ERROR,
                    "HTTP webhook action requires either 'connection' or 'url' parameter",
                )
                return

            method = params.get("method", "POST")
            body = params.get("body", {})
            headers = params.get("headers", {})

            log_with_context(
                logger,
                logging.INFO,
                "Plugin action: HTTP webhook (direct URL)",
                url=url,
                method=method,
                event_id=context.event_id,
            )

            if self.http_client:
                async with self.http_client.request(
                    method=method,
                    url=url,
                    json=body,
                    headers=headers,
                ) as response:
                    log_with_context(
                        logger,
                        logging.DEBUG,
                        "Webhook response",
                        status=response.status,
                        url=url,
                    )
            else:
                log_with_context(
                    logger,
                    logging.WARNING,
                    "No HTTP client configured - webhook action logged only",
                    url=url,
                    method=method,
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
            log_with_context(
                logger,
                logging.ERROR,
                "Send email action requires connection manager but none configured",
                connection=connection_name,
                event_id=context.event_id,
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

        log_with_context(
            logger,
            logging.INFO,
            "Plugin action: send email",
            connection=connection_name,
            to_count=len(to_addresses),
            subject=subject[:50] + "..." if len(subject) > 50 else subject,
            has_template=template_id is not None,
            event_id=context.event_id,
            project_id=context.project_id,
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
                log_with_context(
                    logger,
                    logging.ERROR,
                    "Email send failed with error status",
                    status=response.status,
                    response=response_body[:200],
                    connection=connection_name,
                    event_id=context.event_id,
                )
            else:
                log_with_context(
                    logger,
                    logging.INFO,
                    "Email sent successfully",
                    status=response.status,
                    connection=connection_name,
                    to_count=len(to_addresses),
                    event_id=context.event_id,
                    project_id=context.project_id,
                )
        except Exception as e:
            log_with_context(
                logger,
                logging.ERROR,
                "Email send request failed",
                connection=connection_name,
                error=str(e),
                event_id=context.event_id,
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
            log_with_context(
                logger,
                logging.ERROR,
                "Create ClaimX task action requires connection manager but none configured",
                connection=connection_name,
                event_id=context.event_id,
            )
            return

        project_id = params.get("project_id")
        claim_number = params.get("claim_number")
        task_type = params.get("task_type")
        task_data = params.get("task_data", {})
        use_primary_contact_as_sender = params.get(
            "use_primary_contact_as_sender", True
        )
        sender_username = params.get("sender_username")

        # Resolve project_id from claim_number if needed
        if not project_id and claim_number:
            log_with_context(
                logger,
                logging.INFO,
                "Resolving ClaimX project ID from claim number",
                connection=connection_name,
                claim_number=claim_number,
                event_id=context.event_id,
            )

            try:
                response = await self.connection_manager.request(
                    connection_name=connection_name,
                    method="GET",
                    path="/export/project/projectId",
                    params={
                        "projectNumber": claim_number
                    },  # API uses projectNumber param
                )

                if response.status >= 400:
                    response_body = await response.text()
                    log_with_context(
                        logger,
                        logging.WARNING,
                        "Failed to resolve ClaimX project ID from claim number",
                        status=response.status,
                        response=response_body[:200],
                        connection=connection_name,
                        claim_number=claim_number,
                        event_id=context.event_id,
                    )
                    return

                # Parse the response - may be just a number or a dict
                response_data = await response.json()
                if isinstance(response_data, int):
                    project_id = response_data
                elif isinstance(response_data, dict):
                    project_id = response_data.get("projectId") or response_data.get(
                        "id"
                    )

                if not project_id:
                    log_with_context(
                        logger,
                        logging.WARNING,
                        "ClaimX project ID not found in API response",
                        connection=connection_name,
                        claim_number=claim_number,
                        response=str(response_data)[:200],
                        event_id=context.event_id,
                    )
                    return

                log_with_context(
                    logger,
                    logging.INFO,
                    "Resolved ClaimX project ID from claim number",
                    connection=connection_name,
                    claim_number=claim_number,
                    project_id=project_id,
                    event_id=context.event_id,
                )

            except Exception as e:
                log_with_context(
                    logger,
                    logging.ERROR,
                    "Failed to resolve ClaimX project ID from claim number",
                    connection=connection_name,
                    claim_number=claim_number,
                    error=str(e),
                    event_id=context.event_id,
                    exc_info=True,
                )
                raise

        if not project_id:
            log_with_context(
                logger,
                logging.ERROR,
                "Create ClaimX task action requires project_id or claim_number parameter",
                event_id=context.event_id,
            )
            return

        if not task_type:
            log_with_context(
                logger,
                logging.ERROR,
                "Create ClaimX task action requires task_type parameter",
                event_id=context.event_id,
                project_id=project_id,
            )
            return

        if not task_data:
            log_with_context(
                logger,
                logging.ERROR,
                "Create ClaimX task action requires task_data parameter",
                event_id=context.event_id,
                project_id=project_id,
            )
            return

        # Build the API payload according to ClaimX spec
        payload = {
            "projectId": project_id,
            "usePrimaryContactAsSender": use_primary_contact_as_sender,
            "type": task_type,
            "data": task_data,
        }

        # Add sender username if provided
        if sender_username:
            payload["senderUserName"] = sender_username

        log_with_context(
            logger,
            logging.INFO,
            "Plugin action: create ClaimX task",
            connection=connection_name,
            project_id=project_id,
            task_type=task_type,
            event_id=context.event_id,
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
                log_with_context(
                    logger,
                    logging.ERROR,
                    "ClaimX task creation failed with error status",
                    status=response.status,
                    response=response_body[:500],
                    connection=connection_name,
                    project_id=project_id,
                    task_type=task_type,
                    event_id=context.event_id,
                )
            else:
                log_with_context(
                    logger,
                    logging.INFO,
                    "ClaimX task created successfully",
                    status=response.status,
                    connection=connection_name,
                    project_id=project_id,
                    task_type=task_type,
                    event_id=context.event_id,
                )
        except Exception as e:
            log_with_context(
                logger,
                logging.ERROR,
                "ClaimX task creation request failed",
                connection=connection_name,
                project_id=project_id,
                task_type=task_type,
                error=str(e),
                event_id=context.event_id,
                exc_info=True,
            )
            raise

    def _log(self, params: dict, context: PluginContext) -> None:
        """Log a message."""
        level = params.get("level", "info").upper()
        message = params.get("message", "")

        log_level = getattr(logging, level, logging.INFO)
        log_with_context(
            logger,
            log_level,
            f"Plugin log: {message}",
            event_id=context.event_id,
            project_id=context.project_id,
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

        log_with_context(
            logger,
            logging.DEBUG,
            "Plugin action: emit metric",
            metric_name=name,
            labels=labels,
        )


def get_plugin_registry() -> PluginRegistry:
    """Get or create global plugin registry."""
    global _plugin_registry
    if _plugin_registry is None:
        _plugin_registry = PluginRegistry()
    return _plugin_registry


def reset_plugin_registry() -> None:
    """Reset global registry (for testing)."""
    global _plugin_registry
    _plugin_registry = None


def register_plugin(cls_or_instance):
    """
    Decorator/function to register a plugin.

    Can be used as:
        @register_plugin
        class MyPlugin(Plugin): ...

    Or:
        register_plugin(MyPlugin())
        register_plugin(MyPlugin(config={...}))
    """
    registry = get_plugin_registry()

    if isinstance(cls_or_instance, type):
        # Class decorator - instantiate and register
        plugin = cls_or_instance()
        registry.register(plugin)
        return cls_or_instance
    else:
        # Instance - register directly
        registry.register(cls_or_instance)
        return cls_or_instance
