"""
Plugin registry and orchestration.

Manages plugin registration, lookup, and execution.
"""

import logging
from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Type

from core.logging import log_with_context

from kafka_pipeline.plugins.shared.base import (
    Plugin,
    PluginContext,
    PluginResult,
    PluginAction,
    ActionType,
    PipelineStage,
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
        self._plugins: Dict[str, Plugin] = {}
        self._plugins_by_stage: Dict[PipelineStage, List[Plugin]] = defaultdict(list)

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

    def unregister(self, plugin_name: str) -> Optional[Plugin]:
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

    def get_plugin(self, name: str) -> Optional[Plugin]:
        """Get plugin by name."""
        return self._plugins.get(name)

    def get_plugins_for_stage(self, stage: PipelineStage) -> List[Plugin]:
        """Get all plugins that run at a given stage."""
        return self._plugins_by_stage.get(stage, [])

    def list_plugins(self) -> List[Plugin]:
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
        results: List[Tuple[str, PluginResult]] = []
        actions_executed = 0
        terminated = False
        termination_reason = None

        for plugin in plugins:
            if not plugin.should_run(context):
                continue

            try:
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "Executing plugin",
                    plugin_name=plugin.name,
                    stage=context.stage.value,
                    event_type=context.event_type,
                    project_id=context.project_id,
                )

                result = await plugin.execute(context)
                results.append((plugin.name, result))

                # Execute actions
                for action in result.actions:
                    try:
                        await self.action_executor.execute(action, context)
                        actions_executed += 1
                    except Exception as action_error:
                        log_with_context(
                            logger,
                            logging.ERROR,
                            "Plugin action execution failed",
                            plugin_name=plugin.name,
                            action_type=action.action_type.value,
                            action_params=action.params,
                            error=str(action_error),
                            event_id=context.event_id,
                            event_type=context.event_type,
                            project_id=context.project_id,
                            exc_info=True,
                        )
                        raise

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
                results.append((plugin.name, PluginResult(
                    success=False,
                    message=f"Error: {str(e)}"
                )))

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
        results: List[Tuple[str, PluginResult]],
        actions_executed: int = 0,
        terminated: bool = False,
        termination_reason: Optional[str] = None,
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
        params: Dict,
        context: PluginContext,
    ) -> None:
        """Publish message to Kafka topic."""
        topic = params["topic"]
        payload = params.get("payload", {})
        headers = {**context.headers, **params.get("headers", {})}

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
                value=payload,
                headers=[(k, v.encode()) for k, v in headers.items()],
            )
        else:
            # Log only mode when no producer configured
            log_with_context(
                logger,
                logging.WARNING,
                "No producer configured - publish action logged only",
                topic=topic,
                payload=payload,
            )

    async def _http_webhook(
        self,
        params: Dict,
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

    def _log(self, params: Dict, context: PluginContext) -> None:
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

    def _add_header(self, params: Dict, context: PluginContext) -> None:
        """Add header to context."""
        key = params.get("key")
        value = params.get("value")
        if key and value:
            context.headers[key] = value

    def _emit_metric(self, params: Dict, context: PluginContext) -> None:
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
