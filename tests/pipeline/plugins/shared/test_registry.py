"""Tests for plugin registry and orchestration."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel

from pipeline.plugins.shared.base import (
    ActionType,
    Domain,
    PipelineStage,
    Plugin,
    PluginAction,
    PluginContext,
    PluginResult,
)
from pipeline.plugins.shared.registry import (
    ActionExecutor,
    OrchestratorResult,
    PluginOrchestrator,
    PluginRegistry,
    get_global_registry,
    reset_plugin_registry,
)


# --- Helpers ---


class DummyMessage(BaseModel):
    value: str = "test"


class StubPlugin(Plugin):
    name = "stub"

    async def execute(self, context: PluginContext) -> PluginResult:
        return PluginResult.success("ok")


def _make_plugin(name="p1", domains=None, stages=None, event_types=None, priority=100):
    p = StubPlugin()
    p.name = name
    p.domains = domains or []
    p.stages = stages or []
    p.event_types = event_types or []
    p.priority = priority
    return p


def _make_context(**overrides):
    defaults = dict(
        domain=Domain.CLAIMX,
        stage=PipelineStage.ENRICHMENT_COMPLETE,
        message=DummyMessage(),
        event_id="evt-1",
        event_type="CUSTOM_TASK_COMPLETED",
        project_id="proj-1",
    )
    defaults.update(overrides)
    return PluginContext(**defaults)


# =====================
# PluginRegistry tests
# =====================


class TestPluginRegistry:
    def test_register_and_get_plugin(self):
        registry = PluginRegistry()
        plugin = _make_plugin("alpha")
        registry.register(plugin)

        assert registry.get_plugin("alpha") is plugin

    def test_register_overwrites_existing(self):
        registry = PluginRegistry()
        p1 = _make_plugin("same")
        p2 = _make_plugin("same")
        registry.register(p1)
        registry.register(p2)

        assert registry.get_plugin("same") is p2

    def test_get_plugin_returns_none_for_missing(self):
        registry = PluginRegistry()
        assert registry.get_plugin("nonexistent") is None

    def test_list_plugins(self):
        registry = PluginRegistry()
        registry.register(_make_plugin("a"))
        registry.register(_make_plugin("b"))

        names = {p.name for p in registry.list_plugins()}
        assert names == {"a", "b"}

    def test_unregister_existing(self):
        registry = PluginRegistry()
        plugin = _make_plugin("to_remove")
        registry.register(plugin)

        removed = registry.unregister("to_remove")
        assert removed is plugin
        assert registry.get_plugin("to_remove") is None

    def test_unregister_missing_returns_none(self):
        registry = PluginRegistry()
        assert registry.unregister("ghost") is None

    def test_unregister_removes_from_stage_index(self):
        registry = PluginRegistry()
        plugin = _make_plugin("indexed", stages=[PipelineStage.DLQ])
        registry.register(plugin)

        registry.unregister("indexed")
        assert plugin not in registry.get_plugins_for_stage(PipelineStage.DLQ)

    def test_clear_removes_all(self):
        registry = PluginRegistry()
        registry.register(_make_plugin("a"))
        registry.register(_make_plugin("b"))
        registry.clear()

        assert registry.list_plugins() == []

    def test_plugins_indexed_by_stage(self):
        registry = PluginRegistry()
        plugin = _make_plugin("stage_plugin", stages=[PipelineStage.DLQ])
        registry.register(plugin)

        assert plugin in registry.get_plugins_for_stage(PipelineStage.DLQ)
        assert plugin not in registry.get_plugins_for_stage(PipelineStage.ERROR)

    def test_no_stage_filter_indexes_in_all_stages(self):
        registry = PluginRegistry()
        plugin = _make_plugin("all_stages", stages=[])
        registry.register(plugin)

        for stage in PipelineStage:
            assert plugin in registry.get_plugins_for_stage(stage)

    def test_plugins_sorted_by_priority(self):
        registry = PluginRegistry()
        low = _make_plugin("low", priority=10, stages=[PipelineStage.DLQ])
        high = _make_plugin("high", priority=200, stages=[PipelineStage.DLQ])
        mid = _make_plugin("mid", priority=100, stages=[PipelineStage.DLQ])

        registry.register(high)
        registry.register(low)
        registry.register(mid)

        stage_plugins = registry.get_plugins_for_stage(PipelineStage.DLQ)
        names = [p.name for p in stage_plugins]
        assert names == ["low", "mid", "high"]

    def test_get_plugins_for_stage_returns_empty_for_unused_stage(self):
        registry = PluginRegistry()
        assert registry.get_plugins_for_stage(PipelineStage.RETRY) == []


# =====================
# Global registry tests
# =====================


class TestGlobalRegistry:
    def test_get_global_registry_creates_singleton(self):
        reset_plugin_registry()
        r1 = get_global_registry()
        r2 = get_global_registry()
        assert r1 is r2

    def test_reset_clears_singleton(self):
        reset_plugin_registry()
        r1 = get_global_registry()
        reset_plugin_registry()
        r2 = get_global_registry()
        assert r1 is not r2


# =====================
# OrchestratorResult tests
# =====================


class TestOrchestratorResult:
    def test_success_and_failure_counts(self):
        results = [
            ("p1", PluginResult(success=True)),
            ("p2", PluginResult(success=False)),
            ("p3", PluginResult(success=True)),
        ]
        orch_result = OrchestratorResult(results=results)
        assert orch_result.success_count == 2
        assert orch_result.failure_count == 1

    def test_empty_results(self):
        orch_result = OrchestratorResult(results=[])
        assert orch_result.success_count == 0
        assert orch_result.failure_count == 0
        assert orch_result.terminated is False
        assert orch_result.termination_reason is None

    def test_termination_fields(self):
        orch_result = OrchestratorResult(
            results=[],
            terminated=True,
            termination_reason="filtered",
        )
        assert orch_result.terminated is True
        assert orch_result.termination_reason == "filtered"


# =====================
# PluginOrchestrator tests
# =====================


class TestPluginOrchestrator:
    async def test_executes_matching_plugins(self):
        registry = PluginRegistry()
        plugin = _make_plugin("runner", stages=[PipelineStage.ENRICHMENT_COMPLETE])
        plugin.execute = AsyncMock(return_value=PluginResult.success("ran"))
        registry.register(plugin)

        orchestrator = PluginOrchestrator(registry)
        ctx = _make_context()
        result = await orchestrator.execute(ctx)

        plugin.execute.assert_awaited_once_with(ctx)
        assert result.success_count == 1

    async def test_skips_plugins_that_should_not_run(self):
        registry = PluginRegistry()
        plugin = _make_plugin(
            "verisk_only",
            domains=[Domain.VERISK],
            stages=[PipelineStage.ENRICHMENT_COMPLETE],
        )
        plugin.execute = AsyncMock()
        registry.register(plugin)

        orchestrator = PluginOrchestrator(registry)
        ctx = _make_context(domain=Domain.CLAIMX)
        result = await orchestrator.execute(ctx)

        plugin.execute.assert_not_awaited()
        assert result.success_count == 0

    async def test_executes_actions(self):
        registry = PluginRegistry()
        action = PluginAction(
            action_type=ActionType.LOG,
            params={"level": "info", "message": "test"},
        )
        plugin = _make_plugin("with_action", stages=[PipelineStage.ENRICHMENT_COMPLETE])
        plugin.execute = AsyncMock(
            return_value=PluginResult(success=True, actions=[action])
        )
        registry.register(plugin)

        executor = AsyncMock()
        orchestrator = PluginOrchestrator(registry, action_executor=executor)
        ctx = _make_context()
        result = await orchestrator.execute(ctx)

        executor.execute.assert_awaited_once_with(action, ctx)
        assert result.actions_executed == 1

    async def test_terminates_on_terminate_pipeline(self):
        registry = PluginRegistry()
        p1 = _make_plugin("terminator", stages=[PipelineStage.ENRICHMENT_COMPLETE], priority=1)
        p1.execute = AsyncMock(
            return_value=PluginResult(
                success=True, terminate_pipeline=True, message="stop"
            )
        )

        p2 = _make_plugin("after", stages=[PipelineStage.ENRICHMENT_COMPLETE], priority=2)
        p2.execute = AsyncMock()
        registry.register(p1)
        registry.register(p2)

        orchestrator = PluginOrchestrator(registry)
        ctx = _make_context()
        result = await orchestrator.execute(ctx)

        assert result.terminated is True
        assert result.termination_reason == "stop"
        p2.execute.assert_not_awaited()

    async def test_handles_plugin_exception(self):
        registry = PluginRegistry()
        plugin = _make_plugin("broken", stages=[PipelineStage.ENRICHMENT_COMPLETE])
        plugin.execute = AsyncMock(side_effect=RuntimeError("boom"))
        registry.register(plugin)

        orchestrator = PluginOrchestrator(registry)
        ctx = _make_context()
        result = await orchestrator.execute(ctx)

        assert result.failure_count == 1
        assert "Error: boom" in result.results[0][1].message

    async def test_multiple_plugins_run_in_priority_order(self):
        registry = PluginRegistry()
        call_order = []

        async def make_exec(name):
            async def _exec(ctx):
                call_order.append(name)
                return PluginResult.success(name)
            return _exec

        p1 = _make_plugin("first", stages=[PipelineStage.ENRICHMENT_COMPLETE], priority=10)
        p1.execute = AsyncMock(side_effect=lambda ctx: _track(call_order, "first"))
        p2 = _make_plugin("second", stages=[PipelineStage.ENRICHMENT_COMPLETE], priority=20)
        p2.execute = AsyncMock(side_effect=lambda ctx: _track(call_order, "second"))

        registry.register(p2)
        registry.register(p1)

        orchestrator = PluginOrchestrator(registry)
        ctx = _make_context()
        await orchestrator.execute(ctx)

        assert call_order == ["first", "second"]


def _track(order_list, name):
    order_list.append(name)
    return PluginResult.success(name)


# =====================
# ActionExecutor tests
# =====================


class TestActionExecutor:
    async def test_publish_to_topic_with_producer(self):
        producer = AsyncMock()
        executor = ActionExecutor(producer=producer)
        action = PluginAction(
            action_type=ActionType.PUBLISH_TO_TOPIC,
            params={"topic": "t1", "payload": {"k": "v"}, "headers": {"h": "1"}},
        )
        ctx = _make_context(headers={"base": "header"})

        await executor.execute(action, ctx)

        producer.send.assert_awaited_once()
        call_kwargs = producer.send.call_args.kwargs
        assert call_kwargs["value"] == {"k": "v"}
        assert call_kwargs["key"] == "evt-1"
        assert call_kwargs["headers"]["h"] == "1"
        assert call_kwargs["headers"]["base"] == "header"

    async def test_publish_to_topic_uses_custom_key(self):
        producer = AsyncMock()
        executor = ActionExecutor(producer=producer)
        action = PluginAction(
            action_type=ActionType.PUBLISH_TO_TOPIC,
            params={"topic": "t", "payload": {}, "key": "custom-key"},
        )
        ctx = _make_context()

        await executor.execute(action, ctx)
        assert producer.send.call_args.kwargs["key"] == "custom-key"

    async def test_publish_to_topic_without_producer_logs_only(self):
        executor = ActionExecutor()
        action = PluginAction(
            action_type=ActionType.PUBLISH_TO_TOPIC,
            params={"topic": "t", "payload": {}},
        )
        ctx = _make_context()
        # Should not raise
        await executor.execute(action, ctx)

    async def test_http_webhook_named_connection(self):
        conn_mgr = AsyncMock()
        response = AsyncMock()
        response.status = 200
        conn_mgr.request.return_value = response

        executor = ActionExecutor(connection_manager=conn_mgr)
        action = PluginAction(
            action_type=ActionType.HTTP_WEBHOOK,
            params={
                "connection": "ext_api",
                "path": "/notify",
                "method": "POST",
                "body": {"data": "test"},
                "headers": {"x-custom": "val"},
            },
        )
        ctx = _make_context()

        await executor.execute(action, ctx)
        conn_mgr.request.assert_awaited_once_with(
            connection_name="ext_api",
            method="POST",
            path="/notify",
            json={"data": "test"},
            headers={"x-custom": "val"},
        )

    async def test_http_webhook_named_connection_without_manager(self):
        executor = ActionExecutor()
        action = PluginAction(
            action_type=ActionType.HTTP_WEBHOOK,
            params={"connection": "ext_api", "path": "/notify"},
        )
        ctx = _make_context()
        # Should not raise, just logs error
        await executor.execute(action, ctx)

    async def test_http_webhook_error_response_logged(self):
        conn_mgr = AsyncMock()
        response = AsyncMock()
        response.status = 500
        response.text.return_value = "Internal Server Error"
        conn_mgr.request.return_value = response

        executor = ActionExecutor(connection_manager=conn_mgr)
        action = PluginAction(
            action_type=ActionType.HTTP_WEBHOOK,
            params={"connection": "ext_api", "path": "/fail"},
        )
        ctx = _make_context()
        # Should not raise
        await executor.execute(action, ctx)

    async def test_http_webhook_legacy_url_with_client(self):
        http_client = MagicMock()
        response_ctx = AsyncMock()
        response_ctx.status = 200
        http_client.request.return_value.__aenter__ = AsyncMock(
            return_value=response_ctx
        )
        http_client.request.return_value.__aexit__ = AsyncMock(return_value=False)

        executor = ActionExecutor(http_client=http_client)
        action = PluginAction(
            action_type=ActionType.HTTP_WEBHOOK,
            params={"url": "https://example.com/hook", "method": "POST", "body": {}},
        )
        ctx = _make_context()

        await executor.execute(action, ctx)
        http_client.request.assert_called_once()

    async def test_http_webhook_legacy_url_without_client(self):
        executor = ActionExecutor()
        action = PluginAction(
            action_type=ActionType.HTTP_WEBHOOK,
            params={"url": "https://example.com/hook"},
        )
        ctx = _make_context()
        # Should not raise, just logs warning
        await executor.execute(action, ctx)

    async def test_http_webhook_no_connection_no_url_logs_error(self):
        executor = ActionExecutor()
        action = PluginAction(
            action_type=ActionType.HTTP_WEBHOOK,
            params={},
        )
        ctx = _make_context()
        # Should not raise
        await executor.execute(action, ctx)

    async def test_send_email_success(self):
        conn_mgr = AsyncMock()
        response = AsyncMock()
        response.status = 200
        conn_mgr.request.return_value = response

        executor = ActionExecutor(connection_manager=conn_mgr)
        action = PluginAction(
            action_type=ActionType.SEND_EMAIL,
            params={
                "to": ["user@example.com"],
                "subject": "Test",
                "body": "Hello",
                "cc": ["cc@example.com"],
                "bcc": ["bcc@example.com"],
                "reply_to": "reply@example.com",
                "template_id": "tmpl-1",
                "template_data": {"name": "Test"},
            },
        )
        ctx = _make_context()

        await executor.execute(action, ctx)
        call_kwargs = conn_mgr.request.call_args.kwargs
        assert call_kwargs["method"] == "POST"
        assert call_kwargs["path"] == "/send"
        payload = call_kwargs["json"]
        assert payload["to"] == ["user@example.com"]
        assert payload["cc"] == ["cc@example.com"]
        assert payload["template_id"] == "tmpl-1"
        assert "metadata" in payload

    async def test_send_email_without_connection_manager(self):
        executor = ActionExecutor()
        action = PluginAction(
            action_type=ActionType.SEND_EMAIL,
            params={"to": ["user@example.com"], "subject": "Hi", "body": "Hello"},
        )
        ctx = _make_context()
        # Should not raise, just logs error
        await executor.execute(action, ctx)

    async def test_send_email_error_response(self):
        conn_mgr = AsyncMock()
        response = AsyncMock()
        response.status = 422
        response.text.return_value = "Unprocessable"
        conn_mgr.request.return_value = response

        executor = ActionExecutor(connection_manager=conn_mgr)
        action = PluginAction(
            action_type=ActionType.SEND_EMAIL,
            params={"to": ["u@e.com"], "subject": "S", "body": "B"},
        )
        ctx = _make_context()
        # Should not raise (logs error but doesn't re-raise)
        await executor.execute(action, ctx)

    async def test_send_email_exception_raises(self):
        conn_mgr = AsyncMock()
        conn_mgr.request.side_effect = ConnectionError("timeout")

        executor = ActionExecutor(connection_manager=conn_mgr)
        action = PluginAction(
            action_type=ActionType.SEND_EMAIL,
            params={"to": ["u@e.com"], "subject": "S", "body": "B"},
        )
        ctx = _make_context()

        with pytest.raises(ConnectionError):
            await executor.execute(action, ctx)

    async def test_log_action(self):
        executor = ActionExecutor()
        action = PluginAction(
            action_type=ActionType.LOG,
            params={"level": "warning", "message": "test message"},
        )
        ctx = _make_context()
        # Should not raise
        await executor.execute(action, ctx)

    async def test_log_action_defaults(self):
        executor = ActionExecutor()
        action = PluginAction(action_type=ActionType.LOG, params={})
        ctx = _make_context()
        await executor.execute(action, ctx)

    async def test_add_header_action(self):
        executor = ActionExecutor()
        action = PluginAction(
            action_type=ActionType.ADD_HEADER,
            params={"key": "x-custom", "value": "123"},
        )
        ctx = _make_context()

        await executor.execute(action, ctx)
        assert ctx.headers["x-custom"] == "123"

    async def test_add_header_skips_when_missing_key_or_value(self):
        executor = ActionExecutor()
        ctx = _make_context()

        # Missing value
        action = PluginAction(
            action_type=ActionType.ADD_HEADER, params={"key": "x-test"}
        )
        await executor.execute(action, ctx)
        assert "x-test" not in ctx.headers

        # Missing key
        action = PluginAction(
            action_type=ActionType.ADD_HEADER, params={"value": "val"}
        )
        await executor.execute(action, ctx)

    async def test_metric_action(self):
        executor = ActionExecutor()
        action = PluginAction(
            action_type=ActionType.METRIC,
            params={"name": "test_metric", "labels": {"env": "test"}},
        )
        ctx = _make_context()
        await executor.execute(action, ctx)

    async def test_filter_action_is_noop(self):
        executor = ActionExecutor()
        action = PluginAction(
            action_type=ActionType.FILTER,
            params={"reason": "spam"},
        )
        ctx = _make_context()
        # Filter is handled by orchestrator, executor does nothing
        await executor.execute(action, ctx)

    async def test_create_claimx_task_with_project_id(self):
        conn_mgr = AsyncMock()
        response = AsyncMock()
        response.status = 201
        conn_mgr.request.return_value = response

        executor = ActionExecutor(connection_manager=conn_mgr)
        action = PluginAction(
            action_type=ActionType.CREATE_CLAIMX_TASK,
            params={
                "project_id": 100,
                "task_type": "CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
                "task_data": {"customTaskName": "Test"},
                "sender_username": "admin",
            },
        )
        ctx = _make_context()

        await executor.execute(action, ctx)

        call_kwargs = conn_mgr.request.call_args.kwargs
        assert call_kwargs["path"] == "/import/project/actions"
        payload = call_kwargs["json"]
        assert payload["projectId"] == 100
        assert payload["type"] == "CUSTOM_TASK_ASSIGN_EXTERNAL_LINK"
        assert payload["senderUserName"] == "admin"

    async def test_create_claimx_task_resolves_from_claim_number(self):
        conn_mgr = AsyncMock()

        # First call: resolve project ID
        resolve_response = AsyncMock()
        resolve_response.status = 200
        resolve_response.json.return_value = 555

        # Second call: create task
        create_response = AsyncMock()
        create_response.status = 201

        conn_mgr.request.side_effect = [resolve_response, create_response]

        executor = ActionExecutor(connection_manager=conn_mgr)
        action = PluginAction(
            action_type=ActionType.CREATE_CLAIMX_TASK,
            params={
                "claim_number": "CLM-001",
                "task_type": "CUSTOM_TASK",
                "task_data": {"name": "Task"},
            },
        )
        ctx = _make_context()

        await executor.execute(action, ctx)

        # Should have made two requests
        assert conn_mgr.request.await_count == 2

    async def test_create_claimx_task_without_connection_manager(self):
        executor = ActionExecutor()
        action = PluginAction(
            action_type=ActionType.CREATE_CLAIMX_TASK,
            params={
                "project_id": 1,
                "task_type": "CUSTOM_TASK",
                "task_data": {"name": "Task"},
            },
        )
        ctx = _make_context()
        # Should not raise
        await executor.execute(action, ctx)

    async def test_create_claimx_task_missing_project_id_and_claim(self):
        conn_mgr = AsyncMock()
        executor = ActionExecutor(connection_manager=conn_mgr)
        action = PluginAction(
            action_type=ActionType.CREATE_CLAIMX_TASK,
            params={
                "task_type": "CUSTOM_TASK",
                "task_data": {"name": "Task"},
            },
        )
        ctx = _make_context()
        # Should not raise, logs error
        await executor.execute(action, ctx)
        conn_mgr.request.assert_not_awaited()

    async def test_create_claimx_task_missing_task_type(self):
        conn_mgr = AsyncMock()
        executor = ActionExecutor(connection_manager=conn_mgr)
        action = PluginAction(
            action_type=ActionType.CREATE_CLAIMX_TASK,
            params={
                "project_id": 1,
                "task_data": {"name": "Task"},
            },
        )
        ctx = _make_context()
        await executor.execute(action, ctx)
        conn_mgr.request.assert_not_awaited()

    async def test_create_claimx_task_missing_task_data(self):
        conn_mgr = AsyncMock()
        executor = ActionExecutor(connection_manager=conn_mgr)
        action = PluginAction(
            action_type=ActionType.CREATE_CLAIMX_TASK,
            params={
                "project_id": 1,
                "task_type": "CUSTOM_TASK",
            },
        )
        ctx = _make_context()
        await executor.execute(action, ctx)
        conn_mgr.request.assert_not_awaited()

    async def test_create_claimx_task_resolve_fails_with_http_error(self):
        conn_mgr = AsyncMock()
        resolve_response = AsyncMock()
        resolve_response.status = 404
        resolve_response.text.return_value = "Not found"
        conn_mgr.request.return_value = resolve_response

        executor = ActionExecutor(connection_manager=conn_mgr)
        action = PluginAction(
            action_type=ActionType.CREATE_CLAIMX_TASK,
            params={
                "claim_number": "CLM-MISSING",
                "task_type": "CUSTOM_TASK",
                "task_data": {"name": "Task"},
            },
        )
        ctx = _make_context()
        await executor.execute(action, ctx)
        # Only one request (resolve), no create
        assert conn_mgr.request.await_count == 1

    async def test_create_claimx_task_resolve_returns_dict_with_id(self):
        conn_mgr = AsyncMock()

        resolve_response = AsyncMock()
        resolve_response.status = 200
        resolve_response.json.return_value = {"id": 777}

        create_response = AsyncMock()
        create_response.status = 201

        conn_mgr.request.side_effect = [resolve_response, create_response]

        executor = ActionExecutor(connection_manager=conn_mgr)
        action = PluginAction(
            action_type=ActionType.CREATE_CLAIMX_TASK,
            params={
                "claim_number": "CLM-002",
                "task_type": "CUSTOM_TASK",
                "task_data": {"name": "Task"},
            },
        )
        ctx = _make_context()
        await executor.execute(action, ctx)

        # Second call should use resolved project_id
        create_payload = conn_mgr.request.call_args_list[1].kwargs["json"]
        assert create_payload["projectId"] == 777

    async def test_create_claimx_task_resolve_exception_raises(self):
        conn_mgr = AsyncMock()
        conn_mgr.request.side_effect = ConnectionError("timeout")

        executor = ActionExecutor(connection_manager=conn_mgr)
        action = PluginAction(
            action_type=ActionType.CREATE_CLAIMX_TASK,
            params={
                "claim_number": "CLM-ERR",
                "task_type": "CUSTOM_TASK",
                "task_data": {"name": "Task"},
            },
        )
        ctx = _make_context()

        with pytest.raises(ConnectionError):
            await executor.execute(action, ctx)

    async def test_create_claimx_task_api_error_response(self):
        conn_mgr = AsyncMock()
        response = AsyncMock()
        response.status = 500
        response.text.return_value = "Internal Error"
        conn_mgr.request.return_value = response

        executor = ActionExecutor(connection_manager=conn_mgr)
        action = PluginAction(
            action_type=ActionType.CREATE_CLAIMX_TASK,
            params={
                "project_id": 1,
                "task_type": "CUSTOM_TASK",
                "task_data": {"name": "Task"},
            },
        )
        ctx = _make_context()
        # Should not raise, logs error
        await executor.execute(action, ctx)
