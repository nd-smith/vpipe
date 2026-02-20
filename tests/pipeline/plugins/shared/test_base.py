"""Tests for plugin base classes."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from pipeline.plugins.shared.base import (
    ActionType,
    Domain,
    PipelineStage,
    Plugin,
    PluginContext,
    PluginResult,
    resolve_claimx_project_id,
)

# --- Concrete Plugin for testing the ABC ---


class DummyPlugin(Plugin):
    name = "dummy"
    domains = []
    stages = []
    event_types = []
    priority = 100

    async def execute(self, context: PluginContext) -> PluginResult:
        return PluginResult.success("ok")


class DummyMessage(BaseModel):
    value: str = "test"


def _make_context(**overrides):
    defaults = {
        "domain": Domain.CLAIMX,
        "stage": PipelineStage.ENRICHMENT_COMPLETE,
        "message": DummyMessage(),
        "event_id": "evt-1",
        "event_type": "CUSTOM_TASK_COMPLETED",
        "project_id": "proj-1",
    }
    defaults.update(overrides)
    return PluginContext(**defaults)


# =====================
# PluginResult tests
# =====================


class TestPluginResult:
    def test_success_factory(self):
        result = PluginResult.success("done")
        assert result.success is True
        assert result.message == "done"
        assert result.actions == []
        assert result.terminate_pipeline is False

    def test_skip_factory(self):
        result = PluginResult.skip("not relevant")
        assert result.success is True
        assert "Skipped" in result.message

    def test_publish_factory(self):
        result = PluginResult.publish(
            topic="my-topic",
            payload={"key": "val"},
            headers={"h": "v"},
        )
        assert result.success is True
        assert len(result.actions) == 1
        action = result.actions[0]
        assert action.action_type == ActionType.PUBLISH_TO_TOPIC
        assert action.params["topic"] == "my-topic"
        assert action.params["payload"] == {"key": "val"}
        assert action.params["headers"] == {"h": "v"}

    def test_publish_factory_default_headers(self):
        result = PluginResult.publish(topic="t", payload={})
        assert result.actions[0].params["headers"] == {}

    def test_webhook_factory(self):
        result = PluginResult.webhook(
            url="https://example.com",
            method="PUT",
            body={"a": 1},
            headers={"x": "y"},
        )
        action = result.actions[0]
        assert action.action_type == ActionType.HTTP_WEBHOOK
        assert action.params["url"] == "https://example.com"
        assert action.params["method"] == "PUT"

    def test_webhook_factory_defaults(self):
        result = PluginResult.webhook(url="https://example.com")
        action = result.actions[0]
        assert action.params["method"] == "POST"
        assert action.params["body"] == {}
        assert action.params["headers"] == {}

    def test_log_factory(self):
        result = PluginResult.log("warning", "something happened")
        action = result.actions[0]
        assert action.action_type == ActionType.LOG
        assert action.params["level"] == "warning"
        assert action.params["message"] == "something happened"

    def test_filter_out_factory(self):
        result = PluginResult.filter_out("spam")
        assert result.terminate_pipeline is True
        assert "Filtered" in result.message
        assert result.actions[0].action_type == ActionType.FILTER
        assert result.actions[0].params["reason"] == "spam"

    def test_email_factory_simple(self):
        result = PluginResult.email(
            to="user@example.com",
            subject="Hello",
            body="Body text",
        )
        action = result.actions[0]
        assert action.action_type == ActionType.SEND_EMAIL
        assert action.params["to"] == ["user@example.com"]
        assert action.params["subject"] == "Hello"
        assert action.params["html"] is False
        assert action.params["connection"] == "email_service"

    def test_email_factory_with_all_options(self):
        result = PluginResult.email(
            to=["a@b.com", "c@d.com"],
            subject="Hi",
            body="<b>bold</b>",
            html=True,
            cc="cc@d.com",
            bcc=["bcc@d.com"],
            reply_to="reply@d.com",
            template_id="tmpl-1",
            template_data={"name": "test"},
            connection="custom_email",
        )
        params = result.actions[0].params
        assert params["to"] == ["a@b.com", "c@d.com"]
        assert params["html"] is True
        assert params["cc"] == ["cc@d.com"]
        assert params["bcc"] == ["bcc@d.com"]
        assert params["reply_to"] == "reply@d.com"
        assert params["template_id"] == "tmpl-1"
        assert params["template_data"] == {"name": "test"}
        assert params["connection"] == "custom_email"

    def test_create_claimx_task_with_project_id(self):
        result = PluginResult.create_claimx_task(
            task_type="CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
            task_data={"customTaskName": "Test"},
            project_id=123,
        )
        params = result.actions[0].params
        assert params["project_id"] == 123
        assert params["task_type"] == "CUSTOM_TASK_ASSIGN_EXTERNAL_LINK"
        assert params["task_data"] == {"customTaskName": "Test"}

    def test_create_claimx_task_with_claim_number(self):
        result = PluginResult.create_claimx_task(
            task_type="CUSTOM_TASK",
            task_data={"name": "Task"},
            claim_number="CLM-001",
        )
        params = result.actions[0].params
        assert params["claim_number"] == "CLM-001"
        assert params["project_id"] is None

    def test_create_claimx_task_raises_without_id_or_claim(self):
        with pytest.raises(ValueError, match="project_id or claim_number"):
            PluginResult.create_claimx_task(task_type="CUSTOM_TASK", task_data={"name": "Task"})


# =====================
# PluginContext tests
# =====================


class TestPluginContext:
    def test_defaults(self):
        ctx = _make_context()
        assert ctx.data == {}
        assert ctx.headers == {}
        assert ctx.error is None
        assert ctx.retry_count == 0
        assert isinstance(ctx.timestamp, datetime)

    def test_get_claimx_entities_returns_none_when_empty(self):
        ctx = _make_context()
        assert ctx.get_claimx_entities() is None

    def test_get_claimx_entities_returns_value(self):
        sentinel = object()
        ctx = _make_context(data={"entities": sentinel})
        assert ctx.get_claimx_entities() is sentinel

    def test_get_tasks_returns_empty_when_no_entities(self):
        ctx = _make_context()
        assert ctx.get_tasks() == []

    def test_get_tasks_delegates_to_entities(self):
        entities = MagicMock()
        entities.tasks = [{"task_id": 1}, {"task_id": 2}]
        ctx = _make_context(data={"entities": entities})
        assert ctx.get_tasks() == [{"task_id": 1}, {"task_id": 2}]

    def test_get_first_task_returns_none_when_empty(self):
        ctx = _make_context()
        assert ctx.get_first_task() is None

    def test_get_first_task_returns_first(self):
        entities = MagicMock()
        entities.tasks = [{"task_id": 10}, {"task_id": 20}]
        ctx = _make_context(data={"entities": entities})
        assert ctx.get_first_task() == {"task_id": 10}


# =====================
# Plugin.should_run tests
# =====================


class TestPluginShouldRun:
    def test_no_filters_matches_everything(self):
        plugin = DummyPlugin()
        ctx = _make_context()
        assert plugin.should_run(ctx) is True

    def test_domain_filter_matches(self):
        plugin = DummyPlugin()
        plugin.domains = [Domain.CLAIMX]
        ctx = _make_context(domain=Domain.CLAIMX)
        assert plugin.should_run(ctx) is True

    def test_domain_filter_rejects(self):
        plugin = DummyPlugin()
        plugin.domains = [Domain.VERISK]
        ctx = _make_context(domain=Domain.CLAIMX)
        assert plugin.should_run(ctx) is False

    def test_stage_filter_matches(self):
        plugin = DummyPlugin()
        plugin.stages = [PipelineStage.ENRICHMENT_COMPLETE]
        ctx = _make_context(stage=PipelineStage.ENRICHMENT_COMPLETE)
        assert plugin.should_run(ctx) is True

    def test_stage_filter_rejects(self):
        plugin = DummyPlugin()
        plugin.stages = [PipelineStage.DLQ]
        ctx = _make_context(stage=PipelineStage.ENRICHMENT_COMPLETE)
        assert plugin.should_run(ctx) is False

    def test_event_type_filter_matches(self):
        plugin = DummyPlugin()
        plugin.event_types = ["CUSTOM_TASK_COMPLETED"]
        ctx = _make_context(event_type="CUSTOM_TASK_COMPLETED")
        assert plugin.should_run(ctx) is True

    def test_event_type_filter_rejects(self):
        plugin = DummyPlugin()
        plugin.event_types = ["CUSTOM_TASK_ASSIGNED"]
        ctx = _make_context(event_type="CUSTOM_TASK_COMPLETED")
        assert plugin.should_run(ctx) is False

    def test_event_type_none_passes_when_filter_empty(self):
        plugin = DummyPlugin()
        ctx = _make_context(event_type=None)
        assert plugin.should_run(ctx) is True

    def test_event_type_none_rejected_when_filter_set(self):
        plugin = DummyPlugin()
        plugin.event_types = ["CUSTOM_TASK_COMPLETED"]
        ctx = _make_context(event_type=None)
        assert plugin.should_run(ctx) is False

    def test_conditions_all_pass(self):
        entities = MagicMock()
        entities.tasks = [{"task_id": 1234, "status": "COMPLETED"}]
        plugin = DummyPlugin(config={
            "conditions": {
                "all": [
                    {"field": "task_id", "in": [1234, 4321]},
                    {"field": "event_type", "equals": "CUSTOM_TASK_COMPLETED"},
                ]
            }
        })
        ctx = _make_context(
            event_type="CUSTOM_TASK_COMPLETED",
            data={"entities": entities},
        )
        assert plugin.should_run(ctx) is True

    def test_conditions_all_fail(self):
        entities = MagicMock()
        entities.tasks = [{"task_id": 9999}]
        plugin = DummyPlugin(config={
            "conditions": {
                "all": [
                    {"field": "task_id", "in": [1234, 4321]},
                    {"field": "event_type", "equals": "CUSTOM_TASK_COMPLETED"},
                ]
            }
        })
        ctx = _make_context(
            event_type="CUSTOM_TASK_COMPLETED",
            data={"entities": entities},
        )
        assert plugin.should_run(ctx) is False

    def test_conditions_any_pass(self):
        entities = MagicMock()
        entities.tasks = [{"task_id": 4321}]
        plugin = DummyPlugin(config={
            "conditions": {
                "any": [
                    {"field": "task_id", "equals": 1234},
                    {"field": "task_id", "equals": 4321},
                ]
            }
        })
        ctx = _make_context(data={"entities": entities})
        assert plugin.should_run(ctx) is True

    def test_conditions_any_fail(self):
        entities = MagicMock()
        entities.tasks = [{"task_id": 9999}]
        plugin = DummyPlugin(config={
            "conditions": {
                "any": [
                    {"field": "task_id", "equals": 1234},
                    {"field": "task_id", "equals": 4321},
                ]
            }
        })
        ctx = _make_context(data={"entities": entities})
        assert plugin.should_run(ctx) is False

    def test_no_conditions_backward_compat(self):
        plugin = DummyPlugin(config={"some_other_key": "value"})
        ctx = _make_context()
        assert plugin.should_run(ctx) is True


# =====================
# Plugin config merge tests
# =====================


class TestPluginConfig:
    def test_default_config_used_when_none_provided(self):
        class ConfigPlugin(DummyPlugin):
            default_config = {"key": "default_value"}

        plugin = ConfigPlugin()
        assert plugin.config["key"] == "default_value"

    def test_config_overrides_defaults(self):
        class ConfigPlugin(DummyPlugin):
            default_config = {"key": "default", "other": "kept"}

        plugin = ConfigPlugin(config={"key": "override"})
        assert plugin.config["key"] == "override"
        assert plugin.config["other"] == "kept"


# =====================
# resolve_claimx_project_id tests
# =====================


class TestResolveClaimxProjectId:
    async def test_returns_int_response(self):
        conn_mgr = AsyncMock()
        response = AsyncMock()
        response.status = 200
        response.json.return_value = 42
        conn_mgr.request.return_value = response

        result = await resolve_claimx_project_id("CLM-001", conn_mgr)
        assert result == 42

    async def test_returns_dict_response_with_projectId(self):
        conn_mgr = AsyncMock()
        response = AsyncMock()
        response.status = 200
        response.json.return_value = {"projectId": 99}
        conn_mgr.request.return_value = response

        result = await resolve_claimx_project_id("CLM-002", conn_mgr)
        assert result == 99

    async def test_returns_dict_response_with_id(self):
        conn_mgr = AsyncMock()
        response = AsyncMock()
        response.status = 200
        response.json.return_value = {"id": 77}
        conn_mgr.request.return_value = response

        result = await resolve_claimx_project_id("CLM-003", conn_mgr)
        assert result == 77

    async def test_returns_none_on_http_error(self):
        conn_mgr = AsyncMock()
        response = AsyncMock()
        response.status = 404
        conn_mgr.request.return_value = response

        result = await resolve_claimx_project_id("CLM-004", conn_mgr)
        assert result is None

    async def test_returns_none_on_exception(self):
        conn_mgr = AsyncMock()
        conn_mgr.request.side_effect = Exception("network error")

        result = await resolve_claimx_project_id("CLM-005", conn_mgr)
        assert result is None

    async def test_returns_none_for_unexpected_response(self):
        conn_mgr = AsyncMock()
        response = AsyncMock()
        response.status = 200
        response.json.return_value = "unexpected_string"
        conn_mgr.request.return_value = response

        result = await resolve_claimx_project_id("CLM-006", conn_mgr)
        assert result is None

    async def test_uses_custom_connection_name(self):
        conn_mgr = AsyncMock()
        response = AsyncMock()
        response.status = 200
        response.json.return_value = 10
        conn_mgr.request.return_value = response

        await resolve_claimx_project_id("CLM-007", conn_mgr, connection_name="custom")
        conn_mgr.request.assert_called_once_with(
            connection_name="custom",
            method="GET",
            path="/export/project/projectId",
            params={"projectNumber": "CLM-007"},
        )


# =====================
# Enum value tests
# =====================


class TestEnumValues:
    def test_domain_values(self):
        assert Domain.VERISK.value == "verisk"
        assert Domain.CLAIMX.value == "claimx"

    def test_pipeline_stage_values(self):
        assert PipelineStage.EVENT_INGEST.value == "event_ingest"
        assert PipelineStage.DLQ.value == "dlq"

    def test_action_type_values(self):
        assert ActionType.PUBLISH_TO_TOPIC.value == "publish_to_topic"
        assert ActionType.SEND_EMAIL.value == "send_email"
        assert ActionType.CREATE_CLAIMX_TASK.value == "create_claimx_task"
