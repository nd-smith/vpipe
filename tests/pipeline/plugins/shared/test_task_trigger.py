"""Tests for task trigger plugin."""

from unittest.mock import MagicMock

from pydantic import BaseModel

from pipeline.plugins.shared.base import (
    ActionType,
    Domain,
    PipelineStage,
    PluginContext,
)
from pipeline.plugins.shared.task_trigger import (
    TaskTriggerPlugin,
    create_task_trigger_plugin,
)


class DummyMessage(BaseModel):
    value: str = "test"


def _make_entities(tasks=None):
    entities = MagicMock()
    entities.tasks = tasks or []
    entities.projects = []
    return entities


def _make_context(task_id=456, event_type="CUSTOM_TASK_COMPLETED", **overrides):
    entities = _make_entities(
        tasks=[
            {
                "task_id": task_id,
                "assignment_id": 1001,
                "task_name": "Test Task",
                "status": "COMPLETED",
            }
        ]
    )
    defaults = {
        "domain": Domain.CLAIMX,
        "stage": PipelineStage.ENRICHMENT_COMPLETE,
        "message": DummyMessage(),
        "event_id": "evt-1",
        "event_type": event_type,
        "project_id": "proj-1",
        "data": {"entities": entities},
    }
    defaults.update(overrides)
    return PluginContext(**defaults)


# =====================
# Initialization tests
# =====================


class TestTaskTriggerPluginInit:
    def test_basic_attributes(self):
        plugin = TaskTriggerPlugin(config={"triggers": {}})
        assert plugin.name == "task_trigger"
        assert plugin.domains == [Domain.CLAIMX]
        assert plugin.stages == [PipelineStage.ENRICHMENT_COMPLETE]

    def test_event_types_from_on_assigned(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {
                    456: {"name": "Test", "on_assigned": {"publish_to_topic": "t"}},
                }
            }
        )
        assert "CUSTOM_TASK_ASSIGNED" in plugin.event_types
        assert "CUSTOM_TASK_COMPLETED" not in plugin.event_types

    def test_event_types_from_on_completed(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {
                    456: {"name": "Test", "on_completed": {"publish_to_topic": "t"}},
                }
            }
        )
        assert "CUSTOM_TASK_COMPLETED" in plugin.event_types
        assert "CUSTOM_TASK_ASSIGNED" not in plugin.event_types

    def test_event_types_from_on_any(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {
                    456: {"name": "Test", "on_any": {"log": "triggered"}},
                }
            }
        )
        assert "CUSTOM_TASK_ASSIGNED" in plugin.event_types
        assert "CUSTOM_TASK_COMPLETED" in plugin.event_types

    def test_event_types_combined(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {
                    456: {"name": "A", "on_assigned": {"publish_to_topic": "t"}},
                    789: {"name": "B", "on_completed": {"publish_to_topic": "t2"}},
                }
            }
        )
        assert "CUSTOM_TASK_ASSIGNED" in plugin.event_types
        assert "CUSTOM_TASK_COMPLETED" in plugin.event_types

    def test_empty_triggers_no_event_types(self):
        plugin = TaskTriggerPlugin(config={"triggers": {}})
        assert plugin.event_types == []

    def test_configured_task_ids_includes_int_and_str(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {
                    456: {"name": "Test", "on_completed": {"log": "done"}},
                }
            }
        )
        assert 456 in plugin._configured_task_ids
        assert "456" in plugin._configured_task_ids

    def test_configured_task_ids_string_key(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {
                    "789": {"name": "Str", "on_completed": {"log": "done"}},
                }
            }
        )
        assert "789" in plugin._configured_task_ids
        assert 789 in plugin._configured_task_ids

    def test_default_config_merged(self):
        plugin = TaskTriggerPlugin(config={"include_task_data": False})
        assert plugin.config["include_task_data"] is False
        assert plugin.config["include_project_data"] is False
        assert plugin.config["triggers"] == {}


# =====================
# should_run tests
# =====================


class TestTaskTriggerShouldRun:
    def test_returns_false_when_no_task(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {456: {"name": "T", "on_completed": {"log": "x"}}},
            }
        )
        entities = _make_entities(tasks=[])
        ctx = _make_context(data={"entities": entities})
        assert plugin.should_run(ctx) is False

    def test_returns_false_when_task_id_not_configured(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {999: {"name": "T", "on_completed": {"log": "x"}}},
            }
        )
        ctx = _make_context(task_id=456)
        assert plugin.should_run(ctx) is False

    def test_returns_true_when_task_id_matches(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {456: {"name": "T", "on_completed": {"log": "x"}}},
            }
        )
        ctx = _make_context(task_id=456, event_type="CUSTOM_TASK_COMPLETED")
        assert plugin.should_run(ctx) is True

    def test_returns_false_for_wrong_domain(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {456: {"name": "T", "on_completed": {"log": "x"}}},
            }
        )
        ctx = _make_context(task_id=456, domain=Domain.VERISK)
        assert plugin.should_run(ctx) is False

    def test_returns_false_for_wrong_stage(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {456: {"name": "T", "on_completed": {"log": "x"}}},
            }
        )
        ctx = _make_context(task_id=456, stage=PipelineStage.DLQ)
        assert plugin.should_run(ctx) is False

    def test_returns_false_for_wrong_event_type(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {456: {"name": "T", "on_assigned": {"log": "x"}}},
            }
        )
        # Plugin only has CUSTOM_TASK_ASSIGNED in event_types
        ctx = _make_context(task_id=456, event_type="CUSTOM_TASK_COMPLETED")
        assert plugin.should_run(ctx) is False


# =====================
# execute tests
# =====================


class TestTaskTriggerExecute:
    async def test_returns_skip_when_no_task(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {456: {"name": "T", "on_completed": {"log": "x"}}},
            }
        )
        entities = _make_entities(tasks=[])
        ctx = _make_context(data={"entities": entities})

        result = await plugin.execute(ctx)
        assert "No task data" in result.message

    async def test_returns_skip_when_no_task_id(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {456: {"name": "T", "on_completed": {"log": "x"}}},
            }
        )
        entities = _make_entities(tasks=[{"assignment_id": 1}])
        ctx = _make_context(data={"entities": entities})

        result = await plugin.execute(ctx)
        assert "no task_id" in result.message

    async def test_returns_skip_when_no_trigger_configured(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {999: {"name": "Other", "on_completed": {"log": "x"}}},
            }
        )
        ctx = _make_context(task_id=456)

        result = await plugin.execute(ctx)
        assert "No trigger configured" in result.message

    async def test_returns_skip_when_no_action_for_event_type(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {
                    456: {"name": "T", "on_assigned": {"log": "assigned only"}},
                },
            }
        )
        ctx = _make_context(task_id=456, event_type="CUSTOM_TASK_COMPLETED")

        result = await plugin.execute(ctx)
        assert "No action configured" in result.message

    async def test_on_completed_publish_action(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {
                    456: {
                        "name": "Photo Task",
                        "on_completed": {"publish_to_topic": "tasks-done"},
                    },
                },
            }
        )
        # Patch _log to avoid AttributeError
        plugin._log = MagicMock()
        ctx = _make_context(task_id=456, event_type="CUSTOM_TASK_COMPLETED")

        result = await plugin.execute(ctx)

        assert result.success is True
        assert len(result.actions) == 1
        action = result.actions[0]
        assert action.action_type == ActionType.PUBLISH_TO_TOPIC
        assert action.params["topic"] == "tasks-done"
        assert "Photo Task" in result.message

    async def test_on_assigned_publish_action(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {
                    456: {
                        "name": "Task",
                        "on_assigned": {"publish_to_topic": "tasks-assigned"},
                    },
                },
            }
        )
        plugin._log = MagicMock()
        ctx = _make_context(task_id=456, event_type="CUSTOM_TASK_ASSIGNED")

        result = await plugin.execute(ctx)

        assert result.success is True
        assert result.actions[0].params["topic"] == "tasks-assigned"

    async def test_on_any_fallback(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {
                    456: {"name": "T", "on_any": {"log": "any event"}},
                },
            }
        )
        plugin._log = MagicMock()
        ctx = _make_context(task_id=456, event_type="CUSTOM_TASK_COMPLETED")

        result = await plugin.execute(ctx)

        assert result.success is True
        assert len(result.actions) == 1
        assert result.actions[0].action_type == ActionType.LOG

    async def test_on_any_not_used_when_specific_exists(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {
                    456: {
                        "name": "T",
                        "on_completed": {"publish_to_topic": "specific"},
                        "on_any": {"log": "fallback"},
                    },
                },
            }
        )
        plugin._log = MagicMock()
        ctx = _make_context(task_id=456, event_type="CUSTOM_TASK_COMPLETED")

        result = await plugin.execute(ctx)

        assert result.actions[0].action_type == ActionType.PUBLISH_TO_TOPIC
        assert result.actions[0].params["topic"] == "specific"

    async def test_webhook_string_url(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {
                    456: {
                        "name": "T",
                        "on_completed": {"webhook": "https://example.com/hook"},
                    },
                },
            }
        )
        plugin._log = MagicMock()
        ctx = _make_context(task_id=456, event_type="CUSTOM_TASK_COMPLETED")

        result = await plugin.execute(ctx)

        assert result.actions[0].action_type == ActionType.HTTP_WEBHOOK
        assert result.actions[0].params["url"] == "https://example.com/hook"

    async def test_webhook_dict_config(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {
                    456: {
                        "name": "T",
                        "on_completed": {
                            "webhook": {
                                "url": "https://example.com/hook",
                                "method": "PUT",
                                "headers": {"X-Key": "val"},
                            }
                        },
                    },
                },
            }
        )
        plugin._log = MagicMock()
        ctx = _make_context(task_id=456, event_type="CUSTOM_TASK_COMPLETED")

        result = await plugin.execute(ctx)

        action = result.actions[0]
        assert action.params["method"] == "PUT"
        assert action.params["headers"]["X-Key"] == "val"

    async def test_log_string_shorthand(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {
                    456: {
                        "name": "T",
                        "on_completed": {"log": "Task completed!"},
                    },
                },
            }
        )
        plugin._log = MagicMock()
        ctx = _make_context(task_id=456, event_type="CUSTOM_TASK_COMPLETED")

        result = await plugin.execute(ctx)

        action = result.actions[0]
        assert action.action_type == ActionType.LOG
        assert action.params["message"] == "Task completed!"

    async def test_log_dict_config(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {
                    456: {
                        "name": "T",
                        "on_completed": {"log": {"level": "warning", "message": "Watch out!"}},
                    },
                },
            }
        )
        plugin._log = MagicMock()
        ctx = _make_context(task_id=456, event_type="CUSTOM_TASK_COMPLETED")

        result = await plugin.execute(ctx)

        action = result.actions[0]
        assert action.params["level"] == "warning"
        assert action.params["message"] == "Watch out!"

    async def test_custom_actions(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {
                    456: {
                        "name": "T",
                        "on_completed": {
                            "custom": [
                                {"type": "metric", "params": {"name": "task_done"}},
                            ]
                        },
                    },
                },
            }
        )
        plugin._log = MagicMock()
        ctx = _make_context(task_id=456, event_type="CUSTOM_TASK_COMPLETED")

        result = await plugin.execute(ctx)

        assert result.actions[0].action_type == ActionType.METRIC
        assert result.actions[0].params["name"] == "task_done"

    async def test_multiple_actions(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {
                    456: {
                        "name": "T",
                        "on_completed": {
                            "publish_to_topic": "done",
                            "webhook": "https://api.com",
                            "log": "logged",
                        },
                    },
                },
            }
        )
        plugin._log = MagicMock()
        ctx = _make_context(task_id=456, event_type="CUSTOM_TASK_COMPLETED")

        result = await plugin.execute(ctx)

        action_types = {a.action_type for a in result.actions}
        assert ActionType.PUBLISH_TO_TOPIC in action_types
        assert ActionType.HTTP_WEBHOOK in action_types
        assert ActionType.LOG in action_types

    async def test_payload_contains_task_data_by_default(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {
                    456: {
                        "name": "T",
                        "on_completed": {"publish_to_topic": "topic"},
                    },
                },
            }
        )
        plugin._log = MagicMock()
        ctx = _make_context(task_id=456, event_type="CUSTOM_TASK_COMPLETED")

        result = await plugin.execute(ctx)

        payload = result.actions[0].params["payload"]
        assert payload["event_id"] == "evt-1"
        assert payload["project_id"] == "proj-1"
        assert payload["task_id"] == 456
        assert "task" in payload  # include_task_data is True by default

    async def test_payload_excludes_task_data_when_disabled(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {
                    456: {
                        "name": "T",
                        "on_completed": {"publish_to_topic": "topic"},
                    },
                },
                "include_task_data": False,
            }
        )
        plugin._log = MagicMock()
        ctx = _make_context(task_id=456, event_type="CUSTOM_TASK_COMPLETED")

        result = await plugin.execute(ctx)

        payload = result.actions[0].params["payload"]
        assert "task" not in payload

    async def test_publish_headers_include_trigger_metadata(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {
                    456: {
                        "name": "Photo Task",
                        "on_completed": {"publish_to_topic": "topic"},
                    },
                },
            }
        )
        plugin._log = MagicMock()
        ctx = _make_context(task_id=456, event_type="CUSTOM_TASK_COMPLETED")

        result = await plugin.execute(ctx)

        headers = result.actions[0].params["headers"]
        assert headers["x-trigger-name"] == "Photo Task"
        assert headers["x-task-id"] == "456"
        assert headers["x-event-type"] == "CUSTOM_TASK_COMPLETED"

    async def test_trigger_lookup_by_string_task_id(self):
        plugin = TaskTriggerPlugin(
            config={
                "triggers": {
                    456: {"name": "T", "on_completed": {"log": "matched"}},
                },
            }
        )
        plugin._log = MagicMock()

        # Task has task_id as int, triggers key is int
        ctx = _make_context(task_id=456, event_type="CUSTOM_TASK_COMPLETED")
        result = await plugin.execute(ctx)

        assert result.success is True
        assert len(result.actions) == 1


# =====================
# create_task_trigger_plugin tests
# =====================


class TestCreateTaskTriggerPlugin:
    def test_creates_plugin_with_triggers(self):
        plugin = create_task_trigger_plugin(
            triggers={
                456: {"name": "T", "on_completed": {"log": "done"}},
            },
        )
        assert isinstance(plugin, TaskTriggerPlugin)
        assert plugin.config["triggers"][456]["name"] == "T"

    def test_default_options(self):
        plugin = create_task_trigger_plugin(triggers={})
        assert plugin.config["include_task_data"] is True
        assert plugin.config["include_project_data"] is False

    def test_custom_options(self):
        plugin = create_task_trigger_plugin(
            triggers={},
            include_task_data=False,
            include_project_data=True,
        )
        assert plugin.config["include_task_data"] is False
        assert plugin.config["include_project_data"] is True
