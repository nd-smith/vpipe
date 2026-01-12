"""
Tests for TaskTriggerPlugin.

Demonstrates how the plugin system works with task-based triggers.
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from kafka_pipeline.plugins.base import (
    PluginContext,
    PluginResult,
    ActionType,
    Domain,
    PipelineStage,
)
from kafka_pipeline.plugins.registry import (
    PluginRegistry,
    PluginOrchestrator,
    ActionExecutor,
    reset_plugin_registry,
)
from kafka_pipeline.plugins.task_trigger import (
    TaskTriggerPlugin,
    create_task_trigger_plugin,
)
from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage


@pytest.fixture
def registry():
    """Fresh registry for each test."""
    reset_plugin_registry()
    return PluginRegistry()


@pytest.fixture
def sample_task_data():
    """Sample task row as returned by TaskHandler."""
    return {
        "assignment_id": 12345,
        "task_id": 456,  # This is the template ID we'll trigger on
        "task_name": "Photo Documentation",
        "project_id": "99999",
        "assignee_id": 100,
        "assignor_email": "adjuster@insurance.com",
        "status": "ASSIGNED",
        "date_assigned": datetime.now(timezone.utc),
        "date_completed": None,
        "mfn": "CLM-2024-001",
    }


@pytest.fixture
def sample_event_message():
    """Mock ClaimX event message."""
    mock = MagicMock()
    mock.event_id = "evt_12345"
    mock.event_type = "CUSTOM_TASK_ASSIGNED"
    mock.project_id = "99999"
    mock.task_assignment_id = "12345"
    return mock


def create_context(
    event_message,
    task_data,
    event_type: str = "CUSTOM_TASK_ASSIGNED",
) -> PluginContext:
    """Helper to create plugin context with task data."""
    entities = EntityRowsMessage()
    entities.tasks.append(task_data)

    return PluginContext(
        domain=Domain.CLAIMX,
        stage=PipelineStage.ENRICHMENT_COMPLETE,
        message=event_message,
        event_id=event_message.event_id,
        event_type=event_type,
        project_id=event_message.project_id,
        data={"entities": entities},
    )


class TestTaskTriggerPlugin:
    """Tests for TaskTriggerPlugin."""

    def test_plugin_creation(self):
        """Test creating plugin with config."""
        plugin = TaskTriggerPlugin(config={
            "triggers": {
                456: {
                    "name": "Photo Task",
                    "on_assigned": {"publish_to_topic": "test-topic"},
                }
            }
        })

        assert plugin.name == "task_trigger"
        assert plugin.domains == [Domain.CLAIMX]
        assert plugin.stages == [PipelineStage.ENRICHMENT_COMPLETE]
        assert 456 in plugin.config["triggers"]

    def test_create_task_trigger_plugin_helper(self):
        """Test convenience function."""
        plugin = create_task_trigger_plugin({
            456: {
                "name": "Test Task",
                "on_completed": {"publish_to_topic": "test-complete"},
            }
        })

        assert isinstance(plugin, TaskTriggerPlugin)
        assert 456 in plugin.config["triggers"]

    @pytest.mark.asyncio
    async def test_trigger_matches_task_id(self, sample_event_message, sample_task_data):
        """Test that plugin triggers when task_id matches."""
        plugin = TaskTriggerPlugin(config={
            "triggers": {
                456: {
                    "name": "Photo Documentation",
                    "on_assigned": {"publish_to_topic": "photo-assigned"},
                }
            },
            "include_task_data": True,
        })

        context = create_context(
            sample_event_message,
            sample_task_data,
            event_type="CUSTOM_TASK_ASSIGNED",
        )

        result = await plugin.execute(context)

        assert result.success
        assert len(result.actions) == 1
        assert result.actions[0].action_type == ActionType.PUBLISH_TO_TOPIC
        assert result.actions[0].params["topic"] == "photo-assigned"

        # Check payload includes task data
        payload = result.actions[0].params["payload"]
        assert payload["task_id"] == 456
        assert payload["project_id"] == "99999"
        assert "task" in payload  # Full task data included

    @pytest.mark.asyncio
    async def test_trigger_on_completed(self, sample_event_message, sample_task_data):
        """Test trigger fires on CUSTOM_TASK_COMPLETED."""
        plugin = TaskTriggerPlugin(config={
            "triggers": {
                456: {
                    "name": "Photo Documentation",
                    "on_completed": {
                        "publish_to_topic": "photo-completed",
                        "webhook": "https://api.example.com/notify",
                    },
                }
            }
        })

        sample_task_data["status"] = "COMPLETED"
        sample_task_data["date_completed"] = datetime.now(timezone.utc)

        context = create_context(
            sample_event_message,
            sample_task_data,
            event_type="CUSTOM_TASK_COMPLETED",
        )

        result = await plugin.execute(context)

        assert result.success
        assert len(result.actions) == 2

        # Check publish action
        publish_action = result.actions[0]
        assert publish_action.action_type == ActionType.PUBLISH_TO_TOPIC
        assert publish_action.params["topic"] == "photo-completed"

        # Check webhook action
        webhook_action = result.actions[1]
        assert webhook_action.action_type == ActionType.HTTP_WEBHOOK
        assert webhook_action.params["url"] == "https://api.example.com/notify"

    @pytest.mark.asyncio
    async def test_no_trigger_for_unmatched_task_id(
        self, sample_event_message, sample_task_data
    ):
        """Test plugin skips when task_id doesn't match any trigger."""
        plugin = TaskTriggerPlugin(config={
            "triggers": {
                999: {  # Different task_id
                    "name": "Other Task",
                    "on_assigned": {"publish_to_topic": "other-topic"},
                }
            }
        })

        context = create_context(sample_event_message, sample_task_data)
        result = await plugin.execute(context)

        assert result.success
        assert len(result.actions) == 0
        assert "No trigger configured" in result.message

    @pytest.mark.asyncio
    async def test_no_trigger_for_wrong_event_type(
        self, sample_event_message, sample_task_data
    ):
        """Test plugin skips when event type doesn't match trigger config."""
        plugin = TaskTriggerPlugin(config={
            "triggers": {
                456: {
                    "name": "Photo Documentation",
                    # Only on_completed configured, not on_assigned
                    "on_completed": {"publish_to_topic": "photo-completed"},
                }
            }
        })

        # Event is ASSIGNED but only COMPLETED trigger configured
        context = create_context(
            sample_event_message,
            sample_task_data,
            event_type="CUSTOM_TASK_ASSIGNED",
        )

        result = await plugin.execute(context)

        assert result.success
        assert len(result.actions) == 0
        assert "No action configured" in result.message

    @pytest.mark.asyncio
    async def test_should_run_filtering(self, sample_event_message, sample_task_data):
        """Test should_run filters correctly."""
        plugin = TaskTriggerPlugin()

        # Should run for ClaimX + ENRICHMENT_COMPLETE + task events
        context = create_context(sample_event_message, sample_task_data)
        assert plugin.should_run(context) is True

        # Should not run for wrong domain
        context.domain = Domain.XACT
        assert plugin.should_run(context) is False

        # Should not run for wrong stage
        context.domain = Domain.CLAIMX
        context.stage = PipelineStage.EVENT_INGEST
        assert plugin.should_run(context) is False

        # Should not run for wrong event type
        context.stage = PipelineStage.ENRICHMENT_COMPLETE
        context.event_type = "PROJECT_CREATED"
        assert plugin.should_run(context) is False


class TestPluginOrchestrator:
    """Tests for PluginOrchestrator."""

    @pytest.mark.asyncio
    async def test_orchestrator_executes_plugins(
        self, registry, sample_event_message, sample_task_data
    ):
        """Test orchestrator runs registered plugins."""
        plugin = TaskTriggerPlugin(config={
            "triggers": {
                456: {
                    "name": "Test Trigger",
                    "on_assigned": {"publish_to_topic": "test-topic"},
                }
            }
        })
        registry.register(plugin)

        executor = ActionExecutor()  # No producer - will log only
        orchestrator = PluginOrchestrator(registry, executor)

        context = create_context(sample_event_message, sample_task_data)
        result = await orchestrator.execute(context)

        assert result.success_count == 1
        assert result.failure_count == 0
        assert result.actions_executed == 1

    @pytest.mark.asyncio
    async def test_orchestrator_multiple_plugins(
        self, registry, sample_event_message, sample_task_data
    ):
        """Test orchestrator runs multiple plugins in priority order."""
        # Higher priority (lower number) runs first
        plugin1 = TaskTriggerPlugin(config={
            "triggers": {456: {"name": "First", "on_assigned": {"log": "First triggered"}}}
        })
        plugin1.name = "first_trigger"
        plugin1.priority = 10

        plugin2 = TaskTriggerPlugin(config={
            "triggers": {456: {"name": "Second", "on_assigned": {"log": "Second triggered"}}}
        })
        plugin2.name = "second_trigger"
        plugin2.priority = 20

        registry.register(plugin2)  # Register out of order
        registry.register(plugin1)

        orchestrator = PluginOrchestrator(registry, ActionExecutor())
        context = create_context(sample_event_message, sample_task_data)

        result = await orchestrator.execute(context)

        assert result.success_count == 2
        # Both plugins executed
        plugin_names = [name for name, _ in result.results]
        assert "first_trigger" in plugin_names
        assert "second_trigger" in plugin_names


class TestPluginContext:
    """Tests for PluginContext helper methods."""

    def test_get_field_simple_path(self, sample_event_message, sample_task_data):
        """Test get_field with simple paths."""
        context = create_context(sample_event_message, sample_task_data)

        assert context.get_field("event_id") == "evt_12345"
        assert context.get_field("project_id") == "99999"
        assert context.get_field("event_type") == "CUSTOM_TASK_ASSIGNED"

    def test_get_field_nested_path(self, sample_event_message, sample_task_data):
        """Test get_field with nested paths."""
        context = create_context(sample_event_message, sample_task_data)

        # Access task data through entities
        task_id = context.get_field("data.entities.tasks[0].task_id")
        assert task_id == 456

        task_name = context.get_field("data.entities.tasks[0].task_name")
        assert task_name == "Photo Documentation"

    def test_get_first_task(self, sample_event_message, sample_task_data):
        """Test get_first_task helper."""
        context = create_context(sample_event_message, sample_task_data)

        task = context.get_first_task()
        assert task is not None
        assert task["task_id"] == 456
        assert task["task_name"] == "Photo Documentation"

    def test_get_tasks_empty(self, sample_event_message):
        """Test get_tasks with no task data."""
        entities = EntityRowsMessage()  # Empty

        context = PluginContext(
            domain=Domain.CLAIMX,
            stage=PipelineStage.ENRICHMENT_COMPLETE,
            message=sample_event_message,
            event_id="evt_123",
            data={"entities": entities},
        )

        assert context.get_tasks() == []
        assert context.get_first_task() is None
