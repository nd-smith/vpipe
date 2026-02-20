"""Tests for YAML-driven plugin conditions."""

from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel

from pipeline.plugins.shared.base import Domain, PipelineStage, PluginContext
from pipeline.plugins.shared.conditions import (
    evaluate_condition,
    evaluate_conditions,
    resolve_field,
    validate_conditions,
)


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


def _make_context_with_task(task_dict, **overrides):
    entities = MagicMock()
    entities.tasks = [task_dict]
    return _make_context(data={"entities": entities}, **overrides)


# =====================
# resolve_field tests
# =====================


class TestResolveField:
    def test_domain(self):
        ctx = _make_context(domain=Domain.VERISK)
        assert resolve_field("domain", ctx) == "verisk"

    def test_stage(self):
        ctx = _make_context(stage=PipelineStage.DLQ)
        assert resolve_field("stage", ctx) == "dlq"

    def test_event_type(self):
        ctx = _make_context(event_type="CUSTOM_TASK_ASSIGNED")
        assert resolve_field("event_type", ctx) == "CUSTOM_TASK_ASSIGNED"

    def test_event_id(self):
        ctx = _make_context(event_id="evt-42")
        assert resolve_field("event_id", ctx) == "evt-42"

    def test_project_id(self):
        ctx = _make_context(project_id="proj-99")
        assert resolve_field("project_id", ctx) == "proj-99"

    def test_task_id(self):
        ctx = _make_context_with_task({"task_id": 1234})
        assert resolve_field("task_id", ctx) == 1234

    def test_task_status(self):
        ctx = _make_context_with_task({"status": "COMPLETED"})
        assert resolve_field("task_status", ctx) == "COMPLETED"

    def test_task_name(self):
        ctx = _make_context_with_task({"task_name": "Photo Review"})
        assert resolve_field("task_name", ctx) == "Photo Review"

    def test_assignment_id(self):
        ctx = _make_context_with_task({"assignment_id": "asgn-5"})
        assert resolve_field("assignment_id", ctx) == "asgn-5"

    def test_task_field_returns_none_when_no_task(self):
        ctx = _make_context()
        assert resolve_field("task_id", ctx) is None

    def test_unknown_field_raises(self):
        ctx = _make_context()
        with pytest.raises(ValueError, match="Unknown condition field"):
            resolve_field("bogus_field", ctx)


# =====================
# evaluate_condition tests
# =====================


class TestEvaluateCondition:
    def test_equals_match(self):
        ctx = _make_context(event_type="CUSTOM_TASK_COMPLETED")
        assert evaluate_condition({"field": "event_type", "equals": "CUSTOM_TASK_COMPLETED"}, ctx) is True

    def test_equals_reject(self):
        ctx = _make_context(event_type="CUSTOM_TASK_ASSIGNED")
        assert evaluate_condition({"field": "event_type", "equals": "CUSTOM_TASK_COMPLETED"}, ctx) is False

    def test_not_equals_match(self):
        ctx = _make_context(event_type="CUSTOM_TASK_ASSIGNED")
        assert evaluate_condition({"field": "event_type", "not_equals": "CUSTOM_TASK_COMPLETED"}, ctx) is True

    def test_not_equals_reject(self):
        ctx = _make_context(event_type="CUSTOM_TASK_COMPLETED")
        assert evaluate_condition({"field": "event_type", "not_equals": "CUSTOM_TASK_COMPLETED"}, ctx) is False

    def test_in_match(self):
        ctx = _make_context_with_task({"task_id": 1234})
        assert evaluate_condition({"field": "task_id", "in": [1234, 4321]}, ctx) is True

    def test_in_reject(self):
        ctx = _make_context_with_task({"task_id": 9999})
        assert evaluate_condition({"field": "task_id", "in": [1234, 4321]}, ctx) is False

    def test_not_in_match(self):
        ctx = _make_context_with_task({"task_id": 9999})
        assert evaluate_condition({"field": "task_id", "not_in": [1234, 4321]}, ctx) is True

    def test_not_in_reject(self):
        ctx = _make_context_with_task({"task_id": 1234})
        assert evaluate_condition({"field": "task_id", "not_in": [1234, 4321]}, ctx) is False

    def test_missing_operator_raises(self):
        ctx = _make_context()
        with pytest.raises(ValueError, match="no operator"):
            evaluate_condition({"field": "event_type"}, ctx)


# =====================
# evaluate_conditions tests
# =====================


class TestEvaluateConditions:
    def test_any_one_matches(self):
        ctx = _make_context_with_task({"task_id": 1234})
        config = {
            "any": [
                {"field": "task_id", "equals": 1234},
                {"field": "task_id", "equals": 9999},
            ]
        }
        assert evaluate_conditions(config, ctx) is True

    def test_any_none_match(self):
        ctx = _make_context_with_task({"task_id": 5555})
        config = {
            "any": [
                {"field": "task_id", "equals": 1234},
                {"field": "task_id", "equals": 9999},
            ]
        }
        assert evaluate_conditions(config, ctx) is False

    def test_all_all_match(self):
        ctx = _make_context_with_task(
            {"task_id": 1234},
            event_type="CUSTOM_TASK_COMPLETED",
        )
        config = {
            "all": [
                {"field": "task_id", "equals": 1234},
                {"field": "event_type", "equals": "CUSTOM_TASK_COMPLETED"},
            ]
        }
        assert evaluate_conditions(config, ctx) is True

    def test_all_one_fails(self):
        ctx = _make_context_with_task(
            {"task_id": 1234},
            event_type="CUSTOM_TASK_ASSIGNED",
        )
        config = {
            "all": [
                {"field": "task_id", "equals": 1234},
                {"field": "event_type", "equals": "CUSTOM_TASK_COMPLETED"},
            ]
        }
        assert evaluate_conditions(config, ctx) is False

    def test_missing_any_or_all_raises(self):
        ctx = _make_context()
        with pytest.raises(ValueError, match="'any' or 'all'"):
            evaluate_conditions({"neither": []}, ctx)


# =====================
# validate_conditions tests
# =====================


class TestValidateConditions:
    def test_valid_any_config(self):
        validate_conditions({
            "any": [
                {"field": "task_id", "in": [1, 2, 3]},
                {"field": "event_type", "equals": "CUSTOM_TASK_COMPLETED"},
            ]
        })

    def test_valid_all_config(self):
        validate_conditions({
            "all": [
                {"field": "domain", "equals": "claimx"},
                {"field": "stage", "not_equals": "dlq"},
            ]
        })

    def test_not_a_dict_raises(self):
        with pytest.raises(ValueError, match="must be a dict"):
            validate_conditions("bad")

    def test_missing_any_or_all_raises(self):
        with pytest.raises(ValueError, match="'any' or 'all'"):
            validate_conditions({"neither": []})

    def test_items_not_a_list_raises(self):
        with pytest.raises(ValueError, match="must be a list"):
            validate_conditions({"any": "not-a-list"})

    def test_item_not_a_dict_raises(self):
        with pytest.raises(ValueError, match="must be a dict"):
            validate_conditions({"any": ["not-a-dict"]})

    def test_missing_field_raises(self):
        with pytest.raises(ValueError, match="missing 'field'"):
            validate_conditions({"any": [{"equals": "x"}]})

    def test_unknown_field_raises(self):
        with pytest.raises(ValueError, match="unknown field"):
            validate_conditions({"any": [{"field": "bogus", "equals": "x"}]})

    def test_missing_operator_raises(self):
        with pytest.raises(ValueError, match="no operator"):
            validate_conditions({"any": [{"field": "task_id"}]})
