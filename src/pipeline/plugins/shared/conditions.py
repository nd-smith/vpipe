"""
YAML-driven condition evaluation for plugin triggers.

Allows declarative conditions in plugin config to filter events
without writing Python subclasses.

Supported YAML syntax:
    conditions:
      any:                        # OR — at least one must match
        - field: task_id
          in: [1234, 4321]
        - field: project_id
          equals: "proj-99"

    conditions:
      all:                        # AND — all must match
        - field: task_id
          in: [1234, 4321]
        - field: event_type
          equals: CUSTOM_TASK_COMPLETED

Operators: equals, not_equals, in, not_in
"""

from typing import Any

from pipeline.plugins.shared.base import PluginContext

KNOWN_FIELDS = frozenset({
    "domain",
    "stage",
    "event_type",
    "event_id",
    "project_id",
    "task_id",
    "task_status",
    "task_name",
    "assignment_id",
})

OPERATORS = frozenset({"equals", "not_equals", "in", "not_in"})

# Fields resolved directly from PluginContext (using .value for enums)
_CONTEXT_FIELDS = {
    "domain": lambda ctx: ctx.domain.value,
    "stage": lambda ctx: ctx.stage.value,
    "event_type": lambda ctx: ctx.event_type,
    "event_id": lambda ctx: ctx.event_id,
    "project_id": lambda ctx: ctx.project_id,
}

# Fields resolved from the first task dict
_TASK_FIELD_KEYS = {
    "task_id": "task_id",
    "task_status": "status",
    "task_name": "task_name",
    "assignment_id": "assignment_id",
}


def resolve_field(field: str, context: PluginContext) -> Any:
    """Resolve a field name to its value from the plugin context."""
    if field not in KNOWN_FIELDS:
        raise ValueError(f"Unknown condition field: {field!r}. Known fields: {sorted(KNOWN_FIELDS)}")

    if field in _CONTEXT_FIELDS:
        return _CONTEXT_FIELDS[field](context)

    # Task fields — return None if no task present
    task = context.get_first_task()
    if task is None:
        return None
    return task.get(_TASK_FIELD_KEYS[field])


def evaluate_condition(condition: dict[str, Any], context: PluginContext) -> bool:
    """Evaluate a single condition dict against the context."""
    field = condition["field"]
    value = resolve_field(field, context)

    if "equals" in condition:
        return value == condition["equals"]
    if "not_equals" in condition:
        return value != condition["not_equals"]
    if "in" in condition:
        return value in condition["in"]
    if "not_in" in condition:
        return value not in condition["not_in"]

    raise ValueError(
        f"Condition for field {field!r} has no operator. "
        f"Use one of: {sorted(OPERATORS)}"
    )


def evaluate_conditions(conditions_config: dict[str, Any], context: PluginContext) -> bool:
    """Evaluate a conditions block (any/all) against the context."""
    if "any" in conditions_config:
        return any(evaluate_condition(c, context) for c in conditions_config["any"])
    if "all" in conditions_config:
        return all(evaluate_condition(c, context) for c in conditions_config["all"])

    raise ValueError("Conditions config must contain 'any' or 'all' key")


def validate_conditions(conditions_config: dict[str, Any]) -> None:
    """Validate conditions config at load time. Raises ValueError on bad config."""
    if not isinstance(conditions_config, dict):
        raise ValueError("Conditions must be a dict with 'any' or 'all' key")

    if "any" not in conditions_config and "all" not in conditions_config:
        raise ValueError("Conditions config must contain 'any' or 'all' key")

    key = "any" if "any" in conditions_config else "all"
    items = conditions_config[key]

    if not isinstance(items, list):
        raise ValueError(f"conditions.{key} must be a list")

    for i, condition in enumerate(items):
        if not isinstance(condition, dict):
            raise ValueError(f"conditions.{key}[{i}] must be a dict")

        if "field" not in condition:
            raise ValueError(f"conditions.{key}[{i}] missing 'field'")

        field = condition["field"]
        if field not in KNOWN_FIELDS:
            raise ValueError(
                f"conditions.{key}[{i}]: unknown field {field!r}. "
                f"Known fields: {sorted(KNOWN_FIELDS)}"
            )

        has_operator = any(op in condition for op in OPERATORS)
        if not has_operator:
            raise ValueError(
                f"conditions.{key}[{i}]: no operator for field {field!r}. "
                f"Use one of: {sorted(OPERATORS)}"
            )
