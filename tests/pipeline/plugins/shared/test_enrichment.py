"""Tests for enrichment framework."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from pipeline.plugins.shared.enrichment import (
    BUILTIN_HANDLERS,
    BatchingHandler,
    EnrichmentContext,
    EnrichmentHandler,
    EnrichmentPipeline,
    EnrichmentResult,
    LookupHandler,
    TransformHandler,
    ValidationHandler,
    create_handler_from_config,
)

# =====================
# EnrichmentContext tests
# =====================


class TestEnrichmentContext:
    def test_get_returns_value(self):
        ctx = EnrichmentContext(data={"key": "val"})
        assert ctx.get("key") == "val"

    def test_get_returns_default_for_missing(self):
        ctx = EnrichmentContext(data={})
        assert ctx.get("missing") is None
        assert ctx.get("missing", "fallback") == "fallback"

    def test_set_stores_value(self):
        ctx = EnrichmentContext()
        ctx.set("key", 42)
        assert ctx.data["key"] == 42

    def test_defaults(self):
        ctx = EnrichmentContext()
        assert ctx.data == {}
        assert ctx.metadata == {}
        assert ctx.connection_manager is None


# =====================
# EnrichmentResult tests
# =====================


class TestEnrichmentResult:
    def test_ok_factory(self):
        result = EnrichmentResult.ok({"enriched": True})
        assert result.success is True
        assert result.data == {"enriched": True}
        assert result.skip is False
        assert result.error is None

    def test_skip_message_factory(self):
        result = EnrichmentResult.skip_message("not needed")
        assert result.success is True
        assert result.skip is True

    def test_failed_factory(self):
        result = EnrichmentResult.failed("bad input")
        assert result.success is False
        assert result.error == "bad input"


# =====================
# TransformHandler tests
# =====================


class TestTransformHandler:
    async def test_simple_field_mapping(self):
        handler = TransformHandler(
            config={
                "mappings": {"output_name": "input_name"},
            }
        )
        ctx = EnrichmentContext(data={"input_name": "value"})
        result = await handler.enrich(ctx)

        assert result.success is True
        assert result.data["output_name"] == "value"

    async def test_nested_field_access(self):
        handler = TransformHandler(
            config={
                "mappings": {"project_name": "task.project.name"},
            }
        )
        ctx = EnrichmentContext(data={"task": {"project": {"name": "MyProject"}}})
        result = await handler.enrich(ctx)

        assert result.data["project_name"] == "MyProject"

    async def test_uses_default_when_field_missing(self):
        handler = TransformHandler(
            config={
                "mappings": {"status": "missing_field"},
                "defaults": {"status": "unknown"},
            }
        )
        ctx = EnrichmentContext(data={})
        result = await handler.enrich(ctx)

        assert result.data["status"] == "unknown"

    async def test_defaults_applied_for_unmapped_keys(self):
        handler = TransformHandler(
            config={
                "mappings": {},
                "defaults": {"priority": "normal"},
            }
        )
        ctx = EnrichmentContext(data={})
        result = await handler.enrich(ctx)

        assert result.data["priority"] == "normal"

    async def test_mapping_value_overrides_default(self):
        handler = TransformHandler(
            config={
                "mappings": {"status": "status_field"},
                "defaults": {"status": "unknown"},
            }
        )
        ctx = EnrichmentContext(data={"status_field": "active"})
        result = await handler.enrich(ctx)

        assert result.data["status"] == "active"

    async def test_nested_field_returns_none_for_non_dict(self):
        handler = TransformHandler(
            config={
                "mappings": {"val": "a.b.c"},
                "defaults": {"val": "fallback"},
            }
        )
        ctx = EnrichmentContext(data={"a": "not_a_dict"})
        result = await handler.enrich(ctx)

        assert result.data["val"] == "fallback"

    async def test_nested_field_returns_none_when_intermediate_missing(self):
        handler = TransformHandler(
            config={
                "mappings": {"val": "a.b.c"},
            }
        )
        ctx = EnrichmentContext(data={"a": {"x": 1}})
        result = await handler.enrich(ctx)

        # val should not be set (no default)
        assert "val" not in result.data

    async def test_merges_with_existing_data(self):
        handler = TransformHandler(
            config={
                "mappings": {"new_field": "source"},
            }
        )
        ctx = EnrichmentContext(data={"source": "val", "existing": "keep"})
        result = await handler.enrich(ctx)

        assert result.data["existing"] == "keep"
        assert result.data["new_field"] == "val"

    async def test_empty_mappings_and_defaults(self):
        handler = TransformHandler(config={})
        ctx = EnrichmentContext(data={"original": "data"})
        result = await handler.enrich(ctx)

        assert result.success is True
        assert result.data["original"] == "data"

    def test_handler_name(self):
        handler = TransformHandler()
        assert handler.name == "TransformHandler"


# =====================
# ValidationHandler tests
# =====================


class TestValidationHandler:
    async def test_required_fields_pass(self):
        handler = ValidationHandler(
            config={
                "required_fields": ["project_id", "task_id"],
            }
        )
        ctx = EnrichmentContext(data={"project_id": "1", "task_id": "2"})
        result = await handler.enrich(ctx)

        assert result.success is True

    async def test_required_fields_fail(self):
        handler = ValidationHandler(
            config={
                "required_fields": ["project_id", "missing_field"],
            }
        )
        ctx = EnrichmentContext(data={"project_id": "1"})
        result = await handler.enrich(ctx)

        assert result.success is False
        assert "missing_field" in result.error

    async def test_required_nested_field(self):
        handler = ValidationHandler(
            config={
                "required_fields": ["task.id"],
            }
        )
        ctx = EnrichmentContext(data={"task": {"id": 123}})
        result = await handler.enrich(ctx)

        assert result.success is True

    async def test_allowed_values_pass(self):
        handler = ValidationHandler(
            config={
                "field_rules": {
                    "status": {"allowed_values": ["active", "pending"]},
                },
            }
        )
        ctx = EnrichmentContext(data={"status": "active"})
        result = await handler.enrich(ctx)

        assert result.success is True

    async def test_allowed_values_fail(self):
        handler = ValidationHandler(
            config={
                "field_rules": {
                    "status": {"allowed_values": ["active", "pending"]},
                },
            }
        )
        ctx = EnrichmentContext(data={"status": "deleted"})
        result = await handler.enrich(ctx)

        assert result.success is False
        assert "not in allowed values" in result.error

    async def test_min_value_pass(self):
        handler = ValidationHandler(
            config={
                "field_rules": {"priority": {"min": 1}},
            }
        )
        ctx = EnrichmentContext(data={"priority": 5})
        result = await handler.enrich(ctx)

        assert result.success is True

    async def test_min_value_fail(self):
        handler = ValidationHandler(
            config={
                "field_rules": {"priority": {"min": 1}},
            }
        )
        ctx = EnrichmentContext(data={"priority": 0})
        result = await handler.enrich(ctx)

        assert result.success is False
        assert "below minimum" in result.error

    async def test_max_value_pass(self):
        handler = ValidationHandler(
            config={
                "field_rules": {"priority": {"max": 10}},
            }
        )
        ctx = EnrichmentContext(data={"priority": 10})
        result = await handler.enrich(ctx)

        assert result.success is True

    async def test_max_value_fail(self):
        handler = ValidationHandler(
            config={
                "field_rules": {"priority": {"max": 10}},
            }
        )
        ctx = EnrichmentContext(data={"priority": 11})
        result = await handler.enrich(ctx)

        assert result.success is False
        assert "above maximum" in result.error

    async def test_skip_if_condition_triggers(self):
        handler = ValidationHandler(
            config={
                "skip_if": {"field": "task_status", "equals": "cancelled"},
            }
        )
        ctx = EnrichmentContext(data={"task_status": "cancelled"})
        result = await handler.enrich(ctx)

        assert result.skip is True

    async def test_skip_if_condition_does_not_trigger(self):
        handler = ValidationHandler(
            config={
                "skip_if": {"field": "task_status", "equals": "cancelled"},
            }
        )
        ctx = EnrichmentContext(data={"task_status": "active"})
        result = await handler.enrich(ctx)

        assert result.success is True
        assert result.skip is False

    async def test_field_rule_skipped_when_field_not_present(self):
        handler = ValidationHandler(
            config={
                "field_rules": {"optional_field": {"min": 1}},
            }
        )
        ctx = EnrichmentContext(data={})
        result = await handler.enrich(ctx)

        assert result.success is True

    async def test_nested_field_value_access(self):
        handler = ValidationHandler(
            config={
                "required_fields": ["task.status"],
            }
        )
        ctx = EnrichmentContext(data={"task": {"status": "done"}})
        result = await handler.enrich(ctx)

        assert result.success is True

    async def test_nested_field_missing_intermediate(self):
        handler = ValidationHandler(
            config={
                "required_fields": ["task.deep.field"],
            }
        )
        ctx = EnrichmentContext(data={"task": "not_a_dict"})
        result = await handler.enrich(ctx)

        assert result.success is False


# =====================
# LookupHandler tests
# =====================


class TestLookupHandler:
    async def test_successful_lookup(self):
        conn_mgr = AsyncMock()
        conn_mgr.request_json.return_value = (200, {"name": "Project X"})

        handler = LookupHandler(
            config={
                "connection": "ext_api",
                "endpoint": "/v1/projects/{id}",
                "path_params": {"id": "project_id"},
                "result_field": "project_details",
            }
        )
        ctx = EnrichmentContext(
            data={"project_id": "42"},
            connection_manager=conn_mgr,
        )
        result = await handler.enrich(ctx)

        assert result.success is True
        assert result.data["project_details"] == {"name": "Project X"}

    async def test_no_connection_manager(self):
        handler = LookupHandler(config={"connection": "api"})
        ctx = EnrichmentContext(data={})
        result = await handler.enrich(ctx)

        assert result.success is False
        assert "No connection manager" in result.error

    async def test_no_connection_in_config(self):
        conn_mgr = AsyncMock()
        handler = LookupHandler(config={})
        ctx = EnrichmentContext(data={}, connection_manager=conn_mgr)
        result = await handler.enrich(ctx)

        assert result.success is False
        assert "No connection specified" in result.error

    async def test_missing_path_param_field(self):
        conn_mgr = AsyncMock()
        handler = LookupHandler(
            config={
                "connection": "api",
                "endpoint": "/v1/{id}",
                "path_params": {"id": "missing_field"},
            }
        )
        ctx = EnrichmentContext(data={}, connection_manager=conn_mgr)
        result = await handler.enrich(ctx)

        assert result.success is False
        assert "not found" in result.error

    async def test_api_error_status(self):
        conn_mgr = AsyncMock()
        conn_mgr.request_json.return_value = (404, {"error": "Not Found"})

        handler = LookupHandler(
            config={
                "connection": "api",
                "endpoint": "/v1/items",
            }
        )
        ctx = EnrichmentContext(data={}, connection_manager=conn_mgr)
        result = await handler.enrich(ctx)

        assert result.success is False
        assert "status 404" in result.error

    async def test_exception_returns_failed(self):
        conn_mgr = AsyncMock()
        conn_mgr.request_json.side_effect = ConnectionError("timeout")

        handler = LookupHandler(
            config={
                "connection": "api",
                "endpoint": "/v1/items",
            }
        )
        ctx = EnrichmentContext(data={}, connection_manager=conn_mgr)
        result = await handler.enrich(ctx)

        assert result.success is False
        assert "Lookup exception" in result.error

    async def test_caching(self):
        conn_mgr = AsyncMock()
        conn_mgr.request_json.return_value = (200, {"cached": True})

        handler = LookupHandler(
            config={
                "connection": "api",
                "endpoint": "/v1/data",
                "cache_ttl": 300,
            }
        )
        ctx = EnrichmentContext(data={}, connection_manager=conn_mgr)

        # First call - fetches from API
        await handler.enrich(ctx)
        assert conn_mgr.request_json.await_count == 1

        # Second call - should use cache
        ctx2 = EnrichmentContext(data={}, connection_manager=conn_mgr)
        result = await handler.enrich(ctx2)
        assert conn_mgr.request_json.await_count == 1  # Not called again
        assert result.data["lookup_result"] == {"cached": True}

    async def test_default_result_field(self):
        conn_mgr = AsyncMock()
        conn_mgr.request_json.return_value = (200, {"data": "test"})

        handler = LookupHandler(
            config={
                "connection": "api",
                "endpoint": "/v1/data",
            }
        )
        ctx = EnrichmentContext(data={}, connection_manager=conn_mgr)
        result = await handler.enrich(ctx)

        assert "lookup_result" in result.data

    async def test_query_params_literal_values(self):
        conn_mgr = AsyncMock()
        conn_mgr.request_json.return_value = (200, {})

        handler = LookupHandler(
            config={
                "connection": "api",
                "endpoint": "/v1/data",
                "query_params": {"include": "details"},
            }
        )
        ctx = EnrichmentContext(data={}, connection_manager=conn_mgr)
        await handler.enrich(ctx)

        call_kwargs = conn_mgr.request_json.call_args.kwargs
        assert call_kwargs["params"]["include"] == "details"

    async def test_query_params_field_reference(self):
        conn_mgr = AsyncMock()
        conn_mgr.request_json.return_value = (200, {})

        handler = LookupHandler(
            config={
                "connection": "api",
                "endpoint": "/v1/data",
                "query_params": {"filter_id": "project_id"},
            }
        )
        ctx = EnrichmentContext(
            data={"project_id": "42"},
            connection_manager=conn_mgr,
        )
        await handler.enrich(ctx)

        call_kwargs = conn_mgr.request_json.call_args.kwargs
        assert call_kwargs["params"]["filter_id"] == "42"


# =====================
# BatchingHandler tests
# =====================


class TestBatchingHandler:
    async def test_accumulates_until_batch_size(self):
        handler = BatchingHandler(config={"batch_size": 3})

        ctx1 = EnrichmentContext(data={"id": 1})
        result1 = await handler.enrich(ctx1)
        assert result1.skip is True

        ctx2 = EnrichmentContext(data={"id": 2})
        result2 = await handler.enrich(ctx2)
        assert result2.skip is True

        ctx3 = EnrichmentContext(data={"id": 3})
        result3 = await handler.enrich(ctx3)
        assert result3.success is True
        assert result3.skip is False
        assert result3.data["batch_size"] == 3
        assert len(result3.data["items"]) == 3

    async def test_custom_batch_field(self):
        handler = BatchingHandler(
            config={
                "batch_size": 1,
                "batch_field": "records",
            }
        )
        ctx = EnrichmentContext(data={"id": 1})
        result = await handler.enrich(ctx)

        assert "records" in result.data

    async def test_flush_returns_accumulated_data(self):
        handler = BatchingHandler(config={"batch_size": 100})

        ctx1 = EnrichmentContext(data={"id": 1})
        await handler.enrich(ctx1)
        ctx2 = EnrichmentContext(data={"id": 2})
        await handler.enrich(ctx2)

        batch = await handler.flush()
        assert batch is not None
        assert batch["batch_size"] == 2

    async def test_flush_returns_none_when_empty(self):
        handler = BatchingHandler(config={"batch_size": 100})
        batch = await handler.flush()
        assert batch is None

    async def test_copies_data_shallow(self):
        """BatchingHandler uses dict.copy() which is a shallow copy.

        Top-level keys are independent, but nested mutable values
        share references with the original.
        """
        handler = BatchingHandler(config={"batch_size": 1})

        original = {"id": 1, "name": "original"}
        ctx = EnrichmentContext(data=original)
        result = await handler.enrich(ctx)

        # Modify original top-level key after batching
        original["name"] = "modified"

        # Batched copy has its own top-level keys
        assert result.data["items"][0]["name"] == "original"


# =====================
# create_handler_from_config tests
# =====================


class TestCreateHandlerFromConfig:
    def test_creates_transform_handler(self):
        handler = create_handler_from_config(
            {
                "type": "transform",
                "config": {"mappings": {"a": "b"}},
            }
        )
        assert isinstance(handler, TransformHandler)
        assert handler.config["mappings"] == {"a": "b"}

    def test_creates_validation_handler(self):
        handler = create_handler_from_config(
            {
                "type": "validation",
                "config": {"required_fields": ["x"]},
            }
        )
        assert isinstance(handler, ValidationHandler)

    def test_creates_lookup_handler(self):
        handler = create_handler_from_config({"type": "lookup"})
        assert isinstance(handler, LookupHandler)

    def test_creates_batching_handler(self):
        handler = create_handler_from_config({"type": "batching"})
        assert isinstance(handler, BatchingHandler)

    def test_missing_type_raises(self):
        with pytest.raises(ValueError, match="missing 'type'"):
            create_handler_from_config({})

    def test_unknown_type_raises(self):
        with pytest.raises(ValueError, match="Unknown handler type"):
            create_handler_from_config({"type": "nonexistent"})

    def test_custom_handler_import_invalid_module(self):
        with pytest.raises(ValueError, match="Failed to load"):
            create_handler_from_config(
                {
                    "type": "nonexistent.module:SomeHandler",
                }
            )

    def test_builtin_handlers_registry(self):
        assert "transform" in BUILTIN_HANDLERS
        assert "validation" in BUILTIN_HANDLERS
        assert "lookup" in BUILTIN_HANDLERS
        assert "batching" in BUILTIN_HANDLERS


# =====================
# EnrichmentPipeline tests
# =====================


class TestEnrichmentPipeline:
    async def test_executes_handlers_in_sequence(self):
        h1 = TransformHandler(
            config={
                "mappings": {"output_a": "input_a"},
            }
        )
        h2 = TransformHandler(
            config={
                "mappings": {"output_b": "input_b"},
            }
        )

        pipeline = EnrichmentPipeline(handlers=[h1, h2])
        result = await pipeline.execute({"input_a": "val_a", "input_b": "val_b"})

        assert result.success is True
        assert result.data["output_a"] == "val_a"
        assert result.data["output_b"] == "val_b"

    async def test_stops_on_failure(self):
        h1 = ValidationHandler(
            config={
                "required_fields": ["missing"],
            }
        )
        h2 = TransformHandler(
            config={
                "mappings": {"out": "in"},
            }
        )

        pipeline = EnrichmentPipeline(handlers=[h1, h2])
        result = await pipeline.execute({"in": "val"})

        assert result.success is False
        # h2 should not have run

    async def test_stops_on_skip(self):
        h1 = ValidationHandler(
            config={
                "skip_if": {"field": "status", "equals": "cancelled"},
            }
        )
        h2 = TransformHandler(
            config={
                "mappings": {"out": "in"},
            }
        )

        pipeline = EnrichmentPipeline(handlers=[h1, h2])
        result = await pipeline.execute({"status": "cancelled"})

        assert result.skip is True

    async def test_handler_exception_returns_failed(self):
        handler = AsyncMock(spec=EnrichmentHandler)
        handler.name = "broken"
        handler.enrich.side_effect = RuntimeError("kaboom")

        pipeline = EnrichmentPipeline(handlers=[handler])
        result = await pipeline.execute({"data": "test"})

        assert result.success is False
        assert "Exception in broken" in result.error

    async def test_passes_connection_manager_to_context(self):
        conn_mgr = MagicMock()

        class InspectHandler(EnrichmentHandler):
            async def enrich(self, context):
                assert context.connection_manager is conn_mgr
                return EnrichmentResult.ok(context.data)

        pipeline = EnrichmentPipeline(handlers=[InspectHandler()])
        result = await pipeline.execute({"x": 1}, connection_manager=conn_mgr)

        assert result.success is True

    async def test_initialize_calls_all_handlers(self):
        h1 = AsyncMock(spec=EnrichmentHandler)
        h1.name = "h1"
        h2 = AsyncMock(spec=EnrichmentHandler)
        h2.name = "h2"

        pipeline = EnrichmentPipeline(handlers=[h1, h2])
        await pipeline.initialize()

        h1.initialize.assert_awaited_once()
        h2.initialize.assert_awaited_once()

    async def test_initialize_raises_on_failure(self):
        handler = AsyncMock(spec=EnrichmentHandler)
        handler.name = "broken"
        handler.initialize.side_effect = RuntimeError("init failed")

        pipeline = EnrichmentPipeline(handlers=[handler])
        with pytest.raises(RuntimeError):
            await pipeline.initialize()

    async def test_cleanup_calls_all_handlers(self):
        h1 = AsyncMock(spec=EnrichmentHandler)
        h1.name = "h1"
        h2 = AsyncMock(spec=EnrichmentHandler)
        h2.name = "h2"

        pipeline = EnrichmentPipeline(handlers=[h1, h2])
        await pipeline.cleanup()

        h1.cleanup.assert_awaited_once()
        h2.cleanup.assert_awaited_once()

    async def test_cleanup_continues_on_failure(self):
        h1 = AsyncMock(spec=EnrichmentHandler)
        h1.name = "h1"
        h1.cleanup.side_effect = RuntimeError("fail")
        h2 = AsyncMock(spec=EnrichmentHandler)
        h2.name = "h2"

        pipeline = EnrichmentPipeline(handlers=[h1, h2])
        await pipeline.cleanup()

        # h2 cleanup should still be called despite h1 failure
        h2.cleanup.assert_awaited_once()

    async def test_updates_context_data_between_handlers(self):
        """Each handler gets the enriched data from the previous handler."""

        class AddFieldHandler(EnrichmentHandler):
            def __init__(self, field_name, field_value, **kwargs):
                super().__init__(**kwargs)
                self.field_name = field_name
                self.field_value = field_value

            async def enrich(self, context):
                context.data[self.field_name] = self.field_value
                return EnrichmentResult.ok(context.data)

        h1 = AddFieldHandler("step1", "done")
        h2 = AddFieldHandler("step2", "done")

        pipeline = EnrichmentPipeline(handlers=[h1, h2])
        result = await pipeline.execute({})

        assert result.data["step1"] == "done"
        assert result.data["step2"] == "done"
