"""Tests for ClaimX base handler classes and registry."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock

from core.types import ErrorCategory
from pipeline.claimx.handlers.base import (
    _HANDLERS,
    EnrichmentResult,
    EventHandler,
    HandlerRegistry,
    HandlerResult,
    aggregate_results,
    register_handler,
)
from pipeline.claimx.schemas.entities import EntityRowsMessage

from .conftest import make_event

# ============================================================================
# EnrichmentResult
# ============================================================================


class TestEnrichmentResult:
    def test_enrichment_result_success(self):
        event = make_event()
        result = EnrichmentResult(event=event, success=True)

        assert result.event is event
        assert result.success is True
        assert result.rows is not None
        assert result.error is None
        assert result.is_retryable is True
        assert result.api_calls == 0
        assert result.duration_ms == 0

    def test_enrichment_result_failure(self):
        event = make_event()
        result = EnrichmentResult(
            event=event,
            success=False,
            error="API error",
            error_category=ErrorCategory.TRANSIENT,
            is_retryable=True,
            api_calls=1,
            duration_ms=150,
        )

        assert result.success is False
        assert result.error == "API error"
        assert result.error_category == ErrorCategory.TRANSIENT
        assert result.api_calls == 1
        assert result.duration_ms == 150

    def test_enrichment_result_with_rows(self):
        event = make_event()
        rows = EntityRowsMessage()
        rows.projects.append({"project_id": "123"})
        result = EnrichmentResult(event=event, success=True, rows=rows)

        assert len(result.rows.projects) == 1

    def test_enrichment_result_default_rows_is_empty(self):
        event = make_event()
        result = EnrichmentResult(event=event, success=True)

        assert result.rows.is_empty()

    def test_enrichment_result_none_rows_becomes_empty(self):
        event = make_event()
        result = EnrichmentResult(event=event, success=True, rows=None)

        assert result.rows is not None
        assert result.rows.is_empty()


# ============================================================================
# HandlerResult
# ============================================================================


class TestHandlerResult:
    def test_handler_result_stores_all_fields(self):
        rows = EntityRowsMessage()
        result = HandlerResult(
            handler_name="test",
            total=10,
            succeeded=8,
            failed=2,
            failed_permanent=1,
            skipped=0,
            rows=rows,
            errors=["error1"],
            duration_seconds=1.5,
            api_calls=10,
        )

        assert result.handler_name == "test"
        assert result.total == 10
        assert result.succeeded == 8
        assert result.failed == 2
        assert result.failed_permanent == 1
        assert result.skipped == 0
        assert result.errors == ["error1"]
        assert result.duration_seconds == 1.5
        assert result.api_calls == 10


# ============================================================================
# aggregate_results
# ============================================================================


class TestAggregateResults:
    def test_aggregate_results_counts_successes(self):
        event = make_event()
        results = [
            EnrichmentResult(event=event, success=True, api_calls=1),
            EnrichmentResult(event=event, success=True, api_calls=1),
        ]
        start = datetime.now(UTC)
        handler_result = aggregate_results("test", results, start)

        assert handler_result.total == 2
        assert handler_result.succeeded == 2
        assert handler_result.failed == 0
        assert handler_result.api_calls == 2

    def test_aggregate_results_counts_failures(self):
        event = make_event()
        results = [
            EnrichmentResult(event=event, success=False, error="fail1", is_retryable=True),
            EnrichmentResult(event=event, success=False, error="fail2", is_retryable=False),
        ]
        start = datetime.now(UTC)
        handler_result = aggregate_results("test", results, start)

        assert handler_result.total == 2
        assert handler_result.succeeded == 0
        assert handler_result.failed == 2
        assert handler_result.failed_permanent == 1
        assert handler_result.errors == ["fail1", "fail2"]

    def test_aggregate_results_merges_rows(self):
        event = make_event()
        rows1 = EntityRowsMessage()
        rows1.projects.append({"project_id": "1"})
        rows2 = EntityRowsMessage()
        rows2.contacts.append({"contact_email": "test@test.com"})

        results = [
            EnrichmentResult(event=event, success=True, rows=rows1),
            EnrichmentResult(event=event, success=True, rows=rows2),
        ]
        start = datetime.now(UTC)
        handler_result = aggregate_results("test", results, start)

        assert len(handler_result.rows.projects) == 1
        assert len(handler_result.rows.contacts) == 1

    def test_aggregate_results_skips_none_results(self):
        event = make_event()
        results = [
            None,
            EnrichmentResult(event=event, success=True),
        ]
        start = datetime.now(UTC)
        handler_result = aggregate_results("test", results, start)

        assert handler_result.total == 1
        assert handler_result.succeeded == 1

    def test_aggregate_results_empty_list(self):
        start = datetime.now(UTC)
        handler_result = aggregate_results("test", [], start)

        assert handler_result.total == 0
        assert handler_result.succeeded == 0
        assert handler_result.failed == 0
        assert handler_result.skipped == 0

    def test_aggregate_results_failure_without_error_message(self):
        event = make_event()
        results = [
            EnrichmentResult(event=event, success=False, error=None, is_retryable=True),
        ]
        start = datetime.now(UTC)
        handler_result = aggregate_results("test", results, start)

        assert handler_result.failed == 1
        assert handler_result.errors == []

    def test_aggregate_results_sets_duration(self):
        start = datetime.now(UTC)
        handler_result = aggregate_results("test", [], start)
        assert handler_result.duration_seconds >= 0


# ============================================================================
# EventHandler base class
# ============================================================================


class ConcreteHandler(EventHandler):
    """Concrete implementation for testing the abstract base class."""

    event_types = ["TEST_EVENT"]

    async def handle_event(self, event):
        return EnrichmentResult(event=event, success=True, api_calls=1)


class TestEventHandler:
    def test_handler_name_returns_class_name(self, mock_client):
        handler = ConcreteHandler(mock_client)
        assert handler.name == "ConcreteHandler"

    def test_handler_stores_client(self, mock_client):
        handler = ConcreteHandler(mock_client)
        assert handler.client is mock_client

    def test_handler_stores_project_cache(self, mock_client, mock_project_cache):
        handler = ConcreteHandler(mock_client, project_cache=mock_project_cache)
        assert handler.project_cache is mock_project_cache

    def test_handler_project_cache_defaults_to_none(self, mock_client):
        handler = ConcreteHandler(mock_client)
        assert handler.project_cache is None


class TestEventHandlerProcess:
    async def test_process_returns_empty_result_for_no_events(self, mock_client):
        handler = ConcreteHandler(mock_client)
        result = await handler.process([])

        assert result.handler_name == "ConcreteHandler"
        assert result.total == 0
        assert result.succeeded == 0
        assert result.duration_seconds == 0.0

    async def test_process_handles_single_event(self, mock_client):
        handler = ConcreteHandler(mock_client)
        events = [make_event()]
        result = await handler.process(events)

        assert result.total == 1
        assert result.succeeded == 1
        assert result.failed == 0
        assert result.api_calls == 1

    async def test_process_handles_multiple_events(self, mock_client):
        handler = ConcreteHandler(mock_client)
        events = [make_event(event_id=f"evt_{i}") for i in range(5)]
        result = await handler.process(events)

        assert result.total == 5
        assert result.succeeded == 5


class TestEventHandlerHandleBatch:
    async def test_handle_batch_returns_empty_for_no_events(self, mock_client):
        handler = ConcreteHandler(mock_client)
        results = await handler.handle_batch([])
        assert results == []

    async def test_handle_batch_processes_each_event(self, mock_client):
        handler = ConcreteHandler(mock_client)
        events = [make_event(event_id=f"evt_{i}") for i in range(3)]
        results = await handler.handle_batch(events)

        assert len(results) == 3
        assert all(r.success for r in results)

    async def test_handle_batch_captures_exceptions(self, mock_client):
        class FailingHandler(EventHandler):
            event_types = ["FAIL"]

            async def handle_event(self, event):
                raise ValueError("test error")

        handler = FailingHandler(mock_client)
        events = [make_event()]
        results = await handler.handle_batch(events)

        assert len(results) == 1
        assert results[0].success is False
        assert "test error" in results[0].error
        assert results[0].error_category == ErrorCategory.TRANSIENT
        assert results[0].is_retryable is True

    async def test_handle_batch_mixed_success_and_failure(self, mock_client):
        call_count = 0

        class MixedHandler(EventHandler):
            event_types = ["MIXED"]

            async def handle_event(self, event):
                nonlocal call_count
                call_count += 1
                if call_count % 2 == 0:
                    raise ValueError("even fail")
                return EnrichmentResult(event=event, success=True)

        handler = MixedHandler(mock_client)
        events = [make_event(event_id=f"evt_{i}") for i in range(4)]
        results = await handler.handle_batch(events)

        assert len(results) == 4
        successes = [r for r in results if r.success]
        failures = [r for r in results if not r.success]
        assert len(successes) == 2
        assert len(failures) == 2


class TestEventHandlerEnsureProjectExists:
    async def test_ensure_project_exists_calls_project_handler(self, mock_client):
        handler = ConcreteHandler(mock_client)

        mock_client.get_project = AsyncMock(
            return_value={
                "data": {
                    "project": {"projectId": 123},
                    "teamMembers": [],
                }
            }
        )

        rows = await handler.ensure_project_exists(123, source_event_id="evt_001")
        assert isinstance(rows, EntityRowsMessage)
        mock_client.get_project.assert_called_once_with(123)


# ============================================================================
# HandlerRegistry
# ============================================================================


class TestHandlerRegistry:
    def test_get_handler_class_returns_registered_handler(self):
        registry = HandlerRegistry()
        # ProjectHandler is registered for PROJECT_CREATED
        handler_class = registry.get_handler_class("PROJECT_CREATED")
        assert handler_class is not None

    def test_get_handler_class_returns_none_for_unknown(self):
        registry = HandlerRegistry()
        assert registry.get_handler_class("UNKNOWN_EVENT") is None

    def test_get_handler_returns_instance(self, mock_client):
        registry = HandlerRegistry()
        handler = registry.get_handler("PROJECT_CREATED", mock_client)
        assert handler is not None
        assert handler.client is mock_client

    def test_get_handler_returns_none_for_unknown(self, mock_client):
        registry = HandlerRegistry()
        handler = registry.get_handler("UNKNOWN_EVENT", mock_client)
        assert handler is None

    def test_group_events_by_handler_groups_correctly(self):
        registry = HandlerRegistry()
        events = [
            make_event(event_type="PROJECT_CREATED", event_id="evt_1"),
            make_event(event_type="PROJECT_CREATED", event_id="evt_2"),
            make_event(event_type="PROJECT_FILE_ADDED", event_id="evt_3", media_id="100"),
        ]
        groups = registry.group_events_by_handler(events)

        assert len(groups) == 2
        total_events = sum(len(evts) for evts in groups.values())
        assert total_events == 3

    def test_group_events_by_handler_tracks_unhandled(self):
        registry = HandlerRegistry()
        events = [
            make_event(event_type="UNKNOWN_TYPE", event_id="evt_1"),
        ]
        groups = registry.group_events_by_handler(events)
        assert len(groups) == 0

    def test_group_events_by_handler_empty_list(self):
        registry = HandlerRegistry()
        groups = registry.group_events_by_handler([])
        assert groups == {}

    def test_get_registered_handlers_returns_mapping(self):
        registry = HandlerRegistry()
        registered = registry.get_registered_handlers()

        assert isinstance(registered, dict)
        assert "PROJECT_CREATED" in registered
        assert "PROJECT_FILE_ADDED" in registered
        assert "CUSTOM_TASK_ASSIGNED" in registered


# ============================================================================
# register_handler decorator
# ============================================================================


class TestRegisterHandler:
    def test_register_handler_adds_to_registry(self):
        dict(_HANDLERS)
        try:

            @register_handler
            class TestRegistration(EventHandler):
                event_types = ["__TEST_REGISTER__"]

                async def handle_event(self, event):
                    pass

            assert "__TEST_REGISTER__" in _HANDLERS
            assert _HANDLERS["__TEST_REGISTER__"] is TestRegistration
        finally:
            # Clean up
            _HANDLERS.pop("__TEST_REGISTER__", None)

    def test_register_handler_returns_class(self):
        dict(_HANDLERS)
        try:

            @register_handler
            class TestReturn(EventHandler):
                event_types = ["__TEST_RETURN__"]

                async def handle_event(self, event):
                    pass

            assert TestReturn.event_types == ["__TEST_RETURN__"]
        finally:
            _HANDLERS.pop("__TEST_RETURN__", None)
