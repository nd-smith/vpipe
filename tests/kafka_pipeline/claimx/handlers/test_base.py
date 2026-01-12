"""
Unit tests for ClaimX handler base classes and registry.

Tests EventHandler, NoOpHandler, HandlerRegistry, decorators, and result classes.
"""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from typing import List

import pytest

from kafka_pipeline.claimx.api_client import ClaimXApiClient, ClaimXApiError
from kafka_pipeline.claimx.handlers.base import (
    EnrichmentResult,
    HandlerResult,
    EventHandler,
    NoOpHandler,
    HandlerRegistry,
    get_handler_registry,
    reset_registry,
    register_handler,
    with_api_error_handling,
)
from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage
from kafka_pipeline.common.exceptions import ErrorCategory


# =================================================================
# Test Result Classes
# =================================================================


class TestEnrichmentResult:
    """Test EnrichmentResult dataclass."""

    def test_success_result_creation(self):
        """Test creating a successful enrichment result."""
        event = ClaimXEventMessage(
            event_id="evt_123",
            event_type="PROJECT_CREATED",
            project_id="proj_456",
            ingested_at=datetime.now(timezone.utc),
        )
        rows = EntityRowsMessage()

        result = EnrichmentResult(
            event=event,
            success=True,
            rows=rows,
            api_calls=1,
            duration_ms=100,
        )

        assert result.success is True
        assert result.event == event
        assert result.rows == rows
        assert result.error is None
        assert result.api_calls == 1
        assert result.duration_ms == 100
        assert result.is_retryable is True

    def test_error_result_creation(self):
        """Test creating an error enrichment result."""
        event = ClaimXEventMessage(
            event_id="evt_123",
            event_type="PROJECT_CREATED",
            project_id="proj_456",
            ingested_at=datetime.now(timezone.utc),
        )

        result = EnrichmentResult(
            event=event,
            success=False,
            error="API timeout",
            error_category=ErrorCategory.TRANSIENT,
            is_retryable=True,
            api_calls=1,
            duration_ms=5000,
        )

        assert result.success is False
        assert result.error == "API timeout"
        assert result.error_category == ErrorCategory.TRANSIENT
        assert result.is_retryable is True
        assert isinstance(result.rows, EntityRowsMessage)  # Default empty rows

    def test_default_rows_created(self):
        """Test that rows defaults to empty EntityRowsMessage."""
        event = ClaimXEventMessage(
            event_id="evt_123",
            event_type="PROJECT_CREATED",
            project_id="proj_456",
            ingested_at=datetime.now(timezone.utc),
        )

        result = EnrichmentResult(event=event, success=True)

        assert isinstance(result.rows, EntityRowsMessage)
        assert len(result.rows.projects) == 0


class TestHandlerResult:
    """Test HandlerResult dataclass."""

    def test_handler_result_creation(self):
        """Test creating a handler result."""
        rows = EntityRowsMessage()

        result = HandlerResult(
            handler_name="TestHandler",
            total=10,
            succeeded=8,
            failed=2,
            failed_permanent=1,
            skipped=0,
            rows=rows,
            event_logs=[],
            errors=["Error 1", "Error 2"],
            duration_seconds=1.5,
            api_calls=10,
        )

        assert result.handler_name == "TestHandler"
        assert result.total == 10
        assert result.succeeded == 8
        assert result.failed == 2
        assert result.failed_permanent == 1
        assert result.api_calls == 10
        assert len(result.errors) == 2


# =================================================================
# Test Decorators
# =================================================================


class TestWithApiErrorHandling:
    """Test with_api_error_handling decorator."""

    @pytest.mark.asyncio
    async def test_decorator_success(self):
        """Test decorator passes through successful results."""

        class MockHandler:
            @with_api_error_handling(api_calls=1)
            async def process(self, event, start_time):
                return EnrichmentResult(
                    event=event,
                    success=True,
                    api_calls=1,
                    duration_ms=100,
                )

        handler = MockHandler()

        event = ClaimXEventMessage(
            event_id="evt_123",
            event_type="TEST",
            project_id="proj_456",
            ingested_at=datetime.now(timezone.utc),
        )

        result = await handler.process(event)

        assert result.success is True
        assert result.api_calls == 1

    @pytest.mark.asyncio
    async def test_decorator_handles_api_error(self):
        """Test decorator converts ClaimXApiError to error result."""

        class MockHandler:
            @with_api_error_handling(api_calls=1)
            async def process(self, event, start_time):
                raise ClaimXApiError(
                    "API error",
                    status_code=500,
                    category=ErrorCategory.TRANSIENT,
                    is_retryable=True,
                )

        handler = MockHandler()

        event = ClaimXEventMessage(
            event_id="evt_123",
            event_type="TEST",
            project_id="proj_456",
            ingested_at=datetime.now(timezone.utc),
        )

        result = await handler.process(event)

        assert result.success is False
        assert result.error_category == ErrorCategory.TRANSIENT
        assert result.is_retryable is True
        assert result.api_calls == 1
        assert "API error" in result.error

    @pytest.mark.asyncio
    async def test_decorator_handles_generic_exception(self):
        """Test decorator converts generic exceptions to retryable errors."""

        class MockHandler:
            @with_api_error_handling(api_calls=1)
            async def process(self, event, start_time):
                raise ValueError("Unexpected error")

        handler = MockHandler()

        event = ClaimXEventMessage(
            event_id="evt_123",
            event_type="TEST",
            project_id="proj_456",
            ingested_at=datetime.now(timezone.utc),
        )

        result = await handler.process(event)

        assert result.success is False
        assert result.error_category == ErrorCategory.TRANSIENT
        assert result.is_retryable is True
        assert "Unexpected error" in result.error


# =================================================================
# Test EventHandler Base Class
# =================================================================


class ConcreteHandler(EventHandler):
    """Concrete handler for testing."""

    event_types = ["TEST_EVENT"]

    async def handle_event(self, event: ClaimXEventMessage) -> EnrichmentResult:
        """Mock handle_event that returns success."""
        return EnrichmentResult(
            event=event,
            success=True,
            api_calls=1,
            duration_ms=50,
        )


class ErrorHandler(EventHandler):
    """Handler that raises exceptions."""

    event_types = ["ERROR_EVENT"]

    async def handle_event(self, event: ClaimXEventMessage) -> EnrichmentResult:
        """Mock handle_event that raises error."""
        raise ValueError("Handler error")


@pytest.fixture
def mock_api_client():
    """Create mock API client."""
    client = AsyncMock(spec=ClaimXApiClient)
    return client


class TestEventHandler:
    """Test EventHandler base class."""

    def test_handler_name_property(self, mock_api_client):
        """Test name property returns class name."""
        handler = ConcreteHandler(mock_api_client)
        assert handler.name == "ConcreteHandler"

    @pytest.mark.asyncio
    async def test_handle_batch_processes_events(self, mock_api_client):
        """Test handle_batch processes multiple events."""
        handler = ConcreteHandler(mock_api_client)

        events = [
            ClaimXEventMessage(
                event_id=f"evt_{i}",
                event_type="TEST_EVENT",
                project_id=f"proj_{100+i}",
                ingested_at=datetime.now(timezone.utc),
            )
            for i in range(3)
        ]

        results = await handler.handle_batch(events)

        assert len(results) == 3
        assert all(r.success for r in results)

    @pytest.mark.asyncio
    async def test_handle_batch_handles_exceptions(self, mock_api_client):
        """Test handle_batch creates error results for exceptions."""
        handler = ErrorHandler(mock_api_client)

        events = [
            ClaimXEventMessage(
                event_id="evt_1",
                event_type="ERROR_EVENT",
                project_id="proj_100",
                ingested_at=datetime.now(timezone.utc),
            )
        ]

        results = await handler.handle_batch(events)

        assert len(results) == 1
        assert results[0].success is False
        assert "Handler error" in results[0].error

    @pytest.mark.asyncio
    async def test_handle_batch_empty_list(self, mock_api_client):
        """Test handle_batch with empty list returns empty results."""
        handler = ConcreteHandler(mock_api_client)

        results = await handler.handle_batch([])

        assert results == []

    @pytest.mark.asyncio
    async def test_process_aggregates_results(self, mock_api_client):
        """Test process method aggregates results correctly."""
        handler = ConcreteHandler(mock_api_client)

        events = [
            ClaimXEventMessage(
                event_id=f"evt_{i}",
                event_type="TEST_EVENT",
                project_id=f"proj_{100+i}",
                ingested_at=datetime.now(timezone.utc),
            )
            for i in range(5)
        ]

        handler_result = await handler.process(events)

        assert handler_result.handler_name == "ConcreteHandler"
        assert handler_result.total == 5
        assert handler_result.succeeded == 5
        assert handler_result.failed == 0
        assert handler_result.api_calls == 5  # 1 per event

    @pytest.mark.asyncio
    async def test_process_empty_events(self, mock_api_client):
        """Test process with no events returns empty result."""
        handler = ConcreteHandler(mock_api_client)

        handler_result = await handler.process([])

        assert handler_result.total == 0
        assert handler_result.succeeded == 0
        assert handler_result.failed == 0

    @pytest.mark.asyncio
    async def test_process_counts_failures(self, mock_api_client):
        """Test process correctly counts failures."""

        class MixedHandler(EventHandler):
            event_types = ["MIXED"]
            call_count = 0

            async def handle_event(self, event):
                self.call_count += 1
                if self.call_count % 2 == 0:
                    return EnrichmentResult(
                        event=event,
                        success=False,
                        error="Test error",
                        error_category=ErrorCategory.TRANSIENT,
                        is_retryable=True,
                    )
                return EnrichmentResult(event=event, success=True)

        handler = MixedHandler(mock_api_client)

        events = [
            ClaimXEventMessage(
                event_id=f"evt_{i}",
                event_type="MIXED",
                project_id="proj_100",
                ingested_at=datetime.now(timezone.utc),
            )
            for i in range(4)
        ]

        result = await handler.process(events)

        assert result.total == 4
        assert result.succeeded == 2
        assert result.failed == 2

    @pytest.mark.asyncio
    async def test_process_counts_permanent_failures(self, mock_api_client):
        """Test process correctly counts permanent failures."""

        class PermanentErrorHandler(EventHandler):
            event_types = ["PERM_ERROR"]

            async def handle_event(self, event):
                return EnrichmentResult(
                    event=event,
                    success=False,
                    error="Permanent error",
                    error_category=ErrorCategory.PERMANENT,
                    is_retryable=False,
                )

        handler = PermanentErrorHandler(mock_api_client)

        events = [
            ClaimXEventMessage(
                event_id="evt_1",
                event_type="PERM_ERROR",
                project_id="proj_100",
                ingested_at=datetime.now(timezone.utc),
            )
        ]

        result = await handler.process(events)

        assert result.failed == 1
        assert result.failed_permanent == 1


# =================================================================
# Test NoOpHandler
# =================================================================


class ConcreteNoOpHandler(NoOpHandler):
    """Concrete NoOpHandler for testing."""

    event_types = ["NOOP_EVENT"]

    def extract_rows(self, event: ClaimXEventMessage) -> EntityRowsMessage:
        """Extract rows from event."""
        rows = EntityRowsMessage()
        # Simulate extracting a project row
        rows.projects.append({"project_id": event.project_id})
        return rows


class TestNoOpHandler:
    """Test NoOpHandler base class."""

    @pytest.mark.asyncio
    async def test_handle_event_extracts_rows(self, mock_api_client):
        """Test NoOpHandler extracts rows without API call."""
        handler = ConcreteNoOpHandler(mock_api_client)

        event = ClaimXEventMessage(
            event_id="evt_123",
            event_type="NOOP_EVENT",
            project_id="proj_456",
            ingested_at=datetime.now(timezone.utc),
        )

        result = await handler.handle_event(event)

        assert result.success is True
        assert result.api_calls == 0
        assert len(result.rows.projects) == 1
        assert result.rows.projects[0]["project_id"] == "proj_456"

    @pytest.mark.asyncio
    async def test_handle_event_handles_extraction_error(self, mock_api_client):
        """Test NoOpHandler handles extraction errors."""

        class ErrorNoOpHandler(NoOpHandler):
            event_types = ["ERROR"]

            def extract_rows(self, event):
                raise ValueError("Extraction failed")

        handler = ErrorNoOpHandler(mock_api_client)

        event = ClaimXEventMessage(
            event_id="evt_123",
            event_type="ERROR",
            project_id="proj_456",
            ingested_at=datetime.now(timezone.utc),
        )

        result = await handler.handle_event(event)

        assert result.success is False
        assert "Extraction failed" in result.error
        assert result.error_category == ErrorCategory.TRANSIENT


# =================================================================
# Test HandlerRegistry
# =================================================================


class TestHandlerRegistry:
    """Test HandlerRegistry."""

    def test_register_handler(self, mock_api_client):
        """Test registering a handler."""
        registry = HandlerRegistry()

        class TestHandler(EventHandler):
            event_types = ["TEST"]
            async def handle_event(self, event):
                pass

        registry.register(TestHandler)

        handler_class = registry.get_handler_class("TEST")
        assert handler_class == TestHandler

    def test_register_multiple_event_types(self, mock_api_client):
        """Test handler with multiple event types."""
        registry = HandlerRegistry()

        class MultiHandler(EventHandler):
            event_types = ["TYPE1", "TYPE2", "TYPE3"]
            async def handle_event(self, event):
                pass

        registry.register(MultiHandler)

        assert registry.get_handler_class("TYPE1") == MultiHandler
        assert registry.get_handler_class("TYPE2") == MultiHandler
        assert registry.get_handler_class("TYPE3") == MultiHandler

    def test_get_handler_creates_instance(self, mock_api_client):
        """Test get_handler returns handler instance."""
        registry = HandlerRegistry()

        class TestHandler(EventHandler):
            event_types = ["TEST"]
            async def handle_event(self, event):
                pass

        registry.register(TestHandler)

        handler = registry.get_handler("TEST", mock_api_client)

        assert isinstance(handler, TestHandler)
        assert handler.client == mock_api_client

    def test_get_handler_unknown_type(self, mock_api_client):
        """Test get_handler returns None for unknown type."""
        registry = HandlerRegistry()

        handler = registry.get_handler("UNKNOWN", mock_api_client)

        assert handler is None

    def test_group_events_by_handler(self):
        """Test grouping events by handler class."""
        registry = HandlerRegistry()

        class Handler1(EventHandler):
            event_types = ["TYPE1"]
            async def handle_event(self, event):
                pass

        class Handler2(EventHandler):
            event_types = ["TYPE2"]
            async def handle_event(self, event):
                pass

        registry.register(Handler1)
        registry.register(Handler2)

        events = [
            ClaimXEventMessage(
                event_id="evt_1", event_type="TYPE1", project_id="proj_1",
                ingested_at=datetime.now(timezone.utc),
            ),
            ClaimXEventMessage(
                event_id="evt_2", event_type="TYPE1", project_id="proj_2",
                ingested_at=datetime.now(timezone.utc),
            ),
            ClaimXEventMessage(
                event_id="evt_3", event_type="TYPE2", project_id="proj_3",
                ingested_at=datetime.now(timezone.utc),
            ),
        ]

        groups = registry.group_events_by_handler(events)

        assert len(groups) == 2
        assert Handler1 in groups
        assert Handler2 in groups
        assert len(groups[Handler1]) == 2
        assert len(groups[Handler2]) == 1

    def test_group_events_with_unhandled_types(self):
        """Test grouping events includes unhandled event types."""
        registry = HandlerRegistry()

        class Handler1(EventHandler):
            event_types = ["TYPE1"]
            async def handle_event(self, event):
                pass

        registry.register(Handler1)

        events = [
            ClaimXEventMessage(
                event_id="evt_1", event_type="TYPE1", project_id="proj_1",
                ingested_at=datetime.now(timezone.utc),
            ),
            ClaimXEventMessage(
                event_id="evt_2", event_type="UNKNOWN", project_id="proj_2",
                ingested_at=datetime.now(timezone.utc),
            ),
        ]

        groups = registry.group_events_by_handler(events)

        # Should only include handlers with registered types
        assert len(groups) == 1
        assert Handler1 in groups
        assert len(groups[Handler1]) == 1

    def test_get_registered_handlers(self):
        """Test getting registered handlers map."""
        registry = HandlerRegistry()

        class TestHandler(EventHandler):
            event_types = ["TEST1", "TEST2"]
            async def handle_event(self, event):
                pass

        registry.register(TestHandler)

        handlers_map = registry.get_registered_handlers()

        assert handlers_map == {
            "TEST1": "TestHandler",
            "TEST2": "TestHandler",
        }


class TestRegistryHelpers:
    """Test registry helper functions."""

    def test_get_handler_registry_singleton(self):
        """Test get_handler_registry returns singleton."""
        reset_registry()  # Start fresh

        registry1 = get_handler_registry()
        registry2 = get_handler_registry()

        assert registry1 is registry2

    def test_reset_registry(self):
        """Test reset_registry clears singleton."""
        registry1 = get_handler_registry()
        reset_registry()
        registry2 = get_handler_registry()

        assert registry1 is not registry2

    def test_register_handler_decorator(self):
        """Test @register_handler decorator."""
        reset_registry()

        @register_handler
        class DecoratedHandler(EventHandler):
            event_types = ["DECORATED"]
            async def handle_event(self, event):
                pass

        registry = get_handler_registry()
        handler_class = registry.get_handler_class("DECORATED")

        assert handler_class == DecoratedHandler

    def test_register_handler_decorator_returns_class(self):
        """Test @register_handler returns the class unchanged."""
        reset_registry()

        @register_handler
        class DecoratedHandler(EventHandler):
            event_types = ["TEST"]
            async def handle_event(self, event):
                pass

        # Should still be able to use the class normally
        assert DecoratedHandler.event_types == ["TEST"]
        assert issubclass(DecoratedHandler, EventHandler)
