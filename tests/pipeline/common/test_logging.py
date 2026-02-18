"""Tests for pipeline.common.logging module."""

from dataclasses import dataclass
from enum import Enum
from unittest.mock import AsyncMock, MagicMock

import pytest

from pipeline.common.logging import extract_log_context, with_api_error_handling


class TestExtractLogContext:
    def test_none_input(self):
        assert extract_log_context(None) == {}

    def test_object_with_trace_id(self):
        obj = MagicMock(spec=["trace_id", "assignment_id"])
        obj.trace_id = "t1"
        obj.assignment_id = "a1"
        ctx = extract_log_context(obj)
        assert ctx["trace_id"] == "t1"
        assert ctx["assignment_id"] == "a1"

    def test_none_values_excluded(self):
        obj = MagicMock(spec=["trace_id", "assignment_id"])
        obj.trace_id = "t1"
        obj.assignment_id = None
        ctx = extract_log_context(obj)
        assert ctx["trace_id"] == "t1"
        assert "assignment_id" not in ctx

    def test_unwraps_task_result(self):
        task = MagicMock(spec=["trace_id"])
        task.trace_id = "t1"
        result = MagicMock(spec=["task", "error_category", "http_status"])
        result.task = task
        result.error_category = MagicMock(value="transient")
        result.http_status = 503
        ctx = extract_log_context(result)
        assert ctx["trace_id"] == "t1"
        assert ctx["error_category"] == "transient"
        assert ctx["http_status"] == 503

    def test_unwraps_event_result(self):
        event = MagicMock(spec=["trace_id", "event_type"])
        event.trace_id = "t1"
        event.event_type = "xact"
        result = MagicMock(spec=["event", "error_category", "api_calls"])
        result.event = event
        result.task = None  # has task attr but it's None, so won't match first branch
        result.error_category = "permanent"  # string, no .value
        result.api_calls = 3

        # Need to handle carefully - the code checks hasattr(obj, "task") first
        # Let's use a simpler approach
        @dataclass
        class FakeResult:
            event: object
            error_category: str = "permanent"
            api_calls: int = 3

        inner_event = MagicMock(spec=["trace_id", "event_type"])
        inner_event.trace_id = "t1"
        inner_event.event_type = "xact"
        result = FakeResult(event=inner_event)
        ctx = extract_log_context(result)
        assert ctx["trace_id"] == "t1"
        assert ctx["error_category"] == "permanent"
        assert ctx["api_calls"] == 3

    def test_error_category_with_enum_value(self):
        class Cat(Enum):
            TRANSIENT = "transient"

        task = MagicMock(spec=["trace_id"])
        task.trace_id = "t1"

        @dataclass
        class FakeResult:
            task: object
            error_category: object
            http_status: int = 0

        result = FakeResult(task=task, error_category=Cat.TRANSIENT)
        ctx = extract_log_context(result)
        assert ctx["error_category"] == "transient"

    def test_plain_object_no_relevant_attrs(self):
        obj = MagicMock(spec=[])
        ctx = extract_log_context(obj)
        assert ctx == {}


class TestWithApiErrorHandling:
    @pytest.mark.asyncio
    async def test_async_success(self):
        class Service:
            @with_api_error_handling
            async def fetch(self):
                return "ok"

        svc = Service()
        assert await svc.fetch() == "ok"

    @pytest.mark.asyncio
    async def test_async_error_reraises(self):
        class Service:
            @with_api_error_handling
            async def fetch(self):
                raise ValueError("boom")

        svc = Service()
        with pytest.raises(ValueError, match="boom"):
            await svc.fetch()

    def test_sync_success(self):
        class Service:
            @with_api_error_handling
            def fetch(self):
                return "ok"

        svc = Service()
        assert svc.fetch() == "ok"

    def test_sync_error_reraises(self):
        class Service:
            @with_api_error_handling
            def fetch(self):
                raise ValueError("boom")

        svc = Service()
        with pytest.raises(ValueError, match="boom"):
            svc.fetch()

    @pytest.mark.asyncio
    async def test_uses_instance_logger(self):
        mock_logger = MagicMock()

        class Service:
            _logger = mock_logger

            @with_api_error_handling
            async def fetch(self):
                raise RuntimeError("fail")

        svc = Service()
        with pytest.raises(RuntimeError):
            await svc.fetch()
        # log_exception calls logger.log(level, ..., exc_info=...)
        assert mock_logger.log.called
