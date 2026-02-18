"""Tests for pipeline.common.decorators module."""

from dataclasses import dataclass
from unittest.mock import AsyncMock, patch

import pytest

from pipeline.common.decorators import set_log_context_from_message
from pipeline.common.types import PipelineMessage


class TestSetLogContextFromMessage:
    @pytest.mark.asyncio
    async def test_sets_context_from_message(self):
        msg = PipelineMessage(
            topic="test-topic",
            partition=2,
            offset=100,
            timestamp=1234567890,
        )

        call_log = {}

        class FakeWorker:
            @set_log_context_from_message
            async def handle(self, message):
                from core.logging.context import get_message_context

                ctx = get_message_context()
                call_log.update(ctx)
                return "handled"

        worker = FakeWorker()
        result = await worker.handle(msg)
        assert result == "handled"
        assert call_log["message_topic"] == "test-topic"
        assert call_log["message_partition"] == 2
        assert call_log["message_offset"] == 100
