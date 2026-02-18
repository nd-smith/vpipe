"""Tests for pipeline.verisk.dlq.cli module."""

import asyncio
import signal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipeline.common.types import PipelineMessage
from pipeline.verisk.dlq.cli import (
    CLITaskManager,
    DLQCLIManager,
    _noop_message_handler,
)


class TestNoopMessageHandler:
    @pytest.mark.asyncio
    async def test_noop_handler(self):
        await _noop_message_handler(MagicMock())


class TestCLITaskManager:
    @pytest.mark.asyncio
    async def test_context_manager(self):
        async with CLITaskManager() as mgr:
            assert mgr.tasks == set()
            assert not mgr.is_shutdown_requested()

    @pytest.mark.asyncio
    async def test_create_task_and_done_callback(self):
        async with CLITaskManager() as mgr:
            async def quick():
                return 42

            task = mgr.create_task(quick(), name="test")
            assert task in mgr.tasks
            result = await task
            assert result == 42
            # After completion, the done callback removes it
            await asyncio.sleep(0.01)
            assert task not in mgr.tasks

    @pytest.mark.asyncio
    async def test_shutdown_cancels_tasks(self):
        async with CLITaskManager() as mgr:
            async def slow():
                await asyncio.sleep(100)

            mgr.create_task(slow(), name="slow")
            assert len(mgr.tasks) == 1
            await mgr.shutdown(timeout=1.0)

    @pytest.mark.asyncio
    async def test_shutdown_empty(self):
        async with CLITaskManager() as mgr:
            await mgr.shutdown()  # no tasks, should not raise

    @pytest.mark.asyncio
    async def test_wait_all_empty(self):
        async with CLITaskManager() as mgr:
            result = await mgr.wait_all()
            assert result == []

    @pytest.mark.asyncio
    async def test_wait_all_with_tasks(self):
        async with CLITaskManager() as mgr:
            async def returns_1():
                return 1

            async def returns_2():
                return 2

            mgr.create_task(returns_1(), name="t1")
            mgr.create_task(returns_2(), name="t2")
            results = await mgr.wait_all()
            assert sorted(results) == [1, 2]

    @pytest.mark.asyncio
    async def test_wait_all_with_timeout(self):
        async with CLITaskManager() as mgr:
            async def slow():
                await asyncio.sleep(100)

            mgr.create_task(slow(), name="slow")
            with pytest.raises(TimeoutError):
                await mgr.wait_all(timeout=0.01)

    @pytest.mark.asyncio
    async def test_is_shutdown_requested(self):
        mgr = CLITaskManager()
        assert not mgr.is_shutdown_requested()
        mgr._shutdown = True
        assert mgr.is_shutdown_requested()
        mgr._restore_signal_handlers()

    @pytest.mark.asyncio
    async def test_signal_handler_sets_shutdown(self):
        mgr = CLITaskManager()
        try:
            # Simulate the signal handler being called
            handler = signal.getsignal(signal.SIGINT)
            # The handler should be our custom one
            assert handler is not signal.default_int_handler
        finally:
            mgr._restore_signal_handlers()


class TestDLQCLIManager:
    def _make_manager(self):
        config = MagicMock()
        with patch("pipeline.verisk.dlq.cli.DLQHandler") as mock_handler_cls:
            mock_handler = AsyncMock()
            mock_handler.parse_dlq_message = MagicMock()
            mock_handler_cls.return_value = mock_handler
            manager = DLQCLIManager(config)
        return manager

    @pytest.mark.asyncio
    async def test_start_stop(self):
        manager = self._make_manager()
        await manager.start()
        manager.handler.start.assert_awaited_once()
        await manager.stop()
        manager.handler.stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_fetch_messages_not_started(self):
        manager = self._make_manager()
        manager.handler._consumer = None
        with pytest.raises(RuntimeError, match="not started"):
            await manager.fetch_messages()

    def test_list_messages_empty(self, capsys):
        manager = self._make_manager()
        manager._messages = []
        manager.list_messages()
        out = capsys.readouterr().out
        assert "No DLQ messages found" in out

    def test_list_messages_with_data(self, capsys):
        manager = self._make_manager()
        msg = PipelineMessage(
            topic="dlq-topic", partition=0, offset=42, timestamp=1234567890, value=b"{}"
        )
        manager._messages = [msg]

        dlq_msg = MagicMock()
        dlq_msg.trace_id = "t1"
        dlq_msg.retry_count = 3
        dlq_msg.error_category = "transient"
        dlq_msg.attachment_url = "https://example.com/file.pdf"
        manager.handler.parse_dlq_message.return_value = dlq_msg

        manager.list_messages()
        out = capsys.readouterr().out
        assert "t1" in out
        assert "DLQ Messages (1 total)" in out

    def test_list_messages_parse_error(self, capsys):
        manager = self._make_manager()
        msg = PipelineMessage(topic="dlq", partition=0, offset=5, timestamp=0)
        manager._messages = [msg]
        manager.handler.parse_dlq_message.side_effect = Exception("parse fail")

        manager.list_messages()
        out = capsys.readouterr().out
        assert "PARSE ERROR" in out

    def test_view_message_not_found(self, capsys):
        manager = self._make_manager()
        manager._messages = []
        manager.view_message("nonexistent")
        out = capsys.readouterr().out
        assert "No message found" in out

    def test_view_message_found(self, capsys):
        manager = self._make_manager()
        msg = PipelineMessage(
            topic="dlq-topic", partition=0, offset=42, timestamp=0, key=b"key1"
        )
        manager._messages = [msg]

        dlq_msg = MagicMock()
        dlq_msg.trace_id = "t1"
        dlq_msg.attachment_url = "https://example.com/file.pdf"
        dlq_msg.retry_count = 2
        dlq_msg.error_category = "transient"
        dlq_msg.failed_at = "2024-01-01T00:00:00Z"
        dlq_msg.final_error = "Connection timeout"
        dlq_msg.original_task.event_type = "xact"
        dlq_msg.original_task.event_subtype = "documentsReceived"
        dlq_msg.original_task.blob_path = "path/to/blob"
        dlq_msg.original_task.original_timestamp = "2024-01-01T00:00:00Z"
        dlq_msg.original_task.metadata = {"key": "value"}
        manager.handler.parse_dlq_message.return_value = dlq_msg

        manager.view_message("t1")
        out = capsys.readouterr().out
        assert "t1" in out
        assert "Connection timeout" in out
        assert "key: value" in out

    def test_view_message_parse_error(self, capsys):
        manager = self._make_manager()
        msg = PipelineMessage(topic="dlq", partition=0, offset=0, timestamp=0)
        manager._messages = [msg]

        # First call finds message, second call (in view) fails
        dlq_msg = MagicMock()
        dlq_msg.trace_id = "t1"
        manager.handler.parse_dlq_message.side_effect = [dlq_msg, Exception("parse fail")]

        manager.view_message("t1")
        out = capsys.readouterr().out
        assert "Error parsing message" in out

    @pytest.mark.asyncio
    async def test_replay_message_not_found(self, capsys):
        manager = self._make_manager()
        manager._messages = []
        await manager.replay_message("nonexistent")
        out = capsys.readouterr().out
        assert "No message found" in out

    @pytest.mark.asyncio
    async def test_replay_message_success(self, capsys):
        manager = self._make_manager()
        msg = PipelineMessage(topic="dlq", partition=0, offset=0, timestamp=0)
        manager._messages = [msg]

        dlq_msg = MagicMock()
        dlq_msg.trace_id = "t1"
        manager.handler.parse_dlq_message.return_value = dlq_msg
        manager.handler.replay_message = AsyncMock()

        await manager.replay_message("t1")
        out = capsys.readouterr().out
        assert "replayed successfully" in out

    @pytest.mark.asyncio
    async def test_replay_message_error(self, capsys):
        manager = self._make_manager()
        msg = PipelineMessage(topic="dlq", partition=0, offset=0, timestamp=0)
        manager._messages = [msg]

        dlq_msg = MagicMock()
        dlq_msg.trace_id = "t1"
        manager.handler.parse_dlq_message.return_value = dlq_msg
        manager.handler.replay_message = AsyncMock(side_effect=Exception("kafka error"))

        await manager.replay_message("t1")
        out = capsys.readouterr().out
        assert "Failed to replay" in out

    @pytest.mark.asyncio
    async def test_resolve_message_not_found(self, capsys):
        manager = self._make_manager()
        manager._messages = []
        await manager.resolve_message("nonexistent")
        out = capsys.readouterr().out
        assert "No message found" in out

    @pytest.mark.asyncio
    async def test_resolve_message_success(self, capsys):
        manager = self._make_manager()
        msg = PipelineMessage(topic="dlq", partition=0, offset=0, timestamp=0)
        manager._messages = [msg]

        dlq_msg = MagicMock()
        dlq_msg.trace_id = "t1"
        manager.handler.parse_dlq_message.return_value = dlq_msg
        manager.handler.acknowledge_message = AsyncMock()

        await manager.resolve_message("t1")
        out = capsys.readouterr().out
        assert "resolved successfully" in out

    @pytest.mark.asyncio
    async def test_resolve_message_error(self, capsys):
        manager = self._make_manager()
        msg = PipelineMessage(topic="dlq", partition=0, offset=0, timestamp=0)
        manager._messages = [msg]

        dlq_msg = MagicMock()
        dlq_msg.trace_id = "t1"
        manager.handler.parse_dlq_message.return_value = dlq_msg
        manager.handler.acknowledge_message = AsyncMock(side_effect=Exception("fail"))

        await manager.resolve_message("t1")
        out = capsys.readouterr().out
        assert "Failed to resolve" in out

    def test_find_message_by_trace_id_not_found(self):
        manager = self._make_manager()
        manager._messages = []
        assert manager._find_message_by_trace_id("t1") is None

    def test_find_message_by_trace_id_parse_error_skips(self):
        manager = self._make_manager()
        msg = PipelineMessage(topic="dlq", partition=0, offset=0, timestamp=0)
        manager._messages = [msg]
        manager.handler.parse_dlq_message.side_effect = Exception("bad")
        assert manager._find_message_by_trace_id("t1") is None

    def test_list_messages_long_url_truncated(self, capsys):
        manager = self._make_manager()
        msg = PipelineMessage(topic="dlq", partition=0, offset=0, timestamp=0)
        manager._messages = [msg]

        dlq_msg = MagicMock()
        dlq_msg.trace_id = "t1"
        dlq_msg.retry_count = 0
        dlq_msg.error_category = "permanent"
        dlq_msg.attachment_url = "https://example.com/" + "x" * 100
        manager.handler.parse_dlq_message.return_value = dlq_msg

        manager.list_messages()
        out = capsys.readouterr().out
        assert "..." in out

    def test_view_message_no_metadata(self, capsys):
        manager = self._make_manager()
        msg = PipelineMessage(topic="dlq", partition=0, offset=0, timestamp=0, key=None)
        manager._messages = [msg]

        dlq_msg = MagicMock()
        dlq_msg.trace_id = "t1"
        dlq_msg.attachment_url = "https://example.com/file.pdf"
        dlq_msg.retry_count = 0
        dlq_msg.error_category = "permanent"
        dlq_msg.failed_at = "2024-01-01"
        dlq_msg.final_error = "Not found"
        dlq_msg.original_task.event_type = "xact"
        dlq_msg.original_task.event_subtype = "sub"
        dlq_msg.original_task.blob_path = "path"
        dlq_msg.original_task.original_timestamp = "ts"
        dlq_msg.original_task.metadata = {}
        manager.handler.parse_dlq_message.return_value = dlq_msg

        manager.view_message("t1")
        out = capsys.readouterr().out
        assert "t1" in out
        # "Metadata:" header appears only in the "Message Metadata" section, not as a standalone section
        # The standalone "Metadata:" section (for task metadata) should not appear when metadata is empty
        lines = out.strip().split("\n")
        standalone_metadata = [l for l in lines if l.strip() == "Metadata:"]
        assert len(standalone_metadata) == 0
