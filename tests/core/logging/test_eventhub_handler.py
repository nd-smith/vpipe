"""Tests for EventHubLogHandler."""

import asyncio
import logging
import queue
import threading
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Mock azure.eventhub before importing the handler
with patch.dict("sys.modules", {
    "azure.eventhub": MagicMock(),
    "azure.eventhub.aio": MagicMock(),
}):
    from core.logging.eventhub_handler import EventHubLogHandler


class TestEventHubLogHandlerInit:

    @patch.object(EventHubLogHandler, "_start_sender")
    def test_stores_configuration(self, mock_start):
        handler = EventHubLogHandler(
            connection_string="Endpoint=sb://test",
            eventhub_name="test-hub",
            batch_size=50,
            batch_timeout_seconds=2.0,
            max_queue_size=5000,
            circuit_breaker_threshold=3,
        )

        assert handler.connection_string == "Endpoint=sb://test"
        assert handler.eventhub_name == "test-hub"
        assert handler.batch_size == 50
        assert handler.batch_timeout_seconds == 2.0
        assert handler.circuit_breaker_threshold == 3
        assert handler.log_queue.maxsize == 5000

    @patch.object(EventHubLogHandler, "_start_sender")
    def test_initializes_counters(self, mock_start):
        handler = EventHubLogHandler(
            connection_string="Endpoint=sb://test",
            eventhub_name="test-hub",
        )

        assert handler._total_sent == 0
        assert handler._total_dropped == 0
        assert handler._failure_count == 0
        assert handler._circuit_open is False

    @patch.object(EventHubLogHandler, "_start_sender")
    def test_uses_default_parameters(self, mock_start):
        handler = EventHubLogHandler(
            connection_string="Endpoint=sb://test",
            eventhub_name="test-hub",
        )

        assert handler.batch_size == 100
        assert handler.batch_timeout_seconds == 1.0
        assert handler.log_queue.maxsize == 10000
        assert handler.circuit_breaker_threshold == 5

    @patch.object(EventHubLogHandler, "_start_sender")
    def test_starts_sender_on_init(self, mock_start):
        EventHubLogHandler(
            connection_string="Endpoint=sb://test",
            eventhub_name="test-hub",
        )

        mock_start.assert_called_once()


class TestEventHubLogHandlerEmit:

    @patch.object(EventHubLogHandler, "_start_sender")
    def test_queues_formatted_log(self, mock_start):
        handler = EventHubLogHandler(
            connection_string="Endpoint=sb://test",
            eventhub_name="test-hub",
        )
        handler.setFormatter(logging.Formatter("%(message)s"))

        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="",
            lineno=0, msg="test message", args=(), exc_info=None,
        )

        handler.emit(record)

        assert handler.log_queue.qsize() == 1
        assert handler.log_queue.get_nowait() == "test message"

    @patch.object(EventHubLogHandler, "_start_sender")
    def test_drops_when_circuit_open(self, mock_start):
        handler = EventHubLogHandler(
            connection_string="Endpoint=sb://test",
            eventhub_name="test-hub",
        )
        handler._circuit_open = True

        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="",
            lineno=0, msg="test", args=(), exc_info=None,
        )

        handler.emit(record)

        assert handler.log_queue.qsize() == 0
        assert handler._total_dropped == 1

    @patch.object(EventHubLogHandler, "_start_sender")
    def test_drops_when_queue_full(self, mock_start):
        handler = EventHubLogHandler(
            connection_string="Endpoint=sb://test",
            eventhub_name="test-hub",
            max_queue_size=1,
        )
        handler.setFormatter(logging.Formatter("%(message)s"))

        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="",
            lineno=0, msg="test", args=(), exc_info=None,
        )

        # Fill the queue
        handler.emit(record)
        assert handler.log_queue.qsize() == 1

        # This should be dropped
        handler.emit(record)
        assert handler.log_queue.qsize() == 1
        assert handler._total_dropped == 1

    @patch.object(EventHubLogHandler, "_start_sender")
    def test_handles_format_errors_gracefully(self, mock_start):
        handler = EventHubLogHandler(
            connection_string="Endpoint=sb://test",
            eventhub_name="test-hub",
        )
        handler.setFormatter(logging.Formatter("%(message)s"))
        handler.handleError = MagicMock()

        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="",
            lineno=0, msg="test", args=(), exc_info=None,
        )

        # Make format raise
        with patch.object(handler, "format", side_effect=Exception("format error")):
            handler.emit(record)

        handler.handleError.assert_called_once()


class TestEventHubLogHandlerCircuitBreaker:

    @patch.object(EventHubLogHandler, "_start_sender")
    def test_opens_circuit_after_threshold_failures(self, mock_start):
        handler = EventHubLogHandler(
            connection_string="Endpoint=sb://test",
            eventhub_name="test-hub",
            circuit_breaker_threshold=3,
        )

        error = Exception("send failed")

        handler._handle_send_error(error, loop_time=100.0)
        assert handler._circuit_open is False
        assert handler._failure_count == 1

        handler._handle_send_error(error, loop_time=101.0)
        assert handler._circuit_open is False
        assert handler._failure_count == 2

        handler._handle_send_error(error, loop_time=102.0)
        assert handler._circuit_open is True
        assert handler._failure_count == 3
        assert handler._circuit_opened_at == 102.0

    @patch.object(EventHubLogHandler, "_start_sender")
    def test_does_not_reopen_already_open_circuit(self, mock_start):
        handler = EventHubLogHandler(
            connection_string="Endpoint=sb://test",
            eventhub_name="test-hub",
            circuit_breaker_threshold=2,
        )

        error = Exception("fail")
        handler._handle_send_error(error, loop_time=100.0)
        handler._handle_send_error(error, loop_time=101.0)
        assert handler._circuit_open is True
        opened_at = handler._circuit_opened_at

        # Additional failures should not change opened_at
        handler._handle_send_error(error, loop_time=200.0)
        assert handler._circuit_opened_at == opened_at


class TestEventHubLogHandlerGetStats:

    @patch.object(EventHubLogHandler, "_start_sender")
    def test_returns_stats_dict(self, mock_start):
        handler = EventHubLogHandler(
            connection_string="Endpoint=sb://test",
            eventhub_name="test-hub",
        )
        handler._total_sent = 100
        handler._total_dropped = 5
        handler._circuit_open = True
        handler._failure_count = 3

        stats = handler.get_stats()

        assert stats["total_sent"] == 100
        assert stats["total_dropped"] == 5
        assert stats["circuit_open"] is True
        assert stats["failure_count"] == 3
        assert "queue_size" in stats


class TestEventHubLogHandlerClose:

    @patch.object(EventHubLogHandler, "_start_sender")
    def test_sets_shutdown_event(self, mock_start):
        handler = EventHubLogHandler(
            connection_string="Endpoint=sb://test",
            eventhub_name="test-hub",
        )
        handler._sender_thread = MagicMock()

        handler.close()

        assert handler._shutdown.is_set()
        handler._sender_thread.join.assert_called_once_with(timeout=5.0)

    @patch.object(EventHubLogHandler, "_start_sender")
    def test_close_works_without_sender_thread(self, mock_start):
        handler = EventHubLogHandler(
            connection_string="Endpoint=sb://test",
            eventhub_name="test-hub",
        )
        handler._sender_thread = None

        handler.close()  # Should not raise

        assert handler._shutdown.is_set()


class TestEventHubLogHandlerSendBatch:

    @pytest.mark.asyncio
    @patch.object(EventHubLogHandler, "_start_sender")
    async def test_sends_batch_to_producer(self, mock_start):
        handler = EventHubLogHandler(
            connection_string="Endpoint=sb://test",
            eventhub_name="test-hub",
        )

        mock_producer = AsyncMock()
        mock_batch = MagicMock()
        mock_batch.__len__ = MagicMock(return_value=2)
        mock_producer.create_batch.return_value = mock_batch

        await handler._send_batch(mock_producer, ["log1", "log2"])

        mock_producer.create_batch.assert_called_once()
        assert mock_batch.add.call_count == 2
        mock_producer.send_batch.assert_called_once_with(mock_batch)
        assert handler._total_sent == 2

    @pytest.mark.asyncio
    @patch.object(EventHubLogHandler, "_start_sender")
    async def test_skips_empty_batch(self, mock_start):
        handler = EventHubLogHandler(
            connection_string="Endpoint=sb://test",
            eventhub_name="test-hub",
        )

        mock_producer = AsyncMock()
        await handler._send_batch(mock_producer, [])

        mock_producer.create_batch.assert_not_called()

    @pytest.mark.asyncio
    @patch.object(EventHubLogHandler, "_start_sender")
    async def test_handles_batch_full_error(self, mock_start):
        handler = EventHubLogHandler(
            connection_string="Endpoint=sb://test",
            eventhub_name="test-hub",
        )

        mock_producer = AsyncMock()
        mock_batch1 = MagicMock()
        mock_batch1.__len__ = MagicMock(return_value=1)
        mock_batch2 = MagicMock()
        mock_batch2.__len__ = MagicMock(return_value=1)

        # First add succeeds, second raises ValueError (batch full),
        # then new batch is created
        mock_batch1.add.side_effect = [None, ValueError("batch full")]
        mock_batch2.add.side_effect = [None]
        mock_producer.create_batch.side_effect = [mock_batch1, mock_batch2]

        await handler._send_batch(mock_producer, ["log1", "log2"])

        # Should have created 2 batches and sent both
        assert mock_producer.create_batch.call_count == 2
        assert mock_producer.send_batch.call_count == 2

    @pytest.mark.asyncio
    @patch.object(EventHubLogHandler, "_start_sender")
    async def test_resets_failure_count_on_success(self, mock_start):
        handler = EventHubLogHandler(
            connection_string="Endpoint=sb://test",
            eventhub_name="test-hub",
        )
        handler._failure_count = 3

        mock_producer = AsyncMock()
        mock_batch = MagicMock()
        mock_batch.__len__ = MagicMock(return_value=1)
        mock_producer.create_batch.return_value = mock_batch

        await handler._send_batch(mock_producer, ["log1"])

        assert handler._failure_count == 0

    @pytest.mark.asyncio
    @patch.object(EventHubLogHandler, "_start_sender")
    async def test_closes_circuit_on_success(self, mock_start):
        handler = EventHubLogHandler(
            connection_string="Endpoint=sb://test",
            eventhub_name="test-hub",
        )
        handler._circuit_open = True

        mock_producer = AsyncMock()
        mock_batch = MagicMock()
        mock_batch.__len__ = MagicMock(return_value=1)
        mock_producer.create_batch.return_value = mock_batch

        await handler._send_batch(mock_producer, ["log1"])

        assert handler._circuit_open is False

    @pytest.mark.asyncio
    @patch.object(EventHubLogHandler, "_start_sender")
    async def test_handles_send_error_and_reraises(self, mock_start):
        handler = EventHubLogHandler(
            connection_string="Endpoint=sb://test",
            eventhub_name="test-hub",
        )

        mock_producer = AsyncMock()
        mock_producer.create_batch.side_effect = Exception("network error")

        with pytest.raises(Exception, match="network error"):
            await handler._send_batch(mock_producer, ["log1"])

        assert handler._failure_count == 1

    @pytest.mark.asyncio
    @patch.object(EventHubLogHandler, "_start_sender")
    async def test_does_not_send_when_batch_empty_after_create(self, mock_start):
        handler = EventHubLogHandler(
            connection_string="Endpoint=sb://test",
            eventhub_name="test-hub",
        )

        mock_producer = AsyncMock()
        mock_batch = MagicMock()
        mock_batch.__len__ = MagicMock(return_value=0)
        mock_producer.create_batch.return_value = mock_batch

        await handler._send_batch(mock_producer, ["log1"])

        # add was called, but len is 0 so send_batch should NOT be called
        mock_producer.send_batch.assert_not_called()
        assert handler._total_sent == 0


class TestEventHubLogHandlerStartSender:

    def test_sender_thread_is_daemon(self):
        with patch.object(threading.Thread, "start"):
            handler = EventHubLogHandler(
                connection_string="Endpoint=sb://test",
                eventhub_name="test-hub",
            )

        assert handler._sender_thread is not None
        assert handler._sender_thread.daemon is True
        assert handler._sender_thread.name == "eventhub-log-sender"
