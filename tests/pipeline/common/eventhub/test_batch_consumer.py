"""Tests for pipeline.common.eventhub.batch_consumer module.

Covers EventHubBatchConsumer lifecycle, batch accumulation, timeout flushing,
partition management, and checkpoint behavior.
"""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

from pipeline.common.types import PipelineMessage

# =============================================================================
# BufferedMessage
# =============================================================================


class TestBufferedMessage:
    def test_buffered_message_dataclass(self):
        from pipeline.common.eventhub.batch_consumer import BufferedMessage

        ctx = MagicMock()
        event = MagicMock()
        msg = PipelineMessage(topic="t", partition=0, offset=1, timestamp=100, value=b"v")

        bm = BufferedMessage(partition_context=ctx, event=event, message=msg)

        assert bm.partition_context is ctx
        assert bm.event is event
        assert bm.message is msg


# =============================================================================
# EventHubBatchConsumer - Init
# =============================================================================


class TestBatchConsumerInit:
    def test_init_sets_attributes(self):
        from pipeline.common.eventhub.batch_consumer import EventHubBatchConsumer

        handler = AsyncMock()
        consumer = EventHubBatchConsumer(
            connection_string="Endpoint=sb://test.net/;SharedAccessKey=k",
            domain="verisk",
            worker_name="test-worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            batch_handler=handler,
            batch_size=50,
            batch_timeout_ms=2000,
        )

        assert consumer.eventhub_name == "verisk_events"
        assert consumer.consumer_group == "$Default"
        assert consumer.batch_size == 50
        assert consumer.batch_timeout_ms == 2000
        assert consumer._running is False
        assert consumer._consumer is None
        assert consumer._batch_buffers == {}

    def test_init_defaults(self):
        from pipeline.common.eventhub.batch_consumer import EventHubBatchConsumer

        consumer = EventHubBatchConsumer(
            connection_string="conn",
            domain="verisk",
            worker_name="w",
            eventhub_name="entity",
            consumer_group="$Default",
            batch_handler=AsyncMock(),
        )

        assert consumer.batch_size == 20
        assert consumer.batch_timeout_ms == 1000
        assert consumer._enable_message_commit is True


# =============================================================================
# EventHubBatchConsumer - Start / Stop
# =============================================================================


class TestBatchConsumerLifecycle:
    def _make_consumer(self, **overrides):
        from pipeline.common.eventhub.batch_consumer import EventHubBatchConsumer

        defaults = {
            "connection_string": "Endpoint=sb://test.net/;SharedAccessKey=k",
            "domain": "verisk",
            "worker_name": "test",
            "eventhub_name": "verisk_events",
            "consumer_group": "$Default",
            "batch_handler": AsyncMock(return_value=True),
        }
        defaults.update(overrides)
        return EventHubBatchConsumer(**defaults)

    async def test_start_when_already_running_returns_early(self):
        consumer = self._make_consumer()
        consumer._running = True

        # Should not raise
        await consumer.start()

    @patch("pipeline.common.eventhub.batch_consumer.update_connection_status")
    @patch("pipeline.common.eventhub.batch_consumer.update_assigned_partitions")
    async def test_stop_when_not_running(self, mock_partitions, mock_connection):
        consumer = self._make_consumer()

        # Should not raise
        await consumer.stop()

    @patch("pipeline.common.eventhub.batch_consumer.update_connection_status")
    @patch("pipeline.common.eventhub.batch_consumer.update_assigned_partitions")
    async def test_stop_cancels_flush_task_and_closes_consumer(
        self, mock_partitions, mock_connection
    ):
        consumer = self._make_consumer()
        consumer._running = True
        consumer._consumer = AsyncMock()
        consumer._flush_task = asyncio.create_task(asyncio.sleep(100))

        await consumer.stop()

        assert consumer._running is False
        assert consumer._consumer is None
        mock_connection.assert_called_with("consumer", connected=False)
        mock_partitions.assert_called_with("$Default", 0)

    async def test_commit_is_noop(self):
        consumer = self._make_consumer()

        # Should not raise
        await consumer.commit()


# =============================================================================
# EventHubBatchConsumer - Batch Stats
# =============================================================================


class TestBatchStats:
    def test_get_batch_stats_empty_initially(self):
        from pipeline.common.eventhub.batch_consumer import EventHubBatchConsumer

        consumer = EventHubBatchConsumer(
            connection_string="conn",
            domain="verisk",
            worker_name="w",
            eventhub_name="entity",
            consumer_group="$Default",
            batch_handler=AsyncMock(),
        )

        assert consumer.get_batch_stats() == {}

    def test_get_batch_stats_returns_buffer_sizes(self):
        from pipeline.common.eventhub.batch_consumer import (
            BufferedMessage,
            EventHubBatchConsumer,
        )

        consumer = EventHubBatchConsumer(
            connection_string="conn",
            domain="verisk",
            worker_name="w",
            eventhub_name="entity",
            consumer_group="$Default",
            batch_handler=AsyncMock(),
        )

        msg = PipelineMessage(topic="t", partition=0, offset=0, timestamp=0, value=b"x")
        consumer._batch_buffers["0"] = [
            BufferedMessage(MagicMock(), MagicMock(), msg),
            BufferedMessage(MagicMock(), MagicMock(), msg),
        ]
        consumer._batch_buffers["1"] = [
            BufferedMessage(MagicMock(), MagicMock(), msg),
        ]

        stats = consumer.get_batch_stats()
        assert stats == {"0": 2, "1": 1}


# =============================================================================
# EventHubBatchConsumer - _flush_partition_batch
# =============================================================================


class TestFlushPartitionBatch:
    def _make_consumer(self, handler=None):
        from pipeline.common.eventhub.batch_consumer import EventHubBatchConsumer

        return EventHubBatchConsumer(
            connection_string="conn",
            domain="verisk",
            worker_name="w",
            eventhub_name="entity",
            consumer_group="$Default",
            batch_handler=handler or AsyncMock(return_value=True),
        )

    async def test_flush_empty_buffer_does_nothing(self):
        consumer = self._make_consumer()
        consumer._batch_buffers["0"] = []
        consumer._batch_timers["0"] = time.time()
        consumer._flush_locks["0"] = asyncio.Lock()

        # Should not raise
        await consumer._flush_partition_batch("0")

    async def test_flush_returns_when_lock_not_found(self):
        consumer = self._make_consumer()

        # Partition "99" has no lock (e.g., already closed)
        await consumer._flush_partition_batch("99")

    async def test_flush_calls_handler_with_messages(self):
        from pipeline.common.eventhub.batch_consumer import BufferedMessage

        handler = AsyncMock(return_value=True)
        consumer = self._make_consumer(handler=handler)

        msg1 = PipelineMessage(topic="t", partition=0, offset=0, timestamp=0, value=b"a")
        msg2 = PipelineMessage(topic="t", partition=0, offset=1, timestamp=0, value=b"b")

        ctx = AsyncMock()
        event1 = MagicMock()
        event2 = MagicMock()

        consumer._batch_buffers["0"] = [
            BufferedMessage(ctx, event1, msg1),
            BufferedMessage(ctx, event2, msg2),
        ]
        consumer._batch_timers["0"] = time.time()
        consumer._flush_locks["0"] = asyncio.Lock()

        await consumer._flush_partition_batch("0")

        handler.assert_awaited_once()
        args = handler.call_args[0][0]
        assert len(args) == 2
        assert args[0] is msg1
        assert args[1] is msg2

    async def test_flush_checkpoints_when_handler_returns_true(self):
        from pipeline.common.eventhub.batch_consumer import BufferedMessage

        handler = AsyncMock(return_value=True)
        consumer = self._make_consumer(handler=handler)

        msg = PipelineMessage(topic="t", partition=0, offset=0, timestamp=0, value=b"a")
        ctx = AsyncMock()
        event = MagicMock()

        consumer._batch_buffers["0"] = [BufferedMessage(ctx, event, msg)]
        consumer._batch_timers["0"] = time.time()
        consumer._flush_locks["0"] = asyncio.Lock()

        await consumer._flush_partition_batch("0")

        ctx.update_checkpoint.assert_awaited_once_with(event)

    async def test_flush_skips_checkpoint_when_handler_returns_false(self):
        from pipeline.common.eventhub.batch_consumer import BufferedMessage

        handler = AsyncMock(return_value=False)
        consumer = self._make_consumer(handler=handler)

        msg = PipelineMessage(topic="t", partition=0, offset=0, timestamp=0, value=b"a")
        ctx = AsyncMock()
        event = MagicMock()

        consumer._batch_buffers["0"] = [BufferedMessage(ctx, event, msg)]
        consumer._batch_timers["0"] = time.time()
        consumer._flush_locks["0"] = asyncio.Lock()

        await consumer._flush_partition_batch("0")

        ctx.update_checkpoint.assert_not_awaited()

    async def test_flush_skips_checkpoint_when_commit_disabled(self):
        from pipeline.common.eventhub.batch_consumer import BufferedMessage

        handler = AsyncMock(return_value=True)
        consumer = self._make_consumer(handler=handler)
        consumer._enable_message_commit = False

        msg = PipelineMessage(topic="t", partition=0, offset=0, timestamp=0, value=b"a")
        ctx = AsyncMock()
        event = MagicMock()

        consumer._batch_buffers["0"] = [BufferedMessage(ctx, event, msg)]
        consumer._batch_timers["0"] = time.time()
        consumer._flush_locks["0"] = asyncio.Lock()

        await consumer._flush_partition_batch("0")

        ctx.update_checkpoint.assert_not_awaited()

    async def test_flush_handles_handler_exception_without_raising(self):
        from pipeline.common.eventhub.batch_consumer import BufferedMessage

        handler = AsyncMock(side_effect=RuntimeError("handler boom"))
        consumer = self._make_consumer(handler=handler)

        msg = PipelineMessage(topic="t", partition=0, offset=0, timestamp=0, value=b"a")
        ctx = AsyncMock()
        event = MagicMock()

        consumer._batch_buffers["0"] = [BufferedMessage(ctx, event, msg)]
        consumer._batch_timers["0"] = time.time()
        consumer._flush_locks["0"] = asyncio.Lock()

        # Should not raise - error is caught
        await consumer._flush_partition_batch("0")

        ctx.update_checkpoint.assert_not_awaited()

    async def test_flush_continues_on_checkpoint_error(self):
        from pipeline.common.eventhub.batch_consumer import BufferedMessage

        handler = AsyncMock(return_value=True)
        consumer = self._make_consumer(handler=handler)

        msg1 = PipelineMessage(topic="t", partition=0, offset=0, timestamp=0, value=b"a")
        msg2 = PipelineMessage(topic="t", partition=0, offset=1, timestamp=0, value=b"b")

        ctx1 = AsyncMock()
        ctx1.update_checkpoint = AsyncMock(side_effect=RuntimeError("checkpoint fail"))
        ctx2 = AsyncMock()
        event1 = MagicMock()
        event2 = MagicMock()

        consumer._batch_buffers["0"] = [
            BufferedMessage(ctx1, event1, msg1),
            BufferedMessage(ctx2, event2, msg2),
        ]
        consumer._batch_timers["0"] = time.time()
        consumer._flush_locks["0"] = asyncio.Lock()

        # Should not raise - continues to next message
        await consumer._flush_partition_batch("0")

        # Second checkpoint should still be attempted
        ctx2.update_checkpoint.assert_awaited_once_with(event2)

    async def test_flush_resets_buffer_and_timer(self):
        from pipeline.common.eventhub.batch_consumer import BufferedMessage

        handler = AsyncMock(return_value=True)
        consumer = self._make_consumer(handler=handler)

        msg = PipelineMessage(topic="t", partition=0, offset=0, timestamp=0, value=b"a")

        old_time = time.time() - 10
        consumer._batch_buffers["0"] = [BufferedMessage(AsyncMock(), MagicMock(), msg)]
        consumer._batch_timers["0"] = old_time
        consumer._flush_locks["0"] = asyncio.Lock()

        await consumer._flush_partition_batch("0")

        # Buffer should be empty
        assert consumer._batch_buffers["0"] == []
        # Timer should be reset (more recent)
        assert consumer._batch_timers["0"] > old_time
