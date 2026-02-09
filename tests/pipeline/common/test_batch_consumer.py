"""Tests for MessageBatchConsumer."""

import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest
from aiokafka.structs import ConsumerRecord, TopicPartition

from pipeline.common.types import PipelineMessage


def _make_config(**overrides):
    config = Mock()
    config.bootstrap_servers = overrides.get("bootstrap_servers", "localhost:9092")
    config.request_timeout_ms = 120000
    config.metadata_max_age_ms = 300000
    config.connections_max_idle_ms = 540000
    config.security_protocol = "PLAINTEXT"
    config.get_worker_config.return_value = overrides.get("worker_config", {})
    config.get_consumer_group.return_value = overrides.get(
        "group_id", "test-domain-test-worker"
    )
    return config


def _make_kafka_mock(**overrides):
    """Create a mock AIOKafkaConsumer with sync/async methods set correctly."""
    mock = Mock()
    mock.start = AsyncMock()
    mock.stop = AsyncMock()
    mock.commit = AsyncMock()
    mock.getmany = AsyncMock(return_value={})
    mock.assignment = Mock(return_value=overrides.get("assignment", set()))
    return mock


def _make_consumer_record(
    topic="test-topic", partition=0, offset=0, key=b"k", value=b"v", timestamp=1000
):
    return ConsumerRecord(
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp=timestamp,
        timestamp_type=0,
        key=key,
        value=value,
        headers=[],
        checksum=None,
        serialized_key_size=len(key) if key else 0,
        serialized_value_size=len(value) if value else 0,
    )


class TestMessageBatchConsumerInit:

    def test_raises_on_empty_topics(self):
        from pipeline.common.batch_consumer import MessageBatchConsumer

        config = _make_config()
        with pytest.raises(ValueError, match="At least one topic"):
            MessageBatchConsumer(
                config=config,
                domain="verisk",
                worker_name="test",
                topics=[],
                batch_handler=AsyncMock(),
            )

    def test_stores_batch_settings(self):
        from pipeline.common.batch_consumer import MessageBatchConsumer

        config = _make_config()
        consumer = MessageBatchConsumer(
            config=config,
            domain="verisk",
            worker_name="test",
            topics=["t1"],
            batch_handler=AsyncMock(),
            batch_size=50,
            batch_timeout_ms=2000,
        )
        assert consumer.batch_size == 50
        assert consumer.batch_timeout_ms == 2000

    def test_default_batch_settings(self):
        from pipeline.common.batch_consumer import MessageBatchConsumer

        config = _make_config()
        consumer = MessageBatchConsumer(
            config=config,
            domain="verisk",
            worker_name="test",
            topics=["t1"],
            batch_handler=AsyncMock(),
        )
        assert consumer.batch_size == 20
        assert consumer.batch_timeout_ms == 1000

    def test_reads_max_batches_from_processing_config(self):
        from pipeline.common.batch_consumer import MessageBatchConsumer

        config = _make_config()
        config.get_worker_config.side_effect = lambda d, w, c: (
            {"max_batches": 10} if c == "processing" else {}
        )
        consumer = MessageBatchConsumer(
            config=config,
            domain="verisk",
            worker_name="test",
            topics=["t1"],
            batch_handler=AsyncMock(),
        )
        assert consumer.max_batches == 10

    def test_enable_message_commit_defaults_true(self):
        from pipeline.common.batch_consumer import MessageBatchConsumer

        config = _make_config()
        consumer = MessageBatchConsumer(
            config=config,
            domain="verisk",
            worker_name="test",
            topics=["t1"],
            batch_handler=AsyncMock(),
        )
        assert consumer._enable_message_commit is True


class TestMessageBatchConsumerStart:

    async def test_start_creates_consumer_with_batch_size(self):
        from pipeline.common.batch_consumer import MessageBatchConsumer

        config = _make_config()
        consumer = MessageBatchConsumer(
            config=config,
            domain="verisk",
            worker_name="test",
            topics=["topic-a"],
            batch_handler=AsyncMock(),
            batch_size=30,
        )

        mock_kafka = _make_kafka_mock()

        with patch("pipeline.common.batch_consumer.AIOKafkaConsumer", return_value=mock_kafka) as mock_cls:
            with patch("pipeline.common.batch_consumer.build_kafka_security_config", return_value={}):
                with patch.object(consumer, "_consume_loop", side_effect=asyncio.CancelledError):
                    with pytest.raises(asyncio.CancelledError):
                        await consumer.start()

        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["max_poll_records"] == 30
        mock_kafka.start.assert_awaited_once()

    async def test_start_ignores_duplicate_call(self):
        from pipeline.common.batch_consumer import MessageBatchConsumer

        config = _make_config()
        consumer = MessageBatchConsumer(
            config=config,
            domain="verisk",
            worker_name="test",
            topics=["t1"],
            batch_handler=AsyncMock(),
        )
        consumer._running = True

        await consumer.start()

    async def test_start_uses_instance_id_in_client_id(self):
        from pipeline.common.batch_consumer import MessageBatchConsumer

        config = _make_config()
        consumer = MessageBatchConsumer(
            config=config,
            domain="verisk",
            worker_name="test",
            topics=["t1"],
            batch_handler=AsyncMock(),
            instance_id="5",
        )

        mock_kafka = _make_kafka_mock()

        with patch("pipeline.common.batch_consumer.AIOKafkaConsumer", return_value=mock_kafka) as mock_cls:
            with patch("pipeline.common.batch_consumer.build_kafka_security_config", return_value={}):
                with patch.object(consumer, "_consume_loop", side_effect=asyncio.CancelledError):
                    with pytest.raises(asyncio.CancelledError):
                        await consumer.start()

        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["client_id"] == "verisk-test-5"


class TestMessageBatchConsumerStop:

    async def test_stop_commits_and_stops(self):
        from pipeline.common.batch_consumer import MessageBatchConsumer

        config = _make_config()
        consumer = MessageBatchConsumer(
            config=config,
            domain="verisk",
            worker_name="test",
            topics=["t1"],
            batch_handler=AsyncMock(),
        )
        mock_kafka = _make_kafka_mock()
        consumer._consumer = mock_kafka
        consumer._running = True

        await consumer.stop()

        mock_kafka.commit.assert_awaited_once()
        mock_kafka.stop.assert_awaited_once()
        assert consumer._consumer is None
        assert consumer._running is False

    async def test_stop_noop_when_not_running(self):
        from pipeline.common.batch_consumer import MessageBatchConsumer

        config = _make_config()
        consumer = MessageBatchConsumer(
            config=config,
            domain="verisk",
            worker_name="test",
            topics=["t1"],
            batch_handler=AsyncMock(),
        )
        await consumer.stop()  # Should not raise

    async def test_stop_reraises_on_error(self):
        from pipeline.common.batch_consumer import MessageBatchConsumer

        config = _make_config()
        consumer = MessageBatchConsumer(
            config=config,
            domain="verisk",
            worker_name="test",
            topics=["t1"],
            batch_handler=AsyncMock(),
        )
        mock_kafka = _make_kafka_mock()
        mock_kafka.commit.side_effect = RuntimeError("commit failed")
        consumer._consumer = mock_kafka
        consumer._running = True

        with pytest.raises(RuntimeError, match="commit failed"):
            await consumer.stop()

        # Cleanup still happens in finally
        assert consumer._consumer is None


class TestMessageBatchConsumerCommit:

    async def test_commit_calls_consumer_commit(self):
        from pipeline.common.batch_consumer import MessageBatchConsumer

        config = _make_config()
        consumer = MessageBatchConsumer(
            config=config,
            domain="verisk",
            worker_name="test",
            topics=["t1"],
            batch_handler=AsyncMock(),
        )
        mock_kafka = _make_kafka_mock()
        consumer._consumer = mock_kafka

        await consumer.commit()
        mock_kafka.commit.assert_awaited_once()

    async def test_commit_noop_when_no_consumer(self):
        from pipeline.common.batch_consumer import MessageBatchConsumer

        config = _make_config()
        consumer = MessageBatchConsumer(
            config=config,
            domain="verisk",
            worker_name="test",
            topics=["t1"],
            batch_handler=AsyncMock(),
        )
        consumer._consumer = None
        await consumer.commit()  # Should not raise


class TestMessageBatchConsumerConsumeLoop:

    async def test_stops_at_max_batches(self):
        from pipeline.common.batch_consumer import MessageBatchConsumer

        config = _make_config()
        config.get_worker_config.side_effect = lambda d, w, c: (
            {"max_batches": 1} if c == "processing" else {}
        )
        consumer = MessageBatchConsumer(
            config=config,
            domain="verisk",
            worker_name="test",
            topics=["t1"],
            batch_handler=AsyncMock(),
        )
        consumer._running = True
        consumer._batch_count = 1

        tp = TopicPartition("t1", 0)
        mock_kafka = _make_kafka_mock(assignment={tp})
        consumer._consumer = mock_kafka

        await consumer._consume_loop()
        mock_kafka.getmany.assert_not_awaited()

    async def test_waits_for_partition_assignment(self):
        from pipeline.common.batch_consumer import MessageBatchConsumer

        config = _make_config()
        consumer = MessageBatchConsumer(
            config=config,
            domain="verisk",
            worker_name="test",
            topics=["t1"],
            batch_handler=AsyncMock(),
        )
        consumer._running = True

        mock_kafka = _make_kafka_mock()
        call_count = 0

        def assignment_side_effect():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return set()
            consumer._running = False
            return set()

        mock_kafka.assignment.side_effect = assignment_side_effect
        consumer._consumer = mock_kafka

        with patch("pipeline.common.batch_consumer.asyncio.sleep", new_callable=AsyncMock):
            await consumer._consume_loop()

    async def test_commits_when_handler_returns_true(self):
        from pipeline.common.batch_consumer import MessageBatchConsumer

        config = _make_config()
        handler = AsyncMock(return_value=True)
        consumer = MessageBatchConsumer(
            config=config,
            domain="verisk",
            worker_name="test",
            topics=["t1"],
            batch_handler=handler,
        )
        consumer._running = True

        record = _make_consumer_record()
        tp = TopicPartition("test-topic", 0)
        mock_kafka = _make_kafka_mock(assignment={tp})

        call_count = 0

        async def getmany_side_effect(timeout_ms=1000, max_records=20):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {tp: [record]}
            consumer._running = False
            return {}

        mock_kafka.getmany.side_effect = getmany_side_effect
        consumer._consumer = mock_kafka

        await consumer._consume_loop()

        handler.assert_awaited_once()
        # Verify handler received PipelineMessage list
        batch = handler.call_args[0][0]
        assert len(batch) == 1
        assert isinstance(batch[0], PipelineMessage)
        # Verify commit was called (handler returned True + commit enabled)
        assert mock_kafka.commit.await_count == 1

    async def test_skips_commit_when_handler_returns_false(self):
        from pipeline.common.batch_consumer import MessageBatchConsumer

        config = _make_config()
        handler = AsyncMock(return_value=False)
        consumer = MessageBatchConsumer(
            config=config,
            domain="verisk",
            worker_name="test",
            topics=["t1"],
            batch_handler=handler,
        )
        consumer._running = True

        record = _make_consumer_record()
        tp = TopicPartition("test-topic", 0)
        mock_kafka = _make_kafka_mock(assignment={tp})

        call_count = 0

        async def getmany_side_effect(timeout_ms=1000, max_records=20):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {tp: [record]}
            consumer._running = False
            return {}

        mock_kafka.getmany.side_effect = getmany_side_effect
        consumer._consumer = mock_kafka

        await consumer._consume_loop()

        mock_kafka.commit.assert_not_awaited()

    async def test_skips_commit_when_commit_disabled(self):
        from pipeline.common.batch_consumer import MessageBatchConsumer

        config = _make_config()
        handler = AsyncMock(return_value=True)
        consumer = MessageBatchConsumer(
            config=config,
            domain="verisk",
            worker_name="test",
            topics=["t1"],
            batch_handler=handler,
            enable_message_commit=False,
        )
        consumer._running = True

        record = _make_consumer_record()
        tp = TopicPartition("test-topic", 0)
        mock_kafka = _make_kafka_mock(assignment={tp})

        call_count = 0

        async def getmany_side_effect(timeout_ms=1000, max_records=20):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {tp: [record]}
            consumer._running = False
            return {}

        mock_kafka.getmany.side_effect = getmany_side_effect
        consumer._consumer = mock_kafka

        await consumer._consume_loop()

        mock_kafka.commit.assert_not_awaited()

    async def test_handler_exception_does_not_commit(self):
        from pipeline.common.batch_consumer import MessageBatchConsumer

        config = _make_config()
        handler = AsyncMock(side_effect=ValueError("batch processing error"))
        consumer = MessageBatchConsumer(
            config=config,
            domain="verisk",
            worker_name="test",
            topics=["t1"],
            batch_handler=handler,
        )
        consumer._running = True

        record = _make_consumer_record()
        tp = TopicPartition("test-topic", 0)
        mock_kafka = _make_kafka_mock(assignment={tp})

        call_count = 0

        async def getmany_side_effect(timeout_ms=1000, max_records=20):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {tp: [record]}
            consumer._running = False
            return {}

        mock_kafka.getmany.side_effect = getmany_side_effect
        consumer._consumer = mock_kafka

        # Should not raise - error is caught and logged
        await consumer._consume_loop()

        mock_kafka.commit.assert_not_awaited()

    async def test_flattens_messages_from_multiple_partitions(self):
        from pipeline.common.batch_consumer import MessageBatchConsumer

        config = _make_config()
        received_batches = []

        async def handler(batch):
            received_batches.append(batch)
            return True

        consumer = MessageBatchConsumer(
            config=config,
            domain="verisk",
            worker_name="test",
            topics=["t1"],
            batch_handler=handler,
        )
        consumer._running = True

        tp0 = TopicPartition("t1", 0)
        tp1 = TopicPartition("t1", 1)
        record_p0 = _make_consumer_record(partition=0, offset=0)
        record_p1 = _make_consumer_record(partition=1, offset=0)

        mock_kafka = _make_kafka_mock(assignment={tp0, tp1})

        call_count = 0

        async def getmany_side_effect(timeout_ms=1000, max_records=20):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {tp0: [record_p0], tp1: [record_p1]}
            consumer._running = False
            return {}

        mock_kafka.getmany.side_effect = getmany_side_effect
        consumer._consumer = mock_kafka

        await consumer._consume_loop()

        assert len(received_batches) == 1
        assert len(received_batches[0]) == 2

    async def test_skips_empty_data(self):
        from pipeline.common.batch_consumer import MessageBatchConsumer

        config = _make_config()
        handler = AsyncMock(return_value=True)
        consumer = MessageBatchConsumer(
            config=config,
            domain="verisk",
            worker_name="test",
            topics=["t1"],
            batch_handler=handler,
        )
        consumer._running = True

        tp = TopicPartition("t1", 0)
        mock_kafka = _make_kafka_mock(assignment={tp})

        call_count = 0

        async def getmany_side_effect(timeout_ms=1000, max_records=20):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                return {}  # Empty data
            consumer._running = False
            return {}

        mock_kafka.getmany.side_effect = getmany_side_effect
        consumer._consumer = mock_kafka

        await consumer._consume_loop()

        handler.assert_not_awaited()
        assert consumer._batch_count == 0

    async def test_cancellation_propagates(self):
        from pipeline.common.batch_consumer import MessageBatchConsumer

        config = _make_config()
        consumer = MessageBatchConsumer(
            config=config,
            domain="verisk",
            worker_name="test",
            topics=["t1"],
            batch_handler=AsyncMock(),
        )
        consumer._running = True

        tp = TopicPartition("t1", 0)
        mock_kafka = _make_kafka_mock(assignment={tp})
        mock_kafka.getmany.side_effect = asyncio.CancelledError

        consumer._consumer = mock_kafka

        with pytest.raises(asyncio.CancelledError):
            await consumer._consume_loop()
