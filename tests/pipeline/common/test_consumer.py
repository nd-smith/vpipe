"""Tests for MessageConsumer."""

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
    config.get_consumer_group.return_value = overrides.get("group_id", "test-domain-test-worker")
    return config


def _make_kafka_mock(**overrides):
    """Create a mock AIOKafkaConsumer with sync/async methods set correctly."""
    mock = Mock()
    mock.start = AsyncMock()
    mock.stop = AsyncMock()
    mock.commit = AsyncMock()
    mock.getmany = AsyncMock(return_value={})
    mock.assignment = Mock(return_value=overrides.get("assignment", set()))
    mock.highwater = Mock(return_value=overrides.get("highwater"))
    return mock


def _make_consumer_record(
    topic="test-topic",
    partition=0,
    offset=42,
    key=b"key-1",
    value=b'{"data": "hello"}',
    timestamp=1000,
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


def _make_consumer(**overrides):
    """Create a MessageConsumer with DLQ producer replaced by AsyncMock."""
    from pipeline.common.consumer import MessageConsumer

    config = overrides.pop("config", _make_config(**overrides))
    with (
        patch("pipeline.common.dlq.producer.DLQProducer"),
        patch("pipeline.common.consumer.generate_worker_id", return_value="test-worker-id"),
    ):
        consumer = MessageConsumer(
            config=config,
            domain=overrides.get("domain", "verisk"),
            worker_name=overrides.get("worker_name", "test"),
            topics=overrides.get("topics", ["t1"]),
            message_handler=overrides.get("message_handler", AsyncMock()),
            enable_message_commit=overrides.get("enable_message_commit", True),
            instance_id=overrides.get("instance_id"),
        )
    # Replace the MagicMock DLQ producer with AsyncMock so await works
    consumer._dlq_producer = AsyncMock()
    return consumer


class TestMessageConsumerInit:
    def test_raises_on_empty_topics(self):
        from pipeline.common.consumer import MessageConsumer

        config = _make_config()
        with (
            patch("pipeline.common.dlq.producer.DLQProducer"),
            patch("pipeline.common.consumer.generate_worker_id"),
            pytest.raises(ValueError, match="At least one topic"),
        ):
            MessageConsumer(
                config=config,
                domain="verisk",
                worker_name="test",
                topics=[],
                message_handler=AsyncMock(),
            )

    def test_sets_worker_id_with_instance_id(self):
        with (
            patch("pipeline.common.dlq.producer.DLQProducer"),
            patch(
                "pipeline.common.consumer.generate_worker_id",
                return_value="test-worker-id",
            ) as mock_worker_id,
        ):
            from pipeline.common.consumer import MessageConsumer

            config = _make_config()
            MessageConsumer(
                config=config,
                domain="verisk",
                worker_name="test",
                topics=["t1"],
                message_handler=AsyncMock(),
                instance_id="3",
            )
            mock_worker_id.assert_called_once_with("verisk-test-3")

    def test_sets_worker_id_without_instance_id(self):
        with (
            patch("pipeline.common.dlq.producer.DLQProducer"),
            patch(
                "pipeline.common.consumer.generate_worker_id",
                return_value="test-worker-id",
            ) as mock_worker_id,
        ):
            from pipeline.common.consumer import MessageConsumer

            config = _make_config()
            MessageConsumer(
                config=config,
                domain="verisk",
                worker_name="test",
                topics=["t1"],
                message_handler=AsyncMock(),
            )
            mock_worker_id.assert_called_once_with("verisk-test")

    def test_reads_max_batches_from_processing_config(self):
        config = _make_config()
        config.get_worker_config.side_effect = lambda d, w, c: (
            {"max_batches": 5} if c == "processing" else {}
        )
        consumer = _make_consumer(config=config)
        assert consumer.max_batches == 5

    def test_enable_message_commit_defaults_true(self):
        consumer = _make_consumer()
        assert consumer._enable_message_commit is True

    def test_enable_message_commit_can_be_disabled(self):
        consumer = _make_consumer(enable_message_commit=False)
        assert consumer._enable_message_commit is False


class TestMessageConsumerStart:
    async def test_start_creates_and_starts_kafka_consumer(self):
        consumer = _make_consumer()
        mock_kafka = _make_kafka_mock()

        with (
            patch("pipeline.common.consumer.AIOKafkaConsumer", return_value=mock_kafka),
            patch("pipeline.common.consumer.build_kafka_security_config", return_value={}),
            patch.object(consumer, "_consume_loop", side_effect=asyncio.CancelledError),
            pytest.raises(asyncio.CancelledError),
        ):
            await consumer.start()

        mock_kafka.start.assert_awaited_once()
        assert consumer._running is False

    async def test_start_ignores_duplicate_call(self):
        consumer = _make_consumer()
        consumer._running = True
        await consumer.start()

    async def test_start_uses_instance_id_in_client_id(self):
        consumer = _make_consumer(instance_id="7")
        mock_kafka = _make_kafka_mock()

        with (
            patch("pipeline.common.consumer.AIOKafkaConsumer", return_value=mock_kafka) as mock_cls,
            patch("pipeline.common.consumer.build_kafka_security_config", return_value={}),
            patch.object(consumer, "_consume_loop", side_effect=asyncio.CancelledError),
            pytest.raises(asyncio.CancelledError),
        ):
            await consumer.start()

        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["client_id"] == "verisk-test-7"


class TestMessageConsumerStop:
    async def test_stop_commits_and_stops_consumer(self):
        consumer = _make_consumer()
        consumer._consumer = _make_kafka_mock()
        consumer._running = True

        await consumer.stop()

        assert consumer._consumer is None
        assert consumer._running is False

    async def test_stop_noop_when_not_running(self):
        consumer = _make_consumer()
        await consumer.stop()

    async def test_stop_stops_dlq_producer(self):
        consumer = _make_consumer()
        consumer._consumer = _make_kafka_mock()
        consumer._running = True

        await consumer.stop()

        consumer._dlq_producer.stop.assert_awaited_once()


class TestMessageConsumerCommit:
    async def test_commit_calls_consumer_commit(self):
        consumer = _make_consumer()
        mock_kafka = _make_kafka_mock()
        consumer._consumer = mock_kafka

        await consumer.commit()
        mock_kafka.commit.assert_awaited_once()

    async def test_commit_noop_when_no_consumer(self):
        consumer = _make_consumer()
        consumer._consumer = None
        await consumer.commit()


class TestMessageConsumerConsumeLoop:
    async def test_consume_loop_stops_at_max_batches(self):
        config = _make_config()
        config.get_worker_config.side_effect = lambda d, w, c: (
            {"max_batches": 2} if c == "processing" else {}
        )
        consumer = _make_consumer(config=config)
        consumer._running = True
        consumer._batch_count = 2

        tp = TopicPartition("t1", 0)
        mock_kafka = _make_kafka_mock(assignment={tp})
        consumer._consumer = mock_kafka

        await consumer._consume_loop()
        mock_kafka.getmany.assert_not_awaited()

    async def test_consume_loop_waits_for_partition_assignment(self):
        consumer = _make_consumer()
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

        with patch("pipeline.common.consumer.asyncio.sleep", new_callable=AsyncMock):
            await consumer._consume_loop()

    async def test_consume_loop_processes_messages(self):
        consumer = _make_consumer()
        consumer._running = True

        record = _make_consumer_record()
        tp = TopicPartition("test-topic", 0)

        mock_kafka = _make_kafka_mock(assignment={tp}, highwater=100)
        call_count = 0

        async def getmany_side_effect(timeout_ms=1000):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {tp: [record]}
            consumer._running = False
            return {}

        mock_kafka.getmany.side_effect = getmany_side_effect
        consumer._consumer = mock_kafka

        with patch.object(consumer, "_process_message", new_callable=AsyncMock) as mock_process:
            await consumer._consume_loop()

        mock_process.assert_awaited_once_with(record)
        assert consumer._batch_count == 1

    async def test_consume_loop_increments_batch_count_only_when_data(self):
        consumer = _make_consumer()
        consumer._running = True

        tp = TopicPartition("t1", 0)
        mock_kafka = _make_kafka_mock(assignment={tp})
        call_count = 0

        async def getmany_side_effect(timeout_ms=1000):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                return {}
            consumer._running = False
            return {}

        mock_kafka.getmany.side_effect = getmany_side_effect
        consumer._consumer = mock_kafka

        await consumer._consume_loop()
        assert consumer._batch_count == 0


class TestMessageConsumerProcessMessage:
    async def test_successful_message_commits_offset(self):
        handler = AsyncMock()
        consumer = _make_consumer(message_handler=handler)
        mock_kafka = _make_kafka_mock(highwater=100)
        consumer._consumer = mock_kafka

        record = _make_consumer_record()
        await consumer._process_message(record)

        handler.assert_awaited_once()
        mock_kafka.commit.assert_awaited_once()

    async def test_successful_message_skips_commit_when_disabled(self):
        handler = AsyncMock()
        consumer = _make_consumer(message_handler=handler, enable_message_commit=False)
        mock_kafka = _make_kafka_mock(highwater=100)
        consumer._consumer = mock_kafka

        record = _make_consumer_record()
        await consumer._process_message(record)

        handler.assert_awaited_once()
        mock_kafka.commit.assert_not_awaited()

    async def test_handler_receives_pipeline_message(self):
        received = []

        async def handler(msg):
            received.append(msg)

        consumer = _make_consumer(message_handler=handler)
        mock_kafka = _make_kafka_mock(highwater=100)
        consumer._consumer = mock_kafka

        record = _make_consumer_record(key=b"my-key", value=b"my-value")
        await consumer._process_message(record)

        assert len(received) == 1
        assert isinstance(received[0], PipelineMessage)
        assert received[0].key == b"my-key"
        assert received[0].value == b"my-value"

    async def test_handler_error_routes_to_error_handler(self):
        handler = AsyncMock(side_effect=ValueError("bad data"))
        consumer = _make_consumer(message_handler=handler)
        consumer._consumer = _make_kafka_mock()

        with patch.object(consumer, "_handle_processing_error", new_callable=AsyncMock) as mock_err:
            record = _make_consumer_record()
            await consumer._process_message(record)

            mock_err.assert_awaited_once()
            args = mock_err.call_args[0]
            assert isinstance(args[0], PipelineMessage)
            assert isinstance(args[2], ValueError)


class TestMessageConsumerErrorHandling:
    async def test_permanent_error_routes_to_dlq(self):
        consumer = _make_consumer()
        consumer._consumer = _make_kafka_mock()

        record = _make_consumer_record()
        pipeline_msg = PipelineMessage(
            topic="t1",
            partition=0,
            offset=42,
            timestamp=1000,
            key=b"key-1",
            value=b'{"data": "hello"}',
        )

        with patch(
            "pipeline.common.consumer.TransportErrorClassifier.classify_consumer_error"
        ) as mock_classify:
            from core.types import ErrorCategory

            mock_classified = Mock()
            mock_classified.category = ErrorCategory.PERMANENT
            mock_classify.return_value = mock_classified

            await consumer._handle_processing_error(
                pipeline_msg, record, ValueError("permanent"), 0.1
            )

        consumer._dlq_producer.send.assert_awaited_once()

    async def test_permanent_error_commits_after_dlq(self):
        consumer = _make_consumer(enable_message_commit=True)
        mock_kafka = _make_kafka_mock()
        consumer._consumer = mock_kafka

        record = _make_consumer_record()
        pipeline_msg = PipelineMessage(
            topic="t1",
            partition=0,
            offset=42,
            timestamp=1000,
            key=b"key-1",
            value=b'{"data": "hello"}',
        )

        with patch(
            "pipeline.common.consumer.TransportErrorClassifier.classify_consumer_error"
        ) as mock_classify:
            from core.types import ErrorCategory

            mock_classified = Mock()
            mock_classified.category = ErrorCategory.PERMANENT
            mock_classify.return_value = mock_classified

            await consumer._handle_processing_error(pipeline_msg, record, ValueError("bad"), 0.1)

        mock_kafka.commit.assert_awaited_once()

    async def test_permanent_error_skips_commit_when_disabled(self):
        consumer = _make_consumer(enable_message_commit=False)
        mock_kafka = _make_kafka_mock()
        consumer._consumer = mock_kafka

        record = _make_consumer_record()
        pipeline_msg = PipelineMessage(
            topic="t1",
            partition=0,
            offset=42,
            timestamp=1000,
            key=b"key-1",
            value=b'{"data": "hello"}',
        )

        with patch(
            "pipeline.common.consumer.TransportErrorClassifier.classify_consumer_error"
        ) as mock_classify:
            from core.types import ErrorCategory

            mock_classified = Mock()
            mock_classified.category = ErrorCategory.PERMANENT
            mock_classify.return_value = mock_classified

            await consumer._handle_processing_error(pipeline_msg, record, ValueError("x"), 0.1)

        mock_kafka.commit.assert_not_awaited()

    async def test_transient_error_does_not_route_to_dlq(self):
        consumer = _make_consumer()
        consumer._consumer = _make_kafka_mock()

        record = _make_consumer_record()
        pipeline_msg = PipelineMessage(
            topic="t1",
            partition=0,
            offset=42,
            timestamp=1000,
            key=b"key-1",
            value=b'{"data": "hello"}',
        )

        with patch(
            "pipeline.common.consumer.TransportErrorClassifier.classify_consumer_error"
        ) as mock_classify:
            from core.types import ErrorCategory

            mock_classified = Mock()
            mock_classified.category = ErrorCategory.TRANSIENT
            mock_classify.return_value = mock_classified

            await consumer._handle_processing_error(
                pipeline_msg, record, TimeoutError("timeout"), 0.1
            )

        consumer._dlq_producer.send.assert_not_awaited()

    async def test_dlq_send_failure_does_not_raise(self):
        consumer = _make_consumer()
        consumer._dlq_producer.send = AsyncMock(side_effect=RuntimeError("DLQ down"))
        consumer._consumer = _make_kafka_mock()

        record = _make_consumer_record()
        pipeline_msg = PipelineMessage(
            topic="t1",
            partition=0,
            offset=42,
            timestamp=1000,
            key=b"key-1",
            value=b'{"data": "hello"}',
        )

        with patch(
            "pipeline.common.consumer.TransportErrorClassifier.classify_consumer_error"
        ) as mock_classify:
            from core.types import ErrorCategory

            mock_classified = Mock()
            mock_classified.category = ErrorCategory.PERMANENT
            mock_classify.return_value = mock_classified

            await consumer._handle_processing_error(pipeline_msg, record, ValueError("x"), 0.1)


class TestMessageConsumerPartitionMetrics:
    def test_update_partition_metrics_noop_when_no_consumer(self):
        consumer = _make_consumer()
        consumer._consumer = None

        record = _make_consumer_record()
        consumer._update_partition_metrics(record)

    def test_update_partition_metrics_with_highwater(self):
        consumer = _make_consumer()
        mock_kafka = _make_kafka_mock(highwater=100)
        consumer._consumer = mock_kafka

        record = _make_consumer_record(offset=50)
        with (
            patch("pipeline.common.consumer.update_consumer_offset") as mock_offset,
            patch("pipeline.common.consumer.update_consumer_lag") as mock_lag,
        ):
            consumer._update_partition_metrics(record)

            mock_offset.assert_called_once()
            mock_lag.assert_called_once_with("test-topic", 0, consumer.group_id, 49)

    def test_update_partition_metrics_handles_none_highwater(self):
        consumer = _make_consumer()
        mock_kafka = _make_kafka_mock(highwater=None)
        consumer._consumer = mock_kafka

        record = _make_consumer_record()
        with (
            patch("pipeline.common.consumer.update_consumer_offset"),
            patch("pipeline.common.consumer.update_consumer_lag") as mock_lag,
        ):
            consumer._update_partition_metrics(record)
            mock_lag.assert_not_called()


class TestMessageConsumerIsRunning:
    def test_is_running_true_when_active(self):
        consumer = _make_consumer()
        consumer._running = True
        consumer._consumer = Mock()
        assert consumer.is_running is True

    def test_is_running_false_when_no_consumer(self):
        consumer = _make_consumer()
        consumer._running = True
        consumer._consumer = None
        assert consumer.is_running is False

    def test_is_running_false_when_not_running(self):
        consumer = _make_consumer()
        consumer._running = False
        consumer._consumer = Mock()
        assert consumer.is_running is False
