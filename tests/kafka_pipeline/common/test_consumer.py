"""
Tests for Kafka consumer with circuit breaker integration.

These are unit tests that use mocks - no Docker/Kafka required.
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from aiokafka.structs import ConsumerRecord, TopicPartition

from core.resilience.circuit_breaker import CircuitBreaker, CircuitOpenError
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.common.consumer import BaseKafkaConsumer


@pytest.fixture
def kafka_config():
    """Create test Kafka configuration with required domain config."""
    return KafkaConfig(
        bootstrap_servers="localhost:9092",
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        # Consumer defaults (used by get_worker_config)
        consumer_defaults={
            "enable_auto_commit": False,
            "auto_offset_reset": "earliest",
            "max_poll_records": 100,
            "session_timeout_ms": 30000,
            "max_poll_interval_ms": 300000,
        },
        # Domain config required for get_worker_config and get_consumer_group
        xact={
            "topics": {
                "events": "xact.events.raw",
                "downloads_pending": "xact.downloads.pending",
            },
            "consumer_group_prefix": "xact",
            "test_worker": {
                "consumer": {},
                "processing": {},
            },
        },
    )


@pytest.fixture
def mock_circuit_breaker():
    """Create mock circuit breaker."""
    breaker = MagicMock(spec=CircuitBreaker)

    # Default behavior: call_async passes through and awaits the coroutine
    async def passthrough(coro):
        return await coro()

    breaker.call_async = AsyncMock(side_effect=passthrough)
    return breaker


@pytest.fixture
def mock_message_handler():
    """Create mock message handler."""
    return AsyncMock()


@pytest.fixture
def mock_aiokafka_consumer():
    """Create mock AIOKafkaConsumer."""
    consumer = MagicMock()
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()
    consumer.commit = AsyncMock()
    consumer.getmany = AsyncMock()
    consumer.assignment = MagicMock(return_value=[])
    return consumer


def create_consumer_record(
    topic: str = "test-topic",
    partition: int = 0,
    offset: int = 0,
    key: bytes = b"test-key",
    value: bytes = b'{"test": "value"}',
) -> ConsumerRecord:
    """Helper to create ConsumerRecord for testing."""
    return ConsumerRecord(
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp=1234567890,
        timestamp_type=0,
        key=key,
        value=value,
        headers=[],
        checksum=None,
        serialized_key_size=len(key) if key else 0,
        serialized_value_size=len(value) if value else 0,
    )


class TestBaseKafkaConsumerInit:
    """Tests for consumer initialization."""

    def test_init_with_config(
        self, kafka_config, mock_message_handler, mock_circuit_breaker
    ):
        """Consumer initializes with config and circuit breaker."""
        consumer = BaseKafkaConsumer(
            config=kafka_config,
            domain="xact",
            worker_name="test_worker",
            topics=["test-topic"],
            message_handler=mock_message_handler,
            circuit_breaker=mock_circuit_breaker,
        )

        assert consumer.config == kafka_config
        assert consumer.topics == ["test-topic"]
        assert consumer.domain == "xact"
        assert consumer.worker_name == "test_worker"
        # group_id is now derived from config.get_consumer_group()
        assert consumer.group_id == "xact-test_worker"
        assert consumer.message_handler == mock_message_handler
        assert consumer._circuit_breaker == mock_circuit_breaker
        assert consumer._consumer is None
        assert not consumer._running

    def test_init_creates_default_circuit_breaker(
        self, kafka_config, mock_message_handler
    ):
        """Consumer creates default circuit breaker if not provided."""
        with patch("kafka_pipeline.common.consumer.get_circuit_breaker") as mock_get_breaker:
            mock_breaker = MagicMock(spec=CircuitBreaker)
            mock_get_breaker.return_value = mock_breaker

            consumer = BaseKafkaConsumer(
                config=kafka_config,
                domain="xact",
                worker_name="test_worker",
                topics=["test-topic"],
                message_handler=mock_message_handler,
            )

            mock_get_breaker.assert_called_once()
            assert consumer._circuit_breaker == mock_breaker

    def test_init_with_multiple_topics(
        self, kafka_config, mock_message_handler, mock_circuit_breaker
    ):
        """Consumer can subscribe to multiple topics."""
        topics = ["topic-1", "topic-2", "topic-3"]
        consumer = BaseKafkaConsumer(
            config=kafka_config,
            domain="xact",
            worker_name="test_worker",
            topics=topics,
            message_handler=mock_message_handler,
            circuit_breaker=mock_circuit_breaker,
        )

        assert consumer.topics == topics

    def test_init_requires_topics(self, kafka_config, mock_message_handler):
        """Consumer requires at least one topic."""
        with pytest.raises(ValueError, match="At least one topic must be specified"):
            BaseKafkaConsumer(
                config=kafka_config,
                domain="xact",
                worker_name="test_worker",
                topics=[],
                message_handler=mock_message_handler,
            )


class TestBaseKafkaConsumerStartStop:
    """Tests for consumer start/stop lifecycle."""

    @pytest.mark.asyncio
    async def test_start_creates_consumer(
        self,
        kafka_config,
        mock_message_handler,
        mock_circuit_breaker,
        mock_aiokafka_consumer,
    ):
        """Consumer start creates and starts aiokafka consumer."""
        with patch(
            "kafka_pipeline.common.consumer.AIOKafkaConsumer", return_value=mock_aiokafka_consumer
        ):
            with patch("kafka_pipeline.common.consumer.create_kafka_oauth_callback") as mock_oauth:
                mock_oauth_callback = Mock()
                mock_oauth.return_value = mock_oauth_callback

                consumer = BaseKafkaConsumer(
                    config=kafka_config,
                    domain="xact",
                    worker_name="test_worker",
                    topics=["test-topic"],
                    message_handler=mock_message_handler,
                    circuit_breaker=mock_circuit_breaker,
                )

                # Mock the consume loop to exit immediately
                async def mock_consume_loop():
                    consumer._running = False

                with patch.object(consumer, "_consume_loop", side_effect=mock_consume_loop):
                    await consumer.start()

                # OAuth callback created
                mock_oauth.assert_called_once()

                # Consumer created with correct config
                mock_aiokafka_consumer.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_idempotent(
        self,
        kafka_config,
        mock_message_handler,
        mock_circuit_breaker,
        mock_aiokafka_consumer,
    ):
        """Starting consumer multiple times is safe."""
        consumer = BaseKafkaConsumer(
            config=kafka_config,
            domain="xact",
            worker_name="test_worker",
            topics=["test-topic"],
            message_handler=mock_message_handler,
            circuit_breaker=mock_circuit_breaker,
        )
        consumer._running = True
        consumer._consumer = mock_aiokafka_consumer

        await consumer.start()

        # Consumer not started again
        mock_aiokafka_consumer.start.assert_not_called()

    @pytest.mark.asyncio
    async def test_stop_success(
        self,
        kafka_config,
        mock_message_handler,
        mock_circuit_breaker,
        mock_aiokafka_consumer,
    ):
        """Consumer stops successfully and commits offsets."""
        consumer = BaseKafkaConsumer(
            config=kafka_config,
            domain="xact",
            worker_name="test_worker",
            topics=["test-topic"],
            message_handler=mock_message_handler,
            circuit_breaker=mock_circuit_breaker,
        )
        consumer._running = True
        consumer._consumer = mock_aiokafka_consumer

        await consumer.stop()

        # Consumer committed and stopped
        mock_aiokafka_consumer.commit.assert_called_once()
        mock_aiokafka_consumer.stop.assert_called_once()

        assert not consumer._running
        assert consumer._consumer is None

    @pytest.mark.asyncio
    async def test_stop_idempotent(
        self, kafka_config, mock_message_handler, mock_circuit_breaker
    ):
        """Stopping consumer multiple times is safe."""
        consumer = BaseKafkaConsumer(
            config=kafka_config,
            domain="xact",
            worker_name="test_worker",
            topics=["test-topic"],
            message_handler=mock_message_handler,
            circuit_breaker=mock_circuit_breaker,
        )

        # Stop without starting
        await consumer.stop()  # Should not raise

        # Already stopped
        consumer._running = False
        consumer._consumer = None
        await consumer.stop()  # Should not raise


class TestBaseKafkaConsumerMessageProcessing:
    """Tests for message processing."""

    @pytest.mark.asyncio
    async def test_process_message_success(
        self,
        kafka_config,
        mock_message_handler,
        mock_circuit_breaker,
        mock_aiokafka_consumer,
    ):
        """Message handler called and offset committed on success."""
        consumer = BaseKafkaConsumer(
            config=kafka_config,
            domain="xact",
            worker_name="test_worker",
            topics=["test-topic"],
            message_handler=mock_message_handler,
            circuit_breaker=mock_circuit_breaker,
        )
        consumer._consumer = mock_aiokafka_consumer

        # Create test message
        message = create_consumer_record()

        # Process message
        await consumer._process_message(message)

        # Handler called with message
        mock_message_handler.assert_called_once_with(message)

        # Offset committed after successful processing
        mock_aiokafka_consumer.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_message_handler_error_no_commit(
        self,
        kafka_config,
        mock_message_handler,
        mock_circuit_breaker,
        mock_aiokafka_consumer,
    ):
        """Offset not committed when handler raises error."""
        consumer = BaseKafkaConsumer(
            config=kafka_config,
            domain="xact",
            worker_name="test_worker",
            topics=["test-topic"],
            message_handler=mock_message_handler,
            circuit_breaker=mock_circuit_breaker,
        )
        consumer._consumer = mock_aiokafka_consumer

        # Handler raises error
        mock_message_handler.side_effect = ValueError("Processing failed")

        message = create_consumer_record()

        # Processing should NOT raise (error handling added in WP-207)
        await consumer._process_message(message)

        # Handler was called
        mock_message_handler.assert_called_once_with(message)

        # Offset NOT committed
        mock_aiokafka_consumer.commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_consume_loop_processes_messages(
        self,
        kafka_config,
        mock_message_handler,
        mock_circuit_breaker,
        mock_aiokafka_consumer,
    ):
        """Consume loop fetches and processes messages."""
        consumer = BaseKafkaConsumer(
            config=kafka_config,
            domain="xact",
            worker_name="test_worker",
            topics=["test-topic"],
            message_handler=mock_message_handler,
            circuit_breaker=mock_circuit_breaker,
        )
        consumer._consumer = mock_aiokafka_consumer
        consumer._running = True

        # Create test messages
        tp = TopicPartition("test-topic", 0)
        messages = [
            create_consumer_record(offset=0),
            create_consumer_record(offset=1),
            create_consumer_record(offset=2),
        ]

        # Mock getmany to return messages once, then empty
        # Also mock assignment() to return a partition so consume_loop doesn't skip
        mock_aiokafka_consumer.assignment = MagicMock(return_value=[tp])
        call_count = 0

        async def mock_getmany(timeout_ms):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {tp: messages}
            # Stop the loop after first batch
            consumer._running = False
            return {}

        mock_aiokafka_consumer.getmany = AsyncMock(side_effect=mock_getmany)

        # Run consume loop
        await consumer._consume_loop()

        # Handler called for each message
        assert mock_message_handler.call_count == 3

        # Offset committed for each message
        assert mock_aiokafka_consumer.commit.call_count == 3

    @pytest.mark.asyncio
    async def test_consume_loop_circuit_breaker_records_success(
        self,
        kafka_config,
        mock_message_handler,
        mock_circuit_breaker,
        mock_aiokafka_consumer,
    ):
        """Consume loop records success/failure with circuit breaker."""
        # Note: The current implementation calls getmany() directly (not through
        # circuit_breaker.call_async) but records success/failure manually
        consumer = BaseKafkaConsumer(
            config=kafka_config,
            domain="xact",
            worker_name="test_worker",
            topics=["test-topic"],
            message_handler=mock_message_handler,
            circuit_breaker=mock_circuit_breaker,
        )
        consumer._consumer = mock_aiokafka_consumer
        consumer._running = True

        # Mock partition assignment and empty response, stop after first call
        tp = TopicPartition("test-topic", 0)
        mock_aiokafka_consumer.assignment = MagicMock(return_value=[tp])

        async def mock_getmany(timeout_ms):
            consumer._running = False
            return {}

        mock_aiokafka_consumer.getmany = AsyncMock(side_effect=mock_getmany)

        # Run consume loop
        await consumer._consume_loop()

        # Circuit breaker record_success was called after successful fetch
        mock_circuit_breaker.record_success.assert_called()

    @pytest.mark.asyncio
    async def test_consume_loop_handles_cancellation(
        self,
        kafka_config,
        mock_message_handler,
        mock_circuit_breaker,
        mock_aiokafka_consumer,
    ):
        """Consume loop handles asyncio cancellation gracefully."""
        consumer = BaseKafkaConsumer(
            config=kafka_config,
            domain="xact",
            worker_name="test_worker",
            topics=["test-topic"],
            message_handler=mock_message_handler,
            circuit_breaker=mock_circuit_breaker,
        )
        consumer._consumer = mock_aiokafka_consumer
        consumer._running = True

        # Mock partition assignment
        tp = TopicPartition("test-topic", 0)
        mock_aiokafka_consumer.assignment = MagicMock(return_value=[tp])

        # Mock getmany to raise CancelledError
        mock_aiokafka_consumer.getmany = AsyncMock(side_effect=asyncio.CancelledError())

        # Cancellation should propagate
        with pytest.raises(asyncio.CancelledError):
            await consumer._consume_loop()

    @pytest.mark.asyncio
    async def test_consume_loop_continues_on_error(
        self,
        kafka_config,
        mock_message_handler,
        mock_circuit_breaker,
        mock_aiokafka_consumer,
    ):
        """Consume loop continues processing after errors."""
        consumer = BaseKafkaConsumer(
            config=kafka_config,
            domain="xact",
            worker_name="test_worker",
            topics=["test-topic"],
            message_handler=mock_message_handler,
            circuit_breaker=mock_circuit_breaker,
        )
        consumer._consumer = mock_aiokafka_consumer
        consumer._running = True

        # Mock partition assignment
        tp = TopicPartition("test-topic", 0)
        mock_aiokafka_consumer.assignment = MagicMock(return_value=[tp])

        call_count = 0

        async def mock_getmany(timeout_ms):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("Temporary error")
            # Stop after second call
            consumer._running = False
            return {}

        mock_aiokafka_consumer.getmany = AsyncMock(side_effect=mock_getmany)

        # Should not raise, continues after error
        await consumer._consume_loop()

        # Called twice (error + successful)
        assert call_count == 2


class TestBaseKafkaConsumerUtilities:
    """Tests for utility methods."""

    def test_is_running_property(
        self, kafka_config, mock_message_handler, mock_circuit_breaker
    ):
        """is_running property reflects consumer state."""
        consumer = BaseKafkaConsumer(
            config=kafka_config,
            domain="xact",
            worker_name="test_worker",
            topics=["test-topic"],
            message_handler=mock_message_handler,
            circuit_breaker=mock_circuit_breaker,
        )

        # Not running initially
        assert not consumer.is_running

        # Running when both flags set
        consumer._running = True
        consumer._consumer = MagicMock()
        assert consumer.is_running

        # Not running if only _running set
        consumer._consumer = None
        assert not consumer.is_running

        # Not running if only _consumer set
        consumer._running = False
        consumer._consumer = MagicMock()
        assert not consumer.is_running
