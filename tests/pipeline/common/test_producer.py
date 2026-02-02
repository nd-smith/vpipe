"""
Tests for Kafka producer with circuit breaker integration.

These are unit tests that use mocks - no Docker/Kafka required.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from aiokafka.structs import RecordMetadata
from pydantic import BaseModel

from core.resilience.circuit_breaker import CircuitBreaker, CircuitOpenError
from pipeline.config import KafkaConfig
from pipeline.common.producer import BaseKafkaProducer


class SampleMessage(BaseModel):
    """Sample message schema for testing."""

    id: str
    content: str


@pytest.fixture
def kafka_config():
    """Create test Kafka configuration with required domain config."""
    return KafkaConfig(
        bootstrap_servers="localhost:9092",
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        # Producer defaults (used by get_worker_config)
        producer_defaults={
            "acks": "all",
            "retries": 3,
            "retry_backoff_ms": 1000,
        },
        # Domain config required for get_worker_config
        verisk={
            "topics": {
                "events": "xact.events.raw",
                "downloads_pending": "xact.downloads.pending",
            },
            "consumer_group_prefix": "xact",
            "test_worker": {
                "consumer": {},
                "producer": {},
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
def mock_aiokafka_producer():
    """Create mock AIOKafkaProducer."""
    producer = MagicMock()
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    producer.flush = AsyncMock()
    producer.send_and_wait = AsyncMock()
    producer.send = AsyncMock()
    return producer


@pytest.fixture
def producer(kafka_config):
    """Create producer with mocked dependencies."""
    return BaseKafkaProducer(
        kafka_config,
        domain="verisk",
        worker_name="test_worker",
    )


class TestBaseKafkaProducerInit:
    """Tests for producer initialization."""

    def test_init_with_config(self, kafka_config, mock_circuit_breaker):
        """Producer initializes with config and circuit breaker."""
        producer = BaseKafkaProducer(
            kafka_config,
            domain="verisk",
            worker_name="test_worker",
            circuit_breaker=mock_circuit_breaker,
        )

        assert producer.config == kafka_config
        assert producer.domain == "xact"
        assert producer.worker_name == "test_worker"
        assert producer._circuit_breaker == mock_circuit_breaker
        assert producer._producer is None
        assert not producer._started

    def test_init_creates_default_circuit_breaker(self, kafka_config):
        """Producer creates default circuit breaker if not provided."""
        with patch("pipeline.common.producer.get_circuit_breaker") as mock_get_breaker:
            mock_breaker = MagicMock(spec=CircuitBreaker)
            mock_get_breaker.return_value = mock_breaker

            producer = BaseKafkaProducer(
                kafka_config,
                domain="verisk",
                worker_name="test_worker",
            )

            mock_get_breaker.assert_called_once()
            assert producer._circuit_breaker == mock_breaker


class TestBaseKafkaProducerStartStop:
    """Tests for producer start/stop lifecycle."""

    @pytest.mark.asyncio
    async def test_start_success(self, producer, mock_aiokafka_producer):
        """Producer starts successfully with OAuth authentication."""
        with patch("pipeline.common.producer.AIOKafkaProducer", return_value=mock_aiokafka_producer):
            with patch("pipeline.common.producer.create_kafka_oauth_callback") as mock_oauth:
                mock_oauth_callback = Mock()
                mock_oauth.return_value = mock_oauth_callback

                await producer.start()

                # OAuth callback created
                mock_oauth.assert_called_once()

                # Producer created with correct config
                mock_aiokafka_producer.start.assert_called_once()

                assert producer._started
                assert producer._producer == mock_aiokafka_producer

    @pytest.mark.asyncio
    async def test_start_idempotent(self, producer, mock_aiokafka_producer):
        """Starting producer multiple times is safe."""
        with patch("pipeline.common.producer.AIOKafkaProducer", return_value=mock_aiokafka_producer):
            with patch("pipeline.common.producer.create_kafka_oauth_callback"):
                await producer.start()
                await producer.start()  # Second call

                # Producer started only once
                assert mock_aiokafka_producer.start.call_count == 1

    @pytest.mark.asyncio
    async def test_stop_success(self, producer, mock_aiokafka_producer):
        """Producer stops successfully and flushes messages."""
        with patch("pipeline.common.producer.AIOKafkaProducer", return_value=mock_aiokafka_producer):
            with patch("pipeline.common.producer.create_kafka_oauth_callback"):
                await producer.start()
                await producer.stop()

                # Producer flushed and stopped
                mock_aiokafka_producer.flush.assert_called_once()
                mock_aiokafka_producer.stop.assert_called_once()

                assert not producer._started
                assert producer._producer is None

    @pytest.mark.asyncio
    async def test_stop_idempotent(self, producer):
        """Stopping producer multiple times is safe."""
        # Stop without starting
        await producer.stop()  # Should not raise

        # Start and stop twice
        with patch("pipeline.common.producer.AIOKafkaProducer") as mock_producer_class:
            mock_aiokafka = MagicMock()
            mock_aiokafka.start = AsyncMock()
            mock_aiokafka.stop = AsyncMock()
            mock_aiokafka.flush = AsyncMock()
            mock_producer_class.return_value = mock_aiokafka

            with patch("pipeline.common.producer.create_kafka_oauth_callback"):
                await producer.start()
                await producer.stop()
                await producer.stop()  # Second stop

                # Stop called only once
                assert mock_aiokafka.stop.call_count == 1


class TestBaseKafkaProducerSend:
    """Tests for single message send."""

    @pytest.mark.asyncio
    async def test_send_success(self, producer, mock_aiokafka_producer):
        """Send message successfully returns ProduceResult (not RecordMetadata)."""
        from pipeline.common.types import ProduceResult

        with patch("pipeline.common.producer.AIOKafkaProducer", return_value=mock_aiokafka_producer):
            with patch("pipeline.common.producer.create_kafka_oauth_callback"):
                await producer.start()

                # Setup mock RecordMetadata from aiokafka
                record_metadata = RecordMetadata(
                    topic="test-topic",
                    partition=0,
                    topic_partition=None,
                    offset=123,
                    timestamp=1234567890,
                    timestamp_type=0,
                    log_start_offset=0,
                )
                mock_aiokafka_producer.send_and_wait.return_value = record_metadata

                # Send message
                message = SampleMessage(id="msg-1", content="test content")
                result = await producer.send(
                    topic="test-topic",
                    key="key-1",
                    value=message,
                    headers={"trace_id": "evt-123"},
                )

                # Verify send called correctly
                mock_aiokafka_producer.send_and_wait.assert_called_once()
                call_args = mock_aiokafka_producer.send_and_wait.call_args
                assert call_args[0][0] == "test-topic"
                assert call_args[1]["key"] == b"key-1"
                assert b'"id":"msg-1"' in call_args[1]["value"]
                assert call_args[1]["headers"] == [("trace_id", b"evt-123")]

                # Verify ProduceResult returned (not RecordMetadata)
                assert isinstance(result, ProduceResult)
                assert result.topic == "test-topic"
                assert result.partition == 0
                assert result.offset == 123

    @pytest.mark.asyncio
    async def test_send_without_headers(self, producer, mock_aiokafka_producer):
        """Send message without headers."""
        with patch("pipeline.common.producer.AIOKafkaProducer", return_value=mock_aiokafka_producer):
            with patch("pipeline.common.producer.create_kafka_oauth_callback"):
                await producer.start()

                metadata = RecordMetadata(
                    topic="test-topic",
                    partition=0,
                    topic_partition=None,
                    offset=123,
                    timestamp=1234567890,
                    timestamp_type=0,
                    log_start_offset=0,
                )
                mock_aiokafka_producer.send_and_wait.return_value = metadata

                message = SampleMessage(id="msg-1", content="test")
                await producer.send(topic="test-topic", key="key-1", value=message)

                # Verify no headers passed
                call_args = mock_aiokafka_producer.send_and_wait.call_args
                assert call_args[1]["headers"] is None

    @pytest.mark.asyncio
    async def test_send_not_started(self, producer):
        """Send raises error if producer not started."""
        message = SampleMessage(id="msg-1", content="test")

        with pytest.raises(RuntimeError, match="Producer not started"):
            await producer.send(topic="test-topic", key="key-1", value=message)

    @pytest.mark.asyncio
    async def test_send_with_none_key(self, producer, mock_aiokafka_producer):
        """Send message with None key (should be allowed)."""
        from pipeline.common.types import ProduceResult

        with patch("pipeline.common.producer.AIOKafkaProducer", return_value=mock_aiokafka_producer):
            with patch("pipeline.common.producer.create_kafka_oauth_callback"):
                await producer.start()

                record_metadata = RecordMetadata(
                    topic="test-topic",
                    partition=0,
                    topic_partition=None,
                    offset=123,
                    timestamp=1234567890,
                    timestamp_type=0,
                    log_start_offset=0,
                )
                mock_aiokafka_producer.send_and_wait.return_value = record_metadata

                message = SampleMessage(id="msg-1", content="test")
                result = await producer.send(topic="test-topic", key=None, value=message)

                # Verify result
                assert isinstance(result, ProduceResult)
                assert result.topic == "test-topic"


class TestBaseKafkaProducerSendBatch:
    """Tests for batch message send."""

    @pytest.mark.asyncio
    async def test_send_batch_success(self, producer, mock_aiokafka_producer):
        """Send batch of messages successfully returns List[ProduceResult]."""
        from pipeline.common.types import ProduceResult

        with patch("pipeline.common.producer.AIOKafkaProducer", return_value=mock_aiokafka_producer):
            with patch("pipeline.common.producer.create_kafka_oauth_callback"):
                await producer.start()

                # Setup mock RecordMetadata from aiokafka
                metadata_list = []
                for i in range(3):
                    metadata = RecordMetadata(
                        topic="test-topic",
                        partition=i % 2,
                        topic_partition=None,
                        offset=100 + i,
                        timestamp=1234567890,
                        timestamp_type=0,
                        log_start_offset=0,
                    )
                    metadata_list.append(metadata)

                # Mock send to return futures that resolve to metadata
                # We need to create actual coroutine objects that can be awaited
                async def create_future(meta):
                    return meta

                futures = [create_future(meta) for meta in metadata_list]
                mock_aiokafka_producer.send.side_effect = futures

                # Create batch
                messages = [
                    ("key-1", SampleMessage(id="msg-1", content="content 1")),
                    ("key-2", SampleMessage(id="msg-2", content="content 2")),
                    ("key-3", SampleMessage(id="msg-3", content="content 3")),
                ]

                # Send batch
                results = await producer.send_batch(
                    topic="test-topic",
                    messages=messages,
                    headers={"batch_id": "batch-123"},
                )

                # Verify send called for each message
                assert mock_aiokafka_producer.send.call_count == 3

                # Verify results are ProduceResult (not RecordMetadata)
                assert len(results) == 3
                for i, result in enumerate(results):
                    assert isinstance(result, ProduceResult)
                    assert result.topic == "test-topic"
                    assert result.partition == i % 2
                    assert result.offset == 100 + i

    @pytest.mark.asyncio
    async def test_send_batch_empty(self, producer, mock_aiokafka_producer):
        """Send empty batch returns empty list."""
        with patch("pipeline.common.producer.AIOKafkaProducer", return_value=mock_aiokafka_producer):
            with patch("pipeline.common.producer.create_kafka_oauth_callback"):
                await producer.start()

                results = await producer.send_batch(topic="test-topic", messages=[])

                assert results == []
                mock_aiokafka_producer.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_send_batch_not_started(self, producer):
        """Send batch raises error if producer not started."""
        messages = [("key-1", SampleMessage(id="msg-1", content="test"))]

        with pytest.raises(RuntimeError, match="Producer not started"):
            await producer.send_batch(topic="test-topic", messages=messages)


class TestProduceResultConversion:
    """Tests for RecordMetadata to ProduceResult conversion."""

    @pytest.mark.asyncio
    async def test_record_metadata_to_produce_result_fields(self, producer, mock_aiokafka_producer):
        """Test all fields are correctly converted from RecordMetadata to ProduceResult."""
        from pipeline.common.types import ProduceResult

        with patch("pipeline.common.producer.AIOKafkaProducer", return_value=mock_aiokafka_producer):
            with patch("pipeline.common.producer.create_kafka_oauth_callback"):
                await producer.start()

                # Create RecordMetadata with specific values
                record_metadata = RecordMetadata(
                    topic="events.processed",
                    partition=7,
                    topic_partition=None,
                    offset=999999,
                    timestamp=1609459200000,
                    timestamp_type=0,
                    log_start_offset=0,
                )
                mock_aiokafka_producer.send_and_wait.return_value = record_metadata

                # Send message
                message = SampleMessage(id="test", content="data")
                result = await producer.send(topic="events.processed", key="key", value=message)

                # Verify all fields converted correctly
                assert isinstance(result, ProduceResult)
                assert result.topic == "events.processed"
                assert result.partition == 7
                assert result.offset == 999999

    @pytest.mark.asyncio
    async def test_produce_result_different_partitions(self, producer, mock_aiokafka_producer):
        """Test ProduceResult correctly reports different partitions."""
        from pipeline.common.types import ProduceResult

        with patch("pipeline.common.producer.AIOKafkaProducer", return_value=mock_aiokafka_producer):
            with patch("pipeline.common.producer.create_kafka_oauth_callback"):
                await producer.start()

                # Test different partition numbers
                for partition_num in [0, 1, 5, 10, 100]:
                    record_metadata = RecordMetadata(
                        topic="test-topic",
                        partition=partition_num,
                        topic_partition=None,
                        offset=1000,
                        timestamp=1234567890,
                        timestamp_type=0,
                        log_start_offset=0,
                    )
                    mock_aiokafka_producer.send_and_wait.return_value = record_metadata

                    message = SampleMessage(id="test", content="data")
                    result = await producer.send(topic="test-topic", key="key", value=message)

                    assert result.partition == partition_num

    @pytest.mark.asyncio
    async def test_produce_result_sequential_offsets(self, producer, mock_aiokafka_producer):
        """Test ProduceResult correctly reports sequential offsets."""
        from pipeline.common.types import ProduceResult

        with patch("pipeline.common.producer.AIOKafkaProducer", return_value=mock_aiokafka_producer):
            with patch("pipeline.common.producer.create_kafka_oauth_callback"):
                await producer.start()

                # Test sequential offsets
                for offset in range(100, 110):
                    record_metadata = RecordMetadata(
                        topic="test-topic",
                        partition=0,
                        topic_partition=None,
                        offset=offset,
                        timestamp=1234567890,
                        timestamp_type=0,
                        log_start_offset=0,
                    )
                    mock_aiokafka_producer.send_and_wait.return_value = record_metadata

                    message = SampleMessage(id="test", content="data")
                    result = await producer.send(topic="test-topic", key="key", value=message)

                    assert result.offset == offset

    @pytest.mark.asyncio
    async def test_batch_returns_list_of_produce_results(self, producer, mock_aiokafka_producer):
        """Test send_batch returns List[ProduceResult] with correct field mapping."""
        from pipeline.common.types import ProduceResult

        with patch("pipeline.common.producer.AIOKafkaProducer", return_value=mock_aiokafka_producer):
            with patch("pipeline.common.producer.create_kafka_oauth_callback"):
                await producer.start()

                # Create multiple RecordMetadata instances
                metadata_list = [
                    RecordMetadata(
                        topic="test-topic",
                        partition=0,
                        topic_partition=None,
                        offset=100,
                        timestamp=1234567890,
                        timestamp_type=0,
                        log_start_offset=0,
                    ),
                    RecordMetadata(
                        topic="test-topic",
                        partition=1,
                        topic_partition=None,
                        offset=200,
                        timestamp=1234567890,
                        timestamp_type=0,
                        log_start_offset=0,
                    ),
                ]

                async def create_future(meta):
                    return meta

                futures = [create_future(meta) for meta in metadata_list]
                mock_aiokafka_producer.send.side_effect = futures

                # Send batch
                messages = [
                    ("key-1", SampleMessage(id="msg-1", content="test")),
                    ("key-2", SampleMessage(id="msg-2", content="test")),
                ]
                results = await producer.send_batch(topic="test-topic", messages=messages)

                # Verify each result is ProduceResult with correct fields
                assert len(results) == 2
                assert results[0].topic == "test-topic"
                assert results[0].partition == 0
                assert results[0].offset == 100
                assert results[1].topic == "test-topic"
                assert results[1].partition == 1
                assert results[1].offset == 200


class TestBaseKafkaProducerUtilities:
    """Tests for utility methods."""

    @pytest.mark.asyncio
    async def test_flush(self, producer, mock_aiokafka_producer):
        """Flush calls underlying producer flush."""
        with patch("pipeline.common.producer.AIOKafkaProducer", return_value=mock_aiokafka_producer):
            with patch("pipeline.common.producer.create_kafka_oauth_callback"):
                await producer.start()
                await producer.flush()

                mock_aiokafka_producer.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_flush_not_started(self, producer):
        """Flush raises error if producer not started."""
        with pytest.raises(RuntimeError, match="Producer not started"):
            await producer.flush()

    def test_is_started_property(self, producer, mock_aiokafka_producer):
        """is_started property reflects producer state."""
        assert not producer.is_started

        with patch("pipeline.common.producer.AIOKafkaProducer", return_value=mock_aiokafka_producer):
            with patch("pipeline.common.producer.create_kafka_oauth_callback"):
                import asyncio

                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(producer.start())
                    assert producer.is_started

                    loop.run_until_complete(producer.stop())
                    assert not producer.is_started
                finally:
                    loop.close()
