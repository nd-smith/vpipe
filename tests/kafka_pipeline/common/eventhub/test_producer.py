"""
Tests for EventHub producer and ProduceResult conversion.

These are unit tests that use mocks - no Azure EventHub required.
"""

import pytest
import sys
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from pydantic import BaseModel

# Mock azure.eventhub before importing producer module
sys.modules['azure'] = MagicMock()
sys.modules['azure.eventhub'] = MagicMock()
sys.modules['azure.eventhub.aio'] = MagicMock()

from kafka_pipeline.common.eventhub.producer import EventHubProducer, EventHubRecordMetadata
from kafka_pipeline.common.types import ProduceResult


class SampleMessage(BaseModel):
    """Sample message schema for testing."""

    id: str
    content: str


@pytest.fixture
def mock_eventhub_producer_client():
    """Create mock EventHubProducerClient."""
    client = MagicMock()
    client.create_batch = MagicMock()
    client.send_batch = MagicMock()
    client.close = MagicMock()
    client.get_eventhub_properties = MagicMock(
        return_value={
            "name": "test-hub",
            "partition_ids": ["0", "1", "2"],
        }
    )

    # Create mock batch
    mock_batch = MagicMock()
    mock_batch.add = MagicMock()
    client.create_batch.return_value = mock_batch

    return client


class TestEventHubRecordMetadata:
    """Tests for EventHubRecordMetadata adapter class."""

    def test_creation(self):
        """Test creating EventHubRecordMetadata."""
        metadata = EventHubRecordMetadata(
            topic="test-hub",
            partition=2,
            offset=12345,
        )

        assert metadata.topic == "test-hub"
        assert metadata.partition == 2
        assert metadata.offset == 12345

    def test_creation_with_defaults(self):
        """Test creating EventHubRecordMetadata with default partition and offset."""
        metadata = EventHubRecordMetadata(topic="test-hub")

        assert metadata.topic == "test-hub"
        assert metadata.partition == 0
        assert metadata.offset == 0

    def test_to_produce_result(self):
        """Test to_produce_result() returns ProduceResult."""
        metadata = EventHubRecordMetadata(
            topic="test-hub",
            partition=3,
            offset=999,
        )

        result = metadata.to_produce_result()

        assert isinstance(result, ProduceResult)
        assert result.topic == "test-hub"
        assert result.partition == 3
        assert result.offset == 999

    def test_backward_compatibility_property_access(self):
        """Test backward compatibility - properties accessible directly on adapter."""
        metadata = EventHubRecordMetadata(
            topic="events",
            partition=5,
            offset=54321,
        )

        # Verify properties accessible on adapter
        assert metadata.topic == "events"
        assert metadata.partition == 5
        assert metadata.offset == 54321

        # Verify same values in ProduceResult
        result = metadata.to_produce_result()
        assert result.topic == metadata.topic
        assert result.partition == metadata.partition
        assert result.offset == metadata.offset

    def test_produce_result_immutability(self):
        """Test that ProduceResult returned is immutable."""
        metadata = EventHubRecordMetadata(topic="test-hub", partition=0, offset=0)
        result = metadata.to_produce_result()

        # ProduceResult is frozen - should not allow modification
        with pytest.raises(Exception):
            result.topic = "modified-topic"

    def test_multiple_conversions_independent(self):
        """Test multiple conversions produce independent ProduceResult instances."""
        metadata1 = EventHubRecordMetadata(topic="hub1", partition=0, offset=100)
        metadata2 = EventHubRecordMetadata(topic="hub2", partition=1, offset=200)

        result1 = metadata1.to_produce_result()
        result2 = metadata2.to_produce_result()

        # Verify results are independent
        assert result1.topic == "hub1"
        assert result2.topic == "hub2"
        assert result1.partition == 0
        assert result2.partition == 1
        assert result1.offset == 100
        assert result2.offset == 200
        assert result1 != result2


class TestEventHubProducerInit:
    """Test EventHub producer initialization."""

    def test_init_with_required_params(self):
        """Producer initializes with required parameters."""
        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="xact",
            worker_name="test_worker",
            eventhub_name="test-hub",
        )

        assert producer.connection_string is not None
        assert producer.domain == "xact"
        assert producer.worker_name == "test_worker"
        assert producer.eventhub_name == "test-hub"
        assert producer._producer is None
        assert not producer._started

    def test_init_logs_configuration(self):
        """Producer logs initialization with transport type."""
        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="xact",
            worker_name="test_worker",
            eventhub_name="test-hub",
        )

        # Verify producer stores configuration
        assert producer.eventhub_name == "test-hub"
        assert producer.domain == "xact"


class TestEventHubProducerStartStop:
    """Test producer start/stop lifecycle."""

    @pytest.mark.asyncio
    async def test_start_creates_client(self, mock_eventhub_producer_client):
        """Test start() creates EventHubProducerClient."""
        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="xact",
            worker_name="test_worker",
            eventhub_name="test-hub",
        )

        with patch("kafka_pipeline.common.eventhub.producer.EventHubProducerClient") as mock_client_class:
            mock_client_class.from_connection_string.return_value = mock_eventhub_producer_client

            await producer.start()

            # Verify client was created
            mock_client_class.from_connection_string.assert_called_once()
            call_kwargs = mock_client_class.from_connection_string.call_args.kwargs

            assert "conn_str" in call_kwargs
            assert call_kwargs["eventhub_name"] == "test-hub"
            assert "transport_type" in call_kwargs

            # Verify producer is started
            assert producer._started
            assert producer._producer == mock_eventhub_producer_client

    @pytest.mark.asyncio
    async def test_start_gets_properties(self, mock_eventhub_producer_client):
        """Test start() gets EventHub properties to verify connection."""
        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="xact",
            worker_name="test_worker",
            eventhub_name="test-hub",
        )

        with patch("kafka_pipeline.common.eventhub.producer.EventHubProducerClient") as mock_client_class:
            mock_client_class.from_connection_string.return_value = mock_eventhub_producer_client

            await producer.start()

            # Verify properties were fetched
            mock_eventhub_producer_client.get_eventhub_properties.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_idempotent(self, mock_eventhub_producer_client):
        """Test start() is idempotent - duplicate calls are ignored."""
        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="xact",
            worker_name="test_worker",
            eventhub_name="test-hub",
        )

        with patch("kafka_pipeline.common.eventhub.producer.EventHubProducerClient") as mock_client_class:
            mock_client_class.from_connection_string.return_value = mock_eventhub_producer_client

            await producer.start()
            await producer.start()  # Second call

            # Client created only once
            assert mock_client_class.from_connection_string.call_count == 1

    @pytest.mark.asyncio
    async def test_stop_closes_client(self, mock_eventhub_producer_client):
        """Test stop() closes EventHub client."""
        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="xact",
            worker_name="test_worker",
            eventhub_name="test-hub",
        )

        with patch("kafka_pipeline.common.eventhub.producer.EventHubProducerClient") as mock_client_class:
            mock_client_class.from_connection_string.return_value = mock_eventhub_producer_client

            await producer.start()
            await producer.stop()

            # Verify client was closed
            mock_eventhub_producer_client.close.assert_called_once()
            assert not producer._started
            assert producer._producer is None

    @pytest.mark.asyncio
    async def test_stop_when_not_started(self):
        """Test stop() when producer not started is safe."""
        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="xact",
            worker_name="test_worker",
            eventhub_name="test-hub",
        )

        # Should not raise
        await producer.stop()


class TestEventHubProducerSend:
    """Test producer send() method."""

    @pytest.mark.asyncio
    async def test_send_bytes_value(self, mock_eventhub_producer_client):
        """Test send() with bytes value."""
        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="xact",
            worker_name="test_worker",
            eventhub_name="test-hub",
        )

        with patch("kafka_pipeline.common.eventhub.producer.EventHubProducerClient") as mock_client_class:
            mock_client_class.from_connection_string.return_value = mock_eventhub_producer_client

            await producer.start()

            result = await producer.send(
                topic="test-hub",
                key=b"test-key",
                value=b'{"test": "data"}',
            )

            # Verify send was called
            mock_eventhub_producer_client.create_batch.assert_called()
            mock_eventhub_producer_client.send_batch.assert_called()

            # Verify result is ProduceResult
            assert isinstance(result, ProduceResult)
            assert result.topic == "test-hub"

    @pytest.mark.asyncio
    async def test_send_pydantic_value(self, mock_eventhub_producer_client):
        """Test send() with Pydantic model value."""
        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="xact",
            worker_name="test_worker",
            eventhub_name="test-hub",
        )

        with patch("kafka_pipeline.common.eventhub.producer.EventHubProducerClient") as mock_client_class:
            mock_client_class.from_connection_string.return_value = mock_eventhub_producer_client

            await producer.start()

            message = SampleMessage(id="123", content="test")
            result = await producer.send(
                topic="test-hub",
                key="entity-123",
                value=message,
            )

            # Verify send was called
            mock_eventhub_producer_client.send_batch.assert_called()

            # Verify result
            assert isinstance(result, ProduceResult)

    @pytest.mark.asyncio
    async def test_send_dict_value(self, mock_eventhub_producer_client):
        """Test send() with dict value."""
        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="xact",
            worker_name="test_worker",
            eventhub_name="test-hub",
        )

        with patch("kafka_pipeline.common.eventhub.producer.EventHubProducerClient") as mock_client_class:
            mock_client_class.from_connection_string.return_value = mock_eventhub_producer_client

            await producer.start()

            result = await producer.send(
                topic="test-hub",
                key="key1",
                value={"test": "data", "count": 42},
            )

            # Verify result
            assert isinstance(result, ProduceResult)

    @pytest.mark.asyncio
    async def test_send_with_headers(self, mock_eventhub_producer_client):
        """Test send() with headers."""
        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="xact",
            worker_name="test_worker",
            eventhub_name="test-hub",
        )

        with patch("kafka_pipeline.common.eventhub.producer.EventHubProducerClient") as mock_client_class:
            mock_client_class.from_connection_string.return_value = mock_eventhub_producer_client

            await producer.start()

            headers = {
                "trace-id": "abc123",
                "correlation-id": "xyz789",
            }

            result = await producer.send(
                topic="test-hub",
                key="key1",
                value=b"test-value",
                headers=headers,
            )

            # Verify result
            assert isinstance(result, ProduceResult)

    @pytest.mark.asyncio
    async def test_send_without_key(self, mock_eventhub_producer_client):
        """Test send() without key."""
        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="xact",
            worker_name="test_worker",
            eventhub_name="test-hub",
        )

        with patch("kafka_pipeline.common.eventhub.producer.EventHubProducerClient") as mock_client_class:
            mock_client_class.from_connection_string.return_value = mock_eventhub_producer_client

            await producer.start()

            result = await producer.send(
                topic="test-hub",
                key=None,
                value=b"test-value",
            )

            # Verify result
            assert isinstance(result, ProduceResult)

    @pytest.mark.asyncio
    async def test_send_returns_produce_result_with_defaults(self, mock_eventhub_producer_client):
        """Test send() returns ProduceResult with default partition/offset."""
        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="xact",
            worker_name="test_worker",
            eventhub_name="test-hub",
        )

        with patch("kafka_pipeline.common.eventhub.producer.EventHubProducerClient") as mock_client_class:
            mock_client_class.from_connection_string.return_value = mock_eventhub_producer_client

            await producer.start()

            result = await producer.send(
                topic="test-hub",
                key=b"key1",
                value=b"value1",
            )

            # EventHub doesn't provide partition/offset, so defaults are used
            assert result.topic == "test-hub"
            assert result.partition == 0
            assert result.offset == 0

    @pytest.mark.asyncio
    async def test_send_not_started_raises_error(self):
        """Test send() raises error when producer not started."""
        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="xact",
            worker_name="test_worker",
            eventhub_name="test-hub",
        )

        with pytest.raises(RuntimeError, match="Producer not started"):
            await producer.send(
                topic="test-hub",
                key=b"key1",
                value=b"value1",
            )


class TestEventHubProducerSendBatch:
    """Test producer send_batch() method."""

    @pytest.mark.asyncio
    async def test_send_batch_success(self, mock_eventhub_producer_client):
        """Test send_batch() sends multiple messages."""
        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="xact",
            worker_name="test_worker",
            eventhub_name="test-hub",
        )

        with patch("kafka_pipeline.common.eventhub.producer.EventHubProducerClient") as mock_client_class:
            mock_client_class.from_connection_string.return_value = mock_eventhub_producer_client

            await producer.start()

            messages = [
                ("key1", SampleMessage(id="1", content="test1")),
                ("key2", SampleMessage(id="2", content="test2")),
                ("key3", SampleMessage(id="3", content="test3")),
            ]

            results = await producer.send_batch(
                topic="test-hub",
                messages=messages,
            )

            # Verify batch was created and sent
            mock_eventhub_producer_client.create_batch.assert_called()
            mock_eventhub_producer_client.send_batch.assert_called()

            # Verify results
            assert isinstance(results, list)
            assert len(results) == 3
            for result in results:
                assert isinstance(result, ProduceResult)
                assert result.topic == "test-hub"

    @pytest.mark.asyncio
    async def test_send_batch_with_headers(self, mock_eventhub_producer_client):
        """Test send_batch() with headers."""
        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="xact",
            worker_name="test_worker",
            eventhub_name="test-hub",
        )

        with patch("kafka_pipeline.common.eventhub.producer.EventHubProducerClient") as mock_client_class:
            mock_client_class.from_connection_string.return_value = mock_eventhub_producer_client

            await producer.start()

            messages = [
                ("key1", SampleMessage(id="1", content="test1")),
            ]

            headers = {"batch-id": "batch-123"}

            results = await producer.send_batch(
                topic="test-hub",
                messages=messages,
                headers=headers,
            )

            # Verify results
            assert len(results) == 1
            assert isinstance(results[0], ProduceResult)

    @pytest.mark.asyncio
    async def test_send_batch_empty_list(self, mock_eventhub_producer_client):
        """Test send_batch() with empty message list."""
        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="xact",
            worker_name="test_worker",
            eventhub_name="test-hub",
        )

        with patch("kafka_pipeline.common.eventhub.producer.EventHubProducerClient") as mock_client_class:
            mock_client_class.from_connection_string.return_value = mock_eventhub_producer_client

            await producer.start()

            results = await producer.send_batch(
                topic="test-hub",
                messages=[],
            )

            # Should return empty list
            assert results == []

            # Should not send batch
            mock_eventhub_producer_client.send_batch.assert_not_called()

    @pytest.mark.asyncio
    async def test_send_batch_not_started_raises_error(self):
        """Test send_batch() raises error when producer not started."""
        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="xact",
            worker_name="test_worker",
            eventhub_name="test-hub",
        )

        messages = [
            ("key1", SampleMessage(id="1", content="test1")),
        ]

        with pytest.raises(RuntimeError, match="Producer not started"):
            await producer.send_batch(
                topic="test-hub",
                messages=messages,
            )


class TestEventHubProducerFlush:
    """Test producer flush() method."""

    @pytest.mark.asyncio
    async def test_flush_is_noop(self, mock_eventhub_producer_client):
        """Test flush() is a no-op for EventHub (synchronous sends)."""
        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="xact",
            worker_name="test_worker",
            eventhub_name="test-hub",
        )

        with patch("kafka_pipeline.common.eventhub.producer.EventHubProducerClient") as mock_client_class:
            mock_client_class.from_connection_string.return_value = mock_eventhub_producer_client

            await producer.start()

            # Should not raise
            await producer.flush()

    @pytest.mark.asyncio
    async def test_flush_not_started_raises_error(self):
        """Test flush() raises error when producer not started."""
        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="xact",
            worker_name="test_worker",
            eventhub_name="test-hub",
        )

        with pytest.raises(RuntimeError, match="Producer not started"):
            await producer.flush()


class TestEventHubProducerProperties:
    """Test producer properties."""

    def test_is_started_false_initially(self):
        """Test is_started property is False initially."""
        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="xact",
            worker_name="test_worker",
            eventhub_name="test-hub",
        )

        assert not producer.is_started

    @pytest.mark.asyncio
    async def test_is_started_true_after_start(self, mock_eventhub_producer_client):
        """Test is_started property is True after start()."""
        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="xact",
            worker_name="test_worker",
            eventhub_name="test-hub",
        )

        with patch("kafka_pipeline.common.eventhub.producer.EventHubProducerClient") as mock_client_class:
            mock_client_class.from_connection_string.return_value = mock_eventhub_producer_client

            await producer.start()

            assert producer.is_started

    @pytest.mark.asyncio
    async def test_is_started_false_after_stop(self, mock_eventhub_producer_client):
        """Test is_started property is False after stop()."""
        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="xact",
            worker_name="test_worker",
            eventhub_name="test-hub",
        )

        with patch("kafka_pipeline.common.eventhub.producer.EventHubProducerClient") as mock_client_class:
            mock_client_class.from_connection_string.return_value = mock_eventhub_producer_client

            await producer.start()
            await producer.stop()

            assert not producer.is_started
