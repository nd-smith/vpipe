"""
Tests for EventHub consumer DLQ routing and error handling.

These are unit tests that use mocks - no Azure EventHub required.
"""

import asyncio
import json
import pytest
import sys
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, Mock, call, patch

from aiokafka.structs import ConsumerRecord

# Mock azure.eventhub before importing consumer module
sys.modules['azure'] = MagicMock()
sys.modules['azure.eventhub'] = MagicMock()
sys.modules['azure.eventhub.aio'] = MagicMock()
sys.modules['azure.eventhub.extensions'] = MagicMock()
sys.modules['azure.eventhub.extensions.checkpointstoreblobaio'] = MagicMock()

from core.errors.exceptions import ErrorCategory, PermanentError, TransientError
from kafka_pipeline.common.eventhub.consumer import EventHubConsumer, EventHubConsumerRecord
from kafka_pipeline.common.types import PipelineMessage


# Mock EventData class for testing
class MockEventData:
    """Mock EventData for testing."""

    def __init__(self, body: bytes):
        self._body = body
        self._properties = {}
        self._enqueued_time = None
        self.offset = "0"  # Use public attribute to match real EventData

    @property
    def properties(self):
        return self._properties

    @property
    def enqueued_time(self):
        return self._enqueued_time

    def body_as_bytes(self):
        return self._body


@pytest.fixture
def mock_eventhub_consumer_client():
    """Create mock EventHubConsumerClient."""
    client = MagicMock()
    client.close = AsyncMock()
    client.receive = AsyncMock()

    # Make it async context manager
    async def async_enter(self):
        return self

    async def async_exit(self, *args):
        pass

    client.__aenter__ = async_enter
    client.__aexit__ = async_exit

    return client


@pytest.fixture
def mock_eventhub_producer():
    """Create mock EventHubProducer for DLQ."""
    producer = MagicMock()
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    producer.flush = AsyncMock()
    producer.send = AsyncMock()
    producer.eventhub_name = "xact-dlq"

    # Mock send to return metadata
    metadata = MagicMock()
    metadata.partition = 0
    metadata.offset = 123
    producer.send.return_value = metadata

    return producer


@pytest.fixture
def mock_message_handler():
    """Create mock message handler."""
    return AsyncMock()


def create_event_data(
    body: bytes = b'{"test": "value"}',
    properties: dict = None,
    enqueued_time: datetime = None,
    offset: str = "100",
) -> MockEventData:
    """Helper to create EventData for testing."""
    event = MockEventData(body)

    if properties:
        event._properties = properties
    else:
        event._properties = {"_key": "test-key"}

    if enqueued_time:
        event._enqueued_time = enqueued_time
    else:
        event._enqueued_time = datetime.now()

    event.offset = offset  # Set public attribute

    return event


class TestEventHubConsumerRecordAdapter:
    """Test EventHubConsumerRecord adapter class."""

    def test_adapts_event_data_to_consumer_record(self):
        """Test EventData is correctly adapted to ConsumerRecord interface."""
        properties = {"_key": "my-key", "custom_header": "value"}
        enqueued_time = datetime(2024, 1, 15, 12, 0, 0)
        event = create_event_data(
            body=b'{"field": "data"}',
            properties=properties,
            enqueued_time=enqueued_time,
            offset="42",
        )

        record = EventHubConsumerRecord(event, "test-hub", "3")

        assert record.topic == "test-hub"
        assert record.partition == 3
        assert record.offset == "42"
        assert record.timestamp == int(enqueued_time.timestamp() * 1000)
        assert record.key == b"my-key"
        assert record.value == b'{"field": "data"}'

        # Check headers (should exclude _key)
        headers_dict = dict(record.headers)
        assert "custom_header" in headers_dict
        assert headers_dict["custom_header"] == b"value"
        assert "_key" not in headers_dict

    def test_handles_missing_properties(self):
        """Test adapter handles EventData with no properties."""
        event = MockEventData(b"test body")
        event._enqueued_time = datetime.now()
        event.offset = "0"
        event._properties = None

        record = EventHubConsumerRecord(event, "test-hub", "0")

        assert record.key is None
        assert record.headers is None

    def test_handles_missing_enqueued_time(self):
        """Test adapter handles EventData with no enqueued time."""
        event = MockEventData(b"test body")
        event.offset = "0"
        event._enqueued_time = None

        record = EventHubConsumerRecord(event, "test-hub", "0")

        assert record.timestamp == 0


class TestEventHubConsumerInit:
    """Test EventHub consumer initialization."""

    def test_init_with_required_params(self, mock_message_handler):
        """Consumer initializes with required parameters."""
        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
        )

        assert consumer.domain == "xact"
        assert consumer.worker_name == "test_worker"
        assert consumer.eventhub_name == "verisk_events"
        assert consumer.consumer_group == "$Default"
        assert consumer.message_handler == mock_message_handler
        assert consumer._consumer is None
        assert not consumer._running
        assert consumer._enable_message_commit is True

    def test_init_with_optional_params(self, mock_message_handler):
        """Consumer initializes with optional parameters."""
        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
            enable_message_commit=False,
            instance_id="worker-1",
        )

        assert consumer.instance_id == "worker-1"
        assert consumer._enable_message_commit is False

    def test_init_builds_dlq_entity_map(self, mock_message_handler):
        """Consumer initializes DLQ entity mapping."""
        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
        )

        assert consumer._dlq_entity_map is not None
        assert isinstance(consumer._dlq_entity_map, dict)


class TestEventHubConsumerDLQMapping:
    """Test DLQ entity mapping functions."""

    def test_build_dlq_entity_map(self, mock_message_handler):
        """Test _build_dlq_entity_map returns correct mappings."""
        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
        )

        dlq_map = consumer._build_dlq_entity_map()

        # Check XACT mapping
        assert dlq_map["verisk_events"] == "xact-dlq"

        # Check ClaimX mapping
        assert dlq_map["claimx_events"] == "claimx-dlq"

    def test_get_dlq_entity_name_for_xact(self, mock_message_handler):
        """Test _get_dlq_entity_name resolves XACT topic correctly."""
        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
        )

        dlq_entity = consumer._get_dlq_entity_name("verisk_events")
        assert dlq_entity == "xact-dlq"

    def test_get_dlq_entity_name_for_claimx(self, mock_message_handler):
        """Test _get_dlq_entity_name resolves ClaimX topic correctly."""
        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="claimx",
            worker_name="test_worker",
            eventhub_name="claimx_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
        )

        dlq_entity = consumer._get_dlq_entity_name("claimx_events")
        assert dlq_entity == "claimx-dlq"

    def test_get_dlq_entity_name_for_unknown_topic(self, mock_message_handler):
        """Test _get_dlq_entity_name returns None for unknown topic."""
        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
        )

        dlq_entity = consumer._get_dlq_entity_name("unknown_topic")
        assert dlq_entity is None


class TestEventHubConsumerDLQProducer:
    """Test DLQ producer lazy initialization and management."""

    @pytest.mark.asyncio
    async def test_ensure_dlq_producer_creates_new_producer(
        self, mock_message_handler, mock_eventhub_producer
    ):
        """Test _ensure_dlq_producer creates new producer on first call."""
        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
        )

        with patch(
            "kafka_pipeline.common.eventhub.consumer.EventHubProducer",
            return_value=mock_eventhub_producer,
        ) as mock_producer_class:
            await consumer._ensure_dlq_producer("xact-dlq")

            # Producer was created
            mock_producer_class.assert_called_once_with(
                connection_string=consumer.connection_string,
                domain="verisk",
                worker_name="test_worker",
                eventhub_name="xact-dlq",
            )

            # Producer was started
            mock_eventhub_producer.start.assert_called_once()

            # Producer is stored
            assert consumer._dlq_producer == mock_eventhub_producer

    @pytest.mark.asyncio
    async def test_ensure_dlq_producer_reuses_existing_producer(
        self, mock_message_handler, mock_eventhub_producer
    ):
        """Test _ensure_dlq_producer reuses existing producer for same entity."""
        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
        )

        # Set up existing producer
        consumer._dlq_producer = mock_eventhub_producer
        mock_eventhub_producer.eventhub_name = "xact-dlq"

        with patch(
            "kafka_pipeline.common.eventhub.consumer.EventHubProducer"
        ) as mock_producer_class:
            await consumer._ensure_dlq_producer("xact-dlq")

            # No new producer created
            mock_producer_class.assert_not_called()

            # Existing producer not restarted
            mock_eventhub_producer.start.assert_not_called()

    @pytest.mark.asyncio
    async def test_ensure_dlq_producer_recreates_for_different_entity(
        self, mock_message_handler, mock_eventhub_producer
    ):
        """Test _ensure_dlq_producer recreates producer for different entity."""
        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
        )

        # Set up existing producer for different entity
        old_producer = MagicMock()
        old_producer.eventhub_name = "old-dlq"
        old_producer.stop = AsyncMock()
        consumer._dlq_producer = old_producer

        new_producer = MagicMock()
        new_producer.eventhub_name = "xact-dlq"
        new_producer.start = AsyncMock()

        with patch(
            "kafka_pipeline.common.eventhub.consumer.EventHubProducer",
            return_value=new_producer,
        ):
            await consumer._ensure_dlq_producer("xact-dlq")

            # Old producer was stopped
            old_producer.stop.assert_called_once()

            # New producer was created and started
            new_producer.start.assert_called_once()

            # New producer is stored
            assert consumer._dlq_producer == new_producer


class TestEventHubConsumerDLQSend:
    """Test DLQ message sending."""

    @pytest.mark.asyncio
    async def test_send_to_dlq_constructs_proper_message(
        self, mock_message_handler, mock_eventhub_producer
    ):
        """Test _send_to_dlq constructs DLQ message with all required fields."""
        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
        )
        consumer._dlq_producer = mock_eventhub_producer

        # Create test message
        message = ConsumerRecord(
            topic="verisk_events",
            partition=2,
            offset=100,
            timestamp=1234567890000,
            timestamp_type=0,
            key=b"test-key",
            value=b'{"data": "value"}',
            headers=[("header1", b"value1")],
            checksum=None,
            serialized_key_size=8,
            serialized_value_size=17,
        )

        error = ValueError("Processing failed")

        with patch("kafka_pipeline.common.eventhub.consumer.socket.gethostname", return_value="test-host"):
            with patch("kafka_pipeline.common.eventhub.consumer.time.time", return_value=9999999.0):
                success = await consumer._send_to_dlq(message, error, ErrorCategory.PERMANENT)

        assert success is True

        # Verify send was called
        mock_eventhub_producer.send.assert_called_once()

        # Extract the arguments
        call_args = mock_eventhub_producer.send.call_args
        sent_topic = call_args.kwargs["topic"]
        sent_key = call_args.kwargs["key"]
        sent_value = call_args.kwargs["value"]
        sent_headers = call_args.kwargs["headers"]

        # Verify topic
        assert sent_topic == "xact-dlq"

        # Verify key
        assert sent_key == b"test-key"

        # Verify message structure
        dlq_message = json.loads(sent_value.decode("utf-8"))
        assert dlq_message["original_topic"] == "verisk_events"
        assert dlq_message["original_partition"] == 2
        assert dlq_message["original_offset"] == 100
        assert dlq_message["original_key"] == "test-key"
        assert dlq_message["original_value"] == '{"data": "value"}'
        assert dlq_message["original_headers"] == {"header1": "value1"}
        assert dlq_message["original_timestamp"] == 1234567890000
        assert dlq_message["error_type"] == "ValueError"
        assert dlq_message["error_message"] == "Processing failed"
        assert dlq_message["error_category"] == "permanent"
        assert dlq_message["consumer_group"] == "$Default"
        assert dlq_message["worker_id"] == "test-host"
        assert dlq_message["domain"] == "xact"
        assert dlq_message["worker_name"] == "test_worker"
        assert dlq_message["dlq_timestamp"] == 9999999.0

        # Verify headers
        assert sent_headers["dlq_source_topic"] == "verisk_events"
        assert sent_headers["dlq_error_category"] == "permanent"
        assert sent_headers["dlq_consumer_group"] == "$Default"

    @pytest.mark.asyncio
    async def test_send_to_dlq_no_mapping_returns_false(
        self, mock_message_handler, mock_eventhub_producer
    ):
        """Test _send_to_dlq returns False when no DLQ mapping exists."""
        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
        )

        message = ConsumerRecord(
            topic="unknown_topic",
            partition=0,
            offset=0,
            timestamp=0,
            timestamp_type=0,
            key=b"key",
            value=b"value",
            headers=[],
            checksum=None,
            serialized_key_size=3,
            serialized_value_size=5,
        )

        error = ValueError("Test error")
        success = await consumer._send_to_dlq(message, error, ErrorCategory.PERMANENT)

        assert success is False

    @pytest.mark.asyncio
    async def test_send_to_dlq_producer_init_failure_returns_false(
        self, mock_message_handler
    ):
        """Test _send_to_dlq returns False when DLQ producer initialization fails."""
        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
        )

        message = ConsumerRecord(
            topic="verisk_events",
            partition=0,
            offset=0,
            timestamp=0,
            timestamp_type=0,
            key=b"key",
            value=b"value",
            headers=[],
            checksum=None,
            serialized_key_size=3,
            serialized_value_size=5,
        )

        # Mock producer initialization to fail
        with patch.object(consumer, "_ensure_dlq_producer", side_effect=RuntimeError("Init failed")):
            error = ValueError("Test error")
            success = await consumer._send_to_dlq(message, error, ErrorCategory.PERMANENT)

        assert success is False

    @pytest.mark.asyncio
    async def test_send_to_dlq_send_failure_returns_false(
        self, mock_message_handler, mock_eventhub_producer
    ):
        """Test _send_to_dlq returns False when send fails."""
        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
        )
        consumer._dlq_producer = mock_eventhub_producer

        # Make send fail
        mock_eventhub_producer.send.side_effect = RuntimeError("Send failed")

        message = ConsumerRecord(
            topic="verisk_events",
            partition=0,
            offset=0,
            timestamp=0,
            timestamp_type=0,
            key=b"key",
            value=b"value",
            headers=[],
            checksum=None,
            serialized_key_size=3,
            serialized_value_size=5,
        )

        error = ValueError("Test error")
        success = await consumer._send_to_dlq(message, error, ErrorCategory.PERMANENT)

        assert success is False

    @pytest.mark.asyncio
    async def test_send_to_dlq_records_metrics(
        self, mock_message_handler, mock_eventhub_producer
    ):
        """Test _send_to_dlq records DLQ metrics on success."""
        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
        )
        consumer._dlq_producer = mock_eventhub_producer

        message = ConsumerRecord(
            topic="verisk_events",
            partition=0,
            offset=0,
            timestamp=0,
            timestamp_type=0,
            key=b"key",
            value=b"value",
            headers=[],
            checksum=None,
            serialized_key_size=3,
            serialized_value_size=5,
        )

        error = ValueError("Test error")

        with patch("kafka_pipeline.common.eventhub.consumer.record_dlq_message") as mock_record:
            await consumer._send_to_dlq(message, error, ErrorCategory.PERMANENT)

            # Verify metrics were recorded
            mock_record.assert_called_once_with("xact", "permanent")


class TestEventHubConsumerErrorHandling:
    """Test error handling and DLQ routing integration."""

    @pytest.mark.asyncio
    async def test_permanent_error_triggers_dlq_routing(
        self, mock_message_handler, mock_eventhub_producer
    ):
        """Test that PERMANENT errors trigger DLQ routing."""
        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
        )
        consumer._dlq_producer = mock_eventhub_producer

        message = ConsumerRecord(
            topic="verisk_events",
            partition=0,
            offset=0,
            timestamp=0,
            timestamp_type=0,
            key=b"key",
            value=b"value",
            headers=[],
            checksum=None,
            serialized_key_size=3,
            serialized_value_size=5,
        )

        # Simulate permanent error
        permanent_error = PermanentError("Unrecoverable error")

        # Mock successful DLQ send
        with patch.object(consumer, "_send_to_dlq", return_value=True) as mock_send_dlq:
            # Should not raise - DLQ write successful
            await consumer._handle_processing_error(message, permanent_error, 0.1)

            # Verify DLQ was called
            mock_send_dlq.assert_called_once_with(message, permanent_error, ErrorCategory.PERMANENT)

    @pytest.mark.asyncio
    async def test_permanent_error_failed_dlq_prevents_checkpoint(
        self, mock_message_handler, mock_eventhub_producer
    ):
        """Test that failed DLQ write prevents checkpoint by re-raising."""
        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
        )

        message = ConsumerRecord(
            topic="verisk_events",
            partition=0,
            offset=0,
            timestamp=0,
            timestamp_type=0,
            key=b"key",
            value=b"value",
            headers=[],
            checksum=None,
            serialized_key_size=3,
            serialized_value_size=5,
        )

        permanent_error = PermanentError("Unrecoverable error")

        # Mock failed DLQ send
        with patch.object(consumer, "_send_to_dlq", return_value=False):
            # Should re-raise to prevent checkpoint
            with pytest.raises(PermanentError):
                await consumer._handle_processing_error(message, permanent_error, 0.1)

    @pytest.mark.asyncio
    async def test_transient_error_does_not_trigger_dlq(
        self, mock_message_handler
    ):
        """Test that TRANSIENT errors do not trigger DLQ routing."""
        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
        )

        message = ConsumerRecord(
            topic="verisk_events",
            partition=0,
            offset=0,
            timestamp=0,
            timestamp_type=0,
            key=b"key",
            value=b"value",
            headers=[],
            checksum=None,
            serialized_key_size=3,
            serialized_value_size=5,
        )

        transient_error = TransientError("Temporary error")

        with patch.object(consumer, "_send_to_dlq") as mock_send_dlq:
            # Should not raise
            await consumer._handle_processing_error(message, transient_error, 0.1)

            # DLQ should not be called
            mock_send_dlq.assert_not_called()


class TestEventHubConsumerCheckpointing:
    """Test checkpoint behavior with DLQ."""

    @pytest.mark.asyncio
    async def test_checkpoint_after_successful_dlq_write(
        self, mock_message_handler, mock_eventhub_producer
    ):
        """Test checkpoint advances after successful DLQ write."""
        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
        )
        consumer._dlq_producer = mock_eventhub_producer

        # Mock partition context
        partition_context = MagicMock()
        partition_context.partition_id = "0"
        partition_context.update_checkpoint = AsyncMock()

        event = create_event_data()

        # Simulate handler raising permanent error
        mock_message_handler.side_effect = PermanentError("Permanent failure")

        # Mock successful DLQ send
        with patch.object(consumer, "_send_to_dlq", return_value=True):
            # Simulate the on_event handler logic
            record = EventHubConsumerRecord(event, "verisk_events", "0")

            try:
                await consumer._process_message(record)

                # If we get here, processing "succeeded" (DLQ write successful)
                # In real code, checkpoint would happen in on_event after _process_message
                await partition_context.update_checkpoint(event)
            except Exception:
                # If exception, checkpoint should not happen
                pass

        # Verify checkpoint was called (DLQ write succeeded)
        partition_context.update_checkpoint.assert_called_once_with(event)

    @pytest.mark.asyncio
    async def test_no_checkpoint_after_failed_dlq_write(
        self, mock_message_handler
    ):
        """Test checkpoint does not advance when DLQ write fails."""
        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
        )

        # Mock partition context
        partition_context = MagicMock()
        partition_context.partition_id = "0"
        partition_context.update_checkpoint = AsyncMock()

        event = create_event_data()

        # Simulate handler raising permanent error
        mock_message_handler.side_effect = PermanentError("Permanent failure")

        # Mock failed DLQ send
        with patch.object(consumer, "_send_to_dlq", return_value=False):
            # Simulate the on_event handler logic
            record = EventHubConsumerRecord(event, "verisk_events", "0")

            try:
                await consumer._process_message(record)

                # Should not get here - exception should be raised
                await partition_context.update_checkpoint(event)
                pytest.fail("Expected exception to be raised")
            except PermanentError:
                # Exception raised - checkpoint should NOT happen
                pass

        # Verify checkpoint was NOT called (DLQ write failed)
        partition_context.update_checkpoint.assert_not_called()


class TestEventHubConsumerCleanup:
    """Test consumer cleanup and resource management."""

    @pytest.mark.asyncio
    async def test_stop_cleans_up_dlq_producer(
        self, mock_message_handler, mock_eventhub_consumer_client, mock_eventhub_producer
    ):
        """Test stop() properly cleans up DLQ producer."""
        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
        )

        # Set up consumer as running with DLQ producer
        consumer._running = True
        consumer._consumer = mock_eventhub_consumer_client
        consumer._dlq_producer = mock_eventhub_producer

        await consumer.stop()

        # Verify DLQ producer was flushed and stopped
        mock_eventhub_producer.flush.assert_called_once()
        mock_eventhub_producer.stop.assert_called_once()

        # Verify DLQ producer reference is cleared
        assert consumer._dlq_producer is None

    @pytest.mark.asyncio
    async def test_stop_handles_dlq_producer_error(
        self, mock_message_handler, mock_eventhub_consumer_client, mock_eventhub_producer
    ):
        """Test stop() handles DLQ producer cleanup errors gracefully."""
        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
        )

        consumer._running = True
        consumer._consumer = mock_eventhub_consumer_client
        consumer._dlq_producer = mock_eventhub_producer

        # Make flush fail
        mock_eventhub_producer.flush.side_effect = RuntimeError("Flush failed")

        # Should not raise - error is logged
        await consumer.stop()

        # DLQ producer still cleared
        assert consumer._dlq_producer is None

    @pytest.mark.asyncio
    async def test_stop_without_dlq_producer(
        self, mock_message_handler, mock_eventhub_consumer_client
    ):
        """Test stop() works when no DLQ producer exists."""
        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
        )

        consumer._running = True
        consumer._consumer = mock_eventhub_consumer_client
        consumer._dlq_producer = None

        # Should not raise
        await consumer.stop()

        assert consumer._dlq_producer is None


class TestEventHubConsumerCheckpointStore:
    """Test checkpoint store integration with EventHub consumer."""

    def test_init_with_checkpoint_store(self, mock_message_handler):
        """Test consumer initializes with checkpoint_store parameter."""
        mock_checkpoint_store = MagicMock()

        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
            checkpoint_store=mock_checkpoint_store,
        )

        assert consumer.checkpoint_store is mock_checkpoint_store

    def test_init_without_checkpoint_store(self, mock_message_handler):
        """Test consumer initializes without checkpoint_store (defaults to None)."""
        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
        )

        assert consumer.checkpoint_store is None

    @pytest.mark.asyncio
    async def test_start_passes_checkpoint_store_to_client(
        self, mock_message_handler, mock_eventhub_consumer_client
    ):
        """Test that checkpoint_store is passed to EventHubConsumerClient."""
        mock_checkpoint_store = MagicMock()

        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
            checkpoint_store=mock_checkpoint_store,
        )

        # Mock EventHubConsumerClient.from_connection_string
        with patch("kafka_pipeline.common.eventhub.consumer.EventHubConsumerClient") as mock_client_class:
            mock_client_class.from_connection_string.return_value = mock_eventhub_consumer_client

            # Mock the consume loop to avoid infinite loop
            async def mock_consume():
                pass

            with patch.object(consumer, "_consume_loop", side_effect=mock_consume):
                await consumer.start()

        # Verify checkpoint_store was passed to from_connection_string
        mock_client_class.from_connection_string.assert_called_once()
        call_kwargs = mock_client_class.from_connection_string.call_args.kwargs
        assert call_kwargs["checkpoint_store"] is mock_checkpoint_store

    @pytest.mark.asyncio
    async def test_start_without_checkpoint_store_passes_none(
        self, mock_message_handler, mock_eventhub_consumer_client
    ):
        """Test that None is passed to EventHubConsumerClient when checkpoint_store not configured."""
        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
            checkpoint_store=None,
        )

        # Mock EventHubConsumerClient.from_connection_string
        with patch("kafka_pipeline.common.eventhub.consumer.EventHubConsumerClient") as mock_client_class:
            mock_client_class.from_connection_string.return_value = mock_eventhub_consumer_client

            # Mock the consume loop to avoid infinite loop
            async def mock_consume():
                pass

            with patch.object(consumer, "_consume_loop", side_effect=mock_consume):
                await consumer.start()

        # Verify checkpoint_store=None was passed
        mock_client_class.from_connection_string.assert_called_once()
        call_kwargs = mock_client_class.from_connection_string.call_args.kwargs
        assert call_kwargs["checkpoint_store"] is None

    @pytest.mark.asyncio
    async def test_start_logs_checkpoint_mode_with_blob_storage(
        self, mock_message_handler, mock_eventhub_consumer_client, caplog
    ):
        """Test that start() logs checkpoint mode when using blob storage."""
        import logging

        mock_checkpoint_store = MagicMock()

        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
            checkpoint_store=mock_checkpoint_store,
        )

        with patch("kafka_pipeline.common.eventhub.consumer.EventHubConsumerClient") as mock_client_class:
            mock_client_class.from_connection_string.return_value = mock_eventhub_consumer_client

            async def mock_consume():
                pass

            with patch.object(consumer, "_consume_loop", side_effect=mock_consume):
                with caplog.at_level(logging.INFO):
                    await consumer.start()

        # Check for log message mentioning blob storage checkpoint persistence
        log_messages = [record.message for record in caplog.records if record.levelname == "INFO"]
        assert any("blob storage checkpoint persistence" in msg.lower() for msg in log_messages)

    @pytest.mark.asyncio
    async def test_start_logs_checkpoint_mode_with_in_memory(
        self, mock_message_handler, mock_eventhub_consumer_client, caplog
    ):
        """Test that start() logs checkpoint mode when using in-memory checkpoints."""
        import logging

        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123",
            domain="verisk",
            worker_name="test_worker",
            eventhub_name="verisk_events",
            consumer_group="$Default",
            message_handler=mock_message_handler,
            checkpoint_store=None,
        )

        with patch("kafka_pipeline.common.eventhub.consumer.EventHubConsumerClient") as mock_client_class:
            mock_client_class.from_connection_string.return_value = mock_eventhub_consumer_client

            async def mock_consume():
                pass

            with patch.object(consumer, "_consume_loop", side_effect=mock_consume):
                with caplog.at_level(logging.INFO):
                    await consumer.start()

        # Check for log message mentioning in-memory checkpoints
        log_messages = [record.message for record in caplog.records if record.levelname == "INFO"]
        assert any("in-memory checkpoints" in msg.lower() for msg in log_messages)


class TestEventHubConsumerRecordToPipelineMessage:
    """Test EventHubConsumerRecord to PipelineMessage conversion."""

    def test_to_pipeline_message_basic_conversion(self):
        """Test basic EventData to PipelineMessage conversion."""
        enqueued_time = datetime(2024, 1, 15, 12, 0, 0)
        event = create_event_data(
            body=b'{"test": "data"}',
            properties={"_key": "test-key"},
            enqueued_time=enqueued_time,
            offset="12345",
        )

        record = EventHubConsumerRecord(event, "test-hub", "3")
        msg = record.to_pipeline_message()

        assert isinstance(msg, PipelineMessage)
        assert msg.topic == "test-hub"
        assert msg.partition == 3
        assert msg.offset == "12345"
        assert msg.timestamp == int(enqueued_time.timestamp() * 1000)
        assert msg.key == b"test-key"
        assert msg.value == b'{"test": "data"}'

    def test_to_pipeline_message_with_headers(self):
        """Test conversion preserves headers (excluding _key)."""
        properties = {
            "_key": "entity-123",
            "trace-id": "abc123",
            "correlation-id": "xyz789",
            "content-type": "application/json",
        }
        event = create_event_data(properties=properties)

        record = EventHubConsumerRecord(event, "test-hub", "0")
        msg = record.to_pipeline_message()

        # Verify headers were converted (excluding _key)
        assert msg.headers is not None
        headers_dict = {k: v for k, v in msg.headers}

        assert b"trace-id" not in headers_dict  # Keys are strings, not bytes
        assert "trace-id" in headers_dict
        assert headers_dict["trace-id"] == b"abc123"
        assert headers_dict["correlation-id"] == b"xyz789"
        assert headers_dict["content-type"] == b"application/json"
        assert "_key" not in headers_dict  # Should be excluded from headers

    def test_to_pipeline_message_timestamp_conversion(self):
        """Test datetime to milliseconds timestamp conversion."""
        # Test specific datetime conversion
        enqueued_time = datetime(2021, 1, 1, 0, 0, 0)  # 2021-01-01 00:00:00 UTC
        expected_timestamp_ms = int(enqueued_time.timestamp() * 1000)

        event = create_event_data(enqueued_time=enqueued_time)
        record = EventHubConsumerRecord(event, "test-hub", "0")
        msg = record.to_pipeline_message()

        assert msg.timestamp == expected_timestamp_ms
        assert isinstance(msg.timestamp, int)

    def test_to_pipeline_message_no_enqueued_time(self):
        """Test conversion when EventData has no enqueued_time."""
        event = MockEventData(b"test body")
        event._enqueued_time = None
        event.offset = "0"

        record = EventHubConsumerRecord(event, "test-hub", "0")
        msg = record.to_pipeline_message()

        assert msg.timestamp == 0

    def test_to_pipeline_message_no_key(self):
        """Test conversion when EventData has no key in properties."""
        event = MockEventData(b"test body")
        event._properties = {"other_property": "value"}  # No _key property
        event._enqueued_time = datetime.now()
        event.offset = "0"

        record = EventHubConsumerRecord(event, "test-hub", "0")
        msg = record.to_pipeline_message()

        assert msg.key is None

    def test_to_pipeline_message_empty_key(self):
        """Test conversion when EventData has empty key string."""
        event = create_event_data(properties={"_key": ""})

        record = EventHubConsumerRecord(event, "test-hub", "0")
        msg = record.to_pipeline_message()

        assert msg.key is None

    def test_to_pipeline_message_no_properties(self):
        """Test conversion when EventData has no properties."""
        event = MockEventData(b"test body")
        event._properties = None
        event._enqueued_time = datetime.now()
        event.offset = "0"

        record = EventHubConsumerRecord(event, "test-hub", "0")
        msg = record.to_pipeline_message()

        assert msg.key is None
        assert msg.headers is None

    def test_to_pipeline_message_empty_headers(self):
        """Test conversion when only _key property exists (no other headers)."""
        event = create_event_data(properties={"_key": "test-key"})

        record = EventHubConsumerRecord(event, "test-hub", "0")
        msg = record.to_pipeline_message()

        # Headers should be None since only _key existed and it's excluded
        assert msg.headers is None

    def test_to_pipeline_message_immutability(self):
        """Test that returned PipelineMessage is immutable."""
        event = create_event_data()
        record = EventHubConsumerRecord(event, "test-hub", "0")
        msg = record.to_pipeline_message()

        # PipelineMessage is frozen - should not allow modification
        with pytest.raises(Exception):
            msg.topic = "modified-topic"

    def test_to_pipeline_message_partition_conversion(self):
        """Test partition ID conversion from string to int."""
        event = create_event_data()

        # Test various partition IDs
        record1 = EventHubConsumerRecord(event, "test-hub", "0")
        assert record1.to_pipeline_message().partition == 0

        record2 = EventHubConsumerRecord(event, "test-hub", "5")
        assert record2.to_pipeline_message().partition == 5

        record3 = EventHubConsumerRecord(event, "test-hub", "15")
        assert record3.to_pipeline_message().partition == 15

    def test_backward_compatibility_property_access(self):
        """Test backward compatibility - properties accessible directly on adapter."""
        enqueued_time = datetime(2024, 1, 15, 12, 0, 0)
        properties = {
            "_key": "entity-123",
            "trace-id": "abc123",
        }
        event = create_event_data(
            body=b'{"test": "data"}',
            properties=properties,
            enqueued_time=enqueued_time,
            offset="12345",
        )

        record = EventHubConsumerRecord(event, "test-hub", "3")

        # Verify backward compatibility - properties accessible on adapter
        assert record.topic == "test-hub"
        assert record.partition == 3
        assert record.offset == "12345"
        assert record.timestamp == int(enqueued_time.timestamp() * 1000)
        assert record.key == b"entity-123"
        assert record.value == b'{"test": "data"}'
        assert record.headers is not None

        # Verify same values in PipelineMessage
        msg = record.to_pipeline_message()
        assert msg.topic == record.topic
        assert msg.partition == record.partition
        assert msg.offset == record.offset
        assert msg.timestamp == record.timestamp
        assert msg.key == record.key
        assert msg.value == record.value
        assert msg.headers == record.headers

    def test_headers_conversion_format(self):
        """Test headers are converted to List[Tuple[str, bytes]] format."""
        properties = {
            "_key": "test-key",
            "string_value": "text",
            "numeric_value": 123,
            "bool_value": True,
        }
        event = create_event_data(properties=properties)

        record = EventHubConsumerRecord(event, "test-hub", "0")
        msg = record.to_pipeline_message()

        # Verify headers format
        assert isinstance(msg.headers, list)
        for header in msg.headers:
            assert isinstance(header, tuple)
            assert len(header) == 2
            assert isinstance(header[0], str)
            assert isinstance(header[1], bytes)

        # Verify all values are converted to bytes
        headers_dict = {k: v for k, v in msg.headers}
        assert headers_dict["string_value"] == b"text"
        assert headers_dict["numeric_value"] == b"123"
        assert headers_dict["bool_value"] == b"True"

    def test_multiple_conversions_independent(self):
        """Test multiple conversions produce independent PipelineMessage instances."""
        event1 = create_event_data(
            body=b"body1",
            properties={"_key": "key1"},
            offset="100",
        )
        event2 = create_event_data(
            body=b"body2",
            properties={"_key": "key2"},
            offset="200",
        )

        record1 = EventHubConsumerRecord(event1, "test-hub", "0")
        record2 = EventHubConsumerRecord(event2, "test-hub", "0")

        msg1 = record1.to_pipeline_message()
        msg2 = record2.to_pipeline_message()

        # Verify messages are independent
        assert msg1.value == b"body1"
        assert msg2.value == b"body2"
        assert msg1.key == b"key1"
        assert msg2.key == b"key2"
        assert msg1.offset == "100"
        assert msg2.offset == "200"
        assert msg1 != msg2
