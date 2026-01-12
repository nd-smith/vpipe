"""
Tests for DLQ handler with manual review and replay capability.
"""

import asyncio
import json
import logging
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from aiokafka.structs import ConsumerRecord

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.common.dlq.handler import DLQHandler
from kafka_pipeline.xact.schemas.results import FailedDownloadMessage
from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage


@pytest.fixture
def kafka_config():
    """Create test Kafka configuration with hierarchical domain structure."""
    return KafkaConfig(
        bootstrap_servers="localhost:9092",
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        xact={
            "topics": {
                "events": "test.events.raw",
                "downloads_pending": "test.downloads.pending",
                "downloads_cached": "test.downloads.cached",
                "dlq": "test.dlq",
            },
            "retry_delays": [300, 600, 1200, 2400],
            "consumer_group_prefix": "test",
        },
    )


@pytest.fixture
def sample_task():
    """Create sample download task message."""
    return DownloadTaskMessage(
        trace_id="evt-123",
        media_id="media-123",
        attachment_url="https://storage.example.com/file.pdf",
        blob_path="documentsReceived/C-456/pdf/file.pdf",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="C-456",
        event_type="claim",
        event_subtype="created",
        retry_count=4,
        original_timestamp=datetime.now(timezone.utc),
        metadata={"source": "test"},
    )


@pytest.fixture
def sample_dlq_message(sample_task):
    """Create sample DLQ message."""
    return FailedDownloadMessage(
        trace_id=sample_task.trace_id,
        media_id=sample_task.media_id,
        attachment_url=sample_task.attachment_url,
        original_task=sample_task,
        final_error="File not found after 4 retries",
        error_category="permanent",
        retry_count=4,
        failed_at=datetime.now(timezone.utc),
    )


def create_consumer_record_from_dlq(dlq_msg: FailedDownloadMessage) -> ConsumerRecord:
    """Helper to create ConsumerRecord containing DLQ message."""
    value_json = dlq_msg.model_dump_json()
    return ConsumerRecord(
        topic="test.dlq",
        partition=0,
        offset=42,
        timestamp=1234567890,
        timestamp_type=0,
        key=dlq_msg.trace_id.encode("utf-8"),
        value=value_json.encode("utf-8"),
        headers=[],
        checksum=None,
        serialized_key_size=len(dlq_msg.trace_id),
        serialized_value_size=len(value_json),
    )


class TestDLQHandlerInit:
    """Tests for DLQ handler initialization."""

    def test_init_with_config(self, kafka_config):
        """DLQ handler initializes with config."""
        handler = DLQHandler(kafka_config, domain="xact")

        assert handler.config == kafka_config
        assert handler._consumer is None
        assert handler._producer is None
        assert not handler.is_running

    def test_init_logs_configuration(self, kafka_config, caplog):
        """DLQ handler logs configuration on init."""
        with caplog.at_level(logging.INFO):
            handler = DLQHandler(kafka_config, domain="xact")

            assert "Initialized DLQ handler" in caplog.text


class TestDLQHandlerStartStop:
    """Tests for DLQ handler lifecycle."""

    @pytest.mark.asyncio
    @patch("kafka_pipeline.common.dlq.handler.BaseKafkaProducer")
    @patch("kafka_pipeline.common.dlq.handler.BaseKafkaConsumer")
    async def test_start_creates_producer_and_consumer(
        self, mock_consumer_class, mock_producer_class, kafka_config
    ):
        """Start creates producer and consumer with correct config."""
        # Setup mocks
        mock_producer = MagicMock()
        mock_producer.start = AsyncMock()
        mock_producer_class.return_value = mock_producer

        mock_consumer = MagicMock()
        mock_consumer.start = AsyncMock()
        mock_consumer_class.return_value = mock_consumer

        handler = DLQHandler(kafka_config, domain="xact")

        # Start in background task since consumer.start() blocks
        start_task = None
        try:
            start_task = asyncio.create_task(handler.start())
            # Give it a moment to initialize
            await asyncio.sleep(0.1)

            # Verify producer was created and started
            mock_producer_class.assert_called_once()
            mock_producer.start.assert_called_once()

            # Verify consumer was created with correct parameters
            mock_consumer_class.assert_called_once()
            call_kwargs = mock_consumer_class.call_args.kwargs
            assert call_kwargs["config"] == kafka_config
            assert call_kwargs["topics"] == [kafka_config.get_topic("xact", "dlq")]
            assert callable(call_kwargs["message_handler"])

        finally:
            # Cleanup
            if start_task and not start_task.done():
                start_task.cancel()
                try:
                    await start_task
                except asyncio.CancelledError:
                    pass

    @pytest.mark.asyncio
    @patch("kafka_pipeline.common.dlq.handler.BaseKafkaProducer")
    @patch("kafka_pipeline.common.dlq.handler.BaseKafkaConsumer")
    async def test_stop_cleans_up_resources(
        self, mock_consumer_class, mock_producer_class, kafka_config
    ):
        """Stop cleans up consumer and producer."""
        # Setup mocks
        mock_producer = MagicMock()
        mock_producer.start = AsyncMock()
        mock_producer.stop = AsyncMock()
        mock_producer_class.return_value = mock_producer

        mock_consumer = MagicMock()
        mock_consumer.start = AsyncMock()
        mock_consumer.stop = AsyncMock()
        mock_consumer_class.return_value = mock_consumer

        handler = DLQHandler(kafka_config, domain="xact")

        # Manually set consumer/producer (simulating started state)
        handler._consumer = mock_consumer
        handler._producer = mock_producer

        # Stop
        await handler.stop()

        # Verify cleanup
        mock_consumer.stop.assert_called_once()
        mock_producer.stop.assert_called_once()
        assert handler._consumer is None
        assert handler._producer is None

    @pytest.mark.asyncio
    async def test_stop_safe_when_not_started(self, kafka_config):
        """Stop is safe to call when handler not started."""
        handler = DLQHandler(kafka_config, domain="xact")

        # Should not raise
        await handler.stop()

        assert handler._consumer is None
        assert handler._producer is None


class TestDLQHandlerParsing:
    """Tests for DLQ message parsing."""

    def test_parse_dlq_message_success(self, kafka_config, sample_dlq_message):
        """Parse valid DLQ message from ConsumerRecord."""
        handler = DLQHandler(kafka_config, domain="xact")
        record = create_consumer_record_from_dlq(sample_dlq_message)

        parsed = handler.parse_dlq_message(record)

        assert parsed.trace_id == sample_dlq_message.trace_id
        assert parsed.attachment_url == sample_dlq_message.attachment_url
        assert parsed.retry_count == sample_dlq_message.retry_count
        assert parsed.error_category == sample_dlq_message.error_category
        assert parsed.final_error == sample_dlq_message.final_error

    def test_parse_dlq_message_empty_value(self, kafka_config):
        """Parse raises ValueError for empty message value."""
        handler = DLQHandler(kafka_config, domain="xact")
        record = ConsumerRecord(
            topic="test.dlq",
            partition=0,
            offset=0,
            timestamp=0,
            timestamp_type=0,
            key=b"key",
            value=None,
            headers=[],
            checksum=None,
            serialized_key_size=3,
            serialized_value_size=0,
        )

        with pytest.raises(ValueError, match="empty"):
            handler.parse_dlq_message(record)

    def test_parse_dlq_message_invalid_json(self, kafka_config):
        """Parse raises ValueError for invalid JSON."""
        handler = DLQHandler(kafka_config, domain="xact")
        record = ConsumerRecord(
            topic="test.dlq",
            partition=0,
            offset=0,
            timestamp=0,
            timestamp_type=0,
            key=b"key",
            value=b"not valid json {",
            headers=[],
            checksum=None,
            serialized_key_size=3,
            serialized_value_size=15,
        )

        with pytest.raises(ValueError, match="Invalid JSON"):
            handler.parse_dlq_message(record)

    def test_parse_dlq_message_invalid_schema(self, kafka_config):
        """Parse raises ValueError for JSON that doesn't match schema."""
        handler = DLQHandler(kafka_config, domain="xact")
        record = ConsumerRecord(
            topic="test.dlq",
            partition=0,
            offset=0,
            timestamp=0,
            timestamp_type=0,
            key=b"key",
            value=b'{"invalid": "schema"}',
            headers=[],
            checksum=None,
            serialized_key_size=3,
            serialized_value_size=21,
        )

        with pytest.raises(ValueError, match="Failed to parse"):
            handler.parse_dlq_message(record)


class TestDLQHandlerReplay:
    """Tests for message replay functionality."""

    @pytest.mark.asyncio
    async def test_replay_message_success(
        self, kafka_config, sample_dlq_message, caplog
    ):
        """Replay sends message back to pending topic with reset retry count."""
        handler = DLQHandler(kafka_config, domain="xact")

        # Mock producer
        mock_producer = MagicMock()
        mock_producer.send = AsyncMock()
        mock_producer.is_started = True
        handler._producer = mock_producer

        # Create record from DLQ message
        record = create_consumer_record_from_dlq(sample_dlq_message)

        # Replay
        with caplog.at_level(logging.INFO):
            await handler.replay_message(record)

        # Verify producer.send was called
        mock_producer.send.assert_called_once()
        call_kwargs = mock_producer.send.call_args.kwargs

        # Verify correct topic
        assert call_kwargs["topic"] == kafka_config.get_topic("xact", "downloads_pending")

        # Verify key matches trace_id
        assert call_kwargs["key"] == sample_dlq_message.trace_id

        # Verify task was reset
        replayed_task = call_kwargs["value"]
        assert isinstance(replayed_task, DownloadTaskMessage)
        assert replayed_task.trace_id == sample_dlq_message.trace_id
        assert replayed_task.attachment_url == sample_dlq_message.attachment_url
        assert replayed_task.retry_count == 0  # Reset!
        assert replayed_task.metadata["replayed_from_dlq"] is True
        assert replayed_task.metadata["dlq_offset"] == 42
        assert replayed_task.metadata["dlq_partition"] == 0

        # Verify headers
        headers = call_kwargs["headers"]
        assert headers["trace_id"] == sample_dlq_message.trace_id
        assert headers["replayed_from_dlq"] == "true"

        # Verify logging
        assert "Replaying DLQ message to pending topic" in caplog.text
        assert "DLQ message replayed successfully" in caplog.text

    @pytest.mark.asyncio
    async def test_replay_message_producer_not_started(
        self, kafka_config, sample_dlq_message
    ):
        """Replay raises RuntimeError if producer not started."""
        handler = DLQHandler(kafka_config, domain="xact")
        record = create_consumer_record_from_dlq(sample_dlq_message)

        with pytest.raises(RuntimeError, match="Producer not started"):
            await handler.replay_message(record)

    @pytest.mark.asyncio
    async def test_replay_message_invalid_record(self, kafka_config):
        """Replay raises ValueError for invalid DLQ message."""
        handler = DLQHandler(kafka_config, domain="xact")

        # Mock producer
        mock_producer = MagicMock()
        mock_producer.is_started = True
        handler._producer = mock_producer

        # Create invalid record
        record = ConsumerRecord(
            topic="test.dlq",
            partition=0,
            offset=0,
            timestamp=0,
            timestamp_type=0,
            key=b"key",
            value=b"invalid",
            headers=[],
            checksum=None,
            serialized_key_size=3,
            serialized_value_size=7,
        )

        with pytest.raises(ValueError):
            await handler.replay_message(record)


class TestDLQHandlerAcknowledge:
    """Tests for message acknowledgment."""

    @pytest.mark.asyncio
    async def test_acknowledge_message_success(
        self, kafka_config, sample_dlq_message, caplog
    ):
        """Acknowledge commits offset for message."""
        handler = DLQHandler(kafka_config, domain="xact")

        # Mock consumer with underlying aiokafka consumer
        mock_aiokafka = MagicMock()
        mock_aiokafka.commit = AsyncMock()

        mock_consumer = MagicMock()
        mock_consumer._consumer = mock_aiokafka
        handler._consumer = mock_consumer

        # Create record
        record = create_consumer_record_from_dlq(sample_dlq_message)

        # Acknowledge
        with caplog.at_level(logging.INFO):
            await handler.acknowledge_message(record)

        # Verify commit was called
        mock_aiokafka.commit.assert_called_once()

        # Verify logging
        assert "Acknowledging DLQ message" in caplog.text
        assert "DLQ message acknowledged" in caplog.text

    @pytest.mark.asyncio
    async def test_acknowledge_message_consumer_not_started(
        self, kafka_config, sample_dlq_message
    ):
        """Acknowledge raises RuntimeError if consumer not started."""
        handler = DLQHandler(kafka_config, domain="xact")
        record = create_consumer_record_from_dlq(sample_dlq_message)

        with pytest.raises(RuntimeError, match="Consumer not started"):
            await handler.acknowledge_message(record)


class TestDLQHandlerMessageHandler:
    """Tests for default message handler."""

    @pytest.mark.asyncio
    async def test_handle_dlq_message_logs_for_review(
        self, kafka_config, sample_dlq_message, caplog
    ):
        """Default handler logs DLQ messages for review."""
        handler = DLQHandler(kafka_config, domain="xact")
        record = create_consumer_record_from_dlq(sample_dlq_message)

        with caplog.at_level(logging.INFO):
            await handler._handle_dlq_message(record)

        # Verify logging
        assert "DLQ message received for review" in caplog.text

    @pytest.mark.asyncio
    async def test_handle_dlq_message_invalid_record_logs_error(
        self, kafka_config, caplog
    ):
        """Default handler logs error for invalid messages but doesn't fail."""
        handler = DLQHandler(kafka_config, domain="xact")

        # Create invalid record
        record = ConsumerRecord(
            topic="test.dlq",
            partition=0,
            offset=0,
            timestamp=0,
            timestamp_type=0,
            key=b"key",
            value=b"invalid",
            headers=[],
            checksum=None,
            serialized_key_size=3,
            serialized_value_size=7,
        )

        # Should not raise - just log error
        await handler._handle_dlq_message(record)

        assert "Failed to parse DLQ message" in caplog.text


class TestDLQHandlerIsRunning:
    """Tests for is_running property."""

    def test_is_running_false_when_not_started(self, kafka_config):
        """is_running is False when handler not started."""
        handler = DLQHandler(kafka_config, domain="xact")
        assert not handler.is_running

    def test_is_running_true_when_both_started(self, kafka_config):
        """is_running is True when both consumer and producer started."""
        handler = DLQHandler(kafka_config, domain="xact")

        # Mock both consumer and producer as started
        mock_consumer = MagicMock()
        mock_consumer.is_running = True
        handler._consumer = mock_consumer

        mock_producer = MagicMock()
        mock_producer.is_started = True
        handler._producer = mock_producer

        assert handler.is_running

    def test_is_running_false_when_consumer_not_running(self, kafka_config):
        """is_running is False when consumer not running."""
        handler = DLQHandler(kafka_config, domain="xact")

        # Mock consumer not running
        mock_consumer = MagicMock()
        mock_consumer.is_running = False
        handler._consumer = mock_consumer

        mock_producer = MagicMock()
        mock_producer.is_started = True
        handler._producer = mock_producer

        assert not handler.is_running

    def test_is_running_false_when_producer_not_started(self, kafka_config):
        """is_running is False when producer not started."""
        handler = DLQHandler(kafka_config, domain="xact")

        # Mock producer not started
        mock_consumer = MagicMock()
        mock_consumer.is_running = True
        handler._consumer = mock_consumer

        mock_producer = MagicMock()
        mock_producer.is_started = False
        handler._producer = mock_producer

        assert not handler.is_running
