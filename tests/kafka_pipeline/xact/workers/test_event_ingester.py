"""
Unit tests for EventIngesterWorker.

Tests event consumption, URL validation, path generation, and download task production.
"""

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiokafka.structs import ConsumerRecord

from kafka_pipeline.xact.schemas.events import EventMessage
from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage
from kafka_pipeline.xact.workers.event_ingester import EventIngesterWorker


@pytest.fixture
def kafka_config():
    """Create test Kafka configuration using mock KafkaConfig."""
    config = MagicMock()
    config.bootstrap_servers = "localhost:9092"
    config.security_protocol = "PLAINTEXT"
    config.sasl_mechanism = "PLAIN"

    # Configure topics
    def get_topic(domain, topic_key):
        topics = {
            "events": "xact.events.raw",
            "events_ingested": "xact.events.ingested",
            "downloads_pending": "xact.downloads.pending",
        }
        return topics.get(topic_key, f"xact.{topic_key}")

    config.get_topic = MagicMock(side_effect=get_topic)
    config.get_consumer_group = MagicMock(return_value="xact-event-ingester")
    config.get_worker_config = MagicMock(return_value={})

    return config


@pytest.fixture
def sample_event_data():
    """Create sample event data as it would come from Eventhouse."""
    return {
        "type": "verisk.claims.property.xn.documentsReceived",
        "version": 1,
        "utcDateTime": datetime.now(timezone.utc).isoformat(),
        "traceId": "evt-123",
        "data": {
            "assignmentId": "A-456",
            "claimId": "C-789",
            "attachments": [
                "https://claimxperience.com/files/document1.pdf",
                "https://claimxperience.com/files/document2.pdf",
            ],
        },
    }


@pytest.fixture
def sample_event(sample_event_data):
    """Create sample EventMessage for testing."""
    return EventMessage.from_eventhouse_row(sample_event_data)


@pytest.fixture
def sample_consumer_record(sample_event_data):
    """Create sample ConsumerRecord with EventMessage."""
    # Serialize data as JSON string if dict
    data = sample_event_data.copy()
    if isinstance(data["data"], dict):
        data["data"] = json.dumps(data["data"])

    value = json.dumps(data).encode("utf-8")
    return ConsumerRecord(
        topic="xact.events.raw",
        partition=0,
        offset=10,
        timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
        timestamp_type=0,
        key=b"evt-123",
        value=value,
        headers=[],
        checksum=None,
        serialized_key_size=7,
        serialized_value_size=len(value),
    )


@pytest.mark.asyncio
class TestEventIngesterWorker:
    """Test suite for EventIngesterWorker."""

    async def test_initialization(self, kafka_config):
        """Test worker initialization with correct configuration."""
        worker = EventIngesterWorker(kafka_config, domain="xact")

        assert worker.consumer_config == kafka_config
        assert worker.domain == "xact"
        assert worker.producer is None
        assert worker.consumer is None

    async def test_start_and_stop(self, kafka_config):
        """Test worker start and stop lifecycle."""
        worker = EventIngesterWorker(kafka_config, domain="xact")

        # Mock producer and consumer
        mock_producer = AsyncMock()
        mock_consumer = AsyncMock()

        # Manually assign producer and consumer for stop testing
        worker.producer = mock_producer
        worker.consumer = mock_consumer
        worker._running = True

        # Test stop
        await worker.stop()

        # Verify stop was called on both
        mock_consumer.stop.assert_called_once()
        mock_producer.stop.assert_called_once()

    async def test_handle_event_with_attachments(
        self, kafka_config, sample_consumer_record, sample_event_data
    ):
        """Test processing event with valid attachments."""
        worker = EventIngesterWorker(kafka_config, domain="xact")

        # Mock producer
        mock_producer = AsyncMock()
        mock_metadata = MagicMock()
        mock_metadata.topic = "xact.downloads.pending"
        mock_metadata.partition = 0
        mock_metadata.offset = 20
        mock_producer.send.return_value = mock_metadata
        worker.producer = mock_producer

        # Mock generate_blob_path to return expected values
        with patch("kafka_pipeline.xact.workers.event_ingester.generate_blob_path") as mock_path:
            mock_path.return_value = ("documentsReceived/A-456/evt-123/document1.pdf", "pdf")

            # Mock validate_download_url to return valid
            with patch("kafka_pipeline.xact.workers.event_ingester.validate_download_url") as mock_validate:
                mock_validate.return_value = (True, None)

                # Process the message
                await worker._handle_event_message(sample_consumer_record)

        # Verify producer.send was called for:
        # 1. events_ingested topic (1 call)
        # 2. downloads_pending topic (2 calls for 2 attachments)
        assert mock_producer.send.call_count == 3

        # Find the download task calls (not the ingested event call)
        download_calls = [
            call for call in mock_producer.send.call_args_list
            if call.kwargs.get("topic") == "xact.downloads.pending"
        ]
        assert len(download_calls) == 2

        # Verify first download task
        first_call = download_calls[0]
        assert first_call.kwargs["key"] == "evt-123"

        # Verify download task content
        download_task = first_call.kwargs["value"]
        assert isinstance(download_task, DownloadTaskMessage)
        assert download_task.trace_id == "evt-123"
        assert download_task.event_type == "xact"
        assert download_task.retry_count == 0

    async def test_handle_event_without_attachments(self, kafka_config):
        """Test that events without attachments skip download task creation."""
        worker = EventIngesterWorker(kafka_config, domain="xact")
        mock_producer = AsyncMock()
        mock_metadata = MagicMock()
        mock_producer.send.return_value = mock_metadata
        worker.producer = mock_producer

        # Create event without attachments
        event_data = {
            "type": "verisk.claims.property.xn.documentsReceived",
            "version": 1,
            "utcDateTime": datetime.now(timezone.utc).isoformat(),
            "traceId": "evt-123",
            "data": {
                "assignmentId": "A-456",
                "claimId": "C-789",
                # No attachments
            },
        }

        value = json.dumps(event_data).encode("utf-8")
        record = ConsumerRecord(
            topic="xact.events.raw",
            partition=0,
            offset=10,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"evt-123",
            value=value,
            headers=[],
            checksum=None,
            serialized_key_size=7,
            serialized_value_size=len(value),
        )

        # Process the message
        await worker._handle_event_message(record)

        # Verify only one call for events_ingested topic (no download tasks)
        assert mock_producer.send.call_count == 1
        call = mock_producer.send.call_args
        assert call.kwargs["topic"] == "xact.events.ingested"

    async def test_handle_event_missing_assignment_id(self, kafka_config):
        """Test that events without assignment_id skip download task creation."""
        worker = EventIngesterWorker(kafka_config, domain="xact")
        mock_producer = AsyncMock()
        mock_metadata = MagicMock()
        mock_producer.send.return_value = mock_metadata
        worker.producer = mock_producer

        # Create event without assignment_id
        event_data = {
            "type": "verisk.claims.property.xn.documentsReceived",
            "version": 1,
            "utcDateTime": datetime.now(timezone.utc).isoformat(),
            "traceId": "evt-123",
            "data": {
                "claimId": "C-789",
                "attachments": ["https://claimxperience.com/files/document.pdf"],
                # No assignmentId
            },
        }

        value = json.dumps(event_data).encode("utf-8")
        record = ConsumerRecord(
            topic="xact.events.raw",
            partition=0,
            offset=10,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"evt-123",
            value=value,
            headers=[],
            checksum=None,
            serialized_key_size=7,
            serialized_value_size=len(value),
        )

        # Process the message
        await worker._handle_event_message(record)

        # Verify only one call for events_ingested topic (no download tasks)
        assert mock_producer.send.call_count == 1
        call = mock_producer.send.call_args
        assert call.kwargs["topic"] == "xact.events.ingested"

    async def test_handle_event_invalid_url(self, kafka_config):
        """Test that invalid URLs are skipped with warning."""
        worker = EventIngesterWorker(kafka_config, domain="xact")
        mock_producer = AsyncMock()
        mock_metadata = MagicMock()
        mock_producer.send.return_value = mock_metadata
        worker.producer = mock_producer

        # Create event with invalid URL (not in allowlist)
        event_data = {
            "type": "verisk.claims.property.xn.documentsReceived",
            "version": 1,
            "utcDateTime": datetime.now(timezone.utc).isoformat(),
            "traceId": "evt-123",
            "data": {
                "assignmentId": "A-456",
                "attachments": ["https://evil.com/malware.exe"],
            },
        }

        value = json.dumps(event_data).encode("utf-8")
        record = ConsumerRecord(
            topic="xact.events.raw",
            partition=0,
            offset=10,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"evt-123",
            value=value,
            headers=[],
            checksum=None,
            serialized_key_size=7,
            serialized_value_size=len(value),
        )

        # Mock validate_download_url to return invalid
        with patch("kafka_pipeline.xact.workers.event_ingester.validate_download_url") as mock_validate:
            mock_validate.return_value = (False, "Domain not in allowlist")

            # Process the message
            await worker._handle_event_message(record)

        # Verify only one call for events_ingested topic (no download tasks for invalid URLs)
        assert mock_producer.send.call_count == 1
        call = mock_producer.send.call_args
        assert call.kwargs["topic"] == "xact.events.ingested"

    async def test_handle_event_invalid_json(self, kafka_config):
        """Test that invalid JSON raises exception."""
        worker = EventIngesterWorker(kafka_config, domain="xact")

        record = ConsumerRecord(
            topic="xact.events.raw",
            partition=0,
            offset=10,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"evt-123",
            value=b"invalid json{{{",
            headers=[],
            checksum=None,
            serialized_key_size=7,
            serialized_value_size=16,
        )

        # Should raise JSONDecodeError
        with pytest.raises(json.JSONDecodeError):
            await worker._handle_event_message(record)

    async def test_handle_event_invalid_schema(self, kafka_config):
        """Test that schema validation errors raise exception."""
        worker = EventIngesterWorker(kafka_config, domain="xact")

        # Create invalid event data (missing required fields)
        invalid_event = {"traceId": "evt-123"}  # Missing many required fields

        record = ConsumerRecord(
            topic="xact.events.raw",
            partition=0,
            offset=10,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"evt-123",
            value=json.dumps(invalid_event).encode("utf-8"),
            headers=[],
            checksum=None,
            serialized_key_size=7,
            serialized_value_size=len(json.dumps(invalid_event)),
        )

        # Should raise ValidationError
        with pytest.raises(Exception):  # ValidationError from Pydantic
            await worker._handle_event_message(record)

    async def test_process_attachment_path_generation(self, kafka_config, sample_event):
        """Test blob path generation for different event subtypes."""
        worker = EventIngesterWorker(kafka_config, domain="xact")
        mock_producer = AsyncMock()
        mock_metadata = MagicMock()
        mock_metadata.topic = "xact.downloads.pending"
        mock_metadata.partition = 0
        mock_metadata.offset = 20
        mock_producer.send.return_value = mock_metadata
        worker.producer = mock_producer

        # Mock generate_blob_path
        with patch("kafka_pipeline.xact.workers.event_ingester.generate_blob_path") as mock_path:
            mock_path.return_value = ("documentsReceived/A-456/evt-123/doc.pdf", "pdf")

            # Mock validate_download_url
            with patch("kafka_pipeline.xact.workers.event_ingester.validate_download_url") as mock_validate:
                mock_validate.return_value = (True, None)

                # Test documentsReceived
                await worker._process_attachment(
                    event=sample_event,
                    attachment_url="https://claimxperience.com/files/doc.pdf",
                    assignment_id="A-456",
                )

        # Verify path generator was called
        mock_path.assert_called_once()

        # Verify download task was produced
        call_args = mock_producer.send.call_args
        download_task = call_args.kwargs["value"]
        assert isinstance(download_task, DownloadTaskMessage)
        assert download_task.blob_path == "documentsReceived/A-456/evt-123/doc.pdf"
        assert download_task.file_type == "pdf"

    async def test_process_attachment_producer_failure(
        self, kafka_config, sample_event
    ):
        """Test that producer failures are propagated."""
        worker = EventIngesterWorker(kafka_config, domain="xact")
        mock_producer = AsyncMock()
        mock_producer.send.side_effect = Exception("Kafka broker unavailable")
        worker.producer = mock_producer

        # Mock generate_blob_path
        with patch("kafka_pipeline.xact.workers.event_ingester.generate_blob_path") as mock_path:
            mock_path.return_value = ("documentsReceived/A-456/evt-123/doc.pdf", "pdf")

            # Mock validate_download_url
            with patch("kafka_pipeline.xact.workers.event_ingester.validate_download_url") as mock_validate:
                mock_validate.return_value = (True, None)

                # Should raise the producer exception
                with pytest.raises(Exception, match="Kafka broker unavailable"):
                    await worker._process_attachment(
                        event=sample_event,
                        attachment_url="https://claimxperience.com/files/doc.pdf",
                        assignment_id="A-456",
                    )
