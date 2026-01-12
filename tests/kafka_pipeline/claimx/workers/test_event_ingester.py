"""
Unit tests for ClaimXEventIngesterWorker.

Tests event consumption, enrichment task production, and UUID5 event_id generation.
"""

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiokafka.structs import ConsumerRecord

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
from kafka_pipeline.claimx.schemas.tasks import ClaimXEnrichmentTask
from kafka_pipeline.claimx.workers.event_ingester import ClaimXEventIngesterWorker


@pytest.fixture
def kafka_config():
    """Create test Kafka configuration."""
    return KafkaConfig(
        bootstrap_servers="localhost:9092",
        security_protocol="PLAINTEXT",
        claimx={
            "topics": {
                "events": "test.claimx.events.raw",
                "enrichment_pending": "test.claimx.enrichment.pending",
                "downloads_pending": "test.claimx.downloads.pending",
                "downloads_cached": "test.claimx.downloads.cached",
                "downloads_results": "test.claimx.downloads.results",
                "entities_rows": "test.claimx.entities.rows",
            },
            "consumer_group_prefix": "test-claimx",
            "retry_delays": [300, 600, 1200],
            "event_ingester": {
                "consumer": {"max_poll_records": 100},
                "processing": {"health_port": 8084},
            },
        },
        onelake_base_path="abfss://test@onelake.dfs.fabric.microsoft.com/lakehouse",
    )


@pytest.fixture
def sample_event():
    """Create sample ClaimXEventMessage for testing."""
    return ClaimXEventMessage(
        event_id="evt-12345",
        event_type="PROJECT_FILE_ADDED",
        project_id="proj-67890",
        ingested_at=datetime.now(timezone.utc),
        media_id="media-111",
        task_assignment_id=None,
        video_collaboration_id=None,
        master_file_name=None,
        raw_data={"fileName": "photo.jpg", "fileSize": 1024},
    )


@pytest.fixture
def sample_consumer_record(sample_event):
    """Create sample ConsumerRecord with ClaimXEventMessage."""
    return ConsumerRecord(
        topic="test.claimx.events.raw",
        partition=0,
        offset=10,
        timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
        timestamp_type=0,
        key=b"evt-12345",
        value=sample_event.model_dump_json().encode("utf-8"),
        headers=[],
        checksum=None,
        serialized_key_size=9,
        serialized_value_size=len(sample_event.model_dump_json()),
    )


@pytest.mark.asyncio
class TestClaimXEventIngesterWorker:
    """Test suite for ClaimXEventIngesterWorker."""

    async def test_initialization(self, kafka_config):
        """Test worker initialization with correct configuration."""
        with patch(
            "kafka_pipeline.claimx.workers.event_ingester.HealthCheckServer"
        ) as mock_health:
            mock_health.return_value = MagicMock()
            worker = ClaimXEventIngesterWorker(kafka_config)

            assert worker.consumer_config == kafka_config
            assert worker.domain == "claimx"
            assert worker.enrichment_topic == "test.claimx.enrichment.pending"
            assert worker.producer is None
            assert worker.consumer is None

    async def test_config_property(self, kafka_config):
        """Test backward-compatible config property."""
        with patch(
            "kafka_pipeline.claimx.workers.event_ingester.HealthCheckServer"
        ) as mock_health:
            mock_health.return_value = MagicMock()
            worker = ClaimXEventIngesterWorker(kafka_config)

            # config property should return consumer_config for backward compatibility
            assert worker.config == worker.consumer_config

    async def test_start_and_stop(self, kafka_config):
        """Test worker start and stop lifecycle."""
        with patch(
            "kafka_pipeline.claimx.workers.event_ingester.HealthCheckServer"
        ) as mock_health:
            mock_health.return_value = AsyncMock()
            worker = ClaimXEventIngesterWorker(kafka_config)

            # Mock producer and consumer
            mock_producer = AsyncMock()
            mock_consumer = AsyncMock()

            with patch(
                "kafka_pipeline.claimx.workers.event_ingester.BaseKafkaProducer"
            ) as mock_producer_class, patch(
                "kafka_pipeline.claimx.workers.event_ingester.BaseKafkaConsumer"
            ) as mock_consumer_class:
                mock_producer_class.return_value = mock_producer
                mock_consumer_class.return_value = mock_consumer

                # Set up worker with mocks
                worker.producer = mock_producer
                worker.consumer = mock_consumer

                # Test stop
                await worker.stop()

                # Verify stop was called on both
                mock_consumer.stop.assert_called_once()
                mock_producer.stop.assert_called_once()

    async def test_handle_event_message_creates_enrichment_task(
        self, kafka_config, sample_consumer_record
    ):
        """Test processing event creates enrichment task."""
        with patch(
            "kafka_pipeline.claimx.workers.event_ingester.HealthCheckServer"
        ) as mock_health:
            mock_health.return_value = MagicMock()
            worker = ClaimXEventIngesterWorker(kafka_config)

            # Mock producer
            mock_producer = AsyncMock()
            mock_metadata = MagicMock()
            mock_metadata.partition = 0
            mock_metadata.offset = 20
            mock_producer.send.return_value = mock_metadata
            worker.producer = mock_producer

            # Process the message
            await worker._handle_event_message(sample_consumer_record)

            # Verify producer.send was called once
            assert mock_producer.send.call_count == 1

            # Verify enrichment task details
            call_args = mock_producer.send.call_args
            assert call_args.kwargs["topic"] == "test.claimx.enrichment.pending"

            # Verify enrichment task content
            enrichment_task = call_args.kwargs["value"]
            assert isinstance(enrichment_task, ClaimXEnrichmentTask)
            assert enrichment_task.event_type == "PROJECT_FILE_ADDED"
            assert enrichment_task.project_id == "proj-67890"
            assert enrichment_task.retry_count == 0
            assert enrichment_task.media_id == "media-111"

    async def test_handle_event_message_generates_uuid5_event_id(
        self, kafka_config, sample_consumer_record
    ):
        """Test that deterministic UUID5 event_id is generated."""
        with patch(
            "kafka_pipeline.claimx.workers.event_ingester.HealthCheckServer"
        ) as mock_health:
            mock_health.return_value = MagicMock()
            worker = ClaimXEventIngesterWorker(kafka_config)

            # Mock producer
            mock_producer = AsyncMock()
            mock_metadata = MagicMock()
            mock_metadata.partition = 0
            mock_metadata.offset = 20
            mock_producer.send.return_value = mock_metadata
            worker.producer = mock_producer

            # Process the message twice
            await worker._handle_event_message(sample_consumer_record)
            first_call = mock_producer.send.call_args
            first_event_id = first_call.kwargs["value"].event_id

            # Reset mock
            mock_producer.reset_mock()

            # Process again
            await worker._handle_event_message(sample_consumer_record)
            second_call = mock_producer.send.call_args
            second_event_id = second_call.kwargs["value"].event_id

            # Same input should produce same UUID5 event_id
            assert first_event_id == second_event_id

    async def test_handle_event_message_invalid_json(self, kafka_config):
        """Test that invalid JSON raises exception."""
        with patch(
            "kafka_pipeline.claimx.workers.event_ingester.HealthCheckServer"
        ) as mock_health:
            mock_health.return_value = MagicMock()
            worker = ClaimXEventIngesterWorker(kafka_config)

            record = ConsumerRecord(
                topic="test.claimx.events.raw",
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

    async def test_handle_event_message_invalid_schema(self, kafka_config):
        """Test that schema validation errors raise exception."""
        with patch(
            "kafka_pipeline.claimx.workers.event_ingester.HealthCheckServer"
        ) as mock_health:
            mock_health.return_value = MagicMock()
            worker = ClaimXEventIngesterWorker(kafka_config)

            # Create invalid event data (missing required fields)
            invalid_event = {"event_id": "evt-123"}  # Missing event_type, project_id, etc.

            record = ConsumerRecord(
                topic="test.claimx.events.raw",
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

    async def test_create_enrichment_task(self, kafka_config, sample_event):
        """Test enrichment task creation from event."""
        with patch(
            "kafka_pipeline.claimx.workers.event_ingester.HealthCheckServer"
        ) as mock_health:
            mock_health.return_value = MagicMock()
            worker = ClaimXEventIngesterWorker(kafka_config)

            # Mock producer
            mock_producer = AsyncMock()
            mock_metadata = MagicMock()
            mock_metadata.partition = 0
            mock_metadata.offset = 20
            mock_producer.send.return_value = mock_metadata
            worker.producer = mock_producer

            # Set event_id (normally set by _handle_event_message)
            sample_event.event_id = "test-event-id-123"

            # Call internal method
            await worker._create_enrichment_task(sample_event)

            # Verify producer.send was called
            mock_producer.send.assert_called_once()

            call_args = mock_producer.send.call_args
            assert call_args.kwargs["topic"] == "test.claimx.enrichment.pending"
            assert call_args.kwargs["key"] == "test-event-id-123"

            enrichment_task = call_args.kwargs["value"]
            assert enrichment_task.event_id == "test-event-id-123"
            assert enrichment_task.event_type == sample_event.event_type
            assert enrichment_task.project_id == sample_event.project_id
            assert enrichment_task.media_id == sample_event.media_id

    async def test_create_enrichment_task_producer_failure(
        self, kafka_config, sample_event
    ):
        """Test that producer failures are propagated."""
        with patch(
            "kafka_pipeline.claimx.workers.event_ingester.HealthCheckServer"
        ) as mock_health:
            mock_health.return_value = MagicMock()
            worker = ClaimXEventIngesterWorker(kafka_config)

            # Mock producer to fail
            mock_producer = AsyncMock()
            mock_producer.send.side_effect = Exception("Kafka broker unavailable")
            worker.producer = mock_producer

            sample_event.event_id = "test-event-id-123"

            # Should raise the producer exception
            with pytest.raises(Exception, match="Kafka broker unavailable"):
                await worker._create_enrichment_task(sample_event)

    async def test_wait_for_pending_tasks_empty(self, kafka_config):
        """Test waiting for pending tasks when none exist."""
        with patch(
            "kafka_pipeline.claimx.workers.event_ingester.HealthCheckServer"
        ) as mock_health:
            mock_health.return_value = MagicMock()
            worker = ClaimXEventIngesterWorker(kafka_config)

            # Should return immediately with no tasks
            await worker._wait_for_pending_tasks(timeout_seconds=1)

            # No exception should be raised

    async def test_separate_producer_config(self, kafka_config):
        """Test worker initialization with separate producer config."""
        producer_config = KafkaConfig(
            bootstrap_servers="localhost:9093",  # Different server
            security_protocol="PLAINTEXT",
            claimx=kafka_config.claimx,
            onelake_base_path="abfss://test@onelake.dfs.fabric.microsoft.com/lakehouse",
        )

        with patch(
            "kafka_pipeline.claimx.workers.event_ingester.HealthCheckServer"
        ) as mock_health:
            mock_health.return_value = MagicMock()
            worker = ClaimXEventIngesterWorker(
                kafka_config, producer_config=producer_config
            )

            # Consumer config should be original config
            assert worker.consumer_config == kafka_config
            # Producer config should be the separate config
            assert worker.producer_config == producer_config
            assert worker.producer_config.bootstrap_servers == "localhost:9093"
