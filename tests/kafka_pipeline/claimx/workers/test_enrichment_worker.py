"""
Unit tests for ClaimXEnrichmentWorker.

Tests enrichment task processing, API client integration, entity row production,
and download task generation.
"""

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

import pytest
from aiokafka.structs import ConsumerRecord

from core.types import ErrorCategory
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage
from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
from kafka_pipeline.claimx.schemas.tasks import ClaimXEnrichmentTask, ClaimXDownloadTask
from kafka_pipeline.claimx.workers.enrichment_worker import ClaimXEnrichmentWorker


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
            "enrichment_worker": {
                "consumer": {"max_poll_records": 100},
                "processing": {"health_port": 8081, "max_poll_records": 100},
            },
        },
        onelake_base_path="abfss://test@onelake.dfs.fabric.microsoft.com/lakehouse",
        claimx_api_url="https://test.claimxperience.com/api",
        claimx_api_token="test-token",
        claimx_api_timeout_seconds=30,
        claimx_api_concurrency=20,
    )


@pytest.fixture
def sample_enrichment_task():
    """Create sample ClaimXEnrichmentTask for testing."""
    return ClaimXEnrichmentTask(
        event_id="evt-12345",
        event_type="PROJECT_FILE_ADDED",
        project_id="proj-67890",
        retry_count=0,
        created_at=datetime.now(timezone.utc),
        media_id="media-111",
        task_assignment_id=None,
        video_collaboration_id=None,
        master_file_name=None,
    )


@pytest.fixture
def sample_consumer_record(sample_enrichment_task):
    """Create sample ConsumerRecord with ClaimXEnrichmentTask."""
    return ConsumerRecord(
        topic="test.claimx.enrichment.pending",
        partition=0,
        offset=10,
        timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
        timestamp_type=0,
        key=b"evt-12345",
        value=sample_enrichment_task.model_dump_json().encode("utf-8"),
        headers=[],
        checksum=None,
        serialized_key_size=9,
        serialized_value_size=len(sample_enrichment_task.model_dump_json()),
    )


@pytest.mark.asyncio
class TestClaimXEnrichmentWorker:
    """Test suite for ClaimXEnrichmentWorker."""

    async def test_initialization(self, kafka_config):
        """Test worker initialization with correct configuration."""
        with patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.HealthCheckServer"
        ) as mock_health, patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.get_handler_registry"
        ) as mock_registry:
            mock_health.return_value = MagicMock()
            mock_registry.return_value = MagicMock()

            worker = ClaimXEnrichmentWorker(kafka_config)

            assert worker.consumer_config == kafka_config
            assert worker.domain == "claimx"
            assert worker.enrichment_topic == "test.claimx.enrichment.pending"
            assert worker.download_topic == "test.claimx.downloads.pending"
            assert worker.entity_rows_topic == "test.claimx.entities.rows"
            assert worker.enable_delta_writes is True
            assert worker.producer is None
            assert worker.consumer is None
            assert worker.api_client is None

    async def test_initialization_topics_list(self, kafka_config):
        """Test worker constructs correct topics list with retry topics."""
        with patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.HealthCheckServer"
        ) as mock_health, patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.get_handler_registry"
        ) as mock_registry:
            mock_health.return_value = MagicMock()
            mock_registry.return_value = MagicMock()

            worker = ClaimXEnrichmentWorker(kafka_config)

            # Should have pending topic + retry topics
            assert "test.claimx.enrichment.pending" in worker.topics
            # Verify retry topics are constructed
            assert len(worker.topics) == 4  # pending + 3 retry levels

    async def test_handle_enrichment_task_parse_success(
        self, kafka_config, sample_consumer_record
    ):
        """Test parsing enrichment task from record."""
        with patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.HealthCheckServer"
        ) as mock_health, patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.get_handler_registry"
        ) as mock_registry:
            mock_health.return_value = MagicMock()
            mock_registry.return_value = MagicMock()

            worker = ClaimXEnrichmentWorker(kafka_config)

            # Mock _process_single_task to avoid full handler execution
            worker._process_single_task = AsyncMock()

            await worker._handle_enrichment_task(sample_consumer_record)

            # Verify task was passed to processor
            worker._process_single_task.assert_called_once()
            call_args = worker._process_single_task.call_args[0][0]
            assert isinstance(call_args, ClaimXEnrichmentTask)
            assert call_args.event_id == "evt-12345"
            assert call_args.project_id == "proj-67890"

    async def test_handle_enrichment_task_parse_failure(self, kafka_config):
        """Test handling invalid JSON in enrichment task."""
        with patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.HealthCheckServer"
        ) as mock_health, patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.get_handler_registry"
        ) as mock_registry:
            mock_health.return_value = MagicMock()
            mock_registry.return_value = MagicMock()

            worker = ClaimXEnrichmentWorker(kafka_config)

            record = ConsumerRecord(
                topic="test.claimx.enrichment.pending",
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

            with pytest.raises(json.JSONDecodeError):
                await worker._handle_enrichment_task(record)

    async def test_create_download_tasks_from_media(self, kafka_config):
        """Test creating download tasks from media entity rows."""
        with patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.HealthCheckServer"
        ) as mock_health, patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.get_handler_registry"
        ) as mock_registry:
            mock_health.return_value = MagicMock()
            mock_registry.return_value = MagicMock()

            worker = ClaimXEnrichmentWorker(kafka_config)

            media_rows = [
                {
                    "media_id": "media-111",
                    "project_id": "proj-67890",
                    "full_download_link": "https://s3.amazonaws.com/bucket/file.pdf",
                    "file_type": "pdf",
                    "file_name": "document.pdf",
                    "event_id": "evt-12345",
                    "expires_at": "2024-12-26T10:30:00Z",
                },
                {
                    "media_id": "media-222",
                    "project_id": "proj-67890",
                    "full_download_link": "https://s3.amazonaws.com/bucket/image.jpg",
                    "file_type": "jpg",
                    "file_name": "photo.jpg",
                    "event_id": "evt-12345",
                },
            ]

            download_tasks = worker._create_download_tasks_from_media(media_rows)

            assert len(download_tasks) == 2

            # Verify first task
            task1 = download_tasks[0]
            assert isinstance(task1, ClaimXDownloadTask)
            assert task1.media_id == "media-111"
            assert task1.project_id == "proj-67890"
            assert task1.download_url == "https://s3.amazonaws.com/bucket/file.pdf"
            assert task1.file_type == "pdf"
            assert task1.file_name == "document.pdf"
            assert task1.retry_count == 0

    async def test_create_download_tasks_skips_missing_url(self, kafka_config):
        """Test that media rows without download URL are skipped."""
        with patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.HealthCheckServer"
        ) as mock_health, patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.get_handler_registry"
        ) as mock_registry:
            mock_health.return_value = MagicMock()
            mock_registry.return_value = MagicMock()

            worker = ClaimXEnrichmentWorker(kafka_config)

            media_rows = [
                {
                    "media_id": "media-111",
                    "project_id": "proj-67890",
                    # No full_download_link
                    "file_type": "pdf",
                    "file_name": "document.pdf",
                },
                {
                    "media_id": "media-222",
                    "project_id": "proj-67890",
                    "full_download_link": "https://s3.amazonaws.com/bucket/image.jpg",
                    "file_type": "jpg",
                    "file_name": "photo.jpg",
                },
            ]

            download_tasks = worker._create_download_tasks_from_media(media_rows)

            # Only one task should be created (second row has URL)
            assert len(download_tasks) == 1
            assert download_tasks[0].media_id == "media-222"

    async def test_generate_blob_path(self, kafka_config):
        """Test blob path generation for media."""
        with patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.HealthCheckServer"
        ) as mock_health, patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.get_handler_registry"
        ) as mock_registry:
            mock_health.return_value = MagicMock()
            mock_registry.return_value = MagicMock()

            worker = ClaimXEnrichmentWorker(kafka_config)

            media_row = {
                "media_id": "media-111",
                "project_id": "proj-67890",
                "file_name": "document.pdf",
            }

            blob_path = worker._generate_blob_path(media_row)

            assert blob_path == "claimx/proj-67890/media/document.pdf"

    async def test_get_retry_topic(self, kafka_config):
        """Test retry topic name generation."""
        with patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.HealthCheckServer"
        ) as mock_health, patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.get_handler_registry"
        ) as mock_registry:
            mock_health.return_value = MagicMock()
            mock_registry.return_value = MagicMock()

            worker = ClaimXEnrichmentWorker(kafka_config)

            # Retry delays are [300, 600, 1200] seconds = [5, 10, 20] minutes
            assert worker._get_retry_topic(0) == "test.claimx.enrichment.pending.retry.5m"
            assert worker._get_retry_topic(1) == "test.claimx.enrichment.pending.retry.10m"
            assert worker._get_retry_topic(2) == "test.claimx.enrichment.pending.retry.20m"

    async def test_get_retry_topic_invalid_level(self, kafka_config):
        """Test retry topic raises error for invalid level."""
        with patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.HealthCheckServer"
        ) as mock_health, patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.get_handler_registry"
        ) as mock_registry:
            mock_health.return_value = MagicMock()
            mock_registry.return_value = MagicMock()

            worker = ClaimXEnrichmentWorker(kafka_config)

            with pytest.raises(ValueError, match="exceeds max retries"):
                worker._get_retry_topic(5)  # Only 3 retry levels configured

    async def test_produce_entity_rows(self, kafka_config):
        """Test producing entity rows to Kafka."""
        with patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.HealthCheckServer"
        ) as mock_health, patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.get_handler_registry"
        ) as mock_registry:
            mock_health.return_value = MagicMock()
            mock_registry.return_value = MagicMock()

            worker = ClaimXEnrichmentWorker(kafka_config, enable_delta_writes=True)

            # Mock producer
            mock_producer = AsyncMock()
            worker.producer = mock_producer

            entity_rows = EntityRowsMessage(
                projects=[{"project_id": "proj-123", "name": "Test Project"}],
                media=[{"media_id": "media-111", "file_name": "test.pdf"}],
            )

            tasks = [
                ClaimXEnrichmentTask(
                    event_id="evt-123",
                    event_type="PROJECT_CREATED",
                    project_id="proj-123",
                    retry_count=0,
                    created_at=datetime.now(timezone.utc),
                )
            ]

            await worker._produce_entity_rows(entity_rows, tasks)

            # Verify producer.send was called
            mock_producer.send.assert_called_once()
            call_args = mock_producer.send.call_args
            assert call_args.kwargs["topic"] == "test.claimx.entities.rows"

    async def test_produce_download_tasks(self, kafka_config):
        """Test producing download tasks to Kafka."""
        with patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.HealthCheckServer"
        ) as mock_health, patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.get_handler_registry"
        ) as mock_registry:
            mock_health.return_value = MagicMock()
            mock_registry.return_value = MagicMock()

            worker = ClaimXEnrichmentWorker(kafka_config)

            # Mock producer
            mock_producer = AsyncMock()
            mock_metadata = MagicMock()
            mock_metadata.partition = 0
            mock_metadata.offset = 20
            mock_producer.send.return_value = mock_metadata
            worker.producer = mock_producer

            download_tasks = [
                ClaimXDownloadTask(
                    media_id="media-111",
                    project_id="proj-67890",
                    download_url="https://s3.amazonaws.com/bucket/file.pdf",
                    blob_path="claimx/proj-67890/media/document.pdf",
                    file_type="pdf",
                    file_name="document.pdf",
                    source_event_id="evt-12345",
                    retry_count=0,
                ),
            ]

            await worker._produce_download_tasks(download_tasks)

            # Verify producer.send was called
            mock_producer.send.assert_called_once()
            call_args = mock_producer.send.call_args
            assert call_args.kwargs["topic"] == "test.claimx.downloads.pending"
            assert call_args.kwargs["key"] == "media-111"

    async def test_request_shutdown(self, kafka_config):
        """Test graceful shutdown request."""
        with patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.HealthCheckServer"
        ) as mock_health, patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.get_handler_registry"
        ) as mock_registry:
            mock_health.return_value = MagicMock()
            mock_registry.return_value = MagicMock()

            worker = ClaimXEnrichmentWorker(kafka_config)
            worker._running = True

            await worker.request_shutdown()

            assert worker._running is False

    async def test_separate_producer_config(self, kafka_config):
        """Test worker initialization with separate producer config."""
        producer_config = KafkaConfig(
            bootstrap_servers="localhost:9093",  # Different server
            security_protocol="PLAINTEXT",
            claimx=kafka_config.claimx,
            onelake_base_path="abfss://test@onelake.dfs.fabric.microsoft.com/lakehouse",
        )

        with patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.HealthCheckServer"
        ) as mock_health, patch(
            "kafka_pipeline.claimx.workers.enrichment_worker.get_handler_registry"
        ) as mock_registry:
            mock_health.return_value = MagicMock()
            mock_registry.return_value = MagicMock()

            worker = ClaimXEnrichmentWorker(
                kafka_config, producer_config=producer_config
            )

            # Consumer config should be original config
            assert worker.consumer_config == kafka_config
            # Producer config should be the separate config
            assert worker.producer_config == producer_config
            assert worker.producer_config.bootstrap_servers == "localhost:9093"
