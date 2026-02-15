"""
Unit tests for ClaimX Entity Delta Worker.

Test Coverage:
    - Worker initialization and configuration
    - Lifecycle management (start/stop)
    - Message parsing and validation
    - Batch accumulation (size-based and time-based)
    - Entity merging across batches
    - Delta writes with ClaimXEntityWriter
    - Retry handler integration for failed Delta writes
    - Graceful shutdown with batch flushing
    - Periodic logging and metrics

No infrastructure required - all dependencies mocked.
"""

import contextlib
from unittest.mock import AsyncMock, Mock, patch

import pytest

from config.config import MessageConfig
from pipeline.claimx.schemas.entities import EntityRowsMessage
from pipeline.claimx.workers.entity_delta_worker import ClaimXEntityDeltaWorker
from pipeline.common.types import PipelineMessage


@pytest.fixture
def mock_config():
    """Mock MessageConfig with standard settings."""
    config = Mock(spec=MessageConfig)
    config.get_topic.return_value = "claimx.enriched"
    config.get_consumer_group.return_value = "claimx-entity-delta-writer"

    def mock_get_worker_config(domain, worker_name, config_key=None):
        if config_key == "processing":
            return {
                "batch_size": 100,
                "batch_timeout_seconds": 30.0,
                "max_retries": 3,
                "retry_delays": [60, 300, 900],
                "retry_topic_prefix": "claimx.enriched.retry",
                "dlq_topic": "claimx.enriched.dlq",
                "health_port": 8086,
            }
        return {"health_port": 8086}

    config.get_worker_config = Mock(side_effect=mock_get_worker_config)
    return config


@pytest.fixture
def sample_entity_rows():
    """Sample entity rows message."""
    return EntityRowsMessage(
        trace_id="evt-123",
        event_type="PROJECT_CREATED",
        project_id="proj-456",
        projects=[
            {
                "project_id": "proj-456",
                "claim_number": "CLM123",
                "status": "active",
            }
        ],
        contacts=[
            {
                "contact_id": "cont-789",
                "project_id": "proj-456",
                "name": "John Doe",
            }
        ],
    )


@pytest.fixture
def sample_message(sample_entity_rows):
    """Sample Kafka message with entity rows."""
    return PipelineMessage(
        topic="claimx.enriched",
        partition=0,
        offset=1,
        key=b"evt-123",
        value=sample_entity_rows.model_dump_json().encode(),
        timestamp=None,
        headers=None,
    )


class TestEntityDeltaWorkerInitialization:
    """Test worker initialization and configuration."""

    def test_initialization_with_default_config(self, mock_config):
        """Worker initializes with default configuration."""
        worker = ClaimXEntityDeltaWorker(
            config=mock_config,
            projects_table_path="abfss://test/projects",
            contacts_table_path="abfss://test/contacts",
            media_table_path="abfss://test/media",
            tasks_table_path="abfss://test/tasks",
            task_templates_table_path="abfss://test/task_templates",
            external_links_table_path="abfss://test/external_links",
            video_collab_table_path="abfss://test/video_collab",
        )

        assert worker.domain == "claimx"
        assert worker.worker_id == "entity_delta_writer"
        assert worker.instance_id is None
        assert worker.batch_size == 100
        assert worker.batch_timeout_seconds == 30.0
        assert worker.max_retries == 3

    def test_initialization_with_instance_id(self, mock_config):
        """Worker uses instance ID for worker_id suffix."""
        worker = ClaimXEntityDeltaWorker(
            config=mock_config,
            projects_table_path="abfss://test/projects",
            contacts_table_path="abfss://test/contacts",
            media_table_path="abfss://test/media",
            tasks_table_path="abfss://test/tasks",
            task_templates_table_path="abfss://test/task_templates",
            external_links_table_path="abfss://test/external_links",
            video_collab_table_path="abfss://test/video_collab",
            instance_id="happy-tiger",
        )

        assert worker.worker_id == "entity_delta_writer-happy-tiger"
        assert worker.instance_id == "happy-tiger"

    def test_initialization_with_separate_producer_config(self, mock_config):
        """Worker accepts separate producer config."""
        producer_config = Mock(spec=MessageConfig)
        worker = ClaimXEntityDeltaWorker(
            config=mock_config,
            projects_table_path="abfss://test/projects",
            contacts_table_path="abfss://test/contacts",
            media_table_path="abfss://test/media",
            tasks_table_path="abfss://test/tasks",
            task_templates_table_path="abfss://test/task_templates",
            external_links_table_path="abfss://test/external_links",
            video_collab_table_path="abfss://test/video_collab",
            producer_config=producer_config,
        )

        assert worker.producer_config is producer_config

    def test_metrics_initialized_to_zero(self, mock_config):
        """Worker initializes metrics to zero."""
        worker = ClaimXEntityDeltaWorker(
            config=mock_config,
            projects_table_path="abfss://test/projects",
            contacts_table_path="abfss://test/contacts",
            media_table_path="abfss://test/media",
            tasks_table_path="abfss://test/tasks",
            task_templates_table_path="abfss://test/task_templates",
            external_links_table_path="abfss://test/external_links",
            video_collab_table_path="abfss://test/video_collab",
        )

        assert worker._records_processed == 0
        assert worker._records_succeeded == 0
        assert worker._records_failed == 0
        assert worker._batches_written == 0


class TestEntityDeltaWorkerLifecycle:
    """Test worker lifecycle (start/stop)."""

    @pytest.mark.asyncio
    async def test_start_initializes_components(self, mock_config):
        """Worker start initializes all components."""
        worker = ClaimXEntityDeltaWorker(
            config=mock_config,
            projects_table_path="abfss://test/projects",
            contacts_table_path="abfss://test/contacts",
            media_table_path="abfss://test/media",
            tasks_table_path="abfss://test/tasks",
            task_templates_table_path="abfss://test/task_templates",
            external_links_table_path="abfss://test/external_links",
            video_collab_table_path="abfss://test/video_collab",
        )

        with (
            patch(
                "pipeline.claimx.workers.entity_delta_worker.create_consumer"
            ) as mock_create_consumer,
            patch("pipeline.common.telemetry.initialize_worker_telemetry"),
            patch.object(worker.health_server, "start", new_callable=AsyncMock),
            patch("pipeline.claimx.workers.entity_delta_worker.DeltaRetryHandler"),
        ):
            mock_consumer = AsyncMock()
            mock_consumer.start = AsyncMock(side_effect=Exception("Stop"))
            mock_create_consumer.return_value = mock_consumer

            with contextlib.suppress(Exception):
                await worker.start()

            # Verify components were initialized
            assert worker.retry_handler is not None
            assert worker._consumer is not None

    @pytest.mark.asyncio
    async def test_stop_flushes_pending_batch(self, mock_config):
        """Worker stop flushes pending batch."""
        worker = ClaimXEntityDeltaWorker(
            config=mock_config,
            projects_table_path="abfss://test/projects",
            contacts_table_path="abfss://test/contacts",
            media_table_path="abfss://test/media",
            tasks_table_path="abfss://test/tasks",
            task_templates_table_path="abfss://test/task_templates",
            external_links_table_path="abfss://test/external_links",
            video_collab_table_path="abfss://test/video_collab",
        )

        # Setup mocked components
        worker._consumer = AsyncMock()
        worker._consumer.stop = AsyncMock()
        worker.retry_handler = AsyncMock()
        worker.retry_handler.stop = AsyncMock()

        # Add pending batch
        worker._batch = [Mock()]

        # Mock flush
        worker._flush_batch = AsyncMock()

        # Stop worker
        await worker.stop()

        # Verify flush was called
        worker._flush_batch.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_handles_none_components(self, mock_config):
        """Worker stop handles None components gracefully."""
        worker = ClaimXEntityDeltaWorker(
            config=mock_config,
            projects_table_path="abfss://test/projects",
            contacts_table_path="abfss://test/contacts",
            media_table_path="abfss://test/media",
            tasks_table_path="abfss://test/tasks",
            task_templates_table_path="abfss://test/task_templates",
            external_links_table_path="abfss://test/external_links",
            video_collab_table_path="abfss://test/video_collab",
        )

        # All components are None
        assert worker._consumer is None
        assert worker.retry_handler is None

        # Should not raise
        await worker.stop()


class TestEntityDeltaWorkerMessageProcessing:
    """Test message parsing and processing."""

    @pytest.mark.asyncio
    async def test_entity_rows_parsed_successfully(self, mock_config, sample_message):
        """Worker parses entity rows message."""
        worker = ClaimXEntityDeltaWorker(
            config=mock_config,
            projects_table_path="abfss://test/projects",
            contacts_table_path="abfss://test/contacts",
            media_table_path="abfss://test/media",
            tasks_table_path="abfss://test/tasks",
            task_templates_table_path="abfss://test/task_templates",
            external_links_table_path="abfss://test/external_links",
            video_collab_table_path="abfss://test/video_collab",
        )

        await worker._handle_message(sample_message)

        # Verify message was processed
        assert worker._records_processed == 1
        assert len(worker._batch) == 1

    @pytest.mark.asyncio
    async def test_invalid_json_increments_failed(self, mock_config):
        """Worker handles invalid JSON gracefully."""
        worker = ClaimXEntityDeltaWorker(
            config=mock_config,
            projects_table_path="abfss://test/projects",
            contacts_table_path="abfss://test/contacts",
            media_table_path="abfss://test/media",
            tasks_table_path="abfss://test/tasks",
            task_templates_table_path="abfss://test/task_templates",
            external_links_table_path="abfss://test/external_links",
            video_collab_table_path="abfss://test/video_collab",
        )

        invalid_message = PipelineMessage(
            topic="claimx.enriched",
            partition=0,
            offset=1,
            key=b"key",
            value=b"invalid json{",
            timestamp=None,
            headers=None,
        )

        with patch("pipeline.claimx.workers.entity_delta_worker.log_worker_error"):
            await worker._handle_message(invalid_message)

        # Verify error was tracked
        assert worker._records_failed == 1


class TestEntityDeltaWorkerBatching:
    """Test batch accumulation and flushing."""

    @pytest.mark.asyncio
    async def test_batch_accumulates_entity_rows(self, sample_message):
        """Worker accumulates entity rows in batch."""
        # Create config with custom batch size
        config = Mock(spec=MessageConfig)
        config.get_topic.return_value = "claimx.enriched"
        config.get_consumer_group.return_value = "claimx-entity-delta-writer"

        def mock_get_worker_config(domain, worker_name, config_key=None):
            if config_key == "processing":
                return {"batch_size": 10, "health_port": 8086}
            return {"health_port": 8086}

        config.get_worker_config = Mock(side_effect=mock_get_worker_config)

        worker = ClaimXEntityDeltaWorker(
            config=config,
            projects_table_path="abfss://test/projects",
            contacts_table_path="abfss://test/contacts",
            media_table_path="abfss://test/media",
            tasks_table_path="abfss://test/tasks",
            task_templates_table_path="abfss://test/task_templates",
            external_links_table_path="abfss://test/external_links",
            video_collab_table_path="abfss://test/video_collab",
        )

        # Process multiple messages
        for _ in range(3):
            await worker._handle_message(sample_message)

        # Verify batch accumulation
        assert len(worker._batch) == 3
        assert worker._records_processed == 3

    @pytest.mark.asyncio
    async def test_batch_flushes_on_size_threshold(self, sample_message):
        """Worker flushes batch when size threshold reached."""
        # Create config with batch_size=2
        config = Mock(spec=MessageConfig)
        config.get_topic.return_value = "claimx.enriched"
        config.get_consumer_group.return_value = "claimx-entity-delta-writer"

        def mock_get_worker_config(domain, worker_name, config_key=None):
            if config_key == "processing":
                return {"batch_size": 2, "health_port": 8086}
            return {"health_port": 8086}

        config.get_worker_config = Mock(side_effect=mock_get_worker_config)

        worker = ClaimXEntityDeltaWorker(
            config=config,
            projects_table_path="abfss://test/projects",
            contacts_table_path="abfss://test/contacts",
            media_table_path="abfss://test/media",
            tasks_table_path="abfss://test/tasks",
            task_templates_table_path="abfss://test/task_templates",
            external_links_table_path="abfss://test/external_links",
            video_collab_table_path="abfss://test/video_collab",
        )

        # Mock _trigger_flush (called when batch reaches threshold)
        worker._trigger_flush = Mock()

        # Process messages up to threshold
        await worker._handle_message(sample_message)
        await worker._handle_message(sample_message)

        # Verify flush was triggered
        worker._trigger_flush.assert_called()


class TestEntityDeltaWorkerDeltaWrites:
    """Test Delta table writes."""

    @pytest.mark.asyncio
    async def test_flush_batch_merges_and_writes_entities(self, mock_config, sample_entity_rows):
        """Worker merges entity rows and writes to Delta tables."""
        worker = ClaimXEntityDeltaWorker(
            config=mock_config,
            projects_table_path="abfss://test/projects",
            contacts_table_path="abfss://test/contacts",
            media_table_path="abfss://test/media",
            tasks_table_path="abfss://test/tasks",
            task_templates_table_path="abfss://test/task_templates",
            external_links_table_path="abfss://test/external_links",
            video_collab_table_path="abfss://test/video_collab",
        )

        # Mock entity writer
        worker.entity_writer = AsyncMock()
        worker.entity_writer.write_all = AsyncMock(return_value={"projects": 1, "contacts": 1})
        worker._consumer = AsyncMock()
        worker._consumer.commit = AsyncMock()

        # Add to batch
        worker._batch = [sample_entity_rows]

        # Flush batch
        await worker._flush_batch()

        # Verify Delta write was called
        assert worker.entity_writer.write_all.called
        assert worker._consumer.commit.called
        assert worker._batches_written == 1
        assert worker._records_succeeded == 2  # 1 project + 1 contact

    @pytest.mark.asyncio
    async def test_flush_batch_routes_to_retry_on_failure(self, mock_config, sample_entity_rows):
        """Worker routes batch to retry handler on Delta write failure."""
        worker = ClaimXEntityDeltaWorker(
            config=mock_config,
            projects_table_path="abfss://test/projects",
            contacts_table_path="abfss://test/contacts",
            media_table_path="abfss://test/media",
            tasks_table_path="abfss://test/tasks",
            task_templates_table_path="abfss://test/task_templates",
            external_links_table_path="abfss://test/external_links",
            video_collab_table_path="abfss://test/video_collab",
        )

        # Mock entity writer to fail
        worker.entity_writer = AsyncMock()
        worker.entity_writer.write_all = AsyncMock(side_effect=Exception("Delta write failed"))
        worker.retry_handler = AsyncMock()
        worker.retry_handler.handle_batch_failure = AsyncMock()
        worker.retry_handler.classify_delta_error = Mock(return_value=Mock(value="transient"))

        # Add to batch
        worker._batch = [sample_entity_rows]

        # Flush batch
        with patch("pipeline.claimx.workers.entity_delta_worker.log_worker_error"):
            await worker._flush_batch()

        # Verify retry handler was called
        assert worker.retry_handler.handle_batch_failure.called
        assert worker._batches_written == 0  # Not incremented on failure

    @pytest.mark.asyncio
    async def test_flush_batch_with_empty_entities_warns(self, mock_config):
        """Worker warns when batch contains no entity data."""
        worker = ClaimXEntityDeltaWorker(
            config=mock_config,
            projects_table_path="abfss://test/projects",
            contacts_table_path="abfss://test/contacts",
            media_table_path="abfss://test/media",
            tasks_table_path="abfss://test/tasks",
            task_templates_table_path="abfss://test/task_templates",
            external_links_table_path="abfss://test/external_links",
            video_collab_table_path="abfss://test/video_collab",
        )

        # Add empty entity rows to batch
        empty_rows = EntityRowsMessage()
        worker._batch = [empty_rows]

        # Flush batch should warn and return early
        await worker._flush_batch()

        # Verify no writes occurred
        assert worker._batches_written == 0
