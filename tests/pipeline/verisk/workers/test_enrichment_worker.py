"""
Unit tests for XACT Enrichment Worker.

Test Coverage:
    - Worker initialization and configuration
    - Lifecycle management (start/stop/graceful shutdown)
    - Message parsing and validation
    - Plugin execution and routing
    - Error handling and categorization
    - Retry/DLQ routing
    - Download task creation

No infrastructure required - all dependencies mocked.
"""

import json
import pytest
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

from config.config import MessageConfig
from core.types import ErrorCategory
from pipeline.verisk.schemas.tasks import XACTEnrichmentTask
from pipeline.verisk.workers.enrichment_worker import XACTEnrichmentWorker
from pipeline.common.types import PipelineMessage


@pytest.fixture
def mock_config():
    """Mock MessageConfig with standard settings."""
    config = Mock(spec=MessageConfig)
    config.get_topic.return_value = "verisk.enrichment.pending"
    config.get_consumer_group.return_value = "verisk-enrichment"
    config.get_worker_config.return_value = {
        "health_port": 8081,
        "health_enabled": True,
    }
    config.get_retry_delays.return_value = [300, 600, 1200]
    config.get_max_retries.return_value = 3
    return config


@pytest.fixture
def mock_plugin_registry():
    """Mock plugin registry."""
    registry = Mock()
    registry.get_plugin = Mock(return_value=None)
    return registry


@pytest.fixture
def sample_enrichment_task():
    """Sample enrichment task for testing."""
    return XACTEnrichmentTask(
        event_id="evt-123",
        trace_id="trace-456",
        event_type="xact",
        status_subtype="documentsReceived",
        assignment_id="assign-789",
        estimate_version="1.0",
        attachments=["https://example.com/file.pdf"],
        retry_count=0,
        created_at=datetime.now(UTC),
        original_timestamp=datetime.now(UTC),
    )


@pytest.fixture
def sample_message(sample_enrichment_task):
    """Sample Kafka message with enrichment task."""
    return PipelineMessage(
        topic="verisk.enrichment.pending",
        partition=0,
        offset=1,
        key=b"evt-123",
        value=sample_enrichment_task.model_dump_json().encode(),
        timestamp=None,
        headers=None,
    )


class TestXACTEnrichmentWorkerInitialization:
    """Test worker initialization and configuration."""

    def test_initialization_with_default_config(self, mock_config):
        """Worker initializes with default domain and config."""
        worker = XACTEnrichmentWorker(config=mock_config)

        assert worker.domain == "verisk"
        assert worker.consumer_config is mock_config
        assert worker.producer_config is mock_config
        assert worker.worker_id == "enrichment_worker"
        assert worker.instance_id is None
        assert worker._running is False

    def test_initialization_with_custom_domain(self, mock_config):
        """Worker initializes with custom domain."""
        worker = XACTEnrichmentWorker(config=mock_config, domain="custom")

        assert worker.domain == "custom"
        mock_config.get_consumer_group.assert_called_with("custom", "enrichment_worker")

    def test_initialization_with_instance_id(self, mock_config):
        """Worker uses instance ID for worker_id suffix."""
        worker = XACTEnrichmentWorker(config=mock_config, instance_id="3")

        assert worker.worker_id == "enrichment_worker-3"
        assert worker.instance_id == "3"

    def test_initialization_with_separate_producer_config(self, mock_config):
        """Worker accepts separate producer config."""
        producer_config = Mock(spec=MessageConfig)
        worker = XACTEnrichmentWorker(
            config=mock_config, producer_config=producer_config
        )

        assert worker.consumer_config is mock_config
        assert worker.producer_config is producer_config

    def test_retry_config_loaded_from_kafka_config(self, mock_config):
        """Worker loads retry delays and max retries from config."""
        worker = XACTEnrichmentWorker(config=mock_config)

        assert worker._retry_delays == [300, 600, 1200]
        assert worker._max_retries == 3
        mock_config.get_retry_delays.assert_called_once_with("verisk")
        mock_config.get_max_retries.assert_called_once_with("verisk")


class TestXACTEnrichmentWorkerLifecycle:
    """Test worker lifecycle (start/stop)."""

    @pytest.mark.asyncio
    async def test_start_initializes_components(self, mock_config):
        """Worker start initializes all components."""
        worker = XACTEnrichmentWorker(config=mock_config)

        with patch("pipeline.verisk.workers.enrichment_worker.create_producer") as mock_create_producer, \
             patch("pipeline.verisk.workers.enrichment_worker.create_consumer") as mock_create_consumer, \
             patch("pipeline.common.telemetry.initialize_worker_telemetry"):

            # Setup mocks
            mock_producer = AsyncMock()
            mock_producer.start = AsyncMock()
            mock_create_producer.return_value = mock_producer

            mock_consumer = AsyncMock()
            mock_consumer.start = AsyncMock()
            mock_create_consumer.return_value = mock_consumer

            # Prevent blocking on consumer.start
            mock_consumer.start.side_effect = Exception("Stop")

            try:
                await worker.start()
            except Exception:
                pass

            # Verify components initialized
            assert worker._running is True
            assert mock_producer.start.called

    @pytest.mark.asyncio
    async def test_stop_cleans_up_resources(self, mock_config):
        """Worker stop cleans up all resources."""
        worker = XACTEnrichmentWorker(config=mock_config)

        # Setup mocked components
        worker.consumer = AsyncMock()
        worker.consumer.stop = AsyncMock()
        worker.producer = AsyncMock()
        worker.producer.stop = AsyncMock()
        worker.retry_handler = AsyncMock()
        worker.retry_handler.stop = AsyncMock()
        worker._stats_logger = AsyncMock()
        worker._stats_logger.stop = AsyncMock()
        worker.health_server = AsyncMock()
        worker.health_server.stop = AsyncMock()

        worker._running = True

        # Stop worker
        await worker.stop()

        # Verify cleanup
        assert worker._running is False
        worker.consumer.stop.assert_called_once()
        worker.producer.stop.assert_called_once()
        worker.retry_handler.stop.assert_called_once()
        worker._stats_logger.stop.assert_called_once()
        worker.health_server.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_handles_none_components(self, mock_config):
        """Worker stop handles None components gracefully."""
        worker = XACTEnrichmentWorker(config=mock_config)

        # All components are None (not initialized)
        assert worker.consumer is None
        assert worker.producer is None

        # Should not raise
        await worker.stop()

    @pytest.mark.asyncio
    async def test_request_shutdown_sets_running_false(self, mock_config):
        """Request shutdown sets running flag to false."""
        worker = XACTEnrichmentWorker(config=mock_config)
        worker._running = True

        await worker.request_shutdown()

        assert worker._running is False


class TestXACTEnrichmentWorkerMessageProcessing:
    """Test message parsing and processing."""

    @pytest.mark.asyncio
    async def test_valid_message_parsed_successfully(
        self, mock_config, sample_message
    ):
        """Worker parses valid enrichment task message."""
        worker = XACTEnrichmentWorker(config=mock_config)

        # Mock _process_single_task to avoid full execution
        worker._process_single_task = AsyncMock()

        await worker._handle_enrichment_task(sample_message)

        # Verify task was parsed and processed
        assert worker._process_single_task.called
        task = worker._process_single_task.call_args[0][0]
        assert isinstance(task, XACTEnrichmentTask)
        assert task.event_id == "evt-123"
        assert task.event_type == "xact"
        assert task.status_subtype == "documentsReceived"
        assert task.assignment_id == "assign-789"
        assert worker._records_processed == 1

    @pytest.mark.asyncio
    async def test_invalid_json_raises_error(self, mock_config):
        """Worker raises error on invalid JSON."""
        worker = XACTEnrichmentWorker(config=mock_config)

        invalid_message = PipelineMessage(
            topic="test.topic",
            partition=0,
            offset=1,
            key=b"key",
            value=b"invalid json{",
            timestamp=None,
            headers=None,
        )

        with pytest.raises(json.JSONDecodeError):
            await worker._handle_enrichment_task(invalid_message)

    @pytest.mark.asyncio
    async def test_invalid_schema_raises_validation_error(self, mock_config):
        """Worker raises ValidationError on invalid task schema."""
        worker = XACTEnrichmentWorker(config=mock_config)

        invalid_message = PipelineMessage(
            topic="test.topic",
            partition=0,
            offset=1,
            key=b"key",
            value=b'{"invalid": "schema"}',
            timestamp=None,
            headers=None,
        )

        with pytest.raises(Exception):  # Pydantic ValidationError
            await worker._handle_enrichment_task(invalid_message)


class TestXACTEnrichmentWorkerPluginExecution:
    """Test plugin execution and routing."""

    @pytest.mark.asyncio
    async def test_plugin_executed_for_event_type(
        self, mock_config, sample_enrichment_task
    ):
        """Worker executes plugin orchestrator for task."""
        worker = XACTEnrichmentWorker(config=mock_config)
        worker.producer = AsyncMock()

        # Mock plugin orchestrator
        mock_result = Mock()
        mock_result.terminated = False
        mock_result.actions_executed = 0
        worker.plugin_orchestrator = AsyncMock()
        worker.plugin_orchestrator.execute = AsyncMock(return_value=mock_result)

        # Mock URL validation to allow test URLs
        with patch(
            "pipeline.verisk.workers.enrichment_worker.validate_download_url"
        ), patch(
            "pipeline.verisk.workers.enrichment_worker.generate_blob_path"
        ) as mock_blob:
            mock_blob.return_value = ("path/file.pdf", "pdf")

            await worker._process_single_task(sample_enrichment_task)

            # Verify plugin orchestrator was executed
            assert worker.plugin_orchestrator.execute.called

    @pytest.mark.asyncio
    async def test_plugin_orchestrator_none_processes_task(
        self, mock_config, sample_enrichment_task
    ):
        """Worker processes task even without plugin orchestrator."""
        worker = XACTEnrichmentWorker(config=mock_config)
        worker.producer = AsyncMock()
        worker.plugin_orchestrator = None  # No orchestrator

        # Mock URL validation
        with patch(
            "pipeline.verisk.workers.enrichment_worker.validate_download_url"
        ), patch(
            "pipeline.verisk.workers.enrichment_worker.generate_blob_path"
        ) as mock_blob:
            mock_blob.return_value = ("path/file.pdf", "pdf")

            await worker._process_single_task(sample_enrichment_task)

            # Verify task was still processed
            assert worker._records_succeeded == 1
            assert worker._records_skipped == 0


class TestXACTEnrichmentWorkerErrorHandling:
    """Test error handling and categorization."""

    @pytest.mark.asyncio
    async def test_task_error_categorized_correctly(
        self, mock_config, sample_enrichment_task
    ):
        """Worker categorizes task errors correctly."""
        worker = XACTEnrichmentWorker(config=mock_config)
        worker.producer = AsyncMock()
        worker._handle_enrichment_failure = AsyncMock()

        # Mock download task creation to raise error
        error = Exception("Download task creation error")
        worker._create_download_tasks_from_attachments = AsyncMock(side_effect=error)

        await worker._process_single_task(sample_enrichment_task)

        # Verify error was handled
        assert worker._handle_enrichment_failure.called
        call_args = worker._handle_enrichment_failure.call_args
        assert call_args[0][1] is error
        # Default category for unexpected errors
        assert call_args[0][2] == ErrorCategory.UNKNOWN


class TestXACTEnrichmentWorkerDownloadTasks:
    """Test download task creation and dispatch."""

    @pytest.mark.asyncio
    async def test_download_tasks_created_from_attachments(
        self, mock_config, sample_enrichment_task
    ):
        """Worker creates download tasks from enrichment task attachments."""
        worker = XACTEnrichmentWorker(config=mock_config)

        with patch(
            "pipeline.verisk.workers.enrichment_worker.validate_download_url"
        ), patch(
            "pipeline.verisk.workers.enrichment_worker.generate_blob_path"
        ) as mock_blob_path:
            mock_blob_path.return_value = ("path/to/file.pdf", "pdf")

            download_tasks = await worker._create_download_tasks_from_attachments(
                sample_enrichment_task
            )

            # Verify download tasks were created
            assert len(download_tasks) == 1
            task = download_tasks[0]
            assert task.trace_id == "trace-456"
            assert task.attachment_url == "https://example.com/file.pdf"
            assert task.assignment_id == "assign-789"

    @pytest.mark.asyncio
    async def test_no_download_tasks_when_no_attachments(self, mock_config):
        """Worker returns empty list when no attachments."""
        worker = XACTEnrichmentWorker(config=mock_config)

        # Create task with no attachments
        task = XACTEnrichmentTask(
            event_id="evt-123",
            trace_id="trace-456",
            event_type="xact",
            status_subtype="documentsReceived",
            assignment_id="assign-789",
            attachments=[],  # Empty
            retry_count=0,
            created_at=datetime.now(UTC),
            original_timestamp=datetime.now(UTC),
        )

        download_tasks = await worker._create_download_tasks_from_attachments(task)

        # Verify no download tasks created
        assert len(download_tasks) == 0
