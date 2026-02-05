"""
Unit tests for ClaimX Download Worker.

Test Coverage:
    - Worker initialization and configuration
    - Lifecycle management (start/stop/graceful shutdown)
    - Batch processing with concurrent execution
    - Message parsing and validation
    - Download task processing
    - Success handling (cached message production)
    - Failure handling and retry routing
    - In-flight task tracking
    - Error categorization

No infrastructure required - all dependencies mocked.
"""

import json
import pytest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch
from dataclasses import dataclass

from config.config import KafkaConfig
from core.download.models import DownloadOutcome
from core.types import ErrorCategory
from pipeline.claimx.schemas.tasks import ClaimXDownloadTask
from pipeline.claimx.workers.download_worker import ClaimXDownloadWorker, TaskResult
from pipeline.common.types import PipelineMessage


@pytest.fixture
def mock_config():
    """Mock KafkaConfig with standard settings."""
    config = Mock(spec=KafkaConfig)
    config.get_topic.return_value = "claimx.downloads.pending"
    config.get_consumer_group.return_value = "claimx-download-worker"
    config.get_worker_config.return_value = {
        "health_port": 8082,
        "health_enabled": True,
        "concurrency": 10,
        "batch_size": 100,
    }
    config.cache_dir = "/tmp/cache"
    config.claimx_api_url = "https://api.test.claimxperience.com"
    config.claimx_api_token = "test-token"
    config.claimx_api_timeout_seconds = 30
    config.claimx_api_concurrency = 10
    return config


@pytest.fixture
def sample_download_task():
    """Sample download task for testing."""
    return ClaimXDownloadTask(
        media_id="media-123",
        project_id="proj-456",
        download_url="https://example.com/file.jpg",
        blob_path="claimx/proj-456/media/file.jpg",
        file_type="jpg",
        file_name="file.jpg",
        source_event_id="evt-abc",
        retry_count=0,
    )


@pytest.fixture
def sample_message(sample_download_task):
    """Sample Kafka message with download task."""
    return PipelineMessage(
        topic="claimx.downloads.pending",
        partition=0,
        offset=1,
        key=b"media-123",
        value=sample_download_task.model_dump_json().encode(),
        timestamp=None,
        headers=None,
    )


class TestClaimXDownloadWorkerInitialization:
    """Test worker initialization and configuration."""

    def test_initialization_with_default_config(self, mock_config, tmp_path):
        """Worker initializes with default domain and config."""
        with patch("pipeline.claimx.workers.download_worker.create_producer"):
            worker = ClaimXDownloadWorker(config=mock_config, temp_dir=tmp_path)

            assert worker.domain == "claimx"
            assert worker.config is mock_config
            assert worker.worker_id == "download_worker"
            assert worker.instance_id is None
            assert worker._running is False

    def test_initialization_with_custom_domain(self, mock_config, tmp_path):
        """Worker initializes with custom domain."""
        with patch("pipeline.claimx.workers.download_worker.create_producer"):
            worker = ClaimXDownloadWorker(
                config=mock_config, domain="custom", temp_dir=tmp_path
            )

            assert worker.domain == "custom"

    def test_initialization_with_instance_id(self, mock_config, tmp_path):
        """Worker uses instance ID for worker_id suffix."""
        with patch("pipeline.claimx.workers.download_worker.create_producer"):
            worker = ClaimXDownloadWorker(
                config=mock_config, instance_id="3", temp_dir=tmp_path
            )

            assert worker.worker_id == "download_worker-3"
            assert worker.instance_id == "3"

    def test_initialization_creates_directories(self, mock_config, tmp_path):
        """Worker creates temp and cache directories."""
        temp_dir = tmp_path / "temp"
        with patch("pipeline.claimx.workers.download_worker.create_producer"):
            worker = ClaimXDownloadWorker(config=mock_config, temp_dir=temp_dir)

            assert temp_dir.exists()
            assert worker.cache_dir.exists()

    def test_initialization_loads_concurrency_from_config(self, mock_config, tmp_path):
        """Worker loads concurrency and batch_size from config."""
        with patch("pipeline.claimx.workers.download_worker.create_producer"):
            worker = ClaimXDownloadWorker(config=mock_config, temp_dir=tmp_path)

            assert worker.concurrency == 10
            assert worker.batch_size == 100

    def test_initialization_sets_topics(self, mock_config, tmp_path):
        """Worker sets correct topics for consumption."""
        with patch("pipeline.claimx.workers.download_worker.create_producer"):
            worker = ClaimXDownloadWorker(config=mock_config, temp_dir=tmp_path)

            assert worker.topics == ["claimx.downloads.pending"]

    def test_metrics_initialized_to_zero(self, mock_config, tmp_path):
        """Worker initializes metrics to zero."""
        with patch("pipeline.claimx.workers.download_worker.create_producer"):
            worker = ClaimXDownloadWorker(config=mock_config, temp_dir=tmp_path)

            assert worker._records_processed == 0
            assert worker._records_succeeded == 0
            assert worker._records_failed == 0
            assert worker._records_skipped == 0


class TestClaimXDownloadWorkerLifecycle:
    """Test worker lifecycle (start/stop)."""

    @pytest.mark.asyncio
    async def test_start_initializes_semaphore_and_shutdown_event(self, mock_config, tmp_path):
        """Worker start initializes semaphore and shutdown event."""
        with patch("pipeline.claimx.workers.download_worker.create_producer"):
            worker = ClaimXDownloadWorker(config=mock_config, temp_dir=tmp_path)

        # Before start, these are None
        assert worker._semaphore is None
        assert worker._shutdown_event is None

        with patch("pipeline.claimx.workers.download_worker.create_producer") as mock_create_producer, \
             patch("pipeline.claimx.workers.download_worker.create_batch_consumer") as mock_create_consumer, \
             patch("pipeline.claimx.workers.download_worker.ClaimXApiClient"), \
             patch("pipeline.claimx.workers.download_worker.DownloadRetryHandler"), \
             patch("pipeline.common.telemetry.initialize_worker_telemetry"), \
             patch("aiohttp.ClientSession"):

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

            # After start, these should be initialized
            assert worker._semaphore is not None
            assert worker._shutdown_event is not None

    @pytest.mark.asyncio
    async def test_stop_cleans_up_resources(self, mock_config, tmp_path):
        """Worker stop cleans up all resources."""
        with patch("pipeline.claimx.workers.download_worker.create_producer"):
            worker = ClaimXDownloadWorker(config=mock_config, temp_dir=tmp_path)

        # Setup mocked components
        mock_consumer = AsyncMock()
        mock_producer = AsyncMock()
        mock_session = AsyncMock()
        mock_api = AsyncMock()
        mock_retry = AsyncMock()
        mock_logger = AsyncMock()
        mock_health = AsyncMock()

        worker._consumer = mock_consumer
        worker.producer = mock_producer
        worker._http_session = mock_session
        worker.api_client = mock_api
        worker.retry_handler = mock_retry
        worker._stats_logger = mock_logger
        worker.health_server = mock_health
        worker._shutdown_event = Mock()  # Use Mock not AsyncMock for Event
        worker._in_flight_tasks = set()

        worker._running = True

        # Stop worker
        await worker.stop()

        # Verify cleanup (using saved references since stop() sets them to None)
        assert worker._running is False
        mock_consumer.stop.assert_called_once()
        mock_producer.stop.assert_called_once()
        mock_session.close.assert_called_once()
        mock_api.close.assert_called_once()
        mock_retry.stop.assert_called_once()
        mock_logger.stop.assert_called_once()
        mock_health.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_handles_none_components(self, mock_config, tmp_path):
        """Worker stop handles None components gracefully."""
        with patch("pipeline.claimx.workers.download_worker.create_producer"):
            worker = ClaimXDownloadWorker(config=mock_config, temp_dir=tmp_path)

            # All components are None (not initialized)
            assert worker._consumer is None
            assert worker._http_session is None

            # Should not raise
            await worker.stop()

    @pytest.mark.asyncio
    async def test_request_shutdown_sets_running_false(self, mock_config, tmp_path):
        """Request shutdown sets running flag to false."""
        with patch("pipeline.claimx.workers.download_worker.create_producer"):
            worker = ClaimXDownloadWorker(config=mock_config, temp_dir=tmp_path)
            worker._running = True

            await worker.request_shutdown()

            assert worker._running is False

    @pytest.mark.asyncio
    async def test_wait_for_in_flight_returns_when_no_tasks(self, mock_config, tmp_path):
        """wait_for_in_flight returns immediately when no tasks."""
        with patch("pipeline.claimx.workers.download_worker.create_producer"):
            worker = ClaimXDownloadWorker(config=mock_config, temp_dir=tmp_path)
            worker._in_flight_tasks = set()

            # Should return immediately
            await worker._wait_for_in_flight(timeout=1.0)

            # No exception raised
            assert True

    @pytest.mark.asyncio
    async def test_wait_for_in_flight_times_out(self, mock_config, tmp_path):
        """wait_for_in_flight times out if tasks don't complete."""
        with patch("pipeline.claimx.workers.download_worker.create_producer"):
            worker = ClaimXDownloadWorker(config=mock_config, temp_dir=tmp_path)
            worker._in_flight_tasks = {"task-1", "task-2"}

            # Should timeout and return (not raise)
            await worker._wait_for_in_flight(timeout=0.1)

            # Verify still has tasks
            assert len(worker._in_flight_tasks) == 2


class TestClaimXDownloadWorkerMessageProcessing:
    """Test message parsing and processing."""

    @pytest.mark.asyncio
    async def test_valid_message_parsed_successfully(
        self, mock_config, sample_message, tmp_path
    ):
        """Worker parses valid download task message."""
        with patch("pipeline.claimx.workers.download_worker.create_producer"):
            worker = ClaimXDownloadWorker(config=mock_config, temp_dir=tmp_path)

        worker._semaphore = AsyncMock()
        worker._semaphore.__aenter__ = AsyncMock()
        worker._semaphore.__aexit__ = AsyncMock()

        # Mock dependencies
        worker.downloader = AsyncMock()
        outcome = DownloadOutcome(
            success=True,
            status_code=200,
            file_path=Path("/tmp/file.jpg"),
            bytes_downloaded=1024,
        )
        worker.downloader.download = AsyncMock(return_value=outcome)
        worker.producer = AsyncMock()
        worker.producer.send = AsyncMock()

        # Mock file system operations
        with patch("asyncio.to_thread", new_callable=AsyncMock):
            # Process task
            result = await worker._process_single_task(sample_message)

            # Verify task was parsed
            assert result.task_message.media_id == "media-123"
            assert result.task_message.project_id == "proj-456"
            assert result.task_message.download_url == "https://example.com/file.jpg"

    @pytest.mark.asyncio
    async def test_invalid_json_creates_error_result(self, mock_config, tmp_path):
        """Worker handles invalid JSON gracefully."""
        with patch("pipeline.claimx.workers.download_worker.create_producer"):
            worker = ClaimXDownloadWorker(config=mock_config, temp_dir=tmp_path)

        worker._semaphore = AsyncMock()
        worker._semaphore.__aenter__ = AsyncMock()
        worker._semaphore.__aexit__ = AsyncMock()

        invalid_message = PipelineMessage(
            topic="test.topic",
            partition=0,
            offset=1,
            key=b"key",
            value=b"invalid json{",
            timestamp=None,
            headers=None,
        )

        # Should not raise, but return error result
        result = await worker._process_single_task(invalid_message)

        assert result.success is False
        assert result.error is not None


class TestClaimXDownloadWorkerBatchProcessing:
    """Test batch processing with concurrent execution."""

    @pytest.mark.asyncio
    async def test_process_batch_executes_concurrently(
        self, mock_config, sample_message, sample_download_task, tmp_path
    ):
        """Worker processes batch messages concurrently."""
        with patch("pipeline.claimx.workers.download_worker.create_producer"):
            worker = ClaimXDownloadWorker(config=mock_config, temp_dir=tmp_path)

        worker._semaphore = AsyncMock()
        worker._semaphore.__aenter__ = AsyncMock()
        worker._semaphore.__aexit__ = AsyncMock()

        # Mock _process_single_task
        mock_result = TaskResult(
            message=sample_message,
            task_message=sample_download_task,
            outcome=DownloadOutcome(
                success=True,
                status_code=200,
                file_path=Path("/tmp/file.jpg"),
                bytes_downloaded=1024,
            ),
            processing_time_ms=100,
            success=True,
        )
        worker._process_single_task = AsyncMock(return_value=mock_result)

        # Process batch
        messages = [sample_message, sample_message, sample_message]
        result = await worker._process_batch(messages)

        # Verify all messages processed
        assert worker._process_single_task.call_count == 3
        assert result is True  # Commit batch

    @pytest.mark.asyncio
    async def test_process_batch_handles_exceptions(
        self, mock_config, sample_message, sample_download_task, tmp_path
    ):
        """Worker handles exceptions in batch processing."""
        with patch("pipeline.claimx.workers.download_worker.create_producer"):
            worker = ClaimXDownloadWorker(config=mock_config, temp_dir=tmp_path)

        worker._semaphore = AsyncMock()
        worker._semaphore.__aenter__ = AsyncMock()
        worker._semaphore.__aexit__ = AsyncMock()

        # Mock one success, one exception
        mock_result = TaskResult(
            message=sample_message,
            task_message=sample_download_task,
            outcome=DownloadOutcome(
                success=True,
                status_code=200,
                file_path=Path("/tmp/file.jpg"),
                bytes_downloaded=1024,
            ),
            processing_time_ms=100,
            success=True,
        )

        worker._process_single_task = AsyncMock(
            side_effect=[mock_result, Exception("Test error")]
        )
        worker.retry_handler = AsyncMock()
        worker.retry_handler.route_to_retry_or_dlq = AsyncMock()

        # Process batch
        messages = [sample_message, sample_message]
        result = await worker._process_batch(messages)

        # Should still commit batch
        assert result is True


class TestClaimXDownloadWorkerSuccessHandling:
    """Test success handling and cached message production."""

    @pytest.mark.asyncio
    async def test_handle_success_produces_cached_message(
        self, mock_config, sample_download_task, tmp_path
    ):
        """Worker produces cached message on success."""
        with patch("pipeline.claimx.workers.download_worker.create_producer"):
            worker = ClaimXDownloadWorker(config=mock_config, temp_dir=tmp_path)

        worker.producer = AsyncMock()
        worker.producer.send = AsyncMock()

        outcome = DownloadOutcome(
            success=True,
            status_code=200,
            file_path=Path("/tmp/file.jpg"),
            bytes_downloaded=1024,
        )

        # Mock file system operations
        with patch("asyncio.to_thread", new_callable=AsyncMock):
            await worker._handle_success(sample_download_task, outcome, processing_time_ms=100)

            # Verify cached message produced
            assert worker.producer.send.called
            call_args = worker.producer.send.call_args

            # Verify message key (uses source_event_id)
            assert call_args.kwargs["key"] == "evt-abc"


class TestClaimXDownloadWorkerFailureHandling:
    """Test failure handling and retry routing."""

    @pytest.mark.asyncio
    async def test_handle_failure_routes_to_retry(
        self, mock_config, sample_download_task, tmp_path
    ):
        """Worker routes failed task to retry handler."""
        with patch("pipeline.claimx.workers.download_worker.create_producer"):
            worker = ClaimXDownloadWorker(config=mock_config, temp_dir=tmp_path)

        worker.retry_handler = AsyncMock()
        worker.retry_handler.handle_failure = AsyncMock()

        outcome = DownloadOutcome(
            success=False,
            status_code=500,
            error_message="Server error",
            error_category=ErrorCategory.TRANSIENT,
        )

        # Mock record_processing_error to avoid parameter issues
        with patch("pipeline.claimx.workers.download_worker.record_processing_error"):
            await worker._handle_failure(
                sample_download_task,
                outcome,
                processing_time_ms=100,
            )

            # Verify routed to retry handler
            assert worker.retry_handler.handle_failure.called
            call_args = worker.retry_handler.handle_failure.call_args
            assert call_args.kwargs["error_category"] == ErrorCategory.TRANSIENT


class TestClaimXDownloadWorkerInFlightTracking:
    """Test in-flight task tracking."""

    @pytest.mark.asyncio
    async def test_in_flight_tasks_tracked(
        self, mock_config, sample_message, tmp_path
    ):
        """Worker tracks in-flight tasks during processing."""
        with patch("pipeline.claimx.workers.download_worker.create_producer"):
            worker = ClaimXDownloadWorker(config=mock_config, temp_dir=tmp_path)

        worker._semaphore = AsyncMock()
        worker._semaphore.__aenter__ = AsyncMock()
        worker._semaphore.__aexit__ = AsyncMock()

        # Mock download
        worker.downloader = AsyncMock()
        outcome = DownloadOutcome(
            success=True,
            status_code=200,
            file_path=Path("/tmp/file.jpg"),
            bytes_downloaded=1024,
        )
        worker.downloader.download = AsyncMock(return_value=outcome)
        worker.producer = AsyncMock()
        worker.producer.send = AsyncMock()

        # Mock file system operations
        with patch("asyncio.to_thread", new_callable=AsyncMock):
            # Process task
            await worker._process_single_task(sample_message)

            # In-flight tasks should be empty after completion
            assert len(worker._in_flight_tasks) == 0
