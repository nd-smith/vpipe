"""
Unit tests for Verisk Download Worker.

Test Coverage:
    - Worker initialization and configuration
    - Lifecycle management (start/stop/graceful shutdown)
    - Message parsing and validation
    - Batch processing with concurrency
    - File download and caching
    - Success handling (CachedDownloadMessage production)
    - Failure handling (retry/DLQ routing, DownloadResultMessage production)
    - In-flight task tracking
    - HTTP session management

No infrastructure required - all dependencies mocked.
"""

import contextlib
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import pytest

from config.config import MessageConfig
from core.download.models import DownloadOutcome
from core.types import ErrorCategory
from pipeline.common.types import PipelineMessage
from pipeline.verisk.schemas.cached import CachedDownloadMessage
from pipeline.verisk.schemas.results import DownloadResultMessage
from pipeline.verisk.schemas.tasks import DownloadTaskMessage
from pipeline.verisk.workers.download_worker import DownloadWorker


@pytest.fixture
def mock_config():
    """Mock MessageConfig with standard settings."""
    config = Mock(spec=MessageConfig)
    config.get_topic.return_value = "verisk.downloads.pending"
    config.get_consumer_group.return_value = "verisk-download-worker"

    # get_worker_config is called with (domain, worker_name, "processing")
    # and should return the processing config dict directly
    def mock_get_worker_config(domain, worker_name, config_key=None):
        if config_key == "processing":
            return {
                "concurrency": 10,
                "batch_size": 100,
                "timeout_seconds": 60,
                "health_port": 8090,
            }
        return {"health_port": 8090}

    config.get_worker_config = Mock(side_effect=mock_get_worker_config)
    config.cache_dir = "/tmp/cache"
    return config


@pytest.fixture
def sample_download_task():
    """Sample DownloadTaskMessage for testing."""
    return DownloadTaskMessage(
        media_id="media-123",
        trace_id="trace-456",
        attachment_url="https://example.com/file.pdf",
        blob_path="verisk/A12345/documentsReceived/file.pdf",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="A12345",
        estimate_version="1.0",
        retry_count=0,
        event_type="xact",
        event_subtype="documentsReceived",
        original_timestamp=datetime.now(UTC),
        metadata={},
    )


@pytest.fixture
def sample_message(sample_download_task):
    """Sample Kafka message with download task."""
    return PipelineMessage(
        topic="verisk.downloads.pending",
        partition=0,
        offset=1,
        key=b"trace-456",
        value=sample_download_task.model_dump_json().encode(),
        timestamp=None,
        headers=None,
    )


class TestDownloadWorkerInitialization:
    """Test worker initialization and configuration."""

    def test_initialization_with_default_config(self, mock_config, tmp_path):
        """Worker initializes with default domain and config."""
        with patch("pipeline.verisk.workers.download_worker.create_producer"):
            worker = DownloadWorker(config=mock_config, temp_dir=tmp_path)

            assert worker.domain == "verisk"
            assert worker.config is mock_config
            assert worker.worker_id == "download_worker"
            assert worker.instance_id is None
            assert worker._running is False
            assert worker.temp_dir == tmp_path / "verisk"
            assert worker.cache_dir == Path("/tmp/cache/verisk")

    def test_initialization_with_custom_domain(self, mock_config, tmp_path):
        """Worker initializes with custom domain."""
        with patch("pipeline.verisk.workers.download_worker.create_producer"):
            worker = DownloadWorker(config=mock_config, domain="custom", temp_dir=tmp_path)

            assert worker.domain == "custom"

    def test_initialization_with_instance_id(self, mock_config, tmp_path):
        """Worker uses instance ID for worker_id suffix."""
        with patch("pipeline.verisk.workers.download_worker.create_producer"):
            worker = DownloadWorker(
                config=mock_config, instance_id="happy-tiger", temp_dir=tmp_path
            )

            assert worker.worker_id == "download_worker-happy-tiger"
            assert worker.instance_id == "happy-tiger"

    def test_initialization_creates_directories(self, mock_config, tmp_path):
        """Worker creates temp and cache directories."""
        cache_dir = tmp_path / "cache"
        mock_config.cache_dir = str(cache_dir)

        with patch("pipeline.verisk.workers.download_worker.create_producer"):
            worker = DownloadWorker(config=mock_config, temp_dir=tmp_path)

            assert worker.temp_dir.exists()
            assert worker.cache_dir.exists()

    def test_initialization_loads_concurrency_from_config(self, mock_config, tmp_path):
        """Worker loads concurrency settings from config."""
        with patch("pipeline.verisk.workers.download_worker.create_producer"):
            worker = DownloadWorker(config=mock_config, temp_dir=tmp_path)

            assert worker.concurrency == 10
            assert worker.batch_size == 100
            assert worker.timeout_seconds == 60

    def test_initialization_sets_topics(self, mock_config, tmp_path):
        """Worker sets correct consumer topics."""
        with patch("pipeline.verisk.workers.download_worker.create_producer"):
            worker = DownloadWorker(config=mock_config, temp_dir=tmp_path)

            assert worker.topics == ["verisk.downloads.pending"]
            assert worker.cached_topic == "verisk.downloads.pending"
            assert worker.results_topic == "verisk.downloads.pending"

    def test_metrics_initialized_to_zero(self, mock_config, tmp_path):
        """Worker initializes metrics to zero."""
        with patch("pipeline.verisk.workers.download_worker.create_producer"):
            worker = DownloadWorker(config=mock_config, temp_dir=tmp_path)

            assert worker._records_processed == 0
            assert worker._records_succeeded == 0
            assert worker._records_failed == 0
            assert worker._bytes_downloaded == 0


class TestDownloadWorkerLifecycle:
    """Test worker lifecycle (start/stop)."""

    @pytest.mark.asyncio
    async def test_start_initializes_semaphore_and_shutdown_event(self, mock_config, tmp_path):
        """Worker start initializes concurrency controls."""
        with patch("pipeline.verisk.workers.download_worker.create_producer"):
            worker = DownloadWorker(config=mock_config, temp_dir=tmp_path)

            with (
                patch(
                    "pipeline.verisk.workers.download_worker.create_batch_consumer"
                ) as mock_create_consumer,
                patch("pipeline.verisk.workers.download_worker.initialize_worker_telemetry"),
                patch.object(worker.health_server, "start", new_callable=AsyncMock),
                patch.object(worker.producer, "start", new_callable=AsyncMock),
            ):
                # Setup mock consumer
                mock_consumer = AsyncMock()
                mock_consumer.start = AsyncMock(side_effect=Exception("Stop"))
                mock_create_consumer.return_value = mock_consumer

                with contextlib.suppress(Exception):
                    await worker.start()

                # Verify semaphore and shutdown event were created
                assert worker._semaphore is not None
                assert worker._shutdown_event is not None
                assert worker._semaphore._value == 10  # concurrency limit

    @pytest.mark.asyncio
    async def test_stop_cleans_up_resources(self, mock_config, tmp_path):
        """Worker stop cleans up all resources."""
        with patch("pipeline.verisk.workers.download_worker.create_producer"):
            worker = DownloadWorker(config=mock_config, temp_dir=tmp_path)

            # Setup mocked components
            mock_consumer = AsyncMock()
            mock_consumer.stop = AsyncMock()
            mock_producer = AsyncMock()
            mock_producer.stop = AsyncMock()
            mock_session = AsyncMock()
            mock_session.close = AsyncMock()
            mock_retry_handler = AsyncMock()
            mock_retry_handler.stop = AsyncMock()
            mock_stats_logger = AsyncMock()
            mock_stats_logger.stop = AsyncMock()
            mock_health_server = AsyncMock()
            mock_health_server.stop = AsyncMock()

            worker._consumer = mock_consumer
            worker.producer = mock_producer
            worker._http_session = mock_session
            worker.retry_handler = mock_retry_handler
            worker._stats_logger = mock_stats_logger
            worker.health_server = mock_health_server
            worker._running = True

            # Stop worker
            await worker.stop()

            # Verify cleanup (use saved references since stop() sets them to None)
            assert worker._running is False
            mock_consumer.stop.assert_called_once()
            mock_producer.stop.assert_called_once()
            mock_session.close.assert_called_once()
            mock_retry_handler.stop.assert_called_once()
            mock_stats_logger.stop.assert_called_once()
            mock_health_server.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_handles_none_components(self, mock_config, tmp_path):
        """Worker stop handles None components gracefully."""
        with patch("pipeline.verisk.workers.download_worker.create_producer"):
            worker = DownloadWorker(config=mock_config, temp_dir=tmp_path)

            # All components are None (not initialized)
            assert worker._consumer is None
            assert worker._http_session is None

            # Should not raise
            await worker.stop()

    @pytest.mark.asyncio
    async def test_request_shutdown_sets_running_false(self, mock_config, tmp_path):
        """Request shutdown sets running flag to false."""
        with patch("pipeline.verisk.workers.download_worker.create_producer"):
            worker = DownloadWorker(config=mock_config, temp_dir=tmp_path)
            worker._running = True

            await worker.request_shutdown()

            assert worker._running is False

    @pytest.mark.asyncio
    async def test_wait_for_in_flight_returns_when_no_tasks(self, mock_config, tmp_path):
        """Worker wait completes when no in-flight tasks."""
        with patch("pipeline.verisk.workers.download_worker.create_producer"):
            worker = DownloadWorker(config=mock_config, temp_dir=tmp_path)
            worker._in_flight_tasks = set()

            await worker._wait_for_in_flight(timeout=1.0)

            # Should return immediately
            assert len(worker._in_flight_tasks) == 0

    @pytest.mark.asyncio
    async def test_wait_for_in_flight_times_out(self, mock_config, tmp_path):
        """Worker wait times out with in-flight tasks."""
        with patch("pipeline.verisk.workers.download_worker.create_producer"):
            worker = DownloadWorker(config=mock_config, temp_dir=tmp_path)
            worker._in_flight_tasks = {"media-123"}

            await worker._wait_for_in_flight(timeout=0.1)

            # Should timeout and return
            assert len(worker._in_flight_tasks) == 1


class TestDownloadWorkerMessageProcessing:
    """Test message parsing and processing."""

    @pytest.mark.asyncio
    async def test_valid_message_parsed_successfully(self, mock_config, tmp_path, sample_message):
        """Worker parses valid download task message."""
        with patch("pipeline.verisk.workers.download_worker.create_producer"):
            worker = DownloadWorker(config=mock_config, temp_dir=tmp_path)
            worker.producer = AsyncMock()
            worker._semaphore = AsyncMock()
            worker._semaphore.__aenter__ = AsyncMock()
            worker._semaphore.__aexit__ = AsyncMock()
            worker._consumer_group = "verisk-download-worker"

            # Mock downloader
            mock_outcome = DownloadOutcome(
                success=True,
                file_path=tmp_path / "file.pdf",
                bytes_downloaded=1024,
                content_type="application/pdf",
                status_code=200,
            )
            worker.downloader = AsyncMock()
            worker.downloader.download = AsyncMock(return_value=mock_outcome)

            # Mock file operations
            with patch("asyncio.to_thread", new_callable=AsyncMock):
                result = await worker._process_single_task(sample_message)

                # Verify message was processed
                assert result.success is True
                assert result.task_message.trace_id == "trace-456"
                assert worker._records_processed == 1

    @pytest.mark.asyncio
    async def test_invalid_json_creates_error_result(self, mock_config, tmp_path):
        """Worker creates error result on invalid JSON."""
        with patch("pipeline.verisk.workers.download_worker.create_producer"):
            worker = DownloadWorker(config=mock_config, temp_dir=tmp_path)
            worker._consumer_group = "verisk-download-worker"

            invalid_message = PipelineMessage(
                topic="verisk.downloads.pending",
                partition=0,
                offset=1,
                key=b"key",
                value=b"invalid json{",
                timestamp=None,
                headers=None,
            )

            result = await worker._process_single_task(invalid_message)

            # Verify error result
            assert result.success is False
            assert result.outcome.success is False
            assert result.outcome.error_category == ErrorCategory.PERMANENT


class TestDownloadWorkerBatchProcessing:
    """Test batch processing with concurrency."""

    @pytest.mark.asyncio
    async def test_process_batch_executes_concurrently(self, mock_config, tmp_path, sample_message):
        """Worker processes batch of messages concurrently."""
        with patch("pipeline.verisk.workers.download_worker.create_producer"):
            worker = DownloadWorker(config=mock_config, temp_dir=tmp_path)
            worker.producer = AsyncMock()
            worker._semaphore = AsyncMock()
            worker._semaphore.__aenter__ = AsyncMock()
            worker._semaphore.__aexit__ = AsyncMock()
            worker._consumer_group = "verisk-download-worker"

            # Mock successful download
            mock_outcome = DownloadOutcome(
                success=True,
                file_path=tmp_path / "file.pdf",
                bytes_downloaded=1024,
                content_type="application/pdf",
                status_code=200,
            )
            worker.downloader = AsyncMock()
            worker.downloader.download = AsyncMock(return_value=mock_outcome)

            # Mock file operations
            with patch("asyncio.to_thread", new_callable=AsyncMock):
                commit = await worker._process_batch([sample_message] * 3)

                # Verify batch was processed
                assert commit is True
                assert worker._records_succeeded == 3

    @pytest.mark.asyncio
    async def test_process_batch_handles_exceptions(self, mock_config, tmp_path, sample_message):
        """Worker handles exceptions during batch processing."""
        with patch("pipeline.verisk.workers.download_worker.create_producer"):
            worker = DownloadWorker(config=mock_config, temp_dir=tmp_path)
            worker._semaphore = AsyncMock()
            worker._semaphore.__aenter__ = AsyncMock(side_effect=Exception("Processing error"))
            worker._semaphore.__aexit__ = AsyncMock()
            worker._consumer_group = "verisk-download-worker"

            commit = await worker._process_batch([sample_message])

            # Verify exception was handled
            assert commit is True  # Still commits even with exceptions
            assert worker._records_failed == 1


class TestDownloadWorkerSuccessHandling:
    """Test success handling and cached message production."""

    @pytest.mark.asyncio
    async def test_handle_success_produces_cached_message(
        self, mock_config, tmp_path, sample_download_task
    ):
        """Worker produces CachedDownloadMessage on success."""
        with patch("pipeline.verisk.workers.download_worker.create_producer"):
            worker = DownloadWorker(config=mock_config, temp_dir=tmp_path)
            worker.producer = AsyncMock()
            worker.cached_topic = "verisk.downloads.cached"

            outcome = DownloadOutcome(
                success=True,
                file_path=tmp_path / "file.pdf",
                bytes_downloaded=1024,
                content_type="application/pdf",
                status_code=200,
            )

            # Mock file operations
            with patch("asyncio.to_thread", new_callable=AsyncMock):
                await worker._handle_success(sample_download_task, outcome, processing_time_ms=100)

                # Verify cached message was produced
                assert worker.producer.send.called
                call_args = worker.producer.send.call_args
                cached_msg = call_args.kwargs["value"]
                assert isinstance(cached_msg, CachedDownloadMessage)
                assert cached_msg.trace_id == "trace-456"
                assert cached_msg.media_id == "media-123"
                assert call_args.kwargs["key"] == "trace-456"


class TestDownloadWorkerFailureHandling:
    """Test failure handling and retry/DLQ routing."""

    @pytest.mark.asyncio
    async def test_handle_failure_routes_to_retry(
        self, mock_config, tmp_path, sample_download_task
    ):
        """Worker routes transient failures to retry handler."""
        with patch("pipeline.verisk.workers.download_worker.create_producer"):
            worker = DownloadWorker(config=mock_config, temp_dir=tmp_path)
            worker.producer = AsyncMock()
            worker.retry_handler = AsyncMock()
            worker.retry_handler.handle_failure = AsyncMock()
            worker._consumer_group = "verisk-download-worker"

            outcome = DownloadOutcome(
                success=False,
                error_message="Connection timeout",
                error_category=ErrorCategory.TRANSIENT,
                status_code=None,
            )

            # Mock record_processing_error to avoid parameter issues
            with patch("pipeline.verisk.workers.download_worker.record_processing_error"):
                await worker._handle_failure(sample_download_task, outcome, processing_time_ms=100)

                # Verify retry handler was called
                assert worker.retry_handler.handle_failure.called
                call_args = worker.retry_handler.handle_failure.call_args
                assert call_args.kwargs["error_category"] == ErrorCategory.TRANSIENT

                # Verify result message was produced
                assert worker.producer.send.called
                call_args = worker.producer.send.call_args
                result_msg = call_args.kwargs["value"]
                assert isinstance(result_msg, DownloadResultMessage)
                assert result_msg.status == "failed"


class TestDownloadWorkerInFlightTracking:
    """Test in-flight task tracking."""

    @pytest.mark.asyncio
    async def test_in_flight_tasks_tracked(self, mock_config, tmp_path, sample_message):
        """Worker tracks in-flight tasks correctly."""
        with patch("pipeline.verisk.workers.download_worker.create_producer"):
            worker = DownloadWorker(config=mock_config, temp_dir=tmp_path)
            worker.producer = AsyncMock()
            worker._semaphore = AsyncMock()
            worker._semaphore.__aenter__ = AsyncMock()
            worker._semaphore.__aexit__ = AsyncMock()
            worker._consumer_group = "verisk-download-worker"

            # Track in-flight during processing
            in_flight_during = []

            async def mock_download(task):
                in_flight_during.append(len(worker._in_flight_tasks))
                return DownloadOutcome(
                    success=True,
                    file_path=tmp_path / "file.pdf",
                    bytes_downloaded=1024,
                    content_type="application/pdf",
                    status_code=200,
                )

            worker.downloader = AsyncMock()
            worker.downloader.download = mock_download

            # Mock file operations
            with patch("asyncio.to_thread", new_callable=AsyncMock):
                await worker._process_single_task(sample_message)

                # Verify task was tracked during processing
                assert 1 in in_flight_during
                # Verify task was removed after processing
                assert len(worker._in_flight_tasks) == 0
