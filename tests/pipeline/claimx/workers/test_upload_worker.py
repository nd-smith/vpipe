"""
Unit tests for ClaimX Upload Worker.

Test Coverage:
    - Worker initialization and configuration
    - Lifecycle management (start/stop/graceful shutdown)
    - Message parsing and validation
    - Batch processing with concurrency
    - File upload to OneLake
    - Success handling (ClaimXUploadResultMessage production, cache cleanup)
    - Failure handling (error result production, cache retention)
    - In-flight task tracking
    - OneLake client management

No infrastructure required - all dependencies mocked.
"""

import contextlib
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest

from config.config import MessageConfig
from pipeline.claimx.schemas.cached import ClaimXCachedDownloadMessage
from pipeline.claimx.schemas.results import ClaimXUploadResultMessage
from pipeline.claimx.workers.upload_worker import ClaimXUploadWorker
from pipeline.common.types import PipelineMessage


@pytest.fixture
def mock_config():
    """Mock MessageConfig with standard settings."""
    config = Mock(spec=MessageConfig)
    config.get_topic.return_value = "claimx.downloads.cached"
    config.get_consumer_group.return_value = "claimx-upload-worker"

    # get_worker_config is called with (domain, worker_name, "processing")
    def mock_get_worker_config(domain, worker_name, config_key=None):
        if config_key == "processing":
            return {
                "concurrency": 10,
                "batch_size": 20,
                "health_port": 8083,
            }
        return {"health_port": 8083}

    config.get_worker_config = Mock(side_effect=mock_get_worker_config)
    config.onelake_domain_paths = {"claimx": "/onelake/claimx"}
    config.onelake_base_path = "/onelake/base"
    config.cache_dir = "/tmp/cache"
    return config


@pytest.fixture
def mock_storage_client():
    """Mock OneLake storage client."""
    client = AsyncMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock()
    client.async_upload_file = AsyncMock(return_value="claimx/proj-456/media/file.jpg")
    client.close = Mock()
    return client


@pytest.fixture
def sample_cached_message():
    """Sample ClaimXCachedDownloadMessage for testing."""
    return ClaimXCachedDownloadMessage(
        media_id="media-123",
        project_id="proj-456",
        download_url="https://s3.amazonaws.com/claimx/file.jpg",
        destination_path="claimx/proj-456/media/file.jpg",
        local_cache_path="/tmp/cache/trace-789/file.jpg",
        bytes_downloaded=1024,
        content_type="image/jpeg",
        file_type="jpg",
        file_name="file.jpg",
        source_event_id="evt-abc",
        downloaded_at=datetime.now(UTC),
    )


@pytest.fixture
def sample_message(sample_cached_message):
    """Sample Kafka message with cached download."""
    return PipelineMessage(
        topic="claimx.downloads.cached",
        partition=0,
        offset=1,
        key=b"evt-abc",
        value=sample_cached_message.model_dump_json().encode(),
        timestamp=None,
        headers=None,
    )


class TestClaimXUploadWorkerInitialization:
    """Test worker initialization and configuration."""

    def test_initialization_with_default_config(self, mock_config, mock_storage_client):
        """Worker initializes with default domain and config."""
        with patch("pipeline.claimx.workers.upload_worker.create_producer"):
            worker = ClaimXUploadWorker(config=mock_config, storage_client=mock_storage_client)

            assert worker.domain == "claimx"
            assert worker.config is mock_config
            assert worker.worker_id == "upload_worker"
            assert worker.instance_id is None
            assert worker._running is False

    def test_initialization_with_custom_domain(self, mock_config, mock_storage_client):
        """Worker initializes with custom domain."""
        with patch("pipeline.claimx.workers.upload_worker.create_producer"):
            worker = ClaimXUploadWorker(
                config=mock_config, domain="custom", storage_client=mock_storage_client
            )

            assert worker.domain == "custom"

    def test_initialization_with_instance_id(self, mock_config, mock_storage_client):
        """Worker uses instance ID for worker_id suffix."""
        with patch("pipeline.claimx.workers.upload_worker.create_producer"):
            worker = ClaimXUploadWorker(
                config=mock_config,
                instance_id="happy-tiger",
                storage_client=mock_storage_client,
            )

            assert worker.worker_id == "upload_worker-happy-tiger"
            assert worker.instance_id == "happy-tiger"

    def test_initialization_loads_concurrency_from_config(self, mock_config, mock_storage_client):
        """Worker loads concurrency settings from config."""
        with patch("pipeline.claimx.workers.upload_worker.create_producer"):
            worker = ClaimXUploadWorker(config=mock_config, storage_client=mock_storage_client)

            assert worker.concurrency == 10
            assert worker.batch_size == 20

    def test_initialization_sets_topics(self, mock_config, mock_storage_client):
        """Worker sets correct consumer topics."""
        with patch("pipeline.claimx.workers.upload_worker.create_producer"):
            worker = ClaimXUploadWorker(config=mock_config, storage_client=mock_storage_client)

            assert worker.topics == ["claimx.downloads.cached"]
            assert worker.results_topic == "claimx.downloads.cached"

    def test_metrics_initialized_to_zero(self, mock_config, mock_storage_client):
        """Worker initializes metrics to zero."""
        with patch("pipeline.claimx.workers.upload_worker.create_producer"):
            worker = ClaimXUploadWorker(config=mock_config, storage_client=mock_storage_client)

            assert worker._records_processed == 0
            assert worker._records_succeeded == 0
            assert worker._records_failed == 0
            assert worker._bytes_uploaded == 0

    def test_initialization_requires_onelake_config(self):
        """Worker requires OneLake configuration when no storage client injected."""
        config = Mock(spec=MessageConfig)
        config.onelake_domain_paths = {}
        config.onelake_base_path = None

        with (
            patch("pipeline.claimx.workers.upload_worker.create_producer"),
            pytest.raises(ValueError, match="OneLake path configuration required"),
        ):
            ClaimXUploadWorker(config=config)

    def test_initialization_accepts_injected_storage_client(self, mock_config, mock_storage_client):
        """Worker accepts injected storage client."""
        with patch("pipeline.claimx.workers.upload_worker.create_producer"):
            worker = ClaimXUploadWorker(config=mock_config, storage_client=mock_storage_client)

            assert worker._injected_storage_client is mock_storage_client


class TestClaimXUploadWorkerLifecycle:
    """Test worker lifecycle (start/stop)."""

    @pytest.mark.asyncio
    async def test_start_initializes_semaphore_and_shutdown_event(
        self, mock_config, mock_storage_client
    ):
        """Worker start initializes concurrency controls."""
        with patch("pipeline.claimx.workers.upload_worker.create_producer"):
            worker = ClaimXUploadWorker(config=mock_config, storage_client=mock_storage_client)

            with (
                patch(
                    "pipeline.claimx.workers.upload_worker.create_batch_consumer"
                ) as mock_create_consumer,
                patch("pipeline.common.telemetry.initialize_worker_telemetry"),
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
    async def test_stop_cleans_up_resources(self, mock_config, mock_storage_client):
        """Worker stop cleans up all resources."""
        with patch("pipeline.claimx.workers.upload_worker.create_producer"):
            worker = ClaimXUploadWorker(config=mock_config, storage_client=mock_storage_client)

            # Setup mocked components
            mock_consumer = AsyncMock()
            mock_consumer.stop = AsyncMock()
            mock_producer = AsyncMock()
            mock_producer.stop = AsyncMock()
            mock_stats_logger = AsyncMock()
            mock_stats_logger.stop = AsyncMock()
            mock_health_server = AsyncMock()
            mock_health_server.stop = AsyncMock()

            worker._consumer = mock_consumer
            worker.producer = mock_producer
            worker.onelake_client = mock_storage_client
            worker._stats_logger = mock_stats_logger
            worker.health_server = mock_health_server
            worker._running = True

            # Stop worker
            await worker.stop()

            # Verify cleanup
            assert worker._running is False
            mock_consumer.stop.assert_called_once()
            mock_producer.stop.assert_called_once()
            mock_storage_client.close.assert_called_once()
            mock_stats_logger.stop.assert_called_once()
            mock_health_server.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_handles_none_components(self, mock_config, mock_storage_client):
        """Worker stop handles None components gracefully."""
        with patch("pipeline.claimx.workers.upload_worker.create_producer"):
            worker = ClaimXUploadWorker(config=mock_config, storage_client=mock_storage_client)

            # All components are None (not initialized)
            assert worker._consumer is None
            assert worker.onelake_client is None

            # Should not raise
            await worker.stop()

    @pytest.mark.asyncio
    async def test_request_shutdown_sets_running_false(self, mock_config, mock_storage_client):
        """Request shutdown sets running flag to false."""
        with patch("pipeline.claimx.workers.upload_worker.create_producer"):
            worker = ClaimXUploadWorker(config=mock_config, storage_client=mock_storage_client)
            worker._running = True

            await worker.request_shutdown()

            assert worker._running is False


class TestClaimXUploadWorkerMessageProcessing:
    """Test message parsing and processing."""

    @pytest.mark.asyncio
    async def test_valid_message_parsed_successfully(
        self, mock_config, mock_storage_client, sample_cached_message, tmp_path
    ):
        """Worker parses valid cached download message."""
        with patch("pipeline.claimx.workers.upload_worker.create_producer"):
            worker = ClaimXUploadWorker(config=mock_config, storage_client=mock_storage_client)
            worker.producer = AsyncMock()
            worker.onelake_client = mock_storage_client

            # Create cache file
            cache_path = tmp_path / "cache" / "file.jpg"
            cache_path.parent.mkdir(parents=True)
            cache_path.write_text("test content")

            # Create message with test cache path
            cached_msg = sample_cached_message.model_copy(
                update={"local_cache_path": str(cache_path)}
            )
            message = PipelineMessage(
                topic="claimx.downloads.cached",
                partition=0,
                offset=1,
                key=b"evt-abc",
                value=cached_msg.model_dump_json().encode(),
                timestamp=None,
                headers=None,
            )

            # Mock file cleanup
            with patch("asyncio.to_thread", new_callable=AsyncMock):
                result = await worker._process_single_upload(message)

                # Verify message was processed
                assert result.success is True
                assert result.cached_message.media_id == "media-123"
                assert worker._records_processed == 1

    @pytest.mark.asyncio
    async def test_invalid_json_returns_error_result(self, mock_config, mock_storage_client):
        """Worker returns error result on invalid JSON."""
        with patch("pipeline.claimx.workers.upload_worker.create_producer"):
            worker = ClaimXUploadWorker(config=mock_config, storage_client=mock_storage_client)
            worker.producer = AsyncMock()

            invalid_message = PipelineMessage(
                topic="claimx.downloads.cached",
                partition=0,
                offset=1,
                key=b"key",
                value=b"invalid json{",
                timestamp=None,
                headers=None,
            )

            result = await worker._process_single_upload(invalid_message)

            # Verify error result
            assert result.success is False
            assert result.error is not None


class TestClaimXUploadWorkerBatchProcessing:
    """Test batch processing with concurrency."""

    @pytest.mark.asyncio
    async def test_process_batch_executes_concurrently(
        self, mock_config, mock_storage_client, sample_cached_message, tmp_path
    ):
        """Worker processes batch of messages concurrently."""
        with patch("pipeline.claimx.workers.upload_worker.create_producer"):
            worker = ClaimXUploadWorker(config=mock_config, storage_client=mock_storage_client)
            worker.producer = AsyncMock()
            worker.onelake_client = mock_storage_client
            worker._semaphore = AsyncMock()
            worker._semaphore.__aenter__ = AsyncMock()
            worker._semaphore.__aexit__ = AsyncMock()
            worker._consumer = AsyncMock()  # Required by _process_batch

            # Create cache file
            cache_path = tmp_path / "cache" / "file.jpg"
            cache_path.parent.mkdir(parents=True)
            cache_path.write_text("test content")

            # Create message with test cache path
            cached_msg = sample_cached_message.model_copy(
                update={"local_cache_path": str(cache_path)}
            )
            message = PipelineMessage(
                topic="claimx.downloads.cached",
                partition=0,
                offset=1,
                key=b"evt-abc",
                value=cached_msg.model_dump_json().encode(),
                timestamp=None,
                headers=None,
            )

            # Mock file cleanup
            with patch("asyncio.to_thread", new_callable=AsyncMock):
                commit = await worker._process_batch([message] * 3)

                # Verify batch was processed
                assert commit is True
                assert worker._records_succeeded == 3

    @pytest.mark.asyncio
    async def test_process_batch_does_not_commit_on_failure(
        self, mock_config, mock_storage_client, sample_message
    ):
        """Worker does not commit offsets when batch has failures."""
        with patch("pipeline.claimx.workers.upload_worker.create_producer"):
            worker = ClaimXUploadWorker(config=mock_config, storage_client=mock_storage_client)
            worker.producer = AsyncMock()
            worker.onelake_client = mock_storage_client
            worker._semaphore = AsyncMock()
            worker._semaphore.__aenter__ = AsyncMock()
            worker._semaphore.__aexit__ = AsyncMock()
            worker._consumer = AsyncMock()

            # Mock upload failure (file not found)
            commit = await worker._process_batch([sample_message])

            # Verify batch was not committed
            assert commit is False
            assert worker._records_failed == 1


class TestClaimXUploadWorkerSuccessHandling:
    """Test success handling and result production."""

    @pytest.mark.asyncio
    async def test_handle_success_produces_result_message(
        self, mock_config, mock_storage_client, sample_cached_message, tmp_path
    ):
        """Worker produces ClaimXUploadResultMessage on success."""
        with patch("pipeline.claimx.workers.upload_worker.create_producer"):
            worker = ClaimXUploadWorker(config=mock_config, storage_client=mock_storage_client)
            worker.producer = AsyncMock()
            worker.onelake_client = mock_storage_client

            # Create cache file
            cache_path = tmp_path / "cache" / "file.jpg"
            cache_path.parent.mkdir(parents=True)
            cache_path.write_text("test content")

            # Create message with test cache path
            cached_msg = sample_cached_message.model_copy(
                update={"local_cache_path": str(cache_path)}
            )
            message = PipelineMessage(
                topic="claimx.downloads.cached",
                partition=0,
                offset=1,
                key=b"evt-abc",
                value=cached_msg.model_dump_json().encode(),
                timestamp=None,
                headers=None,
            )

            # Mock file cleanup
            with patch("asyncio.to_thread", new_callable=AsyncMock):
                await worker._process_single_upload(message)

                # Verify result message was produced
                assert worker.producer.send.called
                call_args = worker.producer.send.call_args
                result_msg = call_args.kwargs["value"]
                assert isinstance(result_msg, ClaimXUploadResultMessage)
                assert result_msg.status == "completed"
                assert result_msg.media_id == "media-123"
                assert call_args.kwargs["key"] == "evt-abc"

    @pytest.mark.asyncio
    async def test_handle_success_cleans_up_cache_file(
        self, mock_config, mock_storage_client, sample_cached_message, tmp_path
    ):
        """Worker cleans up cache file after successful upload."""
        with patch("pipeline.claimx.workers.upload_worker.create_producer"):
            worker = ClaimXUploadWorker(config=mock_config, storage_client=mock_storage_client)
            worker.producer = AsyncMock()
            worker.onelake_client = mock_storage_client

            # Create cache file
            cache_path = tmp_path / "cache" / "file.jpg"
            cache_path.parent.mkdir(parents=True)
            cache_path.write_text("test content")

            # Create message with test cache path
            cached_msg = sample_cached_message.model_copy(
                update={"local_cache_path": str(cache_path)}
            )
            message = PipelineMessage(
                topic="claimx.downloads.cached",
                partition=0,
                offset=1,
                key=b"evt-abc",
                value=cached_msg.model_dump_json().encode(),
                timestamp=None,
                headers=None,
            )

            # Mock file cleanup
            mock_cleanup = AsyncMock()
            with patch.object(worker, "_cleanup_cache_file", mock_cleanup):
                await worker._process_single_upload(message)

                # Verify cleanup was called
                assert mock_cleanup.called
                call_args = mock_cleanup.call_args
                assert call_args[0][0] == cache_path


class TestClaimXUploadWorkerFailureHandling:
    """Test failure handling and error result production."""

    @pytest.mark.asyncio
    async def test_handle_failure_produces_error_result(
        self, mock_config, mock_storage_client, sample_message
    ):
        """Worker produces failure result message on upload error."""
        with patch("pipeline.claimx.workers.upload_worker.create_producer"):
            worker = ClaimXUploadWorker(config=mock_config, storage_client=mock_storage_client)
            worker.producer = AsyncMock()
            worker.onelake_client = mock_storage_client

            # Mock record_processing_error to avoid parameter issues
            with patch("pipeline.claimx.workers.upload_worker.record_processing_error"):
                # File doesn't exist - will fail
                result = await worker._process_single_upload(sample_message)

                # Verify error result was produced
                assert result.success is False
                assert worker.producer.send.called
                call_args = worker.producer.send.call_args
                result_msg = call_args.kwargs["value"]
                assert isinstance(result_msg, ClaimXUploadResultMessage)
                assert result_msg.status == "failed_permanent"
                assert result_msg.bytes_uploaded == 0

    @pytest.mark.asyncio
    async def test_handle_failure_does_not_clean_up_cache(
        self, mock_config, mock_storage_client, sample_cached_message, tmp_path
    ):
        """Worker retains cache file on upload failure for manual review."""
        with patch("pipeline.claimx.workers.upload_worker.create_producer"):
            worker = ClaimXUploadWorker(config=mock_config, storage_client=mock_storage_client)
            worker.producer = AsyncMock()
            worker.onelake_client = mock_storage_client

            # Create cache file
            cache_path = tmp_path / "cache" / "file.jpg"
            cache_path.parent.mkdir(parents=True)
            cache_path.write_text("test content")

            # Create message with test cache path
            cached_msg = sample_cached_message.model_copy(
                update={"local_cache_path": str(cache_path)}
            )

            # Force upload failure
            mock_storage_client.async_upload_file = AsyncMock(
                side_effect=Exception("Upload failed")
            )

            message = PipelineMessage(
                topic="claimx.downloads.cached",
                partition=0,
                offset=1,
                key=b"evt-abc",
                value=cached_msg.model_dump_json().encode(),
                timestamp=None,
                headers=None,
            )

            # Mock record_processing_error
            with patch("pipeline.claimx.workers.upload_worker.record_processing_error"):
                result = await worker._process_single_upload(message)

                # Verify upload failed but cache file still exists
                assert result.success is False
                assert cache_path.exists()


class TestClaimXUploadWorkerInFlightTracking:
    """Test in-flight task tracking."""

    @pytest.mark.asyncio
    async def test_in_flight_tasks_tracked(
        self, mock_config, mock_storage_client, sample_cached_message, tmp_path
    ):
        """Worker tracks in-flight tasks correctly."""
        with patch("pipeline.claimx.workers.upload_worker.create_producer"):
            worker = ClaimXUploadWorker(config=mock_config, storage_client=mock_storage_client)
            worker.producer = AsyncMock()
            worker.onelake_client = mock_storage_client

            # Create cache file
            cache_path = tmp_path / "cache" / "file.jpg"
            cache_path.parent.mkdir(parents=True)
            cache_path.write_text("test content")

            # Create message with test cache path
            cached_msg = sample_cached_message.model_copy(
                update={"local_cache_path": str(cache_path)}
            )
            message = PipelineMessage(
                topic="claimx.downloads.cached",
                partition=0,
                offset=1,
                key=b"evt-abc",
                value=cached_msg.model_dump_json().encode(),
                timestamp=None,
                headers=None,
            )

            # Track in-flight during processing
            in_flight_during = []

            async def mock_upload(relative_path, local_path, overwrite):
                in_flight_during.append(len(worker._in_flight_tasks))
                return "claimx/proj-456/media/file.jpg"

            mock_storage_client.async_upload_file = mock_upload

            # Mock file cleanup
            with patch("asyncio.to_thread", new_callable=AsyncMock):
                await worker._process_single_upload(message)

                # Verify task was tracked during processing
                assert 1 in in_flight_during
                # Verify task was removed after processing
                assert len(worker._in_flight_tasks) == 0
