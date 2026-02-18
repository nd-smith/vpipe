"""
Unit tests for Verisk Upload Worker.

Test Coverage:
    - Worker initialization and configuration
    - Lifecycle management (start/stop/graceful shutdown)
    - Message parsing and validation
    - Batch processing with concurrency
    - File upload to OneLake
    - Multi-domain routing
    - Success handling (DownloadResultMessage production, cache cleanup)
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
from pipeline.common.types import PipelineMessage
from pipeline.verisk.schemas.cached import CachedDownloadMessage
from pipeline.verisk.schemas.results import DownloadResultMessage
from pipeline.verisk.workers.upload_worker import UploadWorker


@pytest.fixture
def mock_config():
    """Mock MessageConfig with standard settings."""
    config = Mock(spec=MessageConfig)
    config.get_topic.return_value = "verisk.downloads.cached"
    config.get_consumer_group.return_value = "verisk-upload-worker"
    config.onelake_domain_paths = {"verisk": "/onelake/verisk"}
    config.onelake_base_path = "/onelake/base"
    config.cache_dir = "/tmp/cache"
    return config


@pytest.fixture
def mock_storage_client():
    """Mock OneLake storage client."""
    client = AsyncMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock()
    client.async_upload_file = AsyncMock(return_value="verisk/A12345/documentsReceived/file.pdf")
    client.close = AsyncMock()
    client.base_path = "/onelake/verisk"
    return client


@pytest.fixture
def sample_cached_message():
    """Sample CachedDownloadMessage for testing."""
    return CachedDownloadMessage(
        media_id="media-123",
        trace_id="trace-456",
        attachment_url="https://example.com/file.pdf",
        destination_path="verisk/A12345/documentsReceived/file.pdf",
        local_cache_path="/tmp/cache/trace-456/file.pdf",
        bytes_downloaded=1024,
        content_type="application/pdf",
        event_type="xact",
        event_subtype="documentsReceived",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="A12345",
        original_timestamp=datetime.now(UTC),
        downloaded_at=datetime.now(UTC),
    )


@pytest.fixture
def sample_message(sample_cached_message):
    """Sample Kafka message with cached download."""
    return PipelineMessage(
        topic="verisk.downloads.cached",
        partition=0,
        offset=1,
        key=b"trace-456",
        value=sample_cached_message.model_dump_json().encode(),
        timestamp=None,
        headers=None,
    )


class TestUploadWorkerInitialization:
    """Test worker initialization and configuration."""

    def test_initialization_with_default_config(self, mock_config, mock_storage_client):
        """Worker initializes with default domain and config."""
        with patch("pipeline.verisk.workers.upload_worker.create_producer"):
            worker = UploadWorker(config=mock_config, storage_client=mock_storage_client)

            assert worker.domain == "verisk"
            assert worker.config is mock_config
            assert worker.worker_id == "upload_worker"
            assert worker.instance_id is None
            assert worker._running is False

    def test_initialization_with_custom_domain(self, mock_config, mock_storage_client):
        """Worker initializes with custom domain."""
        with patch("pipeline.verisk.workers.upload_worker.create_producer"):
            worker = UploadWorker(
                config=mock_config, domain="custom", storage_client=mock_storage_client
            )

            assert worker.domain == "custom"

    def test_initialization_with_instance_id(self, mock_config, mock_storage_client):
        """Worker uses instance ID for worker_id suffix."""
        with patch("pipeline.verisk.workers.upload_worker.create_producer"):
            worker = UploadWorker(
                config=mock_config,
                instance_id="happy-tiger",
                storage_client=mock_storage_client,
            )

            assert worker.worker_id == "upload_worker-happy-tiger"
            assert worker.instance_id == "happy-tiger"

    def test_initialization_uses_default_concurrency(self, mock_config, mock_storage_client):
        """Worker uses default concurrency and batch_size."""
        with patch("pipeline.verisk.workers.upload_worker.create_producer"):
            worker = UploadWorker(config=mock_config, storage_client=mock_storage_client)

            assert worker.concurrency == 25
            assert worker.batch_size == 200

    def test_initialization_sets_topics(self, mock_config, mock_storage_client):
        """Worker sets correct consumer topics."""
        with patch("pipeline.verisk.workers.upload_worker.create_producer"):
            worker = UploadWorker(config=mock_config, storage_client=mock_storage_client)

            assert worker.topics == ["verisk.downloads.cached"]
            assert worker.results_topic == "verisk.downloads.cached"

    def test_metrics_initialized_to_zero(self, mock_config, mock_storage_client):
        """Worker initializes metrics to zero."""
        with patch("pipeline.verisk.workers.upload_worker.create_producer"):
            worker = UploadWorker(config=mock_config, storage_client=mock_storage_client)

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
            patch("pipeline.verisk.workers.upload_worker.create_producer"),
            pytest.raises(ValueError, match="OneLake path configuration required"),
        ):
            UploadWorker(config=config)

    def test_initialization_accepts_injected_storage_client(self, mock_config, mock_storage_client):
        """Worker accepts injected storage client."""
        with patch("pipeline.verisk.workers.upload_worker.create_producer"):
            worker = UploadWorker(config=mock_config, storage_client=mock_storage_client)

            assert worker._injected_storage_client is mock_storage_client


class TestUploadWorkerLifecycle:
    """Test worker lifecycle (start/stop)."""

    @pytest.mark.asyncio
    async def test_start_initializes_semaphore_and_shutdown_event(
        self, mock_config, mock_storage_client
    ):
        """Worker start initializes concurrency controls."""
        with patch("pipeline.verisk.workers.upload_worker.create_producer"):
            worker = UploadWorker(config=mock_config, storage_client=mock_storage_client)

            with (
                patch(
                    "pipeline.verisk.workers.upload_worker.create_batch_consumer"
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
                assert worker._semaphore._value == 25  # CONCURRENCY default

    @pytest.mark.asyncio
    async def test_stop_cleans_up_resources(self, mock_config, mock_storage_client):
        """Worker stop cleans up all resources."""
        with patch("pipeline.verisk.workers.upload_worker.create_producer"):
            worker = UploadWorker(config=mock_config, storage_client=mock_storage_client)

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
            worker.onelake_clients = {"verisk": mock_storage_client}
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
        with patch("pipeline.verisk.workers.upload_worker.create_producer"):
            worker = UploadWorker(config=mock_config, storage_client=mock_storage_client)

            # All components are None (not initialized)
            assert worker._consumer is None
            assert worker.onelake_clients == {}

            # Should not raise
            await worker.stop()

    @pytest.mark.asyncio
    async def test_request_shutdown_sets_running_false(self, mock_config, mock_storage_client):
        """Request shutdown sets running flag to false."""
        with patch("pipeline.verisk.workers.upload_worker.create_producer"):
            worker = UploadWorker(config=mock_config, storage_client=mock_storage_client)
            worker._running = True

            await worker.request_shutdown()

            assert worker._running is False


class TestUploadWorkerMessageProcessing:
    """Test message parsing and processing."""

    @pytest.mark.asyncio
    async def test_valid_message_parsed_successfully(
        self, mock_config, mock_storage_client, sample_cached_message, tmp_path
    ):
        """Worker parses valid cached download message."""
        with patch("pipeline.verisk.workers.upload_worker.create_producer"):
            worker = UploadWorker(config=mock_config, storage_client=mock_storage_client)
            worker.producer = AsyncMock()
            worker.onelake_clients = {"xact": mock_storage_client}

            # Create cache file
            cache_path = tmp_path / "cache" / "file.pdf"
            cache_path.parent.mkdir(parents=True)
            cache_path.write_text("test content")

            # Create message with test cache path
            cached_msg = sample_cached_message.model_copy(
                update={"local_cache_path": str(cache_path)}
            )
            message = PipelineMessage(
                topic="verisk.downloads.cached",
                partition=0,
                offset=1,
                key=b"trace-456",
                value=cached_msg.model_dump_json().encode(),
                timestamp=None,
                headers=None,
            )

            # Mock file cleanup
            with patch("asyncio.to_thread", new_callable=AsyncMock):
                result = await worker._process_single_upload(message)

                # Verify message was processed
                assert result.success is True
                assert result.cached_message.trace_id == "trace-456"
                assert worker._records_processed == 1

    @pytest.mark.asyncio
    async def test_invalid_json_returns_error_result(self, mock_config, mock_storage_client):
        """Worker returns error result on invalid JSON."""
        with patch("pipeline.verisk.workers.upload_worker.create_producer"):
            worker = UploadWorker(config=mock_config, storage_client=mock_storage_client)
            worker.producer = AsyncMock()

            invalid_message = PipelineMessage(
                topic="verisk.downloads.cached",
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


class TestUploadWorkerBatchProcessing:
    """Test batch processing with concurrency."""

    @pytest.mark.asyncio
    async def test_process_batch_executes_concurrently(
        self, mock_config, mock_storage_client, sample_cached_message, tmp_path
    ):
        """Worker processes batch of messages concurrently."""
        with patch("pipeline.verisk.workers.upload_worker.create_producer"):
            worker = UploadWorker(config=mock_config, storage_client=mock_storage_client)
            worker.producer = AsyncMock()
            worker.onelake_clients = {"xact": mock_storage_client}
            worker._semaphore = AsyncMock()
            worker._semaphore.__aenter__ = AsyncMock()
            worker._semaphore.__aexit__ = AsyncMock()
            worker._consumer = AsyncMock()  # Required by _process_batch

            # Create cache file
            cache_path = tmp_path / "cache" / "file.pdf"
            cache_path.parent.mkdir(parents=True)
            cache_path.write_text("test content")

            # Create message with test cache path
            cached_msg = sample_cached_message.model_copy(
                update={"local_cache_path": str(cache_path)}
            )
            message = PipelineMessage(
                topic="verisk.downloads.cached",
                partition=0,
                offset=1,
                key=b"trace-456",
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
    async def test_process_batch_commits_even_with_failures(
        self, mock_config, mock_storage_client, sample_message
    ):
        """Worker commits offsets even when batch has failures (unlike ClaimX)."""
        with patch("pipeline.verisk.workers.upload_worker.create_producer"):
            worker = UploadWorker(config=mock_config, storage_client=mock_storage_client)
            worker.producer = AsyncMock()
            worker.onelake_clients = {"xact": mock_storage_client}
            worker._semaphore = AsyncMock()
            worker._semaphore.__aenter__ = AsyncMock()
            worker._semaphore.__aexit__ = AsyncMock()
            worker._consumer = AsyncMock()

            # Mock upload failure (file not found)
            commit = await worker._process_batch([sample_message])

            # Verify batch was committed (Verisk behavior differs from ClaimX)
            assert commit is True
            assert worker._records_failed == 1


class TestUploadWorkerSuccessHandling:
    """Test success handling and result production."""

    @pytest.mark.asyncio
    async def test_handle_success_produces_result_message(
        self, mock_config, mock_storage_client, sample_cached_message, tmp_path
    ):
        """Worker produces DownloadResultMessage on success."""
        with patch("pipeline.verisk.workers.upload_worker.create_producer"):
            worker = UploadWorker(config=mock_config, storage_client=mock_storage_client)
            worker.producer = AsyncMock()
            worker.onelake_clients = {"xact": mock_storage_client}

            # Create cache file
            cache_path = tmp_path / "cache" / "file.pdf"
            cache_path.parent.mkdir(parents=True)
            cache_path.write_text("test content")

            # Create message with test cache path
            cached_msg = sample_cached_message.model_copy(
                update={"local_cache_path": str(cache_path)}
            )
            message = PipelineMessage(
                topic="verisk.downloads.cached",
                partition=0,
                offset=1,
                key=b"trace-456",
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
                assert isinstance(result_msg, DownloadResultMessage)
                assert result_msg.status == "completed"
                assert result_msg.trace_id == "trace-456"
                assert call_args.kwargs["key"] == "trace-456"

    @pytest.mark.asyncio
    async def test_handle_success_cleans_up_cache_file(
        self, mock_config, mock_storage_client, sample_cached_message, tmp_path
    ):
        """Worker cleans up cache file after successful upload."""
        with patch("pipeline.verisk.workers.upload_worker.create_producer"):
            worker = UploadWorker(config=mock_config, storage_client=mock_storage_client)
            worker.producer = AsyncMock()
            worker.onelake_clients = {"xact": mock_storage_client}

            # Create cache file
            cache_path = tmp_path / "cache" / "file.pdf"
            cache_path.parent.mkdir(parents=True)
            cache_path.write_text("test content")

            # Create message with test cache path
            cached_msg = sample_cached_message.model_copy(
                update={"local_cache_path": str(cache_path)}
            )
            message = PipelineMessage(
                topic="verisk.downloads.cached",
                partition=0,
                offset=1,
                key=b"trace-456",
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


class TestUploadWorkerFailureHandling:
    """Test failure handling and error result production."""

    @pytest.mark.asyncio
    async def test_handle_failure_produces_error_result(
        self, mock_config, mock_storage_client, sample_message
    ):
        """Worker produces failure result message on upload error."""
        with patch("pipeline.verisk.workers.upload_worker.create_producer"):
            worker = UploadWorker(config=mock_config, storage_client=mock_storage_client)
            worker.producer = AsyncMock()
            worker.onelake_clients = {"xact": mock_storage_client}

            # Mock record_processing_error to avoid parameter issues
            with patch("pipeline.verisk.workers.upload_worker.record_processing_error"):
                # File doesn't exist - will fail
                result = await worker._process_single_upload(sample_message)

                # Verify error result was produced
                assert result.success is False
                assert worker.producer.send.called
                call_args = worker.producer.send.call_args
                result_msg = call_args.kwargs["value"]
                assert isinstance(result_msg, DownloadResultMessage)
                assert result_msg.status == "failed_permanent"
                assert result_msg.bytes_downloaded == 0

    @pytest.mark.asyncio
    async def test_handle_failure_does_not_clean_up_cache(
        self, mock_config, mock_storage_client, sample_cached_message, tmp_path
    ):
        """Worker retains cache file on upload failure for manual review."""
        with patch("pipeline.verisk.workers.upload_worker.create_producer"):
            worker = UploadWorker(config=mock_config, storage_client=mock_storage_client)
            worker.producer = AsyncMock()
            worker.onelake_clients = {"xact": mock_storage_client}

            # Create cache file
            cache_path = tmp_path / "cache" / "file.pdf"
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
                topic="verisk.downloads.cached",
                partition=0,
                offset=1,
                key=b"trace-456",
                value=cached_msg.model_dump_json().encode(),
                timestamp=None,
                headers=None,
            )

            # Mock record_processing_error
            with patch("pipeline.verisk.workers.upload_worker.record_processing_error"):
                result = await worker._process_single_upload(message)

                # Verify upload failed but cache file still exists
                assert result.success is False
                assert cache_path.exists()


class TestUploadWorkerInFlightTracking:
    """Test in-flight task tracking."""

    @pytest.mark.asyncio
    async def test_in_flight_tasks_tracked(
        self, mock_config, mock_storage_client, sample_cached_message, tmp_path
    ):
        """Worker tracks in-flight tasks correctly."""
        with patch("pipeline.verisk.workers.upload_worker.create_producer"):
            worker = UploadWorker(config=mock_config, storage_client=mock_storage_client)
            worker.producer = AsyncMock()
            worker.onelake_clients = {"xact": mock_storage_client}

            # Create cache file
            cache_path = tmp_path / "cache" / "file.pdf"
            cache_path.parent.mkdir(parents=True)
            cache_path.write_text("test content")

            # Create message with test cache path
            cached_msg = sample_cached_message.model_copy(
                update={"local_cache_path": str(cache_path)}
            )
            message = PipelineMessage(
                topic="verisk.downloads.cached",
                partition=0,
                offset=1,
                key=b"trace-456",
                value=cached_msg.model_dump_json().encode(),
                timestamp=None,
                headers=None,
            )

            # Track in-flight during processing
            in_flight_during = []

            async def mock_upload(relative_path, local_path, overwrite):
                in_flight_during.append(len(worker._in_flight_tasks))
                return "verisk/A12345/documentsReceived/file.pdf"

            mock_storage_client.async_upload_file = mock_upload

            # Mock file cleanup
            with patch("asyncio.to_thread", new_callable=AsyncMock):
                await worker._process_single_upload(message)

                # Verify task was tracked during processing
                assert 1 in in_flight_during
                # Verify task was removed after processing
                assert len(worker._in_flight_tasks) == 0
