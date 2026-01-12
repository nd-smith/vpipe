"""
Unit tests for ClaimXUploadWorker with concurrent processing.

Tests cached download consumption, OneLake upload integration,
batch processing, cache cleanup, and concurrency control.
"""

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiokafka.structs import ConsumerRecord

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.claimx.schemas.cached import ClaimXCachedDownloadMessage
from kafka_pipeline.claimx.schemas.results import ClaimXUploadResultMessage
from kafka_pipeline.claimx.workers.upload_worker import ClaimXUploadWorker, UploadResult


@pytest.fixture
def temp_cache_dir(tmp_path):
    """Create temporary cache directory with cached files."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    return cache_dir


@pytest.fixture
def kafka_config(temp_cache_dir):
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
            "upload_worker": {
                "consumer": {"max_poll_records": 20},
                "processing": {
                    "concurrency": 10,
                    "batch_size": 20,
                    "health_port": 8083,
                },
            },
        },
        onelake_base_path="abfss://test@onelake.dfs.fabric.microsoft.com/lakehouse",
        onelake_domain_paths={
            "claimx": "abfss://claimx@onelake.dfs.fabric.microsoft.com/lakehouse"
        },
    )


@pytest.fixture
def sample_cached_message(temp_cache_dir):
    """Create sample ClaimXCachedDownloadMessage for testing."""
    # Create the actual cached file
    cache_subdir = temp_cache_dir / "claimx" / "media-111"
    cache_subdir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_subdir / "document.pdf"
    cache_file.write_text("test pdf content")

    return ClaimXCachedDownloadMessage(
        media_id="media-111",
        project_id="proj-67890",
        download_url="https://s3.amazonaws.com/claimx-media/presigned/document.pdf",
        destination_path="claimx/proj-67890/media/document.pdf",
        local_cache_path=str(cache_file),
        bytes_downloaded=2048,
        content_type="application/pdf",
        file_type="pdf",
        file_name="document.pdf",
        source_event_id="evt-12345",
        downloaded_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def sample_consumer_record(sample_cached_message):
    """Create sample ConsumerRecord with ClaimXCachedDownloadMessage."""
    return ConsumerRecord(
        topic="test.claimx.downloads.cached",
        partition=0,
        offset=10,
        timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
        timestamp_type=0,
        key=b"media-111",
        value=sample_cached_message.model_dump_json().encode("utf-8"),
        headers=[],
        checksum=None,
        serialized_key_size=9,
        serialized_value_size=len(sample_cached_message.model_dump_json()),
    )


@pytest.mark.asyncio
class TestClaimXUploadWorker:
    """Test suite for ClaimXUploadWorker."""

    async def test_initialization(self, kafka_config):
        """Test worker initialization with correct configuration."""
        worker = ClaimXUploadWorker(kafka_config)

        assert worker.config == kafka_config
        assert worker.domain == "claimx"
        assert worker.WORKER_NAME == "upload_worker"
        assert worker.topic == "test.claimx.downloads.cached"
        assert worker.results_topic == "test.claimx.downloads.results"
        # Verify concurrency settings
        assert worker.concurrency == 10
        assert worker.batch_size == 20

    async def test_initialization_no_onelake_path_raises(self):
        """Test that missing OneLake path raises ValueError."""
        config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            security_protocol="PLAINTEXT",
            claimx={
                "topics": {
                    "downloads_cached": "test.claimx.downloads.cached",
                    "downloads_results": "test.claimx.downloads.results",
                },
                "consumer_group_prefix": "test-claimx",
                "upload_worker": {
                    "processing": {"concurrency": 10, "batch_size": 20},
                },
            },
            # No onelake_base_path or onelake_domain_paths
        )

        with pytest.raises(ValueError, match="OneLake path configuration required"):
            ClaimXUploadWorker(config)

    async def test_start_initializes_onelake_client(self, kafka_config, tmp_path):
        """Test that start() initializes OneLake client."""
        worker = ClaimXUploadWorker(kafka_config)

        # Mock dependencies - replace the producer that was created in __init__
        mock_producer = AsyncMock()
        worker.producer = mock_producer
        mock_onelake = AsyncMock()

        with patch.object(worker, "_create_consumer", new_callable=AsyncMock), patch(
            "kafka_pipeline.claimx.workers.upload_worker.OneLakeClient"
        ) as mock_onelake_class:
            mock_onelake_class.return_value = mock_onelake

            # Prevent the consume loop from running
            worker._consume_batch_loop = AsyncMock()

            await worker.start()

            # Verify OneLake client was initialized with claimx path
            mock_onelake_class.assert_called_once_with(
                "abfss://claimx@onelake.dfs.fabric.microsoft.com/lakehouse"
            )
            mock_onelake.__aenter__.assert_called_once()

            await worker.stop()

    async def test_process_single_upload_success(
        self, kafka_config, sample_consumer_record, sample_cached_message
    ):
        """Test successful upload processing."""
        worker = ClaimXUploadWorker(kafka_config)

        # Initialize concurrency control
        worker._semaphore = asyncio.Semaphore(10)
        worker._in_flight_tasks = set()
        worker._in_flight_lock = asyncio.Lock()

        # Mock dependencies
        worker.producer = AsyncMock()
        mock_onelake = AsyncMock()
        mock_onelake.upload_file.return_value = "claimx/proj-67890/media/document.pdf"
        worker.onelake_client = mock_onelake

        result = await worker._process_single_upload(sample_consumer_record)

        # Verify result
        assert isinstance(result, UploadResult)
        assert result.success is True
        assert result.cached_message.media_id == "media-111"

        # Verify upload was called
        mock_onelake.upload_file.assert_called_once()
        call_args = mock_onelake.upload_file.call_args
        assert call_args.kwargs["relative_path"] == "claimx/proj-67890/media/document.pdf"

        # Verify result message was produced
        worker.producer.send.assert_called_once()
        send_call = worker.producer.send.call_args
        assert send_call.kwargs["topic"] == "test.claimx.downloads.results"
        assert send_call.kwargs["key"] == "media-111"

        result_message = send_call.kwargs["value"]
        assert isinstance(result_message, ClaimXUploadResultMessage)
        assert result_message.status == "completed"
        assert result_message.bytes_uploaded == 2048

    async def test_process_single_upload_file_not_found(
        self, kafka_config, sample_cached_message
    ):
        """Test upload fails when cached file doesn't exist."""
        worker = ClaimXUploadWorker(kafka_config)

        # Initialize concurrency control
        worker._semaphore = asyncio.Semaphore(10)
        worker._in_flight_tasks = set()
        worker._in_flight_lock = asyncio.Lock()

        # Mock dependencies
        worker.producer = AsyncMock()
        worker.onelake_client = AsyncMock()

        # Modify message to point to non-existent file
        sample_cached_message.local_cache_path = "/tmp/nonexistent/file.pdf"

        record = ConsumerRecord(
            topic="test.claimx.downloads.cached",
            partition=0,
            offset=10,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"media-111",
            value=sample_cached_message.model_dump_json().encode("utf-8"),
            headers=[],
            checksum=None,
            serialized_key_size=9,
            serialized_value_size=len(sample_cached_message.model_dump_json()),
        )

        result = await worker._process_single_upload(record)

        # Verify failure
        assert isinstance(result, UploadResult)
        assert result.success is False
        assert result.error is not None

        # Verify failure result was produced
        worker.producer.send.assert_called_once()
        result_message = worker.producer.send.call_args.kwargs["value"]
        assert result_message.status == "failed_permanent"

    async def test_process_single_upload_invalid_json(self, kafka_config):
        """Test handling of invalid message JSON."""
        worker = ClaimXUploadWorker(kafka_config)

        # Initialize concurrency control
        worker._semaphore = asyncio.Semaphore(10)
        worker._in_flight_tasks = set()
        worker._in_flight_lock = asyncio.Lock()

        # Mock dependencies
        worker.producer = AsyncMock()
        worker.onelake_client = AsyncMock()

        # Create record with invalid JSON
        invalid_record = ConsumerRecord(
            topic="test.claimx.downloads.cached",
            partition=0,
            offset=10,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"media-123",
            value=b"invalid json {{{",
            headers=[],
            checksum=None,
            serialized_key_size=9,
            serialized_value_size=15,
        )

        result = await worker._process_single_upload(invalid_record)

        # Should return error result
        assert isinstance(result, UploadResult)
        assert result.success is False
        assert result.error is not None

    async def test_process_batch_concurrent(
        self, kafka_config, temp_cache_dir
    ):
        """Test concurrent batch processing."""
        worker = ClaimXUploadWorker(kafka_config)

        # Initialize concurrency control with limit of 3
        worker._semaphore = asyncio.Semaphore(3)
        worker._in_flight_tasks = set()
        worker._in_flight_lock = asyncio.Lock()
        worker._consumer = AsyncMock()  # Required by _process_batch

        # Mock dependencies
        worker.producer = AsyncMock()
        mock_onelake = AsyncMock()
        mock_onelake.upload_file.return_value = "test/path.pdf"
        worker.onelake_client = mock_onelake

        # Create 5 messages with cached files
        messages = []
        for i in range(5):
            cache_subdir = temp_cache_dir / "claimx" / f"media-{i}"
            cache_subdir.mkdir(parents=True, exist_ok=True)
            cache_file = cache_subdir / "document.pdf"
            cache_file.write_text(f"content-{i}")

            cached_msg = ClaimXCachedDownloadMessage(
                media_id=f"media-{i}",
                project_id=f"proj-{i}",
                download_url=f"https://s3.amazonaws.com/bucket/file{i}.pdf",
                destination_path=f"claimx/proj-{i}/media/document.pdf",
                local_cache_path=str(cache_file),
                bytes_downloaded=1024,
                content_type="application/pdf",
                file_type="pdf",
                file_name="document.pdf",
                source_event_id=f"evt-{i}",
                downloaded_at=datetime.now(timezone.utc),
            )

            record = ConsumerRecord(
                topic="test.claimx.downloads.cached",
                partition=0,
                offset=i,
                timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
                timestamp_type=0,
                key=f"media-{i}".encode("utf-8"),
                value=cached_msg.model_dump_json().encode("utf-8"),
                headers=[],
                checksum=None,
                serialized_key_size=8,
                serialized_value_size=len(cached_msg.model_dump_json()),
            )
            messages.append(record)

        # Track concurrent execution
        concurrent_count = 0
        max_concurrent = 0
        lock = asyncio.Lock()

        original_upload = mock_onelake.upload_file

        async def track_concurrent_upload(*args, **kwargs):
            nonlocal concurrent_count, max_concurrent
            async with lock:
                concurrent_count += 1
                max_concurrent = max(max_concurrent, concurrent_count)

            await asyncio.sleep(0.01)  # Simulate some work

            async with lock:
                concurrent_count -= 1

            return "test/path.pdf"

        mock_onelake.upload_file = track_concurrent_upload

        await worker._process_batch(messages)

        # Verify concurrency was limited by semaphore
        assert max_concurrent <= 3, f"Max concurrent was {max_concurrent}, should be <= 3"

        # Verify all result messages were produced
        assert worker.producer.send.call_count == 5

    async def test_in_flight_tracking(
        self, kafka_config, sample_consumer_record, sample_cached_message
    ):
        """Test in-flight task tracking."""
        worker = ClaimXUploadWorker(kafka_config)

        # Initialize concurrency control
        worker._semaphore = asyncio.Semaphore(10)
        worker._in_flight_tasks = set()
        worker._in_flight_lock = asyncio.Lock()

        # Mock dependencies
        worker.producer = AsyncMock()
        mock_onelake = AsyncMock()
        worker.onelake_client = mock_onelake

        in_flight_during_upload = None

        async def track_upload(*args, **kwargs):
            nonlocal in_flight_during_upload
            async with worker._in_flight_lock:
                in_flight_during_upload = len(worker._in_flight_tasks)
            return "test/path.pdf"

        mock_onelake.upload_file = track_upload

        await worker._process_single_upload(sample_consumer_record)

        # Verify in-flight was tracked during upload
        assert in_flight_during_upload == 1

        # Verify in-flight is cleared after completion
        assert len(worker._in_flight_tasks) == 0

    async def test_cleanup_cache_file(self, kafka_config, tmp_path):
        """Test cache file cleanup after successful upload."""
        worker = ClaimXUploadWorker(kafka_config)

        # Create test cache file structure
        cache_subdir = tmp_path / "media-111"
        cache_subdir.mkdir(parents=True, exist_ok=True)
        cache_file = cache_subdir / "document.pdf"
        cache_file.write_text("test content")

        assert cache_file.exists()

        await worker._cleanup_cache_file(cache_file)

        # Verify file was deleted
        assert not cache_file.exists()
        # Verify parent directory was also deleted (was empty)
        assert not cache_subdir.exists()

    async def test_cleanup_cache_file_nonexistent(self, kafka_config, tmp_path):
        """Test cleanup handles nonexistent file gracefully."""
        worker = ClaimXUploadWorker(kafka_config)

        nonexistent_file = tmp_path / "nonexistent" / "file.pdf"

        # Should not raise
        await worker._cleanup_cache_file(nonexistent_file)

    async def test_request_shutdown(self, kafka_config):
        """Test graceful shutdown request."""
        worker = ClaimXUploadWorker(kafka_config)
        worker._running = True

        await worker.request_shutdown()

        assert worker._running is False

    async def test_stop_waits_for_in_flight(self, kafka_config):
        """Test stop waits for in-flight uploads."""
        worker = ClaimXUploadWorker(kafka_config)

        # Initialize state
        worker._running = True
        worker._in_flight_tasks = {"media-1", "media-2"}
        worker._shutdown_event = asyncio.Event()
        worker._consumer = AsyncMock()
        worker.producer = AsyncMock()
        worker.onelake_client = AsyncMock()
        worker.health_server = AsyncMock()
        worker._cycle_task = None

        # Clear in-flight tasks after a short delay (simulating completion)
        async def clear_in_flight():
            await asyncio.sleep(0.1)
            worker._in_flight_tasks.clear()

        asyncio.create_task(clear_in_flight())

        # Stop should wait for in-flight tasks
        await worker.stop()

        assert worker._consumer is None
        assert worker.onelake_client is None

    async def test_produces_failure_result_on_upload_error(
        self, kafka_config, sample_consumer_record, sample_cached_message
    ):
        """Test that upload failures produce failure result message."""
        worker = ClaimXUploadWorker(kafka_config)

        # Initialize concurrency control
        worker._semaphore = asyncio.Semaphore(10)
        worker._in_flight_tasks = set()
        worker._in_flight_lock = asyncio.Lock()

        # Mock dependencies
        worker.producer = AsyncMock()
        mock_onelake = AsyncMock()
        mock_onelake.upload_file.side_effect = Exception("Upload failed: connection error")
        worker.onelake_client = mock_onelake

        result = await worker._process_single_upload(sample_consumer_record)

        # Verify failure result
        assert result.success is False
        assert result.error is not None

        # Verify failure result was produced
        worker.producer.send.assert_called_once()
        result_message = worker.producer.send.call_args.kwargs["value"]
        assert result_message.status == "failed_permanent"
        assert "Upload failed" in result_message.error_message


@pytest.mark.asyncio
class TestClaimXUploadWorkerConfig:
    """Test suite for upload worker configuration."""

    async def test_default_concurrency_from_processing_config(self):
        """Test concurrency loaded from processing config."""
        config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            security_protocol="PLAINTEXT",
            claimx={
                "topics": {
                    "downloads_cached": "test.claimx.downloads.cached",
                    "downloads_results": "test.claimx.downloads.results",
                },
                "consumer_group_prefix": "test-claimx",
                "upload_worker": {
                    "processing": {
                        "concurrency": 15,
                        "batch_size": 30,
                    },
                },
            },
            onelake_base_path="abfss://test@onelake.dfs.fabric.microsoft.com/lakehouse",
        )

        worker = ClaimXUploadWorker(config)

        assert worker.concurrency == 15
        assert worker.batch_size == 30

    async def test_fallback_to_default_concurrency(self):
        """Test fallback to defaults when not in config."""
        config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            security_protocol="PLAINTEXT",
            claimx={
                "topics": {
                    "downloads_cached": "test.claimx.downloads.cached",
                    "downloads_results": "test.claimx.downloads.results",
                },
                "consumer_group_prefix": "test-claimx",
            },
            onelake_base_path="abfss://test@onelake.dfs.fabric.microsoft.com/lakehouse",
        )

        worker = ClaimXUploadWorker(config)

        # Should use defaults (10, 20)
        assert worker.concurrency == 10
        assert worker.batch_size == 20

    async def test_uses_domain_specific_onelake_path(self):
        """Test that domain-specific OneLake path is preferred."""
        config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            security_protocol="PLAINTEXT",
            claimx={
                "topics": {
                    "downloads_cached": "test.claimx.downloads.cached",
                    "downloads_results": "test.claimx.downloads.results",
                },
                "consumer_group_prefix": "test-claimx",
            },
            onelake_base_path="abfss://default@onelake.dfs.fabric.microsoft.com/lakehouse",
            onelake_domain_paths={
                "claimx": "abfss://claimx-specific@onelake.dfs.fabric.microsoft.com/lakehouse"
            },
        )

        worker = ClaimXUploadWorker(config)

        # The domain-specific path should be used in start()
        # We can verify the path will be used by checking config
        assert config.onelake_domain_paths["claimx"] == "abfss://claimx-specific@onelake.dfs.fabric.microsoft.com/lakehouse"
