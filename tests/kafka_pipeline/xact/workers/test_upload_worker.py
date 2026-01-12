"""
Unit tests for UploadWorker.

Tests cached download consumption, OneLake upload integration,
result production, concurrent processing, and cache cleanup.

Created for WP-314: Upload Worker - Unit Tests.
"""

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiokafka.structs import ConsumerRecord

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.xact.schemas.cached import CachedDownloadMessage
from kafka_pipeline.xact.schemas.results import DownloadResultMessage
from kafka_pipeline.xact.workers.upload_worker import UploadWorker, UploadResult


@pytest.fixture
def kafka_config():
    """Create test Kafka configuration."""
    return KafkaConfig(
        bootstrap_servers="localhost:9092",
        onelake_base_path="abfss://test@test.dfs.core.windows.net/Files",
        onelake_domain_paths={"xact": "abfss://test@test.dfs.core.windows.net/Files"},
        security_protocol="PLAINTEXT",
        sasl_mechanism="PLAIN",
        xact={
            "topics": {
                "events": "test.events.raw",
                "downloads_pending": "test.downloads.pending",
                "downloads_cached": "test.downloads.cached",
                "downloads_results": "test.downloads.results",
                "dlq": "test.downloads.dlq",
            },
            "consumer_group_prefix": "test",
            "upload_worker": {
                "processing": {
                    "concurrency": 10,
                    "batch_size": 20,
                },
            },
        },
    )


@pytest.fixture
def sample_cached_message():
    """Create sample CachedDownloadMessage for testing."""
    return CachedDownloadMessage(
        trace_id="evt-123",
        media_id="media-abc-123",
        attachment_url="https://claimxperience.com/files/document.pdf",
        destination_path="claims/C-456/document.pdf",
        local_cache_path="/tmp/kafka_pipeline_cache/evt-123/document.pdf",
        bytes_downloaded=2048,
        content_type="application/pdf",
        event_type="xact",
        event_subtype="documentsReceived",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="C-456",
        original_timestamp=datetime.now(timezone.utc),
        downloaded_at=datetime.now(timezone.utc),
        metadata={"source_partition": 3, "source_system": "xact"},
    )


@pytest.fixture
def sample_consumer_record(sample_cached_message):
    """Create sample ConsumerRecord with CachedDownloadMessage."""
    return ConsumerRecord(
        topic="test.downloads.cached",
        partition=0,
        offset=10,
        timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
        timestamp_type=0,
        key=b"evt-123",
        value=sample_cached_message.model_dump_json().encode("utf-8"),
        headers=[],
        checksum=None,
        serialized_key_size=7,
        serialized_value_size=len(sample_cached_message.model_dump_json()),
    )


@pytest.fixture
def temp_cache_dir(tmp_path):
    """Create temporary cache directory with test file."""
    cache_dir = tmp_path / "kafka_pipeline_cache"
    cache_dir.mkdir()
    return cache_dir


def create_cache_file(cache_dir: Path, trace_id: str, filename: str) -> Path:
    """Helper to create a cached file for testing."""
    trace_dir = cache_dir / trace_id
    trace_dir.mkdir(parents=True, exist_ok=True)
    file_path = trace_dir / filename
    file_path.write_bytes(b"test content for upload")
    return file_path


@pytest.mark.asyncio
class TestUploadWorkerInitialization:
    """Test suite for UploadWorker initialization."""

    async def test_initialization_success(self, kafka_config):
        """Test worker initialization with correct configuration."""
        worker = UploadWorker(kafka_config)

        assert worker.config == kafka_config
        assert worker.topic == kafka_config.get_topic("xact", "downloads_cached")
        assert worker.WORKER_NAME == "upload_worker"
        assert worker._running is False
        assert worker._consumer is None

    async def test_initialization_without_onelake_path_fails(self):
        """Test worker initialization fails without any OneLake paths configured."""
        config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            onelake_base_path="",  # Not configured
            onelake_domain_paths={},  # No domain paths either
            xact={
                "topics": {
                    "events": "test.events.raw",
                    "downloads_pending": "test.downloads.pending",
                    "downloads_cached": "test.downloads.cached",
                    "downloads_results": "test.downloads.results",
                    "dlq": "test.downloads.dlq",
                },
            },
        )

        with pytest.raises(ValueError) as exc_info:
            UploadWorker(config)

        assert "OneLake" in str(exc_info.value)

    async def test_concurrency_settings(self, kafka_config):
        """Test worker uses configured concurrency settings."""
        worker = UploadWorker(kafka_config)

        assert worker.concurrency == 10
        assert worker.batch_size == 20


@pytest.mark.asyncio
class TestUploadWorkerProcessing:
    """Test suite for UploadWorker message processing."""

    async def test_process_single_upload_success(
        self, kafka_config, sample_consumer_record, sample_cached_message, temp_cache_dir
    ):
        """Test successful upload flow: consume -> upload -> produce result -> cleanup."""
        worker = UploadWorker(kafka_config)

        # Create cache file
        cache_path = create_cache_file(
            temp_cache_dir,
            sample_cached_message.trace_id,
            "document.pdf"
        )

        # Update message with actual cache path
        sample_cached_message.local_cache_path = str(cache_path)
        sample_consumer_record = ConsumerRecord(
            topic="test.downloads.cached",
            partition=0,
            offset=10,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"evt-123",
            value=sample_cached_message.model_dump_json().encode("utf-8"),
            headers=[],
            checksum=None,
            serialized_key_size=7,
            serialized_value_size=len(sample_cached_message.model_dump_json()),
        )

        # Initialize concurrency control
        worker._semaphore = asyncio.Semaphore(10)
        worker._in_flight_tasks = set()
        worker._in_flight_lock = asyncio.Lock()

        # Mock OneLake clients (domain-based)
        mock_client = AsyncMock()
        mock_client.upload_file = AsyncMock(
            return_value="abfss://test@test.dfs.core.windows.net/Files/claims/C-456/document.pdf"
        )
        worker.onelake_clients = {"xact": mock_client}

        # Mock producer
        worker.producer = AsyncMock()

        # Process the message
        result = await worker._process_single_upload(sample_consumer_record)

        # Verify result
        assert isinstance(result, UploadResult)
        assert result.success is True
        assert result.cached_message.trace_id == "evt-123"
        assert result.processing_time_ms >= 0  # Can be 0 in fast unit tests
        assert result.error is None

        # Verify OneLake upload was called
        mock_client.upload_file.assert_called_once()
        call_args = mock_client.upload_file.call_args
        assert call_args.kwargs["relative_path"] == "claims/C-456/document.pdf"
        assert call_args.kwargs["overwrite"] is True

        # Verify result message was produced
        worker.producer.send.assert_called_once()
        call_args = worker.producer.send.call_args
        assert call_args.kwargs["topic"] == kafka_config.get_topic("xact", "downloads_results")
        assert call_args.kwargs["key"] == "evt-123"

        result_msg = call_args.kwargs["value"]
        assert isinstance(result_msg, DownloadResultMessage)
        assert result_msg.status == "completed"
        assert result_msg.trace_id == "evt-123"
        assert result_msg.blob_path == "claims/C-456/document.pdf"
        assert result_msg.bytes_downloaded == 2048

        # Verify cache file was cleaned up
        assert not cache_path.exists()

    async def test_process_single_upload_file_not_found(
        self, kafka_config, sample_consumer_record, sample_cached_message
    ):
        """Test upload failure when cached file doesn't exist."""
        worker = UploadWorker(kafka_config)

        # Point to non-existent file
        sample_cached_message.local_cache_path = "/tmp/does_not_exist/file.pdf"
        sample_consumer_record = ConsumerRecord(
            topic="test.downloads.cached",
            partition=0,
            offset=10,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"evt-123",
            value=sample_cached_message.model_dump_json().encode("utf-8"),
            headers=[],
            checksum=None,
            serialized_key_size=7,
            serialized_value_size=len(sample_cached_message.model_dump_json()),
        )

        # Initialize concurrency control
        worker._semaphore = asyncio.Semaphore(10)
        worker._in_flight_tasks = set()
        worker._in_flight_lock = asyncio.Lock()

        # Mock OneLake clients (domain-based)
        mock_client = AsyncMock()
        worker.onelake_clients = {"xact": mock_client}

        # Mock producer
        worker.producer = AsyncMock()

        # Process the message
        result = await worker._process_single_upload(sample_consumer_record)

        # Verify result
        assert isinstance(result, UploadResult)
        assert result.success is False
        assert result.error is not None
        assert isinstance(result.error, FileNotFoundError)

        # Verify OneLake upload was NOT called
        mock_client.upload_file.assert_not_called()

        # Verify failure result message was produced
        worker.producer.send.assert_called_once()
        call_args = worker.producer.send.call_args
        result_msg = call_args.kwargs["value"]
        assert result_msg.status == "failed_permanent"
        assert "not found" in result_msg.error_message.lower()

    async def test_process_single_upload_onelake_error(
        self, kafka_config, sample_cached_message, temp_cache_dir
    ):
        """Test upload failure when OneLake upload fails."""
        worker = UploadWorker(kafka_config)

        # Create cache file
        cache_path = create_cache_file(
            temp_cache_dir,
            sample_cached_message.trace_id,
            "document.pdf"
        )
        sample_cached_message.local_cache_path = str(cache_path)

        consumer_record = ConsumerRecord(
            topic="test.downloads.cached",
            partition=0,
            offset=10,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"evt-123",
            value=sample_cached_message.model_dump_json().encode("utf-8"),
            headers=[],
            checksum=None,
            serialized_key_size=7,
            serialized_value_size=len(sample_cached_message.model_dump_json()),
        )

        # Initialize concurrency control
        worker._semaphore = asyncio.Semaphore(10)
        worker._in_flight_tasks = set()
        worker._in_flight_lock = asyncio.Lock()

        # Mock OneLake clients to fail
        mock_client = AsyncMock()
        mock_client.upload_file = AsyncMock(
            side_effect=Exception("OneLake connection failed")
        )
        worker.onelake_clients = {"xact": mock_client}

        # Mock producer
        worker.producer = AsyncMock()

        # Process the message
        result = await worker._process_single_upload(consumer_record)

        # Verify result
        assert isinstance(result, UploadResult)
        assert result.success is False
        assert result.error is not None

        # Verify failure result message was produced
        worker.producer.send.assert_called_once()
        call_args = worker.producer.send.call_args
        result_msg = call_args.kwargs["value"]
        assert result_msg.status == "failed_permanent"
        assert "OneLake connection failed" in result_msg.error_message

        # Verify cache file is NOT cleaned up on failure
        assert cache_path.exists()

    async def test_process_single_upload_invalid_json(self, kafka_config):
        """Test handling of invalid message JSON."""
        worker = UploadWorker(kafka_config)

        # Initialize concurrency control
        worker._semaphore = asyncio.Semaphore(10)
        worker._in_flight_tasks = set()
        worker._in_flight_lock = asyncio.Lock()

        # Mock producer
        worker.producer = AsyncMock()

        # Create record with invalid JSON
        invalid_record = ConsumerRecord(
            topic="test.downloads.cached",
            partition=0,
            offset=10,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"evt-123",
            value=b"invalid json {{{",
            headers=[],
            checksum=None,
            serialized_key_size=7,
            serialized_value_size=15,
        )

        # Should return error result, not raise exception
        result = await worker._process_single_upload(invalid_record)

        assert isinstance(result, UploadResult)
        assert result.success is False
        assert result.error is not None


@pytest.mark.asyncio
class TestUploadWorkerConcurrency:
    """Test suite for UploadWorker concurrent processing."""

    async def test_process_batch_concurrent(
        self, kafka_config, sample_cached_message, temp_cache_dir
    ):
        """Test concurrent batch processing with semaphore control."""
        worker = UploadWorker(kafka_config)

        # Initialize concurrency control with limit of 3
        worker._semaphore = asyncio.Semaphore(3)
        worker._in_flight_tasks = set()
        worker._in_flight_lock = asyncio.Lock()
        worker._consumer = AsyncMock()

        # Mock OneLake clients (domain-based)
        mock_client = AsyncMock()
        mock_client.upload_file = AsyncMock(return_value="path/to/file.pdf")
        worker.onelake_clients = {"xact": mock_client}

        # Mock producer
        worker.producer = AsyncMock()

        # Create 5 messages
        messages = []
        for i in range(5):
            cached_msg = sample_cached_message.model_copy(
                update={
                    "trace_id": f"evt-{i}",
                    "destination_path": f"claims/C-{i}/doc.pdf"
                }
            )

            # Create cache files
            cache_path = create_cache_file(temp_cache_dir, f"evt-{i}", "doc.pdf")
            cached_msg.local_cache_path = str(cache_path)

            record = ConsumerRecord(
                topic="test.downloads.cached",
                partition=0,
                offset=i,
                timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
                timestamp_type=0,
                key=f"evt-{i}".encode("utf-8"),
                value=cached_msg.model_dump_json().encode("utf-8"),
                headers=[],
                checksum=None,
                serialized_key_size=5,
                serialized_value_size=len(cached_msg.model_dump_json()),
            )
            messages.append(record)

        # Track concurrent execution
        concurrent_count = 0
        max_concurrent = 0
        lock = asyncio.Lock()

        async def mock_upload(*args, **kwargs):
            nonlocal concurrent_count, max_concurrent
            async with lock:
                concurrent_count += 1
                max_concurrent = max(max_concurrent, concurrent_count)

            await asyncio.sleep(0.01)  # Simulate upload time

            async with lock:
                concurrent_count -= 1

            return "path/to/file.pdf"

        mock_client.upload_file = AsyncMock(side_effect=mock_upload)

        # Process batch
        await worker._process_batch(messages)

        # Verify concurrency was limited by semaphore
        assert max_concurrent <= 3, f"Max concurrent was {max_concurrent}, should be <= 3"

        # Verify all messages were processed
        assert worker.producer.send.call_count == 5

    async def test_in_flight_tracking(
        self, kafka_config, sample_cached_message, temp_cache_dir
    ):
        """Test in-flight task tracking during upload."""
        worker = UploadWorker(kafka_config)

        # Create cache file
        cache_path = create_cache_file(
            temp_cache_dir,
            sample_cached_message.trace_id,
            "document.pdf"
        )
        sample_cached_message.local_cache_path = str(cache_path)

        consumer_record = ConsumerRecord(
            topic="test.downloads.cached",
            partition=0,
            offset=10,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"evt-123",
            value=sample_cached_message.model_dump_json().encode("utf-8"),
            headers=[],
            checksum=None,
            serialized_key_size=7,
            serialized_value_size=len(sample_cached_message.model_dump_json()),
        )

        # Initialize concurrency control
        worker._semaphore = asyncio.Semaphore(10)
        worker._in_flight_tasks = set()
        worker._in_flight_lock = asyncio.Lock()

        # Mock OneLake clients (domain-based)
        mock_client = AsyncMock()

        in_flight_during_upload = None

        async def capture_in_flight(*args, **kwargs):
            nonlocal in_flight_during_upload
            async with worker._in_flight_lock:
                in_flight_during_upload = len(worker._in_flight_tasks)
            return "path/to/file.pdf"

        mock_client.upload_file = AsyncMock(side_effect=capture_in_flight)
        worker.onelake_clients = {"xact": mock_client}

        # Mock producer
        worker.producer = AsyncMock()

        # Process message
        result = await worker._process_single_upload(consumer_record)

        # Verify in-flight was tracked during upload
        assert in_flight_during_upload == 1

        # Verify in-flight is cleared after completion
        assert len(worker._in_flight_tasks) == 0

    async def test_batch_with_mixed_results(
        self, kafka_config, sample_cached_message, temp_cache_dir
    ):
        """Test batch processing with some successes and failures."""
        worker = UploadWorker(kafka_config)

        # Initialize concurrency control
        worker._semaphore = asyncio.Semaphore(10)
        worker._in_flight_tasks = set()
        worker._in_flight_lock = asyncio.Lock()
        worker._consumer = AsyncMock()

        # Create 3 messages
        messages = []
        for i in range(3):
            cached_msg = sample_cached_message.model_copy(
                update={
                    "trace_id": f"evt-{i}",
                    "destination_path": f"claims/C-{i}/doc.pdf"
                }
            )

            # Only create cache file for first two
            if i < 2:
                cache_path = create_cache_file(temp_cache_dir, f"evt-{i}", "doc.pdf")
                cached_msg.local_cache_path = str(cache_path)
            else:
                cached_msg.local_cache_path = "/tmp/does_not_exist/doc.pdf"

            record = ConsumerRecord(
                topic="test.downloads.cached",
                partition=0,
                offset=i,
                timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
                timestamp_type=0,
                key=f"evt-{i}".encode("utf-8"),
                value=cached_msg.model_dump_json().encode("utf-8"),
                headers=[],
                checksum=None,
                serialized_key_size=5,
                serialized_value_size=len(cached_msg.model_dump_json()),
            )
            messages.append(record)

        # Mock OneLake clients (domain-based)
        mock_client = AsyncMock()
        mock_client.upload_file = AsyncMock(return_value="path/to/file.pdf")
        worker.onelake_clients = {"xact": mock_client}

        # Mock producer
        worker.producer = AsyncMock()

        # Process batch
        await worker._process_batch(messages)

        # Verify all 3 messages were processed (sent results)
        assert worker.producer.send.call_count == 3

        # Check the results
        calls = worker.producer.send.call_args_list
        success_count = sum(
            1 for c in calls
            if c.kwargs["value"].status == "completed"
        )
        failure_count = sum(
            1 for c in calls
            if c.kwargs["value"].status == "failed_permanent"
        )

        assert success_count == 2
        assert failure_count == 1


@pytest.mark.asyncio
class TestUploadWorkerCacheCleanup:
    """Test suite for cache file cleanup behavior."""

    async def test_cache_file_deleted_on_success(
        self, kafka_config, sample_cached_message, temp_cache_dir
    ):
        """Test that cache file is deleted after successful upload."""
        worker = UploadWorker(kafka_config)

        # Create cache file
        cache_path = create_cache_file(
            temp_cache_dir,
            sample_cached_message.trace_id,
            "document.pdf"
        )
        sample_cached_message.local_cache_path = str(cache_path)

        consumer_record = ConsumerRecord(
            topic="test.downloads.cached",
            partition=0,
            offset=10,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"evt-123",
            value=sample_cached_message.model_dump_json().encode("utf-8"),
            headers=[],
            checksum=None,
            serialized_key_size=7,
            serialized_value_size=len(sample_cached_message.model_dump_json()),
        )

        # Initialize concurrency control
        worker._semaphore = asyncio.Semaphore(10)
        worker._in_flight_tasks = set()
        worker._in_flight_lock = asyncio.Lock()

        # Mock OneLake clients (domain-based)
        mock_client = AsyncMock()
        mock_client.upload_file = AsyncMock(return_value="path/to/file.pdf")
        worker.onelake_clients = {"xact": mock_client}

        # Mock producer
        worker.producer = AsyncMock()

        # Verify file exists before processing
        assert cache_path.exists()

        # Process the message
        await worker._process_single_upload(consumer_record)

        # Verify file is deleted after successful upload
        assert not cache_path.exists()

    async def test_cache_file_kept_on_failure(
        self, kafka_config, sample_cached_message, temp_cache_dir
    ):
        """Test that cache file is kept after failed upload for manual review."""
        worker = UploadWorker(kafka_config)

        # Create cache file
        cache_path = create_cache_file(
            temp_cache_dir,
            sample_cached_message.trace_id,
            "document.pdf"
        )
        sample_cached_message.local_cache_path = str(cache_path)

        consumer_record = ConsumerRecord(
            topic="test.downloads.cached",
            partition=0,
            offset=10,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"evt-123",
            value=sample_cached_message.model_dump_json().encode("utf-8"),
            headers=[],
            checksum=None,
            serialized_key_size=7,
            serialized_value_size=len(sample_cached_message.model_dump_json()),
        )

        # Initialize concurrency control
        worker._semaphore = asyncio.Semaphore(10)
        worker._in_flight_tasks = set()
        worker._in_flight_lock = asyncio.Lock()

        # Mock OneLake clients to fail
        mock_client = AsyncMock()
        mock_client.upload_file = AsyncMock(
            side_effect=Exception("Upload failed")
        )
        worker.onelake_clients = {"xact": mock_client}

        # Mock producer
        worker.producer = AsyncMock()

        # Verify file exists before processing
        assert cache_path.exists()

        # Process the message
        await worker._process_single_upload(consumer_record)

        # Verify file is KEPT after failed upload
        assert cache_path.exists()

    async def test_empty_parent_dir_removed_on_cleanup(
        self, kafka_config, sample_cached_message, temp_cache_dir
    ):
        """Test that empty parent directory is removed after file cleanup."""
        worker = UploadWorker(kafka_config)

        # Create cache file
        cache_path = create_cache_file(
            temp_cache_dir,
            sample_cached_message.trace_id,
            "document.pdf"
        )
        parent_dir = cache_path.parent
        sample_cached_message.local_cache_path = str(cache_path)

        consumer_record = ConsumerRecord(
            topic="test.downloads.cached",
            partition=0,
            offset=10,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"evt-123",
            value=sample_cached_message.model_dump_json().encode("utf-8"),
            headers=[],
            checksum=None,
            serialized_key_size=7,
            serialized_value_size=len(sample_cached_message.model_dump_json()),
        )

        # Initialize concurrency control
        worker._semaphore = asyncio.Semaphore(10)
        worker._in_flight_tasks = set()
        worker._in_flight_lock = asyncio.Lock()

        # Mock OneLake clients (domain-based)
        mock_client = AsyncMock()
        mock_client.upload_file = AsyncMock(return_value="path/to/file.pdf")
        worker.onelake_clients = {"xact": mock_client}

        # Mock producer
        worker.producer = AsyncMock()

        # Verify parent dir exists before processing
        assert parent_dir.exists()

        # Process the message
        await worker._process_single_upload(consumer_record)

        # Verify parent directory is also removed (it was empty)
        assert not parent_dir.exists()


@pytest.mark.asyncio
class TestUploadWorkerGracefulShutdown:
    """Test suite for graceful shutdown behavior."""

    async def test_stop_waits_for_in_flight_uploads(self, kafka_config):
        """Test that stop() waits for in-flight uploads to complete."""
        worker = UploadWorker(kafka_config)
        worker._running = True
        worker._shutdown_event = asyncio.Event()
        worker._in_flight_tasks = {"evt-1", "evt-2"}
        worker._in_flight_lock = asyncio.Lock()
        worker._consumer = AsyncMock()
        worker.producer = AsyncMock()
        worker.onelake_clients = {"xact": AsyncMock()}

        # Simulate in-flight tasks completing
        async def clear_in_flight():
            await asyncio.sleep(0.1)
            async with worker._in_flight_lock:
                worker._in_flight_tasks.clear()

        asyncio.create_task(clear_in_flight())

        # Stop should wait for in-flight tasks
        await worker.stop()

        # Verify shutdown completed
        assert not worker._running
        assert len(worker._in_flight_tasks) == 0

    async def test_stop_timeout_forces_shutdown(self, kafka_config):
        """Test that stop() forces shutdown after timeout with in-flight tasks."""
        worker = UploadWorker(kafka_config)
        worker._running = True
        worker._shutdown_event = asyncio.Event()
        worker._in_flight_tasks = {"evt-stuck"}  # Will not clear
        worker._in_flight_lock = asyncio.Lock()
        worker._consumer = AsyncMock()
        worker.producer = AsyncMock()
        worker.onelake_clients = {"xact": AsyncMock()}

        # Stop with short timeout (modify for test - in real code it's 30s)
        # The worker has a 30s timeout, but we can't easily override it
        # So we just verify the structure is correct
        await worker.stop()

        # Verify shutdown completed despite stuck task
        assert not worker._running


@pytest.mark.asyncio
class TestUploadWorkerConfig:
    """Test suite for upload concurrency configuration."""

    async def test_default_upload_concurrency_config(self):
        """Test default upload concurrency settings when not specified."""
        config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            onelake_base_path="abfss://test@test.dfs.core.windows.net/Files",
            xact={
                "topics": {
                    "events": "test.events.raw",
                    "downloads_pending": "test.downloads.pending",
                    "downloads_cached": "test.downloads.cached",
                    "downloads_results": "test.downloads.results",
                    "dlq": "test.downloads.dlq",
                },
                # No upload_worker config - should use defaults
            },
        )

        worker = UploadWorker(config)
        # Default concurrency is 10, batch_size is 20 in UploadWorker
        assert worker.concurrency == 10
        assert worker.batch_size == 20

    async def test_custom_upload_concurrency_config(self):
        """Test custom upload concurrency settings."""
        config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            onelake_base_path="abfss://test@test.dfs.core.windows.net/Files",
            xact={
                "topics": {
                    "events": "test.events.raw",
                    "downloads_pending": "test.downloads.pending",
                    "downloads_cached": "test.downloads.cached",
                    "downloads_results": "test.downloads.results",
                    "dlq": "test.downloads.dlq",
                },
                "upload_worker": {
                    "processing": {
                        "concurrency": 25,
                        "batch_size": 50,
                    },
                },
            },
        )

        worker = UploadWorker(config)
        assert worker.concurrency == 25
        assert worker.batch_size == 50

    async def test_upload_concurrency_from_worker_config(self):
        """Test loading upload concurrency settings from worker config."""
        config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            onelake_base_path="abfss://test@test.dfs.core.windows.net/Files",
            xact={
                "topics": {
                    "events": "test.events.raw",
                    "downloads_pending": "test.downloads.pending",
                    "downloads_cached": "test.downloads.cached",
                    "downloads_results": "test.downloads.results",
                    "dlq": "test.downloads.dlq",
                },
                "upload_worker": {
                    "processing": {
                        "concurrency": 30,
                        "batch_size": 40,
                    },
                },
            },
        )

        worker = UploadWorker(config)
        assert worker.concurrency == 30
        assert worker.batch_size == 40
