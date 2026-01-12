"""
Unit tests for ClaimXDownloadWorker with concurrent processing.

Tests download task consumption, AttachmentDownloader integration,
task conversion, batch processing, and concurrency control.
"""

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiokafka.structs import ConsumerRecord

from core.download.models import DownloadOutcome, DownloadTask
from core.types import ErrorCategory
from core.errors.exceptions import CircuitOpenError
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.claimx.schemas.cached import ClaimXCachedDownloadMessage
from kafka_pipeline.claimx.schemas.tasks import ClaimXDownloadTask
from kafka_pipeline.claimx.workers.download_worker import ClaimXDownloadWorker, TaskResult


@pytest.fixture
def temp_cache_dir(tmp_path):
    """Create temporary cache directory for downloaded files awaiting upload."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    return cache_dir


@pytest.fixture
def temp_download_dir(tmp_path):
    """Create temporary download directory."""
    download_dir = tmp_path / "downloads"
    download_dir.mkdir()
    return download_dir


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
            "download_worker": {
                "consumer": {"max_poll_records": 20},
                "processing": {
                    "concurrency": 10,
                    "batch_size": 20,
                    "health_port": 8082,
                },
            },
        },
        onelake_base_path="abfss://test@onelake.dfs.fabric.microsoft.com/lakehouse",
        cache_dir=str(temp_cache_dir),
        claimx_api_url="https://test.claimxperience.com/api",
        claimx_api_token="test-token",
        claimx_api_timeout_seconds=30,
        claimx_api_concurrency=20,
    )


@pytest.fixture
def sample_download_task():
    """Create sample ClaimXDownloadTask for testing."""
    return ClaimXDownloadTask(
        media_id="media-111",
        project_id="proj-67890",
        download_url="https://s3.amazonaws.com/claimx-media/presigned/document.pdf",
        blob_path="claimx/proj-67890/media/document.pdf",
        file_type="pdf",
        file_name="document.pdf",
        source_event_id="evt-12345",
        retry_count=0,
        expires_at="2024-12-26T10:30:00Z",
        refresh_count=0,
    )


@pytest.fixture
def sample_consumer_record(sample_download_task):
    """Create sample ConsumerRecord with ClaimXDownloadTask."""
    return ConsumerRecord(
        topic="test.claimx.downloads.pending",
        partition=0,
        offset=10,
        timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
        timestamp_type=0,
        key=b"media-111",
        value=sample_download_task.model_dump_json().encode("utf-8"),
        headers=[],
        checksum=None,
        serialized_key_size=9,
        serialized_value_size=len(sample_download_task.model_dump_json()),
    )


@pytest.mark.asyncio
class TestClaimXDownloadWorker:
    """Test suite for ClaimXDownloadWorker."""

    async def test_initialization(self, kafka_config, temp_download_dir):
        """Test worker initialization with correct configuration."""
        with patch(
            "kafka_pipeline.claimx.workers.download_worker.HealthCheckServer"
        ) as mock_health:
            mock_health.return_value = MagicMock()
            worker = ClaimXDownloadWorker(kafka_config, temp_dir=temp_download_dir)

            assert worker.config == kafka_config
            assert worker.domain == "claimx"
            assert worker.temp_dir == temp_download_dir
            assert worker.WORKER_NAME == "download_worker"
            assert worker.downloader is not None
            # Verify concurrency settings from processing config
            assert worker.concurrency == 10
            assert worker.batch_size == 20

    async def test_topics_list(self, kafka_config, temp_download_dir):
        """Test worker constructs correct topic list with retry topics."""
        with patch(
            "kafka_pipeline.claimx.workers.download_worker.HealthCheckServer"
        ) as mock_health:
            mock_health.return_value = MagicMock()
            worker = ClaimXDownloadWorker(kafka_config, temp_dir=temp_download_dir)

            # Should have pending topic + retry topics
            assert "test.claimx.downloads.pending" in worker.topics
            # Retry topics constructed from retry_delays [300, 600, 1200] = [5m, 10m, 20m]
            assert "test.claimx.downloads.pending.retry.5m" in worker.topics
            assert len(worker.topics) == 4  # pending + 3 retry levels

    async def test_convert_to_download_task(
        self, kafka_config, temp_download_dir, sample_download_task
    ):
        """Test conversion from ClaimXDownloadTask to core DownloadTask."""
        with patch(
            "kafka_pipeline.claimx.workers.download_worker.HealthCheckServer"
        ) as mock_health:
            mock_health.return_value = MagicMock()
            worker = ClaimXDownloadWorker(kafka_config, temp_dir=temp_download_dir)

            from core.download.models import DownloadTask

            download_task = worker._convert_to_download_task(sample_download_task)

            assert isinstance(download_task, DownloadTask)
            assert download_task.url == sample_download_task.download_url
            assert download_task.timeout == 60
            assert download_task.validate_url is True
            assert download_task.validate_file_type is True

            # Verify temp file path includes media_id and preserves filename
            assert sample_download_task.media_id in str(download_task.destination)
            assert download_task.destination.name == "document.pdf"

    async def test_process_single_task_success(
        self,
        kafka_config,
        temp_download_dir,
        temp_cache_dir,
        sample_consumer_record,
        sample_download_task,
    ):
        """Test successful download task processing."""
        with patch(
            "kafka_pipeline.claimx.workers.download_worker.HealthCheckServer"
        ) as mock_health:
            mock_health.return_value = MagicMock()
            worker = ClaimXDownloadWorker(kafka_config, temp_dir=temp_download_dir)

            # Initialize concurrency control
            worker._semaphore = asyncio.Semaphore(10)
            worker._in_flight_tasks = set()
            worker._in_flight_lock = asyncio.Lock()

            # Mock dependencies
            worker.producer = AsyncMock()
            worker.retry_handler = AsyncMock()

            # Create the temp file that will be moved to cache
            temp_subdir = temp_download_dir / sample_download_task.media_id
            temp_subdir.mkdir(parents=True, exist_ok=True)
            temp_file = temp_subdir / "document.pdf"
            temp_file.write_text("test pdf content")

            # Mock successful download
            mock_outcome = DownloadOutcome.success_outcome(
                file_path=temp_file,
                bytes_downloaded=2048,
                content_type="application/pdf",
                status_code=200,
            )

            with patch.object(
                worker.downloader, "download", new_callable=AsyncMock
            ) as mock_download:
                mock_download.return_value = mock_outcome

                result = await worker._process_single_task(sample_consumer_record)

                # Verify result
                assert isinstance(result, TaskResult)
                assert result.success is True
                assert result.task_message.media_id == "media-111"
                assert result.outcome.success is True

                # Verify download was called
                mock_download.assert_called_once()

                # Verify cache file creation
                expected_cache_path = temp_cache_dir / "claimx" / "media-111" / "document.pdf"
                assert expected_cache_path.exists()
                assert expected_cache_path.read_text() == "test pdf content"

                # Verify CachedDownloadMessage was produced
                worker.producer.send.assert_called()
                send_call = worker.producer.send.call_args
                assert send_call.kwargs["topic"] == "test.claimx.downloads.cached"
                assert send_call.kwargs["key"] == "media-111"

                cached_message = send_call.kwargs["value"]
                assert isinstance(cached_message, ClaimXCachedDownloadMessage)
                assert cached_message.media_id == "media-111"
                assert cached_message.project_id == "proj-67890"

    async def test_process_single_task_failure(
        self, kafka_config, temp_download_dir, sample_consumer_record
    ):
        """Test failed download task processing."""
        with patch(
            "kafka_pipeline.claimx.workers.download_worker.HealthCheckServer"
        ) as mock_health:
            mock_health.return_value = MagicMock()
            worker = ClaimXDownloadWorker(kafka_config, temp_dir=temp_download_dir)

            # Initialize concurrency control
            worker._semaphore = asyncio.Semaphore(10)
            worker._in_flight_tasks = set()
            worker._in_flight_lock = asyncio.Lock()

            # Mock dependencies
            worker.producer = AsyncMock()
            worker.retry_handler = AsyncMock()

            # Mock failed download
            mock_outcome = DownloadOutcome.download_failure(
                error_message="Connection timeout",
                error_category=ErrorCategory.TRANSIENT,
                status_code=None,
            )

            with patch.object(
                worker.downloader, "download", new_callable=AsyncMock
            ) as mock_download:
                mock_download.return_value = mock_outcome

                result = await worker._process_single_task(sample_consumer_record)

                # Verify result
                assert isinstance(result, TaskResult)
                assert result.success is False
                assert result.outcome.error_category == ErrorCategory.TRANSIENT

                # Verify retry handler was called
                worker.retry_handler.handle_failure.assert_called_once()

    async def test_process_single_task_invalid_json(self, kafka_config, temp_download_dir):
        """Test handling of invalid message JSON."""
        with patch(
            "kafka_pipeline.claimx.workers.download_worker.HealthCheckServer"
        ) as mock_health:
            mock_health.return_value = MagicMock()
            worker = ClaimXDownloadWorker(kafka_config, temp_dir=temp_download_dir)

            # Initialize concurrency control
            worker._semaphore = asyncio.Semaphore(10)
            worker._in_flight_tasks = set()
            worker._in_flight_lock = asyncio.Lock()

            # Create record with invalid JSON
            invalid_record = ConsumerRecord(
                topic="test.claimx.downloads.pending",
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

            # Should return error result, not raise exception
            result = await worker._process_single_task(invalid_record)

            assert isinstance(result, TaskResult)
            assert result.success is False
            assert result.error is not None
            assert "Failed to parse message" in result.outcome.error_message

    async def test_process_batch_concurrent(
        self, kafka_config, temp_download_dir, temp_cache_dir, sample_download_task
    ):
        """Test concurrent batch processing."""
        with patch(
            "kafka_pipeline.claimx.workers.download_worker.HealthCheckServer"
        ) as mock_health:
            mock_health.return_value = MagicMock()
            worker = ClaimXDownloadWorker(kafka_config, temp_dir=temp_download_dir)

            # Initialize concurrency control with limit of 3
            worker._semaphore = asyncio.Semaphore(3)
            worker._in_flight_tasks = set()
            worker._in_flight_lock = asyncio.Lock()

            # Mock dependencies
            worker.producer = AsyncMock()
            worker.retry_handler = AsyncMock()

            # Create 5 messages
            messages = []
            for i in range(5):
                task = sample_download_task.model_copy(
                    update={
                        "media_id": f"media-{i}",
                        "blob_path": f"claimx/proj-{i}/media/document.pdf",
                    }
                )
                record = ConsumerRecord(
                    topic="test.claimx.downloads.pending",
                    partition=0,
                    offset=i,
                    timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
                    timestamp_type=0,
                    key=f"media-{i}".encode("utf-8"),
                    value=task.model_dump_json().encode("utf-8"),
                    headers=[],
                    checksum=None,
                    serialized_key_size=8,
                    serialized_value_size=len(task.model_dump_json()),
                )
                messages.append(record)

                # Create temp files
                temp_subdir = temp_download_dir / f"media-{i}"
                temp_subdir.mkdir(parents=True, exist_ok=True)
                (temp_subdir / "document.pdf").write_text(f"content-{i}")

            # Track concurrent execution
            concurrent_count = 0
            max_concurrent = 0
            lock = asyncio.Lock()

            async def mock_download(task):
                nonlocal concurrent_count, max_concurrent
                async with lock:
                    concurrent_count += 1
                    max_concurrent = max(max_concurrent, concurrent_count)

                await asyncio.sleep(0.01)  # Simulate some work

                async with lock:
                    concurrent_count -= 1

                return DownloadOutcome.success_outcome(
                    file_path=task.destination,
                    bytes_downloaded=1024,
                    content_type="application/pdf",
                    status_code=200,
                )

            with patch.object(worker.downloader, "download", side_effect=mock_download):
                results = await worker._process_batch(messages)

            # Verify all messages processed
            assert len(results) == 5
            assert all(r.success for r in results)

            # Verify concurrency was limited by semaphore
            assert max_concurrent <= 3, f"Max concurrent was {max_concurrent}, should be <= 3"

    async def test_in_flight_tracking(
        self, kafka_config, temp_download_dir, temp_cache_dir, sample_consumer_record
    ):
        """Test in-flight task tracking."""
        with patch(
            "kafka_pipeline.claimx.workers.download_worker.HealthCheckServer"
        ) as mock_health:
            mock_health.return_value = MagicMock()
            worker = ClaimXDownloadWorker(kafka_config, temp_dir=temp_download_dir)

            # Initialize concurrency control
            worker._semaphore = asyncio.Semaphore(10)
            worker._in_flight_tasks = set()
            worker._in_flight_lock = asyncio.Lock()

            # Mock dependencies
            worker.producer = AsyncMock()
            worker.retry_handler = AsyncMock()

            # Create temp file
            temp_subdir = temp_download_dir / "media-111"
            temp_subdir.mkdir(parents=True, exist_ok=True)
            (temp_subdir / "document.pdf").write_text("test content")

            in_flight_during_download = None

            async def mock_download(task):
                nonlocal in_flight_during_download
                async with worker._in_flight_lock:
                    in_flight_during_download = len(worker._in_flight_tasks)

                return DownloadOutcome.success_outcome(
                    file_path=temp_subdir / "document.pdf",
                    bytes_downloaded=1024,
                    content_type="application/pdf",
                    status_code=200,
                )

            with patch.object(worker.downloader, "download", side_effect=mock_download):
                result = await worker._process_single_task(sample_consumer_record)

            # Verify in-flight was tracked during download
            assert in_flight_during_download == 1

            # Verify in-flight is cleared after completion
            assert len(worker._in_flight_tasks) == 0
            assert worker.in_flight_count == 0

    async def test_handle_batch_results_with_circuit_error(
        self, kafka_config, temp_download_dir
    ):
        """Test that circuit breaker errors prevent offset commit."""
        from core.errors.exceptions import CircuitOpenError

        with patch(
            "kafka_pipeline.claimx.workers.download_worker.HealthCheckServer"
        ) as mock_health:
            mock_health.return_value = MagicMock()
            worker = ClaimXDownloadWorker(kafka_config, temp_dir=temp_download_dir)

            # Create results with one circuit breaker error
            results = [
                TaskResult(
                    message=MagicMock(),
                    task_message=MagicMock(),
                    outcome=MagicMock(),
                    processing_time_ms=100,
                    success=True,
                    error=None,
                ),
                TaskResult(
                    message=MagicMock(),
                    task_message=MagicMock(),
                    outcome=MagicMock(),
                    processing_time_ms=100,
                    success=False,
                    error=CircuitOpenError("test", 60.0),
                ),
            ]

            should_commit = await worker._handle_batch_results(results)
            assert should_commit is False

    async def test_handle_batch_results_all_success(
        self, kafka_config, temp_download_dir
    ):
        """Test that successful results allow offset commit."""
        with patch(
            "kafka_pipeline.claimx.workers.download_worker.HealthCheckServer"
        ) as mock_health:
            mock_health.return_value = MagicMock()
            worker = ClaimXDownloadWorker(kafka_config, temp_dir=temp_download_dir)

            results = [
                TaskResult(
                    message=MagicMock(),
                    task_message=MagicMock(),
                    outcome=MagicMock(),
                    processing_time_ms=100,
                    success=True,
                    error=None,
                ),
                TaskResult(
                    message=MagicMock(),
                    task_message=MagicMock(),
                    outcome=MagicMock(),
                    processing_time_ms=100,
                    success=False,
                    error=Exception("Regular error"),  # Not a circuit error
                ),
            ]

            should_commit = await worker._handle_batch_results(results)
            assert should_commit is True

    async def test_temp_dir_creation(self, kafka_config, tmp_path):
        """Test that temp directory is created if it doesn't exist."""
        temp_dir = tmp_path / "new_downloads"
        assert not temp_dir.exists()

        with patch(
            "kafka_pipeline.claimx.workers.download_worker.HealthCheckServer"
        ) as mock_health:
            mock_health.return_value = MagicMock()
            worker = ClaimXDownloadWorker(kafka_config, temp_dir=temp_dir)

            assert temp_dir.exists()
            assert temp_dir.is_dir()

    async def test_is_running_property(self, kafka_config, temp_download_dir):
        """Test is_running property."""
        with patch(
            "kafka_pipeline.claimx.workers.download_worker.HealthCheckServer"
        ) as mock_health:
            mock_health.return_value = MagicMock()
            worker = ClaimXDownloadWorker(kafka_config, temp_dir=temp_download_dir)

            # Initially not running
            assert worker.is_running is False

            # Simulate running state
            worker._running = True
            assert worker.is_running is True

    async def test_in_flight_count_property(self, kafka_config, temp_download_dir):
        """Test in_flight_count property."""
        with patch(
            "kafka_pipeline.claimx.workers.download_worker.HealthCheckServer"
        ) as mock_health:
            mock_health.return_value = MagicMock()
            worker = ClaimXDownloadWorker(kafka_config, temp_dir=temp_download_dir)
            worker._in_flight_tasks = {"media-1", "media-2", "media-3"}

            assert worker.in_flight_count == 3

    async def test_request_shutdown(self, kafka_config, temp_download_dir):
        """Test graceful shutdown request."""
        with patch(
            "kafka_pipeline.claimx.workers.download_worker.HealthCheckServer"
        ) as mock_health:
            mock_health.return_value = MagicMock()
            worker = ClaimXDownloadWorker(kafka_config, temp_dir=temp_download_dir)
            worker._running = True

            await worker.request_shutdown()

            assert worker._running is False
