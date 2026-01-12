"""
Tests for download worker error handling (WP-306).

Tests error classification and routing logic:
- Transient errors -> retry topics
- Permanent errors -> DLQ
- Circuit breaker errors -> no commit (reprocess)
- Auth errors -> retry topics
- Retry count incrementation
- Error context preservation

Updated for WP-313: Uses _process_single_task instead of _handle_task_message.
"""

import asyncio
import pytest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, Mock, MagicMock, patch, call
from aiokafka.structs import ConsumerRecord

from core.download.models import DownloadOutcome
from core.errors.exceptions import CircuitOpenError, ErrorCategory
from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage
from kafka_pipeline.xact.workers.download_worker import DownloadWorker, TaskResult


@pytest.fixture
def temp_cache_dir(tmp_path):
    """Create temporary cache directory."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    return cache_dir


@pytest.fixture
def config(temp_cache_dir):
    """Create test Kafka configuration using mock."""
    config = MagicMock()
    config.bootstrap_servers = "localhost:9092"
    config.security_protocol = "PLAINTEXT"
    config.sasl_mechanism = "PLAIN"
    config.request_timeout_ms = 120000
    config.metadata_max_age_ms = 300000
    config.connections_max_idle_ms = 540000
    config.cache_dir = str(temp_cache_dir)

    # Configure topics
    def get_topic(domain, topic_key):
        topics = {
            "downloads_pending": "xact.downloads.pending",
            "downloads_cached": "xact.downloads.cached",
            "downloads_results": "xact.downloads.results",
            "dlq": "xact.downloads.dlq",
        }
        return topics.get(topic_key, f"xact.{topic_key}")

    def get_retry_topic(domain, attempt):
        delays = [300, 600, 1200, 2400]
        if attempt >= len(delays):
            raise ValueError(f"Retry attempt {attempt} exceeds max")
        delay_minutes = delays[attempt] // 60
        return f"xact.downloads.pending.retry.{delay_minutes}m"

    def get_retry_delays(domain):
        return [300, 600, 1200, 2400]

    def get_consumer_group(domain, worker_name):
        return f"{domain}-{worker_name}"

    def get_worker_config(domain, worker_name, component):
        if component == "processing":
            return {"concurrency": 10, "batch_size": 20, "timeout_seconds": 60}
        elif component == "consumer":
            return {"max_poll_records": 20, "session_timeout_ms": 60000}
        return {}

    config.get_topic = MagicMock(side_effect=get_topic)
    config.get_retry_topic = MagicMock(side_effect=get_retry_topic)
    config.get_retry_delays = MagicMock(side_effect=get_retry_delays)
    config.get_consumer_group = MagicMock(side_effect=get_consumer_group)
    config.get_worker_config = MagicMock(side_effect=get_worker_config)

    return config


@pytest.fixture
def sample_task():
    """Create sample download task message."""
    return DownloadTaskMessage(
        trace_id="test-trace-123",
        media_id="media-test-123",
        attachment_url="https://example.com/file.pdf",
        blob_path="documentsReceived/C-123/pdf/file.pdf",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="C-123",
        event_type="claim",
        event_subtype="created",
        retry_count=0,
        original_timestamp=datetime.now(timezone.utc),
        metadata={},
    )


@pytest.fixture
def consumer_record(sample_task):
    """Create sample Kafka consumer record."""
    return ConsumerRecord(
        topic="xact.downloads.pending",
        partition=0,
        offset=42,
        timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
        timestamp_type=0,
        key=b"test-trace-123",
        value=sample_task.model_dump_json().encode("utf-8"),
        headers=[],
        checksum=None,
        serialized_key_size=len(b"test-trace-123"),
        serialized_value_size=len(sample_task.model_dump_json().encode("utf-8")),
    )


def setup_worker_for_testing(worker):
    """Initialize worker state for unit testing (WP-313 concurrency control)."""
    worker._semaphore = asyncio.Semaphore(10)
    worker._in_flight_tasks = set()
    worker._in_flight_lock = asyncio.Lock()


class TestDownloadWorkerErrorHandling:
    """Test error handling and routing in download worker."""

    @pytest.mark.asyncio
    async def test_transient_error_routed_to_retry(self, config, sample_task, consumer_record):
        """Test that transient errors are routed to retry topics."""
        worker = DownloadWorker(config, domain="xact")
        setup_worker_for_testing(worker)

        # Mock dependencies
        worker.downloader = AsyncMock()
        worker.producer = AsyncMock()
        worker.producer.send = AsyncMock()
        worker.retry_handler = AsyncMock()

        # Simulate transient download failure
        failed_outcome = DownloadOutcome(
            success=False,
            file_path=None,
            bytes_downloaded=None,
            content_type=None,
            error_message="Connection timeout",
            error_category=ErrorCategory.TRANSIENT,
            status_code=None,
        )
        worker.downloader.download.return_value = failed_outcome

        # Process message
        result = await worker._process_single_task(consumer_record)

        # Verify result indicates failure
        assert isinstance(result, TaskResult)
        assert result.success is False
        assert result.outcome.error_category == ErrorCategory.TRANSIENT

        # Verify retry handler was called with correct parameters
        worker.retry_handler.handle_failure.assert_called_once()
        call_args = worker.retry_handler.handle_failure.call_args
        assert call_args[1]["task"].trace_id == sample_task.trace_id
        assert call_args[1]["error_category"] == ErrorCategory.TRANSIENT
        assert "Connection timeout" in str(call_args[1]["error"])

        # Verify result message was produced
        assert worker.producer.send.call_count == 1
        result_call = worker.producer.send.call_args
        assert result_call.kwargs["topic"] == "xact.downloads.results"
        result_message = result_call.kwargs["value"]
        assert result_message.status == "failed"  # Transient failure

    @pytest.mark.asyncio
    async def test_permanent_error_routed_to_dlq(self, config, sample_task, consumer_record):
        """Test that permanent errors are routed to DLQ."""
        worker = DownloadWorker(config, domain="xact")
        setup_worker_for_testing(worker)

        # Mock dependencies
        worker.downloader = AsyncMock()
        worker.producer = AsyncMock()
        worker.producer.send = AsyncMock()
        worker.retry_handler = AsyncMock()

        # Simulate permanent download failure (404)
        failed_outcome = DownloadOutcome(
            success=False,
            file_path=None,
            bytes_downloaded=None,
            content_type=None,
            error_message="File not found",
            error_category=ErrorCategory.PERMANENT,
            status_code=404,
        )
        worker.downloader.download.return_value = failed_outcome

        # Process message
        result = await worker._process_single_task(consumer_record)

        # Verify result indicates failure
        assert isinstance(result, TaskResult)
        assert result.success is False

        # Verify retry handler was called (it will route to DLQ)
        worker.retry_handler.handle_failure.assert_called_once()
        call_args = worker.retry_handler.handle_failure.call_args
        assert call_args[1]["error_category"] == ErrorCategory.PERMANENT

        # Verify result message shows permanent failure
        result_call = worker.producer.send.call_args
        result_message = result_call.kwargs["value"]
        assert result_message.status == "failed_permanent"

    @pytest.mark.asyncio
    async def test_circuit_open_error_returns_error_result(self, config, sample_task, consumer_record):
        """Test that circuit breaker errors return a result with CircuitOpenError.

        Note: WP-313 changed behavior - circuit errors return TaskResult with error
        instead of raising. The batch handler uses this to prevent offset commit.
        """
        worker = DownloadWorker(config, domain="xact")
        setup_worker_for_testing(worker)

        # Mock dependencies
        worker.downloader = AsyncMock()
        worker.producer = AsyncMock()
        worker.retry_handler = AsyncMock()

        # Simulate circuit breaker open
        failed_outcome = DownloadOutcome(
            success=False,
            file_path=None,
            bytes_downloaded=None,
            content_type=None,
            error_message="Circuit breaker is open",
            error_category=ErrorCategory.CIRCUIT_OPEN,
            status_code=None,
        )
        worker.downloader.download.return_value = failed_outcome

        # Process message - should return result with CircuitOpenError
        result = await worker._process_single_task(consumer_record)

        # Verify result indicates circuit error
        assert isinstance(result, TaskResult)
        assert result.success is False
        assert result.error is not None
        assert isinstance(result.error, CircuitOpenError)
        assert result.error.circuit_name == "download_worker"

        # Verify retry handler was NOT called (circuit open is special)
        worker.retry_handler.handle_failure.assert_not_called()

        # Verify result message was NOT produced (will be reprocessed)
        worker.producer.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_auth_error_routed_to_retry(self, config, sample_task, consumer_record):
        """Test that auth errors are routed to retry topics."""
        worker = DownloadWorker(config, domain="xact")
        setup_worker_for_testing(worker)

        # Mock dependencies
        worker.downloader = AsyncMock()
        worker.producer = AsyncMock()
        worker.producer.send = AsyncMock()
        worker.retry_handler = AsyncMock()

        # Simulate auth failure
        failed_outcome = DownloadOutcome(
            success=False,
            file_path=None,
            bytes_downloaded=None,
            content_type=None,
            error_message="Authentication failed - token expired",
            error_category=ErrorCategory.AUTH,
            status_code=401,
        )
        worker.downloader.download.return_value = failed_outcome

        # Process message
        result = await worker._process_single_task(consumer_record)

        # Verify result
        assert isinstance(result, TaskResult)
        assert result.success is False

        # Verify retry handler was called (auth errors get retry)
        worker.retry_handler.handle_failure.assert_called_once()
        call_args = worker.retry_handler.handle_failure.call_args
        assert call_args[1]["error_category"] == ErrorCategory.AUTH

        # Verify result message shows transient (auth may recover)
        result_call = worker.producer.send.call_args
        result_message = result_call.kwargs["value"]
        assert result_message.status == "failed"  # Transient failure (auth retryable)

    @pytest.mark.asyncio
    async def test_unknown_error_routed_to_retry(self, config, sample_task, consumer_record):
        """Test that unknown errors are routed to retry topics conservatively."""
        worker = DownloadWorker(config, domain="xact")
        setup_worker_for_testing(worker)

        # Mock dependencies
        worker.downloader = AsyncMock()
        worker.producer = AsyncMock()
        worker.producer.send = AsyncMock()
        worker.retry_handler = AsyncMock()

        # Simulate unknown error (no category)
        failed_outcome = DownloadOutcome(
            success=False,
            file_path=None,
            bytes_downloaded=None,
            content_type=None,
            error_message="Something weird happened",
            error_category=None,  # Unknown
            status_code=None,
        )
        worker.downloader.download.return_value = failed_outcome

        # Process message
        result = await worker._process_single_task(consumer_record)

        # Verify result
        assert isinstance(result, TaskResult)
        assert result.success is False

        # Verify retry handler was called with UNKNOWN category
        worker.retry_handler.handle_failure.assert_called_once()
        call_args = worker.retry_handler.handle_failure.call_args
        assert call_args[1]["error_category"] == ErrorCategory.UNKNOWN

        # Verify result message shows transient (conservative retry)
        result_call = worker.producer.send.call_args
        result_message = result_call.kwargs["value"]
        assert result_message.status == "failed"  # Transient (conservative)

    @pytest.mark.asyncio
    async def test_error_context_preserved(self, config, sample_task, consumer_record):
        """Test that error context is preserved through retry routing."""
        worker = DownloadWorker(config, domain="xact")
        setup_worker_for_testing(worker)

        # Mock dependencies
        worker.downloader = AsyncMock()
        worker.producer = AsyncMock()
        worker.producer.send = AsyncMock()
        worker.retry_handler = AsyncMock()

        # Simulate failure with detailed error
        failed_outcome = DownloadOutcome(
            success=False,
            file_path=None,
            bytes_downloaded=None,
            content_type=None,
            error_message="SSL certificate verification failed for https://example.com/file.pdf",
            error_category=ErrorCategory.TRANSIENT,
            status_code=None,
        )
        worker.downloader.download.return_value = failed_outcome

        # Process message
        result = await worker._process_single_task(consumer_record)

        # Verify result
        assert isinstance(result, TaskResult)
        assert result.success is False

        # Verify error message is preserved in result
        result_call = worker.producer.send.call_args
        result_message = result_call.kwargs["value"]
        assert "SSL certificate verification" in result_message.error_message
        assert result_message.trace_id == sample_task.trace_id
        assert result_message.attachment_url == sample_task.attachment_url

        # Verify error is passed to retry handler
        call_args = worker.retry_handler.handle_failure.call_args
        error = call_args[1]["error"]
        assert "SSL certificate verification" in str(error)

    @pytest.mark.asyncio
    async def test_retry_handler_failure_logged_not_raised(self, config, sample_task, consumer_record):
        """Test that retry handler failures are logged but don't block result production.

        Note: WP-313 changed behavior - retry handler failures are logged but
        result messages are still produced for observability.
        """
        worker = DownloadWorker(config, domain="xact")
        setup_worker_for_testing(worker)

        # Mock dependencies
        worker.downloader = AsyncMock()
        worker.producer = AsyncMock()
        worker.producer.send = AsyncMock()
        worker.retry_handler = AsyncMock()

        # Simulate download failure
        failed_outcome = DownloadOutcome(
            success=False,
            file_path=None,
            bytes_downloaded=None,
            content_type=None,
            error_message="Download failed",
            error_category=ErrorCategory.TRANSIENT,
            status_code=None,
        )
        worker.downloader.download.return_value = failed_outcome

        # Make retry handler fail
        worker.retry_handler.handle_failure.side_effect = Exception("Kafka producer failed")

        # Process message - should NOT raise, but log the error
        result = await worker._process_single_task(consumer_record)

        # Verify result was still returned
        assert isinstance(result, TaskResult)
        assert result.success is False

        # Verify retry handler was called
        worker.retry_handler.handle_failure.assert_called_once()

        # Verify result message was still produced
        assert worker.producer.send.call_count == 1

    @pytest.mark.asyncio
    async def test_temp_file_cleanup_on_circuit_open(self, config, sample_task, consumer_record, tmp_path):
        """Test that temporary files are cleaned up even when circuit is open."""
        worker = DownloadWorker(config, domain="xact", temp_dir=tmp_path / "downloads")
        setup_worker_for_testing(worker)

        # Mock dependencies
        worker.downloader = AsyncMock()
        worker.producer = AsyncMock()
        worker.retry_handler = AsyncMock()

        # Simulate circuit open with a temp file created
        temp_file = tmp_path / "test_file.pdf"
        temp_file.touch()
        failed_outcome = DownloadOutcome(
            success=False,
            file_path=temp_file,
            bytes_downloaded=None,
            content_type=None,
            error_message="Circuit breaker is open",
            error_category=ErrorCategory.CIRCUIT_OPEN,
            status_code=None,
        )
        worker.downloader.download.return_value = failed_outcome

        # Mock cleanup
        with patch.object(worker, "_cleanup_temp_file", new_callable=AsyncMock) as mock_cleanup:
            # Process message
            result = await worker._process_single_task(consumer_record)

            # Verify result indicates circuit error
            assert isinstance(result, TaskResult)
            assert result.success is False
            assert isinstance(result.error, CircuitOpenError)

            # Verify cleanup was called
            mock_cleanup.assert_called_once_with(temp_file)

    @pytest.mark.asyncio
    async def test_retry_count_passed_to_handler(self, config, consumer_record):
        """Test that retry count is correctly passed to retry handler."""
        worker = DownloadWorker(config, domain="xact")
        setup_worker_for_testing(worker)

        # Mock dependencies
        worker.downloader = AsyncMock()
        worker.producer = AsyncMock()
        worker.producer.send = AsyncMock()
        worker.retry_handler = AsyncMock()

        # Create task with retry count
        task_with_retries = DownloadTaskMessage(
            trace_id="test-trace-456",
            media_id="media-test-456",
            attachment_url="https://example.com/file.pdf",
            blob_path="documentsReceived/C-123/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-123",
            event_type="claim",
            event_subtype="created",
            retry_count=2,  # Already retried twice
            original_timestamp=datetime.now(timezone.utc),
            metadata={"last_error": "Previous failure"},
        )

        # Update consumer record with new task
        record = ConsumerRecord(
            topic="xact.downloads.pending.retry.10m",
            partition=0,
            offset=100,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"test-trace-456",
            value=task_with_retries.model_dump_json().encode("utf-8"),
            headers=[],
            checksum=None,
            serialized_key_size=len(b"test-trace-456"),
            serialized_value_size=len(task_with_retries.model_dump_json().encode("utf-8")),
        )

        # Simulate failure
        failed_outcome = DownloadOutcome(
            success=False,
            file_path=None,
            bytes_downloaded=None,
            content_type=None,
            error_message="Still failing",
            error_category=ErrorCategory.TRANSIENT,
            status_code=None,
        )
        worker.downloader.download.return_value = failed_outcome

        # Process message
        result = await worker._process_single_task(record)

        # Verify result
        assert isinstance(result, TaskResult)
        assert result.success is False

        # Verify retry handler received task with retry_count=2
        worker.retry_handler.handle_failure.assert_called_once()
        call_args = worker.retry_handler.handle_failure.call_args
        task_arg = call_args[1]["task"]
        assert task_arg.retry_count == 2
        assert task_arg.metadata["last_error"] == "Previous failure"


class TestDownloadWorkerSuccessPath:
    """Test that successful downloads don't go through error handling."""

    @pytest.mark.asyncio
    async def test_successful_download_no_retry_handler(self, config, sample_task, consumer_record, tmp_path):
        """Test that successful downloads don't call retry handler."""
        from kafka_pipeline.xact.schemas.cached import CachedDownloadMessage

        worker = DownloadWorker(config, domain="xact", temp_dir=tmp_path / "downloads")
        setup_worker_for_testing(worker)

        # Mock dependencies
        worker.downloader = AsyncMock()
        worker.producer = AsyncMock()
        worker.producer.send = AsyncMock()
        worker.retry_handler = AsyncMock()

        # Simulate successful download - create temp dir for cleanup
        temp_dir = worker.temp_dir / sample_task.trace_id
        temp_dir.mkdir(parents=True, exist_ok=True)
        temp_file = temp_dir / "file.pdf"
        temp_file.write_text("pdf content")

        success_outcome = DownloadOutcome(
            success=True,
            file_path=temp_file,
            bytes_downloaded=1024,
            content_type="application/pdf",
            error_message=None,
            error_category=None,
            status_code=200,
        )
        worker.downloader.download.return_value = success_outcome

        # Process message
        result = await worker._process_single_task(consumer_record)

        # Verify result indicates success
        assert isinstance(result, TaskResult)
        assert result.success is True
        assert result.outcome.success is True

        # Verify retry handler was NOT called
        worker.retry_handler.handle_failure.assert_not_called()

        # Verify cached message was produced (download worker now produces to cached topic)
        assert worker.producer.send.call_count == 1
        result_call = worker.producer.send.call_args
        cached_message = result_call.kwargs["value"]
        assert isinstance(cached_message, CachedDownloadMessage)
        assert cached_message.bytes_downloaded == 1024
        assert cached_message.trace_id == sample_task.trace_id


class TestBatchResultHandling:
    """Test batch result handling for commit decisions (WP-313)."""

    @pytest.mark.asyncio
    async def test_batch_with_circuit_error_prevents_commit(self, config):
        """Test that circuit breaker errors in batch prevent offset commit."""
        worker = DownloadWorker(config, domain="xact")

        # Create results with circuit breaker error
        results = [
            TaskResult(
                message=Mock(),
                task_message=Mock(),
                outcome=Mock(),
                processing_time_ms=100,
                success=True,
                error=None,
            ),
            TaskResult(
                message=Mock(),
                task_message=Mock(),
                outcome=Mock(),
                processing_time_ms=100,
                success=False,
                error=CircuitOpenError("test", 60.0),
            ),
        ]

        should_commit = await worker._handle_batch_results(results)
        assert should_commit is False

    @pytest.mark.asyncio
    async def test_batch_without_circuit_error_allows_commit(self, config):
        """Test that batches without circuit errors allow offset commit."""
        worker = DownloadWorker(config, domain="xact")

        # Create results without circuit errors
        results = [
            TaskResult(
                message=Mock(),
                task_message=Mock(),
                outcome=Mock(),
                processing_time_ms=100,
                success=True,
                error=None,
            ),
            TaskResult(
                message=Mock(),
                task_message=Mock(),
                outcome=Mock(),
                processing_time_ms=100,
                success=False,
                error=Exception("Regular error"),  # Not circuit error
            ),
        ]

        should_commit = await worker._handle_batch_results(results)
        assert should_commit is True
