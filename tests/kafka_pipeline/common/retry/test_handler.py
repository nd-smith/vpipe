"""
Tests for RetryHandler.

Tests retry routing logic, DLQ handling, error categorization,
and metadata preservation through retry chain.
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, call

from core.types import ErrorCategory
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.common.retry.handler import RetryHandler
from kafka_pipeline.xact.schemas.results import FailedDownloadMessage
from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage


@pytest.fixture
def kafka_config():
    """Create test Kafka configuration with hierarchical domain structure."""
    return KafkaConfig(
        bootstrap_servers="localhost:9092",
        xact={
            "topics": {
                "events": "test.events.raw",
                "downloads_pending": "test.downloads.pending",
                "downloads_cached": "test.downloads.cached",
                "dlq": "test.downloads.dlq",
            },
            "retry_delays": [300, 600, 1200, 2400],  # 5m, 10m, 20m, 40m
            "consumer_group_prefix": "test",
        },
    )


@pytest.fixture
def mock_producer():
    """Create mock Kafka producer."""
    producer = AsyncMock(spec=BaseKafkaProducer)
    producer.send = AsyncMock()
    return producer


@pytest.fixture
def retry_handler(kafka_config, mock_producer):
    """Create RetryHandler with mocked dependencies."""
    return RetryHandler(kafka_config, mock_producer, domain="xact")


@pytest.fixture
def download_task():
    """Create sample download task."""
    return DownloadTaskMessage(
        trace_id="evt-test-001",
        media_id="media-test-001",
        attachment_url="https://storage.example.com/file.pdf",
        blob_path="documentsReceived/C-123/pdf/file.pdf",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="C-123",
        event_type="claim",
        event_subtype="created",
        retry_count=0,
        original_timestamp=datetime.now(timezone.utc),
        metadata={"source": "test"},
    )


class TestRetryHandlerInit:
    """Test RetryHandler initialization."""

    def test_initialization(self, kafka_config, mock_producer):
        """Test handler initializes with correct configuration."""
        handler = RetryHandler(kafka_config, mock_producer)

        assert handler.config == kafka_config
        assert handler.producer == mock_producer


class TestHandleFailureRetry:
    """Test retry routing for transient failures."""

    @pytest.mark.asyncio
    async def test_first_retry_transient_error(
        self, retry_handler, download_task, mock_producer, kafka_config
    ):
        """Test first retry sends to 5m retry topic with correct metadata."""
        error = ConnectionError("Network timeout")

        await retry_handler.handle_failure(
            task=download_task,
            error=error,
            error_category=ErrorCategory.TRANSIENT,
        )

        # Verify send was called once
        assert mock_producer.send.call_count == 1

        # Extract call arguments
        call_args = mock_producer.send.call_args
        assert call_args.kwargs["topic"] == "test.downloads.pending.retry.5m"
        assert call_args.kwargs["key"] == "evt-test-001"

        # Verify task was updated
        updated_task = call_args.kwargs["value"]
        assert isinstance(updated_task, DownloadTaskMessage)
        assert updated_task.retry_count == 1
        assert updated_task.metadata["last_error"] == "Network timeout"
        assert updated_task.metadata["error_category"] == "transient"
        assert "retry_at" in updated_task.metadata

        # Verify headers
        headers = call_args.kwargs["headers"]
        assert headers["retry_count"] == "1"
        assert headers["error_category"] == "transient"

    @pytest.mark.asyncio
    async def test_second_retry_auth_error(
        self, retry_handler, download_task, mock_producer
    ):
        """Test second retry sends to 10m retry topic for auth errors."""
        download_task.retry_count = 1
        error = PermissionError("Unauthorized")

        await retry_handler.handle_failure(
            task=download_task,
            error=error,
            error_category=ErrorCategory.AUTH,
        )

        call_args = mock_producer.send.call_args
        assert call_args.kwargs["topic"] == "test.downloads.pending.retry.10m"

        updated_task = call_args.kwargs["value"]
        assert updated_task.retry_count == 2
        assert updated_task.metadata["error_category"] == "auth"

    @pytest.mark.asyncio
    async def test_third_retry_circuit_open(
        self, retry_handler, download_task, mock_producer
    ):
        """Test third retry sends to 20m retry topic for circuit open."""
        download_task.retry_count = 2
        error = RuntimeError("Circuit breaker open")

        await retry_handler.handle_failure(
            task=download_task,
            error=error,
            error_category=ErrorCategory.CIRCUIT_OPEN,
        )

        call_args = mock_producer.send.call_args
        assert call_args.kwargs["topic"] == "test.downloads.pending.retry.20m"

        updated_task = call_args.kwargs["value"]
        assert updated_task.retry_count == 3
        assert updated_task.metadata["error_category"] == "circuit_open"

    @pytest.mark.asyncio
    async def test_fourth_retry_unknown_error(
        self, retry_handler, download_task, mock_producer
    ):
        """Test fourth retry sends to 40m retry topic for unknown errors."""
        download_task.retry_count = 3
        error = Exception("Unknown error")

        await retry_handler.handle_failure(
            task=download_task,
            error=error,
            error_category=ErrorCategory.UNKNOWN,
        )

        call_args = mock_producer.send.call_args
        assert call_args.kwargs["topic"] == "test.downloads.pending.retry.40m"

        updated_task = call_args.kwargs["value"]
        assert updated_task.retry_count == 4
        assert updated_task.metadata["error_category"] == "unknown"

    @pytest.mark.asyncio
    async def test_error_message_truncation(
        self, retry_handler, download_task, mock_producer
    ):
        """Test long error messages are truncated to 500 chars."""
        long_error = ValueError("X" * 1000)

        await retry_handler.handle_failure(
            task=download_task,
            error=long_error,
            error_category=ErrorCategory.TRANSIENT,
        )

        updated_task = mock_producer.send.call_args.kwargs["value"]
        assert len(updated_task.metadata["last_error"]) == 500
        assert updated_task.metadata["last_error"].startswith("X")

    @pytest.mark.asyncio
    async def test_metadata_preservation(
        self, retry_handler, download_task, mock_producer
    ):
        """Test original metadata is preserved through retry."""
        download_task.metadata = {
            "source": "test",
            "priority": "high",
            "original_size": 2048,
        }

        await retry_handler.handle_failure(
            task=download_task,
            error=TimeoutError("Timeout"),
            error_category=ErrorCategory.TRANSIENT,
        )

        updated_task = mock_producer.send.call_args.kwargs["value"]
        assert updated_task.metadata["source"] == "test"
        assert updated_task.metadata["priority"] == "high"
        assert updated_task.metadata["original_size"] == 2048
        assert "last_error" in updated_task.metadata
        assert "retry_at" in updated_task.metadata


class TestHandleFailureDLQ:
    """Test DLQ routing for exhausted retries and permanent errors."""

    @pytest.mark.asyncio
    async def test_retries_exhausted_sends_to_dlq(
        self, retry_handler, download_task, mock_producer, kafka_config
    ):
        """Test exhausted retries route to DLQ with FailedDownloadMessage."""
        download_task.retry_count = 4  # Max retries reached
        error = ConnectionError("Final timeout")

        await retry_handler.handle_failure(
            task=download_task,
            error=error,
            error_category=ErrorCategory.TRANSIENT,
        )

        # Verify send to DLQ
        assert mock_producer.send.call_count == 1
        call_args = mock_producer.send.call_args
        assert call_args.kwargs["topic"] == "test.downloads.dlq"
        assert call_args.kwargs["key"] == "evt-test-001"

        # Verify FailedDownloadMessage structure
        dlq_message = call_args.kwargs["value"]
        assert isinstance(dlq_message, FailedDownloadMessage)
        assert dlq_message.trace_id == "evt-test-001"
        assert dlq_message.attachment_url == download_task.attachment_url
        assert dlq_message.original_task == download_task
        assert dlq_message.final_error == "Final timeout"
        assert dlq_message.error_category == "transient"
        assert dlq_message.retry_count == 4
        assert isinstance(dlq_message.failed_at, datetime)

        # Verify headers
        headers = call_args.kwargs["headers"]
        assert headers["retry_count"] == "4"
        assert headers["error_category"] == "transient"
        assert headers["failed"] == "true"

    @pytest.mark.asyncio
    async def test_permanent_error_skips_retry(
        self, retry_handler, download_task, mock_producer
    ):
        """Test permanent errors go directly to DLQ without retry."""
        download_task.retry_count = 0  # First attempt
        error = ValueError("File type not allowed")

        await retry_handler.handle_failure(
            task=download_task,
            error=error,
            error_category=ErrorCategory.PERMANENT,
        )

        # Should go to DLQ, not retry topic
        call_args = mock_producer.send.call_args
        assert call_args.kwargs["topic"] == "test.downloads.dlq"

        dlq_message = call_args.kwargs["value"]
        assert dlq_message.retry_count == 0  # No retries were attempted
        assert dlq_message.error_category == "permanent"

    @pytest.mark.asyncio
    async def test_dlq_error_message_truncation(
        self, retry_handler, download_task, mock_producer
    ):
        """Test DLQ message truncates long error messages."""
        download_task.retry_count = 4
        long_error = RuntimeError("Y" * 1000)

        await retry_handler.handle_failure(
            task=download_task,
            error=long_error,
            error_category=ErrorCategory.TRANSIENT,
        )

        dlq_message = mock_producer.send.call_args.kwargs["value"]
        assert len(dlq_message.final_error) <= 500
        assert dlq_message.final_error.startswith("Y")
        if len(str(long_error)) > 500:
            assert dlq_message.final_error.endswith("...")

    @pytest.mark.asyncio
    async def test_dlq_preserves_original_task(
        self, retry_handler, download_task, mock_producer
    ):
        """Test DLQ message preserves complete original task for replay."""
        download_task.retry_count = 4
        download_task.metadata = {
            "source": "test",
            "last_error": "Previous error",
            "retry_at": "2024-12-25T10:00:00Z",
        }

        await retry_handler.handle_failure(
            task=download_task,
            error=TimeoutError("Final error"),
            error_category=ErrorCategory.TRANSIENT,
        )

        dlq_message = mock_producer.send.call_args.kwargs["value"]
        assert dlq_message.original_task == download_task
        assert dlq_message.original_task.metadata["source"] == "test"
        assert dlq_message.original_task.metadata["last_error"] == "Previous error"


class TestRetryTopicGeneration:
    """Test retry topic naming and configuration."""

    def test_retry_topic_names(self, kafka_config):
        """Test retry topic names match delay configuration."""
        assert kafka_config.get_retry_topic("xact", 0) == "test.downloads.pending.retry.5m"
        assert kafka_config.get_retry_topic("xact", 1) == "test.downloads.pending.retry.10m"
        assert kafka_config.get_retry_topic("xact", 2) == "test.downloads.pending.retry.20m"
        assert kafka_config.get_retry_topic("xact", 3) == "test.downloads.pending.retry.40m"

    def test_retry_topic_out_of_bounds(self, kafka_config):
        """Test requesting invalid retry topic raises error."""
        with pytest.raises(ValueError, match="exceeds max retries"):
            kafka_config.get_retry_topic("xact", 4)


class TestEdgeCases:
    """Test edge cases and error conditions."""

    @pytest.mark.asyncio
    async def test_empty_error_message(
        self, retry_handler, download_task, mock_producer
    ):
        """Test handling of error with empty string representation."""
        error = Exception("")

        await retry_handler.handle_failure(
            task=download_task,
            error=error,
            error_category=ErrorCategory.TRANSIENT,
        )

        updated_task = mock_producer.send.call_args.kwargs["value"]
        assert updated_task.metadata["last_error"] == ""

    @pytest.mark.asyncio
    async def test_task_with_empty_metadata(
        self, retry_handler, mock_producer
    ):
        """Test task with empty metadata dict."""
        task = DownloadTaskMessage(
            trace_id="evt-empty-meta",
            media_id="media-empty-meta",
            attachment_url="https://example.com/file.pdf",
            blob_path="documentsReceived/T-001/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="T-001",
            event_type="test",
            event_subtype="test",
            retry_count=0,
            original_timestamp=datetime.now(timezone.utc),
            metadata={},  # Empty metadata
        )

        await retry_handler.handle_failure(
            task=task,
            error=RuntimeError("Test error"),
            error_category=ErrorCategory.TRANSIENT,
        )

        updated_task = mock_producer.send.call_args.kwargs["value"]
        assert "last_error" in updated_task.metadata
        assert "retry_at" in updated_task.metadata
        assert "error_category" in updated_task.metadata

    @pytest.mark.asyncio
    async def test_exactly_max_retries_boundary(
        self, retry_handler, download_task, mock_producer
    ):
        """Test boundary condition: retry_count == max_retries."""
        download_task.retry_count = 4  # Exactly at max_retries

        await retry_handler.handle_failure(
            task=download_task,
            error=TimeoutError("Timeout"),
            error_category=ErrorCategory.TRANSIENT,
        )

        # Should go to DLQ, not retry
        call_args = mock_producer.send.call_args
        assert call_args.kwargs["topic"] == "test.downloads.dlq"

    @pytest.mark.asyncio
    async def test_exceeds_max_retries(
        self, retry_handler, download_task, mock_producer
    ):
        """Test retry_count > max_retries routes to DLQ."""
        download_task.retry_count = 10  # Way over max_retries

        await retry_handler.handle_failure(
            task=download_task,
            error=TimeoutError("Timeout"),
            error_category=ErrorCategory.TRANSIENT,
        )

        # Should go to DLQ
        call_args = mock_producer.send.call_args
        assert call_args.kwargs["topic"] == "test.downloads.dlq"

        dlq_message = call_args.kwargs["value"]
        assert dlq_message.retry_count == 10


class TestConfigurableRetryDelays:
    """Test custom retry delay configurations."""

    @pytest.mark.asyncio
    async def test_custom_retry_delays(self, mock_producer):
        """Test handler with custom retry delay configuration."""
        custom_config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            xact={
                "topics": {
                    "events": "test.events.raw",
                    "downloads_pending": "test.downloads.pending",
                    "downloads_cached": "test.downloads.cached",
                    "dlq": "test.downloads.dlq",
                },
                "retry_delays": [60, 300, 900],  # 1m, 5m, 15m
                "consumer_group_prefix": "test",
            },
        )
        handler = RetryHandler(custom_config, mock_producer, domain="xact")

        task = DownloadTaskMessage(
            trace_id="evt-custom",
            media_id="media-custom",
            attachment_url="https://example.com/file.pdf",
            blob_path="documentsReceived/T-002/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="T-002",
            event_type="test",
            event_subtype="test",
            retry_count=0,
            original_timestamp=datetime.now(timezone.utc),
        )

        await handler.handle_failure(
            task=task,
            error=TimeoutError("Timeout"),
            error_category=ErrorCategory.TRANSIENT,
        )

        # Should use 1m (60s) topic
        call_args = mock_producer.send.call_args
        assert call_args.kwargs["topic"] == "test.downloads.pending.retry.1m"

    @pytest.mark.asyncio
    async def test_custom_max_retries(self, mock_producer):
        """Test handler with custom max_retries configuration."""
        custom_config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            xact={
                "topics": {
                    "events": "test.events.raw",
                    "downloads_pending": "test.downloads.pending",
                    "downloads_cached": "test.downloads.cached",
                    "dlq": "test.downloads.dlq",
                },
                "retry_delays": [300, 600],  # Only 2 retry levels (max_retries=2)
                "consumer_group_prefix": "test",
            },
        )
        handler = RetryHandler(custom_config, mock_producer, domain="xact")

        task = DownloadTaskMessage(
            trace_id="evt-max",
            media_id="media-max",
            attachment_url="https://example.com/file.pdf",
            blob_path="documentsReceived/T-003/pdf/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="T-003",
            event_type="test",
            event_subtype="test",
            retry_count=2,  # At max
            original_timestamp=datetime.now(timezone.utc),
        )

        await handler.handle_failure(
            task=task,
            error=TimeoutError("Timeout"),
            error_category=ErrorCategory.TRANSIENT,
        )

        # Should go to DLQ at count=2 with max=2
        call_args = mock_producer.send.call_args
        assert call_args.kwargs["topic"] == "test.downloads.dlq"
