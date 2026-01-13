"""
Tests for BaseRetryHandler.

Tests generic retry logic that can be extended for different task types.
Verifies that subclasses can implement task-specific behavior while
inheriting common retry patterns.
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, call

from core.types import ErrorCategory
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.common.retry.base_handler import BaseRetryHandler
from kafka_pipeline.xact.schemas.results import FailedDownloadMessage
from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage


class ConcreteRetryHandler(BaseRetryHandler[DownloadTaskMessage, FailedDownloadMessage]):
    """Concrete implementation for testing BaseRetryHandler."""

    def _create_updated_task(self, task: DownloadTaskMessage) -> DownloadTaskMessage:
        """Create updated task with incremented retry count."""
        updated = task.model_copy(deep=True)
        updated.retry_count += 1
        return updated

    def _create_dlq_message(
        self,
        task: DownloadTaskMessage,
        error_message: str,
        error_category: ErrorCategory,
    ) -> FailedDownloadMessage:
        """Create DLQ message for failed task."""
        return FailedDownloadMessage(
            trace_id=task.trace_id,
            media_id=task.media_id,
            attachment_url=task.attachment_url,
            original_task=task,
            final_error=error_message,
            error_category=error_category.value,
            retry_count=task.retry_count,
            failed_at=datetime.now(timezone.utc),
        )

    def _get_task_key(self, task: DownloadTaskMessage) -> str:
        """Get Kafka partition key for task."""
        return task.trace_id


@pytest.fixture
def kafka_config():
    """Create test Kafka configuration."""
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
    """Create ConcreteRetryHandler with mocked dependencies."""
    return ConcreteRetryHandler(kafka_config, mock_producer, domain="xact")


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


class TestBaseRetryHandlerInit:
    """Test BaseRetryHandler initialization."""

    def test_initialization(self, kafka_config, mock_producer):
        """Test handler initializes with correct configuration."""
        handler = ConcreteRetryHandler(kafka_config, mock_producer, domain="xact")

        assert handler.config == kafka_config
        assert handler.producer == mock_producer
        assert handler.domain == "xact"
        assert handler._retry_delays == [300, 600, 1200, 2400]
        assert handler._max_retries == 4
        assert handler._dlq_topic == "test.downloads.dlq"


class TestBaseRetryHandlerRetry:
    """Test retry routing via base handler."""

    @pytest.mark.asyncio
    async def test_first_retry_transient_error(
        self, retry_handler, download_task, mock_producer, kafka_config
    ):
        """Test first retry sends to retry topic with correct metadata."""
        error = ConnectionError("Network timeout")

        await retry_handler.handle_failure(
            task=download_task,
            error=error,
            error_category=ErrorCategory.TRANSIENT,
        )

        # Verify send was called once
        assert mock_producer.send.call_count == 1

        # Verify sent to correct retry topic
        call_args = mock_producer.send.call_args
        assert call_args.kwargs["topic"] == "test.downloads.pending.retry.5m"

        # Verify retry count incremented
        updated_task = call_args.kwargs["value"]
        assert updated_task.retry_count == 1

        # Verify metadata added
        assert "last_error" in updated_task.metadata
        assert "Network timeout" in updated_task.metadata["last_error"]
        assert updated_task.metadata["error_category"] == "transient"
        assert "retry_at" in updated_task.metadata

        # Verify headers
        headers = call_args.kwargs["headers"]
        assert headers["retry_count"] == "1"
        assert headers["error_category"] == "transient"

    @pytest.mark.asyncio
    async def test_second_retry(
        self, retry_handler, download_task, mock_producer
    ):
        """Test second retry sends to 10m retry topic."""
        download_task.retry_count = 1
        error = TimeoutError("Request timeout")

        await retry_handler.handle_failure(
            task=download_task,
            error=error,
            error_category=ErrorCategory.TRANSIENT,
        )

        call_args = mock_producer.send.call_args
        assert call_args.kwargs["topic"] == "test.downloads.pending.retry.10m"
        assert call_args.kwargs["value"].retry_count == 2

    @pytest.mark.asyncio
    async def test_uses_task_key_for_partitioning(
        self, retry_handler, download_task, mock_producer
    ):
        """Test that task key is used for Kafka partitioning."""
        error = ConnectionError("Network timeout")

        await retry_handler.handle_failure(
            task=download_task,
            error=error,
            error_category=ErrorCategory.TRANSIENT,
        )

        call_args = mock_producer.send.call_args
        assert call_args.kwargs["key"] == download_task.trace_id


class TestBaseRetryHandlerDLQ:
    """Test DLQ routing via base handler."""

    @pytest.mark.asyncio
    async def test_permanent_error_sends_to_dlq(
        self, retry_handler, download_task, mock_producer
    ):
        """Test permanent error skips retry and sends to DLQ."""
        error = ValueError("Invalid file format")

        await retry_handler.handle_failure(
            task=download_task,
            error=error,
            error_category=ErrorCategory.PERMANENT,
        )

        # Verify sent directly to DLQ
        assert mock_producer.send.call_count == 1
        call_args = mock_producer.send.call_args
        assert call_args.kwargs["topic"] == "test.downloads.dlq"

        # Verify DLQ message structure
        dlq_message = call_args.kwargs["value"]
        assert isinstance(dlq_message, FailedDownloadMessage)
        assert dlq_message.trace_id == download_task.trace_id
        assert dlq_message.original_task == download_task
        assert "Invalid file format" in dlq_message.final_error
        assert dlq_message.error_category == "permanent"

        # Verify headers
        headers = call_args.kwargs["headers"]
        assert headers["error_category"] == "permanent"
        assert headers["failed"] == "true"

    @pytest.mark.asyncio
    async def test_retries_exhausted_sends_to_dlq(
        self, retry_handler, download_task, mock_producer
    ):
        """Test exhausted retries send to DLQ."""
        download_task.retry_count = 4  # Max retries
        error = ConnectionError("Network timeout")

        await retry_handler.handle_failure(
            task=download_task,
            error=error,
            error_category=ErrorCategory.TRANSIENT,
        )

        # Verify sent to DLQ
        call_args = mock_producer.send.call_args
        assert call_args.kwargs["topic"] == "test.downloads.dlq"

        # Verify retry count preserved
        dlq_message = call_args.kwargs["value"]
        assert dlq_message.retry_count == 4

    @pytest.mark.asyncio
    async def test_dlq_error_message_truncation(
        self, retry_handler, download_task, mock_producer
    ):
        """Test long error messages are truncated for DLQ."""
        long_error = ConnectionError("E" * 600)

        await retry_handler.handle_failure(
            task=download_task,
            error=long_error,
            error_category=ErrorCategory.PERMANENT,
        )

        dlq_message = mock_producer.send.call_args.kwargs["value"]
        assert len(dlq_message.final_error) == 500
        assert dlq_message.final_error.endswith("...")


class TestBaseRetryHandlerCustomizations:
    """Test customization hooks in base handler."""

    def test_can_override_get_log_context(self, kafka_config, mock_producer):
        """Test subclasses can override _get_log_context for custom logging."""

        class CustomHandler(ConcreteRetryHandler):
            def _get_log_context(self, task):
                return {
                    "trace_id": task.trace_id,
                    "media_id": task.media_id,
                    "custom_field": "custom_value",
                }

        handler = CustomHandler(kafka_config, mock_producer, domain="xact")
        task = DownloadTaskMessage(
            trace_id="evt-001",
            media_id="media-001",
            attachment_url="https://example.com/file.pdf",
            blob_path="path/file.pdf",
            status_subtype="test",
            file_type="pdf",
            assignment_id="A-001",
            event_type="test",
            event_subtype="test",
            retry_count=0,
            original_timestamp=datetime.now(timezone.utc),
            metadata={},
        )

        context = handler._get_log_context(task)
        assert context["custom_field"] == "custom_value"

    @pytest.mark.asyncio
    async def test_can_override_add_error_metadata(
        self, kafka_config, mock_producer, download_task
    ):
        """Test subclasses can override _add_error_metadata for custom metadata."""

        class CustomMetadataHandler(ConcreteRetryHandler):
            def _add_error_metadata(self, task, error, error_category, delay_seconds):
                # Call parent implementation
                super()._add_error_metadata(task, error, error_category, delay_seconds)
                # Add custom metadata
                task.metadata["custom_retry_reason"] = "custom_reason"

        handler = CustomMetadataHandler(kafka_config, mock_producer, domain="xact")
        error = ConnectionError("Network timeout")

        await handler.handle_failure(
            task=download_task,
            error=error,
            error_category=ErrorCategory.TRANSIENT,
        )

        updated_task = mock_producer.send.call_args.kwargs["value"]
        assert "custom_retry_reason" in updated_task.metadata
        assert updated_task.metadata["custom_retry_reason"] == "custom_reason"


class TestBaseRetryHandlerEdgeCases:
    """Test edge cases in base handler."""

    @pytest.mark.asyncio
    async def test_empty_error_message(
        self, retry_handler, download_task, mock_producer
    ):
        """Test handling empty error message."""

        class EmptyError(Exception):
            def __str__(self):
                return ""

        await retry_handler.handle_failure(
            task=download_task,
            error=EmptyError(),
            error_category=ErrorCategory.TRANSIENT,
        )

        updated_task = mock_producer.send.call_args.kwargs["value"]
        assert updated_task.metadata["last_error"] == ""

    @pytest.mark.asyncio
    async def test_error_message_truncation_in_metadata(
        self, retry_handler, download_task, mock_producer
    ):
        """Test error messages are truncated in task metadata."""
        long_error = ConnectionError("E" * 600)

        await retry_handler.handle_failure(
            task=download_task,
            error=long_error,
            error_category=ErrorCategory.TRANSIENT,
        )

        updated_task = mock_producer.send.call_args.kwargs["value"]
        assert len(updated_task.metadata["last_error"]) == 500
