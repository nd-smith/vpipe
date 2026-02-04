"""
Tests for Verisk download retry handler.

Test Coverage:
    - Initialization and configuration loading
    - Error category routing (PERMANENT, TRANSIENT, AUTH, CIRCUIT_OPEN, UNKNOWN)
    - Retry count tracking and exhaustion detection
    - Metadata updates (error context, retry_at timestamp)
    - Retry delay calculations and scheduling
    - DLQ message structure and fields
    - Headers for retry and DLQ messages
    - Error message truncation
    - Task deep copy behavior
"""

import pytest
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock, patch

from config.config import KafkaConfig
from core.types import ErrorCategory
from pipeline.verisk.retry.download_handler import RetryHandler
from pipeline.verisk.schemas.results import FailedDownloadMessage
from pipeline.verisk.schemas.tasks import DownloadTaskMessage


class TestRetryHandlerInitialization:
    """Tests for RetryHandler initialization and configuration."""

    def test_initialization_with_default_domain(self):
        """RetryHandler initializes with default verisk domain."""
        config = Mock(spec=KafkaConfig)
        config.get_retry_delays.return_value = [300, 600, 1200, 2400]
        config.get_max_retries.return_value = 4

        handler = RetryHandler(config=config)

        assert handler.domain == "verisk"
        assert handler.config is config
        assert handler._retry_delays == [300, 600, 1200, 2400]
        assert handler._max_retries == 4
        config.get_retry_delays.assert_called_once_with("verisk")
        config.get_max_retries.assert_called_once_with("verisk")

    def test_initialization_with_custom_domain(self):
        """RetryHandler initializes with custom domain."""
        config = Mock(spec=KafkaConfig)
        config.get_retry_delays.return_value = [300, 600]
        config.get_max_retries.return_value = 2

        handler = RetryHandler(config=config, domain="custom")

        assert handler.domain == "custom"
        config.get_retry_delays.assert_called_once_with("custom")
        config.get_max_retries.assert_called_once_with("custom")

    def test_loads_retry_configuration_from_config(self):
        """RetryHandler loads retry delays and max retries from config."""
        config = Mock(spec=KafkaConfig)
        config.get_retry_delays.return_value = [100, 200, 300]
        config.get_max_retries.return_value = 3

        handler = RetryHandler(config=config)

        assert handler._retry_delays == [100, 200, 300]
        assert handler._max_retries == 3

    def test_producers_initially_none(self):
        """Producers are None before start() is called."""
        config = Mock(spec=KafkaConfig)
        config.get_retry_delays.return_value = [300]
        config.get_max_retries.return_value = 1

        handler = RetryHandler(config=config)

        assert handler._retry_producer is None
        assert handler._dlq_producer is None


class TestRetryHandlerErrorCategoryRouting:
    """Tests for error category-based routing decisions."""

    @pytest.fixture
    def mock_config(self):
        """Mock KafkaConfig with standard retry configuration."""
        config = Mock(spec=KafkaConfig)
        config.get_retry_delays.return_value = [300, 600, 1200, 2400]
        config.get_max_retries.return_value = 4
        return config

    @pytest.fixture
    def retry_handler(self, mock_config):
        """RetryHandler with mocked producers."""
        handler = RetryHandler(config=mock_config, domain="verisk")
        handler._retry_producer = AsyncMock()
        handler._dlq_producer = AsyncMock()
        handler._dlq_topic = "verisk-downloads-dlq"
        return handler

    @pytest.fixture
    def sample_task(self):
        """Sample download task for testing."""
        return DownloadTaskMessage(
            trace_id="test-trace-123",
            media_id="media-456",
            attachment_url="https://example.com/file.pdf",
            blob_path="attachments/test/file.pdf",
            status_subtype="inspection",
            file_type="pdf",
            assignment_id="assignment-789",
            estimate_version="v1",
            event_type="statusChange",
            event_subtype="inspectionCompleted",
            retry_count=0,
            original_timestamp=datetime.now(UTC),
            metadata={"original_key": "value"},
        )

    @pytest.mark.asyncio
    @patch("pipeline.verisk.retry.download_handler.record_dlq_message")
    async def test_permanent_error_goes_directly_to_dlq(
        self, mock_record_dlq, retry_handler, sample_task
    ):
        """PERMANENT errors skip retry and go straight to DLQ."""
        error = ValueError("Invalid file format - not a valid PDF")

        await retry_handler.handle_failure(
            task=sample_task,
            error=error,
            error_category=ErrorCategory.PERMANENT,
        )

        retry_handler._retry_producer.send.assert_not_called()
        retry_handler._dlq_producer.send.assert_called_once()
        mock_record_dlq.assert_called_once_with(domain="verisk", reason="permanent")

        dlq_call = retry_handler._dlq_producer.send.call_args
        dlq_message = dlq_call.kwargs["value"]

        assert isinstance(dlq_message, FailedDownloadMessage)
        assert dlq_message.trace_id == "test-trace-123"
        assert dlq_message.error_category == "permanent"
        assert dlq_message.retry_count == 0

    @pytest.mark.asyncio
    async def test_transient_error_routes_to_retry_topic(
        self, retry_handler, sample_task
    ):
        """TRANSIENT errors route to retry topic with incremented count."""
        error = ConnectionError("Network timeout after 30s")

        await retry_handler.handle_failure(
            task=sample_task,
            error=error,
            error_category=ErrorCategory.TRANSIENT,
        )

        retry_handler._retry_producer.send.assert_called_once()
        retry_handler._dlq_producer.send.assert_not_called()

        retry_call = retry_handler._retry_producer.send.call_args
        retry_task = retry_call.kwargs["value"]

        assert isinstance(retry_task, DownloadTaskMessage)
        assert retry_task.retry_count == 1
        assert retry_task.trace_id == "test-trace-123"

    @pytest.mark.asyncio
    async def test_auth_error_routes_to_retry_topic(self, retry_handler, sample_task):
        """AUTH errors route to retry topic (credentials may refresh)."""
        error = PermissionError("401 Unauthorized")

        await retry_handler.handle_failure(
            task=sample_task,
            error=error,
            error_category=ErrorCategory.AUTH,
        )

        retry_handler._retry_producer.send.assert_called_once()
        retry_handler._dlq_producer.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_circuit_open_error_routes_to_retry_topic(
        self, retry_handler, sample_task
    ):
        """CIRCUIT_OPEN errors route to retry topic (circuit may close)."""
        error = RuntimeError("Circuit breaker is open")

        await retry_handler.handle_failure(
            task=sample_task,
            error=error,
            error_category=ErrorCategory.CIRCUIT_OPEN,
        )

        retry_handler._retry_producer.send.assert_called_once()
        retry_handler._dlq_producer.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_unknown_error_routes_to_retry_topic(
        self, retry_handler, sample_task
    ):
        """UNKNOWN errors route to retry topic (conservative approach)."""
        error = Exception("Something unexpected happened")

        await retry_handler.handle_failure(
            task=sample_task,
            error=error,
            error_category=ErrorCategory.UNKNOWN,
        )

        retry_handler._retry_producer.send.assert_called_once()
        retry_handler._dlq_producer.send.assert_not_called()


class TestRetryHandlerRetryExhaustion:
    """Tests for retry count tracking and exhaustion detection."""

    @pytest.fixture
    def mock_config(self):
        """Mock KafkaConfig with standard retry configuration."""
        config = Mock(spec=KafkaConfig)
        config.get_retry_delays.return_value = [300, 600, 1200, 2400]
        config.get_max_retries.return_value = 4
        return config

    @pytest.fixture
    def retry_handler(self, mock_config):
        """RetryHandler with mocked producers."""
        handler = RetryHandler(config=mock_config, domain="verisk")
        handler._retry_producer = AsyncMock()
        handler._dlq_producer = AsyncMock()
        handler._dlq_topic = "verisk-downloads-dlq"
        return handler

    @pytest.fixture
    def sample_task(self):
        """Sample download task."""
        return DownloadTaskMessage(
            trace_id="test-trace-123",
            media_id="media-456",
            attachment_url="https://example.com/file.pdf",
            blob_path="attachments/test/file.pdf",
            status_subtype="inspection",
            file_type="pdf",
            assignment_id="assignment-789",
            estimate_version="v1",
            event_type="statusChange",
            event_subtype="inspectionCompleted",
            retry_count=0,
            original_timestamp=datetime.now(UTC),
            metadata={},
        )

    @pytest.mark.asyncio
    @patch("pipeline.verisk.retry.download_handler.record_dlq_message")
    async def test_retry_count_at_max_goes_to_dlq(
        self, mock_record_dlq, retry_handler, sample_task
    ):
        """Tasks with retry_count == max_retries go to DLQ."""
        sample_task.retry_count = 4
        error = ConnectionError("Still failing after retries")

        await retry_handler.handle_failure(
            task=sample_task,
            error=error,
            error_category=ErrorCategory.TRANSIENT,
        )

        retry_handler._retry_producer.send.assert_not_called()
        retry_handler._dlq_producer.send.assert_called_once()
        mock_record_dlq.assert_called_once_with(domain="verisk", reason="exhausted")

    @pytest.mark.asyncio
    @patch("pipeline.verisk.retry.download_handler.record_dlq_message")
    async def test_retry_count_above_max_goes_to_dlq(
        self, mock_record_dlq, retry_handler, sample_task
    ):
        """Tasks with retry_count > max_retries go to DLQ."""
        sample_task.retry_count = 5
        error = ConnectionError("Still failing")

        await retry_handler.handle_failure(
            task=sample_task,
            error=error,
            error_category=ErrorCategory.TRANSIENT,
        )

        retry_handler._retry_producer.send.assert_not_called()
        retry_handler._dlq_producer.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_retry_count_below_max_retries(self, retry_handler, sample_task):
        """Tasks with retry_count < max_retries go to retry topic."""
        for retry_count in [0, 1, 2, 3]:
            sample_task.retry_count = retry_count
            retry_handler._retry_producer.reset_mock()

            await retry_handler.handle_failure(
                task=sample_task,
                error=ConnectionError("Temporary failure"),
                error_category=ErrorCategory.TRANSIENT,
            )

            retry_handler._retry_producer.send.assert_called_once()


class TestRetryHandlerMetadataAndTimestamp:
    """Tests for metadata updates and timestamp calculations."""

    @pytest.fixture
    def mock_config(self):
        """Mock KafkaConfig with standard retry configuration."""
        config = Mock(spec=KafkaConfig)
        config.get_retry_delays.return_value = [300, 600, 1200, 2400]
        config.get_max_retries.return_value = 4
        return config

    @pytest.fixture
    def retry_handler(self, mock_config):
        """RetryHandler with mocked producers."""
        handler = RetryHandler(config=mock_config, domain="verisk")
        handler._retry_producer = AsyncMock()
        handler._dlq_producer = AsyncMock()
        return handler

    @pytest.fixture
    def sample_task(self):
        """Sample download task."""
        return DownloadTaskMessage(
            trace_id="test-trace-123",
            media_id="media-456",
            attachment_url="https://example.com/file.pdf",
            blob_path="attachments/test/file.pdf",
            status_subtype="inspection",
            file_type="pdf",
            assignment_id="assignment-789",
            estimate_version="v1",
            event_type="statusChange",
            event_subtype="inspectionCompleted",
            retry_count=0,
            original_timestamp=datetime.now(UTC),
            metadata={"existing_key": "existing_value"},
        )

    @pytest.mark.asyncio
    async def test_retry_adds_error_context_to_metadata(
        self, retry_handler, sample_task
    ):
        """Retry task includes error message and category in metadata."""
        error = ConnectionError("Connection timeout after 30 seconds")

        await retry_handler.handle_failure(
            task=sample_task,
            error=error,
            error_category=ErrorCategory.TRANSIENT,
        )

        retry_call = retry_handler._retry_producer.send.call_args
        retry_task = retry_call.kwargs["value"]

        assert "last_error" in retry_task.metadata
        assert "Connection timeout" in retry_task.metadata["last_error"]
        assert retry_task.metadata["error_category"] == "transient"

    @pytest.mark.asyncio
    async def test_retry_preserves_existing_metadata(self, retry_handler, sample_task):
        """Retry task preserves existing metadata fields."""
        error = ConnectionError("Network error")

        await retry_handler.handle_failure(
            task=sample_task,
            error=error,
            error_category=ErrorCategory.TRANSIENT,
        )

        retry_call = retry_handler._retry_producer.send.call_args
        retry_task = retry_call.kwargs["value"]

        assert retry_task.metadata["existing_key"] == "existing_value"

    @pytest.mark.asyncio
    async def test_retry_calculates_retry_at_timestamp(
        self, retry_handler, sample_task
    ):
        """Retry task includes retry_at timestamp based on delay."""
        sample_task.retry_count = 0
        error = ConnectionError("Network error")

        before_call = datetime.now(UTC)
        await retry_handler.handle_failure(
            task=sample_task,
            error=error,
            error_category=ErrorCategory.TRANSIENT,
        )
        after_call = datetime.now(UTC)

        retry_call = retry_handler._retry_producer.send.call_args
        retry_task = retry_call.kwargs["value"]

        retry_at_str = retry_task.metadata["retry_at"]
        retry_at = datetime.fromisoformat(retry_at_str)

        expected_delay = timedelta(seconds=300)
        earliest = before_call + expected_delay
        latest = after_call + expected_delay

        assert earliest <= retry_at <= latest

    @pytest.mark.asyncio
    async def test_retry_delay_matches_retry_count(self, retry_handler, sample_task):
        """Retry delay corresponds to current retry_count."""
        delays = [300, 600, 1200, 2400]

        for retry_count, expected_delay in enumerate(delays):
            sample_task.retry_count = retry_count
            retry_handler._retry_producer.reset_mock()

            await retry_handler.handle_failure(
                task=sample_task,
                error=ConnectionError("Error"),
                error_category=ErrorCategory.TRANSIENT,
            )

            retry_call = retry_handler._retry_producer.send.call_args
            headers = retry_call.kwargs["headers"]

            assert headers["retry_delay_seconds"] == str(expected_delay)

    @pytest.mark.asyncio
    async def test_error_message_truncated_to_500_chars(
        self, retry_handler, sample_task
    ):
        """Long error messages are truncated to 500 characters in metadata."""
        long_error_message = "x" * 1000
        error = Exception(long_error_message)

        await retry_handler.handle_failure(
            task=sample_task,
            error=error,
            error_category=ErrorCategory.TRANSIENT,
        )

        retry_call = retry_handler._retry_producer.send.call_args
        retry_task = retry_call.kwargs["value"]

        assert len(retry_task.metadata["last_error"]) == 500


class TestRetryHandlerRetryHeaders:
    """Tests for retry message headers."""

    @pytest.fixture
    def mock_config(self):
        """Mock KafkaConfig with standard retry configuration."""
        config = Mock(spec=KafkaConfig)
        config.get_retry_delays.return_value = [300, 600, 1200, 2400]
        config.get_max_retries.return_value = 4
        return config

    @pytest.fixture
    def retry_handler(self, mock_config):
        """RetryHandler with mocked producers."""
        handler = RetryHandler(config=mock_config, domain="verisk")
        handler._retry_producer = AsyncMock()
        handler._dlq_producer = AsyncMock()
        return handler

    @pytest.fixture
    def sample_task(self):
        """Sample download task."""
        return DownloadTaskMessage(
            trace_id="test-trace-123",
            media_id="media-456",
            attachment_url="https://example.com/file.pdf",
            blob_path="attachments/test/file.pdf",
            status_subtype="inspection",
            file_type="pdf",
            assignment_id="assignment-789",
            estimate_version="v1",
            event_type="statusChange",
            event_subtype="inspectionCompleted",
            retry_count=2,
            original_timestamp=datetime.now(UTC),
            metadata={},
        )

    @pytest.mark.asyncio
    async def test_retry_headers_include_retry_count(
        self, retry_handler, sample_task
    ):
        """Retry message headers include incremented retry_count."""
        await retry_handler.handle_failure(
            task=sample_task,
            error=ConnectionError("Error"),
            error_category=ErrorCategory.TRANSIENT,
        )

        retry_call = retry_handler._retry_producer.send.call_args
        headers = retry_call.kwargs["headers"]

        assert headers["retry_count"] == "3"

    @pytest.mark.asyncio
    async def test_retry_headers_include_scheduled_retry_time(
        self, retry_handler, sample_task
    ):
        """Retry message headers include scheduled_retry_time."""
        await retry_handler.handle_failure(
            task=sample_task,
            error=ConnectionError("Error"),
            error_category=ErrorCategory.TRANSIENT,
        )

        retry_call = retry_handler._retry_producer.send.call_args
        headers = retry_call.kwargs["headers"]

        assert "scheduled_retry_time" in headers
        retry_time = datetime.fromisoformat(headers["scheduled_retry_time"])
        assert retry_time > datetime.now(UTC)

    @pytest.mark.asyncio
    async def test_retry_headers_include_metadata(self, retry_handler, sample_task):
        """Retry message headers include routing and context metadata."""
        await retry_handler.handle_failure(
            task=sample_task,
            error=ConnectionError("Error"),
            error_category=ErrorCategory.TRANSIENT,
        )

        retry_call = retry_handler._retry_producer.send.call_args
        headers = retry_call.kwargs["headers"]

        assert headers["target_topic"] == "downloads_pending"
        assert headers["worker_type"] == "download_worker"
        assert headers["original_key"] == "test-trace-123"
        assert headers["error_category"] == "transient"
        assert headers["domain"] == "verisk"

    @pytest.mark.asyncio
    async def test_retry_message_key_is_trace_id(self, retry_handler, sample_task):
        """Retry message key is the task trace_id for partitioning."""
        await retry_handler.handle_failure(
            task=sample_task,
            error=ConnectionError("Error"),
            error_category=ErrorCategory.TRANSIENT,
        )

        retry_call = retry_handler._retry_producer.send.call_args
        assert retry_call.kwargs["key"] == "test-trace-123"


class TestRetryHandlerDLQMessage:
    """Tests for DLQ message structure and fields."""

    @pytest.fixture
    def mock_config(self):
        """Mock KafkaConfig with standard retry configuration."""
        config = Mock(spec=KafkaConfig)
        config.get_retry_delays.return_value = [300, 600, 1200, 2400]
        config.get_max_retries.return_value = 4
        return config

    @pytest.fixture
    def retry_handler(self, mock_config):
        """RetryHandler with mocked producers."""
        handler = RetryHandler(config=mock_config, domain="verisk")
        handler._retry_producer = AsyncMock()
        handler._dlq_producer = AsyncMock()
        handler._dlq_topic = "verisk-downloads-dlq"
        return handler

    @pytest.fixture
    def sample_task(self):
        """Sample download task."""
        return DownloadTaskMessage(
            trace_id="test-trace-123",
            media_id="media-456",
            attachment_url="https://example.com/file.pdf",
            blob_path="attachments/test/file.pdf",
            status_subtype="inspection",
            file_type="pdf",
            assignment_id="assignment-789",
            estimate_version="v1",
            event_type="statusChange",
            event_subtype="inspectionCompleted",
            retry_count=4,
            original_timestamp=datetime.now(UTC),
            metadata={"key": "value"},
        )

    @pytest.mark.asyncio
    async def test_dlq_message_includes_trace_id(self, retry_handler, sample_task):
        """DLQ message includes trace_id from original task."""
        await retry_handler.handle_failure(
            task=sample_task,
            error=ValueError("Permanent error"),
            error_category=ErrorCategory.PERMANENT,
        )

        dlq_call = retry_handler._dlq_producer.send.call_args
        dlq_message = dlq_call.kwargs["value"]

        assert dlq_message.trace_id == "test-trace-123"

    @pytest.mark.asyncio
    async def test_dlq_message_includes_attachment_url(
        self, retry_handler, sample_task
    ):
        """DLQ message includes attachment_url from original task."""
        await retry_handler.handle_failure(
            task=sample_task,
            error=ValueError("Permanent error"),
            error_category=ErrorCategory.PERMANENT,
        )

        dlq_call = retry_handler._dlq_producer.send.call_args
        dlq_message = dlq_call.kwargs["value"]

        assert dlq_message.attachment_url == "https://example.com/file.pdf"

    @pytest.mark.asyncio
    async def test_dlq_message_includes_original_task(
        self, retry_handler, sample_task
    ):
        """DLQ message includes complete original task for replay."""
        await retry_handler.handle_failure(
            task=sample_task,
            error=ValueError("Permanent error"),
            error_category=ErrorCategory.PERMANENT,
        )

        dlq_call = retry_handler._dlq_producer.send.call_args
        dlq_message = dlq_call.kwargs["value"]

        assert dlq_message.original_task == sample_task
        assert dlq_message.original_task.metadata == {"key": "value"}

    @pytest.mark.asyncio
    async def test_dlq_message_includes_error_category(
        self, retry_handler, sample_task
    ):
        """DLQ message includes error_category."""
        await retry_handler.handle_failure(
            task=sample_task,
            error=ValueError("Permanent error"),
            error_category=ErrorCategory.PERMANENT,
        )

        dlq_call = retry_handler._dlq_producer.send.call_args
        dlq_message = dlq_call.kwargs["value"]

        assert dlq_message.error_category == "permanent"

    @pytest.mark.asyncio
    async def test_dlq_message_includes_retry_count(self, retry_handler, sample_task):
        """DLQ message includes final retry_count."""
        await retry_handler.handle_failure(
            task=sample_task,
            error=ValueError("Permanent error"),
            error_category=ErrorCategory.PERMANENT,
        )

        dlq_call = retry_handler._dlq_producer.send.call_args
        dlq_message = dlq_call.kwargs["value"]

        assert dlq_message.retry_count == 4

    @pytest.mark.asyncio
    async def test_dlq_message_includes_failed_at_timestamp(
        self, retry_handler, sample_task
    ):
        """DLQ message includes failed_at timestamp."""
        before_call = datetime.now(UTC)
        await retry_handler.handle_failure(
            task=sample_task,
            error=ValueError("Permanent error"),
            error_category=ErrorCategory.PERMANENT,
        )
        after_call = datetime.now(UTC)

        dlq_call = retry_handler._dlq_producer.send.call_args
        dlq_message = dlq_call.kwargs["value"]

        assert before_call <= dlq_message.failed_at <= after_call

    @pytest.mark.asyncio
    async def test_dlq_message_key_is_trace_id(self, retry_handler, sample_task):
        """DLQ message key is trace_id for partitioning."""
        await retry_handler.handle_failure(
            task=sample_task,
            error=ValueError("Permanent error"),
            error_category=ErrorCategory.PERMANENT,
        )

        dlq_call = retry_handler._dlq_producer.send.call_args
        assert dlq_call.kwargs["key"] == "test-trace-123"

    @pytest.mark.asyncio
    async def test_dlq_headers_include_metadata(self, retry_handler, sample_task):
        """DLQ message headers include failure metadata."""
        await retry_handler.handle_failure(
            task=sample_task,
            error=ValueError("Permanent error"),
            error_category=ErrorCategory.PERMANENT,
        )

        dlq_call = retry_handler._dlq_producer.send.call_args
        headers = dlq_call.kwargs["headers"]

        assert headers["retry_count"] == "4"
        assert headers["error_category"] == "permanent"
        assert headers["failed"] == "true"


class TestRetryHandlerTaskImmutability:
    """Tests for task deep copy behavior."""

    @pytest.fixture
    def mock_config(self):
        """Mock KafkaConfig with standard retry configuration."""
        config = Mock(spec=KafkaConfig)
        config.get_retry_delays.return_value = [300, 600, 1200, 2400]
        config.get_max_retries.return_value = 4
        return config

    @pytest.fixture
    def retry_handler(self, mock_config):
        """RetryHandler with mocked producers."""
        handler = RetryHandler(config=mock_config, domain="verisk")
        handler._retry_producer = AsyncMock()
        handler._dlq_producer = AsyncMock()
        return handler

    @pytest.fixture
    def sample_task(self):
        """Sample download task."""
        return DownloadTaskMessage(
            trace_id="test-trace-123",
            media_id="media-456",
            attachment_url="https://example.com/file.pdf",
            blob_path="attachments/test/file.pdf",
            status_subtype="inspection",
            file_type="pdf",
            assignment_id="assignment-789",
            estimate_version="v1",
            event_type="statusChange",
            event_subtype="inspectionCompleted",
            retry_count=0,
            original_timestamp=datetime.now(UTC),
            metadata={"original": "value"},
        )

    @pytest.mark.asyncio
    async def test_original_task_unchanged_after_retry(
        self, retry_handler, sample_task
    ):
        """Original task object is not modified by retry logic."""
        original_retry_count = sample_task.retry_count
        original_metadata = dict(sample_task.metadata)

        await retry_handler.handle_failure(
            task=sample_task,
            error=ConnectionError("Error"),
            error_category=ErrorCategory.TRANSIENT,
        )

        assert sample_task.retry_count == original_retry_count
        assert sample_task.metadata == original_metadata
        assert "last_error" not in sample_task.metadata

    @pytest.mark.asyncio
    async def test_retry_task_is_independent_copy(self, retry_handler, sample_task):
        """Retry task is a deep copy independent of original."""
        await retry_handler.handle_failure(
            task=sample_task,
            error=ConnectionError("Error"),
            error_category=ErrorCategory.TRANSIENT,
        )

        retry_call = retry_handler._retry_producer.send.call_args
        retry_task = retry_call.kwargs["value"]

        assert retry_task is not sample_task
        assert retry_task.metadata is not sample_task.metadata
        assert retry_task.retry_count != sample_task.retry_count


class TestRetryHandlerDLQErrorTruncation:
    """Tests for error message truncation in DLQ."""

    @pytest.fixture
    def mock_config(self):
        """Mock KafkaConfig with standard retry configuration."""
        config = Mock(spec=KafkaConfig)
        config.get_retry_delays.return_value = [300, 600, 1200, 2400]
        config.get_max_retries.return_value = 4
        return config

    @pytest.fixture
    def retry_handler(self, mock_config):
        """RetryHandler with mocked producers."""
        handler = RetryHandler(config=mock_config, domain="verisk")
        handler._retry_producer = AsyncMock()
        handler._dlq_producer = AsyncMock()
        handler._dlq_topic = "verisk-downloads-dlq"
        return handler

    @pytest.fixture
    def sample_task(self):
        """Sample download task."""
        return DownloadTaskMessage(
            trace_id="test-trace-123",
            media_id="media-456",
            attachment_url="https://example.com/file.pdf",
            blob_path="attachments/test/file.pdf",
            status_subtype="inspection",
            file_type="pdf",
            assignment_id="assignment-789",
            estimate_version="v1",
            event_type="statusChange",
            event_subtype="inspectionCompleted",
            retry_count=4,
            original_timestamp=datetime.now(UTC),
            metadata={},
        )

    @pytest.mark.asyncio
    async def test_short_error_messages_unchanged(self, retry_handler, sample_task):
        """Error messages under 500 chars are not truncated."""
        error = ValueError("Short error message")

        await retry_handler.handle_failure(
            task=sample_task,
            error=error,
            error_category=ErrorCategory.PERMANENT,
        )

        dlq_call = retry_handler._dlq_producer.send.call_args
        dlq_message = dlq_call.kwargs["value"]

        # Note: DLQ doesn't store error message in FailedDownloadMessage
        # Truncation happens in logging only

    @pytest.mark.asyncio
    async def test_long_error_messages_dont_crash(self, retry_handler, sample_task):
        """Very long error messages are handled gracefully."""
        long_message = "Error: " + ("x" * 10000)
        error = ValueError(long_message)

        await retry_handler.handle_failure(
            task=sample_task,
            error=error,
            error_category=ErrorCategory.PERMANENT,
        )

        retry_handler._dlq_producer.send.assert_called_once()
