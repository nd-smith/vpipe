"""
Tests for ClaimX enrichment retry handler.

Test Coverage:
    - Initialization and configuration loading
    - Error category routing (PERMANENT, TRANSIENT, AUTH, CIRCUIT_OPEN, UNKNOWN)
    - Retry count tracking and exhaustion detection
    - Metadata updates (error context, retry_at timestamp)
    - Retry delay calculations and scheduling
    - DLQ message structure and fields
    - Headers for retry and DLQ messages
    - Task deep copy behavior
"""

import pytest
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock

from config.config import KafkaConfig
from core.types import ErrorCategory
from pipeline.claimx.retry.enrichment_handler import EnrichmentRetryHandler
from pipeline.claimx.schemas.results import FailedEnrichmentMessage
from pipeline.claimx.schemas.tasks import ClaimXEnrichmentTask


class TestEnrichmentRetryHandlerInitialization:
    """Tests for EnrichmentRetryHandler initialization and configuration."""

    def test_initialization_with_default_domain(self):
        """EnrichmentRetryHandler initializes with default claimx domain."""
        config = Mock(spec=KafkaConfig)
        config.get_retry_delays.return_value = [300, 600, 1200, 2400]
        config.get_max_retries.return_value = 4

        handler = EnrichmentRetryHandler(config=config)

        assert handler.domain == "claimx"
        assert handler.config is config
        assert handler._retry_delays == [300, 600, 1200, 2400]
        assert handler._max_retries == 4
        config.get_retry_delays.assert_called_once_with("claimx")
        config.get_max_retries.assert_called_once_with("claimx")

    def test_initialization_with_custom_domain(self):
        """EnrichmentRetryHandler initializes with custom domain."""
        config = Mock(spec=KafkaConfig)
        config.get_retry_delays.return_value = [100, 200]
        config.get_max_retries.return_value = 2

        handler = EnrichmentRetryHandler(config=config, domain="custom")

        assert handler.domain == "custom"
        config.get_retry_delays.assert_called_once_with("custom")
        config.get_max_retries.assert_called_once_with("custom")

    def test_producers_initially_none(self):
        """Producers are None before start() is called."""
        config = Mock(spec=KafkaConfig)
        config.get_retry_delays.return_value = [300]
        config.get_max_retries.return_value = 1

        handler = EnrichmentRetryHandler(config=config)

        assert handler._retry_producer is None
        assert handler._dlq_producer is None


class TestEnrichmentRetryHandlerErrorCategoryRouting:
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
        """EnrichmentRetryHandler with mocked producers."""
        handler = EnrichmentRetryHandler(config=mock_config, domain="claimx")
        handler._retry_producer = AsyncMock()
        handler._dlq_producer = AsyncMock()
        return handler

    @pytest.fixture
    def sample_task(self):
        """Sample ClaimX enrichment task for testing."""
        return ClaimXEnrichmentTask(
            event_id="evt-123",
            event_type="PROJECT_FILE_ADDED",
            project_id="proj-456",
            retry_count=0,
            created_at=datetime.now(UTC),
            media_id="media-789",
            metadata={"original_key": "value"},
        )

    @pytest.mark.asyncio
    async def test_permanent_error_goes_directly_to_dlq(
        self, retry_handler, sample_task
    ):
        """PERMANENT errors skip retry and go straight to DLQ."""
        error = ValueError("Invalid project ID format")

        await retry_handler.handle_failure(
            task=sample_task,
            error=error,
            error_category=ErrorCategory.PERMANENT,
        )

        retry_handler._retry_producer.send.assert_not_called()
        retry_handler._dlq_producer.send.assert_called_once()

        dlq_call = retry_handler._dlq_producer.send.call_args
        dlq_message = dlq_call.kwargs["value"]

        assert isinstance(dlq_message, FailedEnrichmentMessage)
        assert dlq_message.event_id == "evt-123"
        assert dlq_message.error_category == "permanent"

    @pytest.mark.asyncio
    async def test_transient_error_routes_to_retry_topic(
        self, retry_handler, sample_task
    ):
        """TRANSIENT errors route to retry topic with incremented count."""
        error = ConnectionError("API timeout after 30s")

        await retry_handler.handle_failure(
            task=sample_task,
            error=error,
            error_category=ErrorCategory.TRANSIENT,
        )

        retry_handler._retry_producer.send.assert_called_once()
        retry_handler._dlq_producer.send.assert_not_called()

        retry_call = retry_handler._retry_producer.send.call_args
        retry_task = retry_call.kwargs["value"]

        assert isinstance(retry_task, ClaimXEnrichmentTask)
        assert retry_task.retry_count == 1
        assert retry_task.event_id == "evt-123"

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


class TestEnrichmentRetryHandlerRetryExhaustion:
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
        """EnrichmentRetryHandler with mocked producers."""
        handler = EnrichmentRetryHandler(config=mock_config, domain="claimx")
        handler._retry_producer = AsyncMock()
        handler._dlq_producer = AsyncMock()
        return handler

    @pytest.fixture
    def sample_task(self):
        """Sample ClaimX enrichment task."""
        return ClaimXEnrichmentTask(
            event_id="evt-123",
            event_type="PROJECT_CREATED",
            project_id="proj-456",
            retry_count=0,
            created_at=datetime.now(UTC),
            metadata={},
        )

    @pytest.mark.asyncio
    async def test_retry_count_at_max_goes_to_dlq(
        self, retry_handler, sample_task
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


class TestEnrichmentRetryHandlerMetadata:
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
        """EnrichmentRetryHandler with mocked producers."""
        handler = EnrichmentRetryHandler(config=mock_config, domain="claimx")
        handler._retry_producer = AsyncMock()
        handler._dlq_producer = AsyncMock()
        return handler

    @pytest.fixture
    def sample_task(self):
        """Sample ClaimX enrichment task."""
        return ClaimXEnrichmentTask(
            event_id="evt-123",
            event_type="PROJECT_FILE_ADDED",
            project_id="proj-456",
            retry_count=0,
            created_at=datetime.now(UTC),
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


class TestEnrichmentRetryHandlerHeaders:
    """Tests for retry and DLQ message headers."""

    @pytest.fixture
    def mock_config(self):
        """Mock KafkaConfig with standard retry configuration."""
        config = Mock(spec=KafkaConfig)
        config.get_retry_delays.return_value = [300, 600, 1200, 2400]
        config.get_max_retries.return_value = 4
        return config

    @pytest.fixture
    def retry_handler(self, mock_config):
        """EnrichmentRetryHandler with mocked producers."""
        handler = EnrichmentRetryHandler(config=mock_config, domain="claimx")
        handler._retry_producer = AsyncMock()
        handler._dlq_producer = AsyncMock()
        return handler

    @pytest.fixture
    def sample_task(self):
        """Sample ClaimX enrichment task."""
        return ClaimXEnrichmentTask(
            event_id="evt-abc-123",
            event_type="PROJECT_FILE_ADDED",
            project_id="proj-456",
            retry_count=2,
            created_at=datetime.now(UTC),
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
    async def test_retry_headers_include_routing_metadata(
        self, retry_handler, sample_task
    ):
        """Retry message headers include routing and context metadata."""
        await retry_handler.handle_failure(
            task=sample_task,
            error=ConnectionError("Error"),
            error_category=ErrorCategory.TRANSIENT,
        )

        retry_call = retry_handler._retry_producer.send.call_args
        headers = retry_call.kwargs["headers"]

        assert headers["target_topic"] == "enrichment_pending"
        assert headers["worker_type"] == "enrichment_worker"
        assert headers["original_key"] == "evt-abc-123"
        assert headers["error_category"] == "transient"
        assert headers["domain"] == "claimx"

    @pytest.mark.asyncio
    async def test_retry_message_key_is_event_id(self, retry_handler, sample_task):
        """Retry message key is event_id for partitioning."""
        await retry_handler.handle_failure(
            task=sample_task,
            error=ConnectionError("Error"),
            error_category=ErrorCategory.TRANSIENT,
        )

        retry_call = retry_handler._retry_producer.send.call_args
        assert retry_call.kwargs["key"] == "evt-abc-123"

    @pytest.mark.asyncio
    async def test_dlq_message_key_is_event_id(self, retry_handler, sample_task):
        """DLQ message key is event_id for partitioning."""
        sample_task.retry_count = 4

        await retry_handler.handle_failure(
            task=sample_task,
            error=ConnectionError("Error"),
            error_category=ErrorCategory.TRANSIENT,
        )

        dlq_call = retry_handler._dlq_producer.send.call_args
        assert dlq_call.kwargs["key"] == "evt-abc-123"


class TestEnrichmentRetryHandlerDLQMessage:
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
        """EnrichmentRetryHandler with mocked producers."""
        handler = EnrichmentRetryHandler(config=mock_config, domain="claimx")
        handler._retry_producer = AsyncMock()
        handler._dlq_producer = AsyncMock()
        return handler

    @pytest.fixture
    def sample_task(self):
        """Sample ClaimX enrichment task."""
        return ClaimXEnrichmentTask(
            event_id="evt-123",
            event_type="PROJECT_FILE_ADDED",
            project_id="proj-456",
            retry_count=4,
            created_at=datetime.now(UTC),
            media_id="media-789",
            metadata={"key": "value"},
        )

    @pytest.mark.asyncio
    async def test_dlq_message_includes_event_id(self, retry_handler, sample_task):
        """DLQ message includes event_id from original task."""
        await retry_handler.handle_failure(
            task=sample_task,
            error=ValueError("Permanent error"),
            error_category=ErrorCategory.PERMANENT,
        )

        dlq_call = retry_handler._dlq_producer.send.call_args
        dlq_message = dlq_call.kwargs["value"]

        assert dlq_message.event_id == "evt-123"

    @pytest.mark.asyncio
    async def test_dlq_message_includes_event_type(self, retry_handler, sample_task):
        """DLQ message includes event_type from original task."""
        await retry_handler.handle_failure(
            task=sample_task,
            error=ValueError("Permanent error"),
            error_category=ErrorCategory.PERMANENT,
        )

        dlq_call = retry_handler._dlq_producer.send.call_args
        dlq_message = dlq_call.kwargs["value"]

        assert dlq_message.event_type == "PROJECT_FILE_ADDED"

    @pytest.mark.asyncio
    async def test_dlq_message_includes_project_id(self, retry_handler, sample_task):
        """DLQ message includes project_id from original task."""
        await retry_handler.handle_failure(
            task=sample_task,
            error=ValueError("Permanent error"),
            error_category=ErrorCategory.PERMANENT,
        )

        dlq_call = retry_handler._dlq_producer.send.call_args
        dlq_message = dlq_call.kwargs["value"]

        assert dlq_message.project_id == "proj-456"

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
    async def test_dlq_message_includes_error_details(
        self, retry_handler, sample_task
    ):
        """DLQ message includes error category and final error message."""
        await retry_handler.handle_failure(
            task=sample_task,
            error=ValueError("Permanent error"),
            error_category=ErrorCategory.PERMANENT,
        )

        dlq_call = retry_handler._dlq_producer.send.call_args
        dlq_message = dlq_call.kwargs["value"]

        assert dlq_message.error_category == "permanent"
        assert "Permanent error" in dlq_message.final_error


class TestEnrichmentRetryHandlerTaskImmutability:
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
        """EnrichmentRetryHandler with mocked producers."""
        handler = EnrichmentRetryHandler(config=mock_config, domain="claimx")
        handler._retry_producer = AsyncMock()
        handler._dlq_producer = AsyncMock()
        return handler

    @pytest.fixture
    def sample_task(self):
        """Sample ClaimX enrichment task."""
        return ClaimXEnrichmentTask(
            event_id="evt-123",
            event_type="PROJECT_CREATED",
            project_id="proj-456",
            retry_count=0,
            created_at=datetime.now(UTC),
            metadata={"original": "value"},
        )

    @pytest.mark.asyncio
    async def test_original_task_unchanged_after_retry(
        self, retry_handler, sample_task
    ):
        """Original task object is not modified by retry logic."""
        original_retry_count = sample_task.retry_count
        original_metadata = dict(sample_task.metadata) if sample_task.metadata else {}

        await retry_handler.handle_failure(
            task=sample_task,
            error=ConnectionError("Error"),
            error_category=ErrorCategory.TRANSIENT,
        )

        assert sample_task.retry_count == original_retry_count
        if sample_task.metadata:
            assert "last_error" not in sample_task.metadata
            assert sample_task.metadata.get("original") == "value"

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
        assert retry_task.retry_count != sample_task.retry_count
