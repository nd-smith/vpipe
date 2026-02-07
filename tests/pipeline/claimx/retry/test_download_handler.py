"""
Tests for ClaimX download retry handler with URL refresh capability.

Test Coverage:
    - Initialization and configuration loading
    - Error category routing (PERMANENT, TRANSIENT, AUTH, CIRCUIT_OPEN, UNKNOWN)
    - Retry count tracking and exhaustion detection
    - URL expiration detection logic
    - URL refresh from API (success and failure paths)
    - Metadata updates (error context, retry_at timestamp, url_refreshed_at)
    - Retry delay calculations and scheduling
    - DLQ message structure with url_refresh_attempted flag
    - Headers for retry and DLQ messages
    - Task deep copy behavior
    - API client integration for URL refresh
"""

import pytest
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock

from config.config import MessageConfig
from core.types import ErrorCategory
from pipeline.claimx.api_client import ClaimXApiClient
from pipeline.claimx.retry.download_handler import DownloadRetryHandler
from pipeline.claimx.schemas.results import FailedDownloadMessage
from pipeline.claimx.schemas.tasks import ClaimXDownloadTask


class TestDownloadRetryHandlerInitialization:
    """Tests for DownloadRetryHandler initialization and configuration."""

    def test_initialization_with_default_domain(self):
        """DownloadRetryHandler initializes with default claimx domain."""
        config = Mock(spec=MessageConfig)
        config.get_retry_delays.return_value = [300, 600, 1200, 2400]
        config.get_max_retries.return_value = 4
        api_client = Mock(spec=ClaimXApiClient)

        handler = DownloadRetryHandler(config=config, api_client=api_client)

        assert handler.domain == "claimx"
        assert handler.config is config
        assert handler.api_client is api_client
        assert handler._retry_delays == [300, 600, 1200, 2400]
        assert handler._max_retries == 4
        config.get_retry_delays.assert_called_once_with("claimx")
        config.get_max_retries.assert_called_once_with("claimx")

    def test_initialization_with_custom_domain(self):
        """DownloadRetryHandler initializes with custom domain."""
        config = Mock(spec=MessageConfig)
        config.get_retry_delays.return_value = [100, 200]
        config.get_max_retries.return_value = 2
        api_client = Mock(spec=ClaimXApiClient)

        handler = DownloadRetryHandler(
            config=config, api_client=api_client, domain="custom"
        )

        assert handler.domain == "custom"
        config.get_retry_delays.assert_called_once_with("custom")
        config.get_max_retries.assert_called_once_with("custom")

    def test_producers_initially_none(self):
        """Producers are None before start() is called."""
        config = Mock(spec=MessageConfig)
        config.get_retry_delays.return_value = [300]
        config.get_max_retries.return_value = 1
        api_client = Mock(spec=ClaimXApiClient)

        handler = DownloadRetryHandler(config=config, api_client=api_client)

        assert handler._retry_producer is None
        assert handler._dlq_producer is None


class TestDownloadRetryHandlerErrorCategoryRouting:
    """Tests for error category-based routing decisions."""

    @pytest.fixture
    def mock_config(self):
        """Mock MessageConfig with standard retry configuration."""
        config = Mock()
        config.get_retry_delays.return_value = [300, 600, 1200, 2400]
        config.get_max_retries.return_value = 4
        config.get_retry_topic.return_value = "claimx.enrichment.retry"
        config.get_dlq_topic.return_value = "claimx.enrichment.dlq"
        config.get_topic.return_value = "enrichment_pending"
        return config

    @pytest.fixture
    def mock_api_client(self):
        """Mock ClaimX API client."""
        return AsyncMock(spec=ClaimXApiClient)

    @pytest.fixture
    def retry_handler(self, mock_config, mock_api_client):
        """DownloadRetryHandler with mocked producers."""
        handler = DownloadRetryHandler(
            config=mock_config, api_client=mock_api_client, domain="claimx"
        )
        handler._retry_producer = AsyncMock()
        handler._dlq_producer = AsyncMock()
        return handler

    @pytest.fixture
    def sample_task(self):
        """Sample ClaimX download task for testing."""
        return ClaimXDownloadTask(
            media_id="12345",
            project_id="67890",
            download_url="https://example.com/presigned/file.pdf?sig=abc123",
            blob_path="claimx/projects/67890/media/12345.pdf",
            file_type="pdf",
            file_name="document.pdf",
            source_event_id="evt-123",
            retry_count=0,
            metadata={"original_key": "value"},
        )

    @pytest.mark.asyncio
    async def test_permanent_error_goes_directly_to_dlq(
        self, retry_handler, sample_task
    ):
        """PERMANENT errors skip retry and go straight to DLQ."""
        error = ValueError("Invalid file format - corrupted PDF")

        await retry_handler.handle_failure(
            task=sample_task,
            error=error,
            error_category=ErrorCategory.PERMANENT,
        )

        retry_handler._retry_producer.send.assert_not_called()
        retry_handler._dlq_producer.send.assert_called_once()

        dlq_call = retry_handler._dlq_producer.send.call_args
        dlq_message = dlq_call.kwargs["value"]
        headers = dlq_call.kwargs["headers"]

        assert isinstance(dlq_message, FailedDownloadMessage)
        assert dlq_message.media_id == "12345"
        assert dlq_message.error_category == "permanent"
        assert headers["url_refresh_attempted"] == "False"

    @pytest.mark.asyncio
    async def test_transient_error_without_url_expiration_routes_to_retry(
        self, retry_handler, sample_task
    ):
        """TRANSIENT errors without URL expiration indicators go to retry topic."""
        error = ConnectionError("Network timeout after 30s")

        await retry_handler.handle_failure(
            task=sample_task,
            error=error,
            error_category=ErrorCategory.TRANSIENT,
        )

        retry_handler._retry_producer.send.assert_called_once()
        retry_handler._dlq_producer.send.assert_not_called()

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


class TestDownloadRetryHandlerURLExpirationDetection:
    """Tests for URL expiration detection logic."""

    @pytest.fixture
    def mock_config(self):
        """Mock MessageConfig with standard retry configuration."""
        config = Mock()
        config.get_retry_delays.return_value = [300, 600, 1200, 2400]
        config.get_max_retries.return_value = 4
        config.get_retry_topic.return_value = "claimx.enrichment.retry"
        config.get_dlq_topic.return_value = "claimx.enrichment.dlq"
        config.get_topic.return_value = "enrichment_pending"
        return config

    @pytest.fixture
    def mock_api_client(self):
        """Mock ClaimX API client."""
        return AsyncMock(spec=ClaimXApiClient)

    @pytest.fixture
    def retry_handler(self, mock_config, mock_api_client):
        """DownloadRetryHandler with mocked producers."""
        handler = DownloadRetryHandler(
            config=mock_config, api_client=mock_api_client, domain="claimx"
        )
        return handler

    def test_detects_403_forbidden_error(self, retry_handler):
        """_should_refresh_url detects 403 Forbidden errors."""
        error = Exception("HTTP 403 Forbidden")
        should_refresh = retry_handler._should_refresh_url(
            error, ErrorCategory.TRANSIENT
        )
        assert should_refresh is True

    def test_detects_expired_keyword(self, retry_handler):
        """_should_refresh_url detects 'expired' keyword."""
        error = Exception("URL has expired")
        should_refresh = retry_handler._should_refresh_url(
            error, ErrorCategory.TRANSIENT
        )
        assert should_refresh is True

    def test_detects_forbidden_keyword(self, retry_handler):
        """_should_refresh_url detects 'forbidden' keyword."""
        error = Exception("Access forbidden")
        should_refresh = retry_handler._should_refresh_url(
            error, ErrorCategory.TRANSIENT
        )
        assert should_refresh is True

    def test_detects_access_denied(self, retry_handler):
        """_should_refresh_url detects 'access denied' keyword."""
        error = Exception("Access denied")
        should_refresh = retry_handler._should_refresh_url(
            error, ErrorCategory.TRANSIENT
        )
        assert should_refresh is True

    def test_detects_unauthorized(self, retry_handler):
        """_should_refresh_url detects 'unauthorized' keyword."""
        error = Exception("Unauthorized access")
        should_refresh = retry_handler._should_refresh_url(
            error, ErrorCategory.UNKNOWN
        )
        assert should_refresh is True

    def test_case_insensitive_detection(self, retry_handler):
        """_should_refresh_url is case-insensitive."""
        error = Exception("HTTP 403 FORBIDDEN")
        should_refresh = retry_handler._should_refresh_url(
            error, ErrorCategory.TRANSIENT
        )
        assert should_refresh is True

    def test_does_not_refresh_for_permanent_errors(self, retry_handler):
        """_should_refresh_url returns False for PERMANENT errors."""
        error = Exception("403 Forbidden")
        should_refresh = retry_handler._should_refresh_url(
            error, ErrorCategory.PERMANENT
        )
        assert should_refresh is False

    def test_does_not_refresh_for_auth_errors(self, retry_handler):
        """_should_refresh_url returns False for AUTH errors."""
        error = Exception("403 Forbidden")
        should_refresh = retry_handler._should_refresh_url(
            error, ErrorCategory.AUTH
        )
        assert should_refresh is False

    def test_does_not_refresh_for_circuit_open(self, retry_handler):
        """_should_refresh_url returns False for CIRCUIT_OPEN errors."""
        error = Exception("403 Forbidden")
        should_refresh = retry_handler._should_refresh_url(
            error, ErrorCategory.CIRCUIT_OPEN
        )
        assert should_refresh is False

    def test_does_not_refresh_for_non_expiration_errors(self, retry_handler):
        """_should_refresh_url returns False for non-expiration errors."""
        error = Exception("Connection timeout")
        should_refresh = retry_handler._should_refresh_url(
            error, ErrorCategory.TRANSIENT
        )
        assert should_refresh is False


class TestDownloadRetryHandlerURLRefresh:
    """Tests for URL refresh from API."""

    @pytest.fixture
    def mock_config(self):
        """Mock MessageConfig with standard retry configuration."""
        config = Mock()
        config.get_retry_delays.return_value = [300, 600, 1200, 2400]
        config.get_max_retries.return_value = 4
        config.get_retry_topic.return_value = "claimx.enrichment.retry"
        config.get_dlq_topic.return_value = "claimx.enrichment.dlq"
        config.get_topic.return_value = "enrichment_pending"
        return config

    @pytest.fixture
    def mock_api_client(self):
        """Mock ClaimX API client."""
        return AsyncMock(spec=ClaimXApiClient)

    @pytest.fixture
    def retry_handler(self, mock_config, mock_api_client):
        """DownloadRetryHandler with mocked producers and API client."""
        handler = DownloadRetryHandler(
            config=mock_config, api_client=mock_api_client, domain="claimx"
        )
        handler._retry_producer = AsyncMock()
        handler._dlq_producer = AsyncMock()
        return handler

    @pytest.fixture
    def sample_task(self):
        """Sample ClaimX download task."""
        return ClaimXDownloadTask(
            media_id="12345",
            project_id="67890",
            download_url="https://example.com/old_url.pdf?sig=expired",
            blob_path="claimx/projects/67890/media/12345.pdf",
            file_type="pdf",
            file_name="document.pdf",
            source_event_id="evt-123",
            retry_count=0,
            metadata={},
        )

    @pytest.mark.asyncio
    async def test_url_refresh_success_updates_task(
        self, retry_handler, sample_task, mock_api_client
    ):
        """Successful URL refresh updates task with new URL."""
        new_url = "https://example.com/new_url.pdf?sig=fresh"
        mock_api_client.get_project_media.return_value = [
            {"full_download_link": new_url}
        ]

        refreshed_task = await retry_handler._try_refresh_url(sample_task)

        assert refreshed_task is not None
        assert refreshed_task.download_url == new_url
        assert refreshed_task.media_id == "12345"
        assert refreshed_task.project_id == "67890"

    @pytest.mark.asyncio
    async def test_url_refresh_adds_metadata(
        self, retry_handler, sample_task, mock_api_client
    ):
        """URL refresh adds metadata about refresh operation."""
        new_url = "https://example.com/new_url.pdf?sig=fresh"
        mock_api_client.get_project_media.return_value = [
            {"full_download_link": new_url}
        ]

        before_refresh = datetime.now(UTC)
        refreshed_task = await retry_handler._try_refresh_url(sample_task)
        after_refresh = datetime.now(UTC)

        assert "url_refreshed_at" in refreshed_task.metadata
        assert "original_url" in refreshed_task.metadata

        refresh_time = datetime.fromisoformat(refreshed_task.metadata["url_refreshed_at"])
        assert before_refresh <= refresh_time <= after_refresh

    @pytest.mark.asyncio
    async def test_url_refresh_preserves_existing_metadata(
        self, retry_handler, sample_task, mock_api_client
    ):
        """URL refresh preserves existing task metadata."""
        sample_task.metadata = {"existing_key": "existing_value"}
        mock_api_client.get_project_media.return_value = [
            {"full_download_link": "https://example.com/new.pdf"}
        ]

        refreshed_task = await retry_handler._try_refresh_url(sample_task)

        assert refreshed_task.metadata["existing_key"] == "existing_value"

    @pytest.mark.asyncio
    async def test_url_refresh_calls_api_with_correct_params(
        self, retry_handler, sample_task, mock_api_client
    ):
        """URL refresh calls API with correct project_id and media_id."""
        mock_api_client.get_project_media.return_value = [
            {"full_download_link": "https://example.com/new.pdf"}
        ]

        await retry_handler._try_refresh_url(sample_task)

        mock_api_client.get_project_media.assert_called_once_with(
            project_id=67890, media_ids=[12345]
        )

    @pytest.mark.asyncio
    async def test_url_refresh_returns_none_if_api_returns_empty(
        self, retry_handler, sample_task, mock_api_client
    ):
        """URL refresh returns None if API returns empty list."""
        mock_api_client.get_project_media.return_value = []

        refreshed_task = await retry_handler._try_refresh_url(sample_task)

        assert refreshed_task is None

    @pytest.mark.asyncio
    async def test_url_refresh_returns_none_if_no_download_link(
        self, retry_handler, sample_task, mock_api_client
    ):
        """URL refresh returns None if media has no download link."""
        mock_api_client.get_project_media.return_value = [{"id": 12345}]

        refreshed_task = await retry_handler._try_refresh_url(sample_task)

        assert refreshed_task is None

    @pytest.mark.asyncio
    async def test_url_refresh_returns_none_if_api_raises_exception(
        self, retry_handler, sample_task, mock_api_client
    ):
        """URL refresh returns None if API call raises exception."""
        mock_api_client.get_project_media.side_effect = Exception("API error")

        refreshed_task = await retry_handler._try_refresh_url(sample_task)

        assert refreshed_task is None

    @pytest.mark.asyncio
    async def test_url_refresh_returns_none_for_invalid_media_id(
        self, retry_handler, sample_task, mock_api_client
    ):
        """URL refresh returns None if media_id is not an integer."""
        sample_task.media_id = "not-an-integer"

        refreshed_task = await retry_handler._try_refresh_url(sample_task)

        assert refreshed_task is None
        mock_api_client.get_project_media.assert_not_called()

    @pytest.mark.asyncio
    async def test_url_refresh_returns_none_for_invalid_project_id(
        self, retry_handler, sample_task, mock_api_client
    ):
        """URL refresh returns None if project_id is not an integer."""
        sample_task.project_id = "not-an-integer"

        refreshed_task = await retry_handler._try_refresh_url(sample_task)

        assert refreshed_task is None
        mock_api_client.get_project_media.assert_not_called()


class TestDownloadRetryHandlerURLRefreshIntegration:
    """Tests for URL refresh integration with retry flow."""

    @pytest.fixture
    def mock_config(self):
        """Mock MessageConfig with standard retry configuration."""
        config = Mock()
        config.get_retry_delays.return_value = [300, 600, 1200, 2400]
        config.get_max_retries.return_value = 4
        config.get_retry_topic.return_value = "claimx.enrichment.retry"
        config.get_dlq_topic.return_value = "claimx.enrichment.dlq"
        config.get_topic.return_value = "enrichment_pending"
        return config

    @pytest.fixture
    def mock_api_client(self):
        """Mock ClaimX API client."""
        return AsyncMock(spec=ClaimXApiClient)

    @pytest.fixture
    def retry_handler(self, mock_config, mock_api_client):
        """DownloadRetryHandler with mocked producers and API client."""
        handler = DownloadRetryHandler(
            config=mock_config, api_client=mock_api_client, domain="claimx"
        )
        handler._retry_producer = AsyncMock()
        handler._dlq_producer = AsyncMock()
        return handler

    @pytest.fixture
    def sample_task(self):
        """Sample ClaimX download task."""
        return ClaimXDownloadTask(
            media_id="12345",
            project_id="67890",
            download_url="https://example.com/old_url.pdf?sig=expired",
            blob_path="claimx/projects/67890/media/12345.pdf",
            file_type="pdf",
            file_name="document.pdf",
            source_event_id="evt-123",
            retry_count=0,
            metadata={},
        )

    @pytest.mark.asyncio
    async def test_expired_url_triggers_refresh_and_retry(
        self, retry_handler, sample_task, mock_api_client
    ):
        """Expired URL error triggers refresh and sends refreshed task to retry."""
        error = Exception("HTTP 403 Forbidden - URL expired")
        new_url = "https://example.com/new_url.pdf?sig=fresh"
        mock_api_client.get_project_media.return_value = [
            {"full_download_link": new_url}
        ]

        await retry_handler.handle_failure(
            task=sample_task,
            error=error,
            error_category=ErrorCategory.TRANSIENT,
        )

        mock_api_client.get_project_media.assert_called_once()
        retry_handler._retry_producer.send.assert_called_once()
        retry_handler._dlq_producer.send.assert_not_called()

        retry_call = retry_handler._retry_producer.send.call_args
        retry_task = retry_call.kwargs["value"]
        assert retry_task.download_url == new_url

    @pytest.mark.asyncio
    async def test_failed_url_refresh_sends_to_dlq(
        self, retry_handler, sample_task, mock_api_client
    ):
        """Failed URL refresh sends task to DLQ."""
        error = Exception("HTTP 403 Forbidden - URL expired")
        mock_api_client.get_project_media.return_value = []

        await retry_handler.handle_failure(
            task=sample_task,
            error=error,
            error_category=ErrorCategory.TRANSIENT,
        )

        mock_api_client.get_project_media.assert_called_once()
        retry_handler._retry_producer.send.assert_not_called()
        retry_handler._dlq_producer.send.assert_called_once()

        dlq_call = retry_handler._dlq_producer.send.call_args
        headers = dlq_call.kwargs["headers"]
        assert headers["url_refresh_attempted"] == "True"


class TestDownloadRetryHandlerRetryExhaustion:
    """Tests for retry count tracking and exhaustion detection."""

    @pytest.fixture
    def mock_config(self):
        """Mock MessageConfig with standard retry configuration."""
        config = Mock()
        config.get_retry_delays.return_value = [300, 600, 1200, 2400]
        config.get_max_retries.return_value = 4
        config.get_retry_topic.return_value = "claimx.enrichment.retry"
        config.get_dlq_topic.return_value = "claimx.enrichment.dlq"
        config.get_topic.return_value = "enrichment_pending"
        return config

    @pytest.fixture
    def mock_api_client(self):
        """Mock ClaimX API client."""
        return AsyncMock(spec=ClaimXApiClient)

    @pytest.fixture
    def retry_handler(self, mock_config, mock_api_client):
        """DownloadRetryHandler with mocked producers."""
        handler = DownloadRetryHandler(
            config=mock_config, api_client=mock_api_client, domain="claimx"
        )
        handler._retry_producer = AsyncMock()
        handler._dlq_producer = AsyncMock()
        return handler

    @pytest.fixture
    def sample_task(self):
        """Sample ClaimX download task."""
        return ClaimXDownloadTask(
            media_id="12345",
            project_id="67890",
            download_url="https://example.com/file.pdf",
            blob_path="claimx/projects/67890/media/12345.pdf",
            file_type="pdf",
            file_name="document.pdf",
            source_event_id="evt-123",
            retry_count=0,
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


class TestDownloadRetryHandlerMessageKeys:
    """Tests for message key usage (source_event_id)."""

    @pytest.fixture
    def mock_config(self):
        """Mock MessageConfig with standard retry configuration."""
        config = Mock()
        config.get_retry_delays.return_value = [300, 600, 1200, 2400]
        config.get_max_retries.return_value = 4
        config.get_retry_topic.return_value = "claimx.enrichment.retry"
        config.get_dlq_topic.return_value = "claimx.enrichment.dlq"
        config.get_topic.return_value = "enrichment_pending"
        return config

    @pytest.fixture
    def mock_api_client(self):
        """Mock ClaimX API client."""
        return AsyncMock(spec=ClaimXApiClient)

    @pytest.fixture
    def retry_handler(self, mock_config, mock_api_client):
        """DownloadRetryHandler with mocked producers."""
        handler = DownloadRetryHandler(
            config=mock_config, api_client=mock_api_client, domain="claimx"
        )
        handler._retry_producer = AsyncMock()
        handler._dlq_producer = AsyncMock()
        return handler

    @pytest.fixture
    def sample_task(self):
        """Sample ClaimX download task."""
        return ClaimXDownloadTask(
            media_id="12345",
            project_id="67890",
            download_url="https://example.com/file.pdf",
            blob_path="claimx/projects/67890/media/12345.pdf",
            file_type="pdf",
            file_name="document.pdf",
            source_event_id="evt-abc-123",
            retry_count=0,
            metadata={},
        )

    @pytest.mark.asyncio
    async def test_retry_message_key_is_source_event_id(
        self, retry_handler, sample_task
    ):
        """Retry message key is source_event_id for consistent partitioning."""
        await retry_handler.handle_failure(
            task=sample_task,
            error=ConnectionError("Error"),
            error_category=ErrorCategory.TRANSIENT,
        )

        retry_call = retry_handler._retry_producer.send.call_args
        assert retry_call.kwargs["key"] == "evt-abc-123"

    @pytest.mark.asyncio
    async def test_dlq_message_key_is_source_event_id(
        self, retry_handler, sample_task
    ):
        """DLQ message key is source_event_id for consistent partitioning."""
        sample_task.retry_count = 4

        await retry_handler.handle_failure(
            task=sample_task,
            error=ConnectionError("Error"),
            error_category=ErrorCategory.TRANSIENT,
        )

        dlq_call = retry_handler._dlq_producer.send.call_args
        assert dlq_call.kwargs["key"] == "evt-abc-123"

    @pytest.mark.asyncio
    async def test_retry_headers_include_original_key(
        self, retry_handler, sample_task
    ):
        """Retry headers include original_key as source_event_id."""
        await retry_handler.handle_failure(
            task=sample_task,
            error=ConnectionError("Error"),
            error_category=ErrorCategory.TRANSIENT,
        )

        retry_call = retry_handler._retry_producer.send.call_args
        headers = retry_call.kwargs["headers"]
        assert headers["original_key"] == "evt-abc-123"


class TestDownloadRetryHandlerTaskImmutability:
    """Tests for task deep copy behavior."""

    @pytest.fixture
    def mock_config(self):
        """Mock MessageConfig with standard retry configuration."""
        config = Mock()
        config.get_retry_delays.return_value = [300, 600, 1200, 2400]
        config.get_max_retries.return_value = 4
        config.get_retry_topic.return_value = "claimx.enrichment.retry"
        config.get_dlq_topic.return_value = "claimx.enrichment.dlq"
        config.get_topic.return_value = "enrichment_pending"
        return config

    @pytest.fixture
    def mock_api_client(self):
        """Mock ClaimX API client."""
        return AsyncMock(spec=ClaimXApiClient)

    @pytest.fixture
    def retry_handler(self, mock_config, mock_api_client):
        """DownloadRetryHandler with mocked producers."""
        handler = DownloadRetryHandler(
            config=mock_config, api_client=mock_api_client, domain="claimx"
        )
        handler._retry_producer = AsyncMock()
        handler._dlq_producer = AsyncMock()
        return handler

    @pytest.fixture
    def sample_task(self):
        """Sample ClaimX download task."""
        return ClaimXDownloadTask(
            media_id="12345",
            project_id="67890",
            download_url="https://example.com/file.pdf",
            blob_path="claimx/projects/67890/media/12345.pdf",
            file_type="pdf",
            file_name="document.pdf",
            source_event_id="evt-123",
            retry_count=0,
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
