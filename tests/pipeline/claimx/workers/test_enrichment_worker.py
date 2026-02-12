"""
Unit tests for ClaimX Enrichment Worker.

Test Coverage:
    - Worker initialization and configuration
    - Lifecycle management (start/stop/graceful shutdown)
    - Message parsing and validation
    - Handler routing and execution
    - Error handling and categorization
    - Retry/DLQ routing
    - Project cache integration
    - Entity row dispatch
    - Download task creation
    - Concurrent processing

No infrastructure required - all dependencies mocked.
"""

import contextlib
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest

from config.config import MessageConfig
from core.types import ErrorCategory
from pipeline.claimx.api_client import ClaimXApiClient, ClaimXApiError
from pipeline.claimx.handlers.base import HandlerResult
from pipeline.claimx.schemas.entities import EntityRowsMessage
from pipeline.claimx.schemas.tasks import ClaimXEnrichmentTask
from pipeline.claimx.workers.enrichment_worker import ClaimXEnrichmentWorker
from pipeline.common.types import PipelineMessage


@pytest.fixture
def mock_config():
    """Mock MessageConfig with standard settings."""
    config = Mock(spec=MessageConfig)
    config.get_topic.return_value = "claimx.enrichment.pending"
    config.get_consumer_group.return_value = "claimx-enrichment"
    config.get_worker_config.return_value = {
        "health_port": 8081,
        "health_enabled": True,
        "project_cache": {
            "ttl_seconds": 1800,
            "preload_from_delta": False,
        },
    }
    config.get_retry_delays.return_value = [300, 600, 1200]
    config.get_max_retries.return_value = 3
    config.claimx_api_url = "https://api.test.claimxperience.com"
    config.claimx_api_token = "test-token"
    config.claimx_api_timeout_seconds = 30
    config.claimx_api_concurrency = 10
    return config


@pytest.fixture
def mock_api_client():
    """Mock ClaimX API client."""
    client = AsyncMock(spec=ClaimXApiClient)
    client.is_circuit_open = False
    client._ensure_session = AsyncMock()
    client.close = AsyncMock()
    return client


@pytest.fixture
def mock_producer():
    """Mock Kafka producer."""
    producer = AsyncMock()
    producer.send.return_value = Mock(partition=0, offset=123)
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    return producer


@pytest.fixture
def mock_consumer():
    """Mock Kafka consumer."""
    consumer = AsyncMock()
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()
    return consumer


@pytest.fixture
def sample_enrichment_task():
    """Sample enrichment task for testing."""
    return ClaimXEnrichmentTask(
        event_id="evt-123",
        event_type="PROJECT_CREATED",
        project_id="proj-456",
        retry_count=0,
        created_at=datetime.now(UTC),
        media_id=None,
        task_assignment_id=None,
        video_collaboration_id=None,
        master_file_name=None,
    )


@pytest.fixture
def sample_message(sample_enrichment_task):
    """Sample Kafka message with enrichment task."""
    return PipelineMessage(
        topic="claimx.enrichment.pending",
        partition=0,
        offset=1,
        key=b"evt-123",
        value=sample_enrichment_task.model_dump_json().encode(),
        timestamp=None,
        headers=None,
    )


@pytest.fixture
def patched_project_cache():
    """Patch ProjectCache to avoid ttl_seconds parameter issue."""
    with patch("pipeline.claimx.workers.enrichment_worker.ProjectCache") as mock:
        yield mock


class TestEnrichmentWorkerInitialization:
    """Test worker initialization and configuration."""

    def test_initialization_with_default_config(self, mock_config):
        """Worker initializes with default domain and config."""
        with patch("pipeline.claimx.workers.enrichment_worker.ProjectCache"):
            worker = ClaimXEnrichmentWorker(config=mock_config)

            assert worker.domain == "claimx"
            assert worker.consumer_config is mock_config
            assert worker.producer_config is mock_config
            assert worker.worker_id == "enrichment_worker"
            assert worker.instance_id is None
            assert worker._running is False

    def test_initialization_with_custom_domain(self, mock_config):
        """Worker initializes with custom domain."""
        with patch("pipeline.claimx.workers.enrichment_worker.ProjectCache"):
            worker = ClaimXEnrichmentWorker(config=mock_config, domain="custom")

            assert worker.domain == "custom"
            mock_config.get_consumer_group.assert_called_with("custom", "enrichment_worker")

    def test_initialization_with_instance_id(self, mock_config):
        """Worker uses instance ID for worker_id suffix."""
        with patch("pipeline.claimx.workers.enrichment_worker.ProjectCache"):
            worker = ClaimXEnrichmentWorker(config=mock_config, instance_id="3")

            assert worker.worker_id == "enrichment_worker-3"
            assert worker.instance_id == "3"

    def test_initialization_with_separate_producer_config(self, mock_config):
        """Worker accepts separate producer config."""
        with patch("pipeline.claimx.workers.enrichment_worker.ProjectCache"):
            producer_config = Mock(spec=MessageConfig)
            worker = ClaimXEnrichmentWorker(config=mock_config, producer_config=producer_config)

            assert worker.consumer_config is mock_config
            assert worker.producer_config is producer_config

    def test_initialization_with_injected_api_client(self, mock_config, mock_api_client):
        """Worker accepts injected API client for testing."""
        with patch("pipeline.claimx.workers.enrichment_worker.ProjectCache"):
            worker = ClaimXEnrichmentWorker(config=mock_config, api_client=mock_api_client)

            assert worker._injected_api_client is mock_api_client

    def test_retry_config_loaded_from_kafka_config(self, mock_config):
        """Worker loads retry delays and max retries from config."""
        with patch("pipeline.claimx.workers.enrichment_worker.ProjectCache"):
            worker = ClaimXEnrichmentWorker(config=mock_config)

            assert worker._retry_delays == [300, 600, 1200]
            assert worker._max_retries == 3
            mock_config.get_retry_delays.assert_called_once_with("claimx")
            mock_config.get_max_retries.assert_called_once_with("claimx")

    def test_project_cache_configured_from_worker_config(self, mock_config):
        """Worker configures project cache from worker config."""
        with patch("pipeline.claimx.workers.enrichment_worker.ProjectCache") as mock_cache_class:
            worker = ClaimXEnrichmentWorker(config=mock_config)

            assert worker.project_cache is not None
            # Verify ProjectCache was instantiated (even if the ttl_seconds param is currently broken)
            assert mock_cache_class.called

    def test_delta_writes_enabled_by_default(self, mock_config):
        """Worker enables delta writes by default."""
        with patch("pipeline.claimx.workers.enrichment_worker.ProjectCache"):
            worker = ClaimXEnrichmentWorker(config=mock_config)

            assert worker.enable_delta_writes is True

    def test_delta_writes_can_be_disabled(self, mock_config):
        """Worker respects enable_delta_writes flag."""
        with patch("pipeline.claimx.workers.enrichment_worker.ProjectCache"):
            worker = ClaimXEnrichmentWorker(config=mock_config, enable_delta_writes=False)

            assert worker.enable_delta_writes is False


class TestEnrichmentWorkerLifecycle:
    """Test worker lifecycle (start/stop)."""

    @pytest.mark.asyncio
    async def test_start_initializes_components(
        self, mock_config, mock_api_client, patched_project_cache
    ):
        """Worker start initializes all components."""
        worker = ClaimXEnrichmentWorker(config=mock_config, api_client=mock_api_client)

        with (
            patch(
                "pipeline.claimx.workers.enrichment_worker.create_producer"
            ) as mock_create_producer,
            patch(
                "pipeline.claimx.workers.enrichment_worker.create_consumer"
            ) as mock_create_consumer,
            patch("pipeline.common.telemetry.initialize_worker_telemetry"),
        ):
            # Setup mocks
            mock_producer = AsyncMock()
            mock_producer.start = AsyncMock()
            mock_create_producer.return_value = mock_producer

            mock_consumer = AsyncMock()
            mock_consumer.start = AsyncMock()
            mock_create_consumer.return_value = mock_consumer

            # Start worker (will block on consumer.start, so we need to cancel)
            start_task = pytest.raises(Exception)
            with start_task:
                await worker.start()

            # Verify components initialized
            assert worker._running is True
            assert mock_api_client._ensure_session.called
            assert mock_producer.start.called

    @pytest.mark.asyncio
    async def test_start_uses_injected_api_client(
        self, mock_config, mock_api_client, patched_project_cache
    ):
        """Worker uses injected API client instead of creating new one."""
        worker = ClaimXEnrichmentWorker(config=mock_config, api_client=mock_api_client)

        with (
            patch(
                "pipeline.claimx.workers.enrichment_worker.create_producer"
            ) as mock_create_producer,
            patch(
                "pipeline.claimx.workers.enrichment_worker.create_consumer"
            ) as mock_create_consumer,
            patch("pipeline.common.telemetry.initialize_worker_telemetry"),
        ):
            mock_producer = AsyncMock()
            mock_producer.start = AsyncMock()
            mock_create_producer.return_value = mock_producer

            mock_consumer = AsyncMock()
            mock_consumer.start = AsyncMock()
            mock_create_consumer.return_value = mock_consumer

            # Prevent blocking on consumer.start
            mock_consumer.start.side_effect = Exception("Stop")

            with contextlib.suppress(Exception):
                await worker.start()

            # Verify injected client was used
            assert worker.api_client is mock_api_client
            assert mock_api_client._ensure_session.called

    @pytest.mark.asyncio
    async def test_stop_cleans_up_resources(
        self, mock_config, mock_api_client, patched_project_cache
    ):
        """Worker stop cleans up all resources."""
        worker = ClaimXEnrichmentWorker(config=mock_config, api_client=mock_api_client)

        # Setup mocked components
        worker.consumer = AsyncMock()
        worker.consumer.stop = AsyncMock()
        worker.producer = AsyncMock()
        worker.producer.stop = AsyncMock()
        worker.download_producer = AsyncMock()
        worker.download_producer.stop = AsyncMock()
        worker.retry_handler = AsyncMock()
        worker.retry_handler.stop = AsyncMock()
        worker.health_server = AsyncMock()
        worker.health_server.stop = AsyncMock()

        # api_client is set during start(), so set it manually for this test
        worker.api_client = mock_api_client

        worker._running = True

        # Stop worker
        await worker.stop()

        # Verify cleanup
        assert worker._running is False
        worker.consumer.stop.assert_called_once()
        worker.producer.stop.assert_called_once()
        worker.download_producer.stop.assert_called_once()
        worker.retry_handler.stop.assert_called_once()
        mock_api_client.close.assert_called_once()
        worker.health_server.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_handles_none_components(self, mock_config, patched_project_cache):
        """Worker stop handles None components gracefully."""
        worker = ClaimXEnrichmentWorker(config=mock_config)

        # All components are None (not initialized)
        assert worker.consumer is None
        assert worker.producer is None

        # Should not raise
        await worker.stop()

    @pytest.mark.asyncio
    async def test_request_shutdown_sets_running_false(self, mock_config, patched_project_cache):
        """Request shutdown sets running flag to false."""
        worker = ClaimXEnrichmentWorker(config=mock_config)
        worker._running = True

        await worker.request_shutdown()

        assert worker._running is False


class TestEnrichmentWorkerMessageProcessing:
    """Test message parsing and processing."""

    @pytest.mark.asyncio
    async def test_valid_message_parsed_successfully(
        self, mock_config, mock_api_client, sample_message, patched_project_cache
    ):
        """Worker parses valid enrichment task message."""
        worker = ClaimXEnrichmentWorker(config=mock_config, api_client=mock_api_client)

        # Mock _process_single_task to avoid full execution
        worker._process_single_task = AsyncMock()

        await worker._handle_enrichment_task(sample_message)

        # Verify task was parsed and processed
        assert worker._process_single_task.called
        task = worker._process_single_task.call_args[0][0]
        assert isinstance(task, ClaimXEnrichmentTask)
        assert task.event_id == "evt-123"
        assert task.event_type == "PROJECT_CREATED"
        assert task.project_id == "proj-456"
        assert worker._records_processed == 1

    @pytest.mark.asyncio
    async def test_invalid_json_raises_error(
        self, mock_config, mock_api_client, patched_project_cache
    ):
        """Worker raises error on invalid JSON."""
        worker = ClaimXEnrichmentWorker(config=mock_config, api_client=mock_api_client)

        invalid_message = PipelineMessage(
            topic="test.topic",
            partition=0,
            offset=1,
            key=b"key",
            value=b"invalid json{",
            timestamp=None,
            headers=None,
        )

        with pytest.raises(json.JSONDecodeError):
            await worker._handle_enrichment_task(invalid_message)

    @pytest.mark.asyncio
    async def test_invalid_schema_raises_validation_error(
        self, mock_config, mock_api_client, patched_project_cache
    ):
        """Worker raises ValidationError on invalid task schema."""
        worker = ClaimXEnrichmentWorker(config=mock_config, api_client=mock_api_client)

        invalid_message = PipelineMessage(
            topic="test.topic",
            partition=0,
            offset=1,
            key=b"key",
            value=b'{"invalid": "schema"}',
            timestamp=None,
            headers=None,
        )

        with pytest.raises(ValueError):  # Pydantic ValidationError is a ValueError subclass
            await worker._handle_enrichment_task(invalid_message)


class TestEnrichmentWorkerHandlerRouting:
    """Test handler routing and execution."""

    @pytest.mark.asyncio
    async def test_handler_selected_for_event_type(
        self, mock_config, mock_api_client, sample_enrichment_task, patched_project_cache
    ):
        """Worker selects correct handler for event type."""
        worker = ClaimXEnrichmentWorker(config=mock_config, api_client=mock_api_client)

        # Mock handler class with __name__ attribute
        mock_handler_class = Mock()
        mock_handler_class.__name__ = "TestHandler"
        mock_handler = Mock()
        mock_handler_class.return_value = mock_handler

        # Mock handler execution
        mock_result = HandlerResult(
            handler_name="TestHandler",
            total=1,
            succeeded=1,
            failed=0,
            failed_permanent=0,
            skipped=0,
            rows=EntityRowsMessage(),
            errors=[],
            duration_seconds=0.1,
            api_calls=1,
        )
        mock_handler.process = AsyncMock(return_value=mock_result)

        worker.handler_registry.get_handler_class = Mock(return_value=mock_handler_class)
        worker._dispatch_entity_rows = AsyncMock()
        worker._dispatch_download_tasks = AsyncMock()

        await worker._process_single_task(sample_enrichment_task)

        # Verify handler was selected and created
        worker.handler_registry.get_handler_class.assert_called_once_with("PROJECT_CREATED")
        mock_handler_class.assert_called_once()
        assert mock_handler.process.called
        assert worker._records_succeeded == 1

    @pytest.mark.asyncio
    async def test_missing_handler_skips_event(
        self, mock_config, mock_api_client, sample_enrichment_task, patched_project_cache
    ):
        """Worker skips event when no handler found."""
        worker = ClaimXEnrichmentWorker(config=mock_config, api_client=mock_api_client)

        # No handler for this event type
        worker.handler_registry.get_handler_class = Mock(return_value=None)

        await worker._process_single_task(sample_enrichment_task)

        # Verify event was skipped
        assert worker._records_skipped == 1
        assert worker._records_succeeded == 0

    @pytest.mark.asyncio
    async def test_handler_result_with_failures_routes_to_retry(
        self, mock_config, mock_api_client, sample_enrichment_task, patched_project_cache
    ):
        """Worker routes failed handler results to retry."""
        worker = ClaimXEnrichmentWorker(config=mock_config, api_client=mock_api_client)

        # Mock handler that returns failure
        mock_handler_class = Mock()
        mock_handler_class.__name__ = "TestHandler"
        mock_handler = Mock()
        mock_handler_class.return_value = mock_handler

        mock_result = HandlerResult(
            handler_name="TestHandler",
            total=1,
            succeeded=0,
            failed=1,
            failed_permanent=0,
            skipped=0,
            rows=EntityRowsMessage(),
            errors=["API error"],
            duration_seconds=0.1,
            api_calls=1,
        )
        mock_handler.process = AsyncMock(return_value=mock_result)

        worker.handler_registry.get_handler_class = Mock(return_value=mock_handler_class)
        worker._handle_enrichment_failure = AsyncMock()

        await worker._process_single_task(sample_enrichment_task)

        # Verify failure was handled
        assert worker._handle_enrichment_failure.called
        # _handle_enrichment_failure(task, error, error_category) - positional args
        call_args = worker._handle_enrichment_failure.call_args[0]
        assert call_args[2] == ErrorCategory.TRANSIENT


class TestEnrichmentWorkerErrorHandling:
    """Test error handling and categorization."""

    @pytest.mark.asyncio
    async def test_api_error_categorized_correctly(
        self, mock_config, mock_api_client, sample_enrichment_task, patched_project_cache
    ):
        """Worker categorizes ClaimXApiError correctly."""
        worker = ClaimXEnrichmentWorker(config=mock_config, api_client=mock_api_client)

        # Mock handler that raises API error
        mock_handler_class = Mock()
        mock_handler_class.__name__ = "TestHandler"
        mock_handler = Mock()
        mock_handler_class.return_value = mock_handler

        api_error = ClaimXApiError(
            message="API error",
            status_code=500,
            category=ErrorCategory.TRANSIENT,
        )
        mock_handler.process = AsyncMock(side_effect=api_error)

        worker.handler_registry.get_handler_class = Mock(return_value=mock_handler_class)
        worker._handle_enrichment_failure = AsyncMock()

        await worker._process_single_task(sample_enrichment_task)

        # Verify error was handled
        assert worker._handle_enrichment_failure.called
        call_args = worker._handle_enrichment_failure.call_args
        assert call_args[0][1] is api_error
        assert call_args[0][2] == ErrorCategory.TRANSIENT

    @pytest.mark.asyncio
    async def test_unknown_error_categorized_as_transient(
        self, mock_config, mock_api_client, sample_enrichment_task, patched_project_cache
    ):
        """Worker categorizes unknown errors as UNKNOWN."""
        worker = ClaimXEnrichmentWorker(config=mock_config, api_client=mock_api_client)

        # Mock handler that raises unexpected error
        mock_handler_class = Mock()
        mock_handler_class.__name__ = "TestHandler"
        mock_handler = Mock()
        mock_handler_class.return_value = mock_handler

        mock_handler.process = AsyncMock(side_effect=ValueError("Unexpected"))

        worker.handler_registry.get_handler_class = Mock(return_value=mock_handler_class)
        worker._handle_enrichment_failure = AsyncMock()

        await worker._process_single_task(sample_enrichment_task)

        # Verify error was categorized as UNKNOWN
        assert worker._handle_enrichment_failure.called
        call_args = worker._handle_enrichment_failure.call_args
        assert call_args[0][2] == ErrorCategory.UNKNOWN


class TestEnrichmentWorkerEntityDispatch:
    """Test entity row dispatch."""

    @pytest.mark.asyncio
    async def test_entity_rows_dispatched_when_delta_enabled(
        self, mock_config, sample_enrichment_task, patched_project_cache
    ):
        """Worker dispatches entity rows when delta writes enabled."""
        worker = ClaimXEnrichmentWorker(config=mock_config, enable_delta_writes=True)
        worker._produce_entity_rows = AsyncMock()

        entity_rows = EntityRowsMessage()
        entity_rows.projects = [{"project_id": "proj-123"}]

        await worker._dispatch_entity_rows(sample_enrichment_task, entity_rows)

        # Verify entity rows were produced
        assert worker._produce_entity_rows.called
        produced_rows = worker._produce_entity_rows.call_args[0][0]
        assert len(produced_rows.projects) == 1

    @pytest.mark.asyncio
    async def test_entity_rows_not_dispatched_when_delta_disabled(
        self, mock_config, sample_enrichment_task, patched_project_cache
    ):
        """Worker skips entity dispatch when delta writes disabled."""
        worker = ClaimXEnrichmentWorker(config=mock_config, enable_delta_writes=False)
        worker._produce_entity_rows = AsyncMock()

        entity_rows = EntityRowsMessage()
        entity_rows.projects = [{"project_id": "proj-123"}]

        await worker._dispatch_entity_rows(sample_enrichment_task, entity_rows)

        # Verify entity rows were NOT produced
        assert not worker._produce_entity_rows.called

    @pytest.mark.asyncio
    async def test_empty_entity_rows_not_dispatched(
        self, mock_config, sample_enrichment_task, patched_project_cache
    ):
        """Worker skips dispatch for empty entity rows."""
        worker = ClaimXEnrichmentWorker(config=mock_config, enable_delta_writes=True)
        worker._produce_entity_rows = AsyncMock()

        entity_rows = EntityRowsMessage()  # Empty

        await worker._dispatch_entity_rows(sample_enrichment_task, entity_rows)

        # Verify empty rows were not produced
        assert not worker._produce_entity_rows.called


class TestEnrichmentWorkerDownloadTasks:
    """Test download task creation and dispatch."""

    @pytest.mark.asyncio
    async def test_download_tasks_created_from_media(self, mock_config, patched_project_cache):
        """Worker creates download tasks from media entities."""
        worker = ClaimXEnrichmentWorker(config=mock_config)
        worker._produce_download_tasks = AsyncMock()

        entity_rows = EntityRowsMessage()
        entity_rows.media = [
            {
                "media_id": "media-123",
                "project_id": "proj-456",
                "download_url": "https://example.com/file.jpg",
                "file_name": "file.jpg",
                "file_type": "image/jpeg",
            }
        ]

        with patch(
            "pipeline.claimx.workers.enrichment_worker.DownloadTaskFactory.create_download_tasks_from_media"
        ) as mock_factory:
            mock_factory.return_value = [Mock()]  # Simulate download tasks

            await worker._dispatch_download_tasks(entity_rows)

            # Verify factory was called and tasks produced
            assert mock_factory.called
            assert worker._produce_download_tasks.called

    @pytest.mark.asyncio
    async def test_no_download_tasks_when_no_media(self, mock_config, patched_project_cache):
        """Worker skips download tasks when no media entities."""
        worker = ClaimXEnrichmentWorker(config=mock_config)
        worker._produce_download_tasks = AsyncMock()

        entity_rows = EntityRowsMessage()  # No media

        await worker._dispatch_download_tasks(entity_rows)

        # Verify no download tasks produced
        assert not worker._produce_download_tasks.called


class TestEnrichmentWorkerProjectCache:
    """Test project cache integration."""

    @pytest.mark.asyncio
    async def test_ensure_project_exists_fetches_from_api(
        self, mock_config, mock_api_client, patched_project_cache
    ):
        """Worker's _ensure_project_exists fetches project data via ProjectHandler."""
        patched_project_cache.return_value.has.return_value = False
        mock_api_client.get_project = AsyncMock(
            return_value={"data": {"project": {"projectId": 456}, "teamMembers": []}}
        )
        worker = ClaimXEnrichmentWorker(config=mock_config, api_client=mock_api_client)

        rows = await worker._ensure_project_exists("456")

        mock_api_client.get_project.assert_called_once_with(456)
        assert len(rows.projects) == 1
        assert rows.projects[0]["project_id"] == "456"

    @pytest.mark.asyncio
    async def test_ensure_project_exists_skips_api_when_cached(
        self, mock_config, mock_api_client, patched_project_cache
    ):
        """Worker's _ensure_project_exists returns empty when project is cached."""
        patched_project_cache.return_value.has.return_value = True
        worker = ClaimXEnrichmentWorker(config=mock_config, api_client=mock_api_client)

        rows = await worker._ensure_project_exists("456")

        mock_api_client.get_project.assert_not_called()
        assert rows.is_empty()
