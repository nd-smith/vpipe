"""
Unit tests for ClaimX Event Ingester Worker.

Test Coverage:
    - Worker initialization and configuration
    - Lifecycle management (start/stop/graceful shutdown)
    - Event message parsing and validation
    - Event deduplication logic
    - Enrichment task creation
    - Producer interaction
    - Background task tracking
    - Periodic cycle output

No infrastructure required - all dependencies mocked.
"""

import contextlib
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest

from config.config import MessageConfig
from pipeline.claimx.schemas.events import ClaimXEventMessage
from pipeline.claimx.schemas.tasks import ClaimXEnrichmentTask
from pipeline.claimx.workers.event_ingester import ClaimXEventIngesterWorker
from pipeline.common.types import PipelineMessage


@pytest.fixture
def mock_config():
    """Mock MessageConfig with standard settings."""
    config = Mock(spec=MessageConfig)
    config.get_topic.return_value = "claimx.events.raw"
    config.get_consumer_group.return_value = "claimx-event-ingester"
    config.get_worker_config.return_value = {
        "health_port": 8080,
        "health_enabled": True,
    }
    return config


@pytest.fixture
def sample_eventhouse_row():
    """Sample Eventhouse row data."""
    return {
        "event_id": "evt-123",
        "event_type": "PROJECT_CREATED",
        "project_id": "proj-456",
        "media_id": None,
        "task_assignment_id": None,
        "video_collaboration_id": None,
        "master_file_name": None,
        "event_timestamp": datetime.now(UTC).isoformat(),
    }


@pytest.fixture
def sample_message(sample_eventhouse_row):
    """Sample Kafka message with Eventhouse event."""
    return PipelineMessage(
        topic="claimx.events.raw",
        partition=0,
        offset=1,
        key=b"evt-123",
        value=json.dumps(sample_eventhouse_row).encode(),
        timestamp=None,
        headers=None,
    )


class TestEventIngesterInitialization:
    """Test worker initialization and configuration."""

    def test_initialization_with_default_config(self, mock_config):
        """Worker initializes with default domain and config."""
        worker = ClaimXEventIngesterWorker(config=mock_config)

        assert worker.domain == "claimx"
        assert worker.consumer_config is mock_config
        assert worker.producer_config is mock_config
        assert worker.worker_id == "event_ingester"
        assert worker.instance_id is None

    def test_initialization_with_custom_domain(self, mock_config):
        """Worker initializes with custom domain."""
        worker = ClaimXEventIngesterWorker(config=mock_config, domain="custom")

        assert worker.domain == "custom"
        mock_config.get_consumer_group.assert_called_with("custom", "event_ingester")

    def test_initialization_with_instance_id(self, mock_config):
        """Worker uses instance ID for worker_id suffix."""
        worker = ClaimXEventIngesterWorker(config=mock_config, instance_id="2")

        assert worker.worker_id == "event_ingester-2"
        assert worker.instance_id == "2"

    def test_initialization_with_separate_producer_config(self, mock_config):
        """Worker accepts separate producer config."""
        producer_config = Mock(spec=MessageConfig)
        worker = ClaimXEventIngesterWorker(config=mock_config, producer_config=producer_config)

        assert worker.consumer_config is mock_config
        assert worker.producer_config is producer_config

    def test_initialization_with_custom_enrichment_topic(self, mock_config):
        """Worker accepts custom enrichment topic."""
        worker = ClaimXEventIngesterWorker(config=mock_config, enrichment_topic="custom.enrichment")

        assert worker.enrichment_topic == "custom.enrichment"

    def test_deduplication_set_initialized_empty(self, mock_config):
        """Worker initializes with empty deduplication cache."""
        worker = ClaimXEventIngesterWorker(config=mock_config)

        assert isinstance(worker._dedup_cache, dict)
        assert len(worker._dedup_cache) == 0
        assert worker._dedup_store is None

    def test_counters_initialized_to_zero(self, mock_config):
        """Worker initializes counters to zero."""
        worker = ClaimXEventIngesterWorker(config=mock_config)

        assert worker._records_processed == 0
        assert worker._records_succeeded == 0
        assert worker._records_deduplicated == 0
        assert worker._cycle_count == 0


class TestEventIngesterLifecycle:
    """Test worker lifecycle (start/stop)."""

    @pytest.mark.asyncio
    async def test_start_initializes_components(self, mock_config):
        """Worker start initializes all components."""
        worker = ClaimXEventIngesterWorker(config=mock_config)

        with (
            patch("pipeline.claimx.workers.event_ingester.create_producer") as mock_create_producer,
            patch("pipeline.claimx.workers.event_ingester.create_consumer") as mock_create_consumer,
            patch("pipeline.common.telemetry.initialize_worker_telemetry"),
        ):
            # Setup mocks
            mock_producer = AsyncMock()
            mock_producer.start = AsyncMock()
            mock_create_producer.return_value = mock_producer

            mock_consumer = AsyncMock()
            mock_consumer.start = AsyncMock()
            mock_create_consumer.return_value = mock_consumer

            # Start worker (will block on consumer.start, so we cancel it)
            mock_consumer.start.side_effect = Exception("Stop")

            with contextlib.suppress(Exception):
                await worker.start()

            # Verify components initialized
            assert worker.producer is not None
            assert mock_producer.start.called

    @pytest.mark.asyncio
    async def test_stop_cleans_up_resources(self, mock_config):
        """Worker stop cleans up all resources."""
        worker = ClaimXEventIngesterWorker(config=mock_config)

        # Setup mocked components
        worker.consumer = AsyncMock()
        worker.consumer.stop = AsyncMock()
        worker.producer = AsyncMock()
        worker.producer.stop = AsyncMock()
        worker.health_server = AsyncMock()
        worker.health_server.stop = AsyncMock()

        # Stop worker
        await worker.stop()

        # Verify cleanup
        worker.consumer.stop.assert_called_once()
        worker.producer.stop.assert_called_once()
        worker.health_server.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_handles_none_components(self, mock_config):
        """Worker stop handles None components gracefully."""
        worker = ClaimXEventIngesterWorker(config=mock_config)

        # All components are None (not initialized)
        assert worker.consumer is None
        assert worker.producer is None

        # Should not raise
        await worker.stop()

    @pytest.mark.asyncio
    async def test_stop_cancels_periodic_cycle_task(self, mock_config):
        """Worker stop cancels periodic cycle output task."""
        import asyncio

        worker = ClaimXEventIngesterWorker(config=mock_config)

        # Create an actual asyncio task that we can cancel
        async def dummy_task():
            await asyncio.sleep(100)  # Long sleep

        worker._cycle_task = asyncio.create_task(dummy_task())

        # Ensure task is running
        assert not worker._cycle_task.done()

        await worker.stop()

        # Verify cycle task was cancelled
        assert worker._cycle_task.cancelled()


class TestEventIngesterMessageProcessing:
    """Test event message parsing and processing."""

    @pytest.mark.asyncio
    async def test_valid_message_parsed_successfully(self, mock_config, sample_message):
        """Worker parses valid event message."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker._create_enrichment_task = AsyncMock()
        worker._is_duplicate = AsyncMock(return_value=False)
        worker._mark_processed = AsyncMock()

        await worker._handle_event_message(sample_message)

        # Verify event was parsed and processed
        assert worker._create_enrichment_task.called
        event = worker._create_enrichment_task.call_args[0][0]
        assert isinstance(event, ClaimXEventMessage)
        assert event.event_id == "evt-123"
        assert event.event_type == "PROJECT_CREATED"
        assert event.project_id == "proj-456"
        assert worker._records_processed == 1

    @pytest.mark.asyncio
    async def test_invalid_json_raises_error(self, mock_config):
        """Worker raises error on invalid JSON."""
        worker = ClaimXEventIngesterWorker(config=mock_config)

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
            await worker._handle_event_message(invalid_message)

    @pytest.mark.asyncio
    async def test_invalid_schema_raises_validation_error(self, mock_config):
        """Worker raises ValidationError on invalid event schema."""
        worker = ClaimXEventIngesterWorker(config=mock_config)

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
            await worker._handle_event_message(invalid_message)


class TestEventIngesterDeduplication:
    """Test event deduplication logic."""

    @pytest.mark.asyncio
    async def test_duplicate_event_skipped(self, mock_config, sample_message):
        """Worker skips duplicate events."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker._create_enrichment_task = AsyncMock()

        # Mark event as already processed (in cache with recent timestamp)
        import time

        worker._dedup_cache["evt-123"] = time.time()

        await worker._handle_event_message(sample_message)

        # Verify event was skipped
        assert not worker._create_enrichment_task.called
        assert worker._records_deduplicated == 1
        assert worker._records_processed == 0

    @pytest.mark.asyncio
    async def test_new_event_processed(self, mock_config, sample_message):
        """Worker processes new (non-duplicate) events."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker._create_enrichment_task = AsyncMock()

        # Event not in cache
        assert "evt-123" not in worker._dedup_cache

        await worker._handle_event_message(sample_message)

        # Verify event was processed
        assert worker._create_enrichment_task.called
        assert worker._records_processed == 1
        assert worker._records_deduplicated == 0

    @pytest.mark.asyncio
    async def test_processed_event_marked_in_dedup_set(self, mock_config, sample_message):
        """Worker marks processed events in deduplication cache."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker._create_enrichment_task = AsyncMock()

        await worker._handle_event_message(sample_message)

        # Verify event was marked in cache
        assert "evt-123" in worker._dedup_cache

    @pytest.mark.asyncio
    async def test_is_duplicate_returns_true_for_seen_event(self, mock_config):
        """_is_duplicate returns True for seen events."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        import time

        worker._dedup_cache["evt-123"] = time.time()

        assert await worker._is_duplicate("evt-123") is True

    @pytest.mark.asyncio
    async def test_is_duplicate_returns_false_for_new_event(self, mock_config):
        """_is_duplicate returns False for new events."""
        worker = ClaimXEventIngesterWorker(config=mock_config)

        assert await worker._is_duplicate("evt-456") is False

    @pytest.mark.asyncio
    async def test_mark_processed_adds_to_dedup_set(self, mock_config):
        """_mark_processed adds event to deduplication cache."""
        worker = ClaimXEventIngesterWorker(config=mock_config)

        await worker._mark_processed("evt-789")

        assert "evt-789" in worker._dedup_cache


class TestEventIngesterEnrichmentTaskCreation:
    """Test enrichment task creation and production."""

    @pytest.mark.asyncio
    async def test_enrichment_task_created_from_event(self, mock_config):
        """Worker creates enrichment task from event."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker.producer = AsyncMock()
        worker.producer.send = AsyncMock(return_value=Mock(partition=0, offset=123))

        event = ClaimXEventMessage(
            event_id="evt-123",
            event_type="PROJECT_CREATED",
            project_id="proj-456",
            media_id="media-789",
            task_assignment_id=None,
            video_collaboration_id=None,
            master_file_name=None,
            ingested_at=datetime.now(UTC),
        )

        await worker._create_enrichment_task(event)

        # Verify task was produced
        assert worker.producer.send.called
        task = worker.producer.send.call_args[1]["value"]
        assert isinstance(task, ClaimXEnrichmentTask)
        assert task.event_id == "evt-123"
        assert task.event_type == "PROJECT_CREATED"
        assert task.project_id == "proj-456"
        assert task.media_id == "media-789"
        assert task.retry_count == 0

    @pytest.mark.asyncio
    async def test_enrichment_task_includes_all_event_fields(self, mock_config):
        """Worker copies all relevant fields to enrichment task."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker.producer = AsyncMock()
        worker.producer.send = AsyncMock(return_value=Mock(partition=0, offset=123))

        event = ClaimXEventMessage(
            event_id="evt-123",
            event_type="VIDEO_UPDATED",
            project_id="proj-456",
            media_id=None,
            task_assignment_id="task-111",
            video_collaboration_id="video-222",
            master_file_name="video.mp4",
            ingested_at=datetime.now(UTC),
        )

        await worker._create_enrichment_task(event)

        task = worker.producer.send.call_args[1]["value"]
        assert task.task_assignment_id == "task-111"
        assert task.video_collaboration_id == "video-222"
        assert task.master_file_name == "video.mp4"

    @pytest.mark.asyncio
    async def test_enrichment_task_uses_event_id_as_key(self, mock_config):
        """Worker uses event_id as Kafka message key."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker.producer = AsyncMock()
        worker.producer.send = AsyncMock(return_value=Mock(partition=0, offset=123))

        event = ClaimXEventMessage(
            event_id="evt-123",
            event_type="PROJECT_CREATED",
            project_id="proj-456",
            ingested_at=datetime.now(UTC),
        )

        await worker._create_enrichment_task(event)

        # Verify key is event_id
        key = worker.producer.send.call_args[1]["key"]
        assert key == "evt-123"

    @pytest.mark.asyncio
    async def test_enrichment_task_includes_headers(self, mock_config):
        """Worker includes event_id in message headers."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker.producer = AsyncMock()
        worker.producer.send = AsyncMock(return_value=Mock(partition=0, offset=123))

        event = ClaimXEventMessage(
            event_id="evt-123",
            event_type="PROJECT_CREATED",
            project_id="proj-456",
            ingested_at=datetime.now(UTC),
        )

        await worker._create_enrichment_task(event)

        # Verify headers
        headers = worker.producer.send.call_args[1]["headers"]
        assert headers == {"event_id": "evt-123"}

    @pytest.mark.asyncio
    async def test_successful_production_increments_counter(self, mock_config):
        """Worker increments success counter on successful production."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker.producer = AsyncMock()
        worker.producer.send = AsyncMock(return_value=Mock(partition=0, offset=123))

        event = ClaimXEventMessage(
            event_id="evt-123",
            event_type="PROJECT_CREATED",
            project_id="proj-456",
            ingested_at=datetime.now(UTC),
        )

        await worker._create_enrichment_task(event)

        assert worker._records_succeeded == 1

    @pytest.mark.asyncio
    async def test_production_failure_raises_exception(self, mock_config):
        """Worker raises exception on production failure."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker.producer = AsyncMock()
        worker.producer.send = AsyncMock(side_effect=Exception("Kafka error"))

        event = ClaimXEventMessage(
            event_id="evt-123",
            event_type="PROJECT_CREATED",
            project_id="proj-456",
            ingested_at=datetime.now(UTC),
        )

        with pytest.raises(Exception, match="Kafka error"):
            await worker._create_enrichment_task(event)


class TestEventIngesterBackgroundTasks:
    """Test background task tracking and management."""

    @pytest.mark.asyncio
    async def test_wait_for_pending_tasks_with_no_tasks(self, mock_config):
        """Worker handles no pending tasks gracefully."""
        worker = ClaimXEventIngesterWorker(config=mock_config)

        # Should not raise
        await worker._wait_for_pending_tasks(timeout_seconds=1)

    @pytest.mark.asyncio
    async def test_wait_for_pending_tasks_waits_for_completion(self, mock_config):
        """Worker waits for pending tasks to complete."""
        worker = ClaimXEventIngesterWorker(config=mock_config)

        # Create fake pending task
        fake_task = AsyncMock()
        fake_task.done = Mock(return_value=False)
        fake_task.exception = Mock(return_value=None)
        worker._pending_tasks.add(fake_task)

        # Wait should complete when task completes
        await worker._wait_for_pending_tasks(timeout_seconds=0.1)

        # Task set should be processed
        # (asyncio.wait will complete immediately for our mocked task)


class TestEventIngesterConfig:
    """Test configuration property access."""

    def test_config_property_returns_consumer_config(self, mock_config):
        """Worker.config property returns consumer_config."""
        worker = ClaimXEventIngesterWorker(config=mock_config)

        assert worker.config is worker.consumer_config
        assert worker.config is mock_config
