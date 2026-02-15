"""
Unit tests for ClaimX Event Ingester Worker.

Test Coverage:
    - Worker initialization and configuration
    - Lifecycle management (start/stop/graceful shutdown)
    - Batch message processing (parsing, dedup, enrichment)
    - Event deduplication (OrderedDict cache, TTL, LRU eviction)
    - Enrichment task creation
    - Producer interaction
    - Background task tracking
    - Periodic cycle output

No infrastructure required - all dependencies mocked.
"""

import contextlib
import hashlib
import json
import time
from collections import OrderedDict
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest

from config.config import MessageConfig
from pipeline.claimx.schemas.tasks import ClaimXEnrichmentTask
from pipeline.claimx.workers.event_ingester import ClaimXEventIngesterWorker
from pipeline.common.types import PipelineMessage


def _compute_trace_id(raw_event: dict) -> str:
    """Compute the SHA-256 trace_id the same way the source does."""
    project_id = str(raw_event.get("project_id") or "")
    event_type = raw_event.get("event_type") or ""
    ingested_at = raw_event.get("ingested_at") or raw_event.get("event_timestamp") or ""

    composite_parts = [project_id, event_type]
    if ingested_at:
        composite_parts.append(str(ingested_at))

    for field, prefix in [
        ("media_id", "media"),
        ("task_assignment_id", "task"),
        ("video_collaboration_id", "video"),
        ("master_file_name", "file"),
    ]:
        val = raw_event.get(field)
        if val:
            composite_parts.append(f"{prefix}:{val}")

    composite_key = "|".join(composite_parts)
    return hashlib.sha256(composite_key.encode()).hexdigest()


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
def sample_raw_event():
    """Sample raw event data."""
    return {
        "event_id": "evt-123",
        "event_type": "PROJECT_CREATED",
        "project_id": "proj-456",
        "media_id": None,
        "task_assignment_id": None,
        "video_collaboration_id": None,
        "master_file_name": None,
        "event_timestamp": datetime.now(UTC).isoformat(),
        "ingested_at": "2026-01-15T12:00:00+00:00",
    }


@pytest.fixture
def sample_message(sample_raw_event):
    """Sample Kafka message with event data."""
    return PipelineMessage(
        topic="claimx.events.raw",
        partition=0,
        offset=1,
        key=b"evt-123",
        value=json.dumps(sample_raw_event).encode(),
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

    def test_deduplication_cache_initialized_as_ordered_dict(self, mock_config):
        """Worker initializes with empty OrderedDict dedup cache."""
        worker = ClaimXEventIngesterWorker(config=mock_config)

        assert isinstance(worker._dedup_cache, OrderedDict)
        assert len(worker._dedup_cache) == 0
        assert worker._dedup_store is None

    def test_counters_initialized_to_zero(self, mock_config):
        """Worker initializes counters to zero."""
        worker = ClaimXEventIngesterWorker(config=mock_config)

        assert worker._records_processed == 0
        assert worker._records_succeeded == 0
        assert worker._records_deduplicated == 0


class TestEventIngesterLifecycle:
    """Test worker lifecycle (start/stop)."""

    @pytest.mark.asyncio
    async def test_start_initializes_components(self, mock_config):
        """Worker start initializes all components."""
        worker = ClaimXEventIngesterWorker(config=mock_config)

        with (
            patch("pipeline.claimx.workers.event_ingester.create_producer") as mock_create_producer,
            patch("pipeline.claimx.workers.event_ingester.create_batch_consumer") as mock_create_consumer,
            patch("pipeline.claimx.workers.event_ingester.get_source_connection_string", return_value="fake-conn"),
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
        mock_consumer = AsyncMock()
        mock_consumer.stop = AsyncMock()
        mock_producer = AsyncMock()
        mock_producer.stop = AsyncMock()
        mock_health = AsyncMock()
        mock_health.stop = AsyncMock()

        worker.consumer = mock_consumer
        worker.producer = mock_producer
        worker.health_server = mock_health
        worker._running = True

        # Stop worker
        await worker.stop()

        # Verify cleanup (stop() sets consumer/producer to None after stopping)
        mock_consumer.stop.assert_called_once()
        mock_producer.stop.assert_called_once()
        mock_health.stop.assert_called_once()

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
        """Worker stop cancels dedup cleanup task."""
        import asyncio

        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker._running = True

        # Create an actual asyncio task that we can cancel
        async def dummy_task():
            await asyncio.sleep(100)  # Long sleep

        worker._dedup_cleanup_task = asyncio.create_task(dummy_task())

        # Ensure task is running
        assert not worker._dedup_cleanup_task.done()

        await worker.stop()

        # Verify dedup cleanup task was cancelled
        assert worker._dedup_cleanup_task.cancelled()


class TestEventIngesterBatchProcessing:
    """Test batch message processing."""

    @pytest.mark.asyncio
    async def test_valid_batch_processed_successfully(self, mock_config, sample_message):
        """Worker processes valid batch and returns True."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker.producer = AsyncMock()
        worker.producer.send_batch = AsyncMock(return_value=[Mock()])

        result = await worker._handle_event_batch([sample_message])

        assert result is True
        assert worker._records_processed == 1
        assert worker._records_succeeded == 1
        assert worker.producer.send_batch.called

    @pytest.mark.asyncio
    async def test_invalid_json_skipped_in_batch(self, mock_config):
        """Unparseable messages are skipped, not raised."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker.producer = AsyncMock()
        worker.producer.send_batch = AsyncMock()

        invalid_message = PipelineMessage(
            topic="test.topic",
            partition=0,
            offset=1,
            key=b"key",
            value=b"invalid json{",
            timestamp=None,
            headers=None,
        )

        result = await worker._handle_event_batch([invalid_message])

        assert result is True
        assert not worker.producer.send_batch.called

    @pytest.mark.asyncio
    async def test_invalid_schema_skipped_in_batch(self, mock_config):
        """Invalid schema messages are skipped, not raised."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker.producer = AsyncMock()
        worker.producer.send_batch = AsyncMock()

        invalid_message = PipelineMessage(
            topic="test.topic",
            partition=0,
            offset=1,
            key=b"key",
            value=b'{"invalid": "schema"}',
            timestamp=None,
            headers=None,
        )

        result = await worker._handle_event_batch([invalid_message])

        assert result is True
        assert not worker.producer.send_batch.called


class TestEventIngesterDeduplication:
    """Test event deduplication logic."""

    @pytest.mark.asyncio
    async def test_duplicate_event_skipped_in_batch(self, mock_config, sample_message):
        """Worker skips duplicate events in batch."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker.producer = AsyncMock()
        worker.producer.send_batch = AsyncMock(return_value=[Mock()])

        # Process first time
        result = await worker._handle_event_batch([sample_message])
        assert result is True
        assert worker._records_deduplicated == 0

        # Process again — should be deduplicated
        result = await worker._handle_event_batch([sample_message])
        assert result is True

        assert worker._records_deduplicated == 1
        assert worker.producer.send_batch.call_count == 1

    @pytest.mark.asyncio
    async def test_new_event_processed_in_batch(self, mock_config, sample_message):
        """Worker processes new (non-duplicate) events in batch."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker.producer = AsyncMock()
        worker.producer.send_batch = AsyncMock(return_value=[Mock()])

        assert "evt-123" not in worker._dedup_cache

        result = await worker._handle_event_batch([sample_message])

        assert result is True
        assert worker._records_processed == 1
        assert worker._records_deduplicated == 0

    @pytest.mark.asyncio
    async def test_processed_event_marked_in_dedup_cache(self, mock_config, sample_raw_event, sample_message):
        """Worker marks processed events in deduplication cache."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker.producer = AsyncMock()
        worker.producer.send_batch = AsyncMock(return_value=[Mock()])

        await worker._handle_event_batch([sample_message])

        trace_id = _compute_trace_id(sample_raw_event)
        assert trace_id in worker._dedup_cache

    @pytest.mark.asyncio
    async def test_seen_trace_id_detected_as_duplicate(self, mock_config, sample_message):
        """Memory cache hit causes event to be deduplicated."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker.producer = AsyncMock()
        worker.producer.send_batch = AsyncMock(return_value=[Mock()])

        # Process once to populate dedup cache
        await worker._handle_event_batch([sample_message])
        assert worker._records_deduplicated == 0

        # Process again — should be deduplicated via memory cache
        await worker._handle_event_batch([sample_message])
        assert worker._records_deduplicated == 1

    def test_unseen_trace_id_not_in_dedup_cache(self, mock_config):
        """New trace_id is not in dedup cache."""
        worker = ClaimXEventIngesterWorker(config=mock_config)

        assert "evt-456" not in worker._dedup_cache

    @pytest.mark.asyncio
    async def test_mark_processed_adds_to_dedup_set(self, mock_config):
        """_mark_processed adds event to deduplication cache."""
        worker = ClaimXEventIngesterWorker(config=mock_config)

        worker._mark_processed("evt-789")

        assert "evt-789" in worker._dedup_cache


class TestEventIngesterEnrichmentTaskCreation:
    """Test enrichment task creation via batch handler."""

    @pytest.mark.asyncio
    async def test_enrichment_task_created_from_event(self, mock_config, sample_raw_event, sample_message):
        """Worker creates enrichment task from event in batch."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker.producer = AsyncMock()
        worker.producer.send_batch = AsyncMock(return_value=[Mock()])

        result = await worker._handle_event_batch([sample_message])

        assert result is True
        assert worker.producer.send_batch.called
        call_args = worker.producer.send_batch.call_args
        messages = call_args.kwargs["messages"]
        assert len(messages) == 1
        key, task = messages[0]
        assert isinstance(task, ClaimXEnrichmentTask)
        expected_trace_id = _compute_trace_id(sample_raw_event)
        assert task.trace_id == expected_trace_id
        assert task.event_type == "PROJECT_CREATED"
        assert task.project_id == "proj-456"
        assert task.retry_count == 0

    @pytest.mark.asyncio
    async def test_enrichment_task_includes_all_event_fields(self, mock_config):
        """Worker copies all relevant fields to enrichment task."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker.producer = AsyncMock()
        worker.producer.send_batch = AsyncMock(return_value=[Mock()])

        raw_event = {
            "event_id": "evt-123",
            "event_type": "VIDEO_UPDATED",
            "project_id": "proj-456",
            "media_id": None,
            "task_assignment_id": "task-111",
            "video_collaboration_id": "video-222",
            "master_file_name": "video.mp4",
            "event_timestamp": datetime.now(UTC).isoformat(),
        }
        message = PipelineMessage(
            topic="claimx.events.raw",
            partition=0,
            offset=1,
            key=b"evt-123",
            value=json.dumps(raw_event).encode(),
            timestamp=None,
            headers=None,
        )

        result = await worker._handle_event_batch([message])

        assert result is True
        messages = worker.producer.send_batch.call_args.kwargs["messages"]
        _, task = messages[0]
        assert task.task_assignment_id == "task-111"
        assert task.video_collaboration_id == "video-222"
        assert task.master_file_name == "video.mp4"

    @pytest.mark.asyncio
    async def test_successful_batch_increments_counter(self, mock_config, sample_message):
        """Worker increments success counter on successful batch production."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker.producer = AsyncMock()
        worker.producer.send_batch = AsyncMock(return_value=[Mock()])

        await worker._handle_event_batch([sample_message])

        assert worker._records_succeeded == 1

    @pytest.mark.asyncio
    async def test_send_batch_failure_returns_false(self, mock_config, sample_message):
        """Worker returns False on send_batch failure to prevent commit."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker.producer = AsyncMock()
        worker.producer.send_batch = AsyncMock(side_effect=Exception("Kafka error"))

        result = await worker._handle_event_batch([sample_message])

        assert result is False


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


class TestEventIngesterDedupSourceTracking:
    """Test dedup source analysis counters."""

    @pytest.mark.asyncio
    async def test_memory_hit_increments_counter(self, mock_config, sample_message):
        """Memory cache hit increments _dedup_memory_hits."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker.producer = AsyncMock()
        worker.producer.send_batch = AsyncMock(return_value=[Mock()])

        # First pass populates memory cache
        await worker._handle_event_batch([sample_message])
        assert worker._dedup_memory_hits == 0

        # Second pass hits memory cache
        await worker._handle_event_batch([sample_message])
        assert worker._dedup_memory_hits == 1
        assert worker._dedup_blob_hits == 0

    @pytest.mark.asyncio
    async def test_blob_hit_increments_counter(self, mock_config, sample_raw_event, sample_message):
        """Blob storage hit increments _dedup_blob_hits."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker.producer = AsyncMock()
        worker.producer.send_batch = AsyncMock(return_value=[Mock()])

        mock_store = AsyncMock()
        mock_store.check_duplicate = AsyncMock(
            return_value=(True, {"timestamp": time.time()})
        )
        worker._dedup_store = mock_store

        # Event not in memory cache, but blob store returns duplicate
        await worker._handle_event_batch([sample_message])

        assert worker._dedup_blob_hits == 1
        assert worker._dedup_memory_hits == 0

    @pytest.mark.asyncio
    async def test_no_hit_increments_nothing(self, mock_config, sample_message):
        """Cache miss does not increment either counter."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker.producer = AsyncMock()
        worker.producer.send_batch = AsyncMock(return_value=[Mock()])

        await worker._handle_event_batch([sample_message])

        assert worker._dedup_memory_hits == 0
        assert worker._dedup_blob_hits == 0


class TestEventIngesterBatchSizeAdjustment:
    """Test backfill prefetch mode batch size adjustment."""

    def test_adjust_to_backfill_size_when_events_are_old(self, mock_config):
        """Batch size increases to BACKFILL_BATCH_SIZE for stale events."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker.consumer = Mock()
        worker.consumer.batch_size = worker.REALTIME_BATCH_SIZE

        worker._adjust_batch_size(event_age_seconds=7200)

        assert worker.consumer.batch_size == worker.BACKFILL_BATCH_SIZE

    def test_adjust_to_realtime_size_when_events_are_fresh(self, mock_config):
        """Batch size decreases to REALTIME_BATCH_SIZE for recent events."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker.consumer = Mock()
        worker.consumer.batch_size = worker.BACKFILL_BATCH_SIZE

        worker._adjust_batch_size(event_age_seconds=60)

        assert worker.consumer.batch_size == worker.REALTIME_BATCH_SIZE

    def test_no_change_when_already_at_correct_size(self, mock_config):
        """No assignment when already at the correct batch size."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker.consumer = Mock()
        worker.consumer.batch_size = worker.REALTIME_BATCH_SIZE

        worker._adjust_batch_size(event_age_seconds=60)

        assert worker.consumer.batch_size == worker.REALTIME_BATCH_SIZE

    def test_no_crash_when_consumer_is_none(self, mock_config):
        """Handles None consumer gracefully."""
        worker = ClaimXEventIngesterWorker(config=mock_config)
        worker.consumer = None

        worker._adjust_batch_size(event_age_seconds=7200)

    def test_constants_have_expected_values(self, mock_config):
        """Verify backfill constants."""
        assert ClaimXEventIngesterWorker.BACKFILL_BATCH_SIZE == 2000
        assert ClaimXEventIngesterWorker.REALTIME_BATCH_SIZE == 100
        assert ClaimXEventIngesterWorker.BACKFILL_THRESHOLD_SECONDS == 3600


class TestEventIngesterConfig:
    """Test configuration property access."""

    def test_config_property_returns_consumer_config(self, mock_config):
        """Worker.config property returns consumer_config."""
        worker = ClaimXEventIngesterWorker(config=mock_config)

        assert worker.config is worker.consumer_config
        assert worker.config is mock_config
