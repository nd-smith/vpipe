"""
Unit tests for Verisk Event Ingester Worker.

Test Coverage:
    - Worker initialization and configuration
    - Lifecycle management (start/stop/graceful shutdown)
    - Message parsing and validation
    - Event deduplication (cache management, TTL, eviction)
    - Enrichment task creation and dispatch
    - Assignment ID validation
    - Error handling (parse errors, send failures)

No infrastructure required - all dependencies mocked.
"""

import json
import pytest
import time
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

from config.config import MessageConfig
from pipeline.verisk.schemas.events import EventMessage
from pipeline.verisk.schemas.tasks import XACTEnrichmentTask
from pipeline.verisk.workers.event_ingester import EventIngesterWorker
from pipeline.common.types import PipelineMessage


@pytest.fixture
def mock_config():
    """Mock MessageConfig with standard settings."""
    config = Mock(spec=MessageConfig)
    config.get_topic.return_value = "verisk.events"
    config.get_consumer_group.return_value = "verisk-event-ingester"
    config.get_worker_config.return_value = {
        "health_port": 8092,
        "health_enabled": True,
    }
    return config


@pytest.fixture
def sample_event():
    """Sample EventMessage for testing."""
    return EventMessage(
        type="verisk.claims.property.xn.documentsReceived",
        version=1,
        utcDateTime="2024-12-25T10:30:00Z",
        traceId="trace-123",
        data=json.dumps({
            "assignmentId": "A12345",
            "estimateVersion": "1.0",
            "attachments": ["https://example.com/file.pdf"],
        }),
    )


@pytest.fixture
def sample_message(sample_event):
    """Sample Kafka message with event data."""
    return PipelineMessage(
        topic="verisk.events",
        partition=0,
        offset=1,
        key=b"trace-123",
        value=json.dumps({
            "type": sample_event.type,
            "version": sample_event.version,
            "utcDateTime": sample_event.utc_datetime,
            "traceId": sample_event.trace_id,
            "data": sample_event.data,
        }).encode(),
        timestamp=None,
        headers=None,
    )


class TestEventIngesterWorkerInitialization:
    """Test worker initialization and configuration."""

    def test_initialization_with_default_config(self, mock_config):
        """Worker initializes with default domain and config."""
        worker = EventIngesterWorker(config=mock_config)

        assert worker.domain == "verisk"
        assert worker.consumer_config is mock_config
        assert worker.producer_config is mock_config
        assert worker.worker_id == "event_ingester"
        assert worker.instance_id is None
        assert worker._running is False

    def test_initialization_with_custom_domain(self, mock_config):
        """Worker initializes with custom domain."""
        worker = EventIngesterWorker(config=mock_config, domain="custom")

        assert worker.domain == "custom"

    def test_initialization_with_instance_id(self, mock_config):
        """Worker uses instance ID for worker_id suffix."""
        worker = EventIngesterWorker(config=mock_config, instance_id="3")

        assert worker.worker_id == "event_ingester-3"
        assert worker.instance_id == "3"

    def test_initialization_with_separate_producer_config(self, mock_config):
        """Worker accepts separate producer config."""
        producer_config = Mock(spec=MessageConfig)
        producer_config.get_topic.return_value = "verisk.enrichment.pending"
        worker = EventIngesterWorker(
            config=mock_config, producer_config=producer_config
        )

        assert worker.consumer_config is mock_config
        assert worker.producer_config is producer_config

    def test_dedup_cache_initialized(self, mock_config):
        """Worker initializes deduplication cache."""
        worker = EventIngesterWorker(config=mock_config)

        assert worker._dedup_cache == {}
        assert worker._dedup_cache_timestamps == {}
        assert worker._dedup_cache_ttl_seconds == 86400  # 24 hours
        assert worker._dedup_cache_max_size == 100_000

    def test_metrics_initialized_to_zero(self, mock_config):
        """Worker initializes metrics to zero."""
        worker = EventIngesterWorker(config=mock_config)

        assert worker._records_processed == 0
        assert worker._records_succeeded == 0
        assert worker._records_skipped == 0
        assert worker._records_deduplicated == 0


class TestEventIngesterWorkerLifecycle:
    """Test worker lifecycle (start/stop)."""

    @pytest.mark.asyncio
    async def test_start_initializes_components(self, mock_config):
        """Worker start initializes all components."""
        worker = EventIngesterWorker(config=mock_config)

        with patch("pipeline.verisk.workers.event_ingester.create_producer") as mock_create_producer, \
             patch("pipeline.verisk.workers.event_ingester.create_consumer") as mock_create_consumer, \
             patch("pipeline.common.telemetry.initialize_worker_telemetry"):

            # Setup mocks
            mock_producer = AsyncMock()
            mock_producer.start = AsyncMock()
            mock_create_producer.return_value = mock_producer

            mock_consumer = AsyncMock()
            mock_consumer.start = AsyncMock()
            mock_create_consumer.return_value = mock_consumer

            # Prevent blocking on consumer.start
            mock_consumer.start.side_effect = Exception("Stop")

            try:
                await worker.start()
            except Exception:
                pass

            # Verify components were initialized (even though _running is reset in finally)
            assert worker.producer is not None
            assert worker.consumer is not None
            assert mock_producer.start.called

    @pytest.mark.asyncio
    async def test_stop_cleans_up_resources(self, mock_config):
        """Worker stop cleans up all resources."""
        worker = EventIngesterWorker(config=mock_config)

        # Setup mocked components
        worker.consumer = AsyncMock()
        worker.consumer.stop = AsyncMock()
        worker.producer = AsyncMock()
        worker.producer.stop = AsyncMock()
        worker._stats_logger = AsyncMock()
        worker._stats_logger.stop = AsyncMock()
        worker.health_server = AsyncMock()
        worker.health_server.stop = AsyncMock()

        worker._running = True

        # Stop worker
        await worker.stop()

        # Verify cleanup
        assert worker._running is False
        worker.consumer.stop.assert_called_once()
        worker.producer.stop.assert_called_once()
        worker._stats_logger.stop.assert_called_once()
        worker.health_server.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_handles_none_components(self, mock_config):
        """Worker stop handles None components gracefully."""
        worker = EventIngesterWorker(config=mock_config)

        # All components are None (not initialized)
        assert worker.consumer is None
        assert worker.producer is None

        # Should not raise
        await worker.stop()


class TestEventIngesterWorkerMessageProcessing:
    """Test message parsing and processing."""

    @pytest.mark.asyncio
    async def test_valid_message_parsed_successfully(
        self, mock_config, sample_message
    ):
        """Worker parses valid event message."""
        worker = EventIngesterWorker(config=mock_config)
        worker.producer = AsyncMock()

        # Mock producer.send
        mock_metadata = Mock()
        mock_metadata.partition = 0
        mock_metadata.offset = 100
        worker.producer.send = AsyncMock(return_value=mock_metadata)

        await worker._handle_event_message(sample_message)

        # Verify message was processed
        assert worker._records_processed == 1
        assert worker._records_succeeded == 1
        assert worker.producer.send.called

    @pytest.mark.asyncio
    async def test_invalid_json_raises_error(self, mock_config):
        """Worker raises error on invalid JSON."""
        worker = EventIngesterWorker(config=mock_config)

        invalid_message = PipelineMessage(
            topic="verisk.events",
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
        worker = EventIngesterWorker(config=mock_config)

        invalid_message = PipelineMessage(
            topic="verisk.events",
            partition=0,
            offset=1,
            key=b"key",
            value=b'{"invalid": "schema"}',
            timestamp=None,
            headers=None,
        )

        with pytest.raises(Exception):  # Pydantic ValidationError
            await worker._handle_event_message(invalid_message)


class TestEventIngesterWorkerDeduplication:
    """Test event deduplication logic."""

    @pytest.mark.asyncio
    async def test_is_duplicate_returns_false_for_new_event(self, mock_config):
        """First occurrence of trace_id is not a duplicate."""
        worker = EventIngesterWorker(config=mock_config)

        is_dup, cached_id = await worker._is_duplicate("trace-123")

        assert is_dup is False
        assert cached_id is None

    @pytest.mark.asyncio
    async def test_is_duplicate_returns_true_for_cached_event(self, mock_config):
        """Cached trace_id within TTL is a duplicate."""
        worker = EventIngesterWorker(config=mock_config)

        # Mark as processed
        await worker._mark_processed("trace-123", "evt-456")

        # Check for duplicate
        is_dup, cached_id = await worker._is_duplicate("trace-123")

        assert is_dup is True
        assert cached_id == "evt-456"

    @pytest.mark.asyncio
    async def test_is_duplicate_returns_false_for_expired_entry(self, mock_config):
        """Expired cache entry is not a duplicate."""
        worker = EventIngesterWorker(config=mock_config)
        worker._dedup_cache_ttl_seconds = 1  # 1 second TTL

        # Mark as processed
        await worker._mark_processed("trace-123", "evt-456")

        # Wait for expiry
        time.sleep(1.1)

        # Check for duplicate - should be expired
        is_dup, cached_id = await worker._is_duplicate("trace-123")

        assert is_dup is False
        assert cached_id is None
        # Verify expired entry was removed
        assert "trace-123" not in worker._dedup_cache

    @pytest.mark.asyncio
    async def test_duplicate_event_skipped(self, mock_config, sample_message):
        """Worker skips duplicate events."""
        worker = EventIngesterWorker(config=mock_config)
        worker.producer = AsyncMock()

        # Mock producer.send
        mock_metadata = Mock()
        mock_metadata.partition = 0
        mock_metadata.offset = 100
        worker.producer.send = AsyncMock(return_value=mock_metadata)

        # Process first time
        await worker._handle_event_message(sample_message)
        assert worker._records_deduplicated == 0

        # Process again - should be deduplicated
        await worker._handle_event_message(sample_message)

        # Verify deduplicated
        assert worker._records_deduplicated == 1
        assert worker.producer.send.call_count == 1  # Only called once

    @pytest.mark.asyncio
    async def test_mark_processed_adds_to_cache(self, mock_config):
        """mark_processed adds entry to dedup cache."""
        worker = EventIngesterWorker(config=mock_config)

        await worker._mark_processed("trace-123", "evt-456")

        assert "trace-123" in worker._dedup_cache
        assert worker._dedup_cache["trace-123"] == "evt-456"
        assert "trace-123" in worker._dedup_cache_timestamps

    @pytest.mark.asyncio
    async def test_mark_processed_evicts_old_entries_when_full(self, mock_config):
        """mark_processed evicts oldest 10% when cache is full."""
        worker = EventIngesterWorker(config=mock_config)
        worker._dedup_cache_max_size = 10  # Small cache for testing

        # Fill cache to capacity
        for i in range(10):
            await worker._mark_processed(f"trace-{i}", f"evt-{i}")

        # Add one more - should trigger eviction
        await worker._mark_processed("trace-new", "evt-new")

        # Verify cache size is maintained
        assert len(worker._dedup_cache) <= 10
        # Verify oldest entry was evicted (trace-0)
        assert "trace-0" not in worker._dedup_cache
        # Verify new entry was added
        assert "trace-new" in worker._dedup_cache

    @pytest.mark.asyncio
    async def test_cleanup_dedup_cache_removes_expired_entries(self, mock_config):
        """cleanup removes expired entries from cache."""
        worker = EventIngesterWorker(config=mock_config)
        worker._dedup_cache_ttl_seconds = 1  # 1 second TTL

        # Add entries
        await worker._mark_processed("trace-1", "evt-1")
        await worker._mark_processed("trace-2", "evt-2")

        # Wait for expiry
        time.sleep(1.1)

        # Add fresh entry
        await worker._mark_processed("trace-3", "evt-3")

        # Cleanup
        worker._cleanup_dedup_cache()

        # Verify expired entries removed, fresh entry kept
        assert "trace-1" not in worker._dedup_cache
        assert "trace-2" not in worker._dedup_cache
        assert "trace-3" in worker._dedup_cache


class TestEventIngesterWorkerEnrichmentTask:
    """Test enrichment task creation and dispatch."""

    @pytest.mark.asyncio
    async def test_enrichment_task_created_for_event(
        self, mock_config, sample_message
    ):
        """Worker creates enrichment task for valid event."""
        worker = EventIngesterWorker(config=mock_config)
        worker.producer = AsyncMock()

        # Mock producer.send
        mock_metadata = Mock()
        mock_metadata.partition = 0
        mock_metadata.offset = 100
        worker.producer.send = AsyncMock(return_value=mock_metadata)

        await worker._handle_event_message(sample_message)

        # Verify enrichment task was produced
        assert worker.producer.send.called
        call_args = worker.producer.send.call_args

        # Verify task structure
        task = call_args.kwargs["value"]
        assert isinstance(task, XACTEnrichmentTask)
        assert task.trace_id == "trace-123"
        assert task.status_subtype == "documentsReceived"
        assert task.assignment_id == "A12345"
        assert task.estimate_version == "1.0"
        assert task.attachments == ["https://example.com/file.pdf"]

    @pytest.mark.asyncio
    async def test_event_without_assignment_id_skipped(self, mock_config):
        """Worker skips events without assignmentId."""
        worker = EventIngesterWorker(config=mock_config)
        worker.producer = AsyncMock()

        # Create event without assignment_id
        message = PipelineMessage(
            topic="verisk.events",
            partition=0,
            offset=1,
            key=b"trace-123",
            value=json.dumps({
                "type": "verisk.claims.property.xn.documentsReceived",
                "version": 1,
                "utcDateTime": "2024-12-25T10:30:00Z",
                "traceId": "trace-123",
                "data": json.dumps({}),  # No assignmentId
            }).encode(),
            timestamp=None,
            headers=None,
        )

        await worker._handle_event_message(message)

        # Verify event was skipped
        assert worker._records_skipped == 1
        assert not worker.producer.send.called

    @pytest.mark.asyncio
    async def test_enrichment_task_send_failure_raises(self, mock_config, sample_message):
        """Worker re-raises send failures to prevent offset commit."""
        worker = EventIngesterWorker(config=mock_config)
        worker.producer = AsyncMock()

        # Mock send failure
        send_error = Exception("Send failed")
        worker.producer.send = AsyncMock(side_effect=send_error)

        # Patch record_processing_error to avoid parameter issues
        with patch("pipeline.verisk.workers.event_ingester.record_processing_error"):
            # Verify error is raised
            with pytest.raises(Exception) as exc_info:
                await worker._handle_event_message(sample_message)

            assert exc_info.value is send_error


class TestEventIngesterWorkerStats:
    """Test cycle statistics and logging."""

    def test_get_cycle_stats_returns_formatted_output(self, mock_config):
        """Worker returns formatted cycle stats."""
        worker = EventIngesterWorker(config=mock_config)
        worker._records_processed = 100
        worker._records_succeeded = 95
        worker._records_skipped = 3
        worker._records_deduplicated = 2

        msg, extra = worker._get_cycle_stats(cycle_count=1)

        # Verify message format
        assert "95" in msg  # succeeded
        assert "3" in msg   # skipped
        assert "2" in msg   # deduplicated

        # Verify extra data
        assert extra["records_processed"] == 100
        assert extra["records_succeeded"] == 95
        assert extra["records_skipped"] == 3
        assert extra["records_deduplicated"] == 2
