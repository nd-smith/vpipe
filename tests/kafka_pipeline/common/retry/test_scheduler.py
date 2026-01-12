"""
Tests for delayed redelivery scheduler.

Tests the DelayedRedeliveryScheduler class which consumes from retry topics
and redelivers messages to the pending topic after delay has elapsed.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from aiokafka.structs import ConsumerRecord, TopicPartition

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.common.retry.scheduler import DelayedRedeliveryScheduler
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
            "retry_delays": [60, 120, 240, 480],  # Short delays for testing
            "consumer_group_prefix": "test",
        },
    )


@pytest.fixture
def mock_producer():
    """Create mock Kafka producer."""
    producer = Mock(spec=BaseKafkaProducer)
    producer.is_started = True
    producer.send = AsyncMock()
    return producer


@pytest.fixture
def download_task():
    """Create test download task message."""
    return DownloadTaskMessage(
        trace_id="test-trace-123",
        media_id="media-trace-123",
        attachment_url="https://example.com/file.pdf",
        blob_path="documentsReceived/C-123/pdf/file.pdf",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="C-123",
        event_type="claim",
        event_subtype="created",
        retry_count=1,
        original_timestamp=datetime.now(timezone.utc),
        metadata={},
    )


@pytest.fixture
def scheduler(kafka_config, mock_producer):
    """Create test scheduler instance."""
    return DelayedRedeliveryScheduler(
        config=kafka_config,
        producer=mock_producer,
        domain="xact",
        lag_threshold=100,
        check_interval_seconds=1,
    )


def create_consumer_record(
    topic: str,
    partition: int,
    offset: int,
    key: str,
    value: bytes,
) -> ConsumerRecord:
    """Helper to create ConsumerRecord for testing."""
    return ConsumerRecord(
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
        timestamp_type=0,
        key=key.encode("utf-8") if key else None,
        value=value,
        checksum=None,
        serialized_key_size=len(key.encode("utf-8")) if key else 0,
        serialized_value_size=len(value),
        headers=[],
    )


class TestDelayedRedeliveryScheduler:
    """Test suite for DelayedRedeliveryScheduler."""

    def test_initialization(self, scheduler, kafka_config):
        """Test scheduler initialization."""
        assert scheduler.config == kafka_config
        assert scheduler.lag_threshold == 100
        assert scheduler.check_interval_seconds == 1
        assert not scheduler.is_running
        assert not scheduler.is_paused

        # Verify retry topics are built correctly
        expected_topics = [
            "test.downloads.pending.retry.1m",
            "test.downloads.pending.retry.2m",
            "test.downloads.pending.retry.4m",
            "test.downloads.pending.retry.8m",
        ]
        assert scheduler.retry_topics == expected_topics

    @pytest.mark.asyncio
    async def test_start_requires_producer_started(self, kafka_config):
        """Test that start() fails if producer not started."""
        producer = Mock(spec=BaseKafkaProducer)
        producer.is_started = False

        scheduler = DelayedRedeliveryScheduler(
            config=kafka_config,
            producer=producer,
            domain="xact",
        )

        with pytest.raises(RuntimeError, match="Producer must be started"):
            await scheduler.start()

    @pytest.mark.asyncio
    async def test_handle_retry_message_delay_elapsed(
        self, scheduler, download_task, mock_producer
    ):
        """Test handling message when delay has elapsed."""
        # Set retry_at to past time
        past_time = datetime.now(timezone.utc) - timedelta(seconds=10)
        download_task.metadata["retry_at"] = past_time.isoformat()

        # Create consumer record
        record = create_consumer_record(
            topic="test.downloads.pending.retry.1m",
            partition=0,
            offset=100,
            key=download_task.trace_id,
            value=download_task.model_dump_json().encode("utf-8"),
        )

        # Handle the message
        await scheduler._handle_retry_message(record)

        # Verify message was sent to pending topic
        mock_producer.send.assert_called_once()
        call_args = mock_producer.send.call_args
        assert call_args.kwargs["topic"] == "test.downloads.pending"
        assert call_args.kwargs["key"] == download_task.trace_id
        assert call_args.kwargs["headers"]["redelivered_from"] == record.topic
        assert call_args.kwargs["headers"]["retry_count"] == str(download_task.retry_count)

    @pytest.mark.asyncio
    async def test_handle_retry_message_delay_not_elapsed(
        self, scheduler, download_task
    ):
        """Test handling message when delay has not elapsed."""
        # Set retry_at to future time
        future_time = datetime.now(timezone.utc) + timedelta(seconds=30)
        download_task.metadata["retry_at"] = future_time.isoformat()

        # Create consumer record
        record = create_consumer_record(
            topic="test.downloads.pending.retry.1m",
            partition=0,
            offset=100,
            key=download_task.trace_id,
            value=download_task.model_dump_json().encode("utf-8"),
        )

        # Should raise exception to prevent commit
        with pytest.raises(RuntimeError, match="Delay not elapsed"):
            await scheduler._handle_retry_message(record)

    @pytest.mark.asyncio
    async def test_handle_retry_message_missing_retry_at(
        self, scheduler, download_task
    ):
        """Test handling message missing retry_at timestamp."""
        # Don't set retry_at in metadata

        # Create consumer record
        record = create_consumer_record(
            topic="test.downloads.pending.retry.1m",
            partition=0,
            offset=100,
            key=download_task.trace_id,
            value=download_task.model_dump_json().encode("utf-8"),
        )

        # Should raise ValueError for missing timestamp
        with pytest.raises(ValueError, match="missing retry_at"):
            await scheduler._handle_retry_message(record)

    @pytest.mark.asyncio
    async def test_handle_retry_message_invalid_retry_at(
        self, scheduler, download_task
    ):
        """Test handling message with invalid retry_at timestamp."""
        # Set invalid retry_at
        download_task.metadata["retry_at"] = "not-a-valid-timestamp"

        # Create consumer record
        record = create_consumer_record(
            topic="test.downloads.pending.retry.1m",
            partition=0,
            offset=100,
            key=download_task.trace_id,
            value=download_task.model_dump_json().encode("utf-8"),
        )

        # Should raise ValueError for invalid timestamp
        with pytest.raises(ValueError, match="Invalid retry_at"):
            await scheduler._handle_retry_message(record)

    @pytest.mark.asyncio
    async def test_handle_retry_message_malformed_json(self, scheduler):
        """Test handling malformed message that can't be parsed."""
        # Create consumer record with invalid JSON
        record = create_consumer_record(
            topic="test.downloads.pending.retry.1m",
            partition=0,
            offset=100,
            key="test-key",
            value=b"not valid json{{{",
        )

        # Should raise ValueError for invalid message
        with pytest.raises(ValueError, match="Invalid DownloadTaskMessage"):
            await scheduler._handle_retry_message(record)

    @pytest.mark.asyncio
    async def test_handle_retry_message_when_paused(
        self, scheduler, download_task
    ):
        """Test that messages are not processed when scheduler is paused."""
        # Pause the scheduler
        scheduler._paused = True

        # Set retry_at to past time (would normally be processed)
        past_time = datetime.now(timezone.utc) - timedelta(seconds=10)
        download_task.metadata["retry_at"] = past_time.isoformat()

        # Create consumer record
        record = create_consumer_record(
            topic="test.downloads.pending.retry.1m",
            partition=0,
            offset=100,
            key=download_task.trace_id,
            value=download_task.model_dump_json().encode("utf-8"),
        )

        # Should raise exception to prevent commit
        with pytest.raises(RuntimeError, match="paused due to high pending topic lag"):
            await scheduler._handle_retry_message(record)

    @pytest.mark.asyncio
    async def test_handle_retry_message_producer_failure(
        self, scheduler, download_task, mock_producer
    ):
        """Test handling producer failure during redelivery."""
        # Set retry_at to past time
        past_time = datetime.now(timezone.utc) - timedelta(seconds=10)
        download_task.metadata["retry_at"] = past_time.isoformat()

        # Make producer.send fail
        mock_producer.send.side_effect = Exception("Kafka broker unavailable")

        # Create consumer record
        record = create_consumer_record(
            topic="test.downloads.pending.retry.1m",
            partition=0,
            offset=100,
            key=download_task.trace_id,
            value=download_task.model_dump_json().encode("utf-8"),
        )

        # Should propagate exception to prevent commit
        with pytest.raises(Exception, match="Kafka broker unavailable"):
            await scheduler._handle_retry_message(record)

    @pytest.mark.asyncio
    async def test_monitor_pending_lag_pause_resume(self, scheduler):
        """Test lag monitoring pauses and resumes scheduler."""
        # Mock _get_pending_topic_lag to return controlled values
        lag_values = [50, 150, 50]  # below threshold, above, below
        lag_iter = iter(lag_values)

        async def mock_get_lag():
            try:
                return next(lag_iter)
            except StopIteration:
                # Stop the monitoring task after test values exhausted
                scheduler._running = False
                return 0

        scheduler._get_pending_topic_lag = mock_get_lag
        scheduler._running = True

        # Run monitoring task
        monitoring_task = asyncio.create_task(scheduler._monitor_pending_lag())

        # Wait for task to complete
        try:
            await asyncio.wait_for(monitoring_task, timeout=5.0)
        except asyncio.TimeoutError:
            monitoring_task.cancel()
            pytest.fail("Lag monitoring did not complete in time")

        # The scheduler should have paused and resumed
        # Final state should be unpaused (last lag value is 50)
        assert not scheduler.is_paused

    @pytest.mark.asyncio
    async def test_monitor_pending_lag_handles_errors(self, scheduler):
        """Test lag monitoring continues even if lag check fails."""
        # Mock _get_pending_topic_lag to raise exception
        call_count = 0

        async def mock_get_lag():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("Lag check failed")
            # Stop after a few iterations
            scheduler._running = False
            return 0

        scheduler._get_pending_topic_lag = mock_get_lag
        scheduler._running = True

        # Run monitoring task
        monitoring_task = asyncio.create_task(scheduler._monitor_pending_lag())

        # Wait for task to complete
        try:
            await asyncio.wait_for(monitoring_task, timeout=5.0)
        except asyncio.TimeoutError:
            monitoring_task.cancel()
            pytest.fail("Lag monitoring did not complete in time")

        # Task should have completed despite errors
        assert call_count >= 3

    @pytest.mark.asyncio
    async def test_stop_graceful_shutdown(self, scheduler):
        """Test graceful shutdown of scheduler."""
        # Mock the consumer
        mock_consumer = AsyncMock()
        mock_consumer.stop = AsyncMock()
        scheduler._consumer = mock_consumer

        # Create a real asyncio task that we can cancel
        async def dummy_task():
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                raise

        scheduler._lag_check_task = asyncio.create_task(dummy_task())
        scheduler._running = True

        # Stop the scheduler
        await scheduler.stop()

        # Verify cleanup
        assert not scheduler.is_running
        # Task should be cleared after stop
        assert scheduler._lag_check_task is None
        mock_consumer.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_idempotent(self, scheduler):
        """Test that stop() can be called multiple times safely."""
        scheduler._running = False
        scheduler._consumer = None

        # Should not raise exception
        await scheduler.stop()
        await scheduler.stop()

    def test_retry_topics_generated_correctly(self, kafka_config):
        """Test that retry topics are generated with correct names."""
        producer = Mock(spec=BaseKafkaProducer)
        producer.is_started = True

        scheduler = DelayedRedeliveryScheduler(
            config=kafka_config,
            producer=producer,
            domain="xact",
        )

        # Verify retry topics match expected pattern
        expected = [
            "test.downloads.pending.retry.1m",
            "test.downloads.pending.retry.2m",
            "test.downloads.pending.retry.4m",
            "test.downloads.pending.retry.8m",
        ]
        assert scheduler.retry_topics == expected

    @pytest.mark.asyncio
    async def test_handle_retry_message_preserves_task_data(
        self, scheduler, download_task, mock_producer
    ):
        """Test that redelivery preserves all task data."""
        # Set retry_at to past time
        past_time = datetime.now(timezone.utc) - timedelta(seconds=10)
        download_task.metadata["retry_at"] = past_time.isoformat()
        download_task.metadata["custom_field"] = "custom_value"

        # Create consumer record
        record = create_consumer_record(
            topic="test.downloads.pending.retry.1m",
            partition=0,
            offset=100,
            key=download_task.trace_id,
            value=download_task.model_dump_json().encode("utf-8"),
        )

        # Handle the message
        await scheduler._handle_retry_message(record)

        # Verify the redelivered task preserves all data
        call_args = mock_producer.send.call_args
        redelivered_task = call_args.kwargs["value"]

        assert redelivered_task.trace_id == download_task.trace_id
        assert redelivered_task.attachment_url == download_task.attachment_url
        assert redelivered_task.blob_path == download_task.blob_path
        assert redelivered_task.retry_count == download_task.retry_count
        assert redelivered_task.metadata["custom_field"] == "custom_value"
        assert redelivered_task.metadata["retry_at"] == past_time.isoformat()

    @pytest.mark.asyncio
    async def test_get_pending_topic_lag_stub(self, scheduler):
        """Test that _get_pending_topic_lag returns 0 (stub implementation)."""
        lag = await scheduler._get_pending_topic_lag()
        # Current stub implementation returns 0
        assert lag == 0

    def test_properties(self, scheduler):
        """Test is_running and is_paused properties."""
        # Initially not running
        assert not scheduler.is_running
        assert not scheduler.is_paused

        # Simulate running state
        scheduler._running = True
        scheduler._consumer = Mock()
        assert scheduler.is_running

        # Simulate paused state
        scheduler._paused = True
        assert scheduler.is_paused

    @pytest.mark.asyncio
    async def test_handle_retry_message_with_timezone_naive_timestamp(
        self, scheduler, download_task, mock_producer
    ):
        """Test handling timestamp without timezone info (should default to UTC)."""
        # Set retry_at without timezone info
        past_time = datetime.now(timezone.utc) - timedelta(seconds=10)
        # Remove timezone info
        naive_time = past_time.replace(tzinfo=None)
        download_task.metadata["retry_at"] = naive_time.isoformat()

        # Create consumer record
        record = create_consumer_record(
            topic="test.downloads.pending.retry.1m",
            partition=0,
            offset=100,
            key=download_task.trace_id,
            value=download_task.model_dump_json().encode("utf-8"),
        )

        # Should handle successfully (timestamp converted to UTC)
        await scheduler._handle_retry_message(record)

        # Verify message was sent
        mock_producer.send.assert_called_once()
