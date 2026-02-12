"""
Unit tests for UnifiedRetryScheduler.

Test Coverage:
    - Helper functions: parse_retry_count, parse_scheduled_time, encode_message_key
    - Scheduler initialization with target_topic_keys
    - Start/stop lifecycle
    - Message handling: missing headers, invalid retry_count, exhausted retries
    - Message handling: invalid scheduled_time, delayed messages, immediate routing
    - Header parsing (bytes, strings, decode errors)
    - DLQ routing on errors
    - Producer pool routing: correct producer selected per target_topic
    - Unknown target_topic → DLQ routing
    - Route to target with context headers preserved
    - Stats and is_running properties

No infrastructure required - all dependencies mocked.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock, patch

import pytest

from pipeline.common.retry.unified_scheduler import (
    UnifiedRetryScheduler,
    encode_message_key,
    parse_retry_count,
    parse_scheduled_time,
)
from pipeline.common.types import PipelineMessage

# --- Helper function tests ---


class TestParseRetryCount:
    """Test parse_retry_count helper."""

    def test_valid_integer_string(self):
        assert parse_retry_count("3") == 3

    def test_zero(self):
        assert parse_retry_count("0") == 0

    def test_negative_number(self):
        assert parse_retry_count("-1") == -1

    def test_non_numeric_returns_none(self):
        assert parse_retry_count("abc") is None

    def test_empty_string_returns_none(self):
        assert parse_retry_count("") is None

    def test_float_string_returns_none(self):
        assert parse_retry_count("3.5") is None


class TestParseScheduledTime:
    """Test parse_scheduled_time helper."""

    def test_valid_iso_format_with_tz(self):
        result = parse_scheduled_time("2024-06-15T12:00:00+00:00")
        assert result == datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)

    def test_valid_iso_format_without_tz_gets_utc(self):
        """Naive datetimes get UTC timezone attached."""
        result = parse_scheduled_time("2024-06-15T12:00:00")
        assert result is not None
        assert result.tzinfo == UTC

    def test_invalid_format_returns_none(self):
        assert parse_scheduled_time("not-a-date") is None

    def test_empty_string_returns_none(self):
        assert parse_scheduled_time("") is None


class TestEncodeMessageKey:
    """Test encode_message_key helper."""

    def test_string_key_encoded_to_bytes(self):
        result = encode_message_key("my-key", Mock(key=None))
        assert result == b"my-key"

    def test_bytes_key_returned_as_is(self):
        result = encode_message_key(b"my-key", Mock(key=None))
        assert result == b"my-key"

    def test_none_key_falls_back_to_message_key_bytes(self):
        message = Mock(key=b"fallback-key")
        result = encode_message_key(None, message)
        assert result == b"fallback-key"

    def test_none_key_falls_back_to_message_key_string(self):
        message = Mock(key="fallback-key")
        result = encode_message_key(None, message)
        assert result == b"fallback-key"

    def test_none_key_with_none_message_key_returns_none(self):
        message = Mock(key=None)
        result = encode_message_key(None, message)
        assert result is None

    def test_integer_key_falls_back_to_message_key(self):
        """Non-str, non-bytes original_key falls through to message.key."""
        message = Mock(key=b"fallback")
        result = encode_message_key(42, message)
        assert result == b"fallback"


# --- Scheduler tests ---

TARGET_TOPIC_KEYS = ["downloads_pending", "enrichment_pending", "downloads_results"]


def _mock_get_topic(domain, topic_key):
    """Mock config.get_topic to return resolved topic names."""
    return f"{domain}.{topic_key}"


@pytest.fixture
def mock_config():
    """Mock MessageConfig for scheduler tests."""
    config = Mock()
    config.get_retry_topic.return_value = "verisk.retry"
    config.get_topic.side_effect = _mock_get_topic
    config.get_max_retries.return_value = 3
    return config


@pytest.fixture
def mock_dlq_producer():
    """Mock DLQ producer."""
    producer = AsyncMock()
    producer.send = AsyncMock()
    return producer


@pytest.fixture
def mock_downloads_producer():
    """Mock producer for downloads_pending topic."""
    producer = AsyncMock()
    producer.send = AsyncMock()
    return producer


@pytest.fixture
def mock_enrichment_producer():
    """Mock producer for enrichment_pending topic."""
    producer = AsyncMock()
    producer.send = AsyncMock()
    return producer


@pytest.fixture
def mock_results_producer():
    """Mock producer for downloads_results topic."""
    producer = AsyncMock()
    producer.send = AsyncMock()
    return producer


@pytest.fixture
def scheduler(
    mock_config,
    mock_dlq_producer,
    mock_downloads_producer,
    mock_enrichment_producer,
    mock_results_producer,
):
    """Create a UnifiedRetryScheduler with mocked dependencies.

    Manually sets up the producer pool and DLQ producer (bypassing start()).
    """
    mock_health = AsyncMock()
    with patch(
        "pipeline.common.retry.unified_scheduler.HealthCheckServer", return_value=mock_health
    ):
        s = UnifiedRetryScheduler(
            config=mock_config,
            domain="verisk",
            target_topic_keys=TARGET_TOPIC_KEYS,
            persistence_interval_seconds=1,
            health_port=0,
        )

    # Manually wire up the producer pool (normally done in start())
    s._producer_pool = {
        "verisk.downloads_pending": mock_downloads_producer,
        "verisk.enrichment_pending": mock_enrichment_producer,
        "verisk.downloads_results": mock_results_producer,
    }
    s._dlq_producer = mock_dlq_producer

    return s


def make_pipeline_message(
    topic="verisk.retry",
    partition=0,
    offset=100,
    timestamp=1700000000,
    key=b"msg-key",
    value=b'{"data": "test"}',
    headers=None,
):
    """Create a PipelineMessage with sensible defaults."""
    return PipelineMessage(
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp=timestamp,
        key=key,
        value=value,
        headers=headers,
    )


def make_retry_headers(
    target_topic="verisk.downloads_pending",
    retry_count="1",
    scheduled_retry_time=None,
    worker_type="download",
    original_key="msg-key",
):
    """Create standard retry headers as list of (key, bytes) tuples."""
    if scheduled_retry_time is None:
        scheduled_retry_time = (datetime.now(UTC) - timedelta(seconds=10)).isoformat()

    return [
        ("target_topic", target_topic.encode()),
        ("retry_count", retry_count.encode()),
        ("scheduled_retry_time", scheduled_retry_time.encode()),
        ("worker_type", worker_type.encode()),
        ("original_key", original_key.encode()),
    ]


class TestUnifiedRetrySchedulerInitialization:
    """Test scheduler initialization."""

    def test_initializes_with_config(self, mock_config):
        with patch("pipeline.common.retry.unified_scheduler.HealthCheckServer"):
            scheduler = UnifiedRetryScheduler(
                config=mock_config,
                domain="verisk",
                target_topic_keys=TARGET_TOPIC_KEYS,
            )

        assert scheduler.domain == "verisk"
        assert scheduler.retry_topic == "verisk.retry"
        assert scheduler._dlq_topic == "verisk.dlq"
        assert scheduler._max_retries == 3
        assert scheduler._running is False

    def test_builds_reverse_topic_mapping(self, mock_config):
        """target_topic_keys are resolved to topic names in the reverse mapping."""
        with patch("pipeline.common.retry.unified_scheduler.HealthCheckServer"):
            scheduler = UnifiedRetryScheduler(
                config=mock_config,
                domain="verisk",
                target_topic_keys=TARGET_TOPIC_KEYS,
            )

        assert scheduler._topic_name_to_key == {
            "verisk.downloads_pending": "downloads_pending",
            "verisk.enrichment_pending": "enrichment_pending",
            "verisk.downloads_results": "downloads_results",
        }

    def test_initial_stats_are_zero(self, scheduler):
        stats = scheduler.stats
        assert stats["messages_routed"] == 0
        assert stats["messages_delayed"] == 0
        assert stats["messages_malformed"] == 0
        assert stats["messages_exhausted"] == 0
        assert stats["messages_restored"] == 0
        assert stats["queue_size"] == 0

    def test_is_running_false_when_not_started(self, scheduler):
        assert scheduler.is_running is False


class TestUnifiedRetrySchedulerStartStop:
    """Test scheduler start/stop lifecycle."""

    async def test_stop_when_not_running_is_noop(self, scheduler):
        """Stopping a scheduler that isn't running does nothing."""
        await scheduler.stop()  # Should not raise

    async def test_stop_persists_queue_and_stops_consumer(self, scheduler):
        """Stop persists queue, cancels tasks, stops consumer and producers."""
        scheduler._running = True
        scheduler._consumer = AsyncMock()

        # Create real asyncio tasks that can be cancelled
        async def noop():
            await asyncio.sleep(3600)

        scheduler._processor_task = asyncio.create_task(noop())
        scheduler._persistence_task = asyncio.create_task(noop())

        with patch.object(scheduler._delay_queue, "persist_to_disk") as mock_persist:
            await scheduler.stop()

        mock_persist.assert_called_once()
        assert scheduler._processor_task.cancelled()
        assert scheduler._persistence_task.cancelled()
        assert scheduler._consumer is None
        # Producer pool should be cleared
        assert len(scheduler._producer_pool) == 0
        assert scheduler._dlq_producer is None


class TestParseHeaders:
    """Test _parse_headers method."""

    def test_parses_bytes_headers(self, scheduler):
        """Bytes header values are decoded to strings."""
        message = make_pipeline_message(headers=[("key1", b"value1"), ("key2", b"value2")])
        result = scheduler._parse_headers(message)
        assert result == {"key1": "value1", "key2": "value2"}

    def test_parses_string_headers(self, scheduler):
        """Non-bytes header values are converted via str()."""
        message = make_pipeline_message(headers=[("key1", "already-string")])
        result = scheduler._parse_headers(message)
        assert result == {"key1": "already-string"}

    def test_handles_none_headers(self, scheduler):
        """None headers return empty dict."""
        message = make_pipeline_message(headers=None)
        result = scheduler._parse_headers(message)
        assert result == {}

    def test_handles_empty_headers(self, scheduler):
        """Empty headers list returns empty dict."""
        message = make_pipeline_message(headers=[])
        result = scheduler._parse_headers(message)
        assert result == {}

    def test_skips_undeccodable_header(self, scheduler):
        """Undeccodable bytes header is skipped with warning."""
        bad_bytes = b"\xff\xfe"
        message = make_pipeline_message(headers=[("good", b"ok"), ("bad", bad_bytes)])
        result = scheduler._parse_headers(message)
        assert "good" in result
        assert result["good"] == "ok"


class TestHandleRetryMessage:
    """Test _handle_retry_message routing logic."""

    async def test_routes_to_dlq_when_headers_missing(self, scheduler, mock_dlq_producer):
        """Message missing required headers goes to DLQ."""
        message = make_pipeline_message(headers=[("some_header", b"value")])

        await scheduler._handle_retry_message(message)

        mock_dlq_producer.send.assert_called_once()
        call_kwargs = mock_dlq_producer.send.call_args[1]
        assert call_kwargs["topic"] == "verisk.dlq"
        assert scheduler._messages_malformed == 1

    async def test_routes_to_dlq_when_retry_count_invalid(self, scheduler, mock_dlq_producer):
        """Invalid retry_count header routes to DLQ."""
        headers = make_retry_headers(retry_count="not-a-number")
        message = make_pipeline_message(headers=headers)

        await scheduler._handle_retry_message(message)

        mock_dlq_producer.send.assert_called_once()
        call_kwargs = mock_dlq_producer.send.call_args[1]
        assert call_kwargs["topic"] == "verisk.dlq"
        assert scheduler._messages_malformed == 1

    async def test_routes_to_dlq_when_retries_exhausted(self, scheduler, mock_dlq_producer):
        """Message with retry_count >= max_retries goes to DLQ."""
        headers = make_retry_headers(retry_count="3")  # max_retries is 3
        message = make_pipeline_message(headers=headers)

        await scheduler._handle_retry_message(message)

        mock_dlq_producer.send.assert_called_once()
        call_kwargs = mock_dlq_producer.send.call_args[1]
        assert call_kwargs["topic"] == "verisk.dlq"
        assert scheduler._messages_exhausted == 1

    async def test_routes_to_dlq_when_scheduled_time_invalid(self, scheduler, mock_dlq_producer):
        """Invalid scheduled_retry_time routes to DLQ."""
        headers = make_retry_headers(
            retry_count="1",
            scheduled_retry_time="not-a-date",
        )
        message = make_pipeline_message(headers=headers)

        await scheduler._handle_retry_message(message)

        mock_dlq_producer.send.assert_called_once()
        call_kwargs = mock_dlq_producer.send.call_args[1]
        assert call_kwargs["topic"] == "verisk.dlq"
        assert scheduler._messages_malformed == 1

    async def test_delays_message_when_not_ready(self, scheduler, mock_downloads_producer):
        """Message with future scheduled_time is added to delay queue."""
        future_time = (datetime.now(UTC) + timedelta(minutes=5)).isoformat()
        headers = make_retry_headers(
            retry_count="1",
            scheduled_retry_time=future_time,
        )
        message = make_pipeline_message(headers=headers)

        await scheduler._handle_retry_message(message)

        # Should not produce to any topic
        mock_downloads_producer.send.assert_not_called()
        assert scheduler._messages_delayed == 1
        assert len(scheduler._delay_queue) == 1

    async def test_routes_immediately_when_ready(self, scheduler, mock_downloads_producer):
        """Message with past scheduled_time is routed via correct producer."""
        past_time = (datetime.now(UTC) - timedelta(seconds=10)).isoformat()
        headers = make_retry_headers(
            retry_count="1",
            scheduled_retry_time=past_time,
            target_topic="verisk.downloads_pending",
        )
        message = make_pipeline_message(headers=headers)

        await scheduler._handle_retry_message(message)

        mock_downloads_producer.send.assert_called_once()
        call_kwargs = mock_downloads_producer.send.call_args[1]
        assert call_kwargs["topic"] == "verisk.downloads_pending"
        assert scheduler._messages_routed == 1

    async def test_immediate_route_includes_redelivery_headers(
        self, scheduler, mock_downloads_producer
    ):
        """Routed messages include redelivery headers."""
        past_time = (datetime.now(UTC) - timedelta(seconds=10)).isoformat()
        headers = make_retry_headers(
            retry_count="2",
            scheduled_retry_time=past_time,
            target_topic="verisk.downloads_pending",
            worker_type="enrichment",
        )
        # Add context headers
        headers.append(("error_category", b"transient"))
        headers.append(("domain", b"verisk"))

        message = make_pipeline_message(headers=headers)

        await scheduler._handle_retry_message(message)

        call_kwargs = mock_downloads_producer.send.call_args[1]
        sent_headers = call_kwargs["headers"]
        assert sent_headers["redelivered_from"] == "verisk.retry"
        assert sent_headers["retry_count"] == "2"
        assert sent_headers["worker_type"] == "enrichment"
        assert sent_headers["error_category"] == "transient"
        assert sent_headers["domain"] == "verisk"

    async def test_route_missing_context_headers_omitted(
        self, scheduler, mock_downloads_producer
    ):
        """Context headers not present in message are not added."""
        past_time = (datetime.now(UTC) - timedelta(seconds=10)).isoformat()
        headers = make_retry_headers(
            retry_count="1",
            scheduled_retry_time=past_time,
        )
        message = make_pipeline_message(headers=headers)

        await scheduler._handle_retry_message(message)

        call_kwargs = mock_downloads_producer.send.call_args[1]
        sent_headers = call_kwargs["headers"]
        assert "error_category" not in sent_headers
        assert "domain" not in sent_headers


class TestProducerPoolRouting:
    """Test that messages are routed to the correct producer based on target_topic."""

    async def test_routes_to_downloads_producer(self, scheduler, mock_downloads_producer):
        """Messages targeting downloads_pending use the downloads producer."""
        past_time = (datetime.now(UTC) - timedelta(seconds=10)).isoformat()
        headers = make_retry_headers(
            target_topic="verisk.downloads_pending",
            scheduled_retry_time=past_time,
        )
        message = make_pipeline_message(headers=headers)

        await scheduler._handle_retry_message(message)

        mock_downloads_producer.send.assert_called_once()
        assert scheduler._messages_routed == 1

    async def test_routes_to_enrichment_producer(self, scheduler, mock_enrichment_producer):
        """Messages targeting enrichment_pending use the enrichment producer."""
        past_time = (datetime.now(UTC) - timedelta(seconds=10)).isoformat()
        headers = make_retry_headers(
            target_topic="verisk.enrichment_pending",
            scheduled_retry_time=past_time,
        )
        message = make_pipeline_message(headers=headers)

        await scheduler._handle_retry_message(message)

        mock_enrichment_producer.send.assert_called_once()
        assert scheduler._messages_routed == 1

    async def test_routes_to_results_producer(self, scheduler, mock_results_producer):
        """Messages targeting downloads_results use the results producer."""
        past_time = (datetime.now(UTC) - timedelta(seconds=10)).isoformat()
        headers = make_retry_headers(
            target_topic="verisk.downloads_results",
            scheduled_retry_time=past_time,
        )
        message = make_pipeline_message(headers=headers)

        await scheduler._handle_retry_message(message)

        mock_results_producer.send.assert_called_once()
        assert scheduler._messages_routed == 1

    async def test_unknown_target_topic_routes_to_dlq(self, scheduler, mock_dlq_producer):
        """Messages with unknown target_topic go to DLQ."""
        past_time = (datetime.now(UTC) - timedelta(seconds=10)).isoformat()
        headers = make_retry_headers(
            target_topic="verisk.nonexistent_topic",
            scheduled_retry_time=past_time,
        )
        message = make_pipeline_message(headers=headers)

        await scheduler._handle_retry_message(message)

        mock_dlq_producer.send.assert_called_once()
        call_kwargs = mock_dlq_producer.send.call_args[1]
        assert call_kwargs["topic"] == "verisk.dlq"
        sent_headers = call_kwargs["headers"]
        assert "Unknown target topic" in sent_headers["dlq_reason"]
        assert scheduler._messages_malformed == 1

    async def test_unknown_target_does_not_raise(self, scheduler, mock_dlq_producer):
        """Unknown target topic is handled gracefully (no exception)."""
        past_time = (datetime.now(UTC) - timedelta(seconds=10)).isoformat()
        headers = make_retry_headers(
            target_topic="verisk.nonexistent_topic",
            scheduled_retry_time=past_time,
        )
        message = make_pipeline_message(headers=headers)

        # Should not raise
        await scheduler._handle_retry_message(message)


class TestSendToDlq:
    """Test _send_to_dlq method."""

    async def test_sends_message_to_dlq_topic(self, scheduler, mock_dlq_producer):
        """DLQ message is sent to the configured DLQ topic."""
        message = make_pipeline_message()
        headers = {"target_topic": "verisk.downloads_pending"}

        await scheduler._send_to_dlq(message, "test reason", headers)

        mock_dlq_producer.send.assert_called_once()
        call_kwargs = mock_dlq_producer.send.call_args[1]
        assert call_kwargs["topic"] == "verisk.dlq"
        assert call_kwargs["key"] == b"msg-key"
        assert call_kwargs["value"] == b'{"data": "test"}'

    async def test_dlq_headers_include_reason_and_context(self, scheduler, mock_dlq_producer):
        """DLQ message headers include reason, original context, and failed flag."""
        message = make_pipeline_message(partition=2, offset=50)
        headers = {"target_topic": "verisk.downloads_pending", "worker_type": "download"}

        await scheduler._send_to_dlq(message, "Missing headers", headers)

        call_kwargs = mock_dlq_producer.send.call_args[1]
        sent_headers = call_kwargs["headers"]
        assert sent_headers["dlq_reason"] == "Missing headers"
        assert sent_headers["original_topic"] == "verisk.retry"
        assert sent_headers["original_partition"] == "2"
        assert sent_headers["original_offset"] == "50"
        assert sent_headers["failed"] == "true"
        assert sent_headers["target_topic"] == "verisk.downloads_pending"
        assert sent_headers["worker_type"] == "download"

    async def test_dlq_send_failure_re_raises(self, scheduler, mock_dlq_producer):
        """DLQ send failure is re-raised to prevent offset commit."""
        mock_dlq_producer.send.side_effect = Exception("Transport unavailable")
        message = make_pipeline_message()

        with pytest.raises(Exception, match="Transport unavailable"):
            await scheduler._send_to_dlq(message, "test reason", {})


class TestRouteToTarget:
    """Test _route_to_target method."""

    async def test_increments_messages_routed(self, scheduler, mock_downloads_producer):
        """Successful routing increments the counter."""
        await scheduler._route_to_target(
            target_topic="verisk.downloads_pending",
            message_key=b"key",
            message_value=b"value",
            retry_count=1,
            worker_type="download",
            headers={},
            original_topic="verisk.retry",
        )

        assert scheduler._messages_routed == 1

    async def test_route_failure_re_raises(self, scheduler, mock_downloads_producer):
        """Routing failure is re-raised to prevent offset commit."""
        mock_downloads_producer.send.side_effect = Exception("Transport down")

        with pytest.raises(Exception, match="Transport down"):
            await scheduler._route_to_target(
                target_topic="verisk.downloads_pending",
                message_key=b"key",
                message_value=b"value",
                retry_count=1,
                worker_type="download",
                headers={},
                original_topic="verisk.retry",
            )

    async def test_preserves_error_category_header(self, scheduler, mock_downloads_producer):
        """error_category from original headers is preserved."""
        await scheduler._route_to_target(
            target_topic="verisk.downloads_pending",
            message_key=b"key",
            message_value=b"value",
            retry_count=1,
            worker_type="download",
            headers={"error_category": "transient", "domain": "verisk"},
            original_topic="verisk.retry",
        )

        call_kwargs = mock_downloads_producer.send.call_args[1]
        assert call_kwargs["headers"]["error_category"] == "transient"
        assert call_kwargs["headers"]["domain"] == "verisk"

    async def test_unknown_topic_routes_to_dlq(self, scheduler, mock_dlq_producer):
        """Unknown target_topic sends message to DLQ instead of raising."""
        await scheduler._route_to_target(
            target_topic="verisk.nonexistent",
            message_key=b"key",
            message_value=b"value",
            retry_count=1,
            worker_type="download",
            headers={},
            original_topic="verisk.retry",
        )

        mock_dlq_producer.send.assert_called_once()
        call_kwargs = mock_dlq_producer.send.call_args[1]
        assert "Unknown target topic" in call_kwargs["headers"]["dlq_reason"]
        assert scheduler._messages_malformed == 1
        assert scheduler._messages_routed == 0


class TestProcessDelayedMessages:
    """Test _process_delayed_messages background task."""

    async def test_processes_ready_message(self, scheduler, mock_downloads_producer):
        """Background processor routes messages when their time arrives."""
        from pipeline.common.retry.delay_queue import DelayedMessage

        past_time = datetime.now(UTC) - timedelta(seconds=10)
        delayed_msg = DelayedMessage(
            scheduled_time=past_time,
            target_topic="verisk.downloads_pending",
            retry_count=1,
            worker_type="download",
            message_key=b"key",
            message_value=b"value",
            headers={"retry_count": "1"},
        )
        scheduler._delay_queue.push(delayed_msg)
        scheduler._running = True

        # Run one iteration by stopping after first loop
        async def stop_after_route(*args, **kwargs):
            scheduler._running = False

        mock_downloads_producer.send.side_effect = stop_after_route

        await scheduler._process_delayed_messages()

        mock_downloads_producer.send.assert_called_once()
        call_kwargs = mock_downloads_producer.send.call_args[1]
        assert call_kwargs["topic"] == "verisk.downloads_pending"

    async def test_requeues_on_route_failure(self, scheduler, mock_downloads_producer):
        """Failed delayed message routing requeues the message."""
        from pipeline.common.retry.delay_queue import DelayedMessage

        past_time = datetime.now(UTC) - timedelta(seconds=10)
        delayed_msg = DelayedMessage(
            scheduled_time=past_time,
            target_topic="verisk.downloads_pending",
            retry_count=1,
            worker_type="download",
            message_key=b"key",
            message_value=b"value",
            headers={},
        )
        scheduler._delay_queue.push(delayed_msg)
        scheduler._running = True

        call_count = 0

        async def fail_then_stop(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("Transport down")
            scheduler._running = False

        mock_downloads_producer.send.side_effect = fail_then_stop

        await scheduler._process_delayed_messages()

        # Message should have been requeued (queue not empty after first failure,
        # then processed again on second iteration)
        assert mock_downloads_producer.send.call_count == 2

    async def test_routing_failure_requeues_with_5s_delay(
        self, scheduler, mock_downloads_producer
    ):
        """Route to target fails -> message requeued in delay queue with 5s delay (not lost)."""
        from pipeline.common.retry.delay_queue import DelayedMessage

        past_time = datetime.now(UTC) - timedelta(seconds=10)
        delayed_msg = DelayedMessage(
            scheduled_time=past_time,
            target_topic="verisk.downloads_pending",
            retry_count=1,
            worker_type="download",
            message_key=b"key",
            message_value=b"value",
            headers={},
        )
        scheduler._delay_queue.push(delayed_msg)
        scheduler._running = True

        # Fail once, then stop
        async def fail_once(*args, **kwargs):
            scheduler._running = False
            raise Exception("Transport down")

        mock_downloads_producer.send.side_effect = fail_once

        before = datetime.now(UTC)
        await scheduler._process_delayed_messages()

        # Message should be requeued (not lost)
        assert len(scheduler._delay_queue) == 1
        # Verify the requeued message has a ~5s delay from now
        next_time = scheduler._delay_queue.next_scheduled_time
        assert next_time is not None
        expected_min = before + timedelta(seconds=4)
        expected_max = before + timedelta(seconds=7)
        assert expected_min <= next_time <= expected_max

    async def test_exhausted_retries_route_to_dlq_not_another_retry(
        self, scheduler, mock_dlq_producer
    ):
        """retry_count >= max_retries -> DLQ, not another retry (verifies DLQ topic used)."""
        # max_retries is 3 from mock_config
        headers = make_retry_headers(retry_count="3")
        message = make_pipeline_message(headers=headers)

        await scheduler._handle_retry_message(message)

        mock_dlq_producer.send.assert_called_once()
        call_kwargs = mock_dlq_producer.send.call_args[1]
        assert call_kwargs["topic"] == "verisk.dlq"
        sent_headers = call_kwargs["headers"]
        assert sent_headers["dlq_reason"] == "Retries exhausted (3/3)"
        assert scheduler._messages_exhausted == 1

    async def test_processor_task_crash_restarts(self, scheduler, mock_downloads_producer):
        """Background delayed message processor throws -> error logged, loop continues."""
        from pipeline.common.retry.delay_queue import DelayedMessage

        past_time = datetime.now(UTC) - timedelta(seconds=10)
        delayed_msg = DelayedMessage(
            scheduled_time=past_time,
            target_topic="verisk.downloads_pending",
            retry_count=1,
            worker_type="download",
            message_key=b"key",
            message_value=b"value",
            headers={},
        )
        scheduler._delay_queue.push(delayed_msg)
        scheduler._running = True

        call_count = 0

        # First call: pop_ready raises unexpected error caught by outer except
        def exploding_pop_ready(now):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("unexpected internal error")
            scheduler._running = False
            return []

        scheduler._delay_queue.pop_ready = exploding_pop_ready

        with patch("pipeline.common.retry.unified_scheduler.asyncio.sleep", new_callable=AsyncMock):
            await scheduler._process_delayed_messages()

        # Loop continued past the crash (called pop_ready at least twice)
        assert call_count >= 2
