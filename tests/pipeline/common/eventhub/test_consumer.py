"""Tests for pipeline.common.eventhub.consumer module.

Covers EventHubConsumerRecord and EventHubConsumer, including message conversion,
start/stop lifecycle, DLQ routing, error handling, and WebSocket reconnection.
"""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipeline.common.types import PipelineMessage

# =============================================================================
# EventHubConsumerRecord
# =============================================================================


class TestEventHubConsumerRecord:
    def _make_event_data(
        self,
        body=b"test-body",
        properties=None,
        enqueued_time=None,
        offset="100",
    ):
        event = MagicMock()
        event.body = [body] if isinstance(body, bytes) else body
        event.properties = properties or {}
        event.enqueued_time = enqueued_time
        event.offset = offset
        return event

    def test_converts_basic_event_to_pipeline_message(self):
        from pipeline.common.eventhub.consumer import EventHubConsumerRecord

        event = self._make_event_data(
            body=b'{"key": "value"}',
            enqueued_time=datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC),
            offset="42",
        )

        record = EventHubConsumerRecord(event, "my-entity", "3")
        msg = record.to_pipeline_message()

        assert msg.topic == "my-entity"
        assert msg.partition == 3
        assert msg.offset == "42"
        assert msg.value == b'{"key": "value"}'
        assert msg.timestamp > 0

    def test_extracts_key_from_properties(self):
        from pipeline.common.eventhub.consumer import EventHubConsumerRecord

        event = self._make_event_data(
            properties={"_key": "my-message-key", "custom": "header"},
        )

        record = EventHubConsumerRecord(event, "entity", "0")
        msg = record.to_pipeline_message()

        assert msg.key == b"my-message-key"

    def test_converts_properties_to_headers_excluding_key(self):
        from pipeline.common.eventhub.consumer import EventHubConsumerRecord

        event = self._make_event_data(
            properties={"_key": "k", "header1": "val1", "header2": "val2"},
        )

        record = EventHubConsumerRecord(event, "entity", "0")
        msg = record.to_pipeline_message()

        header_keys = [h[0] for h in msg.headers]
        assert "_key" not in header_keys
        assert "header1" in header_keys
        assert "header2" in header_keys

    def test_handles_no_properties(self):
        from pipeline.common.eventhub.consumer import EventHubConsumerRecord

        event = self._make_event_data(properties=None)

        record = EventHubConsumerRecord(event, "entity", "0")
        msg = record.to_pipeline_message()

        assert msg.key is None
        assert msg.headers is None

    def test_handles_empty_properties(self):
        from pipeline.common.eventhub.consumer import EventHubConsumerRecord

        event = self._make_event_data(properties={})

        record = EventHubConsumerRecord(event, "entity", "0")
        msg = record.to_pipeline_message()

        assert msg.key is None
        assert msg.headers is None

    def test_handles_none_enqueued_time(self):
        from pipeline.common.eventhub.consumer import EventHubConsumerRecord

        event = self._make_event_data(enqueued_time=None)

        record = EventHubConsumerRecord(event, "entity", "0")
        msg = record.to_pipeline_message()

        assert msg.timestamp == 0

    def test_handles_empty_partition_string(self):
        from pipeline.common.eventhub.consumer import EventHubConsumerRecord

        event = self._make_event_data()

        record = EventHubConsumerRecord(event, "entity", "")
        msg = record.to_pipeline_message()

        assert msg.partition == 0

    def test_handles_empty_key_string_in_properties(self):
        from pipeline.common.eventhub.consumer import EventHubConsumerRecord

        event = self._make_event_data(properties={"_key": ""})

        record = EventHubConsumerRecord(event, "entity", "0")
        msg = record.to_pipeline_message()

        assert msg.key is None

    def test_multi_segment_body(self):
        from pipeline.common.eventhub.consumer import EventHubConsumerRecord

        event = MagicMock()
        event.body = [b"hello ", b"world"]
        event.properties = {}
        event.enqueued_time = None
        event.offset = "0"

        record = EventHubConsumerRecord(event, "entity", "0")
        msg = record.to_pipeline_message()

        assert msg.value == b"hello world"

    def test_raw_bytes_body(self):
        from pipeline.common.eventhub.consumer import EventHubConsumerRecord

        event = MagicMock()
        event.body = b'{"key": "value"}'
        event.properties = {}
        event.enqueued_time = None
        event.offset = "0"

        record = EventHubConsumerRecord(event, "entity", "0")
        msg = record.to_pipeline_message()

        assert msg.value == b'{"key": "value"}'


# =============================================================================
# EventHubConsumer
# =============================================================================


class TestEventHubConsumer:
    def _make_consumer(self, **overrides):
        from pipeline.common.eventhub.consumer import EventHubConsumer

        defaults = {
            "connection_string": "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=P;SharedAccessKey=k",
            "domain": "verisk",
            "worker_name": "test-worker",
            "eventhub_name": "verisk_events",
            "consumer_group": "$Default",
            "message_handler": AsyncMock(),
        }
        defaults.update(overrides)

        with patch("pipeline.common.eventhub.consumer.generate_worker_id", return_value="test-id"):
            return EventHubConsumer(**defaults)

    def test_init_sets_attributes(self):
        consumer = self._make_consumer()

        assert consumer.eventhub_name == "verisk_events"
        assert consumer.consumer_group == "$Default"
        assert consumer.domain == "verisk"
        assert consumer._running is False
        assert consumer._consumer is None

    def test_init_with_instance_id(self):
        consumer = self._make_consumer(instance_id="inst-1")
        assert consumer.instance_id == "inst-1"

    def test_init_with_checkpoint_store(self):
        store = MagicMock()
        consumer = self._make_consumer(checkpoint_store=store)
        assert consumer.checkpoint_store is store

    def test_default_starting_position_is_latest(self):
        consumer = self._make_consumer()
        assert consumer.starting_position == "@latest"
        assert consumer.starting_position_inclusive is False

    def test_explicit_starting_position(self):
        consumer = self._make_consumer(starting_position="-1", starting_position_inclusive=False)
        assert consumer.starting_position == "-1"
        assert consumer.starting_position_inclusive is False

    def test_is_running_property_false_when_not_started(self):
        consumer = self._make_consumer()
        assert consumer.is_running is False

    async def test_start_when_already_running_returns_early(self):
        consumer = self._make_consumer()
        consumer._running = True

        # Should not raise or try to create client
        await consumer.start()

    async def test_stop_when_not_running(self):
        consumer = self._make_consumer()
        # Should not raise
        await consumer.stop()

    @patch("pipeline.common.eventhub.consumer.update_connection_status")
    @patch("pipeline.common.eventhub.consumer.update_assigned_partitions")
    async def test_stop_closes_consumer_and_updates_metrics(self, mock_partitions, mock_connection):
        consumer = self._make_consumer()
        consumer._running = True
        consumer._consumer = AsyncMock()

        await consumer.stop()

        assert consumer._consumer is None
        mock_connection.assert_called_with("consumer", connected=False)
        mock_partitions.assert_called_with("$Default", 0)
        assert consumer._running is False

    @patch("pipeline.common.eventhub.consumer.update_connection_status")
    @patch("pipeline.common.eventhub.consumer.update_assigned_partitions")
    async def test_stop_flushes_and_stops_dlq_producer(self, mock_partitions, mock_connection):
        consumer = self._make_consumer()
        consumer._running = True
        consumer._consumer = AsyncMock()
        consumer._dlq_producer = AsyncMock()

        await consumer.stop()

        assert consumer._dlq_producer is None

    async def test_commit_when_consumer_not_started(self):
        consumer = self._make_consumer()
        consumer._consumer = None

        # Should not raise
        await consumer.commit()

    async def test_commit_when_consumer_started_with_no_events(self):
        consumer = self._make_consumer()
        consumer._consumer = MagicMock()

        # No events tracked — should return without error
        await consumer.commit()

    async def test_commit_checkpoints_each_partition(self):
        consumer = self._make_consumer()
        consumer._consumer = MagicMock()

        ctx_0 = AsyncMock()
        ctx_1 = AsyncMock()
        event_0 = MagicMock(name="event-p0")
        event_1 = MagicMock(name="event-p1")

        consumer._current_partition_context = {"0": ctx_0, "1": ctx_1}
        consumer._last_partition_event = {"0": event_0, "1": event_1}

        await consumer.commit()

        ctx_0.update_checkpoint.assert_awaited_once_with(event_0)
        ctx_1.update_checkpoint.assert_awaited_once_with(event_1)
        assert consumer._checkpoint_count == 2
        assert consumer._last_partition_event == {}

    async def test_commit_skips_partition_without_context(self):
        consumer = self._make_consumer()
        consumer._consumer = MagicMock()

        event_0 = MagicMock(name="event-p0")
        consumer._current_partition_context = {}  # no context for partition "0"
        consumer._last_partition_event = {"0": event_0}

        await consumer.commit()

        # No checkpoint call since context is missing
        assert consumer._checkpoint_count == 0


# =============================================================================
# DLQ entity mapping
# =============================================================================


class TestDLQEntityMapping:
    def _make_consumer(self):
        from pipeline.common.eventhub.consumer import EventHubConsumer

        with patch("pipeline.common.eventhub.consumer.generate_worker_id", return_value="test-id"):
            return EventHubConsumer(
                connection_string="Endpoint=sb://test.net/;SharedAccessKey=k",
                domain="verisk",
                worker_name="test",
                eventhub_name="verisk_events",
                consumer_group="$Default",
                message_handler=AsyncMock(),
            )

    def test_build_dlq_entity_map_has_verisk_mapping(self):
        consumer = self._make_consumer()
        assert consumer._dlq_entity_map["verisk_events"] == "verisk-dlq"

    def test_build_dlq_entity_map_has_claimx_mapping(self):
        consumer = self._make_consumer()
        assert consumer._dlq_entity_map["claimx_events"] == "claimx-dlq"

    def test_get_dlq_entity_name_returns_mapped_entity(self):
        consumer = self._make_consumer()
        assert consumer._get_dlq_entity_name("verisk_events") == "verisk-dlq"

    def test_get_dlq_entity_name_returns_none_for_unknown_topic(self):
        consumer = self._make_consumer()
        assert consumer._get_dlq_entity_name("unknown_topic") is None


# =============================================================================
# _process_message
# =============================================================================


class TestProcessMessage:
    def _make_consumer(self, handler=None):
        from pipeline.common.eventhub.consumer import EventHubConsumer

        with patch("pipeline.common.eventhub.consumer.generate_worker_id", return_value="test-id"):
            return EventHubConsumer(
                connection_string="Endpoint=sb://test.net/;SharedAccessKey=k",
                domain="verisk",
                worker_name="test",
                eventhub_name="verisk_events",
                consumer_group="$Default",
                message_handler=handler or AsyncMock(),
            )

    @patch("pipeline.common.eventhub.consumer.record_message_consumed")
    @patch("pipeline.common.eventhub.consumer.message_processing_duration_seconds")
    async def test_process_message_calls_handler(self, mock_duration, mock_consumed):
        handler = AsyncMock()
        consumer = self._make_consumer(handler=handler)

        msg = PipelineMessage(
            topic="verisk_events",
            partition=0,
            offset=1,
            timestamp=1000,
            key=b"key1",
            value=b'{"data": "test"}',
        )

        await consumer._process_message(msg)

        handler.assert_awaited_once_with(msg)
        mock_consumed.assert_called_once()

    @patch("pipeline.common.eventhub.consumer.record_message_consumed")
    @patch("pipeline.common.eventhub.consumer.message_processing_duration_seconds")
    @patch("pipeline.common.eventhub.consumer.record_processing_error")
    async def test_process_message_handles_handler_error(
        self, mock_proc_error, mock_duration, mock_consumed
    ):

        handler = AsyncMock(side_effect=ValueError("bad data"))
        consumer = self._make_consumer(handler=handler)

        # Patch the DLQ send to succeed (so permanent errors don't re-raise)
        consumer._send_to_dlq = AsyncMock(return_value=True)

        msg = PipelineMessage(
            topic="verisk_events",
            partition=0,
            offset=1,
            timestamp=1000,
            key=None,
            value=b"data",
        )

        await consumer._process_message(msg)

        # Consumed should still be called (once with success=False)
        assert mock_consumed.call_count == 1


# =============================================================================
# _send_to_dlq
# =============================================================================


class TestSendToDlq:
    def _make_consumer(self):
        from pipeline.common.eventhub.consumer import EventHubConsumer

        with patch("pipeline.common.eventhub.consumer.generate_worker_id", return_value="test-id"):
            return EventHubConsumer(
                connection_string="Endpoint=sb://test.net/;SharedAccessKey=k",
                domain="verisk",
                worker_name="test",
                eventhub_name="verisk_events",
                consumer_group="$Default",
                message_handler=AsyncMock(),
            )

    async def test_send_to_dlq_returns_false_when_no_dlq_mapping(self):
        from core.errors.exceptions import ErrorCategory

        consumer = self._make_consumer()

        msg = PipelineMessage(
            topic="unknown_topic",
            partition=0,
            offset=1,
            timestamp=1000,
            value=b"data",
        )

        result = await consumer._send_to_dlq(msg, ValueError("err"), ErrorCategory.PERMANENT)
        assert result is False

    @patch("pipeline.common.eventhub.consumer.record_dlq_message")
    async def test_send_to_dlq_returns_true_on_success(self, mock_dlq_metric):
        from core.errors.exceptions import ErrorCategory

        consumer = self._make_consumer()

        # Mock the DLQ producer
        mock_producer = AsyncMock()
        mock_produce_result = MagicMock()
        mock_produce_result.partition = 0
        mock_produce_result.offset = 5
        mock_producer.send = AsyncMock(return_value=mock_produce_result)
        mock_producer.eventhub_name = "verisk-dlq"
        consumer._dlq_producer = mock_producer

        msg = PipelineMessage(
            topic="verisk_events",
            partition=0,
            offset=1,
            timestamp=1000,
            key=b"key1",
            value=b'{"data": "test"}',
            headers=[("h1", b"v1")],
        )

        result = await consumer._send_to_dlq(msg, ValueError("bad"), ErrorCategory.PERMANENT)

        assert result is True
        mock_producer.send.assert_awaited_once()
        mock_dlq_metric.assert_called_once()

    async def test_send_to_dlq_returns_false_when_producer_init_fails(self):
        from core.errors.exceptions import ErrorCategory

        consumer = self._make_consumer()
        consumer._ensure_dlq_producer = AsyncMock(side_effect=RuntimeError("fail"))

        msg = PipelineMessage(
            topic="verisk_events",
            partition=0,
            offset=1,
            timestamp=1000,
            value=b"data",
        )

        result = await consumer._send_to_dlq(msg, ValueError("err"), ErrorCategory.PERMANENT)
        assert result is False

    async def test_send_to_dlq_returns_false_when_send_fails(self):
        from core.errors.exceptions import ErrorCategory

        consumer = self._make_consumer()

        mock_producer = AsyncMock()
        mock_producer.send = AsyncMock(side_effect=RuntimeError("send fail"))
        mock_producer.eventhub_name = "verisk-dlq"
        consumer._dlq_producer = mock_producer

        msg = PipelineMessage(
            topic="verisk_events",
            partition=0,
            offset=1,
            timestamp=1000,
            value=b"data",
        )

        result = await consumer._send_to_dlq(msg, ValueError("err"), ErrorCategory.PERMANENT)
        assert result is False

    async def test_send_to_dlq_uses_offset_as_key_when_no_message_key(self):
        from core.errors.exceptions import ErrorCategory

        consumer = self._make_consumer()

        mock_producer = AsyncMock()
        mock_produce_result = MagicMock()
        mock_produce_result.partition = 0
        mock_produce_result.offset = 5
        mock_producer.send = AsyncMock(return_value=mock_produce_result)
        mock_producer.eventhub_name = "verisk-dlq"
        consumer._dlq_producer = mock_producer

        msg = PipelineMessage(
            topic="verisk_events",
            partition=0,
            offset=42,
            timestamp=1000,
            key=None,
            value=b"data",
        )

        await consumer._send_to_dlq(msg, ValueError("err"), ErrorCategory.PERMANENT)

        call_kwargs = mock_producer.send.call_args[1]
        assert call_kwargs["key"] == b"dlq-42"


# =============================================================================
# _ensure_dlq_producer
# =============================================================================


class TestEnsureDlqProducer:
    def _make_consumer(self):
        from pipeline.common.eventhub.consumer import EventHubConsumer

        with patch("pipeline.common.eventhub.consumer.generate_worker_id", return_value="test-id"):
            return EventHubConsumer(
                connection_string="Endpoint=sb://test.net/;SharedAccessKey=k",
                domain="verisk",
                worker_name="test",
                eventhub_name="verisk_events",
                consumer_group="$Default",
                message_handler=AsyncMock(),
            )

    @patch("pipeline.common.eventhub.consumer.EventHubProducer")
    async def test_creates_new_dlq_producer(self, MockProducer):
        consumer = self._make_consumer()
        mock_instance = AsyncMock()
        mock_instance.eventhub_name = "verisk-dlq"
        MockProducer.return_value = mock_instance

        await consumer._ensure_dlq_producer("verisk-dlq")

        assert consumer._dlq_producer is mock_instance
        mock_instance.start.assert_awaited_once()

    @patch("pipeline.common.eventhub.consumer.EventHubProducer")
    async def test_reuses_existing_dlq_producer_for_same_entity(self, MockProducer):
        consumer = self._make_consumer()
        existing = AsyncMock()
        existing.eventhub_name = "verisk-dlq"
        consumer._dlq_producer = existing

        await consumer._ensure_dlq_producer("verisk-dlq")

        # Should not create a new one
        MockProducer.assert_not_called()
        assert consumer._dlq_producer is existing

    @patch("pipeline.common.eventhub.consumer.EventHubProducer")
    async def test_recreates_producer_for_different_entity(self, MockProducer):
        consumer = self._make_consumer()
        old_producer = AsyncMock()
        old_producer.eventhub_name = "verisk-dlq"
        consumer._dlq_producer = old_producer

        new_instance = AsyncMock()
        new_instance.eventhub_name = "claimx-dlq"
        MockProducer.return_value = new_instance

        await consumer._ensure_dlq_producer("claimx-dlq")

        old_producer.stop.assert_awaited_once()
        assert consumer._dlq_producer is new_instance


# =============================================================================
# _handle_processing_error
# =============================================================================


class TestHandleProcessingError:
    def _make_consumer(self):
        from pipeline.common.eventhub.consumer import EventHubConsumer

        with patch("pipeline.common.eventhub.consumer.generate_worker_id", return_value="test-id"):
            return EventHubConsumer(
                connection_string="Endpoint=sb://test.net/;SharedAccessKey=k",
                domain="verisk",
                worker_name="test",
                eventhub_name="verisk_events",
                consumer_group="$Default",
                message_handler=AsyncMock(),
            )

    @patch("pipeline.common.eventhub.consumer.record_processing_error")
    @patch("pipeline.common.eventhub.consumer.TransportErrorClassifier")
    async def test_permanent_error_routes_to_dlq(self, MockClassifier, mock_record):
        from core.errors.exceptions import PermanentError

        consumer = self._make_consumer()
        consumer._send_to_dlq = AsyncMock(return_value=True)

        classified = PermanentError("bad data")
        MockClassifier.classify_consumer_error.return_value = classified

        msg = PipelineMessage(
            topic="verisk_events", partition=0, offset=1, timestamp=1000, value=b"x"
        )

        # Should not raise when DLQ succeeds
        await consumer._handle_processing_error(msg, ValueError("bad"), 0.1)

        consumer._send_to_dlq.assert_awaited_once()

    @patch("pipeline.common.eventhub.consumer.record_processing_error")
    @patch("pipeline.common.eventhub.consumer.TransportErrorClassifier")
    async def test_permanent_error_reraises_when_dlq_fails(self, MockClassifier, mock_record):
        from core.errors.exceptions import PermanentError

        consumer = self._make_consumer()
        consumer._send_to_dlq = AsyncMock(return_value=False)

        classified = PermanentError("bad data")
        MockClassifier.classify_consumer_error.return_value = classified

        msg = PipelineMessage(
            topic="verisk_events", partition=0, offset=1, timestamp=1000, value=b"x"
        )

        original_error = ValueError("original")
        with pytest.raises(ValueError, match="original"):
            await consumer._handle_processing_error(msg, original_error, 0.1)

    @patch("pipeline.common.eventhub.consumer.record_processing_error")
    @patch("pipeline.common.eventhub.consumer.TransportErrorClassifier")
    async def test_transient_error_does_not_raise(self, MockClassifier, mock_record):
        from core.errors.exceptions import TransientError

        consumer = self._make_consumer()

        classified = TransientError("timeout")
        MockClassifier.classify_consumer_error.return_value = classified

        msg = PipelineMessage(
            topic="verisk_events", partition=0, offset=1, timestamp=1000, value=b"x"
        )

        # Should not raise for transient errors
        await consumer._handle_processing_error(msg, RuntimeError("timeout"), 0.1)


# =============================================================================
# _create_consumer_client
# =============================================================================


class TestCreateConsumerClient:
    def _make_consumer(self):
        from pipeline.common.eventhub.consumer import EventHubConsumer

        with patch("pipeline.common.eventhub.consumer.generate_worker_id", return_value="test-id"):
            return EventHubConsumer(
                connection_string="Endpoint=sb://test.net/;SharedAccessKeyName=P;SharedAccessKey=k",
                domain="verisk",
                worker_name="test",
                eventhub_name="verisk_events",
                consumer_group="$Default",
                message_handler=AsyncMock(),
            )

    @patch("pipeline.common.eventhub.consumer.get_ca_bundle_kwargs", return_value={})
    @patch("pipeline.common.eventhub.consumer.EventHubConsumerClient")
    def test_creates_client_with_correct_params(self, MockClient, mock_ssl):
        consumer = self._make_consumer()
        mock_instance = MagicMock()
        MockClient.from_connection_string.return_value = mock_instance

        result = consumer._create_consumer_client()

        assert result is mock_instance
        MockClient.from_connection_string.assert_called_once()
        call_kwargs = MockClient.from_connection_string.call_args[1]
        assert call_kwargs["consumer_group"] == "$Default"
        assert call_kwargs["eventhub_name"] == "verisk_events"


# =============================================================================
# _consume_loop reconnection
# =============================================================================


class TestConsumeLoopReconnection:
    def _make_consumer(self, **overrides):
        from pipeline.common.eventhub.consumer import EventHubConsumer

        defaults = {
            "connection_string": "Endpoint=sb://test.net/;SharedAccessKeyName=P;SharedAccessKey=k",
            "domain": "verisk",
            "worker_name": "test",
            "eventhub_name": "verisk_events",
            "consumer_group": "$Default",
            "message_handler": AsyncMock(),
        }
        defaults.update(overrides)

        with patch("pipeline.common.eventhub.consumer.generate_worker_id", return_value="test-id"):
            return EventHubConsumer(**defaults)

    @patch("pipeline.common.eventhub.consumer.update_connection_status")
    @patch("pipeline.common.eventhub.consumer.update_assigned_partitions")
    async def test_reconnects_on_transient_connect_error(
        self, mock_partitions, mock_connection
    ):
        """Consumer reconnects when receive() raises a transient error (WebSocket disconnect)."""
        consumer = self._make_consumer()
        consumer._running = True

        # First call raises, second call succeeds (returns normally)
        mock_client_1 = AsyncMock()
        mock_client_1.receive = AsyncMock(side_effect=RuntimeError("connection lost"))
        mock_client_1.close = AsyncMock()

        mock_client_2 = AsyncMock()
        mock_client_2.receive = AsyncMock(return_value=None)

        consumer._consumer = mock_client_1

        with (
            patch.object(consumer, "_create_consumer_client", return_value=mock_client_2),
            patch("pipeline.common.eventhub.consumer.asyncio.sleep", new_callable=AsyncMock),
            patch("pipeline.common.eventhub.consumer.TransportErrorClassifier") as MockClassifier,
        ):
            from core.errors.exceptions import TransientError

            MockClassifier.classify_consumer_error.return_value = TransientError("connection lost")

            await consumer._consume_loop()

        # Old client should have been closed
        mock_client_1.close.assert_awaited_once()

        # New client should have been used
        mock_client_2.receive.assert_awaited_once()

    @patch("pipeline.common.eventhub.consumer.update_connection_status")
    @patch("pipeline.common.eventhub.consumer.update_assigned_partitions")
    async def test_does_not_reconnect_on_permanent_error(
        self, mock_partitions, mock_connection
    ):
        """Consumer does NOT reconnect on permanent errors — raises immediately."""
        consumer = self._make_consumer()
        consumer._running = True

        mock_client = AsyncMock()
        mock_client.receive = AsyncMock(side_effect=RuntimeError("topic does not exist"))
        consumer._consumer = mock_client

        with (
            patch("pipeline.common.eventhub.consumer.TransportErrorClassifier") as MockClassifier,
        ):
            from core.errors.exceptions import PermanentError

            MockClassifier.classify_consumer_error.return_value = PermanentError(
                "topic does not exist"
            )

            with pytest.raises(RuntimeError, match="topic does not exist"):
                await consumer._consume_loop()

    @patch("pipeline.common.eventhub.consumer.update_connection_status")
    @patch("pipeline.common.eventhub.consumer.update_assigned_partitions")
    async def test_gives_up_after_max_consecutive_errors(
        self, mock_partitions, mock_connection
    ):
        """Consumer gives up after max consecutive transient errors."""
        consumer = self._make_consumer()
        consumer._running = True

        mock_client = AsyncMock()
        mock_client.receive = AsyncMock(side_effect=RuntimeError("connection lost"))
        mock_client.close = AsyncMock()
        consumer._consumer = mock_client

        with (
            patch.object(consumer, "_create_consumer_client", return_value=mock_client),
            patch("pipeline.common.eventhub.consumer.asyncio.sleep", new_callable=AsyncMock),
            patch("pipeline.common.eventhub.consumer.TransportErrorClassifier") as MockClassifier,
        ):
            from core.errors.exceptions import TransientError

            MockClassifier.classify_consumer_error.return_value = TransientError("connection lost")

            with pytest.raises(RuntimeError, match="connection lost"):
                await consumer._consume_loop()

        # Should have attempted 5 reconnections (max_consecutive_errors) before giving up.
        # receive() is called 6 times: initial + 5 retries
        assert mock_client.receive.await_count == 6

    @patch("pipeline.common.eventhub.consumer.update_connection_status")
    @patch("pipeline.common.eventhub.consumer.update_assigned_partitions")
    async def test_resets_error_count_after_stable_period(
        self, mock_partitions, mock_connection
    ):
        """Consecutive error count resets if receive() ran for >60s (stable_threshold)."""
        consumer = self._make_consumer()
        consumer._running = True

        call_count = 0

        async def receive_side_effect(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise RuntimeError("connection lost")
            # Third call succeeds
            return None

        mock_client = AsyncMock()
        mock_client.receive = AsyncMock(side_effect=receive_side_effect)
        mock_client.close = AsyncMock()
        consumer._consumer = mock_client

        with (
            patch.object(consumer, "_create_consumer_client", return_value=mock_client),
            patch("pipeline.common.eventhub.consumer.asyncio.sleep", new_callable=AsyncMock),
            patch("pipeline.common.eventhub.consumer.TransportErrorClassifier") as MockClassifier,
            patch("pipeline.common.eventhub.consumer.time") as mock_time,
        ):
            from core.errors.exceptions import TransientError

            MockClassifier.classify_consumer_error.return_value = TransientError("connection lost")

            # Simulate: first call runs 0s (immediate error), second call runs 120s (stable)
            # monotonic() is called once per loop iteration (receive_start)
            mock_time.monotonic.side_effect = [
                0.0,    # receive_start for attempt 1
                0.1,    # receive_duration check — ran 0.1s (not stable)
                100.0,  # receive_start for attempt 2
                220.0,  # receive_duration check — ran 120s (stable, resets counter)
                220.0,  # receive_start for attempt 3
            ]

            await consumer._consume_loop()

        assert call_count == 3

    async def test_exits_loop_when_running_set_to_false(self):
        """Consumer exits reconnection loop when _running is set to False."""
        consumer = self._make_consumer()
        consumer._running = True

        mock_client = AsyncMock()

        async def stop_running(**kwargs):
            consumer._running = False
            raise RuntimeError("connection lost")

        mock_client.receive = AsyncMock(side_effect=stop_running)
        consumer._consumer = mock_client

        # Should exit cleanly without raising
        await consumer._consume_loop()
