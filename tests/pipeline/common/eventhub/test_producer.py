"""Tests for pipeline.common.eventhub.producer module.

Covers EventHubRecordMetadata and EventHubProducer, including send, send_batch,
start/stop lifecycle, and error handling.
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipeline.common.types import ProduceResult


# =============================================================================
# EventHubRecordMetadata
# =============================================================================


class TestEventHubRecordMetadata:
    def test_creates_produce_result(self):
        from pipeline.common.eventhub.producer import EventHubRecordMetadata

        metadata = EventHubRecordMetadata(topic="my-entity", partition=2, offset=10)
        result = metadata.to_produce_result()

        assert isinstance(result, ProduceResult)
        assert result.topic == "my-entity"
        assert result.partition == 2
        assert result.offset == 10

    def test_defaults_partition_and_offset_to_zero(self):
        from pipeline.common.eventhub.producer import EventHubRecordMetadata

        metadata = EventHubRecordMetadata(topic="entity")
        result = metadata.to_produce_result()

        assert result.partition == 0
        assert result.offset == 0


# =============================================================================
# EventHubProducer - Init
# =============================================================================


class TestEventHubProducerInit:
    def test_init_sets_attributes(self):
        from pipeline.common.eventhub.producer import EventHubProducer

        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.net/;SharedAccessKey=k",
            domain="verisk",
            worker_name="test-worker",
            eventhub_name="my-entity",
        )

        assert producer.eventhub_name == "my-entity"
        assert producer.domain == "verisk"
        assert producer._started is False
        assert producer._producer is None

    def test_is_started_property_false_initially(self):
        from pipeline.common.eventhub.producer import EventHubProducer

        producer = EventHubProducer(
            connection_string="conn",
            domain="verisk",
            worker_name="w",
            eventhub_name="entity",
        )

        assert producer.is_started is False


# =============================================================================
# EventHubProducer - Start / Stop
# =============================================================================


class TestEventHubProducerLifecycle:
    def _make_producer(self):
        from pipeline.common.eventhub.producer import EventHubProducer

        return EventHubProducer(
            connection_string="Endpoint=sb://test.net/;SharedAccessKeyName=P;SharedAccessKey=k",
            domain="verisk",
            worker_name="test",
            eventhub_name="my-entity",
        )

    @patch("pipeline.common.eventhub.producer.update_connection_status")
    @patch("pipeline.common.eventhub.producer.log_connection_attempt_details")
    @patch("pipeline.common.eventhub.producer.log_connection_diagnostics")
    @patch("pipeline.common.eventhub.producer.EventHubProducerClient")
    async def test_start_creates_client_and_gets_properties(
        self, MockClient, mock_diag, mock_attempt, mock_conn_status
    ):
        producer = self._make_producer()
        mock_instance = MagicMock()
        mock_instance.get_eventhub_properties.return_value = {
            "name": "my-entity",
            "partition_ids": ["0", "1"],
        }
        MockClient.from_connection_string.return_value = mock_instance

        await producer.start()

        assert producer._started is True
        assert producer._producer is mock_instance
        mock_conn_status.assert_called_with("producer", connected=True)

    async def test_start_when_already_started_returns_early(self):
        producer = self._make_producer()
        producer._started = True

        # Should not raise
        await producer.start()

    @patch("pipeline.common.eventhub.producer.update_connection_status")
    @patch("pipeline.common.eventhub.producer.log_connection_attempt_details")
    @patch("pipeline.common.eventhub.producer.log_connection_diagnostics")
    @patch("pipeline.common.eventhub.producer.EventHubProducerClient")
    async def test_start_raises_on_connection_failure(
        self, MockClient, mock_diag, mock_attempt, mock_conn_status
    ):
        producer = self._make_producer()
        MockClient.from_connection_string.side_effect = RuntimeError("connect failed")

        with pytest.raises(RuntimeError, match="connect failed"):
            await producer.start()

        assert producer._started is False

    @patch("pipeline.common.eventhub.producer.update_connection_status")
    async def test_stop_closes_producer(self, mock_conn_status):
        producer = self._make_producer()
        producer._started = True
        mock_client = MagicMock()
        producer._producer = mock_client

        await producer.stop()

        mock_client.close.assert_called_once()
        assert producer._started is False
        assert producer._producer is None
        mock_conn_status.assert_called_with("producer", connected=False)

    async def test_stop_when_not_started(self):
        producer = self._make_producer()

        # Should not raise
        await producer.stop()

    @patch("pipeline.common.eventhub.producer.update_connection_status")
    async def test_stop_handles_close_error(self, mock_conn_status):
        producer = self._make_producer()
        producer._started = True
        mock_client = MagicMock()
        mock_client.close.side_effect = RuntimeError("close fail")
        producer._producer = mock_client

        # Should not raise, error is logged
        await producer.stop()

        assert producer._started is False
        assert producer._producer is None


# =============================================================================
# EventHubProducer - send
# =============================================================================


class TestEventHubProducerSend:
    def _make_started_producer(self):
        from pipeline.common.eventhub.producer import EventHubProducer

        producer = EventHubProducer(
            connection_string="conn",
            domain="verisk",
            worker_name="test",
            eventhub_name="my-entity",
        )
        producer._started = True
        mock_client = MagicMock()
        mock_batch = MagicMock()
        mock_client.create_batch.return_value = mock_batch
        producer._producer = mock_client
        return producer, mock_client, mock_batch

    async def test_send_raises_when_not_started(self):
        from pipeline.common.eventhub.producer import EventHubProducer

        producer = EventHubProducer(
            connection_string="conn",
            domain="verisk",
            worker_name="test",
            eventhub_name="entity",
        )

        with pytest.raises(RuntimeError, match="not started"):
            await producer.send(value=b"data")

    @patch("pipeline.common.eventhub.producer.record_message_produced")
    @patch("pipeline.common.eventhub.producer.EventData")
    async def test_send_bytes_value(self, MockEventData, mock_record):
        producer, mock_client, mock_batch = self._make_started_producer()
        mock_event = MagicMock()
        mock_event.properties = {}
        MockEventData.return_value = mock_event

        result = await producer.send(value=b"raw bytes")

        assert isinstance(result, ProduceResult)
        assert result.topic == "my-entity"
        MockEventData.assert_called_once_with(b"raw bytes")
        mock_batch.add.assert_called_once_with(mock_event)
        mock_client.send_batch.assert_called_once_with(mock_batch)
        mock_record.assert_called_once_with("my-entity", 9, success=True)

    @patch("pipeline.common.eventhub.producer.record_message_produced")
    @patch("pipeline.common.eventhub.producer.EventData")
    async def test_send_dict_value(self, MockEventData, mock_record):
        producer, mock_client, mock_batch = self._make_started_producer()
        mock_event = MagicMock()
        mock_event.properties = {}
        MockEventData.return_value = mock_event

        result = await producer.send(value={"key": "value"})

        assert isinstance(result, ProduceResult)
        call_args = MockEventData.call_args[0][0]
        parsed = json.loads(call_args)
        assert parsed == {"key": "value"}

    @patch("pipeline.common.eventhub.producer.record_message_produced")
    @patch("pipeline.common.eventhub.producer.EventData")
    async def test_send_with_key_string(self, MockEventData, mock_record):
        producer, mock_client, mock_batch = self._make_started_producer()
        mock_event = MagicMock()
        mock_event.properties = {}
        MockEventData.return_value = mock_event

        await producer.send(value=b"data", key="my-key")

        assert mock_event.properties["_key"] == "my-key"

    @patch("pipeline.common.eventhub.producer.record_message_produced")
    @patch("pipeline.common.eventhub.producer.EventData")
    async def test_send_with_key_bytes(self, MockEventData, mock_record):
        producer, mock_client, mock_batch = self._make_started_producer()
        mock_event = MagicMock()
        mock_event.properties = {}
        MockEventData.return_value = mock_event

        await producer.send(value=b"data", key=b"bytes-key")

        assert mock_event.properties["_key"] == "bytes-key"

    @patch("pipeline.common.eventhub.producer.record_message_produced")
    @patch("pipeline.common.eventhub.producer.EventData")
    async def test_send_with_headers(self, MockEventData, mock_record):
        producer, mock_client, mock_batch = self._make_started_producer()
        mock_event = MagicMock()
        mock_event.properties = {}
        MockEventData.return_value = mock_event

        await producer.send(
            value=b"data",
            headers={"header1": "val1", "header2": "val2"},
        )

        assert mock_event.properties["header1"] == "val1"
        assert mock_event.properties["header2"] == "val2"

    @patch("pipeline.common.eventhub.producer.record_message_produced")
    @patch("pipeline.common.eventhub.producer.EventData")
    async def test_send_defaults_topic_to_eventhub_name(self, MockEventData, mock_record):
        producer, mock_client, mock_batch = self._make_started_producer()
        mock_event = MagicMock()
        mock_event.properties = {}
        MockEventData.return_value = mock_event

        result = await producer.send(value=b"data")

        assert result.topic == "my-entity"

    @patch("pipeline.common.eventhub.producer.record_message_produced")
    @patch("pipeline.common.eventhub.producer.record_producer_error")
    @patch("pipeline.common.eventhub.producer.EventData")
    async def test_send_records_failure_on_exception(
        self, MockEventData, mock_error, mock_record
    ):
        producer, mock_client, mock_batch = self._make_started_producer()
        mock_event = MagicMock()
        mock_event.properties = {}
        MockEventData.return_value = mock_event
        mock_client.send_batch.side_effect = RuntimeError("send fail")

        with pytest.raises(RuntimeError, match="send fail"):
            await producer.send(value=b"data")

        mock_record.assert_called_once_with("my-entity", 4, success=False)
        mock_error.assert_called_once()

    @patch("pipeline.common.eventhub.producer.record_message_produced")
    @patch("pipeline.common.eventhub.producer.EventData")
    async def test_send_with_pydantic_model(self, MockEventData, mock_record):
        producer, mock_client, mock_batch = self._make_started_producer()
        mock_event = MagicMock()
        mock_event.properties = {}
        MockEventData.return_value = mock_event

        # Create a mock BaseModel
        mock_model = MagicMock()
        mock_model.model_dump_json.return_value = '{"field": "value"}'

        # Need to make isinstance check work for BaseModel
        from pydantic import BaseModel
        mock_model.__class__ = type("MockModel", (BaseModel,), {"__annotations__": {}})

        result = await producer.send(value=mock_model)
        assert isinstance(result, ProduceResult)


# =============================================================================
# EventHubProducer - send_batch
# =============================================================================


class TestEventHubProducerSendBatch:
    def _make_started_producer(self):
        from pipeline.common.eventhub.producer import EventHubProducer

        producer = EventHubProducer(
            connection_string="conn",
            domain="verisk",
            worker_name="test",
            eventhub_name="my-entity",
        )
        producer._started = True
        mock_client = MagicMock()
        mock_batch = MagicMock()
        mock_client.create_batch.return_value = mock_batch
        producer._producer = mock_client
        return producer, mock_client, mock_batch

    async def test_send_batch_raises_when_not_started(self):
        from pipeline.common.eventhub.producer import EventHubProducer

        producer = EventHubProducer(
            connection_string="conn",
            domain="verisk",
            worker_name="test",
            eventhub_name="entity",
        )

        with pytest.raises(RuntimeError, match="not started"):
            await producer.send_batch(messages=[("key", MagicMock())])

    @patch("pipeline.common.eventhub.producer.record_message_produced")
    @patch("pipeline.common.eventhub.producer.message_processing_duration_seconds")
    async def test_send_batch_empty_messages_returns_empty_list(
        self, mock_duration, mock_record
    ):
        producer, _, _ = self._make_started_producer()

        result = await producer.send_batch(messages=[])

        assert result == []

    @patch("pipeline.common.eventhub.producer.record_message_produced")
    @patch("pipeline.common.eventhub.producer.message_processing_duration_seconds")
    @patch("pipeline.common.eventhub.producer.EventData")
    async def test_send_batch_returns_produce_results(
        self, MockEventData, mock_duration, mock_record
    ):
        producer, mock_client, mock_batch = self._make_started_producer()

        mock_event = MagicMock()
        mock_event.properties = {}
        MockEventData.return_value = mock_event

        model1 = MagicMock()
        model1.model_dump_json.return_value = '{"a": 1}'
        model2 = MagicMock()
        model2.model_dump_json.return_value = '{"b": 2}'

        results = await producer.send_batch(messages=[("k1", model1), ("k2", model2)])

        assert len(results) == 2
        assert all(isinstance(r, ProduceResult) for r in results)
        assert results[0].offset == 0
        assert results[1].offset == 1
        mock_client.send_batch.assert_called_once_with(mock_batch)

    @patch("pipeline.common.eventhub.producer.record_message_produced")
    @patch("pipeline.common.eventhub.producer.record_producer_error")
    @patch("pipeline.common.eventhub.producer.message_processing_duration_seconds")
    @patch("pipeline.common.eventhub.producer.EventData")
    async def test_send_batch_records_failure_on_exception(
        self, MockEventData, mock_duration, mock_error, mock_record
    ):
        producer, mock_client, mock_batch = self._make_started_producer()

        mock_event = MagicMock()
        mock_event.properties = {}
        MockEventData.return_value = mock_event
        mock_client.send_batch.side_effect = RuntimeError("batch fail")

        model = MagicMock()
        model.model_dump_json.return_value = '{"x": 1}'

        with pytest.raises(RuntimeError, match="batch fail"):
            await producer.send_batch(messages=[("k", model)])

        mock_error.assert_called_once()

    @patch("pipeline.common.eventhub.producer.record_message_produced")
    @patch("pipeline.common.eventhub.producer.message_processing_duration_seconds")
    @patch("pipeline.common.eventhub.producer.EventData")
    async def test_send_batch_with_headers(
        self, MockEventData, mock_duration, mock_record
    ):
        producer, mock_client, mock_batch = self._make_started_producer()

        mock_event = MagicMock()
        mock_event.properties = {}
        MockEventData.return_value = mock_event

        model = MagicMock()
        model.model_dump_json.return_value = '{"x": 1}'

        await producer.send_batch(
            messages=[("k", model)],
            headers={"h1": "v1"},
        )

        # Properties should include the header
        assert mock_event.properties["h1"] == "v1"


# =============================================================================
# EventHubProducer - flush
# =============================================================================


class TestEventHubProducerFlush:
    async def test_flush_raises_when_not_started(self):
        from pipeline.common.eventhub.producer import EventHubProducer

        producer = EventHubProducer(
            connection_string="conn",
            domain="verisk",
            worker_name="test",
            eventhub_name="entity",
        )

        with pytest.raises(RuntimeError, match="not started"):
            await producer.flush()

    async def test_flush_succeeds_when_started(self):
        from pipeline.common.eventhub.producer import EventHubProducer

        producer = EventHubProducer(
            connection_string="conn",
            domain="verisk",
            worker_name="test",
            eventhub_name="entity",
        )
        producer._started = True
        producer._producer = MagicMock()

        # Should not raise
        await producer.flush()
