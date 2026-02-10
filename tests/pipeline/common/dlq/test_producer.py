"""
Unit tests for DLQProducer.

Test Coverage:
    - Lazy initialization (producer not created until first send)
    - Message production with full context
    - DLQ message serialization (JSON structure, headers)
    - Key handling (original key present, fallback to offset-based key)
    - Original headers decoded correctly
    - Error handling on send failure (logs, does not raise)
    - Stop lifecycle (flush, stop, idempotent)
    - Ensure_started idempotence (only creates producer once)

No infrastructure required - aiokafka mocked.
"""

import json
from unittest.mock import AsyncMock, Mock, patch

import pytest

from core.types import ErrorCategory
from pipeline.common.dlq.producer import DLQProducer
from pipeline.common.types import PipelineMessage


@pytest.fixture
def mock_config():
    """Mock MessageConfig."""
    config = Mock()
    config.bootstrap_servers = "localhost:9092"
    config.request_timeout_ms = 30000
    config.metadata_max_age_ms = 300000
    config.connections_max_idle_ms = 540000
    return config


@pytest.fixture
def dlq_producer(mock_config):
    """Create a DLQProducer with mocked config."""
    return DLQProducer(
        config=mock_config,
        domain="verisk",
        worker_name="download_worker",
        group_id="verisk-download-group",
        worker_id="worker-1",
    )


def make_pipeline_message(
    topic="verisk.downloads",
    partition=0,
    offset=42,
    timestamp=1700000000,
    key=b"msg-key-123",
    value=b'{"traceId": "abc"}',
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


class TestDLQProducerInitialization:
    """Test DLQProducer initialization."""

    def test_producer_not_created_on_init(self, dlq_producer):
        """Producer is None until first send."""
        assert dlq_producer._producer is None

    def test_stores_config_fields(self, dlq_producer):
        """All configuration fields are stored."""
        assert dlq_producer._domain == "verisk"
        assert dlq_producer._worker_name == "download_worker"
        assert dlq_producer._group_id == "verisk-download-group"
        assert dlq_producer._worker_id == "worker-1"


class TestDLQProducerEnsureStarted:
    """Test lazy producer initialization."""

    async def test_creates_producer_on_first_call(self, dlq_producer):
        """_ensure_started creates and starts the producer."""
        mock_aioproducer = AsyncMock()

        with (
            patch("pipeline.common.dlq.producer.AIOKafkaProducer", return_value=mock_aioproducer),
            patch("pipeline.common.dlq.producer.build_kafka_security_config", return_value={}),
        ):
            await dlq_producer._ensure_started()

        assert dlq_producer._producer is mock_aioproducer
        mock_aioproducer.start.assert_called_once()

    async def test_does_not_recreate_on_second_call(self, dlq_producer):
        """_ensure_started is idempotent."""
        mock_aioproducer = AsyncMock()
        dlq_producer._producer = mock_aioproducer

        await dlq_producer._ensure_started()

        # start should not be called again
        mock_aioproducer.start.assert_not_called()

    async def test_passes_security_config(self, mock_config):
        """Security config from build_kafka_security_config is applied."""
        producer = DLQProducer(
            config=mock_config,
            domain="verisk",
            worker_name="worker",
            group_id="group",
            worker_id="id",
        )

        security_config = {"security_protocol": "SASL_SSL", "sasl_mechanism": "OAUTHBEARER"}

        with (
            patch(
                "pipeline.common.dlq.producer.AIOKafkaProducer", return_value=AsyncMock()
            ) as mock_cls,
            patch(
                "pipeline.common.dlq.producer.build_kafka_security_config",
                return_value=security_config,
            ),
        ):
            await producer._ensure_started()

        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["security_protocol"] == "SASL_SSL"
        assert call_kwargs["sasl_mechanism"] == "OAUTHBEARER"
        assert call_kwargs["acks"] == "all"
        assert call_kwargs["enable_idempotence"] is True


class TestDLQProducerSend:
    """Test DLQ message production."""

    async def test_sends_message_to_dlq_topic(self, dlq_producer):
        """Message is sent to {original_topic}.dlq."""
        mock_kafka_producer = AsyncMock()
        mock_metadata = Mock(partition=0, offset=10)
        mock_kafka_producer.send_and_wait = AsyncMock(return_value=mock_metadata)
        dlq_producer._producer = mock_kafka_producer

        message = make_pipeline_message(topic="verisk.downloads")
        error = ValueError("Bad data")

        with patch("pipeline.common.dlq.producer.record_dlq_message"):
            await dlq_producer.send(message, error, ErrorCategory.PERMANENT)

        mock_kafka_producer.send_and_wait.assert_called_once()
        call_args = mock_kafka_producer.send_and_wait.call_args
        assert call_args[0][0] == "verisk.downloads.dlq"

    async def test_dlq_message_body_contains_full_context(self, dlq_producer):
        """DLQ message body includes original message context and error details."""
        mock_kafka_producer = AsyncMock()
        mock_metadata = Mock(partition=0, offset=10)
        mock_kafka_producer.send_and_wait = AsyncMock(return_value=mock_metadata)
        dlq_producer._producer = mock_kafka_producer

        message = make_pipeline_message(
            topic="verisk.downloads",
            partition=2,
            offset=99,
            timestamp=1700000000,
            key=b"msg-key",
            value=b'{"traceId": "abc"}',
            headers=[("source", b"test")],
        )
        error = RuntimeError("Connection refused")

        with patch("pipeline.common.dlq.producer.record_dlq_message"):
            await dlq_producer.send(message, error, ErrorCategory.TRANSIENT)

        call_args = mock_kafka_producer.send_and_wait.call_args
        dlq_value = json.loads(call_args[1]["value"])

        assert dlq_value["original_topic"] == "verisk.downloads"
        assert dlq_value["original_partition"] == 2
        assert dlq_value["original_offset"] == 99
        assert dlq_value["original_key"] == "msg-key"
        assert dlq_value["original_value"] == '{"traceId": "abc"}'
        assert dlq_value["original_headers"] == {"source": "test"}
        assert dlq_value["original_timestamp"] == 1700000000
        assert dlq_value["error_type"] == "RuntimeError"
        assert dlq_value["error_message"] == "Connection refused"
        assert dlq_value["error_category"] == "transient"
        assert dlq_value["consumer_group"] == "verisk-download-group"
        assert dlq_value["worker_id"] == "worker-1"
        assert dlq_value["domain"] == "verisk"
        assert dlq_value["worker_name"] == "download_worker"
        assert "dlq_timestamp" in dlq_value

    async def test_uses_original_key_when_present(self, dlq_producer):
        """DLQ message key is the original message key when present."""
        mock_kafka_producer = AsyncMock()
        mock_metadata = Mock(partition=0, offset=10)
        mock_kafka_producer.send_and_wait = AsyncMock(return_value=mock_metadata)
        dlq_producer._producer = mock_kafka_producer

        message = make_pipeline_message(key=b"original-key")

        with patch("pipeline.common.dlq.producer.record_dlq_message"):
            await dlq_producer.send(message, Exception("err"), ErrorCategory.PERMANENT)

        call_args = mock_kafka_producer.send_and_wait.call_args
        assert call_args[1]["key"] == b"original-key"

    async def test_generates_key_from_offset_when_key_is_none(self, dlq_producer):
        """DLQ message key falls back to offset-based key when original is None."""
        mock_kafka_producer = AsyncMock()
        mock_metadata = Mock(partition=0, offset=10)
        mock_kafka_producer.send_and_wait = AsyncMock(return_value=mock_metadata)
        dlq_producer._producer = mock_kafka_producer

        message = make_pipeline_message(key=None, offset=55)

        with patch("pipeline.common.dlq.producer.record_dlq_message"):
            await dlq_producer.send(message, Exception("err"), ErrorCategory.PERMANENT)

        call_args = mock_kafka_producer.send_and_wait.call_args
        assert call_args[1]["key"] == b"dlq-55"

    async def test_sends_dlq_headers(self, dlq_producer):
        """DLQ message includes appropriate Kafka headers."""
        mock_kafka_producer = AsyncMock()
        mock_metadata = Mock(partition=0, offset=10)
        mock_kafka_producer.send_and_wait = AsyncMock(return_value=mock_metadata)
        dlq_producer._producer = mock_kafka_producer

        message = make_pipeline_message(topic="verisk.downloads")

        with patch("pipeline.common.dlq.producer.record_dlq_message"):
            await dlq_producer.send(message, Exception("err"), ErrorCategory.PERMANENT)

        call_args = mock_kafka_producer.send_and_wait.call_args
        headers = call_args[1]["headers"]

        assert ("dlq_source_topic", b"verisk.downloads") in headers
        assert ("dlq_error_category", b"permanent") in headers
        assert ("dlq_consumer_group", b"verisk-download-group") in headers

    async def test_records_metrics_on_success(self, dlq_producer):
        """Successful DLQ send records a metric."""
        mock_kafka_producer = AsyncMock()
        mock_metadata = Mock(partition=0, offset=10)
        mock_kafka_producer.send_and_wait = AsyncMock(return_value=mock_metadata)
        dlq_producer._producer = mock_kafka_producer

        message = make_pipeline_message()

        with patch("pipeline.common.dlq.producer.record_dlq_message") as mock_record:
            await dlq_producer.send(message, Exception("err"), ErrorCategory.PERMANENT)

        mock_record.assert_called_once_with("verisk", "permanent")

    async def test_handles_none_value(self, dlq_producer):
        """DLQ message handles None original value."""
        mock_kafka_producer = AsyncMock()
        mock_metadata = Mock(partition=0, offset=10)
        mock_kafka_producer.send_and_wait = AsyncMock(return_value=mock_metadata)
        dlq_producer._producer = mock_kafka_producer

        message = make_pipeline_message(value=None)

        with patch("pipeline.common.dlq.producer.record_dlq_message"):
            await dlq_producer.send(message, Exception("err"), ErrorCategory.PERMANENT)

        call_args = mock_kafka_producer.send_and_wait.call_args
        dlq_value = json.loads(call_args[1]["value"])
        assert dlq_value["original_value"] is None

    async def test_handles_none_headers(self, dlq_producer):
        """DLQ message handles None original headers."""
        mock_kafka_producer = AsyncMock()
        mock_metadata = Mock(partition=0, offset=10)
        mock_kafka_producer.send_and_wait = AsyncMock(return_value=mock_metadata)
        dlq_producer._producer = mock_kafka_producer

        message = make_pipeline_message(headers=None)

        with patch("pipeline.common.dlq.producer.record_dlq_message"):
            await dlq_producer.send(message, Exception("err"), ErrorCategory.PERMANENT)

        call_args = mock_kafka_producer.send_and_wait.call_args
        dlq_value = json.loads(call_args[1]["value"])
        assert dlq_value["original_headers"] == {}


class TestDLQProducerSendErrorHandling:
    """Test DLQ send error handling."""

    async def test_send_failure_does_not_raise(self, dlq_producer):
        """Send failure is logged but does not raise."""
        mock_kafka_producer = AsyncMock()
        mock_kafka_producer.send_and_wait = AsyncMock(side_effect=Exception("Kafka down"))
        dlq_producer._producer = mock_kafka_producer

        message = make_pipeline_message()

        # Should not raise
        await dlq_producer.send(message, Exception("err"), ErrorCategory.PERMANENT)

    async def test_send_failure_does_not_record_metrics(self, dlq_producer):
        """Failed DLQ send does not record metrics."""
        mock_kafka_producer = AsyncMock()
        mock_kafka_producer.send_and_wait = AsyncMock(side_effect=Exception("Kafka down"))
        dlq_producer._producer = mock_kafka_producer

        message = make_pipeline_message()

        with patch("pipeline.common.dlq.producer.record_dlq_message") as mock_record:
            await dlq_producer.send(message, Exception("err"), ErrorCategory.PERMANENT)

        mock_record.assert_not_called()


class TestDLQProducerStop:
    """Test DLQ producer stop lifecycle."""

    async def test_stop_flushes_and_stops_producer(self, dlq_producer):
        """Stop flushes pending messages and stops the producer."""
        mock_kafka_producer = AsyncMock()
        dlq_producer._producer = mock_kafka_producer

        await dlq_producer.stop()

        mock_kafka_producer.flush.assert_called_once()
        mock_kafka_producer.stop.assert_called_once()
        assert dlq_producer._producer is None

    async def test_stop_when_producer_is_none(self, dlq_producer):
        """Stop is a no-op when producer was never initialized."""
        assert dlq_producer._producer is None
        await dlq_producer.stop()  # Should not raise

    async def test_stop_handles_flush_error(self, dlq_producer):
        """Stop handles errors during flush gracefully."""
        mock_kafka_producer = AsyncMock()
        mock_kafka_producer.flush.side_effect = Exception("Flush failed")
        dlq_producer._producer = mock_kafka_producer

        # Should not raise
        await dlq_producer.stop()

        # Producer should be set to None even on error
        assert dlq_producer._producer is None

    async def test_stop_handles_stop_error(self, dlq_producer):
        """Stop handles errors during producer.stop gracefully."""
        mock_kafka_producer = AsyncMock()
        mock_kafka_producer.stop.side_effect = Exception("Stop failed")
        dlq_producer._producer = mock_kafka_producer

        # Should not raise
        await dlq_producer.stop()

        # Producer should be set to None even on error
        assert dlq_producer._producer is None

    async def test_stop_idempotent(self, dlq_producer):
        """Calling stop twice does not raise."""
        mock_kafka_producer = AsyncMock()
        dlq_producer._producer = mock_kafka_producer

        await dlq_producer.stop()
        await dlq_producer.stop()  # Second call is no-op


class TestDLQProducerLazyInit:
    """Test lazy initialization through send."""

    async def test_send_initializes_producer_lazily(self, dlq_producer):
        """First send call triggers producer initialization."""
        mock_aioproducer = AsyncMock()
        mock_metadata = Mock(partition=0, offset=10)
        mock_aioproducer.send_and_wait = AsyncMock(return_value=mock_metadata)

        message = make_pipeline_message()

        with (
            patch("pipeline.common.dlq.producer.AIOKafkaProducer", return_value=mock_aioproducer),
            patch("pipeline.common.dlq.producer.build_kafka_security_config", return_value={}),
            patch("pipeline.common.dlq.producer.record_dlq_message"),
        ):
            await dlq_producer.send(message, Exception("err"), ErrorCategory.PERMANENT)

        assert dlq_producer._producer is mock_aioproducer
        mock_aioproducer.start.assert_called_once()
        mock_aioproducer.send_and_wait.assert_called_once()
