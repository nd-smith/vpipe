"""Tests for MessageProducer."""

import asyncio
import json
from unittest.mock import AsyncMock, Mock, patch

import pytest
from pydantic import BaseModel

from pipeline.common.types import ProduceResult


def _make_config(**overrides):
    config = Mock()
    config.bootstrap_servers = overrides.get("bootstrap_servers", "localhost:9092")
    config.request_timeout_ms = 120000
    config.metadata_max_age_ms = 300000
    config.connections_max_idle_ms = 540000
    config.security_protocol = overrides.get("security_protocol", "PLAINTEXT")
    config.sasl_mechanism = overrides.get("sasl_mechanism", "PLAIN")
    config.get_worker_config.return_value = overrides.get("producer_config", {})
    return config


class SampleModel(BaseModel):
    name: str
    value: int


class TestMessageProducerInit:
    def test_stores_config(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")

        assert producer.config is config
        assert producer.domain == "verisk"
        assert producer.worker_name == "test"
        assert producer._started is False
        assert producer._producer is None


class TestMessageProducerStart:
    async def test_start_creates_and_starts_kafka_producer(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")

        mock_kafka = AsyncMock()
        with (
            patch("pipeline.common.producer.AIOKafkaProducer", return_value=mock_kafka),
            patch("pipeline.common.producer.build_kafka_security_config", return_value={}),
        ):
            await producer.start()

        mock_kafka.start.assert_awaited_once()
        assert producer._started is True
        assert producer.is_started is True

    async def test_start_ignores_duplicate_call(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")
        producer._started = True

        # Should return without creating a new producer
        await producer.start()

    async def test_idempotence_overrides_acks_to_all(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config(producer_config={"acks": "1", "enable_idempotence": True})
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")

        mock_kafka = AsyncMock()
        with (
            patch("pipeline.common.producer.AIOKafkaProducer", return_value=mock_kafka) as mock_cls,
            patch("pipeline.common.producer.build_kafka_security_config", return_value={}),
        ):
            await producer.start()

        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["acks"] == "all"

    async def test_acks_string_digit_converted_to_int(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config(producer_config={"acks": "0", "enable_idempotence": False})
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")

        mock_kafka = AsyncMock()
        with (
            patch("pipeline.common.producer.AIOKafkaProducer", return_value=mock_kafka) as mock_cls,
            patch("pipeline.common.producer.build_kafka_security_config", return_value={}),
        ):
            await producer.start()

        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["acks"] == 0
        assert isinstance(call_kwargs["acks"], int)

    async def test_optional_producer_config_applied(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config(
            producer_config={
                "batch_size": 32768,
                "linger_ms": 100,
                "compression_type": "gzip",
                "max_request_size": 5_000_000,
            }
        )
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")

        mock_kafka = AsyncMock()
        with (
            patch("pipeline.common.producer.AIOKafkaProducer", return_value=mock_kafka) as mock_cls,
            patch("pipeline.common.producer.build_kafka_security_config", return_value={}),
        ):
            await producer.start()

        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["max_batch_size"] == 32768
        assert call_kwargs["linger_ms"] == 100
        assert call_kwargs["compression_type"] == "gzip"
        assert call_kwargs["max_request_size"] == 5_000_000

    async def test_compression_type_none_string_maps_to_none(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config(producer_config={"compression_type": "none"})
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")

        mock_kafka = AsyncMock()
        with (
            patch("pipeline.common.producer.AIOKafkaProducer", return_value=mock_kafka) as mock_cls,
            patch("pipeline.common.producer.build_kafka_security_config", return_value={}),
        ):
            await producer.start()

        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["compression_type"] is None

    async def test_default_max_request_size_10mb(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config(producer_config={})
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")

        mock_kafka = AsyncMock()
        with (
            patch("pipeline.common.producer.AIOKafkaProducer", return_value=mock_kafka) as mock_cls,
            patch("pipeline.common.producer.build_kafka_security_config", return_value={}),
        ):
            await producer.start()

        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["max_request_size"] == 10 * 1024 * 1024


class TestMessageProducerStop:
    async def test_stop_flushes_and_stops(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")
        mock_kafka = AsyncMock()
        producer._producer = mock_kafka
        producer._started = True

        await producer.stop()

        mock_kafka.flush.assert_awaited_once()
        mock_kafka.stop.assert_awaited_once()
        assert producer._producer is None
        assert producer._started is False

    async def test_stop_noop_when_no_producer(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")
        producer._producer = None

        await producer.stop()  # Should not raise

    async def test_stop_skips_flush_when_not_started(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")
        mock_kafka = AsyncMock()
        producer._producer = mock_kafka
        producer._started = False

        await producer.stop()

        mock_kafka.flush.assert_not_awaited()
        mock_kafka.stop.assert_awaited_once()

    async def test_stop_handles_exception_gracefully(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")
        mock_kafka = AsyncMock()
        mock_kafka.flush.side_effect = RuntimeError("flush failed")
        producer._producer = mock_kafka
        producer._started = True

        # Should not raise
        await producer.stop()

        assert producer._producer is None
        assert producer._started is False


class TestMessageProducerSend:
    async def test_send_raises_when_not_started(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")

        with pytest.raises(RuntimeError, match="not started"):
            await producer.send("topic", "key", b"value")

    async def test_send_bytes_value_passthrough(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")

        mock_metadata = Mock(topic="t", partition=0, offset=1)
        mock_kafka = AsyncMock()
        mock_kafka.send_and_wait.return_value = mock_metadata
        producer._producer = mock_kafka
        producer._started = True

        result = await producer.send("t", "k", b"raw-bytes")

        call_kwargs = mock_kafka.send_and_wait.call_args[1]
        assert call_kwargs["value"] == b"raw-bytes"
        assert isinstance(result, ProduceResult)

    async def test_send_pydantic_model_serialized(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")

        mock_metadata = Mock(topic="t", partition=0, offset=1)
        mock_kafka = AsyncMock()
        mock_kafka.send_and_wait.return_value = mock_metadata
        producer._producer = mock_kafka
        producer._started = True

        model = SampleModel(name="test", value=42)
        await producer.send("t", "k", model)

        call_kwargs = mock_kafka.send_and_wait.call_args[1]
        value_bytes = call_kwargs["value"]
        parsed = json.loads(value_bytes)
        assert parsed["name"] == "test"
        assert parsed["value"] == 42

    async def test_send_dict_value_json_encoded(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")

        mock_metadata = Mock(topic="t", partition=0, offset=1)
        mock_kafka = AsyncMock()
        mock_kafka.send_and_wait.return_value = mock_metadata
        producer._producer = mock_kafka
        producer._started = True

        await producer.send("t", "k", {"foo": "bar"})

        call_kwargs = mock_kafka.send_and_wait.call_args[1]
        value_bytes = call_kwargs["value"]
        parsed = json.loads(value_bytes)
        assert parsed["foo"] == "bar"

    async def test_send_encodes_string_key(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")

        mock_metadata = Mock(topic="t", partition=0, offset=1)
        mock_kafka = AsyncMock()
        mock_kafka.send_and_wait.return_value = mock_metadata
        producer._producer = mock_kafka
        producer._started = True

        await producer.send("t", "my-key", b"v")

        call_kwargs = mock_kafka.send_and_wait.call_args[1]
        assert call_kwargs["key"] == b"my-key"

    async def test_send_passes_bytes_key_directly(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")

        mock_metadata = Mock(topic="t", partition=0, offset=1)
        mock_kafka = AsyncMock()
        mock_kafka.send_and_wait.return_value = mock_metadata
        producer._producer = mock_kafka
        producer._started = True

        await producer.send("t", b"bytes-key", b"v")

        call_kwargs = mock_kafka.send_and_wait.call_args[1]
        assert call_kwargs["key"] == b"bytes-key"

    async def test_send_none_key(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")

        mock_metadata = Mock(topic="t", partition=0, offset=1)
        mock_kafka = AsyncMock()
        mock_kafka.send_and_wait.return_value = mock_metadata
        producer._producer = mock_kafka
        producer._started = True

        await producer.send("t", None, b"v")

        call_kwargs = mock_kafka.send_and_wait.call_args[1]
        assert call_kwargs["key"] is None

    async def test_send_encodes_headers(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")

        mock_metadata = Mock(topic="t", partition=0, offset=1)
        mock_kafka = AsyncMock()
        mock_kafka.send_and_wait.return_value = mock_metadata
        producer._producer = mock_kafka
        producer._started = True

        await producer.send("t", "k", b"v", headers={"trace-id": "abc"})

        call_kwargs = mock_kafka.send_and_wait.call_args[1]
        assert call_kwargs["headers"] == [("trace-id", b"abc")]

    async def test_send_no_headers_passes_none(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")

        mock_metadata = Mock(topic="t", partition=0, offset=1)
        mock_kafka = AsyncMock()
        mock_kafka.send_and_wait.return_value = mock_metadata
        producer._producer = mock_kafka
        producer._started = True

        await producer.send("t", "k", b"v")

        call_kwargs = mock_kafka.send_and_wait.call_args[1]
        assert call_kwargs["headers"] is None

    async def test_send_returns_produce_result(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")

        mock_metadata = Mock(topic="my-topic", partition=3, offset=99)
        mock_kafka = AsyncMock()
        mock_kafka.send_and_wait.return_value = mock_metadata
        producer._producer = mock_kafka
        producer._started = True

        result = await producer.send("my-topic", "k", b"v")

        assert result.topic == "my-topic"
        assert result.partition == 3
        assert result.offset == 99

    async def test_send_reraises_on_failure(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")

        mock_kafka = AsyncMock()
        mock_kafka.send_and_wait.side_effect = RuntimeError("broker down")
        producer._producer = mock_kafka
        producer._started = True

        with pytest.raises(RuntimeError, match="broker down"):
            await producer.send("t", "k", b"v")


class TestMessageProducerSendBatch:
    async def test_send_batch_raises_when_not_started(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")

        with pytest.raises(RuntimeError, match="not started"):
            await producer.send_batch("topic", [("k", SampleModel(name="a", value=1))])

    async def test_send_batch_returns_empty_for_empty_list(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")
        producer._started = True
        producer._producer = AsyncMock()

        results = await producer.send_batch("topic", [])
        assert results == []

    async def test_send_batch_returns_results(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")

        future1 = asyncio.Future()
        future1.set_result(Mock(topic="t", partition=0, offset=10))
        future2 = asyncio.Future()
        future2.set_result(Mock(topic="t", partition=0, offset=11))

        mock_kafka = AsyncMock()
        mock_kafka.send.side_effect = [future1, future2]
        producer._producer = mock_kafka
        producer._started = True

        messages = [
            ("k1", SampleModel(name="a", value=1)),
            ("k2", SampleModel(name="b", value=2)),
        ]
        results = await producer.send_batch("t", messages)

        assert len(results) == 2
        assert results[0].offset == 10
        assert results[1].offset == 11

    async def test_send_batch_reraises_on_failure(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")

        future1 = asyncio.Future()
        future1.set_exception(RuntimeError("batch fail"))

        mock_kafka = AsyncMock()
        mock_kafka.send.return_value = future1
        producer._producer = mock_kafka
        producer._started = True

        messages = [("k1", SampleModel(name="a", value=1))]
        with pytest.raises(RuntimeError, match="batch fail"):
            await producer.send_batch("t", messages)


class TestMessageProducerFlush:
    async def test_flush_raises_when_not_started(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")

        with pytest.raises(RuntimeError, match="not started"):
            await producer.flush()

    async def test_flush_calls_producer_flush(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")
        mock_kafka = AsyncMock()
        producer._producer = mock_kafka
        producer._started = True

        await producer.flush()
        mock_kafka.flush.assert_awaited_once()


class TestMessageProducerIsStarted:
    def test_is_started_true_when_active(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")
        producer._started = True
        producer._producer = Mock()
        assert producer.is_started is True

    def test_is_started_false_when_no_producer(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")
        producer._started = True
        producer._producer = None
        assert producer.is_started is False

    def test_is_started_false_when_not_started(self):
        from pipeline.common.producer import MessageProducer

        config = _make_config()
        producer = MessageProducer(config=config, domain="verisk", worker_name="test")
        producer._started = False
        producer._producer = Mock()
        assert producer.is_started is False
