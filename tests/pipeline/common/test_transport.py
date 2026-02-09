"""Tests for transport layer abstraction."""

import os
from unittest.mock import AsyncMock, Mock, patch

import pytest

from pipeline.common.transport import (
    TransportType,
    _resolve_eventhub_consumer_group,
    _resolve_eventhub_name,
    _strip_entity_path,
    get_transport_type,
)


class TestTransportType:

    def test_eventhub_value(self):
        assert TransportType.EVENTHUB.value == "eventhub"

    def test_kafka_value(self):
        assert TransportType.KAFKA.value == "kafka"

    def test_is_string_enum(self):
        assert isinstance(TransportType.EVENTHUB, str)
        assert TransportType.KAFKA == "kafka"


class TestGetTransportType:

    def test_defaults_to_eventhub(self, monkeypatch):
        monkeypatch.delenv("PIPELINE_TRANSPORT", raising=False)
        assert get_transport_type() == TransportType.EVENTHUB

    def test_returns_kafka_when_set(self, monkeypatch):
        monkeypatch.setenv("PIPELINE_TRANSPORT", "kafka")
        assert get_transport_type() == TransportType.KAFKA

    def test_returns_eventhub_when_set(self, monkeypatch):
        monkeypatch.setenv("PIPELINE_TRANSPORT", "eventhub")
        assert get_transport_type() == TransportType.EVENTHUB

    def test_case_insensitive(self, monkeypatch):
        monkeypatch.setenv("PIPELINE_TRANSPORT", "KAFKA")
        assert get_transport_type() == TransportType.KAFKA

    def test_invalid_value_defaults_to_eventhub(self, monkeypatch):
        monkeypatch.setenv("PIPELINE_TRANSPORT", "rabbitmq")
        assert get_transport_type() == TransportType.EVENTHUB


class TestStripEntityPath:

    def test_strips_entity_path(self):
        conn = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123;EntityPath=my-hub"
        result = _strip_entity_path(conn)
        assert "EntityPath" not in result
        assert "Endpoint=sb://ns.servicebus.windows.net/" in result
        assert "SharedAccessKeyName=RootManageSharedAccessKey" in result

    def test_no_entity_path_unchanged(self):
        conn = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123"
        result = _strip_entity_path(conn)
        assert result == conn

    def test_strips_empty_parts(self):
        conn = "Endpoint=sb://ns.servicebus.windows.net/;;SharedAccessKey=abc123"
        result = _strip_entity_path(conn)
        assert ";;" not in result


class TestResolveEventHubName:

    def test_resolves_from_config(self):
        mock_config = {
            "verisk": {
                "events": {
                    "eventhub_name": "my-event-hub",
                }
            }
        }
        with patch("pipeline.common.transport._load_eventhub_config", return_value=mock_config):
            name = _resolve_eventhub_name("verisk", "events", "event_ingester")
        assert name == "my-event-hub"

    def test_resolves_from_worker_env_var(self, monkeypatch):
        monkeypatch.setenv("EVENTHUB_NAME_EVENT_INGESTER", "env-hub")
        with patch("pipeline.common.transport._load_eventhub_config", return_value={}):
            name = _resolve_eventhub_name("verisk", None, "event_ingester")
        assert name == "env-hub"

    def test_env_var_normalizes_dashes(self, monkeypatch):
        monkeypatch.setenv("EVENTHUB_NAME_DOWNLOAD_WORKER", "download-hub")
        with patch("pipeline.common.transport._load_eventhub_config", return_value={}):
            name = _resolve_eventhub_name("verisk", None, "download-worker")
        assert name == "download-hub"

    def test_raises_when_not_configured(self):
        with patch("pipeline.common.transport._load_eventhub_config", return_value={}):
            with pytest.raises(ValueError, match="Event Hub name not configured"):
                _resolve_eventhub_name("verisk", "events", "test_worker")

    def test_config_takes_priority_over_env_var(self, monkeypatch):
        monkeypatch.setenv("EVENTHUB_NAME_EVENT_INGESTER", "env-hub")
        mock_config = {
            "verisk": {
                "events": {
                    "eventhub_name": "config-hub",
                }
            }
        }
        with patch("pipeline.common.transport._load_eventhub_config", return_value=mock_config):
            name = _resolve_eventhub_name("verisk", "events", "event_ingester")
        assert name == "config-hub"

    def test_skips_config_when_no_topic_key(self, monkeypatch):
        monkeypatch.setenv("EVENTHUB_NAME_WORKER", "env-hub")
        mock_config = {
            "verisk": {
                "events": {"eventhub_name": "config-hub"},
            }
        }
        with patch("pipeline.common.transport._load_eventhub_config", return_value=mock_config):
            name = _resolve_eventhub_name("verisk", None, "worker")
        assert name == "env-hub"


class TestResolveEventHubConsumerGroup:

    def test_resolves_from_config(self):
        mock_config = {
            "verisk": {
                "events": {
                    "consumer_group": "my-group",
                }
            }
        }
        message_config = Mock()
        with patch("pipeline.common.transport._load_eventhub_config", return_value=mock_config):
            group = _resolve_eventhub_consumer_group(
                "verisk", "events", "event_ingester", message_config
            )
        assert group == "my-group"

    def test_falls_back_to_message_config(self):
        message_config = Mock()
        message_config.get_consumer_group.return_value = "fallback-group"
        with patch("pipeline.common.transport._load_eventhub_config", return_value={}):
            group = _resolve_eventhub_consumer_group(
                "verisk", None, "event_ingester", message_config
            )
        assert group == "fallback-group"

    def test_falls_back_to_default_consumer_group(self):
        mock_config = {"default_consumer_group": "default-group"}
        message_config = Mock()
        message_config.get_consumer_group.side_effect = ValueError("no group")
        with patch("pipeline.common.transport._load_eventhub_config", return_value=mock_config):
            group = _resolve_eventhub_consumer_group(
                "verisk", None, "worker", message_config
            )
        assert group == "default-group"

    def test_raises_when_no_consumer_group(self):
        message_config = Mock()
        message_config.get_consumer_group.side_effect = ValueError("no group")
        with patch("pipeline.common.transport._load_eventhub_config", return_value={}):
            with pytest.raises(ValueError, match="consumer group not configured"):
                _resolve_eventhub_consumer_group(
                    "verisk", "events", "worker", message_config
                )


class TestCreateProducer:

    def test_creates_kafka_producer(self):
        from pipeline.common.transport import create_producer

        config = Mock()
        with patch("pipeline.common.producer.MessageProducer") as mock_cls:
            result = create_producer(
                config, "verisk", "test", transport_type=TransportType.KAFKA
            )
        mock_cls.assert_called_once_with(
            config=config, domain="verisk", worker_name="test"
        )

    def test_creates_eventhub_producer(self):
        from pipeline.common.transport import create_producer

        config = Mock()
        with patch("pipeline.common.transport._get_namespace_connection_string", return_value="conn-str"):
            with patch("pipeline.common.transport._resolve_eventhub_name", return_value="hub-name"):
                with patch("pipeline.common.eventhub.producer.EventHubProducer") as mock_cls:
                    result = create_producer(
                        config, "verisk", "test",
                        transport_type=TransportType.EVENTHUB,
                        topic_key="events",
                    )
        mock_cls.assert_called_once_with(
            connection_string="conn-str",
            domain="verisk",
            worker_name="test",
            eventhub_name="hub-name",
        )

    def test_eventhub_producer_uses_explicit_topic(self):
        from pipeline.common.transport import create_producer

        config = Mock()
        with patch("pipeline.common.transport._get_namespace_connection_string", return_value="conn-str"):
            with patch("pipeline.common.eventhub.producer.EventHubProducer") as mock_cls:
                create_producer(
                    config, "verisk", "test",
                    transport_type=TransportType.EVENTHUB,
                    topic="explicit-hub",
                )
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["eventhub_name"] == "explicit-hub"

    def test_defaults_to_env_transport_type(self, monkeypatch):
        from pipeline.common.transport import create_producer

        monkeypatch.setenv("PIPELINE_TRANSPORT", "kafka")
        config = Mock()
        with patch("pipeline.common.producer.MessageProducer") as mock_cls:
            create_producer(config, "verisk", "test")
        mock_cls.assert_called_once()


class TestCreateConsumer:

    async def test_creates_kafka_consumer(self):
        from pipeline.common.transport import create_consumer

        config = Mock()
        config.get_consumer_group.return_value = "grp"
        config.get_worker_config.return_value = {}
        handler = AsyncMock()

        with patch("pipeline.common.consumer.MessageConsumer") as mock_cls:
            with patch("pipeline.common.dlq.producer.DLQProducer"):
                result = await create_consumer(
                    config, "verisk", "test", ["t1"], handler,
                    transport_type=TransportType.KAFKA,
                )
        mock_cls.assert_called_once()

    async def test_eventhub_rejects_multiple_topics(self):
        from pipeline.common.transport import create_consumer

        config = Mock()
        handler = AsyncMock()

        with pytest.raises(ValueError, match="single topic"):
            await create_consumer(
                config, "verisk", "test", ["t1", "t2"], handler,
                transport_type=TransportType.EVENTHUB,
            )

    async def test_creates_eventhub_consumer(self):
        from pipeline.common.transport import create_consumer

        config = Mock()
        config.get_consumer_group.return_value = "grp"
        handler = AsyncMock()

        with patch("pipeline.common.transport._get_namespace_connection_string", return_value="conn"):
            with patch("pipeline.common.transport._resolve_eventhub_consumer_group", return_value="cg"):
                with patch("pipeline.common.transport.get_checkpoint_store", new_callable=AsyncMock, return_value=None):
                    with patch("pipeline.common.eventhub.consumer.EventHubConsumer") as mock_cls:
                        await create_consumer(
                            config, "verisk", "test", ["t1"], handler,
                            transport_type=TransportType.EVENTHUB,
                        )
        mock_cls.assert_called_once()
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["eventhub_name"] == "t1"  # Falls back to topic name
        assert call_kwargs["consumer_group"] == "cg"


class TestCreateBatchConsumer:

    async def test_creates_kafka_batch_consumer(self):
        from pipeline.common.transport import create_batch_consumer

        config = Mock()
        config.get_consumer_group.return_value = "grp"
        config.get_worker_config.return_value = {}
        handler = AsyncMock()

        with patch("pipeline.common.batch_consumer.MessageBatchConsumer") as mock_cls:
            await create_batch_consumer(
                config, "verisk", "test", ["t1"], handler,
                batch_size=50,
                batch_timeout_ms=2000,
                transport_type=TransportType.KAFKA,
            )
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["batch_size"] == 50
        assert call_kwargs["batch_timeout_ms"] == 2000

    async def test_eventhub_batch_consumer_rejects_multiple_topics(self):
        from pipeline.common.transport import create_batch_consumer

        config = Mock()
        handler = AsyncMock()

        with pytest.raises(ValueError, match="single topic"):
            await create_batch_consumer(
                config, "verisk", "test", ["t1", "t2"], handler,
                transport_type=TransportType.EVENTHUB,
            )

    async def test_creates_eventhub_batch_consumer(self):
        from pipeline.common.transport import create_batch_consumer

        config = Mock()
        config.get_consumer_group.return_value = "grp"
        handler = AsyncMock()

        with patch("pipeline.common.transport._get_namespace_connection_string", return_value="conn"):
            with patch("pipeline.common.transport._resolve_eventhub_consumer_group", return_value="cg"):
                with patch("pipeline.common.transport.get_checkpoint_store", new_callable=AsyncMock, return_value=None):
                    with patch("pipeline.common.eventhub.batch_consumer.EventHubBatchConsumer") as mock_cls:
                        await create_batch_consumer(
                            config, "verisk", "test", ["t1"], handler,
                            transport_type=TransportType.EVENTHUB,
                        )
        mock_cls.assert_called_once()


class TestGetNamespaceConnectionString:

    def test_from_env_var(self, monkeypatch):
        from pipeline.common.transport import _get_namespace_connection_string

        monkeypatch.setenv(
            "EVENTHUB_NAMESPACE_CONNECTION_STRING",
            "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKey=abc",
        )
        result = _get_namespace_connection_string()
        assert "Endpoint=sb://ns.servicebus.windows.net/" in result

    def test_strips_entity_path_from_env(self, monkeypatch):
        from pipeline.common.transport import _get_namespace_connection_string

        monkeypatch.setenv(
            "EVENTHUB_NAMESPACE_CONNECTION_STRING",
            "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKey=abc;EntityPath=hub",
        )
        result = _get_namespace_connection_string()
        assert "EntityPath" not in result

    def test_raises_when_env_var_empty(self, monkeypatch):
        from pipeline.common.transport import _get_namespace_connection_string

        monkeypatch.setenv("EVENTHUB_NAMESPACE_CONNECTION_STRING", "")
        with patch("pipeline.common.transport._load_eventhub_config", return_value={}):
            with pytest.raises(ValueError, match="namespace connection string is required"):
                _get_namespace_connection_string()

    def test_raises_when_env_var_whitespace_only(self, monkeypatch):
        from pipeline.common.transport import _get_namespace_connection_string

        # After stripping entity path from a whitespace-only string, result is empty
        monkeypatch.setenv("EVENTHUB_NAMESPACE_CONNECTION_STRING", "   ")
        with pytest.raises(ValueError, match="empty or contains only whitespace"):
            _get_namespace_connection_string()

    def test_from_config_file(self, monkeypatch):
        from pipeline.common.transport import _get_namespace_connection_string

        monkeypatch.delenv("EVENTHUB_NAMESPACE_CONNECTION_STRING", raising=False)
        mock_config = {
            "namespace_connection_string": "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKey=abc"
        }
        with patch("pipeline.common.transport._load_eventhub_config", return_value=mock_config):
            result = _get_namespace_connection_string()
        assert "Endpoint=sb://ns.servicebus.windows.net/" in result

    def test_raises_when_nothing_configured(self, monkeypatch):
        from pipeline.common.transport import _get_namespace_connection_string

        monkeypatch.delenv("EVENTHUB_NAMESPACE_CONNECTION_STRING", raising=False)
        with patch("pipeline.common.transport._load_eventhub_config", return_value={}):
            with pytest.raises(ValueError, match="namespace connection string is required"):
                _get_namespace_connection_string()
