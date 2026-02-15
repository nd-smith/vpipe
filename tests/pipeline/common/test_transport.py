"""Tests for transport layer abstraction."""

import datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest

from pipeline.common.transport import (
    TransportType,
    _parse_starting_position,
    _resolve_eventhub_consumer_group,
    _resolve_eventhub_name,
    _resolve_starting_position,
    _strip_entity_path,
    get_source_connection_string,
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
        conn = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=fake-key;EntityPath=my-hub"
        result = _strip_entity_path(conn)
        assert "EntityPath" not in result
        assert "Endpoint=sb://ns.servicebus.windows.net/" in result
        assert "SharedAccessKeyName=RootManageSharedAccessKey" in result

    def test_no_entity_path_unchanged(self):
        conn = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=fake-key"
        result = _strip_entity_path(conn)
        assert result == conn

    def test_strips_empty_parts(self):
        conn = "Endpoint=sb://ns.servicebus.windows.net/;;SharedAccessKey=fake-key"
        result = _strip_entity_path(conn)
        assert ";;" not in result


class TestResolveEventHubName:
    def test_resolves_from_source_config(self):
        mock_config = {
            "source": {
                "verisk": {
                    "events": {
                        "eventhub_name": "source-event-hub",
                    }
                }
            }
        }
        with patch("pipeline.common.transport._load_eventhub_config", return_value=mock_config):
            name = _resolve_eventhub_name("verisk", "events", "event_ingester")
        assert name == "source-event-hub"

    def test_source_config_takes_priority_over_domain_config(self):
        mock_config = {
            "source": {
                "verisk": {
                    "events": {"eventhub_name": "source-hub"},
                }
            },
            "verisk": {
                "events": {"eventhub_name": "domain-hub"},
            },
        }
        with patch("pipeline.common.transport._load_eventhub_config", return_value=mock_config):
            name = _resolve_eventhub_name("verisk", "events", "event_ingester")
        assert name == "source-hub"

    def test_falls_back_to_domain_config_when_not_in_source(self):
        mock_config = {
            "source": {},
            "verisk": {
                "enrichment_pending": {"eventhub_name": "domain-hub"},
            },
        }
        with patch("pipeline.common.transport._load_eventhub_config", return_value=mock_config):
            name = _resolve_eventhub_name("verisk", "enrichment_pending", "enrichment_worker")
        assert name == "domain-hub"

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
        with (
            patch("pipeline.common.transport._load_eventhub_config", return_value={}),
            pytest.raises(ValueError, match="Event Hub name not configured"),
        ):
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
    def test_resolves_from_source_config(self):
        mock_config = {
            "source": {
                "verisk": {
                    "events": {
                        "consumer_groups": {
                            "event_ingester": "source-group",
                        },
                    }
                }
            }
        }
        message_config = Mock()
        with patch("pipeline.common.transport._load_eventhub_config", return_value=mock_config):
            group = _resolve_eventhub_consumer_group(
                "verisk", "events", "event_ingester", message_config
            )
        assert group == "source-group"

    def test_source_config_takes_priority_over_domain_config(self):
        mock_config = {
            "source": {
                "verisk": {
                    "events": {
                        "consumer_groups": {"event_ingester": "source-group"},
                    }
                }
            },
            "verisk": {
                "events": {
                    "consumer_groups": {"event_ingester": "domain-group"},
                }
            },
        }
        message_config = Mock()
        with patch("pipeline.common.transport._load_eventhub_config", return_value=mock_config):
            group = _resolve_eventhub_consumer_group(
                "verisk", "events", "event_ingester", message_config
            )
        assert group == "source-group"

    def test_falls_back_to_domain_config_when_not_in_source(self):
        mock_config = {
            "source": {},
            "verisk": {
                "enrichment_pending": {
                    "consumer_groups": {"enrichment_worker": "domain-group"},
                }
            },
        }
        message_config = Mock()
        with patch("pipeline.common.transport._load_eventhub_config", return_value=mock_config):
            group = _resolve_eventhub_consumer_group(
                "verisk", "enrichment_pending", "enrichment_worker", message_config
            )
        assert group == "domain-group"

    def test_resolves_from_config(self):
        mock_config = {
            "verisk": {
                "events": {
                    "consumer_groups": {
                        "event_ingester": "my-group",
                    },
                }
            }
        }
        message_config = Mock()
        with patch("pipeline.common.transport._load_eventhub_config", return_value=mock_config):
            group = _resolve_eventhub_consumer_group(
                "verisk", "events", "event_ingester", message_config
            )
        assert group == "my-group"

    def test_falls_back_to_default_consumer_group(self):
        mock_config = {"default_consumer_group": "default-group"}
        message_config = Mock()
        with patch("pipeline.common.transport._load_eventhub_config", return_value=mock_config):
            group = _resolve_eventhub_consumer_group("verisk", None, "worker", message_config)
        assert group == "default-group"

    def test_raises_when_no_consumer_group(self):
        message_config = Mock()
        with (
            patch("pipeline.common.transport._load_eventhub_config", return_value={}),
            pytest.raises(ValueError, match="consumer group not configured"),
        ):
            _resolve_eventhub_consumer_group("verisk", "events", "worker", message_config)

    def test_raises_when_no_topic_key_and_no_default(self):
        message_config = Mock()
        with (
            patch("pipeline.common.transport._load_eventhub_config", return_value={}),
            pytest.raises(ValueError, match="consumer group not configured"),
        ):
            _resolve_eventhub_consumer_group("verisk", None, "worker", message_config)


class TestCreateProducer:
    def test_creates_kafka_producer(self):
        from pipeline.common.transport import create_producer

        config = Mock()
        with patch("pipeline.common.producer.MessageProducer") as mock_cls:
            create_producer(config, "verisk", "test", transport_type=TransportType.KAFKA)
        mock_cls.assert_called_once_with(config=config, domain="verisk", worker_name="test")

    def test_creates_eventhub_producer(self):
        from pipeline.common.transport import create_producer

        config = Mock()
        with (
            patch(
                "pipeline.common.transport._get_namespace_connection_string",
                return_value="conn-str",
            ),
            patch("pipeline.common.transport._resolve_eventhub_name", return_value="hub-name"),
            patch("pipeline.common.eventhub.producer.EventHubProducer") as mock_cls,
        ):
            create_producer(
                config,
                "verisk",
                "test",
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
        with (
            patch(
                "pipeline.common.transport._get_namespace_connection_string",
                return_value="conn-str",
            ),
            patch("pipeline.common.eventhub.producer.EventHubProducer") as mock_cls,
        ):
            create_producer(
                config,
                "verisk",
                "test",
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

        with (
            patch("pipeline.common.consumer.MessageConsumer") as mock_cls,
            patch("pipeline.common.dlq.producer.DLQProducer"),
        ):
            await create_consumer(
                config,
                "verisk",
                "test",
                ["t1"],
                handler,
                transport_type=TransportType.KAFKA,
            )
        mock_cls.assert_called_once()

    async def test_eventhub_rejects_multiple_topics(self):
        from pipeline.common.transport import create_consumer

        config = Mock()
        handler = AsyncMock()

        with pytest.raises(ValueError, match="single topic"):
            await create_consumer(
                config,
                "verisk",
                "test",
                ["t1", "t2"],
                handler,
                transport_type=TransportType.EVENTHUB,
            )

    async def test_creates_eventhub_consumer(self):
        from pipeline.common.transport import create_consumer

        config = Mock()
        config.get_consumer_group.return_value = "grp"
        handler = AsyncMock()

        with (
            patch(
                "pipeline.common.transport._get_namespace_connection_string", return_value="conn"
            ),
            patch("pipeline.common.transport._resolve_eventhub_consumer_group", return_value="cg"),
            patch(
                "pipeline.common.transport.get_checkpoint_store",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch("pipeline.common.eventhub.consumer.EventHubConsumer") as mock_cls,
        ):
            await create_consumer(
                config,
                "verisk",
                "test",
                ["t1"],
                handler,
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
                config,
                "verisk",
                "test",
                ["t1"],
                handler,
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
                config,
                "verisk",
                "test",
                ["t1", "t2"],
                handler,
                transport_type=TransportType.EVENTHUB,
            )

    async def test_creates_eventhub_batch_consumer(self):
        from pipeline.common.transport import create_batch_consumer

        config = Mock()
        config.get_consumer_group.return_value = "grp"
        handler = AsyncMock()

        with (
            patch(
                "pipeline.common.transport._get_namespace_connection_string", return_value="conn"
            ),
            patch("pipeline.common.transport._resolve_eventhub_consumer_group", return_value="cg"),
            patch(
                "pipeline.common.transport.get_checkpoint_store",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch("pipeline.common.eventhub.batch_consumer.EventHubBatchConsumer") as mock_cls,
        ):
            await create_batch_consumer(
                config,
                "verisk",
                "test",
                ["t1"],
                handler,
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
        with (
            patch("pipeline.common.transport._load_eventhub_config", return_value={}),
            pytest.raises(ValueError, match="namespace connection string is required"),
        ):
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
        with (
            patch("pipeline.common.transport._load_eventhub_config", return_value={}),
            pytest.raises(ValueError, match="namespace connection string is required"),
        ):
            _get_namespace_connection_string()


class TestGetSourceConnectionString:
    def test_from_env_var(self, monkeypatch):
        monkeypatch.setenv(
            "SOURCE_EVENTHUB_NAMESPACE_CONNECTION_STRING",
            "Endpoint=sb://source-ns.servicebus.windows.net/;SharedAccessKey=abc",
        )
        result = get_source_connection_string()
        assert "source-ns" in result

    def test_from_config_file(self, monkeypatch):
        monkeypatch.delenv("SOURCE_EVENTHUB_NAMESPACE_CONNECTION_STRING", raising=False)
        mock_config = {
            "source": {
                "namespace_connection_string": "Endpoint=sb://source-ns.servicebus.windows.net/;SharedAccessKey=abc"
            }
        }
        with patch("pipeline.common.transport._load_eventhub_config", return_value=mock_config):
            result = get_source_connection_string()
        assert "source-ns" in result

    def test_falls_back_to_regular_connection_string(self, monkeypatch):
        monkeypatch.delenv("SOURCE_EVENTHUB_NAMESPACE_CONNECTION_STRING", raising=False)
        monkeypatch.setenv(
            "EVENTHUB_NAMESPACE_CONNECTION_STRING",
            "Endpoint=sb://regular-ns.servicebus.windows.net/;SharedAccessKey=abc",
        )
        with patch("pipeline.common.transport._load_eventhub_config", return_value={}):
            result = get_source_connection_string()
        assert "regular-ns" in result

    def test_raises_when_env_var_whitespace_only(self, monkeypatch):
        monkeypatch.setenv("SOURCE_EVENTHUB_NAMESPACE_CONNECTION_STRING", "   ")
        with pytest.raises(ValueError, match="empty or contains only whitespace"):
            get_source_connection_string()

    def test_strips_entity_path(self, monkeypatch):
        monkeypatch.setenv(
            "SOURCE_EVENTHUB_NAMESPACE_CONNECTION_STRING",
            "Endpoint=sb://source-ns.servicebus.windows.net/;SharedAccessKey=abc;EntityPath=hub",
        )
        result = get_source_connection_string()
        assert "EntityPath" not in result


class TestConsumerConnectionStringOverride:
    async def test_consumer_uses_provided_connection_string(self):
        from pipeline.common.transport import create_consumer

        config = Mock()
        handler = AsyncMock()

        with (
            patch("pipeline.common.transport._resolve_eventhub_name", return_value="hub"),
            patch("pipeline.common.transport._resolve_eventhub_consumer_group", return_value="cg"),
            patch(
                "pipeline.common.transport.get_checkpoint_store",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch("pipeline.common.eventhub.consumer.EventHubConsumer") as mock_cls,
        ):
            await create_consumer(
                config,
                "verisk",
                "event_ingester",
                ["verisk_events"],
                handler,
                transport_type=TransportType.EVENTHUB,
                topic_key="events",
                connection_string="source-conn",
            )
        assert mock_cls.call_args[1]["connection_string"] == "source-conn"

    async def test_consumer_falls_back_to_default_without_connection_string(self):
        from pipeline.common.transport import create_consumer

        config = Mock()
        handler = AsyncMock()

        with (
            patch(
                "pipeline.common.transport._get_namespace_connection_string",
                return_value="default-conn",
            ),
            patch("pipeline.common.transport._resolve_eventhub_consumer_group", return_value="cg"),
            patch(
                "pipeline.common.transport.get_checkpoint_store",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch("pipeline.common.eventhub.consumer.EventHubConsumer") as mock_cls,
        ):
            await create_consumer(
                config,
                "verisk",
                "enrichment_worker",
                ["verisk-enrichment-pending"],
                handler,
                transport_type=TransportType.EVENTHUB,
            )
        assert mock_cls.call_args[1]["connection_string"] == "default-conn"

    async def test_batch_consumer_uses_provided_connection_string(self):
        from pipeline.common.transport import create_batch_consumer

        config = Mock()
        handler = AsyncMock()

        with (
            patch("pipeline.common.transport._resolve_eventhub_name", return_value="hub"),
            patch("pipeline.common.transport._resolve_eventhub_consumer_group", return_value="cg"),
            patch(
                "pipeline.common.transport.get_checkpoint_store",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch("pipeline.common.eventhub.batch_consumer.EventHubBatchConsumer") as mock_cls,
        ):
            await create_batch_consumer(
                config,
                "verisk",
                "delta_events_writer",
                ["verisk_events"],
                handler,
                transport_type=TransportType.EVENTHUB,
                topic_key="events",
                connection_string="source-conn",
            )
        assert mock_cls.call_args[1]["connection_string"] == "source-conn"


class TestParseStartingPosition:
    def test_latest(self):
        pos, inclusive = _parse_starting_position("@latest")
        assert pos == "@latest"
        assert inclusive is False

    def test_earliest(self):
        pos, inclusive = _parse_starting_position("-1")
        assert pos == "-1"
        assert inclusive is False

    def test_datetime_with_tz(self):
        pos, inclusive = _parse_starting_position("2025-06-01T00:00:00+00:00")
        assert isinstance(pos, datetime.datetime)
        assert pos.tzinfo is not None
        assert inclusive is True

    def test_naive_datetime_gets_utc(self):
        pos, inclusive = _parse_starting_position("2025-06-01T00:00:00")
        assert isinstance(pos, datetime.datetime)
        assert pos.tzinfo == datetime.UTC
        assert inclusive is True

    def test_invalid_raises_value_error(self):
        with pytest.raises(ValueError, match="Invalid starting_position"):
            _parse_starting_position("bogus")


class TestResolveStartingPosition:
    def test_source_config_priority(self):
        mock_config = {
            "source": {
                "verisk": {
                    "events": {
                        "starting_position": "-1",
                    }
                }
            },
            "default_starting_position": "@latest",
        }
        with patch("pipeline.common.transport._load_eventhub_config", return_value=mock_config):
            pos, inclusive = _resolve_starting_position("verisk", "events")
        assert pos == "-1"
        assert inclusive is False

    def test_domain_fallback(self):
        mock_config = {
            "source": {},
            "verisk": {
                "events": {
                    "starting_position": "-1",
                }
            },
        }
        with patch("pipeline.common.transport._load_eventhub_config", return_value=mock_config):
            pos, inclusive = _resolve_starting_position("verisk", "events")
        assert pos == "-1"

    def test_global_default(self):
        mock_config = {
            "default_starting_position": "-1",
        }
        with patch("pipeline.common.transport._load_eventhub_config", return_value=mock_config):
            pos, inclusive = _resolve_starting_position("verisk", "events")
        assert pos == "-1"

    def test_hardcoded_latest_fallback(self):
        with patch("pipeline.common.transport._load_eventhub_config", return_value={}):
            pos, inclusive = _resolve_starting_position("verisk", "events")
        assert pos == "@latest"
        assert inclusive is False

    def test_datetime_parsing(self):
        mock_config = {
            "default_starting_position": "2025-06-01T00:00:00Z",
        }
        with patch("pipeline.common.transport._load_eventhub_config", return_value=mock_config):
            pos, inclusive = _resolve_starting_position("verisk", None)
        assert isinstance(pos, datetime.datetime)
        assert inclusive is True

    def test_no_topic_key_skips_topic_lookups(self):
        mock_config = {
            "source": {
                "verisk": {
                    "events": {"starting_position": "-1"},
                }
            },
            "default_starting_position": "@latest",
        }
        with patch("pipeline.common.transport._load_eventhub_config", return_value=mock_config):
            pos, inclusive = _resolve_starting_position("verisk", None)
        # Should skip topic lookups and use global default
        assert pos == "@latest"


class TestCreateConsumerStartingPosition:
    async def test_passes_resolved_starting_position(self):
        from pipeline.common.transport import create_consumer

        config = Mock()
        handler = AsyncMock()

        with (
            patch(
                "pipeline.common.transport._get_namespace_connection_string", return_value="conn"
            ),
            patch("pipeline.common.transport._resolve_eventhub_consumer_group", return_value="cg"),
            patch(
                "pipeline.common.transport.get_checkpoint_store",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch(
                "pipeline.common.transport._resolve_starting_position",
                return_value=("@latest", False),
            ) as mock_resolve,
            patch("pipeline.common.eventhub.consumer.EventHubConsumer") as mock_cls,
        ):
            await create_consumer(
                config,
                "verisk",
                "test",
                ["t1"],
                handler,
                transport_type=TransportType.EVENTHUB,
            )
        mock_resolve.assert_called_once()
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["starting_position"] == "@latest"
        assert call_kwargs["starting_position_inclusive"] is False

    async def test_passes_explicit_starting_position(self):
        from pipeline.common.transport import create_consumer

        config = Mock()
        handler = AsyncMock()

        with (
            patch(
                "pipeline.common.transport._get_namespace_connection_string", return_value="conn"
            ),
            patch("pipeline.common.transport._resolve_eventhub_consumer_group", return_value="cg"),
            patch(
                "pipeline.common.transport.get_checkpoint_store",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch("pipeline.common.eventhub.consumer.EventHubConsumer") as mock_cls,
        ):
            await create_consumer(
                config,
                "verisk",
                "test",
                ["t1"],
                handler,
                transport_type=TransportType.EVENTHUB,
                starting_position="-1",
            )
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["starting_position"] == "-1"
        assert call_kwargs["starting_position_inclusive"] is False


class TestCreateBatchConsumerStartingPosition:
    async def test_passes_resolved_starting_position(self):
        from pipeline.common.transport import create_batch_consumer

        config = Mock()
        handler = AsyncMock()

        with (
            patch(
                "pipeline.common.transport._get_namespace_connection_string", return_value="conn"
            ),
            patch("pipeline.common.transport._resolve_eventhub_consumer_group", return_value="cg"),
            patch(
                "pipeline.common.transport.get_checkpoint_store",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch(
                "pipeline.common.transport._resolve_starting_position",
                return_value=("@latest", False),
            ) as mock_resolve,
            patch("pipeline.common.eventhub.batch_consumer.EventHubBatchConsumer") as mock_cls,
        ):
            await create_batch_consumer(
                config,
                "verisk",
                "test",
                ["t1"],
                handler,
                transport_type=TransportType.EVENTHUB,
            )
        mock_resolve.assert_called_once()
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["starting_position"] == "@latest"
        assert call_kwargs["starting_position_inclusive"] is False

    async def test_passes_explicit_starting_position(self):
        from pipeline.common.transport import create_batch_consumer

        config = Mock()
        handler = AsyncMock()

        with (
            patch(
                "pipeline.common.transport._get_namespace_connection_string", return_value="conn"
            ),
            patch("pipeline.common.transport._resolve_eventhub_consumer_group", return_value="cg"),
            patch(
                "pipeline.common.transport.get_checkpoint_store",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch("pipeline.common.eventhub.batch_consumer.EventHubBatchConsumer") as mock_cls,
        ):
            await create_batch_consumer(
                config,
                "verisk",
                "test",
                ["t1"],
                handler,
                transport_type=TransportType.EVENTHUB,
                starting_position="-1",
            )
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["starting_position"] == "-1"
        assert call_kwargs["starting_position_inclusive"] is False
