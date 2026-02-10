import os
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from config.pipeline_config import (
    ClaimXEventhouseSourceConfig,
    EventhouseSourceConfig,
    EventHubConfig,
    EventSourceType,
    PipelineConfig,
    _get_config_value,
    _parse_bool_env,
    get_event_source_type,
)


def _write_config(tmp_path, data):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump(data))
    return config_file


# =========================================================================
# _get_config_value
# =========================================================================


class TestGetConfigValue:
    def test_prefers_env_var(self):
        with patch.dict(os.environ, {"MY_VAR": "from_env"}):
            assert _get_config_value("MY_VAR", "yaml_val") == "from_env"

    def test_falls_back_to_yaml_value(self):
        with patch.dict(os.environ, {}, clear=True):
            assert _get_config_value("MY_VAR", "yaml_val") == "yaml_val"

    def test_falls_back_to_default(self):
        with patch.dict(os.environ, {}, clear=True):
            assert _get_config_value("MY_VAR", "", "default") == "default"

    def test_expands_env_vars_in_yaml_value(self):
        with patch.dict(os.environ, {"INNER": "expanded"}, clear=True):
            result = _get_config_value("MISSING", "$INNER")
            assert result == "expanded"

    def test_warns_on_unexpanded_variable(self):
        with patch.dict(os.environ, {}, clear=True):
            # ${UNDEFINED} stays literal, triggering the warning
            result = _get_config_value("MISSING", "${UNDEFINED}")
            assert "${UNDEFINED}" in result


# =========================================================================
# _parse_bool_env
# =========================================================================


class TestParseBoolEnv:
    def test_true_from_env(self):
        with patch.dict(os.environ, {"FLAG": "true"}):
            assert _parse_bool_env("FLAG", False) is True

    def test_1_from_env(self):
        with patch.dict(os.environ, {"FLAG": "1"}):
            assert _parse_bool_env("FLAG", False) is True

    def test_yes_from_env(self):
        with patch.dict(os.environ, {"FLAG": "yes"}):
            assert _parse_bool_env("FLAG", False) is True

    def test_false_from_env(self):
        with patch.dict(os.environ, {"FLAG": "false"}):
            assert _parse_bool_env("FLAG", True) is False

    def test_falls_back_to_yaml_bool(self):
        with patch.dict(os.environ, {}, clear=True):
            assert _parse_bool_env("MISSING", True) is True
            assert _parse_bool_env("MISSING", False) is False

    def test_yaml_string_true(self):
        with patch.dict(os.environ, {}, clear=True):
            assert _parse_bool_env("MISSING", "true") is True

    def test_yaml_string_false(self):
        with patch.dict(os.environ, {}, clear=True):
            assert _parse_bool_env("MISSING", "false") is False


# =========================================================================
# EventSourceType
# =========================================================================


class TestEventSourceType:
    def test_eventhub_value(self):
        assert EventSourceType.EVENTHUB.value == "eventhub"

    def test_eventhouse_value(self):
        assert EventSourceType.EVENTHOUSE.value == "eventhouse"

    def test_from_string(self):
        assert EventSourceType("eventhub") == EventSourceType.EVENTHUB
        assert EventSourceType("eventhouse") == EventSourceType.EVENTHOUSE

    def test_invalid_value_raises(self):
        with pytest.raises(ValueError):
            EventSourceType("invalid")


# =========================================================================
# EventHubConfig.from_env
# =========================================================================


class TestEventHubConfigFromEnv:
    def test_raises_without_connection_string(self):
        with (
            patch.dict(os.environ, {}, clear=True),
            pytest.raises(ValueError, match="connection string is required"),
        ):
            EventHubConfig.from_env()

    def test_loads_from_namespace_connection_string(self):
        env = {"EVENTHUB_NAMESPACE_CONNECTION_STRING": "Endpoint=sb://ns.servicebus.windows.net/"}
        with patch.dict(os.environ, env, clear=True):
            config = EventHubConfig.from_env()
            assert config.namespace_connection_string == "Endpoint=sb://ns.servicebus.windows.net/"

    def test_loads_from_legacy_connection_string(self):
        env = {"EVENTHUB_CONNECTION_STRING": "Endpoint=sb://legacy.servicebus.windows.net/"}
        with patch.dict(os.environ, env, clear=True):
            config = EventHubConfig.from_env()
            assert (
                config.namespace_connection_string == "Endpoint=sb://legacy.servicebus.windows.net/"
            )

    def test_prefers_namespace_over_legacy(self):
        env = {
            "EVENTHUB_NAMESPACE_CONNECTION_STRING": "namespace_conn",
            "EVENTHUB_CONNECTION_STRING": "legacy_conn",
        }
        with patch.dict(os.environ, env, clear=True):
            config = EventHubConfig.from_env()
            assert config.namespace_connection_string == "namespace_conn"

    def test_loads_optional_settings(self):
        env = {
            "EVENTHUB_NAMESPACE_CONNECTION_STRING": "conn",
            "EVENTHUB_BOOTSTRAP_SERVERS": "ns.servicebus.windows.net:9093",
            "EVENTHUB_EVENTS_TOPIC": "custom_topic",
            "EVENTHUB_CONSUMER_GROUP": "custom-group",
            "EVENTHUB_AUTO_OFFSET_RESET": "latest",
        }
        with patch.dict(os.environ, env, clear=True):
            config = EventHubConfig.from_env()
            assert config.bootstrap_servers == "ns.servicebus.windows.net:9093"
            assert config.events_topic == "custom_topic"
            assert config.consumer_group == "custom-group"
            assert config.auto_offset_reset == "latest"

    def test_defaults_for_optional_settings(self):
        env = {"EVENTHUB_NAMESPACE_CONNECTION_STRING": "conn"}
        with patch.dict(os.environ, env, clear=True):
            config = EventHubConfig.from_env()
            assert config.events_topic == "verisk_events"
            assert config.consumer_group == "xact-event-ingester"
            assert config.auto_offset_reset == "earliest"


# =========================================================================
# EventhouseSourceConfig.load_config
# =========================================================================


class TestEventhouseSourceConfigLoadConfig:
    def test_raises_without_cluster_url(self, tmp_path):
        data = {"verisk_eventhouse": {"database": "mydb"}}
        config_file = _write_config(tmp_path, data)
        with (
            patch.dict(os.environ, {}, clear=True),
            pytest.raises(ValueError, match="cluster_url is required"),
        ):
            EventhouseSourceConfig.load_config(config_file)

    def test_raises_without_database(self, tmp_path):
        data = {"verisk_eventhouse": {"cluster_url": "https://cluster.kusto.windows.net"}}
        config_file = _write_config(tmp_path, data)
        with (
            patch.dict(os.environ, {}, clear=True),
            pytest.raises(ValueError, match="database is required"),
        ):
            EventhouseSourceConfig.load_config(config_file)

    def test_loads_from_yaml(self, tmp_path):
        data = {
            "verisk_eventhouse": {
                "cluster_url": "https://cluster.kusto.windows.net",
                "database": "VeriskDB",
                "query_timeout_seconds": 60,
                "poller": {
                    "source_table": "MyEvents",
                    "poll_interval_seconds": 15,
                    "batch_size": 500,
                },
                "dedup": {
                    "verisk_events_window_hours": 48,
                    "eventhouse_query_window_hours": 2,
                    "overlap_minutes": 10,
                },
            }
        }
        config_file = _write_config(tmp_path, data)
        with patch.dict(os.environ, {}, clear=True):
            config = EventhouseSourceConfig.load_config(config_file)
        assert config.cluster_url == "https://cluster.kusto.windows.net"
        assert config.database == "VeriskDB"
        assert config.source_table == "MyEvents"
        assert config.poll_interval_seconds == 15
        assert config.batch_size == 500
        assert config.query_timeout_seconds == 60
        assert config.verisk_events_window_hours == 48
        assert config.eventhouse_query_window_hours == 2
        assert config.overlap_minutes == 10

    def test_env_vars_override_yaml(self, tmp_path):
        data = {
            "verisk_eventhouse": {
                "cluster_url": "https://yaml.kusto.windows.net",
                "database": "YamlDB",
            }
        }
        config_file = _write_config(tmp_path, data)
        env = {
            "EVENTHOUSE_CLUSTER_URL": "https://env.kusto.windows.net",
            "VERISK_EVENTHOUSE_DATABASE": "EnvDB",
            "POLL_INTERVAL_SECONDS": "10",
            "POLL_BATCH_SIZE": "250",
        }
        with patch.dict(os.environ, env, clear=True):
            config = EventhouseSourceConfig.load_config(config_file)
        assert config.cluster_url == "https://env.kusto.windows.net"
        assert config.database == "EnvDB"
        assert config.poll_interval_seconds == 10
        assert config.batch_size == 250

    def test_defaults(self, tmp_path):
        data = {
            "verisk_eventhouse": {
                "cluster_url": "https://cluster.kusto.windows.net",
                "database": "DB",
            }
        }
        config_file = _write_config(tmp_path, data)
        with patch.dict(os.environ, {}, clear=True):
            config = EventhouseSourceConfig.load_config(config_file)
        assert config.source_table == "Events"
        assert config.poll_interval_seconds == 30
        assert config.batch_size == 1000
        assert config.query_timeout_seconds == 120
        assert config.verisk_events_window_hours == 24
        assert config.eventhouse_query_window_hours == 1
        assert config.overlap_minutes == 5
        assert config.backfill_start_stamp is None
        assert config.backfill_stop_stamp is None
        assert config.bulk_backfill is False

    def test_backfill_config(self, tmp_path):
        data = {
            "verisk_eventhouse": {
                "cluster_url": "https://cluster.kusto.windows.net",
                "database": "DB",
                "poller": {
                    "bulk_backfill": True,
                    "backfill_start_stamp": "2025-01-01T00:00:00Z",
                    "backfill_stop_stamp": "2025-01-02T00:00:00Z",
                },
            }
        }
        config_file = _write_config(tmp_path, data)
        with patch.dict(os.environ, {}, clear=True):
            config = EventhouseSourceConfig.load_config(config_file)
        assert config.bulk_backfill is True
        assert config.backfill_start_stamp == "2025-01-01T00:00:00Z"
        assert config.backfill_stop_stamp == "2025-01-02T00:00:00Z"

    def test_raises_for_missing_config_file(self):
        with pytest.raises(FileNotFoundError):
            EventhouseSourceConfig.load_config(Path("/nonexistent/config.yaml"))


# =========================================================================
# ClaimXEventhouseSourceConfig.load_config
# =========================================================================


class TestClaimXEventhouseSourceConfigLoadConfig:
    def test_raises_without_cluster_url(self, tmp_path):
        data = {"claimx_eventhouse": {"database": "mydb"}}
        config_file = _write_config(tmp_path, data)
        with (
            patch.dict(os.environ, {}, clear=True),
            pytest.raises(ValueError, match="cluster_url is required"),
        ):
            ClaimXEventhouseSourceConfig.load_config(config_file)

    def test_raises_without_database(self, tmp_path):
        data = {"claimx_eventhouse": {"cluster_url": "https://cluster.kusto.windows.net"}}
        config_file = _write_config(tmp_path, data)
        with (
            patch.dict(os.environ, {}, clear=True),
            pytest.raises(ValueError, match="database is required"),
        ):
            ClaimXEventhouseSourceConfig.load_config(config_file)

    def test_loads_from_yaml(self, tmp_path):
        data = {
            "claimx_eventhouse": {
                "cluster_url": "https://cluster.kusto.windows.net",
                "database": "ClaimXDB",
                "poller": {
                    "source_table": "CustomTable",
                    "poll_interval_seconds": 20,
                    "batch_size": 750,
                    "events_topic": "custom_claimx_events",
                },
                "dedup": {
                    "claimx_events_window_hours": 12,
                    "eventhouse_query_window_hours": 3,
                    "overlap_minutes": 15,
                },
            }
        }
        config_file = _write_config(tmp_path, data)
        with patch.dict(os.environ, {}, clear=True):
            config = ClaimXEventhouseSourceConfig.load_config(config_file)
        assert config.cluster_url == "https://cluster.kusto.windows.net"
        assert config.database == "ClaimXDB"
        assert config.source_table == "CustomTable"
        assert config.poll_interval_seconds == 20
        assert config.batch_size == 750
        assert config.events_topic == "custom_claimx_events"
        assert config.claimx_events_window_hours == 12
        assert config.eventhouse_query_window_hours == 3
        assert config.overlap_minutes == 15

    def test_defaults(self, tmp_path):
        data = {
            "claimx_eventhouse": {
                "cluster_url": "https://cluster.kusto.windows.net",
                "database": "DB",
            }
        }
        config_file = _write_config(tmp_path, data)
        with patch.dict(os.environ, {}, clear=True):
            config = ClaimXEventhouseSourceConfig.load_config(config_file)
        assert config.source_table == "ClaimXEvents"
        assert config.poll_interval_seconds == 30
        assert config.batch_size == 1000
        assert config.query_timeout_seconds == 120
        assert config.events_topic == "claimx_events"
        assert config.claimx_events_window_hours == 24
        assert config.bulk_backfill is False

    def test_env_vars_override_yaml(self, tmp_path):
        data = {
            "claimx_eventhouse": {
                "cluster_url": "https://yaml.kusto.windows.net",
                "database": "YamlDB",
            }
        }
        config_file = _write_config(tmp_path, data)
        env = {
            "EVENTHOUSE_CLUSTER_URL": "https://env.kusto.windows.net",
            "CLAIMX_EVENTHOUSE_DATABASE": "EnvDB",
            "CLAIMX_POLL_INTERVAL_SECONDS": "5",
            "CLAIMX_EVENTS_TOPIC": "env_topic",
        }
        with patch.dict(os.environ, env, clear=True):
            config = ClaimXEventhouseSourceConfig.load_config(config_file)
        assert config.cluster_url == "https://env.kusto.windows.net"
        assert config.database == "EnvDB"
        assert config.poll_interval_seconds == 5
        assert config.events_topic == "env_topic"


# =========================================================================
# PipelineConfig
# =========================================================================


class TestPipelineConfig:
    def test_is_eventhub_source(self):
        config = PipelineConfig(event_source=EventSourceType.EVENTHUB)
        assert config.is_eventhub_source is True
        assert config.is_eventhouse_source is False

    def test_is_eventhouse_source(self):
        config = PipelineConfig(event_source=EventSourceType.EVENTHOUSE)
        assert config.is_eventhouse_source is True
        assert config.is_eventhub_source is False

    def test_load_config_eventhub(self, tmp_path):
        data = {"event_source": "eventhub"}
        config_file = _write_config(tmp_path, data)
        env = {"EVENTHUB_NAMESPACE_CONNECTION_STRING": "conn_str"}
        with patch.dict(os.environ, env, clear=True):
            config = PipelineConfig.load_config(config_file)
        assert config.event_source == EventSourceType.EVENTHUB
        assert config.eventhub is not None
        assert config.verisk_eventhouse is None

    def test_load_config_eventhouse(self, tmp_path):
        data = {
            "event_source": "eventhouse",
            "verisk_eventhouse": {
                "cluster_url": "https://cluster.kusto.windows.net",
                "database": "DB",
            },
        }
        config_file = _write_config(tmp_path, data)
        with patch.dict(os.environ, {}, clear=True):
            config = PipelineConfig.load_config(config_file)
        assert config.event_source == EventSourceType.EVENTHOUSE
        assert config.verisk_eventhouse is not None
        assert config.eventhub is None

    def test_env_var_overrides_event_source(self, tmp_path):
        data = {
            "event_source": "eventhub",
            "verisk_eventhouse": {
                "cluster_url": "https://cluster.kusto.windows.net",
                "database": "DB",
            },
        }
        config_file = _write_config(tmp_path, data)
        env = {"EVENT_SOURCE": "eventhouse"}
        with patch.dict(os.environ, env, clear=True):
            config = PipelineConfig.load_config(config_file)
        assert config.event_source == EventSourceType.EVENTHOUSE

    def test_invalid_event_source_raises(self, tmp_path):
        data = {"event_source": "invalid"}
        config_file = _write_config(tmp_path, data)
        with (
            patch.dict(os.environ, {}, clear=True),
            pytest.raises(ValueError, match="Invalid event_source"),
        ):
            PipelineConfig.load_config(config_file)

    def test_loads_delta_config(self, tmp_path):
        data = {
            "event_source": "eventhouse",
            "verisk_eventhouse": {
                "cluster_url": "https://cluster.kusto.windows.net",
                "database": "DB",
            },
            "delta": {
                "verisk": {
                    "events_table_path": "/delta/events",
                    "inventory_table_path": "/delta/inventory",
                }
            },
        }
        config_file = _write_config(tmp_path, data)
        with patch.dict(os.environ, {"PIPELINE_DOMAIN": "verisk"}, clear=True):
            config = PipelineConfig.load_config(config_file)
        assert config.events_table_path == "/delta/events"
        assert config.inventory_table_path == "/delta/inventory"

    def test_delta_env_vars_override_yaml(self, tmp_path):
        data = {
            "event_source": "eventhouse",
            "verisk_eventhouse": {
                "cluster_url": "https://cluster.kusto.windows.net",
                "database": "DB",
            },
        }
        config_file = _write_config(tmp_path, data)
        env = {
            "VERISK_EVENTS_TABLE_PATH": "/env/events",
            "PIPELINE_DOMAIN": "verisk",
        }
        with patch.dict(os.environ, env, clear=True):
            config = PipelineConfig.load_config(config_file)
        assert config.events_table_path == "/env/events"

    def test_enable_delta_writes_from_env(self, tmp_path):
        data = {
            "event_source": "eventhouse",
            "verisk_eventhouse": {
                "cluster_url": "https://cluster.kusto.windows.net",
                "database": "DB",
            },
        }
        config_file = _write_config(tmp_path, data)
        env = {"ENABLE_DELTA_WRITES": "false"}
        with patch.dict(os.environ, env, clear=True):
            config = PipelineConfig.load_config(config_file)
        assert config.enable_delta_writes is False

    def test_enable_delta_writes_defaults_true(self, tmp_path):
        data = {
            "event_source": "eventhouse",
            "verisk_eventhouse": {
                "cluster_url": "https://cluster.kusto.windows.net",
                "database": "DB",
            },
        }
        config_file = _write_config(tmp_path, data)
        with patch.dict(os.environ, {}, clear=True):
            config = PipelineConfig.load_config(config_file)
        assert config.enable_delta_writes is True

    def test_loads_claimx_eventhouse_when_configured(self, tmp_path):
        data = {
            "event_source": "eventhouse",
            "verisk_eventhouse": {
                "cluster_url": "https://cluster.kusto.windows.net",
                "database": "VeriskDB",
            },
            "claimx_eventhouse": {
                "cluster_url": "https://cluster.kusto.windows.net",
                "database": "ClaimXDB",
            },
        }
        config_file = _write_config(tmp_path, data)
        with patch.dict(os.environ, {}, clear=True):
            config = PipelineConfig.load_config(config_file)
        assert config.claimx_eventhouse is not None
        assert config.claimx_eventhouse.database == "ClaimXDB"

    def test_claimx_eventhouse_none_when_not_configured(self, tmp_path):
        data = {
            "event_source": "eventhouse",
            "verisk_eventhouse": {
                "cluster_url": "https://cluster.kusto.windows.net",
                "database": "DB",
            },
        }
        config_file = _write_config(tmp_path, data)
        with patch.dict(os.environ, {}, clear=True):
            config = PipelineConfig.load_config(config_file)
        assert config.claimx_eventhouse is None

    def test_claimx_delta_table_paths(self, tmp_path):
        data = {
            "event_source": "eventhouse",
            "verisk_eventhouse": {
                "cluster_url": "https://cluster.kusto.windows.net",
                "database": "DB",
            },
            "delta": {
                "claimx": {
                    "events_table_path": "/claimx/events",
                    "projects_table_path": "/claimx/projects",
                    "contacts_table_path": "/claimx/contacts",
                }
            },
        }
        config_file = _write_config(tmp_path, data)
        with patch.dict(os.environ, {}, clear=True):
            config = PipelineConfig.load_config(config_file)
        assert config.claimx_events_table_path == "/claimx/events"
        assert config.claimx_projects_table_path == "/claimx/projects"
        assert config.claimx_contacts_table_path == "/claimx/contacts"

    def test_missing_config_file_raises(self):
        with pytest.raises(FileNotFoundError):
            PipelineConfig.load_config(Path("/nonexistent/config.yaml"))


# =========================================================================
# get_event_source_type
# =========================================================================


class TestGetEventSourceType:
    def test_returns_eventhub_by_default(self, tmp_path):
        data = {}
        config_file = _write_config(tmp_path, data)
        with patch.dict(os.environ, {}, clear=True):
            result = get_event_source_type(config_file)
        assert result == EventSourceType.EVENTHUB

    def test_returns_eventhouse_from_yaml(self, tmp_path):
        data = {"event_source": "eventhouse"}
        config_file = _write_config(tmp_path, data)
        with patch.dict(os.environ, {}, clear=True):
            result = get_event_source_type(config_file)
        assert result == EventSourceType.EVENTHOUSE

    def test_env_var_overrides_yaml(self, tmp_path):
        data = {"event_source": "eventhub"}
        config_file = _write_config(tmp_path, data)
        with patch.dict(os.environ, {"EVENT_SOURCE": "eventhouse"}, clear=True):
            result = get_event_source_type(config_file)
        assert result == EventSourceType.EVENTHOUSE
