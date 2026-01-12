"""Unit tests for multi-file config loading from config/ directory.

Tests configuration loading from split YAML files in config/ directory structure.
Single-file config.yaml is no longer supported.
"""

import os
import tempfile
from pathlib import Path
from typing import Any, Dict

import pytest
import yaml

from kafka_pipeline.config import (
    KafkaConfig,
    load_config,
    reset_config,
)


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def temp_config_dir(tmp_path):
    """Create a temporary config directory for testing."""
    config_dir = tmp_path / "config"
    config_dir.mkdir(exist_ok=True)
    return config_dir


@pytest.fixture
def monolithic_config_content() -> Dict[str, Any]:
    """Standard monolithic config content for testing."""
    return {
        "event_source": "eventhouse",
        "kafka": {
            "connection": {
                "bootstrap_servers": "localhost:9094",
                "security_protocol": "PLAINTEXT",
                "sasl_mechanism": "OAUTHBEARER",
                "request_timeout_ms": 120000,
                "metadata_max_age_ms": 300000,
            },
            "consumer_defaults": {
                "auto_offset_reset": "earliest",
                "enable_auto_commit": False,
                "max_poll_records": 1000,
                "max_poll_interval_ms": 300000,
                "session_timeout_ms": 60000,
                "heartbeat_interval_ms": 3000,
            },
            "producer_defaults": {
                "acks": "all",
                "retries": 3,
                "retry_backoff_ms": 1000,
                "batch_size": 16384,
                "linger_ms": 10,
                "compression_type": "lz4",
            },
            "xact": {
                "topics": {
                    "events": "xact.events.raw",
                    "downloads_pending": "xact.downloads.pending",
                    "downloads_cached": "xact.downloads.cached",
                    "downloads_results": "xact.downloads.results",
                    "dlq": "xact.downloads.dlq",
                    "events_ingested": "xact.events.ingested",
                },
                "consumer_group_prefix": "xact",
                "retry_delays": [300, 600],
                "max_retries": 4,
                "event_ingester": {
                    "consumer": {
                        "group_id": "xact-event-ingester",
                        "max_poll_records": 1000,
                        "max_poll_interval_ms": 3000000,
                        "session_timeout_ms": 45000,
                    },
                    "producer": {
                        "acks": "1",
                        "batch_size": 32768,
                        "linger_ms": 100,
                        "compression_type": "lz4",
                    },
                    "processing": {
                        "batch_size": 1000,
                        "max_batches": None,
                        "health_port": 8080,
                    },
                },
                "download_worker": {
                    "consumer": {
                        "group_id": "xact-download-worker",
                        "max_poll_records": 20,
                        "max_poll_interval_ms": 900000,
                    },
                    "producer": {
                        "acks": "all",
                        "compression_type": "none",
                    },
                    "processing": {
                        "concurrency": 20,
                        "batch_size": 20,
                        "timeout_seconds": 60,
                    },
                },
            },
            "claimx": {
                "topics": {
                    "events": "claimx.events.raw",
                    "enrichment_pending": "claimx.enrichment.pending",
                    "downloads_pending": "claimx.downloads.pending",
                },
                "consumer_group_prefix": "claimx",
                "retry_delays": [300, 600],
                "max_retries": 2,
                "event_ingester": {
                    "consumer": {
                        "group_id": "claimx-event-ingester",
                        "max_poll_records": 1000,
                    },
                    "producer": {
                        "acks": "1",
                    },
                    "processing": {
                        "batch_size": 100,
                        "health_port": 8084,
                    },
                },
            },
            "storage": {
                "onelake_base_path": "abfss://workspace@onelake/lakehouse/Files",
                "onelake_domain_paths": {
                    "xact": "abfss://workspace@onelake/lakehouse/Files/xact",
                    "claimx": "abfss://workspace@onelake/lakehouse/Files/claimx",
                },
                "cache_dir": "/tmp/kafka_pipeline_cache",
            },
        },
        "delta": {
            "enable_writes": True,
            "xact": {
                "events_table_path": "abfss://workspace@onelake/Tables/xact_events",
                "inventory_table_path": "abfss://workspace@onelake/Tables/xact_attachments",
            },
        },
    }


@pytest.fixture
def split_config_files() -> Dict[str, Dict[str, Any]]:
    """Split config files content for testing."""
    return {
        "01_shared.yaml": {
            "kafka": {
                "connection": {
                    "bootstrap_servers": "localhost:9094",
                    "security_protocol": "PLAINTEXT",
                    "sasl_mechanism": "OAUTHBEARER",
                    "request_timeout_ms": 120000,
                    "metadata_max_age_ms": 300000,
                },
                "consumer_defaults": {
                    "auto_offset_reset": "earliest",
                    "enable_auto_commit": False,
                    "max_poll_records": 1000,
                    "max_poll_interval_ms": 300000,
                    "session_timeout_ms": 60000,
                    "heartbeat_interval_ms": 3000,
                },
                "producer_defaults": {
                    "acks": "all",
                    "retries": 3,
                    "retry_backoff_ms": 1000,
                    "batch_size": 16384,
                    "linger_ms": 10,
                    "compression_type": "lz4",
                },
            },
        },
        "02_storage.yaml": {
            "kafka": {
                "storage": {
                    "onelake_base_path": "abfss://workspace@onelake/lakehouse/Files",
                    "onelake_domain_paths": {
                        "xact": "abfss://workspace@onelake/lakehouse/Files/xact",
                        "claimx": "abfss://workspace@onelake/lakehouse/Files/claimx",
                    },
                    "cache_dir": "/tmp/kafka_pipeline_cache",
                },
            },
        },
        "03_domain_xact.yaml": {
            "kafka": {
                "xact": {
                    "topics": {
                        "events": "xact.events.raw",
                        "downloads_pending": "xact.downloads.pending",
                        "downloads_cached": "xact.downloads.cached",
                        "downloads_results": "xact.downloads.results",
                        "dlq": "xact.downloads.dlq",
                        "events_ingested": "xact.events.ingested",
                    },
                    "consumer_group_prefix": "xact",
                    "retry_delays": [300, 600],
                    "max_retries": 4,
                    "event_ingester": {
                        "consumer": {
                            "group_id": "xact-event-ingester",
                            "max_poll_records": 1000,
                            "max_poll_interval_ms": 3000000,
                            "session_timeout_ms": 45000,
                        },
                        "producer": {
                            "acks": "1",
                            "batch_size": 32768,
                            "linger_ms": 100,
                            "compression_type": "lz4",
                        },
                        "processing": {
                            "batch_size": 1000,
                            "max_batches": None,
                            "health_port": 8080,
                        },
                    },
                    "download_worker": {
                        "consumer": {
                            "group_id": "xact-download-worker",
                            "max_poll_records": 20,
                            "max_poll_interval_ms": 900000,
                        },
                        "producer": {
                            "acks": "all",
                            "compression_type": "none",
                        },
                        "processing": {
                            "concurrency": 20,
                            "batch_size": 20,
                            "timeout_seconds": 60,
                        },
                    },
                },
            },
            "delta": {
                "xact": {
                    "events_table_path": "abfss://workspace@onelake/Tables/xact_events",
                    "inventory_table_path": "abfss://workspace@onelake/Tables/xact_attachments",
                },
            },
        },
        "04_domain_claimx.yaml": {
            "kafka": {
                "claimx": {
                    "topics": {
                        "events": "claimx.events.raw",
                        "enrichment_pending": "claimx.enrichment.pending",
                        "downloads_pending": "claimx.downloads.pending",
                    },
                    "consumer_group_prefix": "claimx",
                    "retry_delays": [300, 600],
                    "max_retries": 2,
                    "event_ingester": {
                        "consumer": {
                            "group_id": "claimx-event-ingester",
                            "max_poll_records": 1000,
                        },
                        "producer": {
                            "acks": "1",
                        },
                        "processing": {
                            "batch_size": 100,
                            "health_port": 8084,
                        },
                    },
                },
            },
        },
        "05_event_source.yaml": {
            "event_source": "eventhouse",
            "delta": {
                "enable_writes": True,
            },
        },
    }


def write_yaml_file(path: Path, content: Dict[str, Any]) -> None:
    """Helper to write YAML file."""
    with open(path, "w") as f:
        yaml.safe_dump(content, f, default_flow_style=False, sort_keys=False)


# =============================================================================
# =============================================================================
# Test: Loading from config/ Directory
# =============================================================================


class TestSplitConfigLoading:
    """Test loading configuration from split config/ directory."""

    @pytest.fixture(autouse=True)
    def reset_singleton(self):
        """Reset singleton before and after each test."""
        reset_config()
        yield
        reset_config()

    def test_load_from_split_config_directory(self, temp_config_dir, split_config_files):
        """Test loading from split YAML files in config/ directory."""
        # Write all split config files
        for filename, content in split_config_files.items():
            write_yaml_file(temp_config_dir / filename, content)

        # TODO: Once implementation is ready, this should load from directory
        # For now, create a merged config manually for testing structure
        # config = load_config_from_directory(config_dir=temp_config_dir)

        # Placeholder: Load from merged file for now
        # This test will be updated when load_config_from_directory is implemented
        pytest.skip("Waiting for load_config_from_directory implementation")

    def test_split_config_file_ordering(self, temp_config_dir):
        """Test that files are loaded in lexicographic order (01, 02, 03...)."""
        # Create files with conflicting values
        write_yaml_file(
            temp_config_dir / "01_first.yaml",
            {"kafka": {"connection": {"bootstrap_servers": "first:9092"}}},
        )
        write_yaml_file(
            temp_config_dir / "02_second.yaml",
            {"kafka": {"connection": {"bootstrap_servers": "second:9092"}}},
        )

        # TODO: Implement and test
        pytest.skip("Waiting for load_config_from_directory implementation")

        # Later file should override
        # config = load_config_from_directory(config_dir=temp_config_dir)
        # assert config.bootstrap_servers == "second:9092"

    def test_split_config_ignores_non_yaml_files(self, temp_config_dir):
        """Test that non-YAML files in config/ are ignored."""
        write_yaml_file(
            temp_config_dir / "01_valid.yaml",
            {"kafka": {"connection": {"bootstrap_servers": "valid:9092"}}},
        )

        # Create non-YAML files
        (temp_config_dir / "README.md").write_text("# Config files")
        (temp_config_dir / "notes.txt").write_text("Some notes")
        (temp_config_dir / ".hidden.yaml").write_text("hidden: true")

        # TODO: Implement and test
        pytest.skip("Waiting for load_config_from_directory implementation")

        # Should load without errors
        # config = load_config_from_directory(config_dir=temp_config_dir)
        # assert config.bootstrap_servers == "valid:9092"


# =============================================================================
# Test: Merge Precedence (Later Files Override Earlier)
# =============================================================================


class TestMergePrecedence:
    """Test that later files override earlier files correctly."""

    @pytest.fixture(autouse=True)
    def reset_singleton(self):
        """Reset singleton before and after each test."""
        reset_config()
        yield
        reset_config()

    def test_later_file_overrides_earlier_scalar(self, temp_config_dir):
        """Test that scalar values in later files override earlier values."""
        write_yaml_file(
            temp_config_dir / "01_base.yaml",
            {"kafka": {"consumer_defaults": {"max_poll_records": 1000}}},
        )
        write_yaml_file(
            temp_config_dir / "02_override.yaml",
            {"kafka": {"consumer_defaults": {"max_poll_records": 2000}}},
        )

        # TODO: Implement and test
        pytest.skip("Waiting for merge implementation")

        # config = load_config_from_directory(config_dir=temp_config_dir)
        # assert config.consumer_defaults["max_poll_records"] == 2000

    def test_later_file_overrides_arrays_completely(self, temp_config_dir):
        """Test that arrays in later files completely replace earlier arrays."""
        write_yaml_file(
            temp_config_dir / "01_base.yaml",
            {"kafka": {"xact": {"retry_delays": [300, 600, 1200, 2400]}}},
        )
        write_yaml_file(
            temp_config_dir / "02_override.yaml",
            {"kafka": {"xact": {"retry_delays": [300, 600]}}},
        )

        # TODO: Implement and test
        pytest.skip("Waiting for merge implementation")

        # config = load_config_from_directory(config_dir=temp_config_dir)
        # assert config.xact["retry_delays"] == [300, 600]  # NOT [300, 600, 1200, 2400]


# =============================================================================
# Test: Deep Merge Behavior for Nested Dicts
# =============================================================================


class TestDeepMerge:
    """Test that nested dictionaries merge deeply."""

    @pytest.fixture(autouse=True)
    def reset_singleton(self):
        """Reset singleton before and after each test."""
        reset_config()
        yield
        reset_config()

    def test_nested_dicts_merge_deeply(self, temp_config_dir):
        """Test that nested dicts merge keys instead of replacing entire dict."""
        write_yaml_file(
            temp_config_dir / "01_base.yaml",
            {
                "kafka": {
                    "connection": {
                        "bootstrap_servers": "localhost:9094",
                        "security_protocol": "PLAINTEXT",
                    }
                }
            },
        )
        write_yaml_file(
            temp_config_dir / "02_add_sasl.yaml",
            {"kafka": {"connection": {"sasl_mechanism": "OAUTHBEARER"}}},
        )

        # TODO: Implement and test
        pytest.skip("Waiting for deep merge implementation")

        # config = load_config_from_directory(config_dir=temp_config_dir)
        # All three keys should be present
        # assert config.bootstrap_servers == "localhost:9094"
        # assert config.security_protocol == "PLAINTEXT"
        # assert config.sasl_mechanism == "OAUTHBEARER"

    def test_deep_merge_preserves_worker_configs(self, temp_config_dir):
        """Test that worker configs merge properly across files."""
        write_yaml_file(
            temp_config_dir / "01_shared.yaml",
            {"kafka": {"consumer_defaults": {"max_poll_records": 1000}}},
        )
        write_yaml_file(
            temp_config_dir / "02_xact.yaml",
            {
                "kafka": {
                    "xact": {
                        "event_ingester": {
                            "consumer": {
                                "group_id": "xact-event-ingester",
                                "max_poll_interval_ms": 3000000,
                            }
                        }
                    }
                }
            },
        )

        # TODO: Implement and test
        pytest.skip("Waiting for deep merge implementation")

        # config = load_config_from_directory(config_dir=temp_config_dir)
        # worker_cfg = config.get_worker_config("xact", "event_ingester", "consumer")
        # Should have both defaults and worker-specific
        # assert worker_cfg["max_poll_records"] == 1000  # From defaults
        # assert worker_cfg["group_id"] == "xact-event-ingester"  # Worker-specific


# =============================================================================
# Test: Environment Variable Overrides
# =============================================================================


class TestEnvironmentOverrides:
    """Test that environment variables can still override config values."""

    @pytest.fixture(autouse=True)
    def reset_singleton(self):
        """Reset singleton before and after each test."""
        reset_config()
        yield
        reset_config()

    def test_env_var_overrides_yaml(self, temp_config_dir, split_config_files, monkeypatch):
        """Test that environment variables override YAML config."""
        for filename, content in split_config_files.items():
            write_yaml_file(temp_config_dir / filename, content)

        # Set environment variable for ClaimX API token (the one env var we support)
        monkeypatch.setenv("CLAIMX_API_TOKEN", "env-override-token")

        # TODO: Implement env var override support
        pytest.skip("Waiting for environment override implementation")

        # config = load_config(config_path=temp_config_dir)
        # assert config.claimx_api_token == "env-override-token"

    def test_env_vars_work_with_split_config(self, temp_config_dir, split_config_files, monkeypatch):
        """Test that env vars work with split config directory."""
        for filename, content in split_config_files.items():
            write_yaml_file(temp_config_dir / filename, content)

        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "env-override:9092")

        # TODO: Implement
        pytest.skip("Waiting for environment override implementation")


# =============================================================================
# Test: Missing File Handling
# =============================================================================


class TestMissingFiles:
    """Test error handling for missing or invalid config files."""

    @pytest.fixture(autouse=True)
    def reset_singleton(self):
        """Reset singleton before and after each test."""
        reset_config()
        yield
        reset_config()

    def test_missing_config_directory_raises_error(self, tmp_path):
        """Test that missing config directory raises FileNotFoundError."""
        missing_dir = tmp_path / "nonexistent_config"

        with pytest.raises(FileNotFoundError):
            load_config(config_path=missing_dir)

    def test_empty_config_directory_raises_error(self, temp_config_dir):
        """Test that empty config/ directory raises error."""
        # TODO: Implement
        pytest.skip("Waiting for directory validation implementation")

        # with pytest.raises(ValueError, match="No YAML files found"):
        #     load_config_from_directory(config_dir=temp_config_dir)

    def test_invalid_yaml_syntax_raises_error(self, temp_config_dir):
        """Test that invalid YAML syntax raises error."""
        invalid_file = temp_config_dir / "01_invalid.yaml"
        invalid_file.write_text("kafka:\n  invalid: [unclosed bracket")

        # TODO: Implement
        pytest.skip("Waiting for YAML validation implementation")

        # with pytest.raises(yaml.YAMLError):
        #     load_config_from_directory(config_dir=temp_config_dir)


# =============================================================================
# Test: Plugin Config Loading
# =============================================================================


class TestPluginConfigLoading:
    """Test loading plugin-specific config from config/plugins/ directory."""

    @pytest.fixture(autouse=True)
    def reset_singleton(self):
        """Reset singleton before and after each test."""
        reset_config()
        yield
        reset_config()

    def test_plugin_config_loaded_separately(self, temp_config_dir):
        """Test that plugin configs in config/plugins/ are loaded."""
        plugins_dir = temp_config_dir / "plugins"
        plugins_dir.mkdir(exist_ok=True)

        write_yaml_file(
            plugins_dir / "custom_plugin.yaml",
            {
                "plugin": {
                    "name": "custom_plugin",
                    "enabled": True,
                    "settings": {"param1": "value1"},
                }
            },
        )

        # TODO: Implement plugin loading
        pytest.skip("Waiting for plugin config implementation")

        # plugins = load_plugin_configs(config_dir=temp_config_dir)
        # assert "custom_plugin" in plugins
        # assert plugins["custom_plugin"]["enabled"] is True

    def test_plugin_config_does_not_affect_main_config(self, temp_config_dir):
        """Test that plugin configs don't interfere with main config."""
        plugins_dir = temp_config_dir / "plugins"
        plugins_dir.mkdir(exist_ok=True)

        # Main config
        write_yaml_file(
            temp_config_dir / "01_shared.yaml",
            {"kafka": {"connection": {"bootstrap_servers": "main:9092"}}},
        )

        # Plugin config (should be separate)
        write_yaml_file(
            plugins_dir / "plugin.yaml",
            {"kafka": {"connection": {"bootstrap_servers": "plugin:9092"}}},
        )

        # TODO: Implement
        pytest.skip("Waiting for plugin isolation implementation")

        # config = load_config_from_directory(config_dir=temp_config_dir)
        # assert config.bootstrap_servers == "main:9092"  # Not affected by plugin config


# =============================================================================
# Test: Config Validation After Loading
# =============================================================================


class TestConfigValidation:
    """Test that loaded configs are validated properly."""

    @pytest.fixture(autouse=True)
    def reset_singleton(self):
        """Reset singleton before and after each test."""
        reset_config()
        yield
        reset_config()

    def test_merged_config_passes_validation(self, temp_config_dir):
        """Test that merged config passes existing validation rules."""
        # Create config directory with properly structured files
        (temp_config_dir / "shared.yaml").write_text("""
kafka:
  connection:
    bootstrap_servers: "localhost:9094"
    security_protocol: "PLAINTEXT"
    sasl_mechanism: "OAUTHBEARER"
    request_timeout_ms: 120000
    metadata_max_age_ms: 300000
  consumer_defaults:
    auto_offset_reset: "earliest"
    enable_auto_commit: false
    max_poll_records: 1000
    max_poll_interval_ms: 300000
    session_timeout_ms: 60000
    heartbeat_interval_ms: 3000
  producer_defaults:
    acks: "all"
    retries: 3
    retry_backoff_ms: 1000
    batch_size: 16384
    linger_ms: 10
    compression_type: "lz4"
  storage:
    onelake_base_path: "abfss://workspace@onelake/lakehouse/Files"
    cache_dir: "/tmp/kafka_pipeline_cache"
""")

        (temp_config_dir / "xact_config.yaml").write_text("""
kafka:
  xact:
    topics:
      events: "xact.events.raw"
      downloads_pending: "xact.downloads.pending"
      downloads_cached: "xact.downloads.cached"
      downloads_results: "xact.downloads.results"
      dlq: "xact.downloads.dlq"
      events_ingested: "xact.events.ingested"
    consumer_group_prefix: "xact"
    retry_delays: [300, 600]
    max_retries: 4
    event_ingester:
      consumer:
        group_id: "xact-event-ingester"
        max_poll_records: 1000
        max_poll_interval_ms: 3000000
        session_timeout_ms: 45000
      producer:
        acks: "1"
        batch_size: 32768
        linger_ms: 100
        compression_type: "lz4"
      processing:
        batch_size: 1000
        health_port: 8080
delta:
  xact:
    events_table_path: "abfss://workspace@onelake/Tables/xact_events"
    inventory_table_path: "abfss://workspace@onelake/Tables/xact_attachments"
""")

        (temp_config_dir / "claimx_config.yaml").write_text("""
kafka:
  claimx:
    topics:
      events: "claimx.events.raw"
      enrichment_pending: "claimx.enrichment.pending"
      downloads_pending: "claimx.downloads.pending"
    consumer_group_prefix: "claimx"
    retry_delays: [300, 600]
    max_retries: 2
""")

        # Should not raise any validation errors
        config = load_config(config_path=temp_config_dir)
        config.validate()  # Explicit validation call

    def test_invalid_timeout_values_caught(self, temp_config_dir):
        """Test that validation catches invalid Kafka timeout values."""
        # Write minimal valid shared.yaml
        write_yaml_file(
            temp_config_dir / "shared.yaml",
            {
                "kafka": {
                    "connection": {"bootstrap_servers": "localhost:9092"},
                    "consumer_defaults": {
                        "heartbeat_interval_ms": 30000,  # TOO HIGH!
                        "session_timeout_ms": 60000,  # heartbeat should be < session/3
                    },
                }
            }
        )

        # Should raise validation error
        with pytest.raises(ValueError, match="heartbeat_interval_ms"):
            load_config(config_path=temp_config_dir)


# =============================================================================
# Performance Tests
# =============================================================================


class TestConfigLoadingPerformance:
    """Test that config loading is performant."""

    @pytest.fixture(autouse=True)
    def reset_singleton(self):
        """Reset singleton before and after each test."""
        reset_config()
        yield
        reset_config()

    def test_loading_completes_quickly(self, temp_config_dir):
        """Test that config loading completes in reasonable time."""
        import time

        # Create minimal config files
        (temp_config_dir / "shared.yaml").write_text("""
kafka:
  connection:
    bootstrap_servers: "localhost:9094"
    security_protocol: "PLAINTEXT"
  consumer_defaults:
    auto_offset_reset: "earliest"
    enable_auto_commit: false
    max_poll_records: 1000
    session_timeout_ms: 60000
    heartbeat_interval_ms: 3000
  producer_defaults:
    acks: "all"
    retries: 3
""")

        (temp_config_dir / "xact_config.yaml").write_text("""
kafka:
  xact:
    topics:
      events: "xact.events.raw"
      downloads_pending: "xact.downloads.pending"
    consumer_group_prefix: "xact"
""")

        (temp_config_dir / "claimx_config.yaml").write_text("""
kafka:
  claimx:
    topics:
      events: "claimx.events.raw"
""")

        start = time.time()
        config = load_config(config_path=temp_config_dir)
        elapsed = time.time() - start

        # Should complete in under 100ms
        assert elapsed < 0.1, f"Config loading took {elapsed:.3f}s (should be < 0.1s)"

    def test_large_split_config_loads_efficiently(self, temp_config_dir):
        """Test that loading many split files is still efficient."""
        import time

        # Create 20 config files
        for i in range(20):
            write_yaml_file(
                temp_config_dir / f"{i:02d}_file.yaml",
                {"kafka": {"connection": {f"setting_{i}": f"value_{i}"}}},
            )

        # TODO: Implement
        pytest.skip("Waiting for split config implementation")

        # start = time.time()
        # config = load_config_from_directory(config_dir=temp_config_dir)
        # elapsed = time.time() - start

        # Should still be fast
        # assert elapsed < 0.2, f"Loading 20 files took {elapsed:.3f}s (should be < 0.2s)"
