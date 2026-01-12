"""Tests for Kafka configuration (YAML-based)."""

import os
from pathlib import Path
import pytest
import tempfile

from kafka_pipeline.config import (
    KafkaConfig,
    load_config,
    get_config,
    set_config,
    reset_config,
    DEFAULT_CONFIG_DIR,
)


class TestKafkaConfig:
    """Test KafkaConfig dataclass with YAML loading."""

    @pytest.fixture(autouse=True)
    def clean_singleton(self):
        """Reset singleton before and after each test."""
        reset_config()
        yield
        reset_config()

    def test_direct_instantiation(self):
        """Test creating KafkaConfig directly without YAML."""
        config = KafkaConfig(
            bootstrap_servers="kafka:9093",
            xact={
                "topics": {
                    "events": "test.events",
                    "downloads_pending": "test.downloads.pending",
                }
            },
        )

        assert config.bootstrap_servers == "kafka:9093"
        assert config.get_topic("xact", "events") == "test.events"
        assert config.security_protocol == "PLAINTEXT"  # default

    def test_get_topic(self):
        """Test get_topic method returns correct topic name."""
        config = KafkaConfig(
            bootstrap_servers="kafka:9093",
            xact={
                "topics": {
                    "events": "xact.events.raw",
                    "downloads_pending": "xact.downloads.pending",
                }
            },
        )

        assert config.get_topic("xact", "events") == "xact.events.raw"
        assert config.get_topic("xact", "downloads_pending") == "xact.downloads.pending"

    def test_get_topic_missing_raises(self):
        """Test get_topic raises ValueError for missing topic."""
        config = KafkaConfig(
            bootstrap_servers="kafka:9093",
            xact={
                "topics": {
                    "events": "xact.events.raw",
                }
            },
        )

        with pytest.raises(ValueError, match="Topic 'missing' not found"):
            config.get_topic("xact", "missing")

    def test_get_retry_delays(self):
        """Test get_retry_delays returns default or configured delays."""
        config = KafkaConfig(
            bootstrap_servers="kafka:9093",
            xact={
                "topics": {"downloads_pending": "xact.downloads.pending"},
                "retry_delays": [300, 600, 1200, 2400],
            },
        )

        assert config.get_retry_delays("xact") == [300, 600, 1200, 2400]

    def test_get_retry_delays_default(self):
        """Test get_retry_delays returns default when not configured."""
        config = KafkaConfig(
            bootstrap_servers="kafka:9093",
            xact={
                "topics": {"downloads_pending": "xact.downloads.pending"},
            },
        )

        # Default is [300, 600, 1200, 2400]
        assert config.get_retry_delays("xact") == [300, 600, 1200, 2400]

    def test_get_max_retries(self):
        """Test get_max_retries returns length of retry_delays."""
        config = KafkaConfig(
            bootstrap_servers="kafka:9093",
            xact={
                "topics": {"downloads_pending": "xact.downloads.pending"},
                "retry_delays": [60, 120, 240],  # 3 retries
            },
        )

        assert config.get_max_retries("xact") == 3

    def test_get_retry_topic(self):
        """Test get_retry_topic generates correct topic names."""
        config = KafkaConfig(
            bootstrap_servers="kafka:9093",
            xact={
                "topics": {"downloads_pending": "xact.downloads.pending"},
                "retry_delays": [300, 600, 1200, 2400],  # 5m, 10m, 20m, 40m
            },
        )

        assert config.get_retry_topic("xact", 0) == "xact.downloads.pending.retry.5m"
        assert config.get_retry_topic("xact", 1) == "xact.downloads.pending.retry.10m"
        assert config.get_retry_topic("xact", 2) == "xact.downloads.pending.retry.20m"
        assert config.get_retry_topic("xact", 3) == "xact.downloads.pending.retry.40m"

    def test_get_retry_topic_exceeds_max_raises(self):
        """Test get_retry_topic raises ValueError when attempt exceeds max."""
        config = KafkaConfig(
            bootstrap_servers="kafka:9093",
            xact={
                "topics": {"downloads_pending": "xact.downloads.pending"},
                "retry_delays": [300, 600, 1200, 2400],
            },
        )

        with pytest.raises(ValueError, match="exceeds max retries"):
            config.get_retry_topic("xact", 4)

    def test_get_retry_topic_custom_delays(self):
        """Test get_retry_topic with custom retry delays."""
        config = KafkaConfig(
            bootstrap_servers="kafka:9093",
            xact={
                "topics": {"downloads_pending": "xact.downloads.pending"},
                "retry_delays": [60, 180],  # 1m, 3m
            },
        )

        assert config.get_retry_topic("xact", 0) == "xact.downloads.pending.retry.1m"
        assert config.get_retry_topic("xact", 1) == "xact.downloads.pending.retry.3m"

    def test_get_consumer_group(self):
        """Test get_consumer_group generates correct group name."""
        config = KafkaConfig(
            bootstrap_servers="kafka:9093",
            xact={
                "topics": {"downloads_pending": "xact.downloads.pending"},
                "consumer_group_prefix": "xact",
            },
        )

        assert config.get_consumer_group("xact", "download_worker") == "xact-download_worker"

    def test_get_consumer_group_custom_prefix(self):
        """Test get_consumer_group with custom prefix."""
        config = KafkaConfig(
            bootstrap_servers="kafka:9093",
            xact={
                "topics": {"downloads_pending": "xact.downloads.pending"},
                "consumer_group_prefix": "custom",
            },
        )

        assert config.get_consumer_group("xact", "download_worker") == "custom-download_worker"

    def test_get_worker_config(self):
        """Test get_worker_config returns merged configuration."""
        config = KafkaConfig(
            bootstrap_servers="kafka:9093",
            consumer_defaults={"max_poll_records": 100, "session_timeout_ms": 30000},
            xact={
                "topics": {"downloads_pending": "xact.downloads.pending"},
                "download_worker": {
                    "consumer": {"max_poll_records": 50},  # Override
                },
            },
        )

        worker_config = config.get_worker_config("xact", "download_worker", "consumer")
        assert worker_config["max_poll_records"] == 50  # Override
        assert worker_config["session_timeout_ms"] == 30000  # Default

    def test_dataclass_mutability(self):
        """Test that config values can be modified after creation."""
        config = KafkaConfig(bootstrap_servers="kafka:9093")
        original_servers = config.bootstrap_servers

        config.bootstrap_servers = "kafka2:9093"
        assert config.bootstrap_servers == "kafka2:9093"
        assert config.bootstrap_servers != original_servers


class TestYamlConfigLoading:
    """Test YAML configuration loading."""

    @pytest.fixture(autouse=True)
    def clean_singleton(self):
        """Reset singleton before and after each test."""
        reset_config()
        yield
        reset_config()

    def test_load_config_from_yaml(self, tmp_path):
        """Test loading configuration from YAML file."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        (config_dir / "shared.yaml").write_text("""
kafka:
  connection:
    bootstrap_servers: "yaml-kafka:9092"
    security_protocol: "PLAINTEXT"
""")

        (config_dir / "xact_config.yaml").write_text("""
kafka:
  xact:
    topics:
      events: "yaml.events"
      downloads_pending: "yaml.downloads.pending"
""")

        (config_dir / "claimx_config.yaml").write_text("""
kafka:
  claimx:
    topics:
      events: "claimx.events"
""")

        config = load_config(config_path=config_dir)

        assert config.bootstrap_servers == "yaml-kafka:9092"
        assert config.security_protocol == "PLAINTEXT"
        assert config.get_topic("xact", "events") == "yaml.events"

    def test_load_config_missing_kafka_section_raises(self, tmp_path):
        """Test loading config without 'kafka:' section raises ValueError."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        (config_dir / "shared.yaml").write_text("""
bootstrap_servers: "flat-kafka:9092"
security_protocol: "SASL_SSL"
""")

        (config_dir / "xact_config.yaml").write_text("""
xact:
  topics:
    downloads_pending: "test.pending"
""")

        (config_dir / "claimx_config.yaml").write_text("""
claimx:
  topics:
    events: "claimx.events"
""")

        with pytest.raises(ValueError, match="missing 'kafka:' section"):
            load_config(config_path=config_dir)

    def test_overrides_parameter(self, tmp_path):
        """Test that overrides parameter works."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        (config_dir / "shared.yaml").write_text("""
kafka:
  connection:
    bootstrap_servers: "yaml-kafka:9092"
""")

        (config_dir / "xact_config.yaml").write_text("""
kafka:
  xact:
    topics:
      downloads_pending: "test.downloads.pending"
""")

        (config_dir / "claimx_config.yaml").write_text("""
kafka:
  claimx:
    topics:
      events: "claimx.events"
""")

        config = load_config(
            config_path=config_dir,
            overrides={"connection": {"bootstrap_servers": "override:9092"}}
        )

        assert config.bootstrap_servers == "override:9092"

    def test_missing_bootstrap_servers_raises(self, tmp_path):
        """Test that missing bootstrap_servers raises ValueError."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        (config_dir / "shared.yaml").write_text("""
kafka:
  connection:
    security_protocol: "PLAINTEXT"
""")

        (config_dir / "xact_config.yaml").write_text("""
kafka:
  xact:
    topics:
      downloads_pending: "test.pending"
""")

        (config_dir / "claimx_config.yaml").write_text("""
kafka:
  claimx:
    topics:
      events: "claimx.events"
""")

        with pytest.raises(ValueError, match="bootstrap_servers is required"):
            load_config(config_path=config_dir)

    def test_retry_delays_from_yaml(self, tmp_path):
        """Test loading retry_delays list from YAML."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        (config_dir / "shared.yaml").write_text("""
kafka:
  connection:
    bootstrap_servers: "kafka:9092"
""")

        (config_dir / "xact_config.yaml").write_text("""
kafka:
  xact:
    topics:
      downloads_pending: "test.downloads.pending"
    retry_delays: [60, 120, 240]
""")

        (config_dir / "claimx_config.yaml").write_text("""
kafka:
  claimx:
    topics:
      events: "claimx.events"
""")

        config = load_config(config_path=config_dir)

        assert config.get_retry_delays("xact") == [60, 120, 240]


class TestConfigSingleton:
    """Test singleton config pattern."""

    @pytest.fixture(autouse=True)
    def reset_singleton(self):
        """Reset singleton before and after each test."""
        reset_config()
        yield
        reset_config()

    def test_get_config_returns_same_instance(self, tmp_path, monkeypatch):
        """Test that get_config returns the same instance."""
        # Create config directory structure
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        # Write shared.yaml
        (config_dir / "shared.yaml").write_text("""
kafka:
  connection:
    bootstrap_servers: "singleton-kafka:9092"
""")

        # Write xact_config.yaml
        (config_dir / "xact_config.yaml").write_text("""
kafka:
  xact:
    topics:
      downloads_pending: "test.pending"
""")

        # Write claimx_config.yaml (minimal)
        (config_dir / "claimx_config.yaml").write_text("""
kafka:
  claimx:
    topics:
      events: "claimx.events"
""")

        # Point to our test config directory
        monkeypatch.setattr(
            "kafka_pipeline.config.DEFAULT_CONFIG_DIR",
            config_dir
        )

        config1 = get_config()
        config2 = get_config()

        assert config1 is config2
        assert config1.bootstrap_servers == "singleton-kafka:9092"

    def test_set_config_overrides_singleton(self):
        """Test that set_config sets the singleton instance."""
        custom_config = KafkaConfig(bootstrap_servers="custom:9092")

        set_config(custom_config)
        retrieved = get_config()

        assert retrieved is custom_config
        assert retrieved.bootstrap_servers == "custom:9092"

    def test_reset_config_clears_singleton(self, tmp_path, monkeypatch):
        """Test that reset_config clears the singleton."""
        # Create config directory structure
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        # Write initial config files
        shared_file = config_dir / "shared.yaml"
        shared_file.write_text("""
kafka:
  connection:
    bootstrap_servers: "first:9092"
""")

        (config_dir / "xact_config.yaml").write_text("""
kafka:
  xact:
    topics:
      downloads_pending: "test.pending"
""")

        (config_dir / "claimx_config.yaml").write_text("""
kafka:
  claimx:
    topics:
      events: "claimx.events"
""")

        monkeypatch.setattr(
            "kafka_pipeline.config.DEFAULT_CONFIG_DIR",
            config_dir
        )

        first = get_config()
        assert first.bootstrap_servers == "first:9092"

        # Update shared config file
        shared_file.write_text("""
kafka:
  connection:
    bootstrap_servers: "second:9092"
""")

        reset_config()
        second = get_config()

        assert second.bootstrap_servers == "second:9092"
        assert first is not second
