"""
Pipeline configuration for Event Hub pipeline.

Architecture:
    - Event Source: Azure Event Hub
    - Internal communication: EventHub (AMQP transport)

Workers:
    - EventIngesterWorker: Reads from Event Hub, produces enrichment tasks
    - DownloadWorker: Reads/writes via EventHub
    - ResultProcessor: Reads from EventHub
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from config.config import MessageConfig

# Default config file: config/config.yaml in src/ directory
DEFAULT_CONFIG_FILE = Path(__file__).parent.parent / "config" / "config.yaml"


def _get_config_value(env_var: str, yaml_value: str, default: str = "") -> str:
    """
    Get config value from env var or yaml or default.

    Expands environment variables in yaml_value if present (e.g., ${VAR_NAME}).
    If expansion fails (env var not set), the literal ${VAR} string remains.
    """
    value = os.getenv(env_var)
    if value:
        return value

    # Expand environment variables in yaml_value
    # This handles cases like "events_table_path: ${CLAIMX_DELTA_EVENTS_TABLE}"
    result = yaml_value or default
    if result:
        expanded = os.path.expandvars(result)

        # Warn if expansion failed (still contains ${...})
        if "${" in expanded and "}" in expanded:
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(
                f"Config value contains unexpanded environment variable: {expanded}. "
                f"Ensure the environment variable is set before starting the worker."
            )

        result = expanded

    return result


def _get_storage_config(kafka_data: dict) -> dict:
    """Get storage config from kafka.storage section only."""
    return kafka_data.get("storage", {})


def _parse_bool_env(env_var: str, yaml_value: Any) -> bool:
    """Parse boolean from env var or yaml value."""
    env_str = os.getenv(env_var)
    if env_str is not None:
        return env_str.lower() in ("true", "1", "yes")
    if isinstance(yaml_value, bool):
        return yaml_value
    if isinstance(yaml_value, str):
        return yaml_value.lower() in ("true", "1", "yes")
    return bool(yaml_value)


def _load_config_data(config_path: Path) -> dict[str, Any]:
    """Load configuration data from config.yaml file.

    Args:
        config_path: Path to config.yaml file

    Returns:
        Configuration dictionary

    Raises:
        FileNotFoundError: If config file doesn't exist
    """
    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}\nExpected file: config/config.yaml"
        )

    # Load from single file
    from config.config import _expand_env_vars, load_yaml

    config_data = load_yaml(config_path)
    return _expand_env_vars(config_data)


@dataclass
class EventHubConfig:
    """Configuration for Azure Event Hub connection.

    Supports two modes:
    1. AMQP transport (default): Namespace connection string + per-entity config from config.yaml
    2. Kafka protocol (legacy): Full connection string with SASL_PLAIN authentication

    For AMQP transport, entity names and consumer groups are defined per-topic
    in config.yaml under eventhub.{domain}.{topic_key}.
    """

    # Namespace-level connection string (no EntityPath)
    namespace_connection_string: str = ""

    # Kafka-compatible settings (legacy, for Kafka protocol fallback)
    bootstrap_servers: str = ""
    security_protocol: str = "SASL_SSL"
    sasl_mechanism: str = "PLAIN"
    sasl_username: str = "$ConnectionString"
    sasl_password: str = ""  # Full connection string for Kafka protocol

    # Consumer settings (defaults, can be overridden per-topic in config.yaml)
    events_topic: str = "verisk_events"
    consumer_group: str = "xact-event-ingester"
    auto_offset_reset: str = "earliest"

    @classmethod
    def from_env(cls) -> "EventHubConfig":
        """Load Event Hub configuration from environment variables.

        Required (at least one):
            EVENTHUB_NAMESPACE_CONNECTION_STRING: Namespace-level connection string (preferred)
            EVENTHUB_CONNECTION_STRING: Full connection string (backward compat)

        Optional (legacy, for Kafka protocol):
            EVENTHUB_BOOTSTRAP_SERVERS: Event Hub namespace bootstrap servers
            EVENTHUB_EVENTS_TOPIC: Topic name
            EVENTHUB_CONSUMER_GROUP: Consumer group
        """
        # Prefer namespace connection string, fall back to legacy
        namespace_conn = os.getenv("EVENTHUB_NAMESPACE_CONNECTION_STRING", "")
        legacy_conn = os.getenv("EVENTHUB_CONNECTION_STRING", "")
        bootstrap_servers = os.getenv("EVENTHUB_BOOTSTRAP_SERVERS", "")

        if not namespace_conn and not legacy_conn:
            raise ValueError(
                "Event Hub connection string is required. "
                "Set EVENTHUB_NAMESPACE_CONNECTION_STRING (preferred) "
                "or EVENTHUB_CONNECTION_STRING environment variable."
            )

        return cls(
            namespace_connection_string=namespace_conn or legacy_conn,
            bootstrap_servers=bootstrap_servers,
            sasl_password=legacy_conn or namespace_conn,
            events_topic=os.getenv("EVENTHUB_EVENTS_TOPIC", "verisk_events"),
            consumer_group=os.getenv("EVENTHUB_CONSUMER_GROUP", "xact-event-ingester"),
            auto_offset_reset=os.getenv("EVENTHUB_AUTO_OFFSET_RESET", "earliest"),
        )

    def to_message_config(self) -> MessageConfig:
        """Convert to MessageConfig for use with MessageConsumer.

        Creates a hierarchical MessageConfig matching the new config.yaml structure.
        Used only when Kafka protocol is needed (not AMQP transport).
        """
        # Build verisk domain config for Event Hub source
        verisk_config = {
            "topics": {
                "events": self.events_topic,
            },
            "consumer_group_prefix": (
                self.consumer_group.rsplit("-", 1)[0] if "-" in self.consumer_group else "verisk"
            ),
            "event_ingester": {
                "consumer": {
                    "group_id": self.consumer_group,
                    "auto_offset_reset": self.auto_offset_reset,
                }
            },
        }

        return MessageConfig(
            bootstrap_servers=self.bootstrap_servers,
            verisk=verisk_config,
        )


# =============================================================================
# Local Kafka transport removed - EventHub is primary
# =============================================================================
# Internal pipeline communication uses EventHub (primary) via AMQP transport.
# Kafka protocol support remains available for local development via MessageConfig.
# Domain-specific settings (topics, retry, storage) remain in MessageConfig.
# =============================================================================


@dataclass
class PipelineConfig:
    """Complete pipeline configuration.

    EventHub is the sole event source. Kafka has been removed.
    """

    # Event Hub config
    eventhub: EventHubConfig | None = None

    # Domain identifier for OneLake routing (e.g., "verisk", "claimx")
    domain: str = "verisk"

    # Delta Lake configuration
    enable_delta_writes: bool = True
    events_table_path: str = ""
    inventory_table_path: str = ""
    failed_table_path: str = ""  # Optional: for tracking permanent failures

    # ClaimX Delta table paths
    claimx_events_table_path: str = ""  # ClaimX events Delta table
    claimx_projects_table_path: str = ""
    claimx_contacts_table_path: str = ""
    claimx_inventory_table_path: str = ""
    claimx_media_table_path: str = ""
    claimx_tasks_table_path: str = ""
    claimx_task_templates_table_path: str = ""
    claimx_external_links_table_path: str = ""
    claimx_video_collab_table_path: str = ""

    @classmethod
    def load_config(cls, config_path: Path | None = None) -> "PipelineConfig":
        """Load complete pipeline configuration from config directory and environment.

        Configuration priority (highest to lowest):
        1. Environment variables
        2. Config files in config/ directory
        3. Dataclass defaults
        """
        # Use default config directory if not specified
        resolved_path = config_path or DEFAULT_CONFIG_FILE

        # Load configuration data from config directory
        yaml_data = _load_config_data(resolved_path)

        eventhub_config = EventHubConfig.from_env()

        # Load delta configuration from yaml with env var override
        delta_config = yaml_data.get("delta", {})
        domain = os.getenv("PIPELINE_DOMAIN", "verisk")
        domain_delta_config = delta_config.get(domain, {})

        # Enable delta writes: env var > yaml > default True
        enable_delta_writes_str = os.getenv("ENABLE_DELTA_WRITES")
        if enable_delta_writes_str is not None:
            enable_delta_writes = enable_delta_writes_str.lower() == "true"
        else:
            enable_delta_writes = delta_config.get("enable_writes", True)

        return cls(
            eventhub=eventhub_config,
            domain=domain,
            enable_delta_writes=enable_delta_writes,
            events_table_path=_get_config_value(
                "VERISK_EVENTS_TABLE_PATH",
                domain_delta_config.get("events_table_path", ""),
            ),
            inventory_table_path=_get_config_value(
                "VERISK_INVENTORY_TABLE_PATH",
                domain_delta_config.get("inventory_table_path", ""),
            ),
            failed_table_path=_get_config_value(
                "VERISK_FAILED_TABLE_PATH",
                domain_delta_config.get("failed_table_path", ""),
            ),
            claimx_events_table_path=_get_config_value(
                "CLAIMX_DELTA_EVENTS_TABLE",
                delta_config.get("claimx", {}).get("events_table_path", ""),
            ),
            claimx_projects_table_path=_get_config_value(
                "CLAIMX_PROJECTS_TABLE_PATH",
                delta_config.get("claimx", {}).get("projects_table_path", ""),
            ),
            claimx_contacts_table_path=_get_config_value(
                "CLAIMX_CONTACTS_TABLE_PATH",
                delta_config.get("claimx", {}).get("contacts_table_path", ""),
            ),
            claimx_inventory_table_path=_get_config_value(
                "CLAIMX_INVENTORY_TABLE_PATH",
                delta_config.get("claimx", {}).get("inventory_table_path", ""),
            ),
            claimx_media_table_path=_get_config_value(
                "CLAIMX_MEDIA_TABLE_PATH",
                delta_config.get("claimx", {}).get("media_table_path", ""),
            ),
            claimx_tasks_table_path=_get_config_value(
                "CLAIMX_TASKS_TABLE_PATH",
                delta_config.get("claimx", {}).get("tasks_table_path", ""),
            ),
            claimx_task_templates_table_path=_get_config_value(
                "CLAIMX_TASK_TEMPLATES_TABLE_PATH",
                delta_config.get("claimx", {}).get("task_templates_table_path", ""),
            ),
            claimx_external_links_table_path=_get_config_value(
                "CLAIMX_EXTERNAL_LINKS_TABLE_PATH",
                delta_config.get("claimx", {}).get("external_links_table_path", ""),
            ),
            claimx_video_collab_table_path=_get_config_value(
                "CLAIMX_VIDEO_COLLAB_TABLE_PATH",
                delta_config.get("claimx", {}).get("video_collab_table_path", ""),
            ),
        )


def get_pipeline_config(config_path: Path | None = None) -> PipelineConfig:
    """Get pipeline configuration from config directory and environment.

    This is the main entry point for loading configuration.
    """
    return PipelineConfig.load_config(config_path)
