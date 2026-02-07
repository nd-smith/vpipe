"""
Pipeline configuration for hybrid Event Hub + Local Kafka setup.

Architecture:
    - Event Source (Event Hub or Eventhouse): Source of raw events
    - Local Kafka: Internal pipeline communication between workers

Workers:
    - EventIngesterWorker (Event Hub mode): Reads from Event Hub, writes to Local Kafka
    - KQLEventPoller (Eventhouse mode): Polls Eventhouse, writes to Local Kafka
    - DownloadWorker: Reads/writes Local Kafka only
    - ResultProcessor: Reads from Local Kafka only

Event Source Configuration:
    Set EVENT_SOURCE=eventhub (default) or EVENT_SOURCE=eventhouse
"""

import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

from config.config import KafkaConfig

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
    # This handles cases like "events_table_path: ${CLAIMX_EVENTS_TABLE_PATH}"
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
            f"Configuration file not found: {config_path}\n"
            f"Expected file: config/config.yaml"
        )

    # Load from single file
    from config.config import _expand_env_vars, load_yaml

    config_data = load_yaml(config_path)
    return _expand_env_vars(config_data)


class EventSourceType(str, Enum):
    """Type of event source for the pipeline."""

    EVENTHUB = "eventhub"
    EVENTHOUSE = "eventhouse"


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
    events_topic: str = "com.allstate.pcesdopodappv1.xact.events.raw"
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
            events_topic=os.getenv(
                "EVENTHUB_EVENTS_TOPIC", "com.allstate.pcesdopodappv1.xact.events.raw"
            ),
            consumer_group=os.getenv("EVENTHUB_CONSUMER_GROUP", "xact-event-ingester"),
            auto_offset_reset=os.getenv("EVENTHUB_AUTO_OFFSET_RESET", "earliest"),
        )

    def to_kafka_config(self) -> KafkaConfig:
        """Convert to KafkaConfig for use with BaseKafkaConsumer.

        Creates a hierarchical KafkaConfig matching the new config.yaml structure.
        Used only when Kafka protocol is needed (not AMQP transport).
        """
        # Build verisk domain config for Event Hub source
        verisk_config = {
            "topics": {
                "events": self.events_topic,
            },
            "consumer_group_prefix": (
                self.consumer_group.rsplit("-", 1)[0]
                if "-" in self.consumer_group
                else "verisk"
            ),
            "event_ingester": {
                "consumer": {
                    "group_id": self.consumer_group,
                }
            },
        }

        return KafkaConfig(
            bootstrap_servers=self.bootstrap_servers,
            security_protocol=self.security_protocol,
            sasl_mechanism=self.sasl_mechanism,
            sasl_plain_username=self.sasl_username,
            sasl_plain_password=self.sasl_password,
            consumer_defaults={
                "auto_offset_reset": self.auto_offset_reset,
            },
            verisk=verisk_config,
        )


# =============================================================================
# LocalKafkaConfig REMOVED - Kafka replaced with EventHub
# =============================================================================
# Internal pipeline communication now uses EventHub instead of local Kafka.
# EventHub configuration is defined per-domain in EventHubConfig (if needed).
# Domain-specific settings (topics, retry, storage) remain in KafkaConfig.
# =============================================================================


@dataclass
class EventhouseSourceConfig:
    """Configuration for Eventhouse as event source.

    Used when EVENT_SOURCE=eventhouse.
    """

    cluster_url: str
    database: str
    source_table: str = "Events"

    # Polling configuration
    poll_interval_seconds: int = 30
    batch_size: int = 1000

    # Query configuration
    query_timeout_seconds: int = 120

    # Deduplication configuration
    verisk_events_table_path: str = ""
    verisk_events_window_hours: int = 24
    eventhouse_query_window_hours: int = 1
    overlap_minutes: int = 5

    # Backfill configuration
    backfill_start_stamp: str | None = None
    backfill_stop_stamp: str | None = None
    bulk_backfill: bool = False

    # KQL start stamp for real-time mode
    kql_start_stamp: str | None = None

    @classmethod
    def load_config(cls, config_path: Path | None = None) -> "EventhouseSourceConfig":
        """Load Eventhouse configuration from config directory and environment variables.

        Configuration priority (highest to lowest):
        1. Environment variables
        2. Config files in config/ directory (under 'verisk_eventhouse:' key, fallback to 'eventhouse:')
        3. Dataclass defaults

        Optional env var overrides:
            VERISK_EVENTHOUSE_CLUSTER_URL: Kusto cluster URL
            VERISK_EVENTHOUSE_DATABASE: Database name
            VERISK_EVENTHOUSE_SOURCE_TABLE: KQL source table name (default: Events)
            EVENTHOUSE_SOURCE_TABLE: Legacy env var for source table (fallback)
            POLL_INTERVAL_SECONDS: Poll interval (default: 30)
            POLL_BATCH_SIZE: Max events per poll (default: 1000)
            EVENTHOUSE_QUERY_TIMEOUT: Query timeout (default: 120)
            VERISK_EVENTS_TABLE_PATH: Path to verisk_events Delta table
        """
        # Use default config directory if not specified
        resolved_path = config_path or DEFAULT_CONFIG_FILE

        # Load configuration data from config directory
        yaml_data = _load_config_data(resolved_path)

        eventhouse_data = yaml_data.get("verisk_eventhouse", {})
        poller_data = eventhouse_data.get("poller", {})
        dedup_data = eventhouse_data.get("dedup", {})

        cluster_url = _get_config_value(
            "EVENTHOUSE_CLUSTER_URL", eventhouse_data.get("cluster_url", "")
        )
        database = _get_config_value(
            "VERISK_EVENTHOUSE_DATABASE", eventhouse_data.get("database", "")
        )

        if not cluster_url:
            raise ValueError(
                "Eventhouse cluster_url is required. "
                "Set EVENTHOUSE_CLUSTER_URL env var or in config.yaml under 'verisk_eventhouse.cluster_url'"
            )
        if not database:
            raise ValueError(
                "Verisk Eventhouse database is required. "
                "Set VERISK_EVENTHOUSE_DATABASE env var or in config.yaml under 'verisk_eventhouse.database'"
            )

        bulk_backfill = _parse_bool_env(
            "VERISK_DEDUP_BULK_BACKFILL", poller_data.get("bulk_backfill", False)
        )

        return cls(
            cluster_url=cluster_url,
            database=database,
            source_table=_get_config_value(
                "VERISK_EVENTHOUSE_SOURCE_TABLE",
                poller_data.get("source_table", "Events"),
            ),
            poll_interval_seconds=int(
                os.getenv(
                    "POLL_INTERVAL_SECONDS",
                    str(poller_data.get("poll_interval_seconds", 30)),
                )
            ),
            batch_size=int(
                os.getenv("POLL_BATCH_SIZE", str(poller_data.get("batch_size", 1000)))
            ),
            query_timeout_seconds=int(
                os.getenv(
                    "EVENTHOUSE_QUERY_TIMEOUT",
                    str(eventhouse_data.get("query_timeout_seconds", 120)),
                )
            ),
            verisk_events_table_path=os.getenv(
                "VERISK_EVENTS_TABLE_PATH", poller_data.get("events_table_path", "")
            ),
            verisk_events_window_hours=int(
                os.getenv(
                    "DEDUP_VERISK_EVENTS_WINDOW_HOURS",
                    str(dedup_data.get("verisk_events_window_hours", 24)),
                )
            ),
            eventhouse_query_window_hours=int(
                os.getenv(
                    "DEDUP_EVENTHOUSE_WINDOW_HOURS",
                    str(dedup_data.get("eventhouse_query_window_hours", 1)),
                )
            ),
            overlap_minutes=int(
                os.getenv(
                    "DEDUP_OVERLAP_MINUTES", str(dedup_data.get("overlap_minutes", 5))
                )
            ),
            # Backfill configuration
            backfill_start_stamp=os.getenv(
                "DEDUP_BACKFILL_START_TIMESTAMP",
                poller_data.get("backfill_start_stamp"),
            ),
            backfill_stop_stamp=os.getenv(
                "DEDUP_BACKFILL_STOP_TIMESTAMP", poller_data.get("backfill_stop_stamp")
            ),
            bulk_backfill=bulk_backfill,
            # KQL start stamp for real-time mode
            kql_start_stamp=os.getenv(
                "DEDUP_KQL_START_TIMESTAMP", dedup_data.get("kql_start_stamp")
            ),
        )


@dataclass
class ClaimXEventhouseSourceConfig:
    """Configuration for ClaimX Eventhouse poller.

    Used for polling ClaimX events from Eventhouse.
    """

    cluster_url: str
    database: str
    source_table: str = "ClaimXEvents"

    # Polling configuration
    poll_interval_seconds: int = 30
    batch_size: int = 1000

    # Query configuration
    query_timeout_seconds: int = 120

    # Deduplication configuration
    claimx_events_table_path: str = ""
    claimx_events_window_hours: int = 24
    eventhouse_query_window_hours: int = 1
    overlap_minutes: int = 5

    # Kafka topic
    events_topic: str = "com.allstate.pcesdopodappv1.claimx.events.raw"

    # Backfill configuration
    backfill_start_stamp: str | None = None
    backfill_stop_stamp: str | None = None
    bulk_backfill: bool = False

    # KQL start stamp for real-time mode
    kql_start_stamp: str | None = None

    @classmethod
    def load_config(
        cls, config_path: Path | None = None
    ) -> "ClaimXEventhouseSourceConfig":
        """Load ClaimX Eventhouse configuration from config directory and environment variables.

        Configuration priority (highest to lowest):
        1. Environment variables
        2. Config files in config/ directory (under 'claimx_eventhouse:' key)
        3. Dataclass defaults

        Optional env var overrides:
            CLAIMX_EVENTHOUSE_CLUSTER_URL: Kusto cluster URL
            CLAIMX_EVENTHOUSE_DATABASE: Database name
            CLAIMX_EVENTHOUSE_SOURCE_TABLE: Table name (default: ClaimXEvents)
            CLAIMX_POLL_INTERVAL_SECONDS: Poll interval (default: 30)
            CLAIMX_POLL_BATCH_SIZE: Max events per poll (default: 1000)
            CLAIMX_EVENTHOUSE_QUERY_TIMEOUT: Query timeout (default: 120)
            CLAIMX_EVENTS_TABLE_PATH: Path to claimx_events Delta table
            CLAIMX_EVENTS_TOPIC: Kafka topic (default: com.allstate.pcesdopodappv1.claimx.events.raw)
        """
        # Use default config directory if not specified
        resolved_path = config_path or DEFAULT_CONFIG_FILE

        # Load configuration data from config directory
        yaml_data = _load_config_data(resolved_path)

        claimx_eventhouse_data = yaml_data.get("claimx_eventhouse", {})
        poller_data = claimx_eventhouse_data.get("poller", {})
        dedup_data = claimx_eventhouse_data.get("dedup", {})

        cluster_url = _get_config_value(
            "EVENTHOUSE_CLUSTER_URL", claimx_eventhouse_data.get("cluster_url", "")
        )
        database = _get_config_value(
            "CLAIMX_EVENTHOUSE_DATABASE", claimx_eventhouse_data.get("database", "")
        )

        if not cluster_url:
            raise ValueError(
                "Eventhouse cluster_url is required. "
                "Set EVENTHOUSE_CLUSTER_URL env var or in config.yaml under 'claimx_eventhouse.cluster_url'"
            )
        if not database:
            raise ValueError(
                "ClaimX Eventhouse database is required. "
                "Set CLAIMX_EVENTHOUSE_DATABASE env var or in config.yaml under 'claimx_eventhouse.database'"
            )

        bulk_backfill = _parse_bool_env(
            "CLAIMX_DEDUP_BULK_BACKFILL", poller_data.get("bulk_backfill", False)
        )

        return cls(
            cluster_url=cluster_url,
            database=database,
            source_table=os.getenv(
                "CLAIMX_EVENTHOUSE_SOURCE_TABLE",
                poller_data.get("source_table", "ClaimXEvents"),
            ),
            poll_interval_seconds=int(
                os.getenv(
                    "CLAIMX_POLL_INTERVAL_SECONDS",
                    str(poller_data.get("poll_interval_seconds", 30)),
                )
            ),
            batch_size=int(
                os.getenv(
                    "CLAIMX_POLL_BATCH_SIZE", str(poller_data.get("batch_size", 1000))
                )
            ),
            query_timeout_seconds=int(
                os.getenv(
                    "CLAIMX_EVENTHOUSE_QUERY_TIMEOUT",
                    str(claimx_eventhouse_data.get("query_timeout_seconds", 120)),
                )
            ),
            claimx_events_table_path=_get_config_value(
                "CLAIMX_EVENTS_TABLE_PATH", poller_data.get("events_table_path", "")
            ),
            claimx_events_window_hours=int(
                os.getenv(
                    "CLAIMX_DEDUP_EVENTS_WINDOW_HOURS",
                    str(dedup_data.get("claimx_events_window_hours", 24)),
                )
            ),
            eventhouse_query_window_hours=int(
                os.getenv(
                    "CLAIMX_DEDUP_EVENTHOUSE_WINDOW_HOURS",
                    str(dedup_data.get("eventhouse_query_window_hours", 1)),
                )
            ),
            overlap_minutes=int(
                os.getenv(
                    "CLAIMX_DEDUP_OVERLAP_MINUTES",
                    str(dedup_data.get("overlap_minutes", 5)),
                )
            ),
            events_topic=os.getenv(
                "CLAIMX_EVENTS_TOPIC",
                poller_data.get(
                    "events_topic", "com.allstate.pcesdopodappv1.claimx.events.raw"
                ),
            ),
            # Backfill configuration
            backfill_start_stamp=os.getenv(
                "CLAIMX_DEDUP_BACKFILL_START_TIMESTAMP",
                poller_data.get("backfill_start_stamp"),
            ),
            backfill_stop_stamp=os.getenv(
                "CLAIMX_DEDUP_BACKFILL_STOP_TIMESTAMP",
                poller_data.get("backfill_stop_stamp"),
            ),
            bulk_backfill=bulk_backfill,
            # KQL start stamp for real-time mode
            kql_start_stamp=os.getenv(
                "CLAIMX_DEDUP_KQL_START_TIMESTAMP", dedup_data.get("kql_start_stamp")
            ),
        )


@dataclass
class PipelineConfig:
    """Complete pipeline configuration.

    Combines event source (EventHub or Eventhouse) with EventHub transport.
    Kafka has been removed - EventHub is now used for all pipeline communication.
    """

    # Event source type (eventhub or eventhouse)
    event_source: EventSourceType

    # Event Hub config (only populated if event_source == eventhub)
    eventhub: EventHubConfig | None = None

    # Eventhouse config (only populated if event_source == eventhouse)
    verisk_eventhouse: EventhouseSourceConfig | None = None

    # ClaimX Eventhouse config (optional)
    claimx_eventhouse: ClaimXEventhouseSourceConfig | None = None

    # Domain identifier for OneLake routing (e.g., "verisk", "claimx")
    domain: str = "verisk"

    # Delta Lake configuration
    enable_delta_writes: bool = True
    events_table_path: str = ""
    inventory_table_path: str = ""
    failed_table_path: str = ""  # Optional: for tracking permanent failures

    # ClaimX entity table paths
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

        The event_source field in config files (or EVENT_SOURCE env var) determines
        which source is used:
        - eventhub: Use Azure Event Hub via Kafka protocol
        - eventhouse: Poll Microsoft Fabric Eventhouse
        """
        # Use default config directory if not specified
        resolved_path = config_path or DEFAULT_CONFIG_FILE

        # Load configuration data from config directory
        yaml_data = _load_config_data(resolved_path)

        # Get event source from config files first, then env var override
        source_str = os.getenv(
            "EVENT_SOURCE", yaml_data.get("event_source", "eventhub")
        ).lower()

        try:
            event_source = EventSourceType(source_str)
        except ValueError:
            raise ValueError(
                f"Invalid event_source '{source_str}'. Must be 'eventhub' or 'eventhouse'"
            ) from None

        eventhub_config = None
        eventhouse_config = None
        claimx_eventhouse_config = None

        if event_source == EventSourceType.EVENTHUB:
            eventhub_config = EventHubConfig.from_env()
        else:
            eventhouse_config = EventhouseSourceConfig.load_config(resolved_path)

        # Optionally load ClaimX Eventhouse config if configured
        # Check for config section or env vars
        # Note: CLAIMX_EVENTHOUSE_DATABASE indicates intent to use ClaimX;
        # EVENTHOUSE_CLUSTER_URL serves as a shared fallback for cluster_url
        has_claimx_config = (
            "claimx_eventhouse" in yaml_data
            or os.getenv("CLAIMX_EVENTHOUSE_CLUSTER_URL")
            or os.getenv("CLAIMX_EVENTHOUSE_DATABASE")
            or os.getenv("CLAIMX_EVENTS_TABLE_PATH")
        )
        if has_claimx_config:
            try:
                claimx_eventhouse_config = ClaimXEventhouseSourceConfig.load_config(
                    resolved_path
                )
            except ValueError:
                # ClaimX config not fully specified, leave as None
                pass

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
            event_source=event_source,
            eventhub=eventhub_config,
            verisk_eventhouse=eventhouse_config,
            claimx_eventhouse=claimx_eventhouse_config,
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

    @property
    def is_eventhub_source(self) -> bool:
        """Check if using Event Hub as source."""
        return self.event_source == EventSourceType.EVENTHUB

    @property
    def is_eventhouse_source(self) -> bool:
        """Check if using Eventhouse as source."""
        return self.event_source == EventSourceType.EVENTHOUSE


def get_pipeline_config(config_path: Path | None = None) -> PipelineConfig:
    """Get pipeline configuration from config directory and environment.

    This is the main entry point for loading configuration.
    """
    return PipelineConfig.load_config(config_path)


def get_event_source_type(config_path: Path | None = None) -> EventSourceType:
    """Get the configured event source type.

    Quick check without loading full config. Reads from config directory first,
    then checks EVENT_SOURCE env var for override.
    """
    # Use default config directory if not specified
    resolved_path = config_path or DEFAULT_CONFIG_FILE

    # Load configuration data from config directory
    yaml_data = _load_config_data(resolved_path)
    yaml_source = yaml_data.get("event_source", "eventhub")

    source_str = os.getenv("EVENT_SOURCE", yaml_source).lower()
    return EventSourceType(source_str)
