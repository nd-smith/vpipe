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
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from config.config import KafkaConfig

# Default config file: config/config.yaml in src/ directory
DEFAULT_CONFIG_FILE = Path(__file__).parent.parent / "config" / "config.yaml"


def _load_config_data(config_path: Path) -> Dict[str, Any]:
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
            f"Configuration file not found: {config_path}\n" f"Expected file: config/config.yaml"
        )

    # Load from single file
    from config.config import load_yaml, _expand_env_vars

    config_data = load_yaml(config_path)
    return _expand_env_vars(config_data)


class EventSourceType(str, Enum):
    """Type of event source for the pipeline."""

    EVENTHUB = "eventhub"
    EVENTHOUSE = "eventhouse"


@dataclass
class EventHubConfig:
    """Configuration for Azure Event Hub connection (Kafka-compatible).

    Event Hubs uses Kafka protocol with SASL_SSL + OAUTHBEARER or SASL_PLAIN.
    Connection string format for SASL_PLAIN:
        Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=...
    """

    bootstrap_servers: str
    security_protocol: str = "SASL_SSL"
    sasl_mechanism: str = "PLAIN"  # Event Hubs uses PLAIN with connection string
    sasl_username: str = "$ConnectionString"
    sasl_password: str = ""  # Full connection string

    # Consumer settings
    events_topic: str = "com.allstate.pcesdopodappv1.xact.events.raw"
    consumer_group: str = "xact-event-ingester"
    auto_offset_reset: str = "earliest"

    @classmethod
    def from_env(cls) -> "EventHubConfig":
        """Load Event Hub configuration from environment variables.

        Required:
            EVENTHUB_BOOTSTRAP_SERVERS: Event Hub namespace (e.g., namespace.servicebus.windows.net:9093)
            EVENTHUB_CONNECTION_STRING: Full Event Hub connection string

        Optional:
            EVENTHUB_EVENTS_TOPIC: Topic name (default: com.allstate.pcesdopodappv1.xact.events.raw)
            EVENTHUB_CONSUMER_GROUP: Consumer group (default: xact-event-ingester)
        """
        bootstrap_servers = os.getenv("EVENTHUB_BOOTSTRAP_SERVERS")
        connection_string = os.getenv("EVENTHUB_CONNECTION_STRING")

        if not bootstrap_servers:
            raise ValueError("EVENTHUB_BOOTSTRAP_SERVERS environment variable is required")
        if not connection_string:
            raise ValueError("EVENTHUB_CONNECTION_STRING environment variable is required")

        return cls(
            bootstrap_servers=bootstrap_servers,
            sasl_password=connection_string,
            events_topic=os.getenv("EVENTHUB_EVENTS_TOPIC", "com.allstate.pcesdopodappv1.xact.events.raw"),
            consumer_group=os.getenv("EVENTHUB_CONSUMER_GROUP", "xact-event-ingester"),
            auto_offset_reset=os.getenv("EVENTHUB_AUTO_OFFSET_RESET", "earliest"),
        )

    def to_kafka_config(self) -> KafkaConfig:
        """Convert to KafkaConfig for use with BaseKafkaConsumer.

        Creates a hierarchical KafkaConfig matching the new config.yaml structure.
        """
        # Build xact domain config for Event Hub source
        xact_config = {
            "topics": {
                "events": self.events_topic,
            },
            "consumer_group_prefix": (
                self.consumer_group.rsplit("-", 1)[0] if "-" in self.consumer_group else "xact"
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
            xact=xact_config,
        )


@dataclass
class LocalKafkaConfig:
    """Configuration for local Kafka instance (internal pipeline communication).

    Uses PLAINTEXT for local development/testing.
    """

    bootstrap_servers: str = "localhost:9092"
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str = ""  # Not used for PLAINTEXT
    sasl_plain_username: str = ""  # For SASL authentication (e.g., Event Hub)
    sasl_plain_password: str = ""  # For SASL authentication (e.g., Event Hub)

    # Topics for internal pipeline
    events_topic: str = "com.allstate.pcesdopodappv1.xact.events.raw"  # Raw events from source
    downloads_pending_topic: str = "com.allstate.pcesdopodappv1.xact.downloads.pending"
    downloads_cached_topic: str = "com.allstate.pcesdopodappv1.xact.downloads.cached"
    downloads_results_topic: str = "com.allstate.pcesdopodappv1.xact.downloads.results"
    dlq_topic: str = "com.allstate.pcesdopodappv1.xact.downloads.dlq"

    # Consumer group prefix
    consumer_group_prefix: str = "xact"

    # Retry configuration (delays in seconds)
    retry_delays: List[int] = field(default_factory=lambda: [300, 600, 1200, 2400])
    max_retries: int = 4

    # Storage
    onelake_base_path: str = ""
    onelake_domain_paths: Dict[str, str] = field(default_factory=dict)

    # Cache directory
    cache_dir: str = "/tmp/kafka_pipeline_cache"

    # Delta events writer settings
    delta_events_batch_size: int = 1000

    # ClaimX domain config (loaded from yaml)
    claimx_config: Dict[str, Any] = field(default_factory=dict)

    # XACT domain config (loaded from yaml) - preserves full yaml structure
    xact_config: Dict[str, Any] = field(default_factory=dict)

    # ClaimX API settings
    claimx_api_url: str = ""
    claimx_api_token: str = ""
    claimx_api_timeout_seconds: int = 30
    claimx_api_concurrency: int = 20

    @classmethod
    def load_config(cls, config_path: Optional[Path] = None) -> "LocalKafkaConfig":
        """Load local Kafka configuration from config directory and environment variables.

        Configuration priority (highest to lowest):
        1. Environment variables
        2. Config files in config/ directory
        3. Dataclass defaults

        Optional env vars (all have defaults):
            LOCAL_KAFKA_BOOTSTRAP_SERVERS: Kafka broker (default: localhost:9092)
            LOCAL_KAFKA_SECURITY_PROTOCOL: Protocol (default: PLAINTEXT)
            KAFKA_DOWNLOADS_PENDING_TOPIC: Pending topic (default: com.allstate.pcesdopodappv1.xact.downloads.pending)
            KAFKA_DOWNLOADS_RESULTS_TOPIC: Results topic (default: com.allstate.pcesdopodappv1.xact.downloads.results)
            KAFKA_DLQ_TOPIC: DLQ topic (default: com.allstate.pcesdopodappv1.xact.downloads.dlq)
            ONELAKE_BASE_PATH: OneLake path for uploads (fallback)
            ONELAKE_XACT_PATH: OneLake path for xact domain
            ONELAKE_CLAIMX_PATH: OneLake path for claimx domain
            DELTA_EVENTS_BATCH_SIZE: Events per batch for Delta writes (default: 1000)
        """
        resolved_path = config_path or DEFAULT_CONFIG_FILE
        yaml_data = _load_config_data(resolved_path)
        kafka_data = yaml_data.get("kafka", {})
        connection_data = kafka_data.get("connection", kafka_data)
        xact_config = kafka_data.get("xact", {})
        topics_data = xact_config.get("topics", {})
        claimx_config = kafka_data.get("claimx", {})
        claimx_root = yaml_data.get("claimx", {})
        claimx_api_data = claimx_root.get("api", {})
        retry_delays_default = xact_config.get(
            "retry_delays", kafka_data.get("retry_delays", [300, 600, 1200, 2400])
        )
        retry_delays_str = os.getenv("RETRY_DELAYS", ",".join(str(d) for d in retry_delays_default))
        retry_delays = [int(d.strip()) for d in retry_delays_str.split(",")]

        # Build storage config from multiple locations (matching config.py logic)
        # Priority: kafka.storage > root storage > flat kafka structure
        storage_data: Dict[str, Any] = {}

        # Start with flat kafka structure as base (lowest priority)
        for key in ["onelake_base_path", "onelake_domain_paths", "cache_dir"]:
            if key in kafka_data:
                storage_data[key] = kafka_data[key]

        # Merge root-level storage section (medium priority)
        root_storage = yaml_data.get("storage", {})
        if root_storage:
            storage_data.update(root_storage)

        # Merge kafka.storage section (highest priority)
        kafka_storage = kafka_data.get("storage", {})
        if kafka_storage:
            storage_data.update(kafka_storage)

        # Build domain paths from merged storage config and environment variables
        onelake_domain_paths: Dict[str, str] = storage_data.get("onelake_domain_paths", {}).copy()
        if os.getenv("ONELAKE_XACT_PATH"):
            onelake_domain_paths["xact"] = os.getenv("ONELAKE_XACT_PATH", "")
        if os.getenv("ONELAKE_CLAIMX_PATH"):
            onelake_domain_paths["claimx"] = os.getenv("ONELAKE_CLAIMX_PATH", "")

        return cls(
            bootstrap_servers=os.getenv(
                "LOCAL_KAFKA_BOOTSTRAP_SERVERS",
                connection_data.get("bootstrap_servers", "localhost:9092"),
            ),
            security_protocol=os.getenv(
                "LOCAL_KAFKA_SECURITY_PROTOCOL",
                connection_data.get("security_protocol", "PLAINTEXT"),
            ),
            sasl_mechanism=os.getenv(
                "LOCAL_KAFKA_SASL_MECHANISM", connection_data.get("sasl_mechanism", "")
            ),
            sasl_plain_username=os.getenv(
                "LOCAL_KAFKA_SASL_USERNAME", connection_data.get("sasl_plain_username", "")
            ),
            sasl_plain_password=os.getenv(
                "LOCAL_KAFKA_SASL_PASSWORD", connection_data.get("sasl_plain_password", "")
            ),
            # Topics: check nested kafka.xact.topics first, then flat kafka.*, then defaults
            events_topic=os.getenv(
                "KAFKA_EVENTS_TOPIC",
                topics_data.get("events", kafka_data.get("events_topic", "com.allstate.pcesdopodappv1.xact.events.raw")),
            ),
            downloads_pending_topic=os.getenv(
                "KAFKA_DOWNLOADS_PENDING_TOPIC",
                topics_data.get(
                    "downloads_pending",
                    kafka_data.get("downloads_pending_topic", "com.allstate.pcesdopodappv1.xact.downloads.pending"),
                ),
            ),
            downloads_cached_topic=os.getenv(
                "KAFKA_DOWNLOADS_CACHED_TOPIC",
                topics_data.get(
                    "downloads_cached",
                    kafka_data.get("downloads_cached_topic", "com.allstate.pcesdopodappv1.xact.downloads.cached"),
                ),
            ),
            downloads_results_topic=os.getenv(
                "KAFKA_DOWNLOADS_RESULTS_TOPIC",
                topics_data.get(
                    "downloads_results",
                    kafka_data.get("downloads_results_topic", "com.allstate.pcesdopodappv1.xact.downloads.results"),
                ),
            ),
            dlq_topic=os.getenv(
                "KAFKA_DLQ_TOPIC",
                topics_data.get("dlq", kafka_data.get("dlq_topic", "com.allstate.pcesdopodappv1.xact.downloads.dlq")),
            ),
            consumer_group_prefix=os.getenv(
                "KAFKA_CONSUMER_GROUP_PREFIX",
                xact_config.get(
                    "consumer_group_prefix", kafka_data.get("consumer_group_prefix", "xact")
                ),
            ),
            retry_delays=retry_delays,
            max_retries=int(
                os.getenv(
                    "MAX_RETRIES",
                    str(xact_config.get("max_retries", kafka_data.get("max_retries", 4))),
                )
            ),
            onelake_base_path=os.getenv(
                "ONELAKE_BASE_PATH", storage_data.get("onelake_base_path", "")
            ),
            onelake_domain_paths=onelake_domain_paths,
            cache_dir=os.getenv(
                "CACHE_DIR", storage_data.get("cache_dir", "/tmp/kafka_pipeline_cache")
            ),
            delta_events_batch_size=int(
                os.getenv(
                    "DELTA_EVENTS_BATCH_SIZE", str(kafka_data.get("delta_events_batch_size", 1000))
                )
            ),
            claimx_config=claimx_config,
            xact_config=xact_config,
            # ClaimX API settings (loaded from root claimx.api section)
            claimx_api_url=os.getenv("CLAIMX_API_BASE_PATH") or os.getenv("CLAIMX_API_URL") or claimx_api_data.get("base_url", ""),
            claimx_api_token=os.getenv("CLAIMX_API_TOKEN", claimx_api_data.get("token", "")),
            claimx_api_timeout_seconds=int(
                os.getenv(
                    "CLAIMX_API_TIMEOUT_SECONDS", str(claimx_api_data.get("timeout_seconds", 30))
                )
            ),
            claimx_api_concurrency=int(
                os.getenv("CLAIMX_API_CONCURRENCY", str(claimx_api_data.get("max_concurrent", 20)))
            ),
        )

    def to_kafka_config(self) -> KafkaConfig:
        """Convert to KafkaConfig for use with workers.

        Creates a hierarchical KafkaConfig matching the new config.yaml structure.
        Uses full yaml config for both xact and claimx domains.
        Flat fields provide fallbacks for topics if not in yaml.
        """
        # Start with full yaml xact config, ensure topics have fallbacks from flat fields
        xact_config = self.xact_config.copy() if self.xact_config else {}

        # Ensure topics exist with fallbacks from flat fields
        if "topics" not in xact_config:
            xact_config["topics"] = {}
        topics = xact_config["topics"]
        topics.setdefault("events", self.events_topic)
        topics.setdefault("downloads_pending", self.downloads_pending_topic)
        topics.setdefault("downloads_cached", self.downloads_cached_topic)
        topics.setdefault("downloads_results", self.downloads_results_topic)
        topics.setdefault("dlq", self.dlq_topic)

        # Ensure other required fields have fallbacks
        xact_config.setdefault("consumer_group_prefix", self.consumer_group_prefix)
        xact_config.setdefault("retry_delays", self.retry_delays)
        xact_config.setdefault("max_retries", self.max_retries)

        # Ensure delta_events_writer has processing defaults
        if "delta_events_writer" not in xact_config:
            xact_config["delta_events_writer"] = {}
        if "processing" not in xact_config["delta_events_writer"]:
            xact_config["delta_events_writer"]["processing"] = {}
        delta_processing = xact_config["delta_events_writer"]["processing"]
        delta_processing.setdefault("batch_size", self.delta_events_batch_size)
        delta_processing.setdefault("retry_delays", self.retry_delays)
        delta_processing.setdefault("max_retries", self.max_retries)
        delta_processing.setdefault("retry_topic_prefix", "com.allstate.pcesdopodappv1.delta-events.retry")
        delta_processing.setdefault("dlq_topic", "com.allstate.pcesdopodappv1.delta-events.dlq")

        return KafkaConfig(
            bootstrap_servers=self.bootstrap_servers,
            security_protocol=self.security_protocol,
            sasl_mechanism=self.sasl_mechanism,
            sasl_plain_username=self.sasl_plain_username,
            sasl_plain_password=self.sasl_plain_password,
            xact=xact_config,
            claimx=self.claimx_config,
            onelake_base_path=self.onelake_base_path,
            onelake_domain_paths=self.onelake_domain_paths,
            cache_dir=self.cache_dir,
            # ClaimX API settings
            claimx_api_url=self.claimx_api_url,
            claimx_api_token=self.claimx_api_token,
            claimx_api_timeout_seconds=self.claimx_api_timeout_seconds,
            claimx_api_concurrency=self.claimx_api_concurrency,
        )


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
    xact_events_table_path: str = ""
    xact_events_window_hours: int = 24
    eventhouse_query_window_hours: int = 1
    overlap_minutes: int = 5

    # Backfill configuration
    backfill_start_stamp: Optional[str] = None
    backfill_stop_stamp: Optional[str] = None
    bulk_backfill: bool = False

    # KQL start stamp for real-time mode
    kql_start_stamp: Optional[str] = None

    @classmethod
    def load_config(cls, config_path: Optional[Path] = None) -> "EventhouseSourceConfig":
        """Load Eventhouse configuration from config directory and environment variables.

        Configuration priority (highest to lowest):
        1. Environment variables
        2. Config files in config/ directory (under 'xact_eventhouse:' key, fallback to 'eventhouse:')
        3. Dataclass defaults

        Optional env var overrides:
            XACT_EVENTHOUSE_CLUSTER_URL: Kusto cluster URL
            XACT_EVENTHOUSE_DATABASE: Database name
            XACT_EVENTHOUSE_SOURCE_TABLE: KQL source table name (default: Events)
            EVENTHOUSE_SOURCE_TABLE: Legacy env var for source table (fallback)
            POLL_INTERVAL_SECONDS: Poll interval (default: 30)
            POLL_BATCH_SIZE: Max events per poll (default: 1000)
            EVENTHOUSE_QUERY_TIMEOUT: Query timeout (default: 120)
            XACT_EVENTS_TABLE_PATH: Path to xact_events Delta table
        """
        # Use default config directory if not specified
        resolved_path = config_path or DEFAULT_CONFIG_FILE

        # Load configuration data from config directory
        yaml_data = _load_config_data(resolved_path)

        # Check for xact_eventhouse first, fallback to legacy eventhouse
        eventhouse_data = yaml_data.get("xact_eventhouse", yaml_data.get("eventhouse", {}))
        poller_data = eventhouse_data.get("poller", {})
        dedup_data = eventhouse_data.get("dedup", {})

        cluster_url = os.getenv(
            "XACT_EVENTHOUSE_CLUSTER_URL",
            os.getenv("EVENTHOUSE_CLUSTER_URL", eventhouse_data.get("cluster_url", "")),
        )
        database = os.getenv(
            "XACT_EVENTHOUSE_DATABASE",
            os.getenv("EVENTHOUSE_DATABASE", eventhouse_data.get("database", "")),
        )

        if not cluster_url:
            raise ValueError(
                "Xact Eventhouse cluster_url is required. "
                "Set in config.yaml under 'xact_eventhouse:' (or 'eventhouse:') "
                "or via XACT_EVENTHOUSE_CLUSTER_URL env var."
            )
        if not database:
            raise ValueError(
                "Xact Eventhouse database is required. "
                "Set in config.yaml under 'xact_eventhouse:' (or 'eventhouse:') "
                "or via XACT_EVENTHOUSE_DATABASE env var."
            )

        # Parse bulk_backfill boolean
        bulk_backfill_str = os.getenv(
            "DEDUP_BULK_BACKFILL", str(poller_data.get("bulk_backfill", False))
        ).lower()
        bulk_backfill = bulk_backfill_str in ("true", "1", "yes")

        return cls(
            cluster_url=cluster_url,
            database=database,
            source_table=os.getenv(
                "XACT_EVENTHOUSE_SOURCE_TABLE",
                os.getenv("EVENTHOUSE_SOURCE_TABLE", poller_data.get("source_table", "Events")),
            ),
            poll_interval_seconds=int(
                os.getenv(
                    "POLL_INTERVAL_SECONDS", str(poller_data.get("poll_interval_seconds", 30))
                )
            ),
            batch_size=int(os.getenv("POLL_BATCH_SIZE", str(poller_data.get("batch_size", 1000)))),
            query_timeout_seconds=int(
                os.getenv(
                    "EVENTHOUSE_QUERY_TIMEOUT",
                    str(eventhouse_data.get("query_timeout_seconds", 120)),
                )
            ),
            xact_events_table_path=os.getenv(
                "XACT_EVENTS_TABLE_PATH", poller_data.get("events_table_path", "")
            ),
            xact_events_window_hours=int(
                os.getenv(
                    "DEDUP_XACT_EVENTS_WINDOW_HOURS",
                    str(dedup_data.get("xact_events_window_hours", 24)),
                )
            ),
            eventhouse_query_window_hours=int(
                os.getenv(
                    "DEDUP_EVENTHOUSE_WINDOW_HOURS",
                    str(dedup_data.get("eventhouse_query_window_hours", 1)),
                )
            ),
            overlap_minutes=int(
                os.getenv("DEDUP_OVERLAP_MINUTES", str(dedup_data.get("overlap_minutes", 5)))
            ),
            # Backfill configuration
            backfill_start_stamp=os.getenv(
                "DEDUP_BACKFILL_START_TIMESTAMP", poller_data.get("backfill_start_stamp")
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
    backfill_start_stamp: Optional[str] = None
    backfill_stop_stamp: Optional[str] = None
    bulk_backfill: bool = False

    # KQL start stamp for real-time mode
    kql_start_stamp: Optional[str] = None

    @classmethod
    def load_config(cls, config_path: Optional[Path] = None) -> "ClaimXEventhouseSourceConfig":
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

        # Get cluster_url with fallback to shared EVENTHOUSE_CLUSTER_URL
        # Priority: CLAIMX_EVENTHOUSE_CLUSTER_URL > EVENTHOUSE_CLUSTER_URL > YAML value
        cluster_url = os.getenv(
            "CLAIMX_EVENTHOUSE_CLUSTER_URL",
            os.getenv("EVENTHOUSE_CLUSTER_URL", claimx_eventhouse_data.get("cluster_url", "")),
        )
        database = os.getenv(
            "CLAIMX_EVENTHOUSE_DATABASE", claimx_eventhouse_data.get("database", "")
        )

        if not cluster_url:
            raise ValueError(
                "ClaimX Eventhouse cluster_url is required. "
                "Set via CLAIMX_EVENTHOUSE_CLUSTER_URL or EVENTHOUSE_CLUSTER_URL env var, "
                "or in config.yaml under 'claimx_eventhouse:'."
            )
        if not database:
            raise ValueError(
                "ClaimX Eventhouse database is required. "
                "Set in config.yaml under 'claimx_eventhouse:' or via CLAIMX_EVENTHOUSE_DATABASE env var."
            )

        # Parse bulk_backfill boolean
        bulk_backfill_str = os.getenv(
            "CLAIMX_DEDUP_BULK_BACKFILL", str(poller_data.get("bulk_backfill", False))
        ).lower()
        bulk_backfill = bulk_backfill_str in ("true", "1", "yes")

        return cls(
            cluster_url=cluster_url,
            database=database,
            source_table=os.getenv(
                "CLAIMX_EVENTHOUSE_SOURCE_TABLE", poller_data.get("source_table", "ClaimXEvents")
            ),
            poll_interval_seconds=int(
                os.getenv(
                    "CLAIMX_POLL_INTERVAL_SECONDS",
                    str(poller_data.get("poll_interval_seconds", 30)),
                )
            ),
            batch_size=int(
                os.getenv("CLAIMX_POLL_BATCH_SIZE", str(poller_data.get("batch_size", 1000)))
            ),
            query_timeout_seconds=int(
                os.getenv(
                    "CLAIMX_EVENTHOUSE_QUERY_TIMEOUT",
                    str(claimx_eventhouse_data.get("query_timeout_seconds", 120)),
                )
            ),
            claimx_events_table_path=(
                os.getenv("CLAIMX_EVENTS_TABLE_PATH")
                or os.getenv("CLAIMX_DELTA_EVENTS_TABLE")
                or poller_data.get("events_table_path", "")
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
                os.getenv("CLAIMX_DEDUP_OVERLAP_MINUTES", str(dedup_data.get("overlap_minutes", 5)))
            ),
            events_topic=os.getenv(
                "CLAIMX_EVENTS_TOPIC", poller_data.get("events_topic", "com.allstate.pcesdopodappv1.claimx.events.raw")
            ),
            # Backfill configuration
            backfill_start_stamp=os.getenv(
                "CLAIMX_DEDUP_BACKFILL_START_TIMESTAMP", poller_data.get("backfill_start_stamp")
            ),
            backfill_stop_stamp=os.getenv(
                "CLAIMX_DEDUP_BACKFILL_STOP_TIMESTAMP", poller_data.get("backfill_stop_stamp")
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

    Combines event source (Event Hub or Eventhouse) and Local Kafka configurations.
    """

    # Event source type (eventhub or eventhouse)
    event_source: EventSourceType

    # Event Hub config (only populated if event_source == eventhub)
    eventhub: Optional[EventHubConfig] = None

    # Eventhouse config (only populated if event_source == eventhouse)
    # This is the Xact domain Eventhouse config (legacy name: eventhouse, new name: xact_eventhouse)
    xact_eventhouse: Optional[EventhouseSourceConfig] = None

    # ClaimX Eventhouse config (optional, can run alongside xact)
    claimx_eventhouse: Optional[ClaimXEventhouseSourceConfig] = None

    # Local Kafka for internal pipeline communication
    local_kafka: LocalKafkaConfig = field(default_factory=LocalKafkaConfig)

    # Domain identifier for OneLake routing (e.g., "xact", "claimx")
    domain: str = "xact"

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

    # Simulation mode configuration (optional, None if not configured)
    simulation: Optional["SimulationConfig"] = None

    @classmethod
    def load_config(cls, config_path: Optional[Path] = None) -> "PipelineConfig":
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
        source_str = os.getenv("EVENT_SOURCE", yaml_data.get("event_source", "eventhub")).lower()

        try:
            event_source = EventSourceType(source_str)
        except ValueError:
            raise ValueError(
                f"Invalid event_source '{source_str}'. Must be 'eventhub' or 'eventhouse'"
            )

        local_kafka = LocalKafkaConfig.load_config(resolved_path)

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
                claimx_eventhouse_config = ClaimXEventhouseSourceConfig.load_config(resolved_path)
            except ValueError:
                # ClaimX config not fully specified, leave as None
                pass

        # Load delta configuration from yaml with env var override
        delta_config = yaml_data.get("delta", {})
        domain = os.getenv("PIPELINE_DOMAIN", "xact")
        domain_delta_config = delta_config.get(domain, {})

        # Enable delta writes: env var > yaml > default True
        enable_delta_writes_str = os.getenv("ENABLE_DELTA_WRITES")
        if enable_delta_writes_str is not None:
            enable_delta_writes = enable_delta_writes_str.lower() == "true"
        else:
            enable_delta_writes = delta_config.get("enable_writes", True)

        # Load simulation configuration if present
        simulation_config = None
        if "simulation" in yaml_data or os.getenv("SIMULATION_MODE"):
            try:
                from kafka_pipeline.simulation import SimulationConfig

                simulation_config = SimulationConfig.from_config_file(resolved_path)
            except ImportError:
                # Simulation module not available, skip
                pass

        return cls(
            event_source=event_source,
            eventhub=eventhub_config,
            xact_eventhouse=eventhouse_config,
            claimx_eventhouse=claimx_eventhouse_config,
            local_kafka=local_kafka,
            domain=domain,
            enable_delta_writes=enable_delta_writes,
            # XACT table paths - check domain-specific env vars first, then generic
            events_table_path=(
                os.getenv("XACT_EVENTS_TABLE_PATH")
                or os.getenv("XACT_DELTA_EVENTS_TABLE")
                or os.getenv("DELTA_EVENTS_TABLE_PATH")
                or domain_delta_config.get("events_table_path", "")
            ),
            inventory_table_path=(
                os.getenv("XACT_INVENTORY_TABLE_PATH")
                or os.getenv("XACT_DELTA_INVENTORY_TABLE")
                or os.getenv("DELTA_INVENTORY_TABLE_PATH")
                or domain_delta_config.get("inventory_table_path", "")
            ),
            failed_table_path=(
                os.getenv("XACT_FAILED_TABLE_PATH")
                or os.getenv("XACT_DELTA_FAILED_TABLE")
                or os.getenv("DELTA_FAILED_TABLE_PATH")
                or domain_delta_config.get("failed_table_path", "")
            ),
            # Load ClaimX table paths - check env vars first, then config file
            claimx_projects_table_path=(
                os.getenv("CLAIMX_PROJECTS_TABLE_PATH")
                or os.getenv("CLAIMX_DELTA_PROJECTS_TABLE")
                or delta_config.get("claimx", {}).get("projects_table_path", "")
            ),
            claimx_contacts_table_path=(
                os.getenv("CLAIMX_CONTACTS_TABLE_PATH")
                or os.getenv("CLAIMX_DELTA_CONTACTS_TABLE")
                or delta_config.get("claimx", {}).get("contacts_table_path", "")
            ),
            claimx_inventory_table_path=(
                os.getenv("CLAIMX_INVENTORY_TABLE_PATH")
                or os.getenv("CLAIMX_DELTA_INVENTORY_TABLE")
                or delta_config.get("claimx", {}).get("inventory_table_path", "")
            ),
            claimx_media_table_path=(
                os.getenv("CLAIMX_MEDIA_TABLE_PATH")
                or os.getenv("CLAIMX_DELTA_MEDIA_TABLE")
                or delta_config.get("claimx", {}).get("media_table_path", "")
            ),
            claimx_tasks_table_path=(
                os.getenv("CLAIMX_TASKS_TABLE_PATH")
                or os.getenv("CLAIMX_DELTA_TASKS_TABLE")
                or delta_config.get("claimx", {}).get("tasks_table_path", "")
            ),
            claimx_task_templates_table_path=(
                os.getenv("CLAIMX_TASK_TEMPLATES_TABLE_PATH")
                or os.getenv("CLAIMX_DELTA_TASK_TEMPLATES_TABLE")
                or delta_config.get("claimx", {}).get("task_templates_table_path", "")
            ),
            claimx_external_links_table_path=(
                os.getenv("CLAIMX_EXTERNAL_LINKS_TABLE_PATH")
                or os.getenv("CLAIMX_DELTA_EXTERNAL_LINKS_TABLE")
                or delta_config.get("claimx", {}).get("external_links_table_path", "")
            ),
            claimx_video_collab_table_path=(
                os.getenv("CLAIMX_VIDEO_COLLAB_TABLE_PATH")
                or os.getenv("CLAIMX_DELTA_VIDEO_COLLAB_TABLE")
                or delta_config.get("claimx", {}).get("video_collab_table_path", "")
            ),
            # Simulation configuration
            simulation=simulation_config,
        )

    @property
    def is_eventhub_source(self):
        """Check if using Event Hub as source."""
        return self.event_source == EventSourceType.EVENTHUB

    @property
    def is_eventhouse_source(self):
        """Check if using Eventhouse as source."""
        return self.event_source == EventSourceType.EVENTHOUSE


def get_pipeline_config(config_path: Optional[Path] = None) -> PipelineConfig:
    """Get pipeline configuration from config directory and environment.

    This is the main entry point for loading configuration.
    """
    return PipelineConfig.load_config(config_path)


def get_event_source_type(config_path: Optional[Path] = None) -> EventSourceType:
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
