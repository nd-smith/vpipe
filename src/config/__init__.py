"""Configuration loading for Kafka pipeline.

This module provides centralized configuration loading for the Kafka pipeline system.
Configuration is loaded from multi-file config/ directory structure.

Configuration Structure
-----------------------

config/
    shared.yaml          # Global connection settings and defaults
    xact_config.yaml     # XACT domain configuration
    claimx_config.yaml   # ClaimX domain configuration
    plugins/             # Optional plugin configurations
        monitoring.yaml

Main Functions
--------------

Core configuration loading:
    - load_config(): Load Kafka configuration from YAML files
    - get_config(): Get or load singleton Kafka config instance
    - reset_config(): Reset singleton config instance

Pipeline configuration:
    - get_pipeline_config(): Load complete pipeline configuration

Usage Examples
--------------

Load configuration:
    >>> from config import load_config, get_config
    >>>
    >>> # Load from default location (config/ directory or config.yaml)
    >>> config = load_config()
    >>>
    >>> # Or use singleton pattern
    >>> config = get_config()
    >>>
    >>> # Access configuration
    >>> bootstrap_servers = config.bootstrap_servers
    >>> topic = config.get_topic("xact", "events")

Load pipeline configuration:
    >>> from config import get_pipeline_config
    >>>
    >>> pipeline_config = get_pipeline_config()
    >>> print(f"Event source: {pipeline_config.event_source}")
    >>> print(f"Kafka servers: {pipeline_config.local_kafka.bootstrap_servers}")

Custom config path:
    >>> from pathlib import Path
    >>> config = load_config(config_path=Path("/custom/path/config.yaml"))

Configuration Priority
---------------------

Settings are merged in the following priority (highest to lowest):

1. Environment variables (for specific settings like CLAIMX_API_TOKEN)
2. YAML configuration files
3. Dataclass defaults

For multi-file mode:
- Files are merged in order: shared.yaml → xact_config.yaml → claimx_config.yaml → plugins/*.yaml
- Later files override earlier files for conflicting keys
- Nested dictionaries are deep-merged

Configuration Loading
--------------------

Configuration is loaded exclusively from the config/ directory.
Single-file config.yaml is no longer supported. See config.yaml.old for
historical reference.

See Also
--------

- config.config: Core configuration classes and loading logic
- config.pipeline_config: Pipeline-specific configuration
"""

from config.config import (
    KafkaConfig,
    get_config,
    load_config,
    reset_config,
    set_config,
)
from config.pipeline_config import (
    ClaimXEventhouseSourceConfig,
    EventhouseSourceConfig,
    EventSourceType,
    LocalKafkaConfig,
    PipelineConfig,
    get_event_source_type,
    get_pipeline_config,
)

__all__ = [
    # Core config functions
    "load_config",
    "get_config",
    "set_config",
    "reset_config",
    # Core config classes
    "KafkaConfig",
    # Pipeline config functions
    "get_pipeline_config",
    "get_event_source_type",
    # Pipeline config classes
    "PipelineConfig",
    "LocalKafkaConfig",
    "EventhouseSourceConfig",
    "ClaimXEventhouseSourceConfig",
    "EventSourceType",
]
