"""Configuration loading for Kafka pipeline.

This module provides centralized configuration loading for the Kafka pipeline system.
Configuration is loaded from a single consolidated config.yaml file.

Configuration Structure
-----------------------

config/
    config.yaml          # Consolidated configuration with all settings

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
    >>> topic = config.get_topic("verisk", "events")

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

1. Environment variables (using ${VAR_NAME} syntax in YAML)
2. YAML configuration values
3. Dataclass defaults

Configuration Loading
--------------------

All configuration is loaded from a single config/config.yaml file with clear
domain sections for XACT and ClaimX. Environment variables are expanded
using ${VAR_NAME} syntax.

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
    "EventhouseSourceConfig",
    "ClaimXEventhouseSourceConfig",
    "EventSourceType",
]
