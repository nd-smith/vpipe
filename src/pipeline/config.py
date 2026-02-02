"""
Configuration compatibility shim.

Re-exports configuration classes from config module for backward compatibility.
Tests expect to import from pipeline.config but the actual
implementations live in config.config and config.pipeline_config.
"""

from config.config import KafkaConfig, load_config, reset_config
from config.pipeline_config import (
    DEFAULT_CONFIG_FILE,
    EventHubConfig,
    EventSourceType,
    LocalKafkaConfig,
    PipelineConfig,
)

__all__ = [
    "KafkaConfig",
    "EventHubConfig",
    "EventSourceType",
    "LocalKafkaConfig",
    "PipelineConfig",
    "DEFAULT_CONFIG_FILE",
    "load_config",
    "reset_config",
]
