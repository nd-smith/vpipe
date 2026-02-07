"""
Configuration compatibility shim.

Re-exports configuration classes from config module for backward compatibility.
Tests expect to import from pipeline.config but the actual
implementations live in config.config and config.pipeline_config.
"""

from config.config import MessageConfig, load_config, reset_config
from config.pipeline_config import (
    DEFAULT_CONFIG_FILE,
    EventHubConfig,
    EventSourceType,
    PipelineConfig,
)

__all__ = [
    "MessageConfig",
    "EventHubConfig",
    "EventSourceType",
    "PipelineConfig",
    "DEFAULT_CONFIG_FILE",
    "load_config",
    "reset_config",
]
