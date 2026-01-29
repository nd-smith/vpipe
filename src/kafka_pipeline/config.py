"""Backward compatibility re-export for config.

This module provides backward compatibility for tests and code that import
from kafka_pipeline.config instead of the canonical config.config location.
"""

from config.config import (
    KafkaConfig,
    load_config,
    get_config,
    set_config,
    reset_config,
    load_yaml,
    DEFAULT_CONFIG_FILE,
)

__all__ = [
    "KafkaConfig",
    "load_config",
    "get_config",
    "set_config",
    "reset_config",
    "load_yaml",
    "DEFAULT_CONFIG_FILE",
]
