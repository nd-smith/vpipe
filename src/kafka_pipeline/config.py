"""Re-export config module for backward compatibility."""

from config.config import (
    KafkaConfig,
    load_config,
    get_config,
    set_config,
    reset_config,
    DEFAULT_CONFIG_DIR,
)

__all__ = [
    "KafkaConfig",
    "load_config",
    "get_config",
    "set_config",
    "reset_config",
    "DEFAULT_CONFIG_DIR",
]
