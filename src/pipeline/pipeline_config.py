"""
Pipeline configuration compatibility module.

Re-exports pipeline config classes and functions from config.pipeline_config
for backward compatibility with tests.
"""

from config.pipeline_config import (
    DEFAULT_CONFIG_FILE,
    EventHubConfig,
    EventhouseSourceConfig,
    EventSourceType,
    LocalKafkaConfig,
    PipelineConfig,
    get_event_source_type,
    get_pipeline_config,
)

__all__ = [
    "DEFAULT_CONFIG_FILE",
    "EventHubConfig",
    "EventhouseSourceConfig",
    "EventSourceType",
    "LocalKafkaConfig",
    "PipelineConfig",
    "get_event_source_type",
    "get_pipeline_config",
]
