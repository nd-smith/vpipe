"""Re-export pipeline_config module for backward compatibility."""

from config.pipeline_config import (
    ClaimXEventhouseSourceConfig,
    EventHubConfig,
    EventhouseSourceConfig,
    EventSourceType,
    LocalKafkaConfig,
    PipelineConfig,
    get_event_source_type,
    get_pipeline_config,
)

__all__ = [
    "ClaimXEventhouseSourceConfig",
    "EventHubConfig",
    "EventhouseSourceConfig",
    "EventSourceType",
    "LocalKafkaConfig",
    "PipelineConfig",
    "get_event_source_type",
    "get_pipeline_config",
]
