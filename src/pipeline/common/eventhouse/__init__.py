"""Eventhouse/KQL infrastructure."""

from pipeline.common.eventhouse.kql_client import (
    EventhouseConfig,
    KQLClient,
    KQLQueryResult,
)
from pipeline.common.eventhouse.poller import (
    KQLEventPoller,
    PollerConfig,
)
from pipeline.common.eventhouse.sinks import (
    EventSink,
    JsonFileSink,
    JsonFileSinkConfig,
    KafkaSink,
    KafkaSinkConfig,
    create_json_sink,
    create_kafka_sink,
)

__all__ = [
    # KQL Client
    "EventhouseConfig",
    "KQLClient",
    "KQLQueryResult",
    # Poller
    "KQLEventPoller",
    "PollerConfig",
    # Sinks
    "EventSink",
    "KafkaSink",
    "KafkaSinkConfig",
    "JsonFileSink",
    "JsonFileSinkConfig",
    "create_kafka_sink",
    "create_json_sink",
]
