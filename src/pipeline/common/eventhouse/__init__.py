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
    MessageSink,
    MessageSinkConfig,
    create_json_sink,
    create_message_sink,
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
    "MessageSink",
    "MessageSinkConfig",
    "JsonFileSink",
    "JsonFileSinkConfig",
    "create_message_sink",
    "create_json_sink",
]
