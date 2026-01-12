"""Eventhouse/KQL infrastructure."""

from kafka_pipeline.common.eventhouse.kql_client import (
    EventhouseConfig,
    KQLClient,
    KQLQueryResult,
)
from kafka_pipeline.common.eventhouse.poller import (
    KQLEventPoller,
    PollerConfig,
)

__all__ = [
    # KQL Client
    "EventhouseConfig",
    "KQLClient",
    "KQLQueryResult",
    # Poller
    "KQLEventPoller",
    "PollerConfig",
]
