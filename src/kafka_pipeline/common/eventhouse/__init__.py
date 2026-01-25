# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
#
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

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
from kafka_pipeline.common.eventhouse.sinks import (
    EventSink,
    KafkaSink,
    KafkaSinkConfig,
    JsonFileSink,
    JsonFileSinkConfig,
    create_kafka_sink,
    create_json_sink,
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
