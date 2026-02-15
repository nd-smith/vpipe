"""Fetch partition properties from EventHub."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from azure.eventhub import TransportType
from azure.eventhub.aio import EventHubConsumerClient


@dataclass
class PartitionInfo:
    partition_id: str
    beginning_sequence_number: int
    last_enqueued_sequence_number: int
    last_enqueued_offset: str
    last_enqueued_time_utc: datetime | None
    is_empty: bool


@dataclass
class EventHubProperties:
    name: str
    partition_count: int
    partition_ids: list[str]
    created_at: datetime | None


async def get_eventhub_properties(
    conn_str: str, eventhub_name: str, ssl_kwargs: dict | None = None,
) -> EventHubProperties:
    """Get high-level EventHub properties (partition count, etc.)."""
    client = EventHubConsumerClient.from_connection_string(
        conn_str=conn_str,
        consumer_group="$Default",
        eventhub_name=eventhub_name,
        transport_type=TransportType.AmqpOverWebsocket,
        **(ssl_kwargs or {}),
    )
    async with client:
        props = await client.get_eventhub_properties()
        return EventHubProperties(
            name=props["eventhub_name"],
            partition_count=len(props["partition_ids"]),
            partition_ids=list(props["partition_ids"]),
            created_at=props.get("created_at"),
        )


async def get_partition_properties(
    conn_str: str, eventhub_name: str, ssl_kwargs: dict | None = None,
) -> list[PartitionInfo]:
    """Get per-partition properties for an EventHub."""
    client = EventHubConsumerClient.from_connection_string(
        conn_str=conn_str,
        consumer_group="$Default",
        eventhub_name=eventhub_name,
        transport_type=TransportType.AmqpOverWebsocket,
        **(ssl_kwargs or {}),
    )
    partitions = []
    async with client:
        partition_ids = await client.get_partition_ids()
        for pid in sorted(partition_ids, key=int):
            props = await client.get_partition_properties(pid)
            partitions.append(PartitionInfo(
                partition_id=pid,
                beginning_sequence_number=props["beginning_sequence_number"],
                last_enqueued_sequence_number=props["last_enqueued_sequence_number"],
                last_enqueued_offset=str(props["last_enqueued_offset"]),
                last_enqueued_time_utc=props.get("last_enqueued_time_utc"),
                is_empty=props["is_empty"],
            ))
    return partitions
