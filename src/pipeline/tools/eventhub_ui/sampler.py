"""Sample recent messages from an EventHub partition."""

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from azure.eventhub import TransportType
from azure.eventhub.aio import EventHubConsumerClient


@dataclass
class SampledMessage:
    sequence_number: int
    offset: str
    enqueued_time: datetime
    partition_key: str | None
    body: str  # JSON string or raw text


def _format_event(event: Any) -> SampledMessage:
    """Convert a received EventHub event to a SampledMessage."""
    body_bytes = event.body_as_str()
    try:
        parsed = json.loads(body_bytes)
        body = json.dumps(parsed, indent=2, default=str)
    except (json.JSONDecodeError, TypeError):
        body = body_bytes

    return SampledMessage(
        sequence_number=event.sequence_number,
        offset=str(event.offset),
        enqueued_time=event.enqueued_time,
        partition_key=event.partition_key,
        body=body,
    )


async def sample_messages(
    conn_str: str,
    eventhub_name: str,
    partition_id: str,
    count: int = 5,
    starting_position: str = "@latest",
    starting_time: datetime | None = None,
    ssl_kwargs: dict | None = None,
) -> list[SampledMessage]:
    """Read a small number of messages from a partition.

    If starting_time is provided, reads from that time forward.
    If starting_position is "@latest", reads the last `count` messages
    by computing an offset from the partition end.
    """
    client = EventHubConsumerClient.from_connection_string(
        conn_str=conn_str,
        consumer_group="$Default",
        eventhub_name=eventhub_name,
        transport_type=TransportType.AmqpOverWebsocket,
        **(ssl_kwargs or {}),
    )

    async with client:
        if starting_time:
            position = starting_time
            inclusive = True
        elif starting_position == "@latest":
            props = await client.get_partition_properties(partition_id)
            if props["is_empty"]:
                return []
            position = max(
                props["beginning_sequence_number"],
                props["last_enqueued_sequence_number"] - count,
            )
            inclusive = True
        else:
            position = starting_position
            inclusive = False

        received = []

        async def on_event(partition_context, event):
            if event is None:
                raise StopIteration
            received.append(event)
            if len(received) >= count:
                raise StopIteration

        try:
            await client.receive(
                on_event=on_event,
                partition_id=partition_id,
                starting_position=position,
                starting_position_inclusive=inclusive,
                max_wait_time=10,
            )
        except StopIteration:
            pass

    return [_format_event(event) for event in received]
