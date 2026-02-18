"""Checkpoint viewing, advancing, and time-based reset."""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime

from azure.eventhub import TransportType
from azure.eventhub.aio import EventHubConsumerClient
from azure.storage.blob import ContainerClient


@dataclass
class CheckpointEntry:
    partition_id: str
    offset: str | None
    sequence_number: int | None


@dataclass
class CheckpointConfig:
    """Connection parameters shared across checkpoint operations."""

    conn_str: str
    eventhub_name: str
    consumer_group: str
    fqdn: str
    blob_conn_str: str
    container_name: str
    ssl_kwargs: dict = field(default_factory=dict)


def list_checkpoints(
    blob_conn_str: str,
    container_name: str,
    fqdn: str,
    eventhub_name: str,
    consumer_group: str,
) -> list[CheckpointEntry]:
    """List all checkpoint entries for a consumer group."""
    container_client = ContainerClient.from_connection_string(
        blob_conn_str, container_name=container_name, connection_verify=False,
    )
    prefix = f"{fqdn}/{eventhub_name}/{consumer_group}/checkpoint/"
    entries = []

    for blob in container_client.list_blobs(name_starts_with=prefix, include=["metadata"]):
        partition_id = blob.name.rsplit("/", 1)[-1]
        metadata = blob.metadata or {}
        seq_str = metadata.get("sequencenumber")
        entries.append(CheckpointEntry(
            partition_id=partition_id,
            offset=metadata.get("offset"),
            sequence_number=int(seq_str) if seq_str else None,
        ))

    return sorted(entries, key=lambda e: int(e.partition_id) if e.partition_id.isdigit() else e.partition_id)


def advance_checkpoints_to_latest(
    conn_str: str,
    eventhub_name: str,
    consumer_group: str,
    fqdn: str,
    blob_conn_str: str,
    container_name: str,
    ssl_kwargs: dict | None = None,
) -> dict[str, dict]:
    """Advance all checkpoints to the latest offset. Returns {partition_id: {old, new}}."""
    loop = asyncio.new_event_loop()
    try:
        offsets = loop.run_until_complete(
            _get_latest_offsets(conn_str, eventhub_name, ssl_kwargs)
        )
    finally:
        loop.close()

    return _update_checkpoint_blobs(
        blob_conn_str, container_name, fqdn, eventhub_name, consumer_group, offsets,
    )


async def reset_checkpoints_to_time(
    cfg: CheckpointConfig,
    target_time: datetime,
) -> dict[str, dict]:
    """Reset checkpoints to a specific point in time. Returns {partition_id: {old, new}}."""
    offsets = await _find_offsets_at_time(
        cfg.conn_str, cfg.eventhub_name, target_time, cfg.ssl_kwargs or None,
    )

    return _update_checkpoint_blobs(
        cfg.blob_conn_str, cfg.container_name, cfg.fqdn,
        cfg.eventhub_name, cfg.consumer_group, offsets,
    )


async def _get_latest_offsets(
    conn_str: str, eventhub_name: str, ssl_kwargs: dict | None = None,
) -> dict[str, dict]:
    """Get the latest offset/sequence for each partition."""
    client = EventHubConsumerClient.from_connection_string(
        conn_str=conn_str,
        consumer_group="$Default",
        eventhub_name=eventhub_name,
        transport_type=TransportType.AmqpOverWebsocket,
        **(ssl_kwargs or {}),
    )
    offsets = {}
    async with client:
        partition_ids = await client.get_partition_ids()
        for pid in sorted(partition_ids, key=int):
            props = await client.get_partition_properties(pid)
            if not props["is_empty"]:
                offsets[pid] = {
                    "offset": str(props["last_enqueued_offset"]),
                    "sequencenumber": str(props["last_enqueued_sequence_number"]),
                }
    return offsets


async def _find_offsets_at_time(
    conn_str: str,
    eventhub_name: str,
    target_time: datetime,
    ssl_kwargs: dict | None = None,
) -> dict[str, dict]:
    """Find the offset/sequence at a given enqueue time for each partition."""
    client = EventHubConsumerClient.from_connection_string(
        conn_str=conn_str,
        consumer_group="$Default",
        eventhub_name=eventhub_name,
        transport_type=TransportType.AmqpOverWebsocket,
        **(ssl_kwargs or {}),
    )
    results = {}

    async with client:
        partition_ids = await client.get_partition_ids()

        for pid in sorted(partition_ids, key=int):
            props = await client.get_partition_properties(pid)
            if props["is_empty"]:
                continue

            first_event = None

            async def on_event(partition_context, event):
                nonlocal first_event
                if event is not None and first_event is None:
                    first_event = event
                    raise StopIteration

            try:
                await client.receive(
                    on_event=on_event,
                    partition_id=pid,
                    starting_position=target_time,
                    starting_position_inclusive=True,
                    max_wait_time=10,
                )
            except StopIteration:
                pass

            if first_event:
                results[pid] = {
                    "offset": str(first_event.offset),
                    "sequencenumber": str(first_event.sequence_number),
                }
            else:
                results[pid] = {
                    "offset": str(props["last_enqueued_offset"]),
                    "sequencenumber": str(props["last_enqueued_sequence_number"]),
                }

    return results


def _update_checkpoint_blobs(
    blob_conn_str: str,
    container_name: str,
    fqdn: str,
    eventhub_name: str,
    consumer_group: str,
    offsets: dict[str, dict],
) -> dict[str, dict]:
    """Write new checkpoint metadata to blob storage. Returns change details."""
    container_client = ContainerClient.from_connection_string(
        blob_conn_str, container_name=container_name, connection_verify=False,
    )

    prefix = f"{fqdn}/{eventhub_name}/{consumer_group}/checkpoint/"
    existing = {
        blob.name.rsplit("/", 1)[-1]: blob
        for blob in container_client.list_blobs(name_starts_with=prefix, include=["metadata"])
    }

    changes = {}
    for partition_id, new_meta in sorted(offsets.items()):
        blob_name = f"{prefix}{partition_id}"

        old_meta = {}
        if partition_id in existing:
            old_meta = existing[partition_id].metadata or {}

        changes[partition_id] = {
            "old_offset": old_meta.get("offset", "(none)"),
            "old_sequence": old_meta.get("sequencenumber", "(none)"),
            "new_offset": new_meta["offset"],
            "new_sequence": new_meta["sequencenumber"],
        }

        blob_client = container_client.get_blob_client(blob_name)
        if partition_id in existing:
            blob_client.set_blob_metadata(new_meta)
        else:
            blob_client.upload_blob(b"", overwrite=True, metadata=new_meta)

    return changes
