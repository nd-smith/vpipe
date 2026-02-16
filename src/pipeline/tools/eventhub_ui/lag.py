"""Consumer lag calculation: checkpoint offsets vs partition end offsets."""

import asyncio
from dataclasses import dataclass

from azure.storage.blob import ContainerClient

from pipeline.tools.eventhub_ui.partitions import get_partition_properties


@dataclass
class PartitionLag:
    partition_id: str
    last_enqueued_sequence: int
    checkpointed_sequence: int | None
    lag: int | None  # None if no checkpoint exists
    checkpointed_offset: str | None


@dataclass
class ConsumerGroupLag:
    eventhub_name: str
    consumer_group: str
    partitions: list[PartitionLag]
    total_lag: int | None  # None if any partition has no checkpoint


async def calculate_lag(
    conn_str: str,
    eventhub_name: str,
    consumer_group: str,
    fqdn: str,
    blob_conn_str: str,
    container_name: str,
    ssl_kwargs: dict | None = None,
) -> ConsumerGroupLag:
    """Calculate consumer lag by comparing checkpoints to partition end offsets."""
    # Fetch partition end offsets and blob checkpoints concurrently
    partition_infos, checkpoints = await asyncio.gather(
        get_partition_properties(conn_str, eventhub_name, ssl_kwargs),
        asyncio.to_thread(_read_checkpoints, blob_conn_str, container_name, fqdn, eventhub_name, consumer_group),
    )

    partition_lags = []
    total_lag = 0
    all_have_checkpoints = True

    for pinfo in partition_infos:
        cp = checkpoints.get(pinfo.partition_id)
        if cp is not None and not pinfo.is_empty:
            lag = max(0, pinfo.last_enqueued_sequence_number - cp["sequence"])
            partition_lags.append(PartitionLag(
                partition_id=pinfo.partition_id,
                last_enqueued_sequence=pinfo.last_enqueued_sequence_number,
                checkpointed_sequence=cp["sequence"],
                lag=lag,
                checkpointed_offset=cp["offset"],
            ))
            total_lag += lag
        elif pinfo.is_empty:
            partition_lags.append(PartitionLag(
                partition_id=pinfo.partition_id,
                last_enqueued_sequence=pinfo.last_enqueued_sequence_number,
                checkpointed_sequence=cp["sequence"] if cp else None,
                lag=0,
                checkpointed_offset=cp["offset"] if cp else None,
            ))
        else:
            all_have_checkpoints = False
            partition_lags.append(PartitionLag(
                partition_id=pinfo.partition_id,
                last_enqueued_sequence=pinfo.last_enqueued_sequence_number,
                checkpointed_sequence=None,
                lag=None,
                checkpointed_offset=None,
            ))

    return ConsumerGroupLag(
        eventhub_name=eventhub_name,
        consumer_group=consumer_group,
        partitions=partition_lags,
        total_lag=total_lag if all_have_checkpoints else None,
    )


def _read_checkpoints(
    blob_conn_str: str,
    container_name: str,
    fqdn: str,
    eventhub_name: str,
    consumer_group: str,
) -> dict[str, dict]:
    """Read checkpoint blob metadata. Returns {partition_id: {offset, sequence}}."""
    container_client = ContainerClient.from_connection_string(
        blob_conn_str, container_name=container_name, connection_verify=False,
    )

    prefix = f"{fqdn}/{eventhub_name}/{consumer_group}/checkpoint/"
    checkpoints = {}

    for blob in container_client.list_blobs(name_starts_with=prefix, include=["metadata"]):
        partition_id = blob.name.rsplit("/", 1)[-1]
        metadata = blob.metadata or {}
        offset = metadata.get("offset")
        seq_str = metadata.get("sequencenumber")
        if seq_str is not None:
            checkpoints[partition_id] = {
                "offset": offset,
                "sequence": int(seq_str),
            }

    return checkpoints
