#!/usr/bin/env python3
"""Drain or advance checkpoints for all internal pipeline Event Hubs.

Useful for resetting environments after bad deployments or during dev/test
cleanup. Event Hub messages can't be deleted (they expire based on retention),
so this script supports two modes:

  drain   — consume and count all pending messages per partition
  advance — move checkpoint blob metadata to the latest offset/sequence number

Configuration is via variables at the top of this file (no CLI args).
Set DISABLE_SSL_VERIFY=true in environment for corporate proxy environments.
"""

import asyncio
import os
import re
import sys

from azure.eventhub import TransportType
from azure.eventhub.aio import EventHubConsumerClient

# ---------------------------------------------------------------------------
# Configuration — edit these variables directly
# ---------------------------------------------------------------------------

DRY_RUN = True                  # True = report only, don't modify anything
MODE = "drain"                  # "drain" to consume/count, "advance" to move checkpoints

NAMESPACE_CONNECTION_STRING = ""    # Event Hub namespace connection string
BLOB_CONNECTION_STRING = ""         # Blob storage connection string (advance mode only)
CHECKPOINT_CONTAINER_NAME = "eventhub-checkpoints"  # Blob container name

# Event hub name -> consumer group (consumer group only used in advance mode)
EVENT_HUBS = {
    # verisk
    "verisk-enrichment-pending": "$Default",
    "verisk-downloads-pending": "$Default",
    "verisk-downloads-cached": "$Default",
    "verisk-downloads-results": "$Default",
    "verisk-retry": "$Default",
    "verisk-dlq": "$Default",
    # claimx
    "claimx-enrichment-pending": "$Default",
    "claimx-enriched": "$Default",
    "claimx-downloads-pending": "$Default",
    "claimx-downloads-cached": "$Default",
    "claimx-downloads-results": "$Default",
    "claimx-retry": "$Default",
    "claimx-dlq": "$Default",
}

# ---------------------------------------------------------------------------
# SSL bypass
# ---------------------------------------------------------------------------


def get_ssl_kwargs():
    if os.getenv("DISABLE_SSL_VERIFY", "false").lower() in ("true", "1", "yes"):
        try:
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
            from core.security.ssl_dev_bypass import apply_ssl_dev_bypass, get_eventhub_ssl_kwargs

            apply_ssl_dev_bypass()
            return get_eventhub_ssl_kwargs()
        except ImportError:
            return {"connection_verify": False}
    return {}


def strip_entity_path(conn_str):
    """Remove EntityPath from a connection string if present."""
    parts = [
        part for part in conn_str.split(";")
        if part.strip() and not part.startswith("EntityPath=")
    ]
    return ";".join(parts)


def resolve_fqdn(conn_str):
    match = re.search(r"Endpoint=sb://([^/;]+)", conn_str, re.IGNORECASE)
    if not match:
        print("Error: Could not extract FQDN from connection string.", file=sys.stderr)
        sys.exit(1)
    return match.group(1)


# ---------------------------------------------------------------------------
# Drain mode
# ---------------------------------------------------------------------------


async def drain_event_hub(conn_str, eventhub_name, ssl_kwargs, dry_run):
    """Consume all pending messages and return count per partition."""
    client = EventHubConsumerClient.from_connection_string(
        conn_str=conn_str,
        consumer_group="$Default",
        eventhub_name=eventhub_name,
        transport_type=TransportType.AmqpOverWebsocket,
        **ssl_kwargs,
    )

    partition_counts = {}

    async with client:
        partition_ids = await client.get_partition_ids()
        print(f"  Partitions: {len(partition_ids)}")

        for pid in sorted(partition_ids, key=int):
            props = await client.get_partition_properties(pid)

            if props["is_empty"]:
                partition_counts[pid] = 0
                print(f"    partition {pid:>3s}  (empty)")
                continue

            last_seq = props["last_enqueued_sequence_number"]
            begin_seq = props["beginning_sequence_number"]
            pending = last_seq - begin_seq + 1

            if dry_run:
                partition_counts[pid] = pending
                print(
                    f"    partition {pid:>3s}  begin_seq={begin_seq}  "
                    f"last_seq={last_seq}  pending~={pending}"
                )
                continue

            # Actually consume messages to get an exact count
            count = 0
            timed_out = False

            async def on_event(partition_context, event):
                nonlocal count, timed_out
                if event is None:
                    timed_out = True
                    raise StopIteration
                count += 1

            try:
                await client.receive(
                    on_event=on_event,
                    partition_id=pid,
                    starting_position="-1",
                    starting_position_inclusive=False,
                    max_wait_time=5,
                )
            except StopIteration:
                pass

            partition_counts[pid] = count
            print(f"    partition {pid:>3s}  consumed {count} message(s)")

    return partition_counts


# ---------------------------------------------------------------------------
# Advance mode
# ---------------------------------------------------------------------------


async def get_partition_end_offsets(conn_str, eventhub_name, ssl_kwargs):
    """Get the latest offset and sequence number for each partition."""
    client = EventHubConsumerClient.from_connection_string(
        conn_str=conn_str,
        consumer_group="$Default",
        eventhub_name=eventhub_name,
        transport_type=TransportType.AmqpOverWebsocket,
        **ssl_kwargs,
    )

    offsets = {}
    async with client:
        partition_ids = await client.get_partition_ids()
        print(f"  Partitions: {len(partition_ids)}")

        for pid in sorted(partition_ids, key=int):
            props = await client.get_partition_properties(pid)

            if props["is_empty"]:
                print(f"    partition {pid:>3s}  (empty, skipping)")
                continue

            offsets[pid] = {
                "offset": str(props["last_enqueued_offset"]),
                "sequencenumber": str(props["last_enqueued_sequence_number"]),
            }
            print(
                f"    partition {pid:>3s}  last_offset={props['last_enqueued_offset']}  "
                f"last_seq={props['last_enqueued_sequence_number']}"
            )

    return offsets


def advance_checkpoints(fqdn, eventhub_name, consumer_group, offsets,
                        blob_conn_str, container_name, dry_run):
    """Update checkpoint blobs to the latest offset/sequence number."""
    from azure.storage.blob import ContainerClient

    container_client = ContainerClient.from_connection_string(
        blob_conn_str, container_name=container_name
    )

    prefix = f"{fqdn}/{eventhub_name}/{consumer_group}/checkpoint/"
    existing = {
        blob.name.rsplit("/", 1)[-1]: blob
        for blob in container_client.list_blobs(name_starts_with=prefix, include=["metadata"])
    }

    updated = 0
    for partition_id, new_meta in sorted(offsets.items()):
        blob_name = f"{prefix}{partition_id}"

        if partition_id in existing:
            old = existing[partition_id].metadata or {}
            old_offset = old.get("offset", "?")
            old_seq = old.get("sequencenumber", "?")
        else:
            old_offset = "(no checkpoint)"
            old_seq = "(no checkpoint)"

        print(
            f"    partition {partition_id:>3s}  "
            f"offset {old_offset} -> {new_meta['offset']}  "
            f"seq {old_seq} -> {new_meta['sequencenumber']}"
        )

        if not dry_run:
            blob_client = container_client.get_blob_client(blob_name)
            if partition_id in existing:
                blob_client.set_blob_metadata(new_meta)
            else:
                blob_client.upload_blob(b"", overwrite=True, metadata=new_meta)
            updated += 1

    return updated


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    if MODE not in ("drain", "advance"):
        print(f"Error: MODE must be 'drain' or 'advance', got '{MODE}'", file=sys.stderr)
        return 1

    conn_str = NAMESPACE_CONNECTION_STRING
    if not conn_str:
        print("Error: Set NAMESPACE_CONNECTION_STRING at the top of this script.", file=sys.stderr)
        return 1

    if MODE == "advance":
        if not BLOB_CONNECTION_STRING:
            print("Error: Set BLOB_CONNECTION_STRING at the top of this script for advance mode.", file=sys.stderr)
            return 1
        fqdn = resolve_fqdn(conn_str)

    conn_str = strip_entity_path(conn_str)
    ssl_kwargs = get_ssl_kwargs()

    print(f"Mode:     {MODE}")
    print(f"Dry run:  {DRY_RUN}")
    print(f"Hubs:     {len(EVENT_HUBS)}")
    print("=" * 60)

    grand_total = 0

    for hub_name, consumer_group in EVENT_HUBS.items():
        print(f"\n--- {hub_name} ---")

        try:
            if MODE == "drain":
                counts = asyncio.run(drain_event_hub(conn_str, hub_name, ssl_kwargs, DRY_RUN))
                total = sum(counts.values())
                grand_total += total
                print(f"  Total: {total} message(s)")

            elif MODE == "advance":
                offsets = asyncio.run(get_partition_end_offsets(conn_str, hub_name, ssl_kwargs))
                if offsets:
                    print(f"  Advancing checkpoints for {consumer_group}:")
                    updated = advance_checkpoints(
                        fqdn, hub_name, consumer_group, offsets,
                        BLOB_CONNECTION_STRING, CHECKPOINT_CONTAINER_NAME, DRY_RUN,
                    )
                    grand_total += len(offsets)
                    if DRY_RUN:
                        print(f"  Dry run — would update {len(offsets)} checkpoint(s)")
                    else:
                        print(f"  Updated {updated} checkpoint(s)")
                else:
                    print("  No partitions with data, nothing to advance.")

        except Exception as e:
            print(f"  ERROR: {e}")
            continue

    print("\n" + "=" * 60)
    if MODE == "drain":
        label = "estimated " if DRY_RUN else ""
        print(f"Grand total: {grand_total} {label}message(s) across {len(EVENT_HUBS)} hub(s)")
    else:
        label = "would advance" if DRY_RUN else "advanced"
        print(f"Grand total: {label} {grand_total} partition(s) across {len(EVENT_HUBS)} hub(s)")

    if DRY_RUN:
        print("\nDry run — no changes were made. Set DRY_RUN = False to apply.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
