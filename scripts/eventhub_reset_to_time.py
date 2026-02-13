"""Reset Event Hub consumer checkpoints to a specific point in time.

Looks up the offset/sequence number at the target enqueue time for each
partition, then updates the checkpoint blobs in Azure Blob Storage.

Usage:
    # Reset to 12 hours ago
    python scripts/eventhub_reset_to_time.py verisk_events verisk-event-ingester --hours-ago 12

    # Reset to a specific UTC datetime
    python scripts/eventhub_reset_to_time.py verisk_events verisk-event-ingester \
        --datetime "2026-02-13T00:00:00"

    # Dry run (show what would change without modifying anything)
    python scripts/eventhub_reset_to_time.py verisk_events verisk-event-ingester --hours-ago 12 --dry-run

Environment variables:
    EVENTHUB_NAMESPACE_CONNECTION_STRING   Event Hub namespace connection string
    EVENTHUB_CHECKPOINT_BLOB_CONNECTION_STRING  Blob storage connection string
    EVENTHUB_CHECKPOINT_CONTAINER_NAME     Blob container (default: eventhub-checkpoints)
    DISABLE_SSL_VERIFY                     Set "true" for corporate proxy environments
"""

import argparse
import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone

from azure.eventhub import TransportType
from azure.eventhub.aio import EventHubConsumerClient
from azure.storage.blob import ContainerClient


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
    """Remove EntityPath from a connection string if present.

    Normalizes entity-level connection strings to namespace-level so the
    SDK's eventhub_name parameter can be used instead.
    """
    parts = [
        part for part in conn_str.split(";")
        if part.strip() and not part.startswith("EntityPath=")
    ]
    return ";".join(parts)


def print_connection_info(conn_str, eventhub_name):
    """Print masked connection details for troubleshooting."""
    import re

    masked = re.sub(
        r"(SharedAccessKey=)[^;]+", r"\1***MASKED***", conn_str, flags=re.IGNORECASE,
    )
    print(f"Connection string (masked): {masked}")

    for part in conn_str.split(";"):
        if part.startswith("EntityPath="):
            print(f"WARNING: Connection string contains EntityPath={part.split('=', 1)[1]}")
            print(f"  EntityPath was stripped; using eventhub_name='{eventhub_name}' instead.")
            break

    endpoint_match = re.search(r"Endpoint=sb://([^/;]+)", conn_str, re.IGNORECASE)
    if endpoint_match:
        print(f"Namespace: {endpoint_match.group(1)}")

    policy_match = re.search(r"SharedAccessKeyName=([^;]+)", conn_str, re.IGNORECASE)
    if policy_match:
        print(f"SAS policy: {policy_match.group(1)}")

    print(f"Event Hub: {eventhub_name}")
    print()


async def find_offsets_at_time(conn_str, eventhub_name, target_time, ssl_kwargs):
    """For each partition, find the offset and sequence number at target_time.

    Starts a receiver at the target enqueue time and reads the first event
    to get its offset/sequence number. Returns a dict of partition_id ->
    {"offset": str, "sequencenumber": str}.
    """
    client = EventHubConsumerClient.from_connection_string(
        conn_str=conn_str,
        consumer_group="$Default",
        eventhub_name=eventhub_name,
        transport_type=TransportType.AmqpOverWebsocket,
        **ssl_kwargs,
    )

    results = {}

    async with client:
        partition_ids = await client.get_partition_ids()
        print(f"Event Hub '{eventhub_name}' has {len(partition_ids)} partitions")
        print(f"Target time: {target_time.isoformat()}")
        print()

        for pid in sorted(partition_ids, key=int):
            props = await client.get_partition_properties(pid)

            if props["is_empty"]:
                print(f"  partition {pid:>3s}  (empty, skipping)")
                continue

            # Receive one event starting from the target time
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
                print(
                    f"  partition {pid:>3s}  offset={first_event.offset}  "
                    f"seq={first_event.sequence_number}  "
                    f"enqueued={first_event.enqueued_time}"
                )
            else:
                # Target time is beyond the last event — use the last known values
                results[pid] = {
                    "offset": str(props["last_enqueued_offset"]),
                    "sequencenumber": str(props["last_enqueued_sequence_number"]),
                }
                print(
                    f"  partition {pid:>3s}  offset={props['last_enqueued_offset']}  "
                    f"seq={props['last_enqueued_sequence_number']}  "
                    f"(no events after target time, using last)"
                )

    return results


def resolve_fqdn(conn_str):
    import re

    match = re.search(r"Endpoint=sb://([^/;]+)", conn_str, re.IGNORECASE)
    if not match:
        print(f"Error: Could not extract FQDN from connection string.", file=sys.stderr)
        sys.exit(1)
    return match.group(1)


def update_checkpoints(fqdn, eventhub_name, consumer_group, offsets, blob_conn_str,
                        container_name, dry_run=False):
    """Update checkpoint blobs with new offset/sequence number metadata."""
    container_client = ContainerClient.from_connection_string(
        blob_conn_str, container_name=container_name
    )

    prefix = f"{fqdn}/{eventhub_name}/{consumer_group}/checkpoint/"
    existing = {
        blob.name.rsplit("/", 1)[-1]: blob
        for blob in container_client.list_blobs(name_starts_with=prefix, include=["metadata"])
    }

    print(f"\nUpdating checkpoints: {fqdn}/{eventhub_name}/{consumer_group}")
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
            f"  partition {partition_id:>3s}  "
            f"offset {old_offset} -> {new_meta['offset']}  "
            f"seq {old_seq} -> {new_meta['sequencenumber']}"
        )

        if not dry_run:
            blob_client = container_client.get_blob_client(blob_name)
            if partition_id in existing:
                blob_client.set_blob_metadata(new_meta)
            else:
                # Create the checkpoint blob if it doesn't exist
                blob_client.upload_blob(b"", overwrite=True, metadata=new_meta)
            updated += 1

    if dry_run:
        print(f"\nDry run — no changes made. Would update {len(offsets)} checkpoint(s).")
    else:
        print(f"\nUpdated {updated} checkpoint(s).")


def main():
    parser = argparse.ArgumentParser(
        description="Reset Event Hub consumer checkpoints to a specific point in time.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("eventhub", help="Event Hub name (e.g. verisk_events)")
    parser.add_argument("consumer_group", help="Consumer group (e.g. verisk-event-ingester)")

    time_group = parser.add_mutually_exclusive_group(required=True)
    time_group.add_argument(
        "--hours-ago", type=float, help="Reset to N hours ago from now"
    )
    time_group.add_argument(
        "--datetime", help="Reset to a specific UTC datetime (e.g. 2026-02-13T00:00:00)"
    )

    parser.add_argument("--dry-run", action="store_true", help="Show changes without applying")
    parser.add_argument("--namespace-conn-str", help="Event Hub namespace connection string")
    parser.add_argument("--blob-conn-str", help="Blob storage connection string")
    parser.add_argument("--container", help="Blob container name (default: eventhub-checkpoints)")

    args = parser.parse_args()

    # Resolve target time
    if args.hours_ago:
        target_time = datetime.now(timezone.utc) - timedelta(hours=args.hours_ago)
    else:
        target_time = datetime.fromisoformat(args.datetime).replace(tzinfo=timezone.utc)

    # Resolve connection strings
    eh_conn_str = (
        args.namespace_conn_str
        or os.environ.get("EVENTHUB_NAMESPACE_CONNECTION_STRING")
        or os.environ.get("SOURCE_EVENTHUB_NAMESPACE_CONNECTION_STRING")
    )
    if not eh_conn_str:
        print(
            "Error: Set EVENTHUB_NAMESPACE_CONNECTION_STRING or use --namespace-conn-str.",
            file=sys.stderr,
        )
        sys.exit(1)

    blob_conn_str = args.blob_conn_str or os.environ.get(
        "EVENTHUB_CHECKPOINT_BLOB_CONNECTION_STRING"
    )
    if not blob_conn_str:
        print(
            "Error: Set EVENTHUB_CHECKPOINT_BLOB_CONNECTION_STRING or use --blob-conn-str.",
            file=sys.stderr,
        )
        sys.exit(1)

    container_name = args.container or os.environ.get(
        "EVENTHUB_CHECKPOINT_CONTAINER_NAME", "eventhub-checkpoints"
    )

    fqdn = resolve_fqdn(eh_conn_str)
    ssl_kwargs = get_ssl_kwargs()

    # Print diagnostics before stripping so EntityPath warnings are visible.
    print_connection_info(eh_conn_str, args.eventhub)

    # Strip EntityPath so the SDK uses eventhub_name instead.
    # Entity-scoped connection strings cause CBS auth failures when the
    # eventhub_name doesn't match the EntityPath.
    eh_conn_str = strip_entity_path(eh_conn_str)

    # Step 1: Find offsets at the target time
    print(f"Looking up offsets at target time...\n")
    try:
        offsets = asyncio.run(find_offsets_at_time(eh_conn_str, args.eventhub, target_time, ssl_kwargs))
    except Exception as e:
        if "CBS Token authentication failed" in str(e):
            print(f"\nError: {e}", file=sys.stderr)
            print("\nTroubleshooting CBS authentication failure:", file=sys.stderr)
            print("  1. Verify the SAS key hasn't expired or been rotated", file=sys.stderr)
            print("  2. Verify the SAS policy has 'Listen' permission on this Event Hub", file=sys.stderr)
            print(f"  3. Verify Event Hub '{args.eventhub}' exists in this namespace", file=sys.stderr)
            print("  4. If behind a proxy, ensure DISABLE_SSL_VERIFY=true is set", file=sys.stderr)
            sys.exit(1)
        raise

    if not offsets:
        print("\nNo partitions with data found. Nothing to update.")
        return 0

    # Step 2: Update checkpoint blobs
    update_checkpoints(
        fqdn, args.eventhub, args.consumer_group, offsets,
        blob_conn_str, container_name, dry_run=args.dry_run,
    )

    if not args.dry_run:
        print(
            "\nIMPORTANT: Restart the consumer for it to pick up the new checkpoints."
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
