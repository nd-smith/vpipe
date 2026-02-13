"""Event Hub Checkpoint Management CLI.

List and reset Event Hub consumer checkpoints stored in Azure Blob Storage.
Deleting checkpoints causes a consumer to restart from its configured
starting_position (typically @latest).
"""

import argparse
import os
import re
import sys

from azure.storage.blob import ContainerClient


def resolve_fqdn(args):
    """Resolve the Event Hub namespace FQDN from args or environment.

    Resolution order:
      1. --namespace-fqdn
      2. --namespace-conn-str
      3. env EVENTHUB_NAMESPACE_CONNECTION_STRING
      4. env SOURCE_EVENTHUB_NAMESPACE_CONNECTION_STRING
    """
    if args.namespace_fqdn:
        return args.namespace_fqdn

    conn_str = (
        args.namespace_conn_str
        or os.environ.get("EVENTHUB_NAMESPACE_CONNECTION_STRING")
        or os.environ.get("SOURCE_EVENTHUB_NAMESPACE_CONNECTION_STRING")
    )
    if not conn_str:
        print(
            "Error: Could not determine Event Hub namespace.\n"
            "Provide --namespace-fqdn, --namespace-conn-str, or set\n"
            "EVENTHUB_NAMESPACE_CONNECTION_STRING / SOURCE_EVENTHUB_NAMESPACE_CONNECTION_STRING.",
            file=sys.stderr,
        )
        sys.exit(1)

    match = re.search(r"Endpoint=sb://([^/;]+)", conn_str, re.IGNORECASE)
    if not match:
        print(
            f"Error: Could not extract FQDN from connection string.\n"
            f"Expected 'Endpoint=sb://<fqdn>' but got: {conn_str[:80]}...",
            file=sys.stderr,
        )
        sys.exit(1)

    return match.group(1)


def get_container_client(args):
    """Build a ContainerClient from args/environment."""
    conn_str = args.blob_conn_str or os.environ.get(
        "EVENTHUB_CHECKPOINT_BLOB_CONNECTION_STRING"
    )
    if not conn_str:
        print(
            "Error: Blob storage connection string not found.\n"
            "Provide --blob-conn-str or set EVENTHUB_CHECKPOINT_BLOB_CONNECTION_STRING.",
            file=sys.stderr,
        )
        sys.exit(1)

    container = args.container or os.environ.get(
        "EVENTHUB_CHECKPOINT_CONTAINER_NAME", "eventhub-checkpoints"
    )
    return ContainerClient.from_connection_string(conn_str, container_name=container)


def list_blobs_with_prefix(container_client, prefix):
    """List blobs under a prefix, including metadata."""
    return list(container_client.list_blobs(name_starts_with=prefix, include=["metadata"]))


def format_checkpoint(blob):
    """Format a checkpoint blob for display."""
    metadata = blob.metadata or {}
    offset = metadata.get("offset", "?")
    seq = metadata.get("sequencenumber", "?")
    partition = blob.name.rsplit("/", 1)[-1]
    return f"  partition {partition:>3s}  offset={offset}  sequence={seq}"


def format_ownership(blob):
    """Format an ownership blob for display."""
    metadata = blob.metadata or {}
    owner = metadata.get("ownerid", "?")
    partition = blob.name.rsplit("/", 1)[-1]
    return f"  partition {partition:>3s}  owner={owner}"


def cmd_list(args):
    """List checkpoint (and optionally ownership) blobs."""
    fqdn = resolve_fqdn(args)
    container_client = get_container_client(args)
    prefix = f"{fqdn}/{args.eventhub}/{args.consumer_group}"

    checkpoint_prefix = f"{prefix}/checkpoint/"
    checkpoints = list_blobs_with_prefix(container_client, checkpoint_prefix)

    print(f"\nCheckpoints: {fqdn}/{args.eventhub}/{args.consumer_group}")
    if checkpoints:
        for blob in sorted(checkpoints, key=lambda b: b.name):
            print(format_checkpoint(blob))
    else:
        print("  (none)")

    if args.show_ownership:
        ownership_prefix = f"{prefix}/ownership/"
        ownership_blobs = list_blobs_with_prefix(container_client, ownership_prefix)

        print(f"\nOwnership:")
        if ownership_blobs:
            for blob in sorted(ownership_blobs, key=lambda b: b.name):
                print(format_ownership(blob))
        else:
            print("  (none)")

    print()
    return 0


def cmd_reset(args):
    """Delete checkpoint (and optionally ownership) blobs."""
    fqdn = resolve_fqdn(args)
    container_client = get_container_client(args)
    prefix = f"{fqdn}/{args.eventhub}/{args.consumer_group}"

    checkpoint_prefix = f"{prefix}/checkpoint/"
    to_delete = list_blobs_with_prefix(container_client, checkpoint_prefix)

    if args.include_ownership:
        ownership_prefix = f"{prefix}/ownership/"
        to_delete.extend(list_blobs_with_prefix(container_client, ownership_prefix))

    if not to_delete:
        print("\nNo checkpoint blobs found — nothing to reset.")
        return 0

    print(f"\nBlobs to delete ({len(to_delete)}):")
    for blob in sorted(to_delete, key=lambda b: b.name):
        print(f"  {blob.name}")

    if not args.yes:
        response = input(
            "\nWARNING: This will permanently delete these checkpoint blobs.\n"
            "The consumer will restart from its configured starting_position.\n"
            "Are you sure you want to continue? (yes/no): "
        )
        if response.lower() not in ("yes", "y"):
            print("Reset cancelled.")
            return 0

    for blob in to_delete:
        container_client.delete_blob(blob.name)

    print(f"\nDeleted {len(to_delete)} blob(s). Consumer will restart from starting_position.")
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Manage Event Hub consumer checkpoints in Azure Blob Storage.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
    # List checkpoints for the verisk event ingester
    python scripts/eventhub_checkpoints.py list verisk_events verisk-event-ingester

    # Reset checkpoints (consumer restarts from @latest)
    python scripts/eventhub_checkpoints.py reset verisk_events verisk-event-ingester

    # Explicit connection string
    python scripts/eventhub_checkpoints.py list verisk-enrichment-pending verisk-enrichment-pipeline \\
        --namespace-conn-str "$EVENTHUB_NAMESPACE_CONNECTION_STRING"
""",
    )

    # Shared arguments
    shared = argparse.ArgumentParser(add_help=False)
    shared.add_argument("eventhub", help="Event Hub name (e.g. verisk_events)")
    shared.add_argument("consumer_group", help="Consumer group (e.g. verisk-event-ingester)")
    shared.add_argument("--namespace-fqdn", help="Event Hub namespace FQDN directly")
    shared.add_argument(
        "--namespace-conn-str", help="Event Hub namespace connection string (FQDN extracted)"
    )
    shared.add_argument("--blob-conn-str", help="Blob storage connection string")
    shared.add_argument("--container", help="Blob container name (default: eventhub-checkpoints)")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # list
    parser_list = subparsers.add_parser(
        "list", parents=[shared], help="List checkpoint blobs and their offsets"
    )
    parser_list.add_argument(
        "--show-ownership", action="store_true", help="Also show ownership blobs"
    )

    # reset
    parser_reset = subparsers.add_parser(
        "reset", parents=[shared], help="Delete checkpoint blobs to reset consumer position"
    )
    parser_reset.add_argument(
        "--include-ownership", action="store_true", help="Also delete ownership blobs"
    )
    parser_reset.add_argument("--yes", action="store_true", help="Skip confirmation prompt")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    if args.command == "list":
        return cmd_list(args)
    elif args.command == "reset":
        return cmd_reset(args)


if __name__ == "__main__":
    sys.exit(main())
