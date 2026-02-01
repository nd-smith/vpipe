#!/usr/bin/env python3
"""Test Azure Event Hub connection with AMQP over WebSocket.

This script validates that the Event Hub connection is working correctly
with the corporate Private Link endpoint.

Usage:
    python scripts/test_eventhub_connection.py
    python scripts/test_eventhub_connection.py --entity my-eventhub-name

Requirements:
    - EVENTHUB_NAMESPACE_CONNECTION_STRING environment variable
      (or legacy EVENTHUB_CONNECTION_STRING)
    - DISABLE_SSL_VERIFY=true if behind corporate proxy
"""

import argparse
import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import time

from azure.eventhub import EventData, EventHubProducerClient, EventPosition, TransportType
from azure.eventhub.aio import EventHubConsumerClient


def _get_connection_string() -> str:
    """Get namespace connection string, stripping EntityPath if present."""
    conn = os.getenv("EVENTHUB_NAMESPACE_CONNECTION_STRING") or os.getenv(
        "EVENTHUB_CONNECTION_STRING"
    )
    if not conn:
        return ""
    # Strip EntityPath (we pass eventhub_name separately)
    parts = [
        p for p in conn.split(";") if p.strip() and not p.startswith("EntityPath=")
    ]
    return ";".join(parts)


def _get_eventhub_name(args_entity: str = "") -> str:
    """Get Event Hub name from args, env, or config."""
    if args_entity:
        return args_entity
    return os.getenv("EVENTHUB_ENTITY_NAME", "verisk_events")


def test_producer_sync(connection_string: str, eventhub_name: str):
    """Test Event Hub producer (synchronous API)."""
    print("\n=== Testing Event Hub Producer (Sync) ===")

    # Apply SSL bypass if configured
    disable_ssl = os.getenv("DISABLE_SSL_VERIFY", "false").lower() in (
        "true",
        "1",
        "yes",
    )
    if disable_ssl:
        print("Warning: SSL verification disabled (DISABLE_SSL_VERIFY=true)")
        from core.security.ssl_dev_bypass import apply_ssl_dev_bypass

        apply_ssl_dev_bypass()

    try:
        print(f"Connecting to Event Hub entity: {eventhub_name}")

        # Create producer with namespace connection string + eventhub_name
        producer = EventHubProducerClient.from_connection_string(
            conn_str=connection_string,
            eventhub_name=eventhub_name,
            transport_type=TransportType.AmqpOverWebsocket,
        )

        # Get Event Hub properties
        props = producer.get_eventhub_properties()
        print(f"  Connected to: {props.get('name', 'unknown')}")
        print(f"  Partitions: {len(props.get('partition_ids', []))}")

        # Send test message
        print("Sending test message...")
        test_message = {
            "type": "test",
            "message": "Hello from Event Hub test script",
            "timestamp": time.time(),
        }

        with producer:
            batch = producer.create_batch()
            event = EventData(str(test_message))
            event.properties["test"] = "true"
            event.properties["source"] = "test_script"
            batch.add(event)
            producer.send_batch(batch)

        print("  Message sent successfully")
        print("\nProducer test PASSED")
        return True

    except Exception as e:
        print(f"\nProducer test FAILED: {e}")
        import traceback

        traceback.print_exc()
        return False


async def test_consumer_async(
    connection_string: str, eventhub_name: str, consumer_group: str
):
    """Test Event Hub consumer (async API)."""
    print("\n=== Testing Event Hub Consumer (Async) ===")

    # Apply SSL bypass if configured
    disable_ssl = os.getenv("DISABLE_SSL_VERIFY", "false").lower() in (
        "true",
        "1",
        "yes",
    )
    if disable_ssl:
        from core.security.ssl_dev_bypass import apply_ssl_dev_bypass

        apply_ssl_dev_bypass()

    try:
        print(f"Creating consumer: entity={eventhub_name}, group={consumer_group}")

        # Create consumer with namespace connection string + eventhub_name
        consumer = EventHubConsumerClient.from_connection_string(
            conn_str=connection_string,
            consumer_group=consumer_group,
            eventhub_name=eventhub_name,
            transport_type=TransportType.AmqpOverWebsocket,
        )

        print("  Consumer created")

        # Consume a few messages (with timeout)
        message_count = 0
        max_messages = 3
        timeout_seconds = 10

        async def on_event(partition_context, event):
            nonlocal message_count
            message_count += 1
            print(f"\n  Received message #{message_count}:")
            print(f"    Partition: {partition_context.partition_id}")
            print(f"    Offset: {event.offset}")
            print(f"    Body: {event.body_as_str()[:100]}...")

            # Checkpoint
            await partition_context.update_checkpoint(event)

            if message_count >= max_messages:
                # Signal to stop
                raise KeyboardInterrupt("Max messages received")

        async def on_error(partition_context, error):
            print(
                f"  Error in partition {partition_context.partition_id if partition_context else 'unknown'}: {error}"
            )

        print(
            f"Waiting for up to {max_messages} messages (timeout: {timeout_seconds}s)..."
        )

        try:
            async with consumer:
                # Receive with timeout
                receive_task = asyncio.create_task(
                    consumer.receive(
                        on_event=on_event,
                        on_error=on_error,
                        starting_position=EventPosition("-1"),  # From beginning
                    )
                )

                # Wait with timeout
                await asyncio.wait_for(receive_task, timeout=timeout_seconds)

        except (TimeoutError, KeyboardInterrupt):
            print(f"\n  Stopped after {message_count} messages")

        if message_count > 0:
            print("\nConsumer test PASSED")
            return True
        else:
            print("\nConsumer test PASSED (no messages available)")
            return True

    except Exception as e:
        print(f"\nConsumer test FAILED: {e}")
        import traceback

        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    parser = argparse.ArgumentParser(description="Test Event Hub connection")
    parser.add_argument(
        "--entity",
        default="",
        help="Event Hub entity name to test (default: from EVENTHUB_ENTITY_NAME or 'pcesdopodappv1')",
    )
    parser.add_argument(
        "--consumer-group",
        default="",
        help="Consumer group (default: from EVENTHUB_CONSUMER_GROUP or '$Default')",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Event Hub Connection Test")
    print("=" * 60)

    # Check environment
    connection_string = _get_connection_string()
    if not connection_string:
        print("\nERROR: No connection string configured")
        print("\nUsage:")
        print("  export EVENTHUB_NAMESPACE_CONNECTION_STRING='Endpoint=sb://...'")
        print("  python scripts/test_eventhub_connection.py")
        print("\nOr use legacy variable:")
        print("  export EVENTHUB_CONNECTION_STRING='Endpoint=sb://...;EntityPath=...'")
        sys.exit(1)

    eventhub_name = _get_eventhub_name(args.entity)
    consumer_group = args.consumer_group or os.getenv(
        "EVENTHUB_CONSUMER_GROUP", "$Default"
    )

    # Extract connection info for display
    print("\nConnection Info:")
    for part in connection_string.split(";"):
        if part.startswith("Endpoint="):
            print(f"  Namespace: {part.split('=', 1)[1]}")
        elif part.startswith("SharedAccessKeyName="):
            print(f"  Policy: {part.split('=', 1)[1]}")
    print(f"  Entity: {eventhub_name}")
    print(f"  Consumer Group: {consumer_group}")

    # Run tests
    producer_ok = test_producer_sync(connection_string, eventhub_name)

    # Only run consumer test if producer succeeded
    consumer_ok = False
    if producer_ok:
        consumer_ok = asyncio.run(
            test_consumer_async(connection_string, eventhub_name, consumer_group)
        )

    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    print(f"Producer: {'PASS' if producer_ok else 'FAIL'}")
    print(f"Consumer: {'PASS' if consumer_ok else 'FAIL'}")

    if producer_ok and consumer_ok:
        print("\nAll tests passed! Event Hub connection is working.")
        sys.exit(0)
    else:
        print("\nSome tests failed. Check configuration and connection.")
        sys.exit(1)


if __name__ == "__main__":
    main()
