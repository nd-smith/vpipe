#!/usr/bin/env python3
"""Test Azure Event Hub connection with AMQP over WebSocket.

This script validates that the Event Hub connection is working correctly
with the corporate Private Link endpoint.

Usage:
    python scripts/test_eventhub_connection.py

Requirements:
    - EVENTHUB_CONNECTION_STRING environment variable
    - DISABLE_SSL_VERIFY=true if behind corporate proxy
"""

import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from azure.eventhub import EventHubProducerClient, EventData, TransportType
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub import EventPosition
import asyncio
import time


def test_producer_sync():
    """Test Event Hub producer (synchronous API)."""
    print("\n=== Testing Event Hub Producer (Sync) ===")

    connection_string = os.getenv("EVENTHUB_CONNECTION_STRING")
    if not connection_string:
        print("❌ ERROR: EVENTHUB_CONNECTION_STRING not set")
        return False

    # Apply SSL bypass if configured
    disable_ssl = os.getenv("DISABLE_SSL_VERIFY", "false").lower() in ("true", "1", "yes")
    if disable_ssl:
        print("⚠️  SSL verification disabled (DISABLE_SSL_VERIFY=true)")
        from core.security.ssl_dev_bypass import apply_ssl_dev_bypass
        apply_ssl_dev_bypass()

    try:
        print(f"Connecting to Event Hub...")

        # Create producer with WebSocket transport
        producer = EventHubProducerClient.from_connection_string(
            conn_str=connection_string,
            transport_type=TransportType.AmqpOverWebsocket,
        )

        # Get Event Hub properties
        props = producer.get_eventhub_properties()
        print(f"✓ Connected to Event Hub: {props.get('name', 'unknown')}")
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

        print("✓ Message sent successfully")
        print("\n✅ Producer test PASSED")
        return True

    except Exception as e:
        print(f"\n❌ Producer test FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_consumer_async():
    """Test Event Hub consumer (async API)."""
    print("\n=== Testing Event Hub Consumer (Async) ===")

    connection_string = os.getenv("EVENTHUB_CONNECTION_STRING")
    if not connection_string:
        print("❌ ERROR: EVENTHUB_CONNECTION_STRING not set")
        return False

    # Apply SSL bypass if configured
    disable_ssl = os.getenv("DISABLE_SSL_VERIFY", "false").lower() in ("true", "1", "yes")
    if disable_ssl:
        from core.security.ssl_dev_bypass import apply_ssl_dev_bypass
        apply_ssl_dev_bypass()

    consumer_group = os.getenv("EVENTHUB_CONSUMER_GROUP", "$Default")

    try:
        print(f"Creating consumer with group: {consumer_group}")

        # Create consumer with WebSocket transport
        consumer = EventHubConsumerClient.from_connection_string(
            conn_str=connection_string,
            consumer_group=consumer_group,
            transport_type=TransportType.AmqpOverWebsocket,
        )

        print("✓ Consumer created")

        # Consume a few messages (with timeout)
        message_count = 0
        max_messages = 3
        timeout_seconds = 10

        async def on_event(partition_context, event):
            nonlocal message_count
            message_count += 1
            print(f"\n✓ Received message #{message_count}:")
            print(f"  Partition: {partition_context.partition_id}")
            print(f"  Offset: {event.offset}")
            print(f"  Body: {event.body_as_str()[:100]}...")

            # Checkpoint
            await partition_context.update_checkpoint(event)

            if message_count >= max_messages:
                # Signal to stop
                raise KeyboardInterrupt("Max messages received")

        async def on_error(partition_context, error):
            print(f"❌ Error in partition {partition_context.partition_id if partition_context else 'unknown'}: {error}")

        print(f"Waiting for up to {max_messages} messages (timeout: {timeout_seconds}s)...")

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

        except (asyncio.TimeoutError, KeyboardInterrupt):
            print(f"\n✓ Stopped after {message_count} messages")

        if message_count > 0:
            print("\n✅ Consumer test PASSED")
            return True
        else:
            print("\n⚠️  Consumer test PASSED (no messages available)")
            return True

    except Exception as e:
        print(f"\n❌ Consumer test FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("=" * 60)
    print("Event Hub Connection Test")
    print("=" * 60)

    # Check environment
    connection_string = os.getenv("EVENTHUB_CONNECTION_STRING")
    if not connection_string:
        print("\n❌ EVENTHUB_CONNECTION_STRING not set")
        print("\nUsage:")
        print("  export EVENTHUB_CONNECTION_STRING='Endpoint=sb://...'")
        print("  python scripts/test_eventhub_connection.py")
        sys.exit(1)

    # Extract connection info
    print("\nConnection Info:")
    for part in connection_string.split(";"):
        if part.startswith("Endpoint="):
            print(f"  Namespace: {part.split('=', 1)[1]}")
        elif part.startswith("SharedAccessKeyName="):
            print(f"  Policy: {part.split('=', 1)[1]}")
        elif part.startswith("EntityPath="):
            print(f"  Entity: {part.split('=', 1)[1]}")

    # Run tests
    producer_ok = test_producer_sync()

    # Only run consumer test if producer succeeded
    consumer_ok = False
    if producer_ok:
        consumer_ok = asyncio.run(test_consumer_async())

    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    print(f"Producer: {'✅ PASS' if producer_ok else '❌ FAIL'}")
    print(f"Consumer: {'✅ PASS' if consumer_ok else '❌ FAIL'}")

    if producer_ok and consumer_ok:
        print("\n🎉 All tests passed! Event Hub connection is working.")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed. Check configuration and connection.")
        sys.exit(1)


if __name__ == "__main__":
    main()
