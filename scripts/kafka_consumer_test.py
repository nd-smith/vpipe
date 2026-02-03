#!/usr/bin/env python3
"""
Bare-bones Event Hub consumer for verifying message consumption.

Uses the azure-eventhub SDK with AMQP over WebSocket (port 443) so it works
behind corporate TLS-intercepting proxies that block the Kafka binary protocol
on port 9093.

Usage:
    export EVENTHUB_NAMESPACE_CONNECTION_STRING="Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=..."
    export EVENTHUB_NAME="your-eventhub-name"
    python scripts/kafka_consumer_test.py

Optional env vars:
    CONSUMER_GROUP     - consumer group (default: $Default)
    DISABLE_SSL_VERIFY - set to "true" to bypass SSL verification (corporate proxy)
    MAX_WAIT_TIME      - seconds to wait for events per receive cycle (default: 30)
    MAX_EVENTS         - max events to display before stopping (default: 50)
"""

import asyncio
import os
import sys

from azure.eventhub import TransportType
from azure.eventhub.aio import EventHubConsumerClient

conn_str = os.environ.get("EVENTHUB_NAMESPACE_CONNECTION_STRING", "")
eventhub_name = os.environ.get("EVENTHUB_NAME", "")

if not conn_str or not eventhub_name:
    print("ERROR: Set EVENTHUB_NAMESPACE_CONNECTION_STRING and EVENTHUB_NAME")
    sys.exit(1)

consumer_group = os.environ.get("CONSUMER_GROUP", "$Default")
max_wait_time = int(os.environ.get("MAX_WAIT_TIME", "30"))
max_events = int(os.environ.get("MAX_EVENTS", "50"))

# SSL bypass for corporate proxy environments
ssl_kwargs: dict = {}
if os.getenv("DISABLE_SSL_VERIFY", "false").lower() in ("true", "1", "yes"):
    # Use the project's existing SSL bypass if available, otherwise patch directly
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
        from core.security.ssl_dev_bypass import apply_ssl_dev_bypass, get_eventhub_ssl_kwargs

        apply_ssl_dev_bypass()
        ssl_kwargs = get_eventhub_ssl_kwargs()
    except ImportError:
        ssl_kwargs = {"connection_verify": False}
        print("SSL verification disabled (direct).")

print(f"Entity:        {eventhub_name}")
print(f"Group:         {consumer_group}")
print(f"Transport:     AMQP over WebSocket (port 443)")
print(f"Max wait:      {max_wait_time}s per cycle")
print("-" * 60)


async def receive_events():
    client = EventHubConsumerClient.from_connection_string(
        conn_str=conn_str,
        consumer_group=consumer_group,
        eventhub_name=eventhub_name,
        transport_type=TransportType.AmqpOverWebsocket,
        **ssl_kwargs,
    )

    count = 0
    done = False

    async def on_event(partition_context, event):
        nonlocal count, done
        if event is None:
            return
        count += 1
        body = event.body_as_str()
        preview = body[:200] + "..." if len(body) > 200 else body
        print(f"[{count}] partition={partition_context.partition_id} "
              f"offset={event.offset} seq={event.sequence_number}")
        print(f"    body={preview}")
        print()
        if count >= max_events:
            done = True
            raise KeyboardInterrupt  # break out of receive loop

    print(f"Listening for events (max {max_events}, {max_wait_time}s idle timeout)...\n")

    async with client:
        try:
            await client.receive(
                on_event=on_event,
                starting_position="-1",  # from beginning
                max_wait_time=max_wait_time,
            )
        except KeyboardInterrupt:
            pass

    if count == 0:
        print("No events received (timeout reached).")
    else:
        print(f"Done. Received {count} event(s).")


try:
    asyncio.run(receive_events())
except KeyboardInterrupt:
    print("\nStopped.")
