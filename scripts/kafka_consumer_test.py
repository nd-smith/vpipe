#!/usr/bin/env python3
"""
Bare-bones Kafka consumer for verifying consumption from Azure Event Hub.

Usage:
    export EVENTHUB_NAMESPACE_CONNECTION_STRING="Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=..."
    export EVENTHUB_NAME="your-eventhub-name"
    python scripts/kafka_consumer_test.py

Optional env vars:
    CONSUMER_GROUP    - consumer group (default: $Default)
    AUTO_OFFSET_RESET - earliest or latest (default: earliest)
"""

import os
import ssl
import sys
from kafka import KafkaConsumer

conn_str = os.environ.get("EVENTHUB_NAMESPACE_CONNECTION_STRING", "")
eventhub_name = os.environ.get("EVENTHUB_NAME", "")

if not conn_str or not eventhub_name:
    print("ERROR: Set EVENTHUB_NAMESPACE_CONNECTION_STRING and EVENTHUB_NAME")
    sys.exit(1)

# Parse namespace from connection string
# Format: Endpoint=sb://<namespace>.servicebus.windows.net/;...
namespace = conn_str.split("sb://")[1].split(".")[0]
bootstrap_server = f"{namespace}.servicebus.windows.net:9093"

consumer_group = os.environ.get("CONSUMER_GROUP", "$Default")
auto_offset_reset = os.environ.get("AUTO_OFFSET_RESET", "earliest")

# SSL bypass
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

print(f"Connecting to: {bootstrap_server}")
print(f"Topic:         {eventhub_name}")
print(f"Group:         {consumer_group}")
print(f"Offset reset:  {auto_offset_reset}")
print("-" * 60)

consumer = KafkaConsumer(
    eventhub_name,
    bootstrap_servers=bootstrap_server,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username="$ConnectionString",
    sasl_plain_password=conn_str,
    ssl_context=ssl_context,
    api_version=(0, 10, 0),  # skip auto-detection; use v0 SASL handshake for Event Hubs
    group_id=consumer_group,
    auto_offset_reset=auto_offset_reset,
    consumer_timeout_ms=60000,
)

print("Listening for messages (60s timeout)...\n")

try:
    count = 0
    for msg in consumer:
        count += 1
        print(f"[{count}] topic={msg.topic} partition={msg.partition} offset={msg.offset}")
        print(f"    key={msg.key}")
        print(f"    value={msg.value}")
        print()
    print("No more messages (timeout reached).")
except KeyboardInterrupt:
    print("\nStopped.")
finally:
    consumer.close()
