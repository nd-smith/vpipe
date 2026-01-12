#!/usr/bin/env python3
"""
Direct itel Cabinet Plugin Trigger Script

This script bypasses the ClaimX enrichment worker and directly triggers
the itel Cabinet plugin from events in claimx.enrichment.pending.

Useful for testing the plugin without needing a real ClaimX API.

Usage:
    python scripts/trigger_itel_plugin_from_events.py
"""

import asyncio
import json
import logging
import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from kafka_pipeline.plugins.shared.task_trigger import TaskTriggerPlugin
from kafka_pipeline.plugins.shared.base import PluginContext, Domain, PipelineStage

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Process events and trigger itel Cabinet plugin."""
    print("=" * 80)
    print("itel Cabinet Plugin Direct Trigger")
    print("=" * 80)
    print()

    kafka_brokers = os.getenv("KAFKA_BROKERS", "localhost:9094")
    print(f"Kafka brokers: {kafka_brokers}")
    print(f"Input topic: claimx.enrichment.pending")
    print(f"Output topic: itel.cabinet.task.tracking")
    print()

    # Create consumer
    consumer = AIOKafkaConsumer(
        "claimx.enrichment.pending",
        bootstrap_servers=kafka_brokers,
        group_id="itel_plugin_trigger_test",
        auto_offset_reset="earliest",  # Start from beginning
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    )

    # Create producer
    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_brokers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )

    # Initialize plugin
    plugin_config = {
        "triggers": {
            32513: {  # itel Cabinet task ID
                "name": "iTel Cabinet Repair Form Task",
                "on_any": {
                    "publish_to_topic": "itel.cabinet.task.tracking",
                },
            }
        },
        "include_task_data": True,
        "include_project_data": True,
    }
    plugin = TaskTriggerPlugin(config=plugin_config)

    try:
        await consumer.start()
        await producer.start()

        print("Processing events... Press Ctrl+C to stop")
        print()

        message_count = 0
        triggered_count = 0

        async for msg in consumer:
            message_count += 1
            event = msg.value

            print(f"[{message_count}] Processing event: {event.get('event_id')}")
            print(f"    Event type: {event.get('event_type')}")
            print(f"    Project ID: {event.get('project_id')}")

            # Extract task data from raw_data (from dummy producer)
            raw_data = event.get("raw_data", {})
            task_id = raw_data.get("taskId")

            print(f"    Task ID: {task_id}")

            if task_id != 32513:
                print(f"    → Skipped (not itel Cabinet task)")
                print()
                continue

            # Build enriched task data from raw_data
            # The dummy producer already included mock ClaimX API response
            claimx_task_details = raw_data.get("claimx_task_details", {})

            if not claimx_task_details:
                print(f"    → Skipped (no task details in raw_data)")
                print()
                continue

            # Create plugin context with task data
            context = PluginContext(
                event_id=event.get("event_id"),
                event_type=event.get("event_type"),
                domain=Domain.CLAIMX,
                stage=PipelineStage.ENRICHMENT_COMPLETE,
                project_id=event.get("project_id"),
                raw_event=event,
                enriched_data={},
                tasks=[claimx_task_details],  # Provide task data to plugin
                project=raw_data.get("project"),
            )

            # Execute plugin
            result = await plugin.execute(context)

            if result.success and result.actions:
                triggered_count += 1
                print(f"    ✓ Plugin triggered! Actions: {len(result.actions)}")

                # Execute actions
                for action in result.actions:
                    if action.type.value == "publish_to_topic":
                        topic = action.params.get("topic")
                        payload = action.params.get("payload", {})

                        print(f"    → Publishing to topic: {topic}")
                        await producer.send(
                            topic,
                            value=payload,
                            key=event.get("event_id").encode('utf-8') if event.get("event_id") else None,
                        )
                        print(f"    ✓ Published successfully")
            else:
                print(f"    → Not triggered: {result.message}")

            print()

            # Stop after processing 10 events (for testing)
            if message_count >= 10:
                print(f"Processed {message_count} events (limit reached)")
                break

    except KeyboardInterrupt:
        print("\nStopped by user")
    finally:
        await consumer.stop()
        await producer.stop()

    print()
    print("=" * 80)
    print(f"Summary: Processed {message_count} events, triggered plugin {triggered_count} times")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
