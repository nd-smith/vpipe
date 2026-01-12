#!/usr/bin/env python3
"""
Debug iTel Cabinet Task Flow

This script helps diagnose why CUSTOM_TASK_COMPLETED messages aren't
reaching the itel.cabinet.task.tracking topic.

It consumes from multiple topics and shows the message flow:
1. claimx.events.raw
2. claimx.enrichment.pending
3. claimx.entities.rows (check tasks field)
4. itel.cabinet.task.tracking
"""

import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from aiokafka import AIOKafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FlowTracer:
    """Traces message flow through pipeline topics."""

    def __init__(self):
        self.events_by_id = {}
        self.stats = defaultdict(int)

    def track_raw_event(self, event):
        """Track event from claimx.events.raw."""
        event_id = event.get("event_id")
        event_type = event.get("event_type")
        task_assignment_id = event.get("task_assignment_id")

        self.events_by_id[event_id] = {
            "event_id": event_id,
            "event_type": event_type,
            "task_assignment_id": task_assignment_id,
            "raw": True,
            "enrichment_pending": False,
            "has_task_rows": False,
            "itel_triggered": False,
        }
        self.stats[f"raw_{event_type}"] += 1

        return f"[RAW] {event_type} | event_id={event_id} | task_assignment_id={task_assignment_id}"

    def track_enrichment_pending(self, task):
        """Track task from claimx.enrichment.pending."""
        event_id = task.get("event_id")
        event_type = task.get("event_type")

        if event_id in self.events_by_id:
            self.events_by_id[event_id]["enrichment_pending"] = True
        else:
            # Event started from enrichment (shouldn't happen)
            self.events_by_id[event_id] = {
                "event_id": event_id,
                "event_type": event_type,
                "raw": False,
                "enrichment_pending": True,
                "has_task_rows": False,
                "itel_triggered": False,
            }

        self.stats[f"enrichment_{event_type}"] += 1
        return f"[ENRICHMENT] {event_type} | event_id={event_id}"

    def track_entity_rows(self, entity_rows):
        """Track entity rows from claimx.entities.rows."""
        event_id = entity_rows.get("event_id")
        event_type = entity_rows.get("event_type")
        tasks = entity_rows.get("tasks", [])
        projects = entity_rows.get("projects", [])

        task_ids = [t.get("task_id") for t in tasks]
        has_itel_task = 32513 in task_ids

        if event_id in self.events_by_id:
            self.events_by_id[event_id]["has_task_rows"] = bool(tasks)
            self.events_by_id[event_id]["task_ids"] = task_ids

        self.stats["entity_rows_with_tasks"] += 1 if tasks else 0
        self.stats["entity_rows_with_projects"] += 1 if projects else 0
        self.stats["entity_rows_with_itel_task"] += 1 if has_itel_task else 0

        return (f"[ENTITIES] {event_type} | event_id={event_id} | "
                f"tasks={len(tasks)} | projects={len(projects)} | "
                f"task_ids={task_ids} | itel_task={has_itel_task}")

    def track_itel_message(self, message):
        """Track message from itel.cabinet.task.tracking."""
        event_id = message.get("event_id")
        event_type = message.get("event_type")
        task_id = message.get("task_id")

        if event_id in self.events_by_id:
            self.events_by_id[event_id]["itel_triggered"] = True

        self.stats["itel_triggered"] += 1
        return f"[ITEL] {event_type} | event_id={event_id} | task_id={task_id}"

    def print_summary(self):
        """Print flow summary."""
        print("\n" + "=" * 80)
        print("FLOW ANALYSIS SUMMARY")
        print("=" * 80)

        # Count flow stages
        total = len(self.events_by_id)
        raw_count = sum(1 for e in self.events_by_id.values() if e["raw"])
        enrichment_count = sum(1 for e in self.events_by_id.values() if e["enrichment_pending"])
        task_rows_count = sum(1 for e in self.events_by_id.values() if e["has_task_rows"])
        itel_count = sum(1 for e in self.events_by_id.values() if e["itel_triggered"])

        print(f"\nTotal Events Tracked: {total}")
        print(f"  ├─ In raw topic:           {raw_count}")
        print(f"  ├─ In enrichment pending:  {enrichment_count}")
        print(f"  ├─ Has task rows:          {task_rows_count}")
        print(f"  └─ Triggered iTel:         {itel_count}")

        # Find broken flows
        print("\n" + "-" * 80)
        print("ISSUES DETECTED:")
        print("-" * 80)

        issues_found = False

        # Events in raw but not enrichment
        raw_not_enrichment = [e for e in self.events_by_id.values()
                              if e["raw"] and not e["enrichment_pending"]]
        if raw_not_enrichment:
            issues_found = True
            print(f"\n⚠️  {len(raw_not_enrichment)} events in RAW but not ENRICHMENT_PENDING:")
            for e in raw_not_enrichment[:5]:
                print(f"   - {e['event_id']} ({e['event_type']})")

        # Events in enrichment but no task rows
        enrichment_no_tasks = [e for e in self.events_by_id.values()
                               if e["enrichment_pending"] and not e["has_task_rows"]
                               and e.get("event_type") == "CUSTOM_TASK_COMPLETED"]
        if enrichment_no_tasks:
            issues_found = True
            print(f"\n⚠️  {len(enrichment_no_tasks)} CUSTOM_TASK_COMPLETED events with NO TASK ROWS:")
            for e in enrichment_no_tasks[:5]:
                print(f"   - {e['event_id']} | task_assignment_id={e.get('task_assignment_id')}")
            print("   → Check TaskHandler logs for errors or empty responses")

        # Events with task rows but not iTel triggered
        tasks_no_itel = [e for e in self.events_by_id.values()
                        if e["has_task_rows"] and not e["itel_triggered"]
                        and e.get("event_type") == "CUSTOM_TASK_COMPLETED"]
        if tasks_no_itel:
            issues_found = True
            print(f"\n⚠️  {len(tasks_no_itel)} events with TASK ROWS but NO ITEL TRIGGER:")
            for e in tasks_no_itel[:5]:
                task_ids = e.get("task_ids", [])
                has_32513 = 32513 in task_ids
                print(f"   - {e['event_id']} | task_ids={task_ids} | has_32513={has_32513}")
            print("   → Check if task_id=32513 is in the task rows")
            print("   → Check if TaskTriggerPlugin is enabled and registered")

        if not issues_found:
            print("\n✓ No obvious issues detected in message flow")

        # Stats
        print("\n" + "-" * 80)
        print("DETAILED STATS:")
        print("-" * 80)
        for key, value in sorted(self.stats.items()):
            print(f"  {key}: {value}")

        print("\n" + "=" * 80)


async def main():
    """Consume from all relevant topics and trace flow."""
    kafka_brokers = os.getenv("KAFKA_BROKERS", "localhost:9094")

    print("=" * 80)
    print("iTel Cabinet Task Flow Debugger")
    print("=" * 80)
    print(f"\nKafka brokers: {kafka_brokers}")
    print(f"Monitoring topics:")
    print(f"  1. claimx.events.raw")
    print(f"  2. claimx.enrichment.pending")
    print(f"  3. claimx.entities.rows")
    print(f"  4. itel.cabinet.task.tracking")
    print(f"\nPress Ctrl+C to stop and see summary")
    print("=" * 80)
    print()

    tracer = FlowTracer()

    # Create consumers for all topics
    consumers = {}
    topics = {
        "raw": "claimx.events.raw",
        "enrichment": "claimx.enrichment.pending",
        "entities": "claimx.entities.rows",
        "itel": "itel.cabinet.task.tracking",
    }

    for key, topic in topics.items():
        consumers[key] = AIOKafkaConsumer(
            topic,
            bootstrap_servers=kafka_brokers,
            group_id=f"debug_itel_flow_{key}",
            auto_offset_reset="latest",  # Start from latest
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        )

    try:
        # Start all consumers
        for consumer in consumers.values():
            await consumer.start()

        print("✓ All consumers started\n")

        # Consume from all topics in parallel
        async def consume_topic(key, consumer, handler):
            async for msg in consumer:
                try:
                    output = handler(msg.value)
                    print(output)
                except Exception as e:
                    logger.error(f"Error processing {key} message: {e}")

        # Start consuming tasks
        tasks = [
            consume_topic("raw", consumers["raw"], tracer.track_raw_event),
            consume_topic("enrichment", consumers["enrichment"], tracer.track_enrichment_pending),
            consume_topic("entities", consumers["entities"], tracer.track_entity_rows),
            consume_topic("itel", consumers["itel"], tracer.track_itel_message),
        ]

        await asyncio.gather(*tasks)

    except KeyboardInterrupt:
        print("\n\nStopped by user")
    finally:
        # Stop all consumers
        for consumer in consumers.values():
            await consumer.stop()

        # Print summary
        tracer.print_summary()


if __name__ == "__main__":
    asyncio.run(main())
