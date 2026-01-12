#!/usr/bin/env python3
"""
Inspect Entity Rows for CUSTOM_TASK_COMPLETED Events

Consumes from claimx.entities.rows and shows detailed breakdown
of what entity types are being written and how often.

Helps diagnose:
- Why project rows appear multiple times
- Whether task rows have the correct task_id
- Whether the plugin can access task data
"""

import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from collections import defaultdict

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from aiokafka import AIOKafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Consume and analyze entity rows."""
    kafka_brokers = os.getenv("KAFKA_BROKERS", "localhost:9094")

    print("=" * 80)
    print("Entity Rows Inspector")
    print("=" * 80)
    print(f"\nKafka brokers: {kafka_brokers}")
    print(f"Topic: claimx.entities.rows")
    print(f"\nPress Ctrl+C to stop")
    print("=" * 80)
    print()

    consumer = AIOKafkaConsumer(
        "claimx.entities.rows",
        bootstrap_servers=kafka_brokers,
        group_id="inspect_entity_rows",
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    )

    stats = defaultdict(int)
    project_writes = defaultdict(int)  # Track project_id write frequency
    task_events = []

    try:
        await consumer.start()
        print("✓ Consumer started\n")

        async for msg in consumer:
            entity_rows = msg.value

            event_id = entity_rows.get("event_id", "unknown")
            event_type = entity_rows.get("event_type", "unknown")
            project_id = entity_rows.get("project_id", "unknown")

            # Extract entity counts
            projects = entity_rows.get("projects", [])
            contacts = entity_rows.get("contacts", [])
            media = entity_rows.get("media", [])
            tasks = entity_rows.get("tasks", [])
            task_templates = entity_rows.get("task_templates", [])
            external_links = entity_rows.get("external_links", [])
            video_collab = entity_rows.get("video_collab", [])

            # Track project writes
            if projects:
                for proj in projects:
                    proj_id = proj.get("project_id", "unknown")
                    project_writes[proj_id] += 1

            # Track task events
            if event_type == "CUSTOM_TASK_COMPLETED":
                task_ids = [t.get("task_id") for t in tasks]
                task_events.append({
                    "event_id": event_id,
                    "project_id": project_id,
                    "task_ids": task_ids,
                    "has_itel_task": 32513 in task_ids,
                })

            stats[f"event_type_{event_type}"] += 1
            stats["total_entity_rows"] += 1
            stats["with_projects"] += 1 if projects else 0
            stats["with_tasks"] += 1 if tasks else 0
            stats["with_contacts"] += 1 if contacts else 0
            stats["with_media"] += 1 if media else 0

            # Print detailed info
            print(f"[{event_type}] event_id={event_id[:8]}... | project_id={project_id}")
            print(f"  └─ Entities: projects={len(projects)} tasks={len(tasks)} "
                  f"contacts={len(contacts)} media={len(media)} "
                  f"templates={len(task_templates)} links={len(external_links)}")

            # If this is a task event, show task details
            if tasks:
                for task in tasks:
                    task_id = task.get("task_id")
                    assignment_id = task.get("assignment_id")
                    task_name = task.get("task_name", "")[:40]
                    status = task.get("status")
                    is_itel = task_id == 32513
                    marker = "🎯 ITEL TASK" if is_itel else ""
                    print(f"     ├─ Task: id={task_id} | assignment={assignment_id} | "
                          f"status={status} | name={task_name} {marker}")

            # If this event has project rows, note it
            if projects:
                for proj in projects:
                    proj_id = proj.get("project_id")
                    proj_name = proj.get("project_name", "")[:40]
                    write_count = project_writes[proj_id]
                    marker = f" (⚠️  written {write_count}x)" if write_count > 1 else ""
                    print(f"     └─ Project: id={proj_id} | name={proj_name}{marker}")

            print()

    except KeyboardInterrupt:
        print("\n\nStopped by user")
    finally:
        await consumer.stop()

        # Print summary
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)

        print("\nEntity Row Stats:")
        for key, value in sorted(stats.items()):
            print(f"  {key}: {value}")

        print("\n" + "-" * 80)
        print("Project Write Frequency:")
        print("-" * 80)
        frequent_projects = [(pid, count) for pid, count in project_writes.items() if count > 1]
        if frequent_projects:
            print(f"\n⚠️  {len(frequent_projects)} projects written multiple times:")
            for proj_id, count in sorted(frequent_projects, key=lambda x: x[1], reverse=True)[:10]:
                print(f"   - project_id={proj_id}: {count} writes")
            print(f"\n   → This is expected: task events trigger project verification")
            print(f"   → Each CUSTOM_TASK_COMPLETED = 1 project write (in-flight verification)")
        else:
            print("✓ No projects written multiple times")

        print("\n" + "-" * 80)
        print("CUSTOM_TASK_COMPLETED Events:")
        print("-" * 80)
        if task_events:
            itel_events = [e for e in task_events if e["has_itel_task"]]
            print(f"\nTotal CUSTOM_TASK_COMPLETED events: {len(task_events)}")
            print(f"Events with iTel task (32513): {len(itel_events)}")

            if itel_events:
                print("\niTel Task Events:")
                for e in itel_events[:10]:
                    print(f"   - event_id={e['event_id'][:8]}... | project_id={e['project_id']} | "
                          f"task_ids={e['task_ids']}")
        else:
            print("No CUSTOM_TASK_COMPLETED events seen")

        print("\n" + "=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
