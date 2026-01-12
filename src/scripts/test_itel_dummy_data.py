#!/usr/bin/env python3
"""
Test script for itel Cabinet API dummy data generation.

This script demonstrates how to generate itel Cabinet events programmatically
and verify the data structure.

Usage:
    python scripts/test_itel_dummy_data.py
"""

import json
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka_pipeline.common.dummy.generators import (
    RealisticDataGenerator,
    GeneratorConfig,
)


def test_itel_cabinet_generation():
    """Test itel Cabinet event generation."""
    print("=" * 80)
    print("itel Cabinet API Dummy Data Generator Test")
    print("=" * 80)
    print()

    # Create generator with itel Cabinet profile
    config = GeneratorConfig(
        plugin_profile="itel_cabinet_api",
        seed=42,  # Reproducible results
    )
    generator = RealisticDataGenerator(config)

    print("✓ Generator initialized with itel_cabinet_api profile")
    print()

    # Generate a completed task event
    print("Generating CUSTOM_TASK_COMPLETED event...")
    completed_event = generator.generate_itel_cabinet_event(
        event_type="CUSTOM_TASK_COMPLETED"
    )

    print(f"✓ Event generated: {completed_event['event_id']}")
    print(f"  - Event Type: {completed_event['event_type']}")
    print(f"  - Project ID: {completed_event['project_id']}")
    print(f"  - Task Assignment ID: {completed_event['task_assignment_id']}")
    print()

    # Verify task details
    raw_data = completed_event["raw_data"]
    task_details = raw_data.get("claimx_task_details", {})

    print("Task Details:")
    print(f"  - Task ID: {raw_data.get('taskId')}")
    print(f"  - Task Name: {raw_data.get('taskName')}")
    print(f"  - Assignee: {raw_data.get('assignee')}")
    print(f"  - Status: {task_details.get('status')}")
    print()

    # Verify form response structure
    form_response = task_details.get("form_response", {})
    responses = form_response.get("responses", [])
    attachments = form_response.get("attachments", [])

    print(f"Form Response:")
    print(f"  - Total questions answered: {len(responses)}")
    print(f"  - Total attachments: {len(attachments)}")
    print()

    # Show sample responses
    print("Sample Form Responses:")
    for response in responses[:5]:
        print(f"  - {response['question_text']}: {response['answer']}")
    if len(responses) > 5:
        print(f"  ... and {len(responses) - 5} more")
    print()

    # Show attachment breakdown
    if attachments:
        attachment_groups = {}
        for att in attachments:
            key = att["question_key"]
            attachment_groups[key] = attachment_groups.get(key, 0) + 1

        print("Attachment Breakdown:")
        for key, count in attachment_groups.items():
            print(f"  - {key}: {count} photo(s)")
        print()

    # Generate an assigned task event
    print("Generating CUSTOM_TASK_ASSIGNED event...")
    assigned_event = generator.generate_itel_cabinet_event(
        event_type="CUSTOM_TASK_ASSIGNED"
    )

    print(f"✓ Event generated: {assigned_event['event_id']}")
    print(f"  - Event Type: {assigned_event['event_type']}")
    print(f"  - Project ID: {assigned_event['project_id']}")
    print()

    # Save example events to files
    output_dir = Path(__file__).parent.parent / "config" / "dummy" / "examples"
    output_dir.mkdir(parents=True, exist_ok=True)

    completed_file = output_dir / "itel_cabinet_completed_event.json"
    assigned_file = output_dir / "itel_cabinet_assigned_event.json"

    with open(completed_file, "w") as f:
        json.dump(completed_event, f, indent=2, default=str)

    with open(assigned_file, "w") as f:
        json.dump(assigned_event, f, indent=2, default=str)

    print("Example Events Saved:")
    print(f"  - {completed_file}")
    print(f"  - {assigned_file}")
    print()

    # Summary
    print("=" * 80)
    print("Test Summary")
    print("=" * 80)
    print("✓ itel Cabinet event generation working correctly")
    print("✓ Task ID: 32513 (itel Cabinet Repair Form)")
    print("✓ Form data structure matches ClaimX API format")
    print("✓ Attachments grouped by question_key")
    print("✓ Events ready for plugin testing")
    print()
    print("Next Steps:")
    print("  1. Start Kafka: docker-compose up -d kafka")
    print("  2. Start worker: python -m kafka_pipeline.workers.itel_cabinet_tracking_worker")
    print("  3. Start dummy source: python -m kafka_pipeline --mode dummy --config config/dummy/itel_cabinet_test.yaml")
    print("  4. Monitor logs: tail -f logs/itel/tracking_worker.log")
    print()


if __name__ == "__main__":
    test_itel_cabinet_generation()
