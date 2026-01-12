#!/usr/bin/env python3
"""
Test iTel Plugin Execution with Real Data

Simulates plugin execution using your actual event data to see why it's not triggering.
"""

import sys
import json
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka_pipeline.plugins.shared.base import Domain, PipelineStage, PluginContext
from kafka_pipeline.plugins.shared.loader import load_plugins_from_directory
from kafka_pipeline.plugins.shared.registry import get_plugin_registry, PluginOrchestrator
from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage
from kafka_pipeline.claimx.schemas.tasks import ClaimXEnrichmentTask

# Your actual data from the trace
ENRICHMENT_TASK = {
    "event_id": "405d39191f29c87710ef18917e41d4ea72be5ff588029727f837ab897394ad3f",
    "event_type": "CUSTOM_TASK_COMPLETED",
    "project_id": "5395115",
    "retry_count": 0,
    "created_at": "2026-01-12T22:30:10.297436+00:00",
    "media_id": None,
    "task_assignment_id": "5423878",
    "video_collaboration_id": None,
    "master_file_name": None,
    "metadata": None
}

ENTITY_ROWS = {
    "event_id": "405d39191f29c87710ef18917e41d4ea72be5ff588029727f837ab897394ad3f",
    "event_type": "CUSTOM_TASK_COMPLETED",
    "project_id": "5395115",
    "tasks": [
        {
            "assignment_id": 5423878,
            "task_id": 32513,
            "task_name": "ITEL Cabinet Repair Form",
            "form_id": "69417fcfa5e6b152c25253d0",
            "project_id": "5395115",
            "assignee_id": None,
            "assignor_id": 485806,
            "assignor_email": "nsmkd@allstate.com",
            "date_assigned": "2026-01-11T15:39:27.526+00:00",
            "date_completed": "2026-01-11T19:19:19.974+00:00",
            "cancelled_date": None,
            "cancelled_by_resource_id": None,
            "status": "COMPLETED",
        }
    ],
    "projects": [{"project_id": "5395115"}],
    "contacts": [],
    "media": [],
    "task_templates": [],
    "external_links": [],
    "video_collab": []
}

def test_plugin_execution():
    """Test plugin execution with your actual event data."""
    print("=" * 80)
    print("iTel Plugin Execution Test")
    print("=" * 80)
    print()

    # 1. Load plugin
    print("1. Loading plugin...")
    registry = get_plugin_registry()
    plugins = load_plugins_from_directory("config/plugins", registry)

    itel_plugin = None
    for plugin in plugins:
        if plugin.name == "itel_cabinet_api":
            itel_plugin = plugin
            break

    if not itel_plugin:
        print("   ✗ FAILED: iTel plugin not found!")
        return

    print(f"   ✓ Plugin loaded: {itel_plugin.name}")
    print(f"     - Enabled: {itel_plugin.enabled}")
    print(f"     - Event types: {itel_plugin.event_types}")
    print(f"     - Domains: {[d.value for d in itel_plugin.domains]}")
    print(f"     - Stages: {[s.value for s in itel_plugin.stages]}")
    print()

    # 2. Create entity rows
    print("2. Creating EntityRowsMessage from your trace data...")
    try:
        entity_rows = EntityRowsMessage(**ENTITY_ROWS)
        print(f"   ✓ EntityRowsMessage created")
        print(f"     - Tasks: {len(entity_rows.tasks)}")
        if entity_rows.tasks:
            task = entity_rows.tasks[0]
            print(f"     - First task_id: {task.get('task_id')} (type: {type(task.get('task_id'))})")
            print(f"     - Task status: {task.get('status')}")
    except Exception as e:
        print(f"   ✗ FAILED: {e}")
        return
    print()

    # 3. Create plugin context
    print("3. Creating PluginContext (simulating enrichment worker)...")
    try:
        task_obj = ClaimXEnrichmentTask(**ENRICHMENT_TASK)

        context = PluginContext(
            domain=Domain.CLAIMX,
            stage=PipelineStage.ENRICHMENT_COMPLETE,
            message=task_obj,
            event_id=task_obj.event_id,
            event_type=task_obj.event_type,
            project_id=task_obj.project_id,
            data={
                "entities": entity_rows,
                "handler_result": {},
            },
            headers={},
            timestamp=datetime.now(timezone.utc),
        )
        print(f"   ✓ PluginContext created")
        print(f"     - Domain: {context.domain.value}")
        print(f"     - Stage: {context.stage.value}")
        print(f"     - Event type: {context.event_type}")
        print(f"     - Project ID: {context.project_id}")
    except Exception as e:
        print(f"   ✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return
    print()

    # 4. Check if plugin should run
    print("4. Checking plugin.should_run()...")
    should_run = itel_plugin.should_run(context)
    print(f"   Result: {should_run}")

    if not should_run:
        print()
        print("   ⚠️  PLUGIN WILL NOT RUN - Checking why:")
        print(f"     - Plugin enabled? {itel_plugin.enabled}")
        print(f"     - Domain match? {context.domain in itel_plugin.domains if itel_plugin.domains else 'no filter'}")
        print(f"     - Stage match? {context.stage in itel_plugin.stages if itel_plugin.stages else 'no filter'}")
        print(f"     - Event type match? {context.event_type in itel_plugin.event_types if itel_plugin.event_types else 'no filter'}")
        print()
        print(f"   Expected:")
        print(f"     - Domain: {itel_plugin.domains}")
        print(f"     - Stage: {itel_plugin.stages}")
        print(f"     - Event types: {itel_plugin.event_types}")
        print()
        print(f"   Actual:")
        print(f"     - Domain: {context.domain}")
        print(f"     - Stage: {context.stage}")
        print(f"     - Event type: '{context.event_type}'")
        return

    print(f"   ✓ Plugin should run")
    print()

    # 5. Get task from context
    print("5. Getting task from context (context.get_first_task())...")
    task = context.get_first_task()
    if not task:
        print("   ✗ FAILED: No task found in context!")
        print("     This is the problem - plugin expects task in entities.tasks")
        entities = context.get_claimx_entities()
        if entities:
            print(f"     - Entity rows exists: True")
            print(f"     - Tasks count: {len(entities.tasks)}")
            print(f"     - Tasks data: {entities.tasks}")
        else:
            print(f"     - Entity rows: None")
        return

    print(f"   ✓ Task found")
    print(f"     - task_id: {task.get('task_id')} (type: {type(task.get('task_id'))})")
    print(f"     - task_name: {task.get('task_name')}")
    print(f"     - status: {task.get('status')}")
    print()

    # 6. Check trigger configuration
    print("6. Checking trigger configuration...")
    task_id = task.get('task_id')
    triggers = itel_plugin.config.get('triggers', {})
    print(f"   - Available triggers: {list(triggers.keys())}")
    print(f"   - Looking for task_id: {task_id} (type: {type(task_id)})")

    trigger_config = triggers.get(task_id) or triggers.get(str(task_id))
    if not trigger_config:
        print(f"   ✗ FAILED: No trigger found for task_id {task_id}")
        print(f"     Tried: triggers[{task_id}] and triggers['{task_id}']")
        return

    print(f"   ✓ Trigger found: {trigger_config.get('name')}")
    print(f"     - on_any: {bool(trigger_config.get('on_any'))}")
    print(f"     - on_completed: {bool(trigger_config.get('on_completed'))}")
    print(f"     - on_assigned: {bool(trigger_config.get('on_assigned'))}")
    print()

    # 7. Execute plugin
    print("7. Executing plugin...")
    try:
        import asyncio

        async def run_plugin():
            result = await itel_plugin.execute(context)
            return result

        result = asyncio.run(run_plugin())

        print(f"   ✓ Plugin executed")
        print(f"     - Success: {result.success}")
        print(f"     - Message: {result.message}")
        print(f"     - Actions: {len(result.actions)}")

        if result.actions:
            print()
            print("   Actions to execute:")
            for i, action in enumerate(result.actions, 1):
                print(f"     {i}. {action.action_type.value}")
                if action.action_type.value == "publish_to_topic":
                    print(f"        - Topic: {action.params.get('topic')}")
                    print(f"        - Payload keys: {list(action.params.get('payload', {}).keys())}")

    except Exception as e:
        print(f"   ✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return

    print()
    print("=" * 80)
    print("DIAGNOSIS:")
    print("=" * 80)
    print()

    if result.success and result.actions:
        print("✓ Plugin execution successful!")
        print()
        print("The plugin SHOULD be publishing to itel.cabinet.task.tracking.")
        print()
        print("If messages aren't reaching the topic, the issue is likely:")
        print("  1. Producer is not configured in the enrichment worker")
        print("  2. Action executor has no producer (logs 'No producer configured')")
        print("  3. Topic publish succeeds but messages aren't committed")
        print()
        print("Check enrichment worker logs for:")
        print("  - 'Plugin action: publish to topic'")
        print("  - 'No producer configured - publish action logged only'")
    else:
        print("✗ Plugin execution failed or returned no actions")
        print()
        print(f"Result: {result.message}")

if __name__ == "__main__":
    test_plugin_execution()
