#!/usr/bin/env python3
"""
Check Plugin Configuration

Verifies that the iTel Cabinet TaskTriggerPlugin is properly:
1. Loaded and registered
2. Configured with correct task_id (32513)
3. Will trigger on CUSTOM_TASK_COMPLETED events
4. Has correct output topic (itel.cabinet.task.tracking)
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka_pipeline.plugins.shared.registry import get_plugin_registry
from kafka_pipeline.plugins.shared.loader import load_plugins_from_directory
from kafka_pipeline.plugins.shared.base import Domain, PipelineStage


def main():
    """Check plugin configuration."""
    print("=" * 80)
    print("Plugin Configuration Checker")
    print("=" * 80)
    print()

    # Load plugins
    plugins_dir = Path(__file__).parent.parent / "config" / "plugins"
    print(f"Loading plugins from: {plugins_dir}")
    print()

    registry = get_plugin_registry()
    loaded_plugins = load_plugins_from_directory(str(plugins_dir), registry)

    print(f"✓ Loaded {len(loaded_plugins)} plugins")
    print()

    # Find iTel Cabinet plugin
    itel_plugin = None
    for plugin in loaded_plugins:
        if hasattr(plugin, '__class__') and plugin.__class__.__name__ == "TaskTriggerPlugin":
            triggers = plugin.config.get("triggers", {})
            if 32513 in triggers or "32513" in triggers:
                itel_plugin = plugin
                break

    if not itel_plugin:
        print("❌ ERROR: iTel Cabinet plugin NOT FOUND")
        print()
        print("Available plugins:")
        for plugin in loaded_plugins:
            print(f"  - {plugin.name} ({plugin.__class__.__name__})")
        print()
        print("Troubleshooting:")
        print("  1. Check config/plugins/claimx/itel_cabinet_api/config.yaml exists")
        print("  2. Verify 'enabled: true' in config")
        print("  3. Check task_id 32513 is in triggers")
        return 1

    print("✓ iTel Cabinet plugin FOUND")
    print()

    # Check plugin configuration
    print("-" * 80)
    print("Plugin Details:")
    print("-" * 80)
    print(f"Name: {itel_plugin.name}")
    print(f"Class: {itel_plugin.__class__.__name__}")
    print(f"Enabled: {itel_plugin.enabled}")
    print(f"Priority: {itel_plugin.priority}")
    print()

    # Check filtering
    print("-" * 80)
    print("Plugin Filtering:")
    print("-" * 80)
    print(f"Domains: {itel_plugin.domains or 'ALL'}")
    print(f"Stages: {itel_plugin.stages or 'ALL'}")
    print(f"Event Types: {itel_plugin.event_types or 'ALL'}")
    print()

    # Check if it will run for CUSTOM_TASK_COMPLETED
    from kafka_pipeline.plugins.shared.base import PluginContext
    from datetime import datetime, timezone

    test_context = PluginContext(
        domain=Domain.CLAIMX,
        stage=PipelineStage.ENRICHMENT_COMPLETE,
        message={},
        event_id="test-event",
        event_type="CUSTOM_TASK_COMPLETED",
        project_id="12345",
        data={},
        headers={},
        timestamp=datetime.now(timezone.utc),
    )

    should_run = itel_plugin.should_run(test_context)
    if should_run:
        print("✓ Plugin WILL run for CUSTOM_TASK_COMPLETED at ENRICHMENT_COMPLETE stage")
    else:
        print("❌ Plugin WILL NOT run for CUSTOM_TASK_COMPLETED")
        print("   Check domains/stages/event_types filters")
    print()

    # Check trigger configuration
    print("-" * 80)
    print("Trigger Configuration:")
    print("-" * 80)
    triggers = itel_plugin.config.get("triggers", {})
    trigger_32513 = triggers.get(32513) or triggers.get("32513")

    if not trigger_32513:
        print("❌ ERROR: No trigger configured for task_id=32513")
        print()
        print("Available triggers:")
        for task_id in triggers.keys():
            print(f"  - task_id={task_id}")
        return 1

    print(f"✓ Trigger found for task_id=32513")
    print()
    print(f"Trigger Name: {trigger_32513.get('name', 'N/A')}")
    print(f"Description: {trigger_32513.get('description', 'N/A')}")
    print()

    # Check actions
    print("Actions:")
    actions = []
    if trigger_32513.get("on_assigned"):
        actions.append("on_assigned")
    if trigger_32513.get("on_completed"):
        actions.append("on_completed")
    if trigger_32513.get("on_any"):
        actions.append("on_any")

    if not actions:
        print("  ❌ No actions configured")
        return 1

    print(f"  Configured for: {', '.join(actions)}")
    print()

    # Check on_any action (should be the one used)
    on_any = trigger_32513.get("on_any", {})
    if on_any:
        print("  on_any action:")
        topic = on_any.get("publish_to_topic")
        if topic:
            print(f"    ✓ publish_to_topic: {topic}")
            if topic != "itel.cabinet.task.tracking":
                print(f"      ⚠️  Expected: itel.cabinet.task.tracking")
        else:
            print(f"    ❌ No publish_to_topic configured")

        if "log" in on_any:
            print(f"    ✓ log action configured")
        if "webhook" in on_any:
            print(f"    ✓ webhook configured: {on_any['webhook']}")
    else:
        print("  ⚠️  'on_any' action not configured")
        print("     Plugin will only trigger on specific events (on_assigned/on_completed)")

    print()

    # Check payload configuration
    print("-" * 80)
    print("Payload Configuration:")
    print("-" * 80)
    include_task_data = itel_plugin.config.get("include_task_data", True)
    include_project_data = itel_plugin.config.get("include_project_data", False)

    print(f"Include task data: {include_task_data}")
    print(f"Include project data: {include_project_data}")
    print()

    # Summary
    print("=" * 80)
    print("CONFIGURATION SUMMARY")
    print("=" * 80)

    issues = []
    if not itel_plugin.enabled:
        issues.append("Plugin is DISABLED")
    if not should_run:
        issues.append("Plugin won't run for CUSTOM_TASK_COMPLETED events")
    if not trigger_32513:
        issues.append("No trigger for task_id=32513")
    if not on_any.get("publish_to_topic"):
        issues.append("No Kafka topic configured for publishing")

    if issues:
        print("\n❌ ISSUES FOUND:")
        for issue in issues:
            print(f"   - {issue}")
        print()
        return 1
    else:
        print("\n✓ Plugin configuration looks GOOD")
        print()
        print("Expected flow:")
        print("  1. CUSTOM_TASK_COMPLETED event received")
        print("  2. TaskHandler enriches with ClaimX API data")
        print("  3. TaskTriggerPlugin checks task_id in entity rows")
        print("  4. If task_id=32513, publish to itel.cabinet.task.tracking")
        print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
