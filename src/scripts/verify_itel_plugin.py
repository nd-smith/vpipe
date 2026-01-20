#!/usr/bin/env python3
"""
Verify iTel Cabinet Plugin Configuration

This script checks if the iTel cabinet plugin is properly configured and loaded.
"""

import sys
import yaml
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka_pipeline.plugins.shared.registry import get_plugin_registry
from kafka_pipeline.plugins.shared.loader import load_plugins_from_directory

def check_plugin_config():
    """Check if plugin config file exists and is valid."""
    config_path = Path("config/plugins/claimx/itel_cabinet_api/config.yaml")

    print("=" * 80)
    print("iTel Cabinet Plugin Configuration Checker")
    print("=" * 80)
    print()

    # Check config file exists
    print(f"1. Checking config file: {config_path}")
    if not config_path.exists():
        print(f"   ✗ FAILED: Config file not found at {config_path}")
        return False
    print(f"   ✓ Config file exists")

    # Load and validate config
    print()
    print("2. Loading plugin configuration...")
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)

        print(f"   ✓ Config loaded successfully")
        print(f"   Plugin name: {config.get('name')}")
        print(f"   Module: {config.get('module')}")
        print(f"   Class: {config.get('class')}")
        print(f"   Enabled: {config.get('enabled')}")
        print(f"   Priority: {config.get('priority')}")

        # Check triggers
        triggers = config.get('config', {}).get('triggers', {})
        if 32513 in triggers or '32513' in triggers:
            print(f"   ✓ Trigger for task_id 32513 found")
            trigger = triggers.get(32513) or triggers.get('32513')
            print(f"     - Name: {trigger.get('name')}")

            if 'on_any' in trigger:
                print(f"     - on_any configured:")
                print(f"       - Topic: {trigger['on_any'].get('publish_to_topic')}")
            if 'on_completed' in trigger:
                print(f"     - on_completed configured")
            if 'on_assigned' in trigger:
                print(f"     - on_assigned configured")
        else:
            print(f"   ✗ WARNING: No trigger for task_id 32513 found")
            print(f"     Available triggers: {list(triggers.keys())}")

    except Exception as e:
        print(f"   ✗ FAILED: Error loading config: {e}")
        return False

    # Try loading plugin
    print()
    print("3. Attempting to load plugin...")
    try:
        registry = get_plugin_registry()
        plugins = load_plugins_from_directory("config/plugins", registry)

        print(f"   ✓ Loaded {len(plugins)} plugin(s)")
        for plugin in plugins:
            print(f"     - {plugin.name} (enabled={plugin.enabled})")
            if plugin.name == "itel_cabinet_api":
                print(f"       ✓ iTel plugin loaded successfully!")
                print(f"       - Domains: {[d.value for d in plugin.domains]}")
                print(f"       - Stages: {[s.value for s in plugin.stages]}")
                print(f"       - Event types: {plugin.event_types}")
                print(f"       - Priority: {plugin.priority}")

                # Check trigger config
                triggers = plugin.config.get('triggers', {})
                if 32513 in triggers or '32513' in triggers:
                    print(f"       ✓ Task 32513 trigger configured in plugin instance")
                else:
                    print(f"       ✗ WARNING: Task 32513 trigger NOT in plugin instance")
                    print(f"         Available: {list(triggers.keys())}")

    except Exception as e:
        print(f"   ✗ FAILED: Error loading plugin: {e}")
        import traceback
        traceback.print_exc()
        return False

    print()
    print("=" * 80)
    print("RECOMMENDATION:")
    print("=" * 80)
    print()

    if plugins and any(p.name == "itel_cabinet_api" for p in plugins):
        print("✓ Plugin configuration looks good!")
        print()
        print("The issue is likely that:")
        print("  1. The enrichment worker is not running with plugins enabled")
        print("  2. OR the plugin loads but doesn't execute (check event_type filtering)")
        print()
        print("Next steps:")
        print("  - Check enrichment worker logs for plugin loading messages")
        print("  - Look for: 'Plugin orchestrator initialized'")
        print("  - Look for: 'Loaded plugins from directory'")
        print("  - Ensure event_type is exactly 'CUSTOM_TASK_COMPLETED'")
    else:
        print("✗ Plugin is NOT loading correctly")
        print()
        print("Check:")
        print("  - config/plugins/claimx/itel_cabinet_api/config.yaml exists")
        print("  - enabled: true is set")
        print("  - triggers.32513 is configured")

    print()

if __name__ == "__main__":
    check_plugin_config()
