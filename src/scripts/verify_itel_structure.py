#!/usr/bin/env python3
"""
Simple verification script for itel Cabinet data structure.

This script verifies the code structure without requiring all dependencies.

Usage:
    python scripts/verify_itel_structure.py
"""

import ast
import sys
from pathlib import Path


def verify_file_structure():
    """Verify that the itel Cabinet profile files are properly structured."""
    print("=" * 80)
    print("itel Cabinet API Dummy Data Structure Verification")
    print("=" * 80)
    print()

    src_dir = Path(__file__).parent.parent
    checks = []

    # Check 1: Plugin profiles file exists
    plugin_profiles_file = src_dir / "kafka_pipeline" / "common" / "dummy" / "plugin_profiles.py"
    if plugin_profiles_file.exists():
        checks.append(("✓", "plugin_profiles.py exists"))

        # Parse and check for key classes
        with open(plugin_profiles_file) as f:
            tree = ast.parse(f.read())

        classes = [node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)]

        if "ItelCabinetDataGenerator" in classes:
            checks.append(("✓", "ItelCabinetDataGenerator class found"))
        else:
            checks.append(("✗", "ItelCabinetDataGenerator class NOT found"))

        if "ItelCabinetFormData" in classes:
            checks.append(("✓", "ItelCabinetFormData class found"))
        else:
            checks.append(("✗", "ItelCabinetFormData class NOT found"))

        if "PluginProfile" in classes:
            checks.append(("✓", "PluginProfile enum found"))
        else:
            checks.append(("✗", "PluginProfile enum NOT found"))
    else:
        checks.append(("✗", "plugin_profiles.py does NOT exist"))

    # Check 2: generators.py updated
    generators_file = src_dir / "kafka_pipeline" / "common" / "dummy" / "generators.py"
    if generators_file.exists():
        checks.append(("✓", "generators.py exists"))

        with open(generators_file) as f:
            content = f.read()

        if "generate_itel_cabinet_event" in content:
            checks.append(("✓", "generate_itel_cabinet_event method found"))
        else:
            checks.append(("✗", "generate_itel_cabinet_event method NOT found"))

        if "plugin_profile" in content:
            checks.append(("✓", "plugin_profile support added"))
        else:
            checks.append(("✗", "plugin_profile support NOT added"))
    else:
        checks.append(("✗", "generators.py does NOT exist"))

    # Check 3: source.py updated
    source_file = src_dir / "kafka_pipeline" / "common" / "dummy" / "source.py"
    if source_file.exists():
        checks.append(("✓", "source.py exists"))

        with open(source_file) as f:
            content = f.read()

        if 'plugin_profile == "itel_cabinet_api"' in content:
            checks.append(("✓", "itel Cabinet profile handling added"))
        else:
            checks.append(("✗", "itel Cabinet profile handling NOT added"))
    else:
        checks.append(("✗", "source.py does NOT exist"))

    # Check 4: Example config exists
    config_file = src_dir / "config" / "dummy" / "itel_cabinet_test.yaml"
    if config_file.exists():
        checks.append(("✓", "itel_cabinet_test.yaml config exists"))

        with open(config_file) as f:
            content = f.read()

        if "itel_cabinet_api" in content:
            checks.append(("✓", "Config references itel_cabinet_api profile"))
        else:
            checks.append(("✗", "Config does NOT reference itel_cabinet_api profile"))
    else:
        checks.append(("✗", "itel_cabinet_test.yaml does NOT exist"))

    # Check 5: Documentation exists
    doc_file = src_dir / "kafka_pipeline" / "common" / "dummy" / "PLUGIN_TESTING.md"
    if doc_file.exists():
        checks.append(("✓", "PLUGIN_TESTING.md documentation exists"))
    else:
        checks.append(("✗", "PLUGIN_TESTING.md does NOT exist"))

    # Print results
    print("Verification Results:")
    print()
    for status, message in checks:
        print(f"  {status} {message}")
    print()

    # Summary
    passed = sum(1 for status, _ in checks if status == "✓")
    total = len(checks)

    print("=" * 80)
    print(f"Summary: {passed}/{total} checks passed")
    print("=" * 80)
    print()

    if passed == total:
        print("✓ All checks passed! itel Cabinet profile is ready for use.")
        print()
        print("Usage:")
        print("  1. Configure: Edit config/dummy/itel_cabinet_test.yaml")
        print("  2. Run: python -m kafka_pipeline --mode dummy --config config/dummy/itel_cabinet_test.yaml")
        print("  3. Or use programmatically:")
        print()
        print("     from kafka_pipeline.common.dummy.generators import GeneratorConfig, RealisticDataGenerator")
        print('     config = GeneratorConfig(plugin_profile="itel_cabinet_api")')
        print("     generator = RealisticDataGenerator(config)")
        print("     event = generator.generate_itel_cabinet_event()")
        print()
        return 0
    else:
        print("✗ Some checks failed. Review the output above.")
        print()
        return 1


if __name__ == "__main__":
    sys.exit(verify_file_structure())
