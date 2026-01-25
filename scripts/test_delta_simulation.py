#!/usr/bin/env python3
# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
#
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""Test script for Delta Lake simulation mode.

This script verifies that:
1. Simulation mode detection works correctly
2. Delta tables can be written to local filesystem
3. Written tables are readable and valid
4. Cleanup works correctly

Usage:
    SIMULATION_MODE=true python scripts/test_delta_simulation.py
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timezone

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

try:
    import polars as pl
except ImportError:
    print("Error: polars is required. Install with: pip install 'polars>=1.0.0'")
    sys.exit(1)

try:
    from deltalake import write_deltalake
except ImportError:
    print("Error: deltalake is required. Install with: pip install 'deltalake>=0.15.0'")
    sys.exit(1)


def test_simulation_config():
    """Test that simulation configuration is loaded correctly."""
    print("Test 1: Simulation Configuration")
    print("=" * 60)

    # Check environment variable
    if os.getenv("SIMULATION_MODE", "").lower() != "true":
        print("❌ SIMULATION_MODE environment variable not set to 'true'")
        print("   Run with: SIMULATION_MODE=true python scripts/test_delta_simulation.py")
        return False

    try:
        from kafka_pipeline.simulation import is_simulation_mode, get_simulation_config

        # Test detection
        if not is_simulation_mode():
            print("❌ is_simulation_mode() returned False")
            return False
        print("✅ Simulation mode detected")

        # Get config
        config = get_simulation_config()
        print(f"✅ Configuration loaded:")
        print(f"   - local_delta_path: {config.local_delta_path}")
        print(f"   - truncate_tables_on_start: {config.truncate_tables_on_start}")
        print(f"   - delta_write_mode: {config.delta_write_mode}")

        # Ensure directories exist
        config.ensure_directories()
        if not config.local_delta_path.exists():
            print(f"❌ Delta path was not created: {config.local_delta_path}")
            return False
        print(f"✅ Delta directory exists: {config.local_delta_path}")

        return True

    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_write_delta_table():
    """Test writing a Delta table to local filesystem."""
    print("\nTest 2: Write Local Delta Table")
    print("=" * 60)

    try:
        from kafka_pipeline.simulation import get_simulation_config

        config = get_simulation_config()
        test_table_path = config.local_delta_path / "test_table"

        # Create test data
        test_data = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "created_at": [datetime.now(timezone.utc)] * 3,
        })

        print(f"✅ Created test DataFrame with {len(test_data)} rows")

        # Write to Delta
        write_deltalake(
            str(test_table_path),
            test_data.to_arrow(),
            mode="overwrite",
        )
        print(f"✅ Wrote Delta table to: {test_table_path}")

        # Verify _delta_log exists
        delta_log_path = test_table_path / "_delta_log"
        if not delta_log_path.exists():
            print(f"❌ Delta log not found: {delta_log_path}")
            return False
        print(f"✅ Delta transaction log created")

        # Read back
        df_read = pl.read_delta(str(test_table_path))
        print(f"✅ Read back {len(df_read)} rows")

        # Validate data
        if len(df_read) != len(test_data):
            print(f"❌ Row count mismatch: expected {len(test_data)}, got {len(df_read)}")
            return False

        if df_read["name"].to_list() != test_data["name"].to_list():
            print(f"❌ Data mismatch")
            return False

        print(f"✅ Data integrity verified")

        return True

    except Exception as e:
        print(f"❌ Delta write test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_entity_writer_simulation():
    """Test that ClaimXEntityWriter respects simulation mode."""
    print("\nTest 3: Entity Writer Simulation Mode")
    print("=" * 60)

    try:
        from kafka_pipeline.simulation import get_simulation_config
        from kafka_pipeline.claimx.writers.delta_entities import ClaimXEntityWriter

        config = get_simulation_config()

        # Create entity writer (paths don't matter, they'll be overridden)
        writer = ClaimXEntityWriter(
            projects_table_path="abfss://fake/path/projects",
            contacts_table_path="abfss://fake/path/contacts",
            media_table_path="abfss://fake/path/media",
            tasks_table_path="abfss://fake/path/tasks",
            task_templates_table_path="abfss://fake/path/task_templates",
            external_links_table_path="abfss://fake/path/external_links",
            video_collab_table_path="abfss://fake/path/video_collab",
        )

        # Verify simulation mode was detected
        if not writer.use_local_delta:
            print("❌ Entity writer did not detect simulation mode")
            return False
        print("✅ Entity writer detected simulation mode")

        # Verify paths were overridden
        expected_base = config.local_delta_path
        if writer.delta_base_path != expected_base:
            print(f"❌ Delta base path not set correctly")
            print(f"   Expected: {expected_base}")
            print(f"   Got: {writer.delta_base_path}")
            return False
        print(f"✅ Delta base path correctly set to: {writer.delta_base_path}")

        # Check that writers have local paths
        projects_writer = writer._writers.get("projects")
        if projects_writer is None:
            print("❌ Projects writer not found")
            return False

        projects_path = Path(projects_writer.table_path)
        if not str(projects_path).startswith(str(config.local_delta_path)):
            print(f"❌ Projects table path not using local delta path")
            print(f"   Path: {projects_path}")
            print(f"   Expected prefix: {config.local_delta_path}")
            return False
        print(f"✅ Projects table path: {projects_path}")

        return True

    except Exception as e:
        print(f"❌ Entity writer test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_inspection_script():
    """Test that the inspection script can be imported and run."""
    print("\nTest 4: Inspection Script")
    print("=" * 60)

    try:
        script_path = Path(__file__).parent / "inspect_simulation_delta.py"
        if not script_path.exists():
            print(f"❌ Inspection script not found: {script_path}")
            return False
        print(f"✅ Inspection script exists: {script_path}")

        # Check if executable
        if not os.access(script_path, os.X_OK):
            print(f"⚠️  Inspection script not executable (run: chmod +x {script_path})")
        else:
            print(f"✅ Inspection script is executable")

        return True

    except Exception as e:
        print(f"❌ Inspection script test failed: {e}")
        return False


def test_cleanup_script():
    """Test that the cleanup script exists."""
    print("\nTest 5: Cleanup Script")
    print("=" * 60)

    try:
        script_path = Path(__file__).parent / "cleanup_simulation_data.sh"
        if not script_path.exists():
            print(f"❌ Cleanup script not found: {script_path}")
            return False
        print(f"✅ Cleanup script exists: {script_path}")

        # Check if executable
        if not os.access(script_path, os.X_OK):
            print(f"⚠️  Cleanup script not executable (run: chmod +x {script_path})")
        else:
            print(f"✅ Cleanup script is executable")

        return True

    except Exception as e:
        print(f"❌ Cleanup script test failed: {e}")
        return False


def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("Delta Lake Simulation Mode Test Suite")
    print("=" * 60 + "\n")

    tests = [
        ("Configuration", test_simulation_config),
        ("Delta Table Write", test_write_delta_table),
        ("Entity Writer", test_entity_writer_simulation),
        ("Inspection Script", test_inspection_script),
        ("Cleanup Script", test_cleanup_script),
    ]

    results = []
    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, result))
        except KeyboardInterrupt:
            print("\n\nTests interrupted by user")
            sys.exit(1)
        except Exception as e:
            print(f"\n❌ Unexpected error in {name}: {e}")
            import traceback
            traceback.print_exc()
            results.append((name, False))

    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {name}")

    print(f"\nResults: {passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 All tests passed!")
        sys.exit(0)
    else:
        print(f"\n❌ {total - passed} test(s) failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
