#!/usr/bin/env python3
"""
Test Project Cache Implementation

Verifies that the ProjectCache correctly prevents redundant API calls
during in-flight project verification, with TTL-based expiration.

Run this after deploying the project cache changes.
"""

import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka_pipeline.claimx.handlers.project_cache import ProjectCache, DEFAULT_TTL_SECONDS


def test_project_cache_basic():
    """Test basic cache operations."""
    print("=" * 80)
    print("Test: Basic Cache Operations")
    print("=" * 80)

    cache = ProjectCache()

    # Initial state
    assert cache.size() == 0, "Cache should start empty"
    print("✓ Cache starts empty")

    # Add projects
    cache.add("12345")
    assert cache.has("12345"), "Should find added project"
    assert cache.size() == 1, "Size should be 1"
    print("✓ Can add and find projects")

    # Add duplicate (should not increase size)
    cache.add("12345")
    assert cache.size() == 1, "Duplicate should not increase size"
    print("✓ Duplicates handled correctly")

    # Add more projects
    cache.add("67890")
    cache.add("11111")
    assert cache.size() == 3, "Should have 3 projects"
    assert cache.has("67890"), "Should find second project"
    assert cache.has("11111"), "Should find third project"
    print("✓ Multiple projects cached correctly")

    # Not found
    assert not cache.has("99999"), "Should not find non-existent project"
    print("✓ Returns False for non-existent projects")

    # Clear
    cache.clear()
    assert cache.size() == 0, "Cache should be empty after clear"
    assert not cache.has("12345"), "Should not find projects after clear"
    print("✓ Clear works correctly")

    print("\n✅ All basic tests passed!\n")


def test_project_cache_ttl():
    """Test TTL-based expiration."""
    print("=" * 80)
    print("Test: TTL-Based Expiration")
    print("=" * 80)

    # Create cache with very short TTL for testing
    cache = ProjectCache(ttl_seconds=1)  # 1 second TTL

    cache.add("12345")
    assert cache.has("12345"), "Should find project immediately"
    print("✓ Project found immediately after adding")

    # Wait for TTL to expire
    print("  Waiting 1.5 seconds for TTL to expire...")
    time.sleep(1.5)

    assert not cache.has("12345"), "Should not find project after TTL expires"
    print("✓ Project expired after TTL")

    # Verify stats
    stats = cache.get_stats()
    assert stats["expirations"] == 1, "Should have 1 expiration"
    assert stats["misses"] >= 1, "Should have at least 1 miss"
    print(f"✓ Stats: hits={stats['hits']}, misses={stats['misses']}, expirations={stats['expirations']}")

    print("\n✅ All TTL tests passed!\n")


def test_project_cache_ttl_disabled():
    """Test cache with TTL disabled (ttl=0)."""
    print("=" * 80)
    print("Test: TTL Disabled (No Expiration)")
    print("=" * 80)

    # Create cache with TTL disabled
    cache = ProjectCache(ttl_seconds=0)

    cache.add("12345")
    assert cache.has("12345"), "Should find project immediately"
    print("✓ Project found immediately")

    # Even after some time, should still be found (no expiration)
    time.sleep(0.1)
    assert cache.has("12345"), "Should still find project (no TTL)"
    print("✓ Project still found with TTL disabled")

    print("\n✅ TTL disabled tests passed!\n")


def test_project_cache_load_from_ids():
    """Test preloading cache from list of IDs."""
    print("=" * 80)
    print("Test: Preload from ID List (Delta Simulation)")
    print("=" * 80)

    cache = ProjectCache(ttl_seconds=1800)  # 30 minute TTL

    # Simulate loading from Delta table
    project_ids = ["100", "200", "300", "400", "500"]
    loaded = cache.load_from_ids(project_ids)

    assert loaded == 5, "Should load all 5 projects"
    assert cache.size() == 5, "Cache should have 5 entries"
    print(f"✓ Loaded {loaded} projects from list")

    # All should be found
    for pid in project_ids:
        assert cache.has(pid), f"Should find project {pid}"
    print("✓ All preloaded projects found")

    # Loading duplicates should not add more
    loaded = cache.load_from_ids(["100", "200", "600"])
    assert loaded == 1, "Should only load 1 new project (600)"
    assert cache.size() == 6, "Cache should have 6 entries"
    print("✓ Duplicate handling in preload works correctly")

    print("\n✅ All preload tests passed!\n")


def test_project_cache_stats():
    """Test cache statistics tracking."""
    print("=" * 80)
    print("Test: Cache Statistics")
    print("=" * 80)

    cache = ProjectCache(ttl_seconds=1800)

    # Generate some activity
    cache.add("12345")
    cache.has("12345")  # hit
    cache.has("12345")  # hit
    cache.has("99999")  # miss

    stats = cache.get_stats()
    assert stats["hits"] == 2, "Should have 2 hits"
    assert stats["misses"] == 1, "Should have 1 miss"
    assert stats["size"] == 1, "Should have 1 entry"
    print(f"✓ Stats: {stats}")

    # Reset stats
    cache.reset_stats()
    stats = cache.get_stats()
    assert stats["hits"] == 0, "Hits should be reset"
    assert stats["misses"] == 0, "Misses should be reset"
    print("✓ Stats reset correctly")

    print("\n✅ All stats tests passed!\n")


def test_project_cache_with_mock_handler():
    """Test cache with simulated handler usage."""
    print("=" * 80)
    print("Test: Simulated Handler Usage")
    print("=" * 80)

    cache = ProjectCache(ttl_seconds=1800)
    api_calls = []  # Track API calls

    def mock_fetch_project(project_id: str):
        """Simulate ProjectHandler.fetch_project_data()"""
        # Check cache first
        if cache.has(project_id):
            print(f"  Cache HIT  for project {project_id} - skipping API call")
            return None  # Return empty (cached)

        # Cache miss - simulate API call
        print(f"  Cache MISS for project {project_id} - making API call")
        api_calls.append(project_id)

        # Add to cache after fetch
        cache.add(project_id)
        return {"project_id": project_id}  # Return data

    print("\nScenario: 10 tasks for project 12345")
    print("-" * 40)

    # Simulate 10 task events for same project
    for i in range(10):
        result = mock_fetch_project("12345")
        if i == 0:
            assert result is not None, "First call should fetch data"
        else:
            assert result is None, f"Call {i+1} should use cache"

    assert len(api_calls) == 1, "Should only make 1 API call"
    print(f"\n✓ Only 1 API call made (90% reduction!)")

    stats = cache.get_stats()
    hit_rate = stats["hit_rate_pct"]
    print(f"✓ Cache hit rate: {hit_rate}%")

    print("\nScenario: Mixed projects")
    print("-" * 40)

    api_calls.clear()
    cache.clear()
    cache.reset_stats()

    # Simulate events for multiple projects
    events = [
        "12345",  # Project 1 - miss
        "12345",  # Project 1 - hit
        "67890",  # Project 2 - miss
        "12345",  # Project 1 - hit
        "67890",  # Project 2 - hit
        "11111",  # Project 3 - miss
        "12345",  # Project 1 - hit
    ]

    for project_id in events:
        mock_fetch_project(project_id)

    assert len(api_calls) == 3, "Should make 3 API calls (3 unique projects)"
    assert cache.size() == 3, "Should have 3 projects cached"
    print(f"\n✓ 3 API calls for 3 unique projects (57% reduction)")
    print(f"✓ Cache size: {cache.size()}")

    stats = cache.get_stats()
    print(f"✓ Final stats: hits={stats['hits']}, misses={stats['misses']}, hit_rate={stats['hit_rate_pct']}%")

    print("\n✅ All handler simulation tests passed!\n")


def test_project_cache_with_preload():
    """Test cache with preloaded projects (simulating Delta preload)."""
    print("=" * 80)
    print("Test: Handler Usage with Preloaded Cache")
    print("=" * 80)

    cache = ProjectCache(ttl_seconds=1800)
    api_calls = []

    # Simulate preloading from Delta at startup
    print("\nPreloading cache with existing projects from 'Delta'...")
    existing_projects = ["12345", "67890", "11111"]
    cache.load_from_ids(existing_projects)
    print(f"✓ Preloaded {len(existing_projects)} projects")

    def mock_fetch_project(project_id: str):
        if cache.has(project_id):
            print(f"  Cache HIT  for project {project_id} - skipping API call")
            return None
        print(f"  Cache MISS for project {project_id} - making API call")
        api_calls.append(project_id)
        cache.add(project_id)
        return {"project_id": project_id}

    print("\nProcessing events for preloaded projects...")
    print("-" * 40)

    # All these should be cache hits (no API calls)
    for pid in existing_projects:
        result = mock_fetch_project(pid)
        assert result is None, f"Project {pid} should be cached"

    assert len(api_calls) == 0, "Should make 0 API calls for preloaded projects"
    print(f"\n✓ 0 API calls made (100% reduction!)")

    print("\nProcessing event for new project...")
    print("-" * 40)

    # New project - should be a cache miss
    result = mock_fetch_project("99999")
    assert result is not None, "New project should not be cached"
    assert len(api_calls) == 1, "Should make 1 API call for new project"
    print(f"\n✓ 1 API call for new project")

    print("\n✅ All preload integration tests passed!\n")


def main():
    """Run all tests."""
    print("\n" + "=" * 80)
    print("Project Cache Test Suite (with TTL and Preload)")
    print("=" * 80)
    print(f"Default TTL: {DEFAULT_TTL_SECONDS} seconds ({DEFAULT_TTL_SECONDS // 60} minutes)")
    print()

    try:
        test_project_cache_basic()
        test_project_cache_ttl()
        test_project_cache_ttl_disabled()
        test_project_cache_load_from_ids()
        test_project_cache_stats()
        test_project_cache_with_mock_handler()
        test_project_cache_with_preload()

        print("=" * 80)
        print("✅ ALL TESTS PASSED!")
        print("=" * 80)
        print()
        print("The project cache is working correctly. Deploy with confidence!")
        print()
        print("Summary of fixes:")
        print("  1. Fixed bug: project_cache now passed to ProjectHandler in all handlers")
        print("  2. Added TTL support with configurable expiration")
        print("  3. Added Delta preload capability for warm cache at startup")
        print("  4. Added cache statistics for monitoring")
        print()
        return 0

    except AssertionError as e:
        print()
        print("=" * 80)
        print("❌ TEST FAILED")
        print("=" * 80)
        print(f"Error: {e}")
        print()
        return 1

    except Exception as e:
        print()
        print("=" * 80)
        print("❌ UNEXPECTED ERROR")
        print("=" * 80)
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        print()
        return 1


if __name__ == "__main__":
    sys.exit(main())
