# Project Cache Implementation

## Overview

Implemented an in-memory cache to prevent redundant API calls during in-flight project verification. This resolves the issue where project data was being fetched multiple times for the same project when processing multiple child events (tasks, media, contacts, video collaborations).

## Problem Statement

When processing CUSTOM_TASK_COMPLETED events (and other child entity events), handlers were calling `ProjectHandler.fetch_project_data()` to ensure the parent project exists before writing child entities. This resulted in:

1. **Multiple API calls** for the same project_id (once per task/event)
2. **Multiple project row writes** to the entity rows topic
3. **Unnecessary load** on ClaimX API and entity writer

Example scenario:
- Project 12345 has 10 tasks that complete in quick succession
- **Before:** 10 API calls + 10 project row writes
- **After:** 1 API call + 1 project row write (9 cache hits)

## Solution Architecture

### Components Created

1. **`ProjectCache`** (`kafka_pipeline/claimx/handlers/project_cache.py`)
   - In-memory cache of project IDs that have been processed
   - Thread-safe for async tasks within a single worker
   - Persists for worker lifetime

2. **Updated `EventHandler`** base class
   - Added optional `project_cache` parameter to `__init__`
   - All handlers automatically get cache reference

3. **Updated `ProjectHandler.fetch_project_data()`**
   - Checks cache before making API call
   - Returns empty EntityRowsMessage if project cached
   - Adds project to cache after successful fetch

4. **Updated `ClaimXEnrichmentWorker`**
   - Creates ProjectCache instance on initialization
   - Passes cache to all handlers
   - Logs cache size in cycle stats

### Flow Diagram

```
Event → Handler → fetch_project_data()
                       ↓
              [Check ProjectCache]
                  ↓           ↓
             CACHE HIT    CACHE MISS
                  ↓           ↓
        Return empty     Call API
                         ↓
                  Transform rows
                         ↓
                  Add to cache
                         ↓
                  Return rows
```

## Affected Handlers

All handlers that do in-flight project verification benefit automatically:

1. **TaskHandler** (CUSTOM_TASK_ASSIGNED, CUSTOM_TASK_COMPLETED)
2. **MediaHandler** (PROJECT_FILE_ADDED)
3. **ContactHandler** (POLICYHOLDER_INVITED, POLICYHOLDER_JOINED)
4. **VideoHandler** (VIDEO_COLLABORATION_*)

## Cache Behavior

### When Cache is Used
- **Check:** Before every `fetch_project_data()` call
- **Add:** After successful API fetch and transformation
- **Hit:** Skip API call, return empty EntityRowsMessage
- **Miss:** Fetch from API, transform, cache, return rows

### Cache Lifetime
- **Scope:** Per-worker instance
- **Duration:** Entire worker session
- **Size:** Unbounded (grows with unique projects)
- **Shared:** No (each worker has its own cache)

### Edge Cases Handled

1. **Worker restart:** Cache cleared, projects re-fetched
   - ✓ Delta merge handles duplicates gracefully

2. **Multiple workers:** Each has own cache, may fetch same project
   - ✓ Delta merge dedupes by project_id

3. **API errors:** Project not added to cache on error
   - ✓ Retry will attempt fetch again

## Code Changes Summary

### Files Modified

1. `kafka_pipeline/claimx/handlers/base.py`
   - Added `project_cache` parameter to EventHandler.__init__

2. `kafka_pipeline/claimx/handlers/project.py`
   - Updated fetch_project_data() to check/use cache

3. `kafka_pipeline/claimx/workers/enrichment_worker.py`
   - Create ProjectCache instance
   - Pass to handlers on instantiation
   - Log cache size in cycle stats

4. `kafka_pipeline/claimx/handlers/__init__.py`
   - Export ProjectCache

### Files Created

1. `kafka_pipeline/claimx/handlers/project_cache.py`
   - ProjectCache class implementation

## Monitoring & Observability

### Logs Added

**Debug Level:**
```
Project in cache - skipping API call | project_id=12345 | cache_size=42
Added project to cache | project_id=12345 | cache_size=43
```

**Info Level (Cycle Stats):**
```
Cycle 5: processed=150 (succeeded=148, failed=2, skipped=0) | project_cache_size=89
```

### Metrics to Watch

1. **Cache Hit Rate** = (Total Events - API Calls) / Total Events
2. **Cache Size Growth** = Projects cached over time
3. **API Call Reduction** = Before vs After comparison

### Expected Behavior

For a project with multiple tasks:
```
Event 1 (Task 1 Complete):  Cache MISS → API call → Project rows written
Event 2 (Task 2 Complete):  Cache HIT  → No API    → No project rows
Event 3 (Task 3 Complete):  Cache HIT  → No API    → No project rows
...
```

## Testing

### Unit Test Approach

```python
from kafka_pipeline.claimx.handlers.project_cache import ProjectCache
from kafka_pipeline.claimx.handlers.project import ProjectHandler

async def test_project_cache():
    cache = ProjectCache()
    handler = ProjectHandler(api_client, project_cache=cache)

    # First call - should fetch from API
    rows1 = await handler.fetch_project_data(12345)
    assert len(rows1.projects) > 0  # Has data
    assert cache.has("12345")       # Added to cache

    # Second call - should use cache
    rows2 = await handler.fetch_project_data(12345)
    assert len(rows2.projects) == 0  # Empty (cached)
```

### Integration Test

1. Start enrichment worker
2. Publish CUSTOM_TASK_COMPLETED events for same project
3. Check logs for "Project in cache" messages
4. Verify only 1 API call made per project
5. Check cycle stats show growing cache_size

## Performance Impact

### Before (No Cache)
- 100 events for 10 unique projects
- **100 API calls**
- **100 project row writes**

### After (With Cache)
- 100 events for 10 unique projects
- **10 API calls** (90% reduction)
- **10 project row writes** (90% reduction)

### Resource Usage
- **Memory:** ~100 bytes per cached project ID
- **CPU:** Negligible (hash set lookup)
- **API Load:** Significantly reduced

## Migration & Rollout

### Backward Compatibility
✓ Fully backward compatible
- `project_cache` parameter is optional
- Handlers work without cache (just less efficient)
- No config changes required

### Deployment
1. Deploy updated code
2. Restart enrichment workers
3. Monitor logs for cache statistics
4. Verify API call reduction in metrics

### Rollback
If issues occur, simply revert and restart workers. No data corruption risk because:
- Cache only affects API calls, not data writes
- Delta merge handles any duplicate project rows
- No persistent state changes

## Future Enhancements

### Potential Improvements (Not Implemented)

1. **Cache Size Limit**
   - LRU eviction policy
   - Max cache size configuration

2. **Cache Metrics**
   - Hit rate tracking
   - Export to Prometheus

3. **Delta Table Lookup**
   - Query Delta before API call
   - Most accurate but adds latency

4. **Distributed Cache**
   - Redis/Memcached for cross-worker sharing
   - Overkill for current needs

## Conclusion

The in-memory project cache provides a simple, effective solution to prevent redundant API calls during in-flight project verification. It's:

- ✓ **Transparent:** Works automatically for all handlers
- ✓ **Safe:** No data loss or corruption risk
- ✓ **Efficient:** Minimal memory/CPU overhead
- ✓ **Observable:** Clear logs and metrics
- ✓ **Maintainable:** Simple, focused implementation

The solution addresses the immediate issue of repeated project downloads while keeping complexity low and maintainability high.
