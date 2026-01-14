# Issue #37: Batch cleared before write verification - Fix Summary

## Problem
Race condition in Delta writer workers where batches are cleared BEFORE verifying the Delta write succeeded, causing data loss on write failures.

### Unsafe Pattern (CURRENT):
```python
batch_to_write = list(self._batch)
self._batch.clear()  # ❌ Cleared before verifying write!
await delta_table.write_batch(batch)
await consumer.commit()
```

### Safe Pattern (TARGET):
```python
batch_to_write = list(self._batch)
# Don't clear yet - wait for verification
await delta_table.write_batch(batch)
await consumer.commit()
# ONLY clear after successful verification
self._batch.clear()
```

## Files Requiring Fixes

### 1. `kafka_pipeline/claimx/workers/delta_events_worker.py`
- **Status**: PARTIALLY FIXED (docstring updated, unsafe clear() removed)
- **Remaining Work**: Add proper batch clearing logic after successful commit

### 2. `kafka_pipeline/claimx/workers/entity_delta_worker.py`
- **Status**: NOT YET FIXED
- **Line 228**: Has race condition - clears batch before commit verification

### 3. `kafka_pipeline/xact/workers/delta_events_worker.py`
- **Status**: ALREADY CORRECT
- **Lines 334-450**: Proper implementation already in place

## Correct Implementation Pattern

```python
async def _flush_batch(self) -> None:
    """
    CRITICAL: Batch is only cleared AFTER successful write verification
    to prevent data loss from race conditions (Issue #37).
    """
    if not self._batch:
        return

    batch_id = uuid.uuid4().hex[:8]
    batch_to_write = list(self._batch)
    # Don't clear batch yet - wait for verified commit

    try:
        # 1. Attempt Delta write
        success = await self.delta_writer.write_batch(batch_to_write)
        
        if not success:
            # Classify error for routing
            error_category = self.retry_handler.classify_delta_error(Exception("Write failed"))
            
            # Route to DLQ or retry
            await self.retry_handler.handle_batch_failure(
                batch=batch_to_write,
                error=Exception("Delta write returned failure"),
                retry_count=0,
                error_category=error_category,
                batch_id=batch_id,
            )
            
            # Clear batch ONLY for permanent errors after DLQ routing
            if error_category == ErrorCategory.PERMANENT:
                self._batch.clear()
            return
        
        # 2. Write succeeded - now commit offsets
        await self.consumer.commit()
        
        # 3. ONLY clear batch after successful commit
        self._batch.clear()
        self._batches_written += 1
        self._records_succeeded += len(batch_to_write)
        
    except Exception as e:
        # Classify error
        error_category = self.retry_handler.classify_delta_error(e)
        
        # Route to DLQ or retry
        await self.retry_handler.handle_batch_failure(
            batch=batch_to_write,
            error=e,
            retry_count=0,
            error_category=error_category,
            batch_id=batch_id,
        )
        
        # Clear batch ONLY for permanent errors after DLQ routing
        if error_category == ErrorCategory.PERMANENT:
            self._batch.clear()
```

## Testing Approach

1. **Unit Tests**: Verify batch is preserved on write failures
2. **Integration Tests**: Verify DLQ routing works correctly
3. **Manual Testing**: Simulate Delta write failures and verify no data loss

