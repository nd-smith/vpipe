# Batch Consumer Design Specification

## Overview

Design for batch-concurrent consumer support in the transport layer to enable high-throughput I/O operations (downloads/uploads) while maintaining transport abstraction between EventHub and Kafka.

## Goals

1. **Restore 10x throughput** for download/upload workers (50 MB/s vs 5 MB/s sequential)
2. **Maintain transport abstraction** - same handler works with both EventHub and Kafka
3. **Simple semantics** - clear commit/checkpoint behavior
4. **Preserve existing patterns** - minimal changes to download worker code

## Design Decisions

### 1. API Signature

```python
async def create_batch_consumer(
    config: KafkaConfig,
    domain: str,
    worker_name: str,
    topics: list[str],
    batch_handler: Callable[[list[PipelineMessage]], Awaitable[bool]],
    batch_size: int = 20,
    batch_timeout_ms: int = 1000,
    enable_message_commit: bool = True,
    instance_id: str | None = None,
    transport_type: TransportType | None = None,
    topic_key: str | None = None,
) -> BatchConsumer:
    """Create a batch consumer for concurrent message processing.

    Args:
        config: KafkaConfig with connection details
        domain: Pipeline domain (e.g., "verisk", "claimx")
        worker_name: Worker name for logging and metrics
        topics: List of topics to consume (EventHub requires single topic)
        batch_handler: Async function that processes message batches
        batch_size: Target batch size (default: 20)
        batch_timeout_ms: Max wait time to accumulate batch (default: 1000ms)
        enable_message_commit: Whether to commit after successful batch processing
        instance_id: Optional instance identifier for parallel consumers
        transport_type: Optional transport override (defaults to PIPELINE_TRANSPORT env)
        topic_key: Optional topic key for EventHub resolution from config.yaml

    Returns:
        EventHubBatchConsumer or KafkaBatchConsumer based on transport

    Batch Handler Contract:
        - Receives list of PipelineMessages
        - Processes messages concurrently (handler controls concurrency)
        - Returns True to commit/checkpoint batch, False to skip commit
        - If handler raises exception, batch is NOT committed (redelivered)
    """
```

### 2. Batch Handler Interface

**Signature:**
```python
batch_handler: Callable[[list[PipelineMessage]], Awaitable[bool]]
```

**Contract:**
- **Input**: List of `PipelineMessage` objects (1 to `batch_size` messages)
- **Output**: `bool` - `True` = commit batch, `False` = skip commit (reprocess)
- **Exceptions**: If handler raises, batch is NOT committed (all messages redelivered)

**Handler Responsibilities:**
1. Process messages (sequentially or concurrently - handler decides)
2. Handle partial failures appropriately
3. Return commit decision based on transient vs permanent errors
4. Be idempotent (messages may be redelivered)

**Example Handler Pattern (from download worker):**
```python
async def _process_batch(self, messages: list[PipelineMessage]) -> bool:
    # Process concurrently with semaphore
    async def bounded_process(msg):
        async with self._semaphore:
            return await self._process_download(msg)

    results = await asyncio.gather(
        *[bounded_process(msg) for msg in messages],
        return_exceptions=True
    )

    # Check for circuit breaker errors (transient - need immediate retry)
    circuit_errors = [
        r for r in results
        if isinstance(r, Exception) and isinstance(r, CircuitOpenError)
    ]

    if circuit_errors:
        # Don't commit - reprocess entire batch when circuit recovers
        logger.warning(f"Circuit breaker errors: {len(circuit_errors)} - not committing")
        return False

    # Permanent errors already sent to DLQ/retry topics during processing
    # Commit the batch to move forward
    return True
```

### 3. Commit/Checkpoint Strategy

**Chosen Strategy: Conditional All-or-Nothing**

- Handler returns `True` → Transport layer commits/checkpoints ALL messages
- Handler returns `False` → Transport layer skips commit (all messages redelivered)
- Handler raises exception → Transport layer skips commit (all messages redelivered)

**Rationale:**
- Matches existing download worker pattern (commit decision based on CircuitOpenError)
- Simple semantics: batch is atomic unit
- Handles transient errors (circuit breaker) gracefully
- Permanent errors handled within batch processing (DLQ/retry topics)

**Alternative Considered (Rejected): Always Commit**
- Always commit regardless of handler result
- Handler must handle ALL errors internally (DLQ routing)
- Problem: No way to handle transient infrastructure failures (EventHub down, circuit open)
- Would require complex retry logic in every handler

### 4. Partial Failure Handling

**Within Batch Processing:**

Handlers are expected to classify errors during processing:

1. **Permanent Errors** (bad data, validation failures, etc.)
   - Send to DLQ or retry topic DURING batch processing
   - Don't prevent batch commit
   - Example: Invalid URL, malformed message, business logic failure

2. **Transient Errors** (circuit breaker, rate limit, temporary outage)
   - Return `False` from handler to skip commit
   - Entire batch redelivered for retry
   - Example: `CircuitOpenError`, network timeout, downstream service unavailable

**Pattern in Handler:**
```python
async def process_single_message(msg):
    try:
        await download_file(msg)
        return Success(msg)
    except ValidationError as e:
        # Permanent error - route to DLQ
        await send_to_dlq(msg, e)
        return PermanentFailure(msg, e)
    except CircuitOpenError as e:
        # Transient error - signal batch should not commit
        return TransientFailure(msg, e)

# After processing all messages
if any(isinstance(r, TransientFailure) for r in results):
    return False  # Don't commit - reprocess batch
else:
    return True  # Commit - permanent errors already handled
```

### 5. EventHub Buffering Strategy

**Challenge:** EventHub SDK is streaming-based (one message at a time), not batch-native like Kafka.

**Solution:** Client-side buffering with per-partition accumulation.

```python
class EventHubBatchConsumer:
    def __init__(self, batch_size=20, batch_timeout_ms=1000, ...):
        self._batch_buffers: dict[str, list[tuple]] = {}  # partition_id -> [(context, event)]
        self._batch_timers: dict[str, float] = {}  # partition_id -> batch_start_time
        self._flush_locks: dict[str, asyncio.Lock] = {}  # Prevent concurrent flushes

    async def on_event(self, partition_context, event):
        """Called by EventHub SDK for each message."""
        partition_id = partition_context.partition_id

        # Initialize partition buffer if needed
        if partition_id not in self._batch_buffers:
            self._batch_buffers[partition_id] = []
            self._batch_timers[partition_id] = time.time()
            self._flush_locks[partition_id] = asyncio.Lock()

        # Add to buffer
        self._batch_buffers[partition_id].append((partition_context, event))

        # Check flush conditions
        batch = self._batch_buffers[partition_id]
        elapsed_ms = (time.time() - self._batch_timers[partition_id]) * 1000

        if len(batch) >= self.batch_size or elapsed_ms >= self.batch_timeout_ms:
            # Flush batch in background task to avoid blocking on_event
            asyncio.create_task(self._flush_partition_batch(partition_id))

    async def _flush_partition_batch(self, partition_id: str):
        """Flush accumulated batch for a partition."""
        async with self._flush_locks[partition_id]:
            # Get batch and reset buffer atomically
            batch = self._batch_buffers.get(partition_id, [])
            if not batch:
                return

            self._batch_buffers[partition_id] = []
            self._batch_timers[partition_id] = time.time()

        # Convert to PipelineMessages
        messages = [
            EventHubConsumerRecord(event, self.eventhub_name, partition_id).to_pipeline_message()
            for _, event in batch
        ]

        try:
            # Call batch handler
            should_commit = await self.batch_handler(messages)

            if should_commit and self._enable_message_commit:
                # Checkpoint all messages in batch
                for context, event in batch:
                    await context.update_checkpoint(event)

                logger.debug(
                    f"Checkpointed batch: partition={partition_id}, size={len(batch)}"
                )
            else:
                logger.debug(
                    f"Skipped checkpoint (handler returned False): "
                    f"partition={partition_id}, size={len(batch)}"
                )

        except Exception as e:
            # Handler raised exception - don't checkpoint
            logger.error(
                f"Batch processing failed: partition={partition_id}, "
                f"size={len(batch)}, error={e}",
                exc_info=True
            )
            # Messages will be redelivered by EventHub
```

**Timeout-Based Flushing:**

Need background task to flush partial batches when timeout expires:

```python
async def _timeout_flush_loop(self):
    """Background task to flush batches that hit timeout."""
    while self._running:
        await asyncio.sleep(0.1)  # Check every 100ms

        for partition_id in list(self._batch_buffers.keys()):
            if partition_id not in self._batch_timers:
                continue

            elapsed_ms = (time.time() - self._batch_timers[partition_id]) * 1000
            batch_size = len(self._batch_buffers.get(partition_id, []))

            if batch_size > 0 and elapsed_ms >= self.batch_timeout_ms:
                logger.debug(
                    f"Timeout flush: partition={partition_id}, "
                    f"size={batch_size}, elapsed_ms={elapsed_ms:.1f}"
                )
                asyncio.create_task(self._flush_partition_batch(partition_id))
```

### 6. Kafka Batch Consumer

**Advantage:** Kafka's `getmany()` is already batch-native.

```python
class KafkaBatchConsumer:
    async def _consume_loop(self):
        while self._running:
            # Fetch batch (already native in Kafka)
            data = await self._consumer.getmany(
                timeout_ms=self.batch_timeout_ms,
                max_records=self.batch_size
            )

            if not data:
                continue

            # Flatten to single list
            messages: list[PipelineMessage] = []
            for partition_messages in data.values():
                messages.extend([
                    from_consumer_record(record)
                    for record in partition_messages
                ])

            if not messages:
                continue

            try:
                # Call batch handler
                should_commit = await self.batch_handler(messages)

                if should_commit and self._enable_message_commit:
                    await self._consumer.commit()
                    logger.debug(f"Committed batch: size={len(messages)}")
                else:
                    logger.debug(
                        f"Skipped commit (handler returned False): "
                        f"size={len(messages)}"
                    )

            except Exception as e:
                # Handler raised exception - don't commit
                logger.error(
                    f"Batch processing failed: size={len(messages)}, error={e}",
                    exc_info=True
                )
                # Messages will be redelivered on next poll
```

### 7. Partition Rebalancing Behavior

**EventHub:**
- When partition is revoked, flush incomplete batch before yielding partition
- New consumer will start from last checkpoint (unflushed messages redelivered)

```python
async def on_partition_close(partition_context):
    """Called when partition is being revoked."""
    partition_id = partition_context.partition_id
    logger.info(f"Partition closing: {partition_id} - flushing incomplete batch")
    await self._flush_partition_batch(partition_id)
```

**Kafka:**
- Kafka handles this automatically - uncommitted messages are redelivered to new consumer
- No special handling needed (existing BaseKafkaConsumer behavior)

### 8. Graceful Shutdown

**Both Transports:**
- Flush all incomplete batches before stopping
- Wait for in-flight batch processing to complete
- Then commit/checkpoint final batches

```python
async def stop(self):
    """Stop consumer and flush remaining batches."""
    logger.info("Stopping batch consumer - flushing remaining batches")
    self._running = False

    # Flush all partition buffers
    for partition_id in list(self._batch_buffers.keys()):
        await self._flush_partition_batch(partition_id)

    # Close underlying consumer
    await self._consumer.close()
```

### 9. Batch Sizing Semantics

**Batch Size is a Maximum, Not Guarantee:**

Batches may be smaller than `batch_size` due to:
1. Timeout expiration (no more messages available within `batch_timeout_ms`)
2. Low message rate (fewer than `batch_size` messages available)
3. Partition assignment (only one partition assigned)
4. Shutdown/rebalancing (incomplete batch flushed)

**Handlers Must Handle Variable Batch Sizes:**

```python
async def batch_handler(messages: list[PipelineMessage]) -> bool:
    # messages can be anywhere from 1 to batch_size
    if len(messages) == 0:
        return True  # Empty batch - nothing to commit

    # Process all messages in batch
    # ...
```

### 10. Error Handling Summary

| Error Type | Where Handled | Result |
|------------|---------------|--------|
| Permanent error (bad data) | Within handler (send to DLQ) | Handler returns `True`, batch committed |
| Transient error (circuit open) | Within handler (detect & return) | Handler returns `False`, batch NOT committed |
| Handler exception | Transport layer catches | Batch NOT committed, messages redelivered |
| EventHub connection loss | EventHub SDK | Messages redelivered from last checkpoint |
| Kafka connection loss | Kafka consumer | Messages redelivered from last commit |

### 11. Concurrency Control

**Where:** Within batch handler (NOT in transport layer)

**Rationale:**
- Different workers need different concurrency limits
- Download workers: ~10 concurrent downloads (I/O bound)
- Other workers: May not need concurrency at all
- Transport layer should be simple: deliver batches, commit results

**Pattern:**
```python
class DownloadWorker:
    def __init__(self, concurrency=10):
        self._semaphore = asyncio.Semaphore(concurrency)

    async def _process_batch(self, messages: list[PipelineMessage]) -> bool:
        async def bounded_process(msg):
            async with self._semaphore:  # Limit to 10 concurrent
                return await self._process_download(msg)

        results = await asyncio.gather(*[bounded_process(m) for m in messages])
        # ... handle results
```

## Migration Path

### Before (Direct AIOKafkaConsumer):
```python
# Worker creates consumer directly
self._consumer = AIOKafkaConsumer(*self.topics, **config)
await self._consumer.start()

# Manual batch loop
while self._running:
    data = await self._consumer.getmany(timeout_ms=1000, max_records=20)
    messages = [from_consumer_record(r) for records in data.values() for r in records]
    should_commit = await self._process_batch(messages)
    if should_commit:
        await self._consumer.commit()
```

### After (Transport Layer):
```python
# Worker uses transport layer factory
self.consumer = await create_batch_consumer(
    config=self.config,
    domain=self.domain,
    worker_name="download-worker",
    topics=[self.topic],
    batch_handler=self._process_batch,  # SAME METHOD!
    batch_size=20,
    batch_timeout_ms=1000,
    topic_key="downloads_pending"
)
await self.consumer.start()
# No manual loop - transport layer handles it
```

**Key Changes:**
1. ✅ Replace `AIOKafkaConsumer` with `create_batch_consumer()`
2. ✅ Remove manual `getmany()` loop
3. ✅ Keep existing `_process_batch()` method (just change return type to `bool`)
4. ✅ Keep existing concurrency control (semaphore)
5. ✅ Keep existing error handling logic

**Minimal Code Changes** - mostly just wiring, not logic rewrite.

## Testing Strategy

### Unit Tests
1. EventHub buffer accumulation (size and timeout triggers)
2. Kafka batch fetching and flattening
3. Conditional commit logic (True/False/Exception cases)
4. Partition rebalancing flush behavior
5. Graceful shutdown batch flushing

### Integration Tests
1. End-to-end EventHub: produce → batch consume → checkpoint
2. End-to-end Kafka: produce → batch consume → commit
3. Circuit breaker scenario (handler returns False, messages redelivered)
4. Handler exception scenario (messages redelivered)
5. Concurrent processing within batch (semaphore respected)
6. Partition rebalancing during processing
7. Graceful shutdown with incomplete batches

### Performance Tests
1. Download worker throughput: Sequential (5 MB/s) vs Batch (50 MB/s)
2. EventHub checkpoint latency with different batch sizes
3. Kafka commit latency with different batch sizes
4. CPU/memory usage with different concurrency settings

## Success Criteria

1. ✅ `create_batch_consumer()` factory function implemented
2. ✅ EventHub batch consumer with buffering works correctly
3. ✅ Kafka batch consumer works correctly
4. ✅ Both transports pass integration tests
5. ✅ Download workers migrated and maintain ~50 MB/s throughput
6. ✅ Upload workers migrated (if using batch pattern)
7. ✅ No data loss (all messages processed at-least-once)
8. ✅ Circuit breaker errors handled correctly (batch redelivery)
9. ✅ Documentation updated with usage examples

## Open Questions

1. **Q: Should we implement cross-partition batching for EventHub?**
   - EventHub delivers messages per-partition via `on_event`
   - Could buffer across all partitions to get larger batches
   - **Decision:** Start with per-partition batching (simpler). If batch sizes are too small, can add cross-partition later.

2. **Q: What if EventHub checkpoint fails?**
   - Partial checkpoint (some messages checkpointed, some fail)
   - **Decision:** Log error and continue. Messages may be redelivered (at-least-once semantics). Handlers must be idempotent.

3. **Q: Should batch_timeout_ms be per-partition or global?**
   - **Decision:** Per-partition. Each partition accumulates independently.

4. **Q: Maximum batch processing time limit?**
   - Kafka has `max_poll_interval_ms` (default 300s)
   - EventHub has no hard limit but long processing blocks partition
   - **Decision:** Document that handlers should complete within reasonable time (< 60s recommended). Add warning log if batch takes > 60s.

## Next Steps

1. ✅ Complete design review and approval → **YOU ARE HERE**
2. Implement EventHubBatchConsumer (`src/pipeline/common/eventhub/batch_consumer.py`)
3. Implement KafkaBatchConsumer (`src/pipeline/common/batch_consumer.py`)
4. Add `create_batch_consumer()` to transport.py
5. Write integration tests
6. Migrate download workers (ClaimX + Verisk)
7. Migrate upload workers (if needed)
8. Update documentation
9. Deploy and validate throughput restoration
