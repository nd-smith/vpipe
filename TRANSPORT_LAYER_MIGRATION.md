# Transport Layer Migration Plan

## Executive Summary

**CRITICAL**: Kafka is being decommissioned. All workers MUST migrate to EventHub (via transport layer) to work in production.

**Strategy**: Ship fast with acceptable performance tradeoff, optimize later when needed.

## Next Steps (Immediate Actions)

### 1. Migrate Remaining Workers (Priority Order)

**Week 1: Single-Message Workers** (Easy, proven pattern)
1. Verisk `enrichment_worker.py` - Mirror ClaimX enricher (30 min)

**Week 2-3: Batch Workers** (Accept sequential processing)
2. ClaimX `download_worker.py` - Remove batch concurrency
3. ClaimX `upload_worker.py` - Remove batch concurrency
4. Verisk `download_worker.py` - Remove batch concurrency
5. Verisk `upload_worker.py` - Remove batch concurrency

**Migration Checklist** (per worker):
- [ ] Replace `AIOKafkaConsumer` with `create_consumer()`
- [ ] Remove `_create_consumer()` method
- [ ] Remove `_consume_loop()` method
- [ ] Change handler to process one message at a time
- [ ] Remove batch processing (`getmany()`, `asyncio.gather()`)
- [ ] Remove dead code (background tasks, `max_poll_records`, etc.)
- [ ] Add transport layer contract to docstring
- [ ] Test with `PIPELINE_TRANSPORT=eventhub`
- [ ] Verify: No `localhost:9092` errors

### 2. Measure Performance (After Migration)

**Collect metrics** for 1-2 weeks in production:
- Download throughput (MB/s, files/minute)
- Upload throughput (MB/s, files/minute)
- Consumer lag (messages behind)
- End-to-end latency (event → file uploaded)

**Decision point**: If downloads/uploads are bottleneck, build batch-concurrent transport layer (Phase 2).

### 3. Documentation

- [ ] Update worker docstrings to explain transport layer usage
- [ ] Document performance tradeoff clearly
- [ ] Create ticket for Phase 2 (batch-concurrent support) - mark as "Future/Blocked"

## Problem Summary

Several workers directly instantiate `AIOKafkaConsumer`, which will fail in production because:

1. **Kafka is being removed** - `AIOKafkaConsumer` won't be available
2. Production uses EventHub exclusively (Azure Private Link exposes AMQP port 443 only)
3. Workers bypassing the transport layer can't connect to EventHub

**Impact**: Workers using direct `AIOKafkaConsumer` will be completely non-functional in production.

## Why the Transport Layer Exists

The transport layer abstracts EventHub vs Kafka to support:

- **Production**: EventHub (AMQP over WebSocket, port 443) - ONLY option
- **Local Dev**: Kafka (Kafka protocol, port 9092) - being phased out

**Current limitation**: Transport layer only supports single-message sequential processing, not batch-concurrent.

## Current State

### Workers Using Transport Layer ✅

**ClaimX Pipeline:**
- ✅ `enrichment_worker.py` (just fixed)
- ✅ `delta_events_worker.py`
- ✅ `entity_delta_worker.py`
- ✅ `event_ingester.py`
- ✅ `result_processor.py`

**Verisk Pipeline:**
- ✅ `delta_events_worker.py`
- ✅ `event_ingester.py`
- ✅ `result_processor.py`

### Workers Still Using AIOKafkaConsumer Directly ❌

**ClaimX Pipeline:**
- ❌ `download_worker.py`
- ❌ `upload_worker.py`

**Verisk Pipeline:**
- ❌ `download_worker.py`
- ❌ `upload_worker.py`
- ❌ `enrichment_worker.py`

**Plugin Workers:**
- ❌ `plugins/claimx_mitigation_task/mitigation_tracking_worker.py`
- ❌ `plugins/itel_cabinet_api/itel_cabinet_api_worker.py`
- ❌ `plugins/itel_cabinet_api/itel_cabinet_tracking_worker.py`
- ❌ `plugins/shared/workers/plugin_action_worker.py`

## Immediate Actions Taken (This Session)

### ClaimX Enrichment Worker Cleanup

**Fixed:**
- ✅ Replaced `AIOKafkaConsumer` with `create_consumer()` from transport layer
- ✅ Removed dead code: `_pending_tasks`, `_task_counter`, `_create_tracked_task()`, `_wait_for_pending_tasks()`
- ✅ Removed unused `max_poll_records` config (not passed to transport layer)
- ✅ Added clear docstring explaining transport layer contract
- ✅ Changed `_dispatch_entity_rows()` to await synchronously (prevents data loss per Issue #38)

**Files modified:**
- `src/pipeline/claimx/workers/enrichment_worker.py`

## CRITICAL DISCOVERY: Transport Layer Limitation

### The Transport Layer Only Supports Single-Message Processing

After analysis, discovered that both EventHub and Kafka transport wrappers **process messages one-at-a-time**:

**EventHub Consumer** (`eventhub/consumer.py:346`):
```python
async def on_event(partition_context, event):
    # Processes ONE message
    message = convert_to_pipeline_message(event)
    await self._process_message(message)
    await partition_context.update_checkpoint(event)  # Commits after EACH
```

**Kafka Consumer Wrapper** (`consumer.py:315-368`):
```python
data = await self._consumer.getmany(timeout_ms=1000)  # Fetches batch
for message in messages:  # BUT processes one-by-one
    await self._process_message(message)
    await self._consumer.commit()  # Commits after EACH message
```

### Download/Upload Workers Need Batch-Concurrent Processing

**Download Workers** fetch batches of 20 messages and process **10 concurrently**:
```python
# Fetch batch
msg_dict = await self._consumer.getmany(timeout_ms=1000, max_records=20)

# Process concurrently (10 at once via semaphore)
tasks = [self._process_download(msg) for msg in messages]
results = await asyncio.gather(*tasks)  # 10x faster than sequential

# Commit ONCE after batch completes
await self._consumer.commit()
```

**Why This Matters**:
- Sequential file downloads: ~5 MB/s per file
- Concurrent downloads (10x): ~50 MB/s aggregate throughput
- **Migrating to transport layer would reduce performance by 10x**

## Migration Strategy: Ship Fast, Optimize Later

### Phase 1: Migrate ALL Workers Sequentially (CURRENT)

**Goal**: Get all workers functional in EventHub-only production environment.

**Approach**: Migrate all workers to use transport layer with single-message sequential processing.

**Workers to Migrate**:
- ✅ ClaimX `enrichment_worker.py` - **DONE**
- ⚠️ Verisk `enrichment_worker.py` - Single-message (easy)
- ⚠️ ClaimX `download_worker.py` - **Will become sequential** (10x slower)
- ⚠️ ClaimX `upload_worker.py` - **Will become sequential** (10x slower)
- ⚠️ Verisk `download_worker.py` - **Will become sequential** (10x slower)
- ⚠️ Verisk `upload_worker.py` - **Will become sequential** (10x slower)

**Performance Impact Accepted**:
- Download throughput: 50 MB/s → 5 MB/s (10x reduction)
- Upload throughput: Similar reduction
- **Tradeoff**: Workers online and functional > optimal performance initially

**Rationale**:
1. Kafka is being removed - no choice but to migrate
2. Unknown if downloads are actually the bottleneck
3. Can measure real impact and prioritize optimization based on data
4. Building batch-concurrent support is complex (~1 week work)

**Migration Pattern:**
```python
# OLD - Direct instantiation
from aiokafka import AIOKafkaConsumer

def _create_consumer(self) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        *self.topics,
        bootstrap_servers=self.config.bootstrap_servers,
        group_id=self.consumer_group,
        # ... 30 lines of config ...
    )

# NEW - Transport layer
from pipeline.common.transport import create_consumer

async def start(self):
    self.consumer = await create_consumer(
        config=self.config,
        domain=self.domain,
        worker_name="worker_name",
        topics=[self.topic],
        message_handler=self._handle_message,
        topic_key="topic_key",  # Maps to eventhub config
        instance_id=self.instance_id,
    )
    await self.consumer.start()
```

**Cleanup Checklist (per worker):**
- [ ] Replace `_create_consumer()` with `create_consumer()` call
- [ ] Remove `_consume_loop()` if present (handled by transport layer)
- [ ] Change message handler to process one message at a time
- [ ] Ensure all work is awaited synchronously (no background tasks)
- [ ] Remove unused config like `max_poll_records`
- [ ] Add transport layer contract to class docstring
- [ ] Test with both `PIPELINE_TRANSPORT=eventhub` and `=kafka`

### Phase 2: Build Batch-Concurrent Transport Layer (FUTURE OPTIMIZATION)

**Trigger**: Only pursue this when performance metrics show downloads/uploads are a bottleneck.

**Goal**: Restore 10x throughput improvement for download/upload workers.

**Approach**: Extend transport layer with `create_batch_consumer()` that supports concurrent processing.

**Required Features**:
1. **Message Buffering**: Accumulate messages into batches (up to `max_batch_size` or `batch_timeout_ms`)
2. **Batch Delivery**: Call handler with `List[PipelineMessage]` instead of single message
3. **Concurrent Processing**: Handler processes messages concurrently (using semaphore)
4. **Batch Checkpointing**: Commit all offsets only after entire batch completes

**Implementation Sketch**:
```python
async def create_batch_consumer(
    config: KafkaConfig,
    domain: str,
    worker_name: str,
    topics: list[str],
    batch_handler: Callable[[List[PipelineMessage]], Awaitable[None]],
    max_batch_size: int = 20,
    batch_timeout_ms: int = 1000,
    topic_key: str | None = None,
    instance_id: str | None = None,
) -> BatchConsumer:
    """
    Create consumer that accumulates messages into batches.

    For EventHub:
    - Buffers messages client-side until max_batch_size or timeout
    - Calls batch_handler([msg1, ..., msg20])
    - Handler can process them concurrently
    - Checkpoints all offsets after batch completes

    For Kafka (if still supported):
    - Uses getmany() to fetch batches naturally
    - Same batch_handler interface
    - Commits after batch completes
    """
```

**EventHub-Specific Challenges**:
- EventHub SDK is streaming (not batch-based) - need client-side buffering
- Checkpoint management: Must track ALL message offsets in batch
- Partial failure handling: What if 18/20 downloads succeed?
- Partition rebalancing: Flush incomplete batches before rebalance

**Download Worker Adaptation**:
```python
async def _handle_batch(self, messages: List[PipelineMessage]) -> None:
    """Process batch of download tasks concurrently."""
    tasks = [self._process_download(msg) for msg in messages]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Handle partial failures (retry failed messages via DLQ)
    for msg, result in zip(messages, results):
        if isinstance(result, Exception):
            await self._handle_failure(msg, result)
```

**Effort Estimate**: 3-5 days
- 1 day: Design and buffering logic
- 1-2 days: EventHub checkpoint management
- 1 day: Kafka wrapper (if needed)
- 1 day: Testing both transports

**Decision Point**: Build this only after measuring real production performance impact.

### Phase 3: Plugin Workers (Low Priority)

Migrate when plugins are actually deployed:
- `plugins/claimx_mitigation_task/mitigation_tracking_worker.py`
- `plugins/itel_cabinet_api/itel_cabinet_api_worker.py`
- `plugins/itel_cabinet_api/itel_cabinet_tracking_worker.py`
- `plugins/shared/workers/plugin_action_worker.py`

**Approach**: Analyze each plugin's processing pattern (single-message vs batch) and migrate accordingly.

### Phase 4: Documentation & Standardization

1. **Document Transport Layer Contract** (`src/pipeline/common/transport.py`):
   ```python
   """
   Transport Layer Contract:

   For Consumers:
   - create_consumer() returns a consumer wrapper that calls message_handler for EACH message
   - Offsets are committed AFTER message_handler returns successfully
   - If message_handler raises an exception, the message is NOT committed
   - All work must complete in message_handler (no background tasks)
   - Message handler signature: async def handler(record: PipelineMessage) -> None

   For Producers:
   - create_producer() returns a producer wrapper (EventHubProducer or BaseKafkaProducer)
   - Producers have a unified .send() interface regardless of transport
   """
   ```

2. **Create Worker Migration Guide** (`docs/WORKER_MIGRATION_GUIDE.md`):
   - Step-by-step instructions for migrating workers
   - Common pitfalls (background tasks, offset commit races)
   - Testing checklist (both transports)

3. **Add Linter Rule** (if possible):
   - Detect direct `AIOKafkaConsumer` imports in worker files
   - Suggest using transport layer instead

## Testing Strategy

For each migrated worker:

1. **Local Kafka Mode:**
   ```bash
   export PIPELINE_TRANSPORT=kafka
   docker-compose --profile kafka up -d
   python -m pipeline.runners.<worker_name>
   ```

2. **EventHub Mode:**
   ```bash
   export PIPELINE_TRANSPORT=eventhub
   export EVENTHUB_NAMESPACE_CONNECTION_STRING="..."
   python -m pipeline.runners.<worker_name>
   ```

3. **Verify:**
   - Worker connects successfully (no localhost:9092 errors)
   - Messages are consumed and processed
   - Offsets are committed (check consumer lag)
   - Graceful shutdown works (no data loss)

## Success Criteria

### Phase 1: All Workers Migrated to EventHub (Current Priority)

**Goal**: All workers functional in production (EventHub-only environment)

- [x] ClaimX `enrichment_worker.py` migrated
- [ ] Verisk `enrichment_worker.py` migrated
- [ ] ClaimX `download_worker.py` migrated (sequential processing)
- [ ] ClaimX `upload_worker.py` migrated (sequential processing)
- [ ] Verisk `download_worker.py` migrated (sequential processing)
- [ ] Verisk `upload_worker.py` migrated (sequential processing)
- [ ] All workers boot with `PIPELINE_TRANSPORT=eventhub`
- [ ] No `localhost:9092` or `AIOKafkaConsumer` errors
- [ ] Dead code cleaned up (no unused background task tracking)
- [ ] Transport layer contract documented in all worker docstrings

**Definition of Done**: All workers deploy to production and process messages successfully.

### Phase 2: Performance Optimization (Future, Triggered by Metrics)

**Goal**: Restore 10x throughput for download/upload workers if needed

- [ ] Production metrics collected for download/upload throughput
- [ ] Performance bottleneck confirmed (downloads ARE the problem)
- [ ] `create_batch_consumer()` designed and implemented
- [ ] Download/upload workers migrated to batch consumer
- [ ] Throughput restored to 50 MB/s (from 5 MB/s)

**Trigger**: Only pursue if download/upload performance impacts business SLAs.

### Phase 3: Complete Migration (Long Term)

- [ ] Plugin workers migrated (when deployed)
- [ ] Kafka support removed entirely from codebase
- [ ] Transport layer documented and standardized
- [ ] Migration patterns captured in developer guide

## Lessons Learned

### What Went Wrong
1. **Partial migration** - Some workers migrated, others weren't
2. **No enforcement** - No linter rule to catch direct AIOKafkaConsumer usage
3. **Unclear contract** - Transport layer behavior not documented
4. **Dead code accumulation** - Old patterns left behind during migration
5. **Abstraction mismatch** - Transport layer built for single-message processing, but some workers need batch-concurrent
6. **Infrastructure change not communicated** - Kafka being removed wasn't clear initially

### What Went Right
1. **Discovered the limitation early** - Before migrating ALL workers and losing performance
2. **Transport layer works well for its use case** - Single-message workers work perfectly
3. **Clear boundary identified** - We know exactly which workers need batch support
4. **Made pragmatic decision** - Ship fast (sequential) vs perfect (batch-concurrent)

### What to Do Better
1. **Understand infrastructure constraints upfront** - Kafka removal should have been communicated earlier
2. **Document contracts clearly** - Make abstraction behavior and limitations explicit
3. **Clean up dead code immediately** - Don't leave confusing remnants
4. **Accept imperfect migrations when necessary** - Sequential processing now > perfect solution in 2 weeks
5. **Measure before optimizing** - Don't build batch-concurrent support until we prove it's needed
6. **Document architectural decisions** - Explain WHY we accepted the performance tradeoff

## References

- Issue #38: Offset commit race condition (background tasks vs commit)
- `src/pipeline/common/transport.py`: Transport layer implementation
- `src/pipeline/common/eventhub/README.md`: EventHub setup guide
- `src/.env.example`: Transport configuration examples
