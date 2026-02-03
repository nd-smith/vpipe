# Transport Layer Migration Plan

## Problem Summary

Several workers in the pipeline are directly instantiating `AIOKafkaConsumer`, bypassing the transport layer abstraction. This causes connection failures when `PIPELINE_TRANSPORT=eventhub` because:

1. Workers try to connect to Kafka protocol on port 9092/9093
2. Azure Private Link only exposes AMQP (port 443), not Kafka protocol
3. The transport layer abstraction exists to handle this, but is not consistently used

## Why the Transport Layer Exists

**This is NOT premature abstraction** - it solves a real infrastructure constraint:

- **Production**: Azure Private Link only exposes port 443 (AMQP/EventHub protocol)
- **Local Dev**: Docker Compose runs local Kafka on port 9092 for testing
- **Both modes are actively used** - not theoretical

The abstraction is justified, but the implementation is incomplete (partial migration).

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

## Revised Migration Plan

### Phase 1: Migrate ONLY Single-Message Workers

**Can Be Migrated:**
- ✅ Verisk `enrichment_worker.py` - Single enrichment task processing

**CANNOT Be Migrated (batch-concurrent processing required):**
- ❌ ClaimX `download_worker.py` - Concurrent file downloads
- ❌ ClaimX `upload_worker.py` - Concurrent OneLake uploads
- ❌ Verisk `download_worker.py` - Concurrent file downloads
- ❌ Verisk `upload_worker.py` - Concurrent OneLake uploads

**Decision**: Keep download/upload workers using direct `AIOKafkaConsumer`. Document clearly.

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

### Phase 2: Document Why Download/Upload Workers Can't Migrate

Add clear documentation to download/upload workers explaining why they use direct `AIOKafkaConsumer`:

```python
class ClaimXDownloadWorker:
    """
    TRANSPORT LAYER COMPATIBILITY:
    This worker uses AIOKafkaConsumer directly instead of the transport layer
    because it requires batch-concurrent processing for performance:

    - Fetches 20 messages per batch via getmany()
    - Downloads up to 10 files concurrently using asyncio.gather()
    - Commits offsets once after the entire batch completes

    The transport layer only supports single-message sequential processing,
    which would reduce download throughput by 10x (from 50 MB/s to 5 MB/s).

    EventHub Limitation: This worker only works with PIPELINE_TRANSPORT=kafka
    in local development. For production EventHub deployment, either:
    1. Extend transport layer to support batch-concurrent processing
    2. Deploy a Kafka bridge for batch workers
    3. Accept 10x slower sequential processing (not recommended)
    """
```

**Workers to document:**
- `claimx/workers/download_worker.py`
- `claimx/workers/upload_worker.py`
- `verisk/workers/download_worker.py`
- `verisk/workers/upload_worker.py`

### Phase 3: Plugin Workers (Future)

Check plugin workers when they become active. Migrate only if they use single-message processing:
- `plugins/claimx_mitigation_task/mitigation_tracking_worker.py`
- `plugins/itel_cabinet_api/itel_cabinet_api_worker.py`
- `plugins/itel_cabinet_api/itel_cabinet_tracking_worker.py`
- `plugins/shared/workers/plugin_action_worker.py`

### Phase 4: Consider Extending Transport Layer (Long Term)

If EventHub deployment is required for download/upload workers, extend the transport layer to support batch processing:

```python
# New factory function
async def create_batch_consumer(
    config: KafkaConfig,
    batch_handler: Callable[[List[PipelineMessage]], Awaitable[None]],
    max_batch_size: int = 20,
    max_concurrency: int = 10,
    ...
) -> BatchConsumer:
    """
    Create consumer that processes messages in batches with concurrency.

    batch_handler receives a list of messages and can process them
    concurrently. Offsets are committed after the entire batch completes.
    """
```

**Challenges:**
- EventHub SDK uses streaming model (not batch-based like Kafka)
- Would need to buffer messages into batches client-side
- Checkpoint management becomes more complex
- May not map cleanly to EventHub's partition context model

**Recommendation**: Only pursue this if EventHub deployment for download/upload workers becomes a hard requirement.

### Phase 5: Documentation & Standardization

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

## Success Criteria (Revised)

**Phase 1: Single-Message Workers Migrated**
- [x] ClaimX enrichment worker uses transport layer
- [ ] Verisk enrichment worker uses transport layer
- [x] Dead code removed from migrated workers
- [x] Transport layer contract documented in worker docstrings

**Phase 2: Batch Workers Documented**
- [ ] Download/upload workers have clear docstrings explaining why they can't migrate
- [ ] Architecture decision recorded (transport layer limitation)

**Phase 3: No localhost:9092 Errors**
- [x] ClaimX enrichment boots with `PIPELINE_TRANSPORT=eventhub`
- [ ] Verisk enrichment boots with `PIPELINE_TRANSPORT=eventhub`
- [ ] All migrated workers tested with both transports

**Phase 4: Future Consideration**
- [ ] Decision made: Extend transport layer OR accept Kafka-only for batch workers

## Lessons Learned

### What Went Wrong
1. **Partial migration** - Some workers migrated, others weren't
2. **No enforcement** - No linter rule to catch direct AIOKafkaConsumer usage
3. **Unclear contract** - Transport layer behavior not documented
4. **Dead code accumulation** - Old patterns left behind during migration
5. **Abstraction mismatch** - Transport layer built for single-message processing, but some workers need batch-concurrent processing

### What Went Right
1. **Discovered the limitation early** - Before migrating all workers and losing performance
2. **Transport layer works well for its use case** - Single-message workers (enrichment, event ingestion) work fine
3. **Clear boundary identified** - Now we know which workers can/can't migrate

### What to Do Better
1. **Understand abstraction capabilities before migrating** - Check if the abstraction supports all worker patterns
2. **Document contracts clearly** - Make the abstraction's behavior and limitations explicit
3. **Clean up dead code immediately** - Don't leave confusing remnants
4. **Don't force-fit abstractions** - It's okay to have two patterns: single-message (transport layer) vs batch-concurrent (direct AIOKafkaConsumer)
5. **Document architectural decisions** - Explain WHY certain workers can't use the abstraction

## References

- Issue #38: Offset commit race condition (background tasks vs commit)
- `src/pipeline/common/transport.py`: Transport layer implementation
- `src/pipeline/common/eventhub/README.md`: EventHub setup guide
- `src/.env.example`: Transport configuration examples
