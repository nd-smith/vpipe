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

## Permanent Resolution Plan

### Phase 1: High Priority - Core Workers (Next Sprint)

Migrate the remaining core pipeline workers to use the transport layer.

**ClaimX:**
1. `download_worker.py` - Downloads media files from URLs
2. `upload_worker.py` - Uploads files to OneLake

**Verisk:**
1. `enrichment_worker.py` - Enriches xact events (mirrors claimx enricher)
2. `download_worker.py` - Downloads xact attachments
3. `upload_worker.py` - Uploads xact files to OneLake

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

### Phase 2: Medium Priority - Plugin Workers (Future)

Migrate plugin workers when they become active/needed:
- `plugins/claimx_mitigation_task/mitigation_tracking_worker.py`
- `plugins/itel_cabinet_api/itel_cabinet_api_worker.py`
- `plugins/itel_cabinet_api/itel_cabinet_tracking_worker.py`
- `plugins/shared/workers/plugin_action_worker.py`

**Note:** Defer these until plugins are actually deployed to avoid premature work.

### Phase 3: Documentation & Standardization

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

- [ ] All core workers (enrichment, download, upload) use transport layer
- [ ] No `KafkaConnectionError: Unable to bootstrap from localhost:9092` errors
- [ ] Workers boot successfully with `PIPELINE_TRANSPORT=eventhub`
- [ ] Local development still works with `PIPELINE_TRANSPORT=kafka`
- [ ] Dead code removed (no unused background task tracking)
- [ ] Transport layer contract documented
- [ ] Migration guide created for future workers

## Lessons Learned

### What Went Wrong
1. **Partial migration** - Some workers migrated, others weren't
2. **No enforcement** - No linter rule to catch direct AIOKafkaConsumer usage
3. **Unclear contract** - Transport layer behavior not documented
4. **Dead code accumulation** - Old patterns left behind during migration

### What to Do Better
1. **Complete migrations atomically** - Migrate all workers in one PR, or none
2. **Document contracts clearly** - Make the abstraction's behavior explicit
3. **Clean up dead code immediately** - Don't leave confusing remnants
4. **Add enforcement** - Linter rules, code review checklists, migration guides

## References

- Issue #38: Offset commit race condition (background tasks vs commit)
- `src/pipeline/common/transport.py`: Transport layer implementation
- `src/pipeline/common/eventhub/README.md`: EventHub setup guide
- `src/.env.example`: Transport configuration examples
