# Testing Next Steps

## Current Status

**Total Test Coverage: 357 tests passing ✓**

### Phase 1: Core Pipeline Workers (224 tests) ✓
- Event Ingesters: 97 tests
- Download Workers: 40 tests
- Upload Workers: 42 tests
- Enrichment Workers: 45 tests

### Phase 2: Delta and Result Processing (79 tests) ✓
- Result Processors: 36 tests
- Delta Workers: 43 tests

### Phase 3: Core Supporting Infrastructure (54 tests) ✓
- HealthCheckServer: 29 tests
- DeltaRetryHandler: 25 tests

---

## Remaining Test Opportunities

### High Priority Infrastructure

#### 1. Metrics Module (`pipeline/common/metrics.py`)
**Complexity:** Low | **Value:** High | **Est. Tests:** 15-20

Simple metrics recording functions with graceful degradation when prometheus-client unavailable.

**Test Coverage Needed:**
- NoOpMetric class behavior (accepts any method call, returns self for chaining)
- Lazy initialization of prometheus_client
- Registry integration via telemetry module
- Metric creation (Counter, Gauge, Histogram)
- Duplicate metric handling (ValueError on re-registration)
- Convenience functions:
  - `record_message_produced/consumed()`
  - `record_processing_error()`, `record_producer_error()`
  - `update_consumer_lag/offset()`
  - `update_connection_status()`
  - `update_assigned_partitions()`
  - `record_delta_write()`
  - `record_dlq_message()`

**Testing Approach:**
- Mock prometheus_client import (both available and unavailable)
- Mock telemetry registry
- Verify NoOpMetric accepts arbitrary method chains
- Verify label creation and value setting
- Test counter increments, gauge sets, histogram observations

---

### Medium Priority Infrastructure

#### 2. OneLake Storage Client (`pipeline/common/storage/onelake.py`)
**Complexity:** Medium | **Value:** Medium | **Est. Tests:** 25-30

Azure OneLake/ADLS Gen2 storage operations.

**Test Coverage Needed:**
- Client initialization (with/without credentials)
- File upload operations (single file, batch)
- File download operations
- Directory operations (create, list, delete)
- Path validation and normalization
- Error handling (network failures, auth errors, not found)
- Retry logic for transient failures
- Connection pooling and cleanup

**Testing Approach:**
- Mock Azure SDK calls (azure.storage.filedatalake)
- Test path transformations (abfss:// URLs)
- Verify retry on transient errors
- Test credential handling

#### 3. Delta Writers (`pipeline/common/writers/delta_writer.py`)
**Complexity:** Medium-High | **Value:** High | **Est. Tests:** 30-35

Delta Lake write implementations for various table types.

**Test Coverage Needed:**
- DeltaEventsWriter (raw event writes)
- ClaimXEventsDeltaWriter (ClaimX events)
- ClaimXEntityWriter (multi-table entity writes)
- Schema validation and evolution
- Merge operations (upsert logic)
- Partition handling
- Transaction commit/rollback
- Error classification (schema vs transient)
- Write performance tracking

**Testing Approach:**
- Mock Delta Lake operations
- Test batch accumulation and flushing
- Verify merge keys and update logic
- Test schema mismatch detection
- Verify partition column handling

#### 4. Transport Layer (`pipeline/common/transport.py`)
**Complexity:** Medium | **Value:** Medium | **Est. Tests:** 20-25

EventHub producer/consumer factory and configuration.

**Test Coverage Needed:**
- `create_producer()` factory function
- `create_consumer()` factory function
- Configuration resolution from KafkaConfig
- Topic name resolution (domain + topic_key)
- Connection string handling
- Consumer group assignment
- Error handling during initialization
- Producer/consumer lifecycle

**Testing Approach:**
- Mock EventHub SDK
- Mock KafkaConfig
- Verify correct parameters passed to EventHub constructors
- Test topic resolution logic

---

### Lower Priority (Optional)

#### 5. Enrichment Pipeline (`pipeline/plugins/shared/enrichment.py`)
**Complexity:** Medium | **Value:** Low | **Est. Tests:** 15-20

Generic enrichment pipeline framework used by plugins.

**Test Coverage Needed:**
- Pipeline initialization
- Stage execution order
- Data flow through stages
- Error handling per stage
- Stage skip logic
- Result aggregation

#### 6. Connection Manager (`pipeline/plugins/shared/connections.py`)
**Complexity:** Medium | **Value:** Low | **Est. Tests:** 20-25

HTTP connection pooling and retry for API calls.

**Test Coverage Needed:**
- Connection pool initialization
- Retry logic with backoff
- Circuit breaker integration
- Timeout handling
- Connection cleanup
- Health check integration

#### 7. DLQ Handler (EventHub-based) (`pipeline/common/dlq/handler.py`)
**Complexity:** Low | **Value:** Low | **Est. Tests:** 10-15

**Note:** Less critical now that DeltaRetryHandler is tested. This is the generic EventHub DLQ handler.

**Test Coverage Needed:**
- Message routing to DLQ
- Error metadata preservation
- Retry count tracking
- DLQ topic resolution

---

## Testing Patterns Established

### Successful Patterns to Reuse:

1. **Real vs Mock Balance**
   - HealthCheckServer: Real HTTP endpoints (authentic testing)
   - DeltaRetryHandler: Mocked dependencies (unit isolation)

2. **Fixture Design**
   - Use `Mock()` without spec for flexible mocking
   - Create reusable message/event fixtures
   - Use `AsyncMock` for async components

3. **Test Organization**
   - Group by functionality (Initialization, Lifecycle, Core Logic)
   - 4-7 test classes per module
   - 15-30 tests total per module

4. **Async Testing**
   - Use `pytest.mark.asyncio` consistently
   - Save mock references before cleanup (before `stop()` sets to None)
   - Test both success and error paths

5. **Coverage Focus**
   - Happy path (basic functionality)
   - Error handling (expected failures)
   - Edge cases (empty batches, None values)
   - Configuration variants

---

## Recommendations

### If Continuing Testing:

**Next Module:** Metrics (`pipeline/common/metrics.py`)
- Quick win (simple, straightforward)
- High value (used everywhere)
- Good warm-up before complex modules

**Then Consider:** Delta Writers
- Critical for data integrity
- Complex logic worth testing
- Build on DeltaRetryHandler tests

### If Stopping Here:

Current coverage (357 tests) provides:
- ✓ All core pipeline workers tested
- ✓ All delta and result processing tested
- ✓ Critical infrastructure (health checks, retry) tested
- ✓ Solid foundation for production confidence

Remaining modules are either:
- Lower risk (metrics gracefully degrade)
- Lower complexity (can test as bugs arise)
- Plugin-specific (less critical for core pipeline)

---

## Test Execution

Run all tests:
```bash
source .venv/bin/activate && pytest tests/ -v
```

Run specific phase:
```bash
# Phase 1
pytest tests/pipeline/claimx/workers/test_event_ingester.py tests/pipeline/verisk/workers/test_event_ingester.py -v
pytest tests/pipeline/claimx/workers/test_download_worker.py tests/pipeline/verisk/workers/test_download_worker.py -v
pytest tests/pipeline/claimx/workers/test_upload_worker.py tests/pipeline/verisk/workers/test_upload_worker.py -v
pytest tests/pipeline/claimx/workers/test_enrichment_worker.py tests/pipeline/verisk/workers/test_enrichment_worker.py -v

# Phase 2
pytest tests/pipeline/claimx/workers/test_result_processor.py tests/pipeline/verisk/workers/test_result_processor.py -v
pytest tests/pipeline/claimx/workers/test_entity_delta_worker.py tests/pipeline/claimx/workers/test_delta_events_worker.py tests/pipeline/verisk/workers/test_delta_events_worker.py -v

# Phase 3
pytest tests/pipeline/common/test_health.py -v
pytest tests/pipeline/common/retry/test_delta_handler.py -v
```

---

## Documentation

**Test Summaries:**
- `/tmp/phase1_worker_testing_summary.txt` - Phase 1 progress
- `/tmp/phase2_delta_and_results_summary.txt` - Phase 2 progress
- `/tmp/phase3_infrastructure_summary.txt` - Phase 3 progress

**Architecture Docs:**
- `docs/CLAIMX_PIPELINE.md` - ClaimX worker flow
- `docs/VERISK_PIPELINE.md` - Verisk worker flow
- `docs/TEST_COVERAGE_REMEDIATION_PLAN.md` - Original test strategy

---

*Last Updated: 2026-02-05*
*Current Test Count: 357 passing*
