# Testing Next Steps

## Current Status

**Total Test Coverage: 415 tests passing ✓**

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

### Phase 4: Metrics and Writers Infrastructure (58 tests) ✓
- Metrics Module: 33 tests
- Delta Writers: 25 tests

---

## Remaining Test Opportunities

### Medium Priority Infrastructure

#### 1. OneLake Storage Client (`pipeline/common/storage/onelake.py`)
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

#### 2. Transport Layer (`pipeline/common/transport.py`)
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

#### 3. Enrichment Pipeline (`pipeline/plugins/shared/enrichment.py`)
**Complexity:** Medium | **Value:** Low | **Est. Tests:** 15-20

Generic enrichment pipeline framework used by plugins.

**Test Coverage Needed:**
- Pipeline initialization
- Stage execution order
- Data flow through stages
- Error handling per stage
- Stage skip logic
- Result aggregation

#### 4. Connection Manager (`pipeline/plugins/shared/connections.py`)
**Complexity:** Medium | **Value:** Low | **Est. Tests:** 20-25

HTTP connection pooling and retry for API calls.

**Test Coverage Needed:**
- Connection pool initialization
- Retry logic with backoff
- Circuit breaker integration
- Timeout handling
- Connection cleanup
- Health check integration

#### 5. DLQ Handler (EventHub-based) (`pipeline/common/dlq/handler.py`)
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

**Next Module:** OneLake Storage Client (`pipeline/common/storage/onelake.py`)
- Medium complexity
- Azure OneLake/ADLS Gen2 storage operations
- Used by Delta writers and workers

**Then Consider:** Transport Layer
- Medium complexity
- EventHub producer/consumer factory
- Used by all workers

### If Stopping Here:

Current coverage (415 tests) provides:
- ✓ All core pipeline workers tested
- ✓ All delta and result processing tested
- ✓ Critical infrastructure (health checks, retry, metrics, writers) tested
- ✓ Solid foundation for production confidence

Remaining modules are either:
- Medium complexity (can test as needed)
- Lower risk (graceful degradation)
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

# Phase 4
pytest tests/pipeline/common/test_metrics.py -v
pytest tests/pipeline/common/writers/test_delta_writer.py -v
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
*Current Test Count: 415 passing*
