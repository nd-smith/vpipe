# Test Coverage Remediation Plan

**Date:** 2026-02-04
**Reviewer:** Claude
**Codebase:** vpipe Pipeline Workers

## Executive Summary

This document outlines missing test coverage for the vpipe pipeline architecture and provides a prioritized remediation plan. The pipeline is well-architected with solid patterns (retry/DLQ, batch processing, circuit breakers), but lacks comprehensive unit tests for core worker components.

**Current State:** ~42 test files covering schemas, retry handlers, DLQ handlers, and Delta writers
**Gap:** No unit tests for 17+ core pipeline workers and 9+ event handlers
**Risk Level:** Medium-High (production code without test coverage for complex async workflows)

### 🎯 Testing Approach: Unit Tests First

**85-90% of this plan can be accomplished with pure unit tests** - no Docker, Kafka, EventHub, or Delta infrastructure required. Following the existing pattern in `test_enrichment_handler.py`, we can test all business logic, error handling, async workflows, and concurrency control using `AsyncMock` and `Mock`.

**What This Plan Covers:**
- ✅ **Unit Tests (No Infrastructure):** ~200 hours - Phases 1-4
- 🔄 **Integration Tests (Optional/Future):** ~25 hours - Phase 5 deferred

**Infrastructure Requirements:** None for 85-90% of tests

---

## 1. Unit Tests vs Integration Tests

### 1.1 What Can Be Tested WITHOUT Infrastructure (85-90%)

**Pure unit tests using AsyncMock** - following existing `test_enrichment_handler.py` pattern:

| Component | Unit Test Coverage | Infra Needed? |
|-----------|-------------------|---------------|
| **Worker Initialization** | 100% | ❌ No |
| **Message Parsing** | 100% | ❌ No |
| **Business Logic** | 100% | ❌ No |
| **Handler Routing** | 100% | ❌ No |
| **Error Categorization** | 100% | ❌ No |
| **Retry/DLQ Logic** | 100% | ❌ No (already tested) |
| **Circuit Breaker** | 100% | ❌ No |
| **Concurrency Control** | 95% | ❌ No |
| **API Call Patterns** | 100% | ❌ No (mock API client) |
| **Entity Row Generation** | 100% | ❌ No |
| **Project Cache** | 100% | ❌ No |
| **Batch Processing** | 95% | ❌ No |
| **Lifecycle (start/stop)** | 90% | ❌ No |

**Example - No Infrastructure Needed:**
```python
@pytest.mark.asyncio
async def test_enrichment_worker_routes_to_correct_handler(
    mock_config, mock_api_client, mock_producer
):
    # All mocked - no Kafka, no EventHub, no Delta
    worker = ClaimXEnrichmentWorker(
        config=mock_config,
        api_client=mock_api_client
    )
    worker.producer = mock_producer  # AsyncMock

    message = create_test_message(event_type="PROJECT_CREATED")
    await worker._handle_enrichment_task(message)

    # Verify handler was called correctly
    assert mock_api_client.get_project.called
    assert mock_producer.send.called
```

### 1.2 What REQUIRES Infrastructure (10-15%)

**Integration tests** - deferred to Phase 5 (optional):

| Test Type | Infra Required | Can Defer? |
|-----------|----------------|------------|
| **Actual Kafka consumption** | Kafka/EventHub | ✅ Yes |
| **Actual Delta writes** | Delta Lake | ✅ Yes |
| **Actual OneLake uploads** | OneLake | ✅ Yes |
| **Actual HTTP downloads** | Test HTTP server | ✅ Yes |
| **End-to-end flows** | Full stack | ✅ Yes |
| **Consumer rebalancing** | Kafka cluster | ✅ Yes |

**These tests validate I/O operations, not business logic.** Business logic is fully testable with mocks.

---

## 2. Coverage Analysis

### 2.1 Existing Coverage (Good ✅)

| Component | Coverage | Test Files |
|-----------|----------|------------|
| **Retry Handlers** | Complete | `test_enrichment_handler.py`, `test_download_handler.py` |
| **DLQ Handlers** | Complete | `test_handler.py` (both domains) |
| **Delta Writers** | Complete | `test_delta_events.py`, `test_delta_entities.py`, `test_delta_inventory.py` |
| **Schemas** | Complete | `test_events.py`, `test_entities.py`, `test_tasks.py`, `test_results.py` |
| **API Client** | Complete | `test_api_client.py` |
| **Checkpoint Stores** | Complete | `test_checkpoint_store.py` |
| **Plugin System** | Partial | `test_loader.py` |
| **Storage Utilities** | Partial | `test_delta_resource_cleanup.py` |

### 2.2 Missing Coverage (Gaps ❌)

#### Priority 1: Core Pipeline Workers
**Risk:** High complexity, async workflows, critical path

| Worker | File | Complexity | Lines | Critical Paths |
|--------|------|-----------|-------|----------------|
| **Event Ingester** | `claimx/workers/event_ingester.py` | Medium | 452 | Event parsing, deduplication, enrichment task creation |
| **Enrichment Worker** | `claimx/workers/enrichment_worker.py` | High | 764 | Handler routing, API enrichment, entity dispatch, download task creation, project cache |
| **Download Worker** | `claimx/workers/download_worker.py` | High | 833 | Concurrent downloads, semaphore control, batch processing, cache management, circuit breaker |
| **Upload Worker** | `claimx/workers/upload_worker.py` | Medium | ? | OneLake upload, cache cleanup, result production |
| **Delta Events Worker** | `claimx/workers/delta_events_worker.py` | Medium | ? | Batch accumulation, flush triggers |
| **Entity Delta Worker** | `claimx/workers/entity_delta_worker.py` | Medium | ? | Entity row batching, Delta writes |
| **Result Processor** | `claimx/workers/result_processor.py` | Medium | ? | Metrics emission, result tracking |
| **Download Factory** | `claimx/workers/download_factory.py` | Low | ? | Media → DownloadTask conversion |

**Verisk workers follow identical patterns - similar test needs**

#### Priority 2: Event Handlers
**Risk:** Business logic, API integration, data transformation

| Handler | File | Complexity | API Calls | Entity Tables |
|---------|------|-----------|-----------|---------------|
| **Handler Registry** | `handlers/base.py` | Medium | 0 | - |
| **Project Handler** | `handlers/project.py` | Medium | 1-2 | projects, contacts |
| **Project Update Handler** | `handlers/project_update.py` | Medium | 1 | projects |
| **Task Handler** | `handlers/task.py` | Medium | 1-2 | tasks, task_templates |
| **Media Handler** | `handlers/media.py` | Medium | 1 | media |
| **Video Handler** | `handlers/video.py` | Medium | 1 | video_collab |
| **Project Cache** | `handlers/project_cache.py` | Low | 0 | - |
| **Transformers** | `handlers/transformers.py` | Low | 0 | - |

#### Priority 3: Plugin Workers
**Risk:** Domain-specific business logic

- `shared/workers/plugin_action_worker.py` - Generic plugin worker
- `claimx_mitigation_task/mitigation_tracking_worker.py` - Mitigation tracking
- `itel_cabinet_api/itel_cabinet_api_worker.py` - iTel API integration
- `itel_cabinet_api/itel_cabinet_tracking_worker.py` - iTel tracking

#### Priority 4: Common Infrastructure
**Risk:** Shared utilities, harder to test in isolation

- `transport.py` - Consumer/producer factories
- `storage/onelake.py` - OneLake client
- `storage/delta.py` - Delta table operations (beyond resource cleanup)
- `health.py` - Health check server
- `metrics.py` - Metrics emission (Prometheus/ADX)
- `telemetry.py` - Telemetry initialization
- `eventhub/batch_consumer.py` - EventHub batch consumer

---

## 3. Testing Strategy

### 3.1 Testing Principles (per CLAUDE.md)

- **Simple and explicit** - No complex test fixtures unless necessary
- **One test per scenario** - Descriptive test names: `test_returns_none_when_user_not_found`
- **Mock at boundaries** - Mock API clients, producers, consumers
- **Use pytest fixtures** - Shared setup in `conftest.py`
- **No unsolicited tests** - Only add when asked or for non-trivial logic

### 3.2 Unit Test Focus

**All tests in Phases 1-4 are pure unit tests:**
- Mock all external dependencies (Kafka, APIs, Delta, OneLake)
- Test business logic in isolation
- Fast execution (<3 seconds per test)
- No Docker, no infrastructure, no network calls
- Follow existing `test_enrichment_handler.py` pattern

### 3.3 Worker Testing Pattern

Based on existing retry handler tests, establish this pattern:

```python
class TestWorkerInitialization:
    """Test worker setup and configuration."""
    - Config validation
    - Topic/consumer group setup
    - Health check initialization
    - Instance ID handling

class TestWorkerLifecycle:
    """Test start/stop/graceful shutdown."""
    - Start sequence (producers, consumers, health server)
    - Stop sequence (cleanup, offset commit)
    - Graceful shutdown with in-flight tasks

class TestMessageProcessing:
    """Test core message handling logic."""
    - Valid message parsing
    - Invalid message handling (ValidationError, JSONDecodeError)
    - Success path (main workflow)
    - Failure path (retry/DLQ routing)

class TestErrorHandling:
    """Test error category routing."""
    - PERMANENT → DLQ
    - TRANSIENT → retry
    - AUTH → retry
    - CIRCUIT_OPEN → reprocess
    - UNKNOWN → retry

class TestConcurrency:  # For download/upload workers
    """Test concurrent processing."""
    - Semaphore limits
    - Batch processing
    - In-flight task tracking
    - Timeout handling
```

### 3.4 Handler Testing Pattern

```python
class TestHandlerRegistration:
    """Test handler registry functionality."""
    - Handler registration
    - Event type routing
    - Handler lookup

class TestHandlerProcessing:
    """Test event processing logic."""
    - Successful API enrichment
    - Entity row generation
    - Error handling (API errors)
    - Batch processing

class TestHandlerCaching:  # For handlers using project cache
    """Test cache behavior."""
    - Cache hits
    - Cache misses
    - Cache TTL
    - Preload from Delta
```

---

## 4. Remediation Plan (Unit Tests Only)

**All phases use pure unit tests with AsyncMock - no infrastructure required.**

### Phase 1: Critical Path Workers (Weeks 1-2) 🚀 **START HERE**

**Goal:** Test the most complex, high-risk workers first
**Infrastructure:** None - all mocked

#### Week 1: Enrichment Workers (40h)
- [ ] `test_claimx_enrichment_worker.py` - Handler routing, API calls, caching
  - Mock: `ClaimXApiClient`, producers, project cache
  - Test: Handler selection, API error handling, entity dispatch, download task creation
- [ ] `test_verisk_enrichment_worker.py` - Plugin execution, attachment detection
  - Mock: API client, plugin registry, producers
  - Test: Plugin routing, attachment filtering, API enrichment
- [ ] `test_event_ingester_worker.py` (ClaimX) - Event parsing, deduplication
  - Mock: Consumer, producer
  - Test: Event parsing, dedup logic, enrichment task creation
- [ ] `test_event_ingester_worker.py` (Verisk) - EventHub integration
  - Mock: EventHub consumer, producer
  - Test: Same as ClaimX

**Why start here:** Enrichment is the most complex stage with handler routing, caching, and multiple outputs. High business logic complexity.

#### Week 2: Download Workers (40h)
- [ ] `test_claimx_download_worker.py` - Concurrent downloads, batch processing, circuit breaker
  - Mock: `AttachmentDownloader.download()`, producers, consumers
  - Test: Semaphore limits, batch processing, in-flight tracking, circuit breaker state
- [ ] `test_verisk_download_worker.py` - Same patterns
- [ ] `test_download_factory.py` - Media → DownloadTask conversion
  - Mock: None (pure logic)
  - Test: URL extraction, path generation, task creation

**Why next:** Download workers have complex concurrency control (semaphores) and circuit breaker logic.

### Phase 2: Supporting Workers (Weeks 3-4)

**Goal:** Complete worker coverage
**Infrastructure:** None - all mocked

#### Week 3: Upload & Result Processing (40h)
- [ ] `test_claimx_upload_worker.py` - OneLake uploads, cache cleanup
  - Mock: `OneLakeClient.upload()`, producers, filesystem operations
  - Test: Upload flow, cache cleanup, error handling, result production
- [ ] `test_verisk_upload_worker.py` - Same patterns
- [ ] `test_claimx_result_processor.py` - Metrics emission, result tracking
  - Mock: Producers, metrics emission
  - Test: Result aggregation, metrics calculation, Delta dispatch
- [ ] `test_verisk_result_processor.py` - Same patterns

#### Week 4: Delta Writers & Logging (40h)
- [ ] `test_claimx_delta_events_worker.py` - Batch accumulation, flush triggers
  - Mock: Delta write operations
  - Test: Batch size triggers, timeout triggers, graceful shutdown flush
- [ ] `test_claimx_entity_delta_worker.py` - Entity batching
  - Mock: Delta write operations
  - Test: Entity batching, flush logic, error routing
- [ ] `test_verisk_delta_events_worker.py` - Same patterns
- [ ] `test_periodic_logger.py` (Verisk) - Health monitoring
  - Mock: Metrics, logging
  - Test: Periodic logging intervals, health metrics

### Phase 3: Event Handlers (Weeks 5-6)

**Goal:** Test business logic and data transformation
**Infrastructure:** None - all mocked

#### Week 5: Core Handlers (20h)
- [ ] `test_handler_registry.py` - Handler registration, event routing
  - Mock: None (pure logic)
  - Test: Handler lookup, event grouping, missing handlers
- [ ] `test_project_handler.py` - Project API enrichment
  - Mock: `ClaimXApiClient.get_project()`
  - Test: API response parsing, entity row generation, error handling
- [ ] `test_task_handler.py` - Task API enrichment
  - Mock: `ClaimXApiClient.get_task()`
  - Test: Task data parsing, entity row generation
- [ ] `test_media_handler.py` - Media API enrichment
  - Mock: `ClaimXApiClient.get_media()`
  - Test: Media data parsing, download URL extraction

#### Week 6: Specialized Handlers & Utilities (20h)
- [ ] `test_video_handler.py` - Video collaboration enrichment
  - Mock: API client
  - Test: Video data parsing, entity generation
- [ ] `test_project_update_handler.py` - Project update events
  - Mock: API client
  - Test: Update parsing, change detection
- [ ] `test_project_cache.py` - Cache TTL, preload, eviction
  - Mock: None (pure logic), Delta reads for preload
  - Test: Cache hit/miss, TTL expiration, preload from Delta
- [ ] `test_transformers.py` - Data transformation utilities
  - Mock: None (pure logic)
  - Test: Data mapping, field transformations

### Phase 4: Plugin Workers (Week 7)

**Goal:** Test domain-specific integrations
**Infrastructure:** None - all mocked

#### Week 7: Plugins (20h)
- [ ] `test_plugin_action_worker.py` - Generic plugin worker
  - Mock: API clients, producers, plugin handlers
  - Test: Handler pipeline, API routing, error handling
- [ ] `test_mitigation_tracking_worker.py` - ClaimX mitigation tasks
  - Mock: ClaimX API, producers
  - Test: Mitigation enrichment, event production
- [ ] `test_itel_cabinet_api_worker.py` - iTel API worker
  - Mock: iTel API client, producers
  - Test: API transformation, error handling
- [ ] `test_itel_cabinet_tracking_worker.py` - iTel tracking worker
  - Mock: Same as above
  - Test: Tracking logic, API calls

---

## 5. Phase 5: Integration Tests (OPTIONAL - DEFERRED)

**⚠️ This phase requires infrastructure and can be skipped initially.**

These tests validate I/O operations, not business logic. Business logic is fully covered in Phases 1-4.

**Infrastructure Required:**
- Docker Compose with Kafka/EventHub emulator
- Delta Lake (local or test environment)
- OneLake test account
- HTTP test server

**When to implement:**
- After Phases 1-4 are complete
- When team is ready to introduce test infrastructure
- For pre-production validation
- For debugging production issues

**Estimated effort:** 25 hours (5-10 test scenarios)

**Example integration tests:**
```python
@pytest.mark.integration
@pytest.mark.requires_kafka
async def test_end_to_end_enrichment_flow():
    # Requires: Kafka, Delta, OneLake
    # Validates: Actual message consumption, writes, uploads
    pass
```

**Recommendation:** Focus on Phases 1-4 first. Integration tests can wait.

---

## 6. Implementation Guidelines

### 6.1 Mocking Strategy (No Infrastructure Pattern)

**Mock at System Boundaries:**
- API clients (`ClaimXApiClient`, `VeriskApiClient`) → `AsyncMock`
- Kafka producers/consumers → `AsyncMock`
- Delta table operations → `Mock` with side effects
- OneLake client → `Mock`
- HTTP sessions (`aiohttp.ClientSession`) → `AsyncMock`

**Don't Mock:**
- Business logic (handlers, transformers)
- Schema validation (Pydantic models)
- Error categorization logic
- Metrics/logging calls

**Example: Complete Worker Test Without Infrastructure**

```python
# tests/pipeline/claimx/workers/test_enrichment_worker.py

@pytest.fixture
def mock_config():
    """Mock KafkaConfig - no actual Kafka needed."""
    config = Mock(spec=KafkaConfig)
    config.get_topic.return_value = "test.topic"
    config.get_consumer_group.return_value = "test-group"
    config.get_retry_delays.return_value = [300, 600]
    config.get_max_retries.return_value = 2
    config.claimx_api_url = "https://api.test.com"
    config.claimx_api_token = "test-token"
    return config

@pytest.fixture
def mock_api_client():
    """Mock API client - no actual API calls."""
    client = AsyncMock(spec=ClaimXApiClient)
    client.get_project.return_value = {
        "id": "proj-123",
        "name": "Test Project"
    }
    client.is_circuit_open = False
    return client

@pytest.fixture
def mock_producer():
    """Mock Kafka producer - no actual Kafka."""
    producer = AsyncMock()
    producer.send.return_value = Mock(partition=0, offset=123)
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    return producer

@pytest.mark.asyncio
async def test_enrichment_worker_processes_valid_message(
    mock_config, mock_api_client, mock_producer
):
    """Test enrichment worker with all dependencies mocked."""
    # Setup
    worker = ClaimXEnrichmentWorker(
        config=mock_config,
        api_client=mock_api_client
    )
    worker.producer = mock_producer
    worker.download_producer = mock_producer

    # Test data
    message = PipelineMessage(
        topic="test.topic",
        partition=0,
        offset=1,
        key=b"evt-123",
        value=json.dumps({
            "event_id": "evt-123",
            "event_type": "PROJECT_CREATED",
            "project_id": "proj-123",
            "created_at": "2024-01-01T00:00:00Z"
        }).encode()
    )

    # Execute
    await worker._handle_enrichment_task(message)

    # Verify
    assert mock_api_client.get_project.called
    assert mock_producer.send.called

    # No Kafka, no Delta, no OneLake - pure unit test!
```

### 6.2 Fixture Organization

Use `conftest.py` per domain:
- `tests/pipeline/claimx/workers/conftest.py` - ClaimX worker fixtures
- `tests/pipeline/verisk/workers/conftest.py` - Verisk worker fixtures
- `tests/pipeline/claimx/handlers/conftest.py` - Handler fixtures

**Common Fixtures:**
```python
@pytest.fixture
def mock_kafka_config():
    """Mock KafkaConfig with standard settings."""

@pytest.fixture
def mock_api_client():
    """Mock API client with common responses."""

@pytest.fixture
def mock_producer():
    """Mock Kafka producer."""

@pytest.fixture
def sample_event():
    """Sample event message for testing."""
```

### 6.3 Test Naming Convention

Follow pattern: `test_<condition>_<expected_result>`

**Examples:**
- `test_valid_message_creates_enrichment_task`
- `test_invalid_json_raises_validation_error`
- `test_permanent_error_routes_to_dlq`
- `test_circuit_open_skips_commit`
- `test_batch_processing_respects_semaphore_limit`

### 6.4 Async Testing

Use `pytest-asyncio`:
```python
@pytest.mark.asyncio
async def test_worker_starts_successfully(mock_config):
    worker = ClaimXEnrichmentWorker(config=mock_config)
    await worker.start()
    assert worker._running
    await worker.stop()
```

### 6.5 Coverage Metrics

**Target Coverage by Component:**
- Workers: 80%+ (focus on critical paths)
- Handlers: 85%+ (business logic)
- Retry/DLQ: 90%+ (already achieved)
- Schemas: 95%+ (already achieved)

**Measure with:**
```bash
pytest --cov=src/pipeline --cov-report=html --cov-report=term-missing
```

---

## 7. Risk Assessment

### 7.1 High-Risk Areas (Test First - All Unit Testable)

**Enrichment Workers:**
- Complex handler routing logic
- Project cache correctness
- API error handling with circuit breaker
- Download task generation from media metadata

**Download Workers:**
- Semaphore concurrency control
- Batch processing with in-flight tracking
- Circuit breaker state management
- Temporary file cleanup

**Handler Registry:**
- Event type → handler mapping
- Missing handler scenarios
- Batch grouping by handler

### 7.2 Medium-Risk Areas (All Unit Testable)

**Upload Workers:**
- OneLake upload retry logic
- Cache cleanup after success/failure
- Result message production

**Delta Writers:**
- Batch flush triggers (size + timeout)
- Graceful shutdown with pending batches
- Failed batch routing to retry topics

### 7.3 Low-Risk Areas (Test Later - All Unit Testable)

**Utility Classes:**
- Download factory (simple conversion)
- Transformers (data mapping)
- Metrics emission (fire-and-forget)

---

## 8. Success Criteria

### 8.1 Quantitative (Unit Tests Only)

- [ ] **85%** code coverage for workers (business logic)
- [ ] **90%** code coverage for handlers (business logic)
- [ ] **~100** new unit test functions added
- [ ] **0** failing tests in CI/CD
- [ ] **<3 min** unit test suite execution time
- [ ] **<3 sec** per individual test (fast with mocks)

### 8.2 Qualitative

- [ ] All critical paths have unit test coverage
- [ ] Error scenarios are tested (retry, DLQ, circuit breaker)
- [ ] Concurrent processing logic is validated
- [ ] Test names clearly describe scenarios
- [ ] Minimal test complexity (AsyncMock pattern)
- [ ] **No infrastructure dependencies** in unit tests

---

## 9. Maintenance Plan

### 9.1 Ongoing

- Add tests for new workers before merging
- Update tests when changing worker behavior
- Monitor test execution time (keep fast)
- Review coverage reports in PR reviews

### 9.2 Monthly

- Review and update test fixtures
- Remove flaky tests
- Refactor duplicated test code

### 9.3 Quarterly

- Audit test coverage for new modules
- Update test patterns based on lessons learned
- Review and update this plan

---

## 10. Notes & Considerations

### 10.1 Testing Philosophy Alignment

Per `CLAUDE.md`:
- "Do not add tests unless asked"
- "When working on non-trivial logic, ask if tests are wanted"

**Action:** This plan assumes approval to add comprehensive tests. Confirm with team before proceeding.

### 10.2 Test Data Strategy

- Use **fixtures** for common test data (events, tasks, configs)
- Keep test data **minimal** (only fields needed for test)
- Use **factories** for complex object graphs (if needed later)

### 10.3 Integration vs Unit Tests

This plan **prioritizes unit tests** (Phases 1-4):
- Mock all external dependencies (Kafka, APIs, Delta, OneLake)
- Test worker logic in isolation
- Fast execution (<3 min total, <3 sec per test)
- **No infrastructure required**

**Phase 5 (Integration tests)** is deferred:
- Requires Docker, Kafka/EventHub, Delta, OneLake
- Tests I/O operations, not business logic
- Can be added later when team is ready
- Estimated 25 hours (vs 200 hours for unit tests)

### 10.4 No Infrastructure in CI/CD

Unit tests run in CI/CD **without any infrastructure**:

Ensure tests run in CI/CD pipeline:
```yaml
# .github/workflows/test.yml
- name: Run unit tests (no infrastructure)
  run: |
    pytest tests/pipeline/ \
      -m "not integration" \
      --cov=src/pipeline \
      --cov-fail-under=80 \
      --maxfail=10 \
      -v \
      --timeout=3  # Each test must complete in <3 sec

# Integration tests (optional, separate job)
- name: Run integration tests (requires infrastructure)
  if: false  # Disabled until infrastructure is ready
  run: |
    pytest tests/pipeline/ \
      -m "integration" \
      --timeout=30
```

**Mark integration tests for future:**
```python
@pytest.mark.integration
@pytest.mark.requires_kafka
async def test_actual_kafka_consumption():
    # Requires Kafka - skip in CI for now
    pass
```

---

## Appendix A: Test File Locations

### ClaimX Workers
```
tests/pipeline/claimx/workers/
├── test_event_ingester.py          (NEW)
├── test_enrichment_worker.py       (NEW)
├── test_download_worker.py         (NEW)
├── test_upload_worker.py           (NEW)
├── test_delta_events_worker.py     (NEW)
├── test_entity_delta_worker.py     (NEW)
├── test_result_processor.py        (NEW)
└── test_download_factory.py        (NEW)
```

### ClaimX Handlers
```
tests/pipeline/claimx/handlers/
├── test_registry.py                (NEW)
├── test_project_handler.py         (NEW)
├── test_project_update_handler.py  (NEW)
├── test_task_handler.py            (NEW)
├── test_media_handler.py           (NEW)
├── test_video_handler.py           (NEW)
├── test_project_cache.py           (NEW)
└── test_transformers.py            (NEW)
```

### Verisk Workers (mirror ClaimX)
```
tests/pipeline/verisk/workers/
├── test_event_ingester.py          (NEW)
├── test_enrichment_worker.py       (NEW)
├── test_download_worker.py         (NEW)
├── test_upload_worker.py           (NEW)
├── test_delta_events_worker.py     (NEW)
├── test_result_processor.py        (NEW)
└── test_periodic_logger.py         (NEW)
```

### Plugin Workers
```
tests/pipeline/plugins/
├── test_plugin_action_worker.py              (NEW)
├── test_mitigation_tracking_worker.py        (NEW)
└── test_itel_cabinet_api_worker.py           (NEW)
```

---

## Appendix B: Estimated Effort (Updated)

### Unit Tests (No Infrastructure)

| Phase | Components | Test Files | Est. Hours | Infra? | Priority |
|-------|-----------|-----------|-----------|--------|----------|
| **Phase 1** | Enrichment & Ingestion | 5 | 80h | ❌ No | **Critical** |
| **Phase 2** | Upload, Result, Delta | 7 | 80h | ❌ No | **High** |
| **Phase 3** | Handlers | 8 | 40h | ❌ No | **High** |
| **Phase 4** | Plugins | 4 | 20h | ❌ No | **Medium** |
| **Phase 5** | Integration Tests | 5-10 | 25h | ✅ **Yes** | **Deferred** |
| **UNIT TOTAL** | **24 files** | **~100 functions** | **220h** | **No** | - |
| **INTEGRATION** | **5-10 files** | **~15 functions** | **25h** | **Yes** | - |

### Breakdown by Test Type

| Test Type | Effort | Infrastructure Required? | When to Implement |
|-----------|--------|-------------------------|-------------------|
| **Unit Tests** | 220h (90%) | ❌ None | **Now (Phases 1-4)** |
| **Integration Tests** | 25h (10%) | ✅ Docker, Kafka, Delta | **Later (Phase 5)** |
| **TOTAL** | 245h | - | - |

**Recommended Approach:**
- **Start:** Phases 1-4 (220h) - No infrastructure needed
- **Defer:** Phase 5 (25h) - Add when ready for test infrastructure

**Team Size:** 1-2 developers
**Timeline:**
- Phases 1-4: 7 weeks (unit tests only)
- Phase 5: 1 week (when infrastructure is ready)

---

## Appendix C: References

- Existing test patterns: `tests/pipeline/claimx/retry/test_enrichment_handler.py`
- Coding standards: `CLAUDE.md`
- pytest documentation: https://docs.pytest.org/
- pytest-asyncio: https://pytest-asyncio.readthedocs.io/

---

**END OF DOCUMENT**
