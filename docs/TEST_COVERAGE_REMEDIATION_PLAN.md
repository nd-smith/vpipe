# Test Coverage Remediation Plan

**Date:** 2026-02-04
**Reviewer:** Claude
**Codebase:** vpipe Pipeline Workers

## Executive Summary

This document outlines missing test coverage for the vpipe pipeline architecture and provides a prioritized remediation plan. The pipeline is well-architected with solid patterns (retry/DLQ, batch processing, circuit breakers), but lacks comprehensive unit tests for core worker components.

**Current State:** ~42 test files covering schemas, retry handlers, DLQ handlers, and Delta writers
**Gap:** No unit tests for 17+ core pipeline workers and 9+ event handlers
**Risk Level:** Medium-High (production code without test coverage for complex async workflows)

---

## 1. Coverage Analysis

### 1.1 Existing Coverage (Good ✅)

| Component | Coverage | Test Files |
|-----------|----------|------------|
| **Retry Handlers** | Complete | `test_enrichment_handler.py`, `test_download_handler.py` |
| **DLQ Handlers** | Complete | `test_handler.py` (both domains) |
| **Delta Writers** | Complete | `test_delta_events.py`, `test_delta_entities.py`, `test_delta_inventory.py` |
| **Schemas** | Complete | `test_events.py`, `test_entities.py`, `test_tasks.py`, `test_results.py` |
| **API Client** | Complete | `test_api_client.py` |
| **Checkpoint Stores** | Complete | `test_checkpoint_store.py`, `test_poller_checkpoint.py` |
| **Plugin System** | Partial | `test_loader.py` |
| **Storage Utilities** | Partial | `test_delta_resource_cleanup.py` |

### 1.2 Missing Coverage (Gaps ❌)

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

## 2. Testing Strategy

### 2.1 Testing Principles (per CLAUDE.md)

- **Simple and explicit** - No complex test fixtures unless necessary
- **One test per scenario** - Descriptive test names: `test_returns_none_when_user_not_found`
- **Mock at boundaries** - Mock API clients, producers, consumers
- **Use pytest fixtures** - Shared setup in `conftest.py`
- **No unsolicited tests** - Only add when asked or for non-trivial logic

### 2.2 Worker Testing Pattern

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

### 2.3 Handler Testing Pattern

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

## 3. Remediation Plan

### Phase 1: Critical Path Workers (Weeks 1-2)

**Goal:** Test the most complex, high-risk workers first

#### Week 1: Enrichment Workers
- [ ] `test_claimx_enrichment_worker.py` - Complex handler routing, API calls, caching
- [ ] `test_verisk_enrichment_worker.py` - Plugin execution, attachment detection
- [ ] `test_event_ingester_worker.py` (ClaimX) - Event parsing, deduplication
- [ ] `test_event_ingester_worker.py` (Verisk) - EventHub integration

**Rationale:** Enrichment is the most complex stage with API integration, handler routing, caching, and multiple outputs.

#### Week 2: Download Workers
- [ ] `test_claimx_download_worker.py` - Concurrent downloads, batch processing, circuit breaker
- [ ] `test_verisk_download_worker.py` - Same patterns
- [ ] `test_download_factory.py` - Media → DownloadTask conversion

**Rationale:** Download workers have complex concurrency control (semaphores), batch processing, and circuit breaker logic.

### Phase 2: Supporting Workers (Weeks 3-4)

#### Week 3: Upload & Result Processing
- [ ] `test_claimx_upload_worker.py` - OneLake uploads, cache cleanup
- [ ] `test_verisk_upload_worker.py` - Same patterns
- [ ] `test_claimx_result_processor.py` - Metrics emission, result tracking
- [ ] `test_verisk_result_processor.py` - Same patterns

#### Week 4: Delta Writers & Logging
- [ ] `test_claimx_delta_events_worker.py` - Batch accumulation, flush triggers
- [ ] `test_claimx_entity_delta_worker.py` - Entity batching
- [ ] `test_verisk_delta_events_worker.py` - Same patterns
- [ ] `test_periodic_logger.py` (Verisk) - Health monitoring

### Phase 3: Event Handlers (Weeks 5-6)

**Goal:** Test business logic and data transformation

#### Week 5: Core Handlers
- [ ] `test_handler_registry.py` - Handler registration, event routing
- [ ] `test_project_handler.py` - Project API enrichment
- [ ] `test_task_handler.py` - Task API enrichment
- [ ] `test_media_handler.py` - Media API enrichment

#### Week 6: Specialized Handlers & Utilities
- [ ] `test_video_handler.py` - Video collaboration enrichment
- [ ] `test_project_update_handler.py` - Project update events
- [ ] `test_project_cache.py` - Cache TTL, preload, eviction
- [ ] `test_transformers.py` - Data transformation utilities

### Phase 4: Plugin Workers (Week 7)

**Goal:** Test domain-specific integrations

- [ ] `test_plugin_action_worker.py` - Generic plugin worker
- [ ] `test_mitigation_tracking_worker.py` - ClaimX mitigation tasks
- [ ] `test_itel_cabinet_api_worker.py` - iTel API worker
- [ ] `test_itel_cabinet_tracking_worker.py` - iTel tracking worker

### Phase 5: Infrastructure (Week 8)

**Goal:** Test shared utilities and transport layer

- [ ] `test_transport.py` - Consumer/producer factories
- [ ] `test_onelake_client.py` - OneLake operations
- [ ] `test_delta_operations.py` - Delta table reads/writes
- [ ] `test_health_server.py` - Health check endpoints
- [ ] `test_batch_consumer.py` - EventHub batch consumer

---

## 4. Implementation Guidelines

### 4.1 Mocking Strategy

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

### 4.2 Fixture Organization

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

### 4.3 Test Naming Convention

Follow pattern: `test_<condition>_<expected_result>`

**Examples:**
- `test_valid_message_creates_enrichment_task`
- `test_invalid_json_raises_validation_error`
- `test_permanent_error_routes_to_dlq`
- `test_circuit_open_skips_commit`
- `test_batch_processing_respects_semaphore_limit`

### 4.4 Async Testing

Use `pytest-asyncio`:
```python
@pytest.mark.asyncio
async def test_worker_starts_successfully(mock_config):
    worker = ClaimXEnrichmentWorker(config=mock_config)
    await worker.start()
    assert worker._running
    await worker.stop()
```

### 4.5 Coverage Metrics

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

## 5. Risk Assessment

### 5.1 High-Risk Areas (Test First)

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

### 5.2 Medium-Risk Areas

**Upload Workers:**
- OneLake upload retry logic
- Cache cleanup after success/failure
- Result message production

**Delta Writers:**
- Batch flush triggers (size + timeout)
- Graceful shutdown with pending batches
- Failed batch routing to retry topics

### 5.3 Low-Risk Areas (Test Later)

**Utility Classes:**
- Download factory (simple conversion)
- Transformers (data mapping)
- Metrics emission (fire-and-forget)

---

## 6. Success Criteria

### 6.1 Quantitative

- [ ] **90%** code coverage for workers
- [ ] **85%** code coverage for handlers
- [ ] **100** new test functions added
- [ ] **0** failing tests in CI/CD
- [ ] **<5 min** test suite execution time

### 6.2 Qualitative

- [ ] All critical paths have test coverage
- [ ] Error scenarios are tested (retry, DLQ, circuit breaker)
- [ ] Concurrent processing logic is validated
- [ ] Test names clearly describe scenarios
- [ ] Minimal test complexity (avoid over-mocking)

---

## 7. Maintenance Plan

### 7.1 Ongoing

- Add tests for new workers before merging
- Update tests when changing worker behavior
- Monitor test execution time (keep fast)
- Review coverage reports in PR reviews

### 7.2 Monthly

- Review and update test fixtures
- Remove flaky tests
- Refactor duplicated test code

### 7.3 Quarterly

- Audit test coverage for new modules
- Update test patterns based on lessons learned
- Review and update this plan

---

## 8. Notes & Considerations

### 8.1 Testing Philosophy Alignment

Per `CLAUDE.md`:
- "Do not add tests unless asked"
- "When working on non-trivial logic, ask if tests are wanted"

**Action:** This plan assumes approval to add comprehensive tests. Confirm with team before proceeding.

### 8.2 Test Data Strategy

- Use **fixtures** for common test data (events, tasks, configs)
- Keep test data **minimal** (only fields needed for test)
- Use **factories** for complex object graphs (if needed later)

### 8.3 Integration vs Unit Tests

This plan focuses on **unit tests**:
- Mock external dependencies (Kafka, APIs, Delta)
- Test worker logic in isolation
- Fast execution (<5 min total)

**Future:** Consider integration tests for end-to-end flows (separate suite).

### 8.4 CI/CD Integration

Ensure tests run in CI/CD pipeline:
```yaml
# .github/workflows/test.yml
- name: Run tests
  run: |
    pytest tests/pipeline/ \
      --cov=src/pipeline \
      --cov-fail-under=80 \
      --maxfail=10 \
      -v
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

## Appendix B: Estimated Effort

| Phase | Components | Test Files | Est. Hours | Priority |
|-------|-----------|-----------|-----------|----------|
| Phase 1 | Enrichment & Ingestion | 4 | 40h | Critical |
| Phase 2 | Upload, Result, Delta | 8 | 50h | High |
| Phase 3 | Handlers | 8 | 40h | High |
| Phase 4 | Plugins | 4 | 20h | Medium |
| Phase 5 | Infrastructure | 5 | 25h | Medium |
| **Total** | **29 files** | **~100 test functions** | **175h** | - |

**Team Size:** 1-2 developers
**Timeline:** 8 weeks (with code reviews and iterations)

---

## Appendix C: References

- Existing test patterns: `tests/pipeline/claimx/retry/test_enrichment_handler.py`
- Coding standards: `CLAUDE.md`
- pytest documentation: https://docs.pytest.org/
- pytest-asyncio: https://pytest-asyncio.readthedocs.io/

---

**END OF DOCUMENT**
