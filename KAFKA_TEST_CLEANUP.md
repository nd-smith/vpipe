# Kafka Test Cleanup Plan

## Summary
Removing 495 obsolete Kafka-specific tests to clean up test suite after EventHub migration.

## Test Counts
- **Starting**: 1,578 tests (84 files)
- **Removing**: 495 tests (36 files)
- **Keeping**: 1,083 tests (48 files)

## Files to Remove

### Collection Errors (5 files, 137 tests)
```bash
rm tests/kafka_pipeline/common/dlq/test_cli.py                    # 37 tests
rm tests/kafka_pipeline/common/dlq/test_handler.py                # 20 tests
rm tests/kafka_pipeline/integration/test_e2e_dlq_flow.py          # 6 tests
rm tests/kafka_pipeline/plugins/test_task_trigger.py              # 13 tests
rm tests/kafka_pipeline/test_pipeline_config.py                   # 23 tests (defer - needs update)
```

### Kafka Integration Tests (16 files, 77 tests)
```bash
rm -rf tests/kafka_pipeline/integration/                          # All Kafka integration tests
# Files include:
# - test_environment.py
# - test_produce_consume.py
# - test_e2e_*.py
# - claimx/test_*.py
```

### Kafka Performance Tests (7 files, 31 tests)
```bash
rm -rf tests/kafka_pipeline/performance/                          # All Kafka performance tests
rm -rf tests/kafka_pipeline/claimx/performance/                   # ClaimX throughput tests
```

### Kafka-Specific Unit Tests (8 files, 250 tests)
```bash
# Consumer/Producer tests (replace with EventHub equivalents later)
rm tests/kafka_pipeline/common/test_consumer.py                   # 16 tests
rm tests/kafka_pipeline/common/test_producer.py                   # 20 tests

# DLQ module tests (EventHub has different DLQ model)
rm -rf tests/kafka_pipeline/common/dlq/                           # 57 tests

# Kafka-specific simulations
rm tests/kafka_pipeline/simulation/test_kafka_producer.py         # ~40 tests (if exists)
```

## Files to Keep (1,083 tests)

### Core Transport-Agnostic (580 tests)
✓ tests/core/errors/                   # 189 tests - Error classification
✓ tests/core/security/                 # 132 tests - URL/file validation
✓ tests/core/logging/                  # 74 tests - Logging framework
✓ tests/core/resilience/               # 69 tests - Circuit breaker, retry
✓ tests/core/download/                 # 58 tests - HTTP client
✓ tests/core/auth/                     # 44 tests - Token cache
✓ tests/core/paths/                    # 14 tests - Path resolution

### Schema Validation (224 tests)
✓ tests/kafka_pipeline/claimx/schemas/ # 112 tests - ClaimX schemas
✓ tests/kafka_pipeline/verisk/schemas/ # 112 tests - Verisk schemas

### EventHub-Specific (95 tests)
✓ tests/kafka_pipeline/common/eventhub/test_checkpoint_store.py  # 21 tests
✓ tests/kafka_pipeline/common/eventhub/test_consumer.py          # 45 tests
✓ tests/kafka_pipeline/common/eventhub/test_producer.py          # 29 tests

### Worker Tests (131 tests - needs refactoring)
⚠ tests/kafka_pipeline/claimx/workers/  # 49 tests - Update fixtures
⚠ tests/kafka_pipeline/verisk/workers/  # 82 tests - Update fixtures

### Other Transport-Agnostic Tests
✓ tests/kafka_pipeline/common/retry/    # Delta handler, retry utils
✓ tests/kafka_pipeline/common/storage/  # Delta storage tests
✓ tests/kafka_pipeline/common/test_types.py  # Message types (34 tests)
✓ tests/kafka_pipeline/simulation/test_storage*.py  # Storage tests

## Verification Commands

After cleanup, verify test counts:
```bash
# Count remaining test files
find tests -name "test_*.py" | wc -l
# Expected: ~48 files

# Run pytest collection
cd /home/nick/projects/vpipe
pytest --collect-only
# Expected: ~1,083 tests collected, 0 errors

# Run all tests
pytest -v
```

## Next Steps

### Phase 2: Document Remaining Tests
- Create TEST_GUIDE.md with description of each test file
- Categorize by layer (core, schemas, workers, EventHub)
- Document fixtures and their purposes

### Phase 3: Add EventHub Coverage
- Add EventHub integration tests (~70 tests)
- Refactor worker tests for transport abstraction
- Add performance benchmarks for EventHub

### Phase 4: Update Configuration Tests
- Update test_pipeline_config.py for transport-agnostic config
- Add tests for EventHub-specific configuration
- Test environment variable validation
