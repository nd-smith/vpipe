# Test Suite Guide - vpipe

> **Last Updated**: 2026-02-01 (Post-Kafka-to-EventHub Migration)
> **Total Tests**: ~1,083 tests across 48 files
> **Framework**: pytest 7.0+

## Overview

This test suite has been cleaned up after migrating from Kafka to Azure EventHub. All Kafka-specific tests have been removed. The remaining tests are either transport-agnostic or EventHub-specific.

## Test Organization

```
tests/
├── core/                    # 580 tests - Core library (transport-agnostic)
├── kafka_pipeline/
│   ├── claimx/
│   │   ├── schemas/         # 112 tests - ClaimX message schemas
│   │   └── workers/         # 49 tests - ClaimX workers
│   ├── verisk/
│   │   ├── schemas/         # 112 tests - Verisk message schemas
│   │   └── workers/         # 82 tests - Verisk workers
│   ├── common/
│   │   ├── eventhub/        # 95 tests - EventHub transport layer
│   │   ├── retry/           # Retry logic tests
│   │   ├── storage/         # Delta storage tests
│   │   └── test_types.py    # 34 tests - Message types
│   └── simulation/          # Simulation infrastructure tests
└── conftest.py              # Base test configuration
```

## Test Categories

### 1. Core Transport-Agnostic Tests (580 tests)

These tests are completely independent of the messaging transport layer.

#### **core/errors/** (189 tests)
- `test_classifiers.py` - Error classification logic
- `test_exceptions.py` - Custom exception classes
- `test_kafka_classifier.py` - Legacy Kafka error mapping (kept for backward compat)

**Purpose**: Ensure error handling and classification works correctly across the application.

#### **core/security/** (132 tests)
- `test_url_validation.py` - URL validation and sanitization
- `test_file_validation.py` - File type and content validation
- `test_presigned_urls.py` - Presigned URL generation and validation

**Purpose**: Validate security controls for external resources and file handling.

#### **core/logging/** (74 tests)
- `test_formatters.py` - JSON log formatting
- `test_message_context.py` - Contextual logging
- `test_context_managers.py` - Context manager utilities
- `test_setup.py` - Logging setup and configuration
- `test_filters.py` - Log filtering logic

**Purpose**: Ensure structured logging works correctly for observability.

#### **core/resilience/** (69 tests)
- `test_circuit_breaker.py` - Circuit breaker patterns for external services
- `test_retry.py` - Retry logic with exponential backoff

**Purpose**: Validate resilience patterns for handling transient failures.

#### **core/download/** (58 tests)
- `test_http_client.py` - HTTP client with retry and timeout
- `test_streaming.py` - Streaming download functionality
- `test_downloader.py` - High-level download orchestration

**Purpose**: Ensure file download functionality is robust and handles errors.

#### **core/auth/** (44 tests)
- `test_token_cache.py` - OAuth token caching
- `test_kafka_oauth.py` - Legacy Kafka OAuth (deprecated, kept for reference)

**Purpose**: Validate authentication and credential management.

#### **core/paths/** (14 tests)
- `test_resolver.py` - Path resolution and validation

**Purpose**: Ensure path handling is secure and cross-platform compatible.

---

### 2. Schema Validation Tests (224 tests)

These tests validate Pydantic models for message serialization/deserialization.

#### **kafka_pipeline/claimx/schemas/** (112 tests)
- `test_events.py` - ClaimX event schemas
- `test_tasks.py` - ClaimX task schemas
- `test_cached.py` - Cached entity schemas
- `test_results.py` - Result schemas

**Purpose**: Ensure ClaimX messages conform to expected schemas.

#### **kafka_pipeline/verisk/schemas/** (112 tests)
- `test_events.py` - Verisk event schemas
- `test_tasks.py` - Verisk task schemas
- `test_cached.py` - Cached entity schemas
- `test_results.py` - Result schemas

**Purpose**: Ensure Verisk messages conform to expected schemas.

**Note**: These are transport-agnostic - they only test Pydantic model validation.

---

### 3. EventHub-Specific Tests (95 tests)

These tests are specific to the Azure EventHub transport layer.

#### **kafka_pipeline/common/eventhub/** (95 tests)

- **test_checkpoint_store.py** (21 tests)
  - EventHub checkpoint management with Azure Blob Storage
  - Checkpoint persistence and retrieval
  - Error handling for checkpoint operations

- **test_consumer.py** (45 tests)
  - EventHub consumer initialization and configuration
  - Message consumption and deserialization
  - DLQ routing for failed messages
  - Error classification and handling
  - EventHubConsumerRecord adapter

- **test_producer.py** (29 tests)
  - EventHub producer initialization and configuration
  - Message batching and sending
  - Metadata handling (partition, offset, timestamp)
  - EventHubRecordMetadata adapter
  - Error handling and retries

**Purpose**: Validate EventHub-specific messaging functionality.

**Note**: These tests use mocks for Azure SDK dependencies, so they don't require actual Azure services.

---

### 4. Worker Tests (131 tests)

These tests validate worker business logic. They currently use Kafka-specific fixtures but test transport-agnostic logic.

#### **kafka_pipeline/claimx/workers/** (49 tests)
- Event ingestion workers
- Upload workers
- Result processing workers

#### **kafka_pipeline/verisk/workers/** (82 tests)
- `test_download_worker.py` - File download orchestration
- `test_download_worker_errors.py` - Error handling in download worker
- `test_upload_worker.py` - File upload to storage
- `test_result_processor.py` - Result processing logic
- `test_event_ingester.py` - Event ingestion and validation

**Purpose**: Ensure workers process messages correctly and handle errors gracefully.

**⚠ TODO**: Refactor worker tests to use transport-agnostic message abstractions instead of `aiokafka.ConsumerRecord`.

---

### 5. Common Infrastructure Tests

#### **kafka_pipeline/common/test_types.py** (34 tests)
- Message type definitions
- Serialization/deserialization
- Type validation

**Purpose**: Validate transport-agnostic message types.

#### **kafka_pipeline/common/test_transport.py** (7 tests)
- Transport factory functions
- Checkpoint store creation
- Transport type selection

**Purpose**: Validate transport abstraction layer (Kafka vs EventHub selection).

**Note**: Uses mocks, doesn't test actual Kafka/EventHub connectivity.

#### **kafka_pipeline/common/retry/**
- `test_delta_handler.py` - Delta-based retry logic
- Retry scheduling and execution

**Purpose**: Validate retry infrastructure for failed messages.

#### **kafka_pipeline/common/storage/**
- `test_delta_resource_cleanup.py` - Delta table cleanup
- Storage management tests

**Purpose**: Validate Delta Lake storage operations.

---

### 6. Simulation Tests

#### **kafka_pipeline/simulation/**
- `test_storage.py` - Mock storage for simulation mode
- `test_storage_integration.py` - Storage integration scenarios

**Purpose**: Validate simulation mode for local testing without real services.

---

## Running Tests

### Run All Tests
```bash
pytest
```

### Run Specific Category
```bash
# Core tests only
pytest tests/core/

# Schema validation only
pytest tests/kafka_pipeline/claimx/schemas/ tests/kafka_pipeline/verisk/schemas/

# EventHub tests only
pytest tests/kafka_pipeline/common/eventhub/

# Worker tests only
pytest tests/kafka_pipeline/claimx/workers/ tests/kafka_pipeline/verisk/workers/
```

### Run With Coverage
```bash
pytest --cov=src --cov-report=html
```

### Run Integration Tests (marked)
```bash
pytest -m integration
```

### Run Fast Tests Only (exclude slow/integration)
```bash
pytest -m "not slow and not integration"
```

---

## Test Markers

Defined in `pytest.ini`:

- `@pytest.mark.integration` - Integration tests requiring external services
- `@pytest.mark.performance` - Performance/load tests (long-running)
- `@pytest.mark.slow` - Slow tests (several minutes)
- `@pytest.mark.extended` - Extended tests (hours)

---

## Fixtures

### Global Fixtures (tests/conftest.py)
- Environment setup
- Base configuration

### kafka_pipeline Fixtures (tests/kafka_pipeline/conftest.py)
- `MockOneLakeClient` - Mock OneLake storage client
- `MockDeltaEventsWriter` - Mock Delta events writer
- `MockDeltaInventoryWriter` - Mock Delta inventory writer

**⚠ TODO**: Remove Kafka-specific fixtures (kafka_container, kafka_producer, kafka_consumer) from conftest.py.

---

## Coverage Gaps

### Missing EventHub Integration Tests
- [ ] Real EventHub consumer/producer interaction (not mocked)
- [ ] EventHub partitioning and load balancing
- [ ] Checkpoint management at scale with real Blob Storage
- [ ] Connection string authentication flows
- [ ] Performance benchmarks (throughput, latency)

### Missing Transport Abstraction Tests
- [ ] Transport factory switching between Kafka and EventHub
- [ ] Message adapter conversion (ConsumerRecord <-> EventData)
- [ ] Error handling parity across transports

### Missing Worker Tests
- [ ] Worker tests with EventHub-specific message types
- [ ] End-to-end worker flows with EventHub

### Missing Configuration Tests
- [ ] Transport-aware configuration loading
- [ ] Environment variable validation for EventHub
- [ ] YAML configuration validation

---

## Migration Notes

### What Was Removed
- **Kafka integration tests** (77 tests) - Used Docker testcontainers
- **Kafka performance tests** (31 tests) - Throughput and latency benchmarks
- **DLQ tests** (83 tests) - Kafka-specific dead letter queue handling
- **Kafka unit tests** (167 tests) - BaseKafkaProducer/Consumer tests
- **Collection error files** (137 tests) - Couldn't run due to missing modules

**Total Removed**: ~495 tests

### What Was Kept
- **Core transport-agnostic tests** (580 tests)
- **Schema validation tests** (224 tests)
- **EventHub-specific tests** (95 tests)
- **Worker tests** (131 tests) - Need refactoring
- **Common infrastructure tests** (~53 tests)

**Total Kept**: ~1,083 tests

### What Needs Updating
1. **Worker tests** - Replace `aiokafka.ConsumerRecord` with transport-agnostic abstraction
2. **Conftest fixtures** - Remove Kafka-specific fixtures, add EventHub equivalents
3. **Integration test markers** - Add `@pytest.mark.integration` to 13 unmarked files
4. **Configuration tests** - Update `test_pipeline_config.py` for transport-agnostic config

---

## Best Practices

### Writing New Tests
1. ✓ Use transport-agnostic abstractions (don't import `aiokafka` directly)
2. ✓ Add docstrings to test functions explaining what's being tested
3. ✓ Use descriptive test names (`test_should_retry_on_transient_error`)
4. ✓ Mark integration tests with `@pytest.mark.integration`
5. ✓ Mock external services (Azure, ClaimX API) in unit tests
6. ✓ Use fixtures from conftest.py for common setup

### Test Layers
- **Unit tests**: Test individual functions/classes in isolation (with mocks)
- **Integration tests**: Test components together (may use test doubles for external services)
- **E2E tests**: Test full workflows (may require real services)

---

## Useful Commands

```bash
# Run only failed tests from last run
pytest --lf

# Run tests matching keyword
pytest -k "download"

# Show test durations
pytest --durations=10

# Stop on first failure
pytest -x

# Run in parallel (requires pytest-xdist)
pytest -n auto

# Update snapshots (if using pytest-snapshot)
pytest --snapshot-update
```

---

## Contact

For questions about the test suite, see:
- `KAFKA_TEST_CLEANUP.md` - Details on what was removed
- `KAFKA_EVENTHUB_MIGRATION_AUDIT.md` - Full migration audit
- `EVENTHUB_QUICKSTART.md` - EventHub configuration guide
