# Kafka-to-EventHub Migration: Full Migration Work Plan

> **Date**: 2026-01-31
> **Branch**: `claude/kafka-eventhub-migration-audit-VVwPu`
> **Scope**: Complete work plan for migrating the entire pipeline from Kafka
> to Azure EventHub. The proof-of-concept phase (2 external event source
> entities on EventHub) is complete. This document covers everything
> required to migrate the remaining 21+ internal topics and fully
> eliminate the Kafka dependency.

---

## Executive Summary

The proof of concept is done -- the two external event source entities
(`verisk_events`, `claimx_events`) are running on EventHub successfully.
Now we need to migrate the remaining **21+ internal worker-to-worker
topics** and close the gaps in the EventHub adapter layer (DLQ routing,
checkpoint persistence, error classification) to reach full functional
parity. Once complete, the `aiokafka` dependency and all Kafka
infrastructure can be removed.

Work falls into 7 categories:

### Work Items at a Glance

| # | Category | Severity | Files Affected | Effort |
|---|----------|----------|----------------|--------|
| 1 | EventHub DLQ routing (not yet implemented) | **Critical** | 1 core + all consumers | Medium |
| 2 | Migrate 21+ internal topics to EventHub | **Critical** | All workers, runners, config | Large |
| 3 | Decouple `aiokafka` types from pipeline | High | 62 files | Medium |
| 4 | Rename Kafka-named modules & classes | Medium | 12 source files | Medium |
| 5 | Add EventHub error classification | High | 2 core files | Medium |
| 6 | Update observability to drop Kafka naming | Medium | 8 config/dashboard files | Small |
| 7 | Remove Kafka dependencies & infrastructure | Medium | 6 config/docker files | Small |

---

## 1. CRITICAL: EventHub Consumer Has No DLQ Routing

**Impact**: Permanent processing errors are logged but silently dropped.
Messages that should be dead-lettered are lost, with no audit trail or
manual recovery path.

### Current State

The Kafka consumer (`src/kafka_pipeline/common/consumer.py:560-641`) has a
full `_send_to_dlq()` implementation:
- Lazy-initializes a dedicated `AIOKafkaProducer` for DLQ writes
- Constructs structured DLQ messages with full original context
- Records `dlq_permanent` / `dlq_transient` metrics
- Commits offsets after DLQ routing to advance past poison pills

The EventHub consumer (`src/kafka_pipeline/common/eventhub/consumer.py:395-420`)
has a **stub**:

```python
if error_category == ErrorCategory.PERMANENT:
    log_exception(...)
    # DLQ routing would go here if needed
    # For now, just log and continue
```

### What Needs to Be Built

| Item | Details |
|------|---------|
| DLQ producer for EventHub | An `EventHubProducer` instance targeting a DLQ entity per domain |
| `_send_to_dlq()` method | Mirror the Kafka implementation's structured message format |
| DLQ metrics recording | `record_dlq_permanent()` / `record_dlq_transient()` calls |
| Offset handling after DLQ | Ensure checkpoint advances after DLQ routing |
| DLQ EventHub entities | Create `xact-dlq` and `claimx-dlq` (plus per-stage DLQs) entities in Azure |

### Files to Modify

- `src/kafka_pipeline/common/eventhub/consumer.py` -- implement `_send_to_dlq()` and `_ensure_dlq_producer()`

---

## 2. CRITICAL: Migrate All 21+ Internal Topics to EventHub

The proof-of-concept phase established EventHub for the 2 external event
source entities. The remaining 21+ internal worker-to-worker topics
(enrichment, downloads, upload, retry, DLQ, plugin topics) are the core
of this migration.

### Current wiring

`src/config/config.yaml:236-238` documents the POC boundary:
```yaml
# Currently Event Hub is only used as the external event source.
# Internal pipeline routing (downloads, enrichment, DLQ) uses local Kafka.
# Add more entries here if/when those stages migrate to Event Hub.
```

The runner registry (`src/kafka_pipeline/runners/registry.py`) hardcodes
`local_kafka_config` for every internal worker, never passing `topic_key`
to the transport factory. This is the main wiring that needs to change.

### Topics Requiring EventHub Entity Definitions

**XACT domain (6 entities needed):**

| Kafka Topic | Proposed EventHub Entity |
|-------------|-------------------------|
| `xact.enrichment.pending` | `xact-enrichment-pending` |
| `xact.downloads.pending` | `xact-downloads-pending` |
| `xact.downloads.cached` | `xact-downloads-cached` |
| `xact.downloads.results` | `xact-downloads-results` |
| `xact.downloads.dlq` | `xact-dlq` |
| `xact.retry` | `xact-retry` |

**ClaimX domain (8 entities needed):**

| Kafka Topic | Proposed EventHub Entity |
|-------------|-------------------------|
| `claimx.enrichment.pending` | `claimx-enrichment-pending` |
| `claimx.enrichment.dlq` | `claimx-enrichment-dlq` |
| `claimx.entities.rows` | `claimx-entities-rows` |
| `claimx.downloads.pending` | `claimx-downloads-pending` |
| `claimx.downloads.cached` | `claimx-downloads-cached` |
| `claimx.downloads.results` | `claimx-downloads-results` |
| `claimx.downloads.dlq` | `claimx-dlq` |
| `claimx.retry` | `claimx-retry` |

**Plugin topics (7 entities needed):**

| Kafka Topic | Proposed EventHub Entity |
|-------------|-------------------------|
| `itel.cabinet.task.tracking` | `itel-cabinet-tracking` |
| `itel.cabinet.completed` | `itel-cabinet-completed` |
| `itel.cabinet.task.tracking.success` | `itel-cabinet-success` |
| `itel.cabinet.task.tracking.errors` | `itel-cabinet-errors` |
| `itel.cabinet.api.success` | `itel-api-success` |
| `itel.cabinet.api.errors` | `itel-api-errors` |
| `claimx.mitigation.task.tracking` | `claimx-mitigation-tracking` |

**Total: 21+ new EventHub entities in the Azure namespace.**

### Architectural Constraint

EventHub consumers can only subscribe to **a single entity** per client
(enforced at `src/kafka_pipeline/common/transport.py:354-358`). Workers
that currently subscribe to multiple Kafka topics will need one consumer
per entity, or the code restructured so each worker handles a single topic.

### Files to Modify

- `src/config/config.yaml` -- add all EventHub entity definitions under `eventhub.xact.*` and `eventhub.claimx.*`
- `src/kafka_pipeline/runners/registry.py` -- pass `topic_key` to `create_producer()` / `create_consumer()` instead of raw `local_kafka_config`
- `src/config/pipeline_config.py` -- `LocalKafkaConfig` class needs refactoring or replacement; its name and fields are Kafka-specific
- Every worker runner that currently accepts `local_kafka_config`

---

## 3. HIGH: `aiokafka` Types Embedded Across 62 Files

**Impact**: Even when using EventHub transport, the entire pipeline
depends on `aiokafka` at import time. The EventHub adapter imports
`aiokafka` types and creates shim classes to mimic them. This makes
`aiokafka` a hard runtime dependency regardless of transport choice.

### The Coupling

| aiokafka Type | Usage Count | Where |
|---------------|-------------|-------|
| `ConsumerRecord` | 28 source files | Every worker's `message_handler` signature |
| `AIOKafkaProducer` | ~20 files | Consumer/producer init, DLQ routing |
| `AIOKafkaConsumer` | ~15 files | Consumer init, integration tests |
| `RecordMetadata` | 3 files | Producer return types |
| `TopicPartition` | 6 files | Partition metrics, enrichment workers |
| `KafkaError` | 4 files | Plugin workers error handling |

### EventHub Adapter Imports aiokafka

`src/kafka_pipeline/common/eventhub/consumer.py:23-24`:
```python
from aiokafka import AIOKafkaProducer          # for DLQ routing
from aiokafka.structs import ConsumerRecord     # for type compatibility
```

`src/kafka_pipeline/common/eventhub/producer.py:19`:
```python
from aiokafka.structs import RecordMetadata     # for return type
```

The adapter creates `EventHubConsumerRecord` and `EventHubRecordMetadata`
shim classes but the rest of the pipeline still type-checks against the
aiokafka originals.

### Recommended Approach

1. Define **transport-agnostic message types** in a new module (e.g., `src/kafka_pipeline/common/types.py`):
   - `PipelineMessage` (replaces `ConsumerRecord`)
   - `ProduceResult` (replaces `RecordMetadata`)
   - `PartitionInfo` (replaces `TopicPartition`)
2. Have both Kafka and EventHub adapters return these types
3. Update all 28+ worker `message_handler` signatures
4. Remove direct `aiokafka` imports from all non-Kafka files

### Files to Modify

**Core (define new types):**
- `src/kafka_pipeline/common/types.py` (new)

**Adapters (return new types):**
- `src/kafka_pipeline/common/consumer.py`
- `src/kafka_pipeline/common/producer.py`
- `src/kafka_pipeline/common/eventhub/consumer.py`
- `src/kafka_pipeline/common/eventhub/producer.py`

**Workers (update signatures -- 28 files):**
- `src/kafka_pipeline/xact/workers/*.py` (6 files)
- `src/kafka_pipeline/claimx/workers/*.py` (7 files)
- `src/kafka_pipeline/xact/dlq/*.py` (2 files)
- `src/kafka_pipeline/claimx/dlq/*.py` (1 file)
- `src/kafka_pipeline/common/retry/unified_scheduler.py`
- `src/kafka_pipeline/plugins/**/*.py` (4 files)

**Tests (update mocks -- 12+ files):**
- `tests/kafka_pipeline/xact/workers/test_*.py`
- `tests/kafka_pipeline/claimx/workers/test_*.py`
- `tests/kafka_pipeline/common/test_consumer.py`
- `tests/kafka_pipeline/common/test_producer.py`
- `tests/kafka_pipeline/integration/*.py`

---

## 4. MEDIUM: Kafka-Named Modules, Classes, and Variables

**Impact**: Naming confusion for developers. The package is called
`kafka_pipeline`, classes are `BaseKafkaProducer`/`BaseKafkaConsumer`,
config is `KafkaConfig`, context is `KafkaLogContext`, etc. -- even
when the transport is EventHub.

### Inventory of Kafka-Named Artifacts

| Current Name | File | Proposed Name | Status |
|-------------|------|---------------|--------|
| `kafka_pipeline/` (package) | top-level package | `pipeline/` | Pending |
| `BaseKafkaProducer` | `common/producer.py:26` | `BaseProducer` | Pending |
| `BaseKafkaConsumer` | `common/consumer.py:31` | `BaseConsumer` | Pending |
| `KafkaConfig` | `config/config.py` | `PipelineTransportConfig` | Pending |
| `KafkaLogContext` | `core/logging/message_context.py:77` | `MessageLogContext` | ✓ Complete |
| `set_kafka_context()` | `core/logging/message_context.py:14` | `set_message_context()` | ✓ Complete |
| `get_kafka_context()` | `core/logging/message_context.py:43` | `get_message_context()` | ✓ Complete |
| `clear_kafka_context()` | `core/logging/message_context.py:68` | `clear_message_context()` | ✓ Complete |
| `_kafka_topic` (ContextVar) | `core/logging/message_context.py:7` | `_message_topic` | ✓ Complete |
| `kafka_classifier.py` | `core/errors/kafka_classifier.py` | `transport_classifier.py` | Pending |
| `KafkaErrorClassifier` | `core/errors/kafka_classifier.py:81` | `TransportErrorClassifier` | Pending |
| `kafka_oauth.py` | `core/auth/kafka_oauth.py` | `eventhub_oauth.py` | Pending |
| `KafkaOAuthError` | `core/auth/kafka_oauth.py:49` | `TransportOAuthError` | Pending |
| `LocalKafkaConfig` | `config/pipeline_config.py:159` | `InternalTransportConfig` | Pending |

### Files to Rename

- `src/core/errors/kafka_classifier.py` -> `transport_classifier.py` (Pending)
- ~~`src/core/logging/kafka_context.py` -> `message_context.py`~~ ✓ Complete
- `src/core/auth/kafka_oauth.py` -> `eventhub_oauth.py` (Pending - this actually targets EventHub already)
- `tests/core/errors/test_kafka_classifier.py` -> `test_transport_classifier.py` (Pending)
- ~~`tests/core/logging/test_kafka_context.py` -> `test_message_context.py`~~ ✓ Complete
- `tests/core/auth/test_kafka_oauth.py` -> `test_eventhub_oauth.py` (Pending)

---

## 5. HIGH: Error Classification Only Handles Kafka Exception Types

**Impact**: When running on EventHub, the `azure-eventhub` SDK raises
its own exception types (e.g., `EventHubError`, `AuthenticationError`,
`ConnectError`, `EventDataSendError`). The current classifier only knows
`aiokafka` exception names and falls through to generic classification
for EventHub errors.

### Current State

`src/core/errors/kafka_classifier.py:22-62` defines `KAFKA_ERROR_MAPPINGS`:
```python
KAFKA_ERROR_MAPPINGS = {
    "transient": [
        "BrokerNotAvailableError",
        "KafkaConnectionError",
        "NodeNotReadyError",
        ...
    ],
    "auth": [
        "TopicAuthorizationFailedError",
        "SaslAuthenticationError",
        ...
    ],
    "permanent": [
        "UnknownTopicOrPartitionError",
        "MessageSizeTooLargeError",
        ...
    ],
    "throttling": [
        "KafkaThrottlingError",
        ...
    ],
}
```

**No `azure-eventhub` exception types are mapped.** When EventHub raises
`EventHubError`, the classifier falls through to string-based matching
(lines 174-205), which may misclassify errors.

### What Needs to Be Added

```python
EVENTHUB_ERROR_MAPPINGS = {
    "transient": [
        "EventHubError",           # General transient
        "ConnectionLostError",     # AMQP connection lost
        "ConnectError",            # Connection failed
        "OperationTimeoutError",   # Request timeout
        "AMQPConnectionError",     # AMQP-level connection issue
    ],
    "auth": [
        "AuthenticationError",     # SAS/AAD auth failure
        "ClientAuthenticationError",
    ],
    "permanent": [
        "EventDataSendError",      # Message too large / invalid
        "EventDataError",          # Malformed event data
        "SchemaError",             # Schema validation failure
    ],
    "throttling": [
        "ServerBusyError",         # Event Hub throttling (HTTP 503)
    ],
}
```

### Files to Modify

- `src/core/errors/kafka_classifier.py` -- add `EVENTHUB_ERROR_MAPPINGS` and update `classify_consumer_error()` / `classify_producer_error()` to handle both
- The EventHub consumer at `src/kafka_pipeline/common/eventhub/consumer.py:376` already calls `KafkaErrorClassifier` -- the classifier just needs the EventHub error types

---

## 6. MEDIUM: Observability Stack References Kafka

**Impact**: Dashboard names, alert rule names, metric label values, and
Prometheus scrape configs all reference "Kafka". This creates confusion
in monitoring when the transport is actually EventHub.

### Dashboard Files

| File | Issue |
|------|-------|
| `observability/grafana/dashboards/kafka-pipeline-overview.json` | Dashboard titled "Kafka Pipeline Overview" |
| `observability/grafana/dashboards/consumer-health.json` | References Kafka consumer groups |

### Alert Rules

`observability/prometheus/alerts/kafka-pipeline.yml` defines alert groups with Kafka naming:
- `KafkaConsumerLagHigh`
- `KafkaConsumerLagWarning`
- `KafkaConsumerNoPartitions`
- `KafkaDLQGrowthRapid`
- `KafkaErrorRateHigh`
- `KafkaDownloadFailureRateHigh`
- `KafkaProducerErrorsHigh`
- etc.

### Prometheus Config

`observability/prometheus/prometheus.yml` -- scrape job labels reference Kafka workers.

### Metric Names in Code

`src/kafka_pipeline/common/metrics.py` -- Prometheus metric names should be audited
for any `kafka_` prefixes that should become transport-agnostic.

### Logging Context

`src/core/logging/message_context.py` emits structured log fields prefixed with
`message_` (e.g., `message_topic`, `message_partition`, `message_offset`,
`message_consumer_group`). These appear in all structured JSON logs regardless
of which transport is active. ✓ Updated to transport-agnostic naming.

---

## 7. MEDIUM: Dependency and Infrastructure Cleanup

### Python Dependency

`src/requirements.txt:22-24`:
```
# Kafka
aiokafka[lz4]>=0.11.0,<1.0.0
gssapi>=1.8.0,<2.0.0  # Kerberos (GSSAPI) SASL auth
```

Once Kafka is fully removed:
- `aiokafka` can be dropped (saves ~20MB including librdkafka)
- `gssapi` can be dropped (Kerberos is Kafka-specific auth)
- `lz4` transitive dependency can be dropped

### Docker Compose Files

| File | Action |
|------|--------|
| `scripts/docker/docker-compose.kafka.yml` | Entire file can be removed |
| `scripts/docker/docker-compose.yml` | Remove `--profile kafka` block, Kafka service definitions |
| `scripts/docker/docker-compose.simulation.yml` | Replace local Kafka with EventHub or dummy transport |

### Configuration Files

| File | Action |
|------|--------|
| `src/config/config.yaml` (lines 23-97) | Remove entire `kafka:` and `local_kafka:` sections |
| `.env.example` (lines ~140-170) | Remove `KAFKA_*` environment variables |
| `config/simulation.yaml` | Replace Kafka bootstrap config with EventHub config |

### Build Scripts

| File | Action |
|------|--------|
| `scripts/build_vendor_gssapi.sh` | Can be removed (Kerberos is Kafka-specific) |
| `scripts/start_simulation.sh` | Remove Kafka health check wait logic |

### Procfile

`Procfile` references `kafka_pipeline` module -- rename if the package is renamed.

### pyproject.toml

- Line 69: `aiokafka.*` mypy override can be removed
- Line 93: Integration test marker description references "Docker and Kafka testcontainers"

---

## Additional Considerations

### EventHub Checkpoint Store

The EventHub consumer (`eventhub/consumer.py:198-213`) has a `commit()` method
that is effectively a no-op -- it logs but does not persist checkpoints to
durable storage. The actual checkpointing happens per-event via
`partition_context.update_checkpoint(event)` at line 243, which uses the
Azure SDK's **in-memory default store**.

**Risk**: If the consumer restarts, it loses its position and replays from
`starting_position="-1"` (beginning of stream).

**Fix needed**: Configure `BlobCheckpointStore` from `azure.eventhub.extensions.checkpointstoreblobasync`
to persist checkpoints to Azure Blob Storage. This requires:
- Adding `azure-eventhub-checkpointstoreblob-aio` to `requirements.txt`
- A storage account + container for checkpoint data
- Passing the checkpoint store to `EventHubConsumerClient`

### EventHub Single-Topic Constraint

EventHub consumers bind to a single entity. The current Kafka consumer
can subscribe to multiple topics (used by some workers). The transport
factory already raises `ValueError` for multi-topic EventHub consumers
(`transport.py:354-358`), but some workers may need restructuring.

### Retry Scheduler Depends on Kafka

The unified retry scheduler (`common/retry/unified_scheduler.py`) directly
uses `BaseKafkaConsumer` and `BaseKafkaProducer`. It needs to be updated
to use the transport factory (`create_consumer()` / `create_producer()`).

### Schema Registry

The Kafka config references a Schema Registry
(`schema_registry_url: https://rtvdevro-kafka.tptigslb.allstate.com:8081`).
If any workers use Avro schema validation via the registry, that dependency
needs an EventHub-compatible replacement (Azure Schema Registry).

### Plugin Workers Use Raw aiokafka

Plugin workers in `src/kafka_pipeline/plugins/` directly import
`AIOKafkaProducer`, `AIOKafkaConsumer`, and `KafkaError` from `aiokafka`
rather than going through the transport factory. These need to be migrated
to use `create_producer()` / `create_consumer()`.

**Affected plugins:**
- `plugins/shared/workers/plugin_action_worker.py`
- `plugins/itel_cabinet_api/itel_cabinet_api_worker.py`
- `plugins/itel_cabinet_api/itel_cabinet_tracking_worker.py`
- `plugins/claimx_mitigation_task/mitigation_tracking_worker.py`

---

## Recommended Migration Order

1. **Define transport-agnostic message types** (Item 3) -- unblocks everything else
2. **Implement EventHub DLQ routing** (Item 1) -- critical for production safety
3. **Add EventHub error classification** (Item 5) -- needed for correct DLQ decisions
4. **Add BlobCheckpointStore** (Additional) -- needed for reliable offset tracking
5. **Migrate internal topics to EventHub** (Item 2) -- largest effort, do domain-by-domain
6. **Update plugin workers** (Additional) -- use transport factory instead of raw aiokafka
7. **Rename Kafka artifacts** (Item 4) -- cosmetic, do alongside other changes
8. **Update observability** (Item 6) -- rename dashboards and alerts
9. **Remove Kafka dependencies** (Item 7) -- final cleanup after all code is migrated
