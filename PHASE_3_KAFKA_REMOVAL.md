# Phase 3: Complete Kafka Removal

**Date:** 2026-02-04
**Impact:** BREAKING - Kafka completely removed from pipeline

## Summary

Removed all Kafka-specific configuration and code after migration to EventHub for all pipeline communication.

**Lines Removed:** -560 lines total (-28.6% reduction)
- config.py: 855 → 605 lines (-250, -29%)
- pipeline_config.py: 999 → 690 lines (-309, -31%)
- __init__.py: 107 → 106 lines (-1)

---

## What Was Removed

###  1. Kafka Connection Settings (config.py)

**Removed fields from `KafkaConfig`:**
```python
# ❌ REMOVED - No longer needed
bootstrap_servers: str
security_protocol: str
sasl_mechanism: str
sasl_plain_username: str
sasl_plain_password: str
sasl_kerberos_service_name: str
schema_registry_url: str
request_timeout_ms: int
metadata_max_age_ms: int
connections_max_idle_ms: int
consumer_defaults: dict[str, Any]
producer_defaults: dict[str, Any]
```

**What remains in `KafkaConfig`:**
```python
# ✅ KEPT - Still needed for domain/storage/API config
verisk: VeriskDomainConfig  # EventHub entity names, retry config
claimx: ClaimXDomainConfig  # EventHub entity names, retry config
onelake_base_path: str
onelake_domain_paths: dict[str, str]
cache_dir: str
claimx_api_url: str
claimx_api_token: str
claimx_api_timeout_seconds: int
claimx_api_concurrency: int
```

### 2. LocalKafkaConfig Class (pipeline_config.py)

**Removed entire class:** ~278 lines deleted

This class configured internal Kafka for pipeline communication between workers. Now replaced with EventHub.

**Removed:**
- Kafka broker connection settings
- Kafka topic names (replaced with EventHub entity names)
- Consumer group configuration
- Kafka-specific transport settings
- `to_kafka_config()` conversion method
- `load_config()` class method with complex env var resolution

### 3. Kafka Validation Logic (config.py)

**Removed validation methods:**
- `_validate_consumer_settings()` - Kafka consumer validation
- `_validate_producer_settings()` - Kafka producer validation
- `_validate_kafka_settings()` consumer/producer branches
- `bootstrap_servers` required check
- `schema_registry_url` URL validation

**Kept:**
- `_validate_processing_settings()` - Still needed for worker processing config
- `_validate_directories()` - Cache directory validation
- `_validate_urls()` - ClaimX API URL validation

### 4. Configuration Loading (config.py)

**Removed from `load_config()`:**
```python
# ❌ REMOVED
connection = kafka_config.get("connection", {})
consumer_defaults = kafka_config.get("consumer_defaults", {})
producer_defaults = kafka_config.get("producer_defaults", {})

# All Kafka connection parameters in KafkaConfig() instantiation
```

**Changed YAML section name:**
- Was: `kafka:` section
- Now: `pipeline:` section

### 5. PipelineConfig Integration

**Removed from `PipelineConfig`:**
```python
# ❌ REMOVED
local_kafka: LocalKafkaConfig = field(default_factory=LocalKafkaConfig)
```

**Removed from `PipelineConfig.load_config()`:**
```python
# ❌ REMOVED
local_kafka = LocalKafkaConfig.load_config(resolved_path)
```

**Updated docstring:**
- Was: "Combines event source (Event Hub or Eventhouse) and Local Kafka configurations"
- Now: "Combines event source (EventHub or Eventhouse) with EventHub transport. Kafka has been removed."

### 6. Module Exports (__init__.py)

**Removed export:**
```python
# ❌ REMOVED from config/__init__.py
from config.pipeline_config import LocalKafkaConfig
```

---

## What Remains

### KafkaConfig Structure (Name Retained for Backward Compatibility)

Despite the name, `KafkaConfig` no longer contains Kafka-specific settings. It now holds:

1. **Domain Configuration** (verisk, claimx)
   - Topics (now EventHub entity names)
   - Worker configurations
   - Retry delays and max retries
   - Consumer group prefixes

2. **Storage Configuration**
   - OneLake paths (base and domain-specific)
   - Cache directory

3. **ClaimX API Configuration**
   - API URL, token, timeout, concurrency

4. **Helper Methods**
   - `_get_domain_config()` - Domain resolution with "xact"/"verisk" aliasing
   - `get_worker_config()` - Get worker component configuration
   - `get_topic()` - Get topic/entity name for domain
   - `get_consumer_group()` - Get consumer group name
   - `get_retry_topic()` - Get retry topic name
   - `get_retry_delays()` - Get retry configuration
   - `get_storage_config()` - Get storage settings

**Why keep the name `KafkaConfig`?**
- Used throughout the codebase (hundreds of imports)
- Renaming would require massive refactor across all workers
- Name is just a class identifier - functionality is what matters
- Docstring updated to clarify it's now EventHub-based

---

## Migration Guide

### 1. Update config.yaml

**Before:**
```yaml
kafka:
  connection:
    bootstrap_servers: localhost:9092
    security_protocol: PLAINTEXT
    sasl_mechanism: GSSAPI
  consumer_defaults:
    auto_offset_reset: earliest
  producer_defaults:
    acks: "1"
  verisk:
    topics:
      events: "com.allstate.pcesdopodappv1.verisk.events.raw"
  storage:
    onelake_base_path: "abfss://..."
```

**After:**
```yaml
pipeline:
  verisk:
    topics:
      events: "com.allstate.pcesdopodappv1.verisk.events.raw"
  storage:
    onelake_base_path: "abfss://..."
```

### 2. Remove Kafka Environment Variables

**Remove these:**
```bash
# ❌ REMOVE - Kafka connection
KAFKA_BOOTSTRAP_SERVERS
KAFKA_SECURITY_PROTOCOL
KAFKA_SASL_MECHANISM
KAFKA_SASL_KERBEROS_SERVICE_NAME
KAFKA_SCHEMA_REGISTRY_URL

# ❌ REMOVE - Local Kafka
LOCAL_KAFKA_BOOTSTRAP_SERVERS
LOCAL_KAFKA_SECURITY_PROTOCOL
LOCAL_KAFKA_SASL_MECHANISM
LOCAL_KAFKA_SASL_USERNAME
LOCAL_KAFKA_SASL_PASSWORD

# ❌ REMOVE - Kafka topics (replaced with EventHub entity names)
KAFKA_EVENTS_TOPIC
KAFKA_DOWNLOADS_PENDING_TOPIC
KAFKA_DOWNLOADS_CACHED_TOPIC
KAFKA_DOWNLOADS_RESULTS_TOPIC
KAFKA_DLQ_TOPIC
```

**Keep these (renamed in YAML but concepts remain):**
```bash
# ✅ KEEP - Now EventHub configuration
EVENTHUB_NAMESPACE_CONNECTION_STRING
EVENTHUB_VERISK_* # EventHub entity names
EVENTHUB_CLAIMX_* # EventHub entity names

# ✅ KEEP - Domain configuration
PIPELINE_DOMAIN
RETRY_DELAYS
MAX_RETRIES

# ✅ KEEP - Storage
ONELAKE_BASE_PATH
ONELAKE_VERISK_PATH
ONELAKE_CLAIMX_PATH
CACHE_DIR

# ✅ KEEP - Delta tables
VERISK_EVENTS_TABLE_PATH
CLAIMX_*_TABLE_PATH

# ✅ KEEP - APIs
CLAIMX_API_URL
CLAIMX_API_TOKEN
```

### 3. Update Code References

**Worker Initialization:**
```python
# Before (using LocalKafkaConfig)
local_kafka = LocalKafkaConfig.load_config()
kafka_config = local_kafka.to_kafka_config()

# After (use KafkaConfig directly)
from config import get_config
kafka_config = get_config()
```

**Pipeline Configuration:**
```python
# Before
pipeline_config = get_pipeline_config()
kafka_config = pipeline_config.local_kafka.to_kafka_config()

# After
pipeline_config = get_pipeline_config()
kafka_config = get_config()  # Separate, simpler
```

**Accessing Topics (EventHub entities):**
```python
# Still works the same!
events_topic = kafka_config.get_topic("verisk", "events")
retry_topic = kafka_config.get_retry_topic("verisk")
```

### 4. EventHub Transport

Internal pipeline communication now uses **EventHub** instead of Kafka:

- **Event Source** → EventHub entities → **Workers** → EventHub entities → **Storage**
- EventHub entity names defined in `pipeline.verisk.topics` and `pipeline.claimx.topics`
- EventHub connection configured via `EVENTHUB_NAMESPACE_CONNECTION_STRING`
- Per-entity configuration in YAML or env vars (see .env.example)

---

## Benefits

1. **28.6% Code Reduction** - 560 fewer lines to maintain
2. **Simplified Architecture** - One transport layer (EventHub) instead of two
3. **Fewer Dependencies** - No Kafka client libraries needed
4. **Clearer Configuration** - `pipeline:` section instead of confusing `kafka:` section
5. **Less Complexity** - No Kafka broker management, consumer group coordination
6. **Unified Transport** - EventHub for both event sourcing and internal communication
7. **Better Azure Integration** - Native Azure service for all messaging

---

## Breaking Changes Checklist

- [ ] Update `config/config.yaml` to use `pipeline:` section instead of `kafka:`
- [ ] Remove all Kafka-specific environment variables
- [ ] Remove Kafka broker infrastructure (if deployed)
- [ ] Update EventHub entities for internal pipeline communication
- [ ] Update worker code to use `get_config()` instead of `LocalKafkaConfig.load_config()`
- [ ] Remove Kafka client library dependencies from requirements
- [ ] Update deployment scripts to configure EventHub instead of Kafka
- [ ] Update monitoring/dashboards (Kafka metrics → EventHub metrics)
- [ ] Update documentation references to "Kafka pipeline" → "EventHub pipeline"
- [ ] Test all workers with EventHub transport

---

## Timeline Summary

| Phase | Changes | Lines Removed | Impact |
|-------|---------|---------------|--------|
| **Phase 1** | Remove redundant docstrings | -70 | Low |
| **Phase 2** | Add config helpers, simplify logic | -10 | Low |
| **Phase 2.5** | Break backward compat, standardize env vars | -53 | **BREAKING** |
| **Phase 3** | **Remove Kafka entirely** | **-560** | **BREAKING** |
| **Total** | **All phases combined** | **-693** | **BREAKING** |

---

## Files Modified

1. `/home/nick/projects/vpipe/src/config/config.py`
   - Removed Kafka connection fields from `KafkaConfig`
   - Removed consumer/producer defaults
   - Removed Kafka-specific validation
   - Changed YAML section from `kafka:` to `pipeline:`
   - Simplified `load_config()` logic

2. `/home/nick/projects/vpipe/src/config/pipeline_config.py`
   - **Deleted entire `LocalKafkaConfig` class** (~278 lines)
   - Removed `local_kafka` field from `PipelineConfig`
   - Updated docstrings and comments

3. `/home/nick/projects/vpipe/src/config/__init__.py`
   - Removed `LocalKafkaConfig` from exports

---

## Next Steps

1. **Update config.yaml.example** to show new `pipeline:` structure
2. **Update .env.example** to remove Kafka env vars, keep EventHub vars
3. **Update BREAKING_CHANGES_CONFIG.md** with Phase 3 changes
4. **Update worker code** to stop using `LocalKafkaConfig`
5. **Remove Kafka from docker-compose** if present
6. **Update README** to reflect EventHub-only architecture
7. **Remove kafka-python** or similar from requirements.txt
8. **Update deployment automation** (Jenkins, etc.)

---

## Verification

```bash
# Test configuration loads
PYTHONPATH=/home/nick/projects/vpipe/src python3 -c "from config import get_config; print('✓ Config loads')"

# Verify no Kafka references in config modules
grep -r "bootstrap_servers\|consumer_defaults\|producer_defaults" src/config/*.py
# Should return no results in KafkaConfig fields
```

---

## Questions?

**Q: Why is the class still named `KafkaConfig`?**
A: Renaming it would require changes across hundreds of files. The name is just an identifier - the functionality has been updated for EventHub.

**Q: What happened to topic names?**
A: They're now EventHub entity names, still defined in domain config under `topics:`.

**Q: How does retry work without Kafka?**
A: Same concept, but using EventHub scheduled messages instead of Kafka retry topics.

**Q: Can I still use "xact" as a domain name?**
A: Yes, `_get_domain_config()` still supports "xact" as a legacy alias for "verisk".
