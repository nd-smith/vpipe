# Configuration Breaking Changes

**Date:** 2026-02-04
**Status:** NOT YET IN PRODUCTION - Safe to break

This document lists all breaking changes made to configuration handling to simplify the codebase and eliminate unnecessary complexity.

---

## Phase 3: Complete Kafka Removal (2026-02-04)

**Status:** BREAKING - Complete removal of Kafka from pipeline

### What Changed

Kafka has been **completely removed** from the pipeline architecture. EventHub is now used for all internal pipeline communication.

**Removed:**
- All Kafka connection configuration (bootstrap_servers, security_protocol, SASL authentication)
- Kafka consumer/producer defaults
- LocalKafkaConfig class (278 lines deleted)
- All Kafka-specific environment variables
- local_kafka field from PipelineConfig

**Retained (now EventHub-based):**
- Domain configuration (verisk, claimx)
- Topic names (now map to EventHub entity names)
- Retry configuration
- Storage configuration
- ClaimX API configuration

### Migration Required

1. **Update config.yaml**: Change section from `kafka:` to `pipeline:`
2. **Remove environment variables**: All KAFKA_*, LOCAL_KAFKA_* variables
3. **Update worker code**: Use `get_config()` instead of `LocalKafkaConfig.load_config()`
4. **Update deployment**: Configure EventHub instead of Kafka brokers

### Impact

- **Code reduction**: -560 lines (-28.6%)
- **Architecture**: Event Source → EventHub → Workers (Kafka fully removed)
- **Configuration**: Simpler, EventHub-only transport

See [PHASE_3_KAFKA_REMOVAL.md](PHASE_3_KAFKA_REMOVAL.md) for complete details.

---

## Phase 2.5: Environment Variable Standardization (2026-02-04)

**Status:** BREAKING - Configuration standardization

### Summary

**Removed:**
- Multiple environment variable names for the same configuration value
- Three-level storage configuration merge (kafka flat, root storage, kafka.storage)
- Legacy/deprecated environment variable names

**Impact:**
- Lines of code: **-108 lines** (-5.8% reduction)
- Complexity: Eliminated 96+ instances of chained fallback logic
- Maintainability: Single canonical name for each configuration value

---

## 1. Environment Variable Standardization

### ClaimX API Configuration

**Before:** Supported 2 environment variable names
```bash
CLAIMX_API_BASE_PATH=https://api.example.com  # ❌ REMOVED
CLAIMX_API_URL=https://api.example.com        # ✅ CANONICAL
```

**After:** Only one canonical name
```bash
CLAIMX_API_URL=https://api.example.com        # ✅ USE THIS
```

**Migration:** Rename `CLAIMX_API_BASE_PATH` → `CLAIMX_API_URL` in your environment

---

### Verisk Delta Lake Table Paths

**Before:** Supported 3 environment variable names per table
```bash
VERISK_EVENTS_TABLE_PATH=abfss://...          # ✅ CANONICAL
VERISK_DELTA_EVENTS_TABLE=abfss://...         # ❌ REMOVED
DELTA_EVENTS_TABLE_PATH=abfss://...           # ❌ REMOVED
```

**After:** Only domain-prefixed `*_TABLE_PATH` pattern
```bash
VERISK_EVENTS_TABLE_PATH=abfss://...          # ✅ USE THIS
VERISK_INVENTORY_TABLE_PATH=abfss://...       # ✅ USE THIS
VERISK_FAILED_TABLE_PATH=abfss://...          # ✅ USE THIS
```

**Migration:** Use `VERISK_*_TABLE_PATH` pattern. Remove `VERISK_DELTA_*` and `DELTA_*` variants.

---

### ClaimX Delta Lake Table Paths

**Before:** Supported 2 environment variable names per table
```bash
CLAIMX_PROJECTS_TABLE_PATH=abfss://...        # ✅ CANONICAL
CLAIMX_DELTA_PROJECTS_TABLE=abfss://...       # ❌ REMOVED
```

**After:** Only `*_TABLE_PATH` pattern
```bash
CLAIMX_PROJECTS_TABLE_PATH=abfss://...        # ✅ USE THIS
CLAIMX_CONTACTS_TABLE_PATH=abfss://...        # ✅ USE THIS
CLAIMX_INVENTORY_TABLE_PATH=abfss://...       # ✅ USE THIS
CLAIMX_MEDIA_TABLE_PATH=abfss://...           # ✅ USE THIS
CLAIMX_TASKS_TABLE_PATH=abfss://...           # ✅ USE THIS
CLAIMX_TASK_TEMPLATES_TABLE_PATH=abfss://...  # ✅ USE THIS
CLAIMX_EXTERNAL_LINKS_TABLE_PATH=abfss://...  # ✅ USE THIS
CLAIMX_VIDEO_COLLAB_TABLE_PATH=abfss://...    # ✅ USE THIS
```

**Migration:** Use `CLAIMX_*_TABLE_PATH` pattern. Remove `CLAIMX_DELTA_*` variants.

---

### Deduplication Backfill Configuration

**Before:** Generic `DEDUP_*` prefixes
```bash
DEDUP_BULK_BACKFILL=false                     # ❌ REMOVED (ambiguous which domain)
```

**After:** Domain-specific prefixes
```bash
VERISK_DEDUP_BULK_BACKFILL=false              # ✅ USE THIS
CLAIMX_DEDUP_BULK_BACKFILL=false              # ✅ USE THIS
```

**Migration:** Prefix all dedup config with domain name.

---

## 2. Storage Configuration Simplification

### Before: Three-Level Merge (REMOVED)

Configuration could be defined in **3 different places** with complex priority:

```yaml
# Option 1: Flat in kafka section (lowest priority)
kafka:
  onelake_base_path: "abfss://..."
  cache_dir: "/tmp/cache"

# Option 2: Root-level storage section (medium priority)
storage:
  onelake_base_path: "abfss://..."
  cache_dir: "/tmp/cache"

# Option 3: Nested kafka.storage (highest priority)
kafka:
  storage:
    onelake_base_path: "abfss://..."
    cache_dir: "/tmp/cache"
```

### After: Single Location Only

**Only `kafka.storage:` is supported:**

```yaml
kafka:
  storage:
    onelake_base_path: ${ONELAKE_BASE_PATH}
    onelake_domain_paths:
      verisk: ""
      claimx: ""
    cache_dir: ${CACHE_DIR:-/tmp/pipeline_cache}
```

**Migration:** Move all storage configuration to `kafka.storage:` section. Remove flat and root-level definitions.

---

## 3. Code Changes for Developers

### Helper Function Simplification

**Before:**
```python
def get_config_value(env_vars: list[str], yaml_value: str, default: str = "") -> str:
    """Get config value from env vars (first wins) or yaml or default."""
    for env_var in env_vars:
        value = os.getenv(env_var)
        if value:
            return value
    return yaml_value or default

# Usage
claimx_api_url = get_config_value(
    ["CLAIMX_API_BASE_PATH", "CLAIMX_API_URL"],  # Multiple fallbacks
    claimx_api.get("base_url", "")
)
```

**After:**
```python
def get_config_value(env_var: str, yaml_value: str, default: str = "") -> str:
    """Get config value from env var or yaml or default."""
    value = os.getenv(env_var)
    if value:
        return value
    return yaml_value or default

# Usage
claimx_api_url = get_config_value(
    "CLAIMX_API_URL",  # Single canonical name
    claimx_api.get("base_url", "")
)
```

### Storage Configuration

**Before:**
```python
def _merge_storage_config(yaml_data: dict, kafka_data: dict) -> dict:
    """Merge storage config. Priority: kafka.storage > root storage > flat kafka."""
    result = {}
    for key in ["onelake_base_path", "onelake_domain_paths", "cache_dir"]:
        if key in kafka_data:
            result[key] = kafka_data[key]
    result.update(yaml_data.get("storage", {}))
    result.update(kafka_data.get("storage", {}))
    return result
```

**After:**
```python
def _get_storage_config(kafka_data: dict) -> dict:
    """Get storage config from kafka.storage section only."""
    return kafka_data.get("storage", {})
```

---

## 4. What DID NOT Change (Backward Compatibility Maintained)

### Domain Name Aliasing in Code

**Users can still use "xact" or "verisk" as domain names in code:**

```python
config.get_topic("xact", "events")     # ✅ Still works (legacy alias)
config.get_topic("verisk", "events")   # ✅ Still works (canonical)
```

This is maintained via the `_get_domain_config()` helper for backward compatibility with existing code.

### Kafka Topic Names (External Contracts)

**Topic names remain unchanged** (these are external contracts):
- `com.allstate.pcesdopodappv1.verisk.events.raw`
- `com.allstate.pcesdopodappv1.claimx.events.raw`

### Monitoring Labels

**Prometheus/Grafana dashboards** may still reference `domain: "xact"` - these are external contracts and unchanged.

---

## Complete Environment Variable Reference

### Canonical Names (USE THESE)

| Configuration | Environment Variable | Example |
|---------------|---------------------|---------|
| **ClaimX API** | | |
| API URL | `CLAIMX_API_URL` | `https://www.claimxperience.com/service/cxedirest` |
| API Token | `CLAIMX_API_TOKEN` | `base64-token` |
| API Timeout | `CLAIMX_API_TIMEOUT_SECONDS` | `30` |
| API Concurrency | `CLAIMX_API_CONCURRENCY` | `20` |
| **Verisk Delta Tables** | | |
| Events Table | `VERISK_EVENTS_TABLE_PATH` | `abfss://.../verisk_events` |
| Inventory Table | `VERISK_INVENTORY_TABLE_PATH` | `abfss://.../verisk_attachments` |
| Failed Table | `VERISK_FAILED_TABLE_PATH` | `abfss://.../verisk_failed` |
| **ClaimX Delta Tables** | | |
| Projects Table | `CLAIMX_PROJECTS_TABLE_PATH` | `abfss://.../claimx_projects` |
| Contacts Table | `CLAIMX_CONTACTS_TABLE_PATH` | `abfss://.../claimx_contacts` |
| Inventory Table | `CLAIMX_INVENTORY_TABLE_PATH` | `abfss://.../claimx_attachments` |
| Media Table | `CLAIMX_MEDIA_TABLE_PATH` | `abfss://.../claimx_attachments_metadata` |
| Tasks Table | `CLAIMX_TASKS_TABLE_PATH` | `abfss://.../claimx_tasks` |
| Task Templates | `CLAIMX_TASK_TEMPLATES_TABLE_PATH` | `abfss://.../claimx_task_templates` |
| External Links | `CLAIMX_EXTERNAL_LINKS_TABLE_PATH` | `abfss://.../claimx_external_link` |
| Video Collab | `CLAIMX_VIDEO_COLLAB_TABLE_PATH` | `abfss://.../claimx_video_collab` |

### Removed Names (DO NOT USE)

| Old Name | New Canonical Name |
|----------|-------------------|
| `CLAIMX_API_BASE_PATH` | `CLAIMX_API_URL` |
| `VERISK_DELTA_EVENTS_TABLE` | `VERISK_EVENTS_TABLE_PATH` |
| `DELTA_EVENTS_TABLE_PATH` | `VERISK_EVENTS_TABLE_PATH` |
| `VERISK_DELTA_INVENTORY_TABLE` | `VERISK_INVENTORY_TABLE_PATH` |
| `DELTA_INVENTORY_TABLE_PATH` | `VERISK_INVENTORY_TABLE_PATH` |
| `VERISK_DELTA_FAILED_TABLE` | `VERISK_FAILED_TABLE_PATH` |
| `DELTA_FAILED_TABLE_PATH` | `VERISK_FAILED_TABLE_PATH` |
| `CLAIMX_DELTA_*_TABLE` | `CLAIMX_*_TABLE_PATH` |
| `DEDUP_BULK_BACKFILL` | `VERISK_DEDUP_BULK_BACKFILL` or `CLAIMX_DEDUP_BULK_BACKFILL` |

---

## Migration Checklist

- [ ] Update `.env` file with canonical environment variable names
- [ ] Update `config.yaml` to use only `kafka.storage:` section
- [ ] Remove all `CLAIMX_API_BASE_PATH` references
- [ ] Remove all `*_DELTA_*_TABLE` environment variables
- [ ] Remove all `DELTA_*_TABLE_PATH` environment variables
- [ ] Update deployment scripts/CI/CD with new variable names
- [ ] Update documentation with canonical names
- [ ] Test configuration loading in all environments

---

## Benefits

1. **Explicit > Implicit:** One clear name per configuration value
2. **Easier Debugging:** No more "which env var was actually used?"
3. **Less Code:** Removed 108 lines of unnecessary complexity
4. **Better Documentation:** Clear canonical names in .env.example
5. **Domain Clarity:** Domain-prefixed names make ownership obvious
6. **Shared Resources:** Explicit shared vs domain-specific configuration

---

## Questions?

If you encounter issues during migration, check:
1. This document for canonical environment variable names
2. `.env.example` for current recommended configuration
3. `config.yaml` for required YAML structure (only `kafka.storage:`)
