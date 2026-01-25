# Delta Lake Simulation Mode - Quick Reference Guide

## Overview

Delta Lake simulation mode allows you to write ClaimX entity tables to local filesystem instead of cloud OneLake storage. This enables local testing, inspection, and debugging without Azure credentials.

## Quick Start

```bash
# 1. Enable simulation mode
export SIMULATION_MODE=true

# 2. Start entity delta writer
python -m kafka_pipeline claimx-entity-delta-writer

# 3. Inspect written tables
python scripts/inspect_simulation_delta.py

# 4. Clean up when done
./scripts/cleanup_simulation_data.sh
```

## Directory Structure

```
/tmp/vpipe_simulation/
├── delta/                          # Delta Lake tables
│   ├── claimx_projects/            # Project metadata
│   │   ├── _delta_log/             # Transaction log
│   │   └── *.parquet               # Data files
│   ├── claimx_contacts/            # Contact information
│   ├── claimx_attachment_metadata/ # Media metadata
│   ├── claimx_tasks/               # Task information
│   ├── claimx_task_templates/      # Task templates
│   ├── claimx_external_links/      # External links
│   └── claimx_video_collab/        # Video collaboration
└── storage/                        # Local file storage (from WP3)
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SIMULATION_MODE` | `false` | Enable simulation mode |
| `SIMULATION_DELTA_PATH` | `/tmp/vpipe_simulation/delta` | Local Delta tables directory |
| `SIMULATION_TRUNCATE_TABLES` | `false` | Clean tables before starting |
| `SIMULATION_DELTA_WRITE_MODE` | `append` | Write mode (append/overwrite/merge) |

## Common Tasks

### Inspect Delta Tables

```bash
# View all tables
python scripts/inspect_simulation_delta.py

# View specific directory
python scripts/inspect_simulation_delta.py /custom/path/to/delta
```

Output shows:
- Row counts for each table
- Schema validation (missing/extra columns)
- Sample data from first row
- Table sizes on disk

### Clean Tables Before Run

Useful for fresh test runs without old data:

```bash
# Option 1: Environment variable
SIMULATION_TRUNCATE_TABLES=true SIMULATION_MODE=true \
  python -m kafka_pipeline claimx-entity-delta-writer

# Option 2: Manual cleanup
./scripts/cleanup_simulation_data.sh
```

### Query Tables with Polars

```python
import polars as pl

# Read entire table
df = pl.read_delta("/tmp/vpipe_simulation/delta/claimx_projects")
print(df)

# Filter and aggregate
df = pl.read_delta("/tmp/vpipe_simulation/delta/claimx_contacts")
summary = df.group_by("contact_type").agg(pl.count())
print(summary)
```

### Query Tables with DuckDB

```python
import duckdb

conn = duckdb.connect()
conn.execute("INSTALL delta")
conn.execute("LOAD delta")

# SQL queries on Delta tables
result = conn.execute("""
    SELECT
        status,
        COUNT(*) as count
    FROM delta_scan('/tmp/vpipe_simulation/delta/claimx_projects')
    GROUP BY status
""").fetchdf()

print(result)
```

### Export to CSV/Parquet

```python
import polars as pl

# Read Delta table
df = pl.read_delta("/tmp/vpipe_simulation/delta/claimx_projects")

# Export to CSV
df.write_csv("/tmp/projects_export.csv")

# Export to Parquet
df.write_parquet("/tmp/projects_export.parquet")
```

## Testing Workflow

### 1. Development Testing

```bash
# Start Kafka
docker-compose -f scripts/docker-compose.kafka.yml up -d

# Enable simulation and truncate on start
export SIMULATION_MODE=true
export SIMULATION_TRUNCATE_TABLES=true

# Run entity delta writer
python -m kafka_pipeline claimx-entity-delta-writer

# In another terminal, send test events
python scripts/send_test_event.py --domain claimx --event-type project_created

# Inspect results
python scripts/inspect_simulation_delta.py
```

### 2. Verification Testing

```bash
# Run automated tests
SIMULATION_MODE=true python scripts/test_delta_simulation.py

# Should see:
# ✅ PASS: Configuration
# ✅ PASS: Delta Table Write
# ✅ PASS: Entity Writer
# ✅ PASS: Inspection Script
# ✅ PASS: Cleanup Script
```

### 3. Schema Validation

```bash
# Inspect tables to check schemas
python scripts/inspect_simulation_delta.py

# Look for schema warnings:
# ⚠️  Missing columns: ...
# ℹ️  Extra columns: ...
```

## Troubleshooting

### Tables Not Created

**Problem**: No Delta tables in `/tmp/vpipe_simulation/delta/`

**Solutions**:
1. Verify simulation mode is enabled: `echo $SIMULATION_MODE`
2. Check worker logs for errors
3. Ensure worker is processing messages
4. Verify directory exists: `ls -la /tmp/vpipe_simulation/`

### Schema Mismatch Errors

**Problem**: Delta write fails with schema/type errors

**Solutions**:
1. Clean tables: `SIMULATION_TRUNCATE_TABLES=true`
2. Check source data types match expected schemas
3. Inspect with: `python scripts/inspect_simulation_delta.py`

### Permission Denied

**Problem**: Cannot write to `/tmp/vpipe_simulation/`

**Solutions**:
1. Check directory permissions: `ls -ld /tmp/vpipe_simulation/`
2. Create manually: `mkdir -p /tmp/vpipe_simulation/delta`
3. Fix permissions: `chmod 755 /tmp/vpipe_simulation`

### Tables Too Large

**Problem**: Disk space issues from accumulated data

**Solutions**:
1. Clean regularly: `./scripts/cleanup_simulation_data.sh`
2. Use custom path: `export SIMULATION_DELTA_PATH=/var/tmp/delta`
3. Enable truncate on start: `export SIMULATION_TRUNCATE_TABLES=true`

## Differences from Production

| Aspect | Simulation Mode | Production Mode |
|--------|----------------|-----------------|
| Storage | Local filesystem | Azure OneLake |
| Authentication | None required | Azure AD tokens |
| Path format | `/tmp/vpipe_simulation/delta/...` | `abfss://workspace@onelake/...` |
| Performance | Fast (local I/O) | Slower (network) |
| Durability | Local disk only | Cloud redundant storage |
| Partitioning | Supported | Supported |
| ACID transactions | Supported | Supported |
| Time travel | Supported | Supported |

## Advanced Usage

### Custom Delta Path

```bash
# Use different directory
export SIMULATION_DELTA_PATH=/var/tmp/my_delta_tables
export SIMULATION_MODE=true
python -m kafka_pipeline claimx-entity-delta-writer
```

### Programmatic Access

```python
from kafka_pipeline.simulation import is_simulation_mode, get_simulation_config

if is_simulation_mode():
    config = get_simulation_config()
    print(f"Delta path: {config.local_delta_path}")
    print(f"Truncate on start: {config.truncate_tables_on_start}")
```

### Integration with CI/CD

```yaml
# .github/workflows/test.yml
- name: Test Delta simulation
  env:
    SIMULATION_MODE: true
    SIMULATION_TRUNCATE_TABLES: true
  run: |
    python -m kafka_pipeline claimx-entity-delta-writer &
    sleep 10
    python scripts/send_test_event.py
    sleep 5
    python scripts/inspect_simulation_delta.py
    python scripts/test_delta_simulation.py
```

## Related Documentation

- **Simulation README**: `kafka_pipeline/simulation/README.md` - Full simulation mode documentation
- **Entity Writer**: `kafka_pipeline/claimx/writers/delta_entities.py` - Implementation details
- **Delta Storage**: `kafka_pipeline/common/storage/delta.py` - Delta Lake operations

## Support

For issues or questions:
1. Check logs: Worker output shows simulation mode detection
2. Run tests: `SIMULATION_MODE=true python scripts/test_delta_simulation.py`
3. Inspect tables: `python scripts/inspect_simulation_delta.py`
4. Clean and retry: `./scripts/cleanup_simulation_data.sh`
