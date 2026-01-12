# Delta Table Initialization Scripts

Scripts for creating and managing Delta tables used by plugin workers.

## iTel Cabinet Task Tracking Table

### Quick Start

```bash
# Dry run - see what will be created
python scripts/delta_tables/create_itel_cabinet_tracking_table.py --dry-run

# Create table in default database
python scripts/delta_tables/create_itel_cabinet_tracking_table.py

# Create table in specific database
python scripts/delta_tables/create_itel_cabinet_tracking_table.py \
    --database my_database

# Create external table at specific location
python scripts/delta_tables/create_itel_cabinet_tracking_table.py \
    --table-location s3://my-bucket/delta/itel_cabinet_task_tracking

# Drop and recreate
python scripts/delta_tables/create_itel_cabinet_tracking_table.py \
    --drop-existing
```

### Table Schema

The `itel_cabinet_task_tracking` table includes:

**Event Tracking:**
- `event_id` - Unique event identifier
- `event_type` - Event type (CUSTOM_TASK_ASSIGNED, CUSTOM_TASK_COMPLETED, etc.)
- `event_timestamp` - When event occurred
- `processed_timestamp` - When worker processed the event

**Task Identification:**
- `task_id` - ClaimX task template ID (32513)
- `assignment_id` - Task assignment instance ID
- `project_id` - ClaimX project ID

**Status & Lifecycle:**
- `task_status` - Current status
- `task_name` - Task name
- `task_created_at` - Creation timestamp
- `task_completed_at` - Completion timestamp (null if incomplete)

**User Tracking:**
- `assigned_to_user_id` - Assignee user ID
- `assigned_by_user_id` - Assigner user ID

**Enriched Data (JSON strings):**
- `claimx_task_full` - Complete task details from ClaimX API
- `original_event` - Original event data
- `project_data` - Project information

**Partitioning:**
- `year`, `month`, `day` - Extracted from `event_timestamp` for query performance

### Table Properties

- **Partitioned by:** year, month, day (improves query performance)
- **Change Data Feed:** Enabled (tracks all changes for downstream CDC)
- **Auto-Optimize:** Enabled (automatic compaction and optimization)
- **Format:** Delta Lake (ACID transactions, time travel, schema evolution)

### Sample Queries

See `itel_cabinet_queries.sql` for example queries.

## Maintenance

### Optimize Table

```sql
-- Optimize and Z-order for common query patterns
OPTIMIZE itel_cabinet_task_tracking
ZORDER BY (task_id, task_status);
```

### Vacuum Old Files

```sql
-- Remove files older than 7 days (after optimization)
VACUUM itel_cabinet_task_tracking RETAIN 168 HOURS;
```

### Table Statistics

```sql
-- Update statistics for query optimization
ANALYZE TABLE itel_cabinet_task_tracking COMPUTE STATISTICS;
```

## Troubleshooting

### Table Already Exists

```bash
# Drop and recreate
python scripts/delta_tables/create_itel_cabinet_tracking_table.py --drop-existing
```

### Permission Errors

Ensure your Spark session has write access to:
- Database/catalog (for managed tables)
- S3/storage location (for external tables)

### Schema Evolution

The table is configured to support schema evolution. New columns can be added automatically by the DeltaTableWriter handler when `schema_evolution: true` is set.

## Related Documentation

- [iTel Cabinet Plugin](../../config/plugins/itel_cabinet_api/README.md)
- [Plugin Specification](../../config/plugins/itel_cabinet_api/SPEC.md)
- [Worker Configuration](../../config/plugins/itel_cabinet_api/workers.yaml)
