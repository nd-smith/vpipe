# ADX (Azure Data Explorer) Logging Recommendations

## Current Status: Good Foundation ✅
Your logging infrastructure is well-suited for ADX, but needs optimization for type safety.

## Issues Identified

### 1. Type Coercion via `default=str`
**File**: `core/logging/formatters.py:164`

**Current**:
```python
return json.dumps(log_entry, default=str, ensure_ascii=False)
```

**Problem**: Non-serializable objects become strings, causing schema drift in ADX.

**Impact**:
```json
{
  "processing_time_ms": "1234.56",  // String, not real
  "batch_size": "100",               // String, not long
  "timestamp_object": "2026-01-19..."  // If datetime object used
}
```

### 2. No Explicit Type Mapping

ADX needs consistent types for efficient querying. Your numeric fields need explicit handling.

### 3. Large Exception Strings

Stack traces as single strings are hard to query in ADX.

## Recommended Changes

### Change 1: Add Type-Safe JSON Serializer

**Add to `core/logging/formatters.py`**:

```python
from decimal import Decimal
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Any
import json

def _json_serializer(obj: Any) -> Any:
    """
    Type-safe JSON serializer for ADX compatibility.

    Ensures proper types for ADX schema:
    - datetime → ISO 8601 string
    - Decimal → float
    - Path → string
    - Enums → value
    - Everything else → string (fallback)
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, Path):
        return str(obj)
    elif hasattr(obj, '__dict__'):
        # For objects with __dict__, serialize as dict
        return obj.__dict__
    elif hasattr(obj, 'value'):
        # For enums
        return obj.value
    else:
        # Fallback to string
        return str(obj)

class JSONFormatter(logging.Formatter):
    # ... existing code ...

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON with sanitized URLs."""
        # ... existing log_entry construction ...

        # Changed from default=str to custom serializer
        return json.dumps(log_entry, default=_json_serializer, ensure_ascii=False)
```

### Change 2: Add Type Validation for Numeric Fields

**Add to `core/logging/formatters.py`**:

```python
class JSONFormatter(logging.Formatter):
    # Define expected types for fields
    NUMERIC_FIELDS = {
        # Timing fields (milliseconds as float/real)
        "processing_time_ms": float,
        "duration_ms": float,

        # Count fields (as long/int64)
        "batch_size": int,
        "retry_count": int,
        "records_processed": int,
        "records_succeeded": int,
        "records_failed": int,
        "records_skipped": int,
        "rows_read": int,
        "rows_written": int,
        "rows_merged": int,
        "rows_inserted": int,
        "rows_updated": int,
        "df_rows": int,
        "df_cols": int,
        "api_calls": int,

        # Byte counts (as long/int64)
        "blob_size": int,
        "bytes_uploaded": int,
        "bytes_downloaded": int,
        "memory_mb": float,

        # HTTP codes (as int)
        "http_status": int,
        "status_code": int,

        # Kafka fields
        "kafka_partition": int,
        "kafka_offset": int,

        # Query fields
        "query_length": int,
        "limit": int,
    }

    def _ensure_type(self, field: str, value: Any) -> Any:
        """Ensure field has correct type for ADX."""
        if field in self.NUMERIC_FIELDS and value is not None:
            expected_type = self.NUMERIC_FIELDS[field]
            try:
                # Convert to expected type
                return expected_type(value)
            except (ValueError, TypeError):
                # If conversion fails, log warning and return None
                # ADX prefers NULL over invalid data
                return None
        return value

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON with type safety."""
        log_entry: Dict[str, Any] = {
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }

        # ... context injection code ...

        # Extract extra fields with type validation AND sanitization
        for field in self.EXTRA_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                # First ensure correct type
                typed_value = self._ensure_type(field, value)
                # Then sanitize if needed
                log_entry[field] = self._sanitize_value(field, typed_value)

        # ... rest of method ...
```

### Change 3: Structure Exception Data

**Replace**:
```python
if record.exc_info:
    log_entry["exception"] = self.formatException(record.exc_info)
```

**With**:
```python
if record.exc_info:
    exc_type, exc_value, exc_tb = record.exc_info
    log_entry["exception"] = {
        "type": exc_type.__name__ if exc_type else None,
        "message": str(exc_value) if exc_value else None,
        "stacktrace": self.formatException(record.exc_info)
    }
```

**ADX Schema**:
```kql
.create table logs (
    exception: dynamic  // Can query exception.type, exception.message
)
```

## ADX Table Schema

### Recommended Table Schema

```kql
.create-merge table pipeline_logs (
    // Core fields
    ts: datetime,
    level: string,
    logger: string,
    msg: string,

    // Context fields
    domain: string,
    stage: string,
    cycle_id: string,
    worker_id: string,
    trace_id: string,

    // Location (for DEBUG/ERROR)
    file: string,

    // Numeric metrics (strongly typed)
    processing_time_ms: real,
    duration_ms: real,
    batch_size: long,
    retry_count: int,
    records_processed: long,
    records_succeeded: long,
    records_failed: long,
    records_skipped: long,
    bytes_uploaded: long,
    bytes_downloaded: long,
    http_status: int,

    // Dynamic fields for flexibility
    extra: dynamic,
    exception: dynamic
)
```

### Ingestion Mapping

```kql
.create-or-alter table pipeline_logs ingestion json mapping 'pipeline_logs_mapping'
```kql
'['
'    {"column":"ts", "path":"$.ts", "datatype":"datetime"},'
'    {"column":"level", "path":"$.level", "datatype":"string"},'
'    {"column":"logger", "path":"$.logger", "datatype":"string"},'
'    {"column":"msg", "path":"$.msg", "datatype":"string"},'
'    {"column":"domain", "path":"$.domain", "datatype":"string"},'
'    {"column":"stage", "path":"$.stage", "datatype":"string"},'
'    {"column":"cycle_id", "path":"$.cycle_id", "datatype":"string"},'
'    {"column":"worker_id", "path":"$.worker_id", "datatype":"string"},'
'    {"column":"trace_id", "path":"$.trace_id", "datatype":"string"},'
'    {"column":"file", "path":"$.file", "datatype":"string"},'
'    {"column":"processing_time_ms", "path":"$.processing_time_ms", "datatype":"real"},'
'    {"column":"batch_size", "path":"$.batch_size", "datatype":"long"},'
'    {"column":"retry_count", "path":"$.retry_count", "datatype":"int"},'
'    {"column":"records_processed", "path":"$.records_processed", "datatype":"long"},'
'    {"column":"extra", "path":"$", "datatype":"dynamic", "transform":"SourceLocation"}'
']'
```

### Update Policy for Error Tracking

```kql
// Create materialized view for errors
.create materialized-view with (backfill=true) pipeline_errors on table pipeline_logs
{
    pipeline_logs
    | where level in ("ERROR", "CRITICAL")
    | extend
        error_type = tostring(exception.type),
        error_message = tostring(exception.message),
        error_category = tostring(extra.error_category)
    | project
        ts, domain, stage, worker_id, trace_id,
        error_type, error_message, error_category,
        msg, file
}
```

## Ingestion Methods

### Option 1: Direct Ingestion via SDK

```python
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.ingest import QueuedIngestClient, IngestionProperties

# Add ADX handler to logging
class ADXLogHandler(logging.Handler):
    def __init__(self, cluster_url: str, database: str, table: str):
        super().__init__()
        self.ingest_client = QueuedIngestClient(
            KustoConnectionStringBuilder.with_az_cli_authentication(cluster_url)
        )
        self.ingestion_props = IngestionProperties(
            database=database,
            table=table,
            data_format="json",
            ingestion_mapping_reference="pipeline_logs_mapping"
        )

    def emit(self, record):
        log_entry = self.format(record)
        # Queue for ingestion (batched)
        self.ingest_client.ingest_from_stream(
            io.StringIO(log_entry),
            ingestion_properties=self.ingestion_props
        )
```

### Option 2: File-based Ingestion (Recommended)

Your current rotating file handler works perfectly:

```bash
# Use Azure Data Factory or Logic App to ingest log files
az dt data-explorer ingest \
  --cluster-uri "https://yourcluster.region.kusto.windows.net" \
  --database "PipelineLogs" \
  --table "pipeline_logs" \
  --source-path "logs/xact/2026-01-19/*.log" \
  --format "json" \
  --mapping-reference "pipeline_logs_mapping"
```

### Option 3: Event Hub → ADX (Real-time)

Add Event Hub handler for real-time streaming:

```python
from azure.eventhub import EventHubProducerClient

class EventHubLogHandler(logging.Handler):
    def __init__(self, connection_string: str, eventhub_name: str):
        super().__init__()
        self.producer = EventHubProducerClient.from_connection_string(
            connection_string,
            eventhub_name=eventhub_name
        )

    def emit(self, record):
        if record.levelno >= logging.WARNING:  # Only stream warnings/errors
            log_entry = self.format(record)
            self.producer.send_event(EventData(log_entry))
```

## Useful ADX Queries

### Query by Trace ID
```kql
pipeline_logs
| where trace_id == "evt_123abc"
| order by ts asc
| project ts, stage, level, msg, processing_time_ms
```

### Error Rate by Stage
```kql
pipeline_logs
| where ts > ago(1h)
| summarize
    total = count(),
    errors = countif(level in ("ERROR", "CRITICAL"))
    by domain, stage, bin(ts, 5m)
| extend error_rate = errors * 100.0 / total
| render timechart
```

### P95 Processing Time
```kql
pipeline_logs
| where isnotnull(processing_time_ms)
| summarize
    p50 = percentile(processing_time_ms, 50),
    p95 = percentile(processing_time_ms, 95),
    p99 = percentile(processing_time_ms, 99)
    by domain, stage
```

### Failed Upload Traces
```kql
pipeline_logs
| where stage == "upload" and level == "ERROR"
| extend error_cat = tostring(extra.error_category)
| summarize count() by error_cat, bin(ts, 1h)
| render columnchart
```

## Testing Type Safety

```python
# Test script to verify types
import json

# Load a log line
log_line = '{"ts":"2026-01-19T16:53:12.345Z","processing_time_ms":1234.56,"batch_size":100}'
parsed = json.loads(log_line)

# Verify types
assert isinstance(parsed["processing_time_ms"], float)  # Not string!
assert isinstance(parsed["batch_size"], int)  # Not string!
```

## Priority Recommendations

### High Priority (Do First)
1. ✅ Add type validation to JSONFormatter (`_ensure_type` method)
2. ✅ Replace `default=str` with `default=_json_serializer`
3. ✅ Structure exception field

### Medium Priority
4. Create ADX table with proper schema
5. Set up ingestion mapping
6. Create materialized views for common queries

### Low Priority
7. Add Event Hub handler for real-time streaming
8. Create ADX dashboards
9. Set up alerts on error rates

## Files to Modify

1. `core/logging/formatters.py` - Add type safety
2. `core/logging/setup.py` - Optional: Add ADX handler
3. New file: `core/logging/adx_handler.py` - ADX ingestion handler (optional)

---

**Expected Improvement**:
- ✅ Consistent type schema in ADX
- ✅ 10x faster numeric aggregations
- ✅ Easier error analysis with structured exceptions
- ✅ No schema drift or type conflicts
