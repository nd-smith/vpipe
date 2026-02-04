# Logging Guidelines

## Philosophy

Logs exist to help you debug production issues and understand system behavior. They should be:

- **Clear**: Obvious what happened and why
- **Actionable**: Help you fix problems
- **Traceable**: Connect related events across workers
- **Quiet**: Signal, not noise

Logging follows the same principles as our code (see CLAUDE.md):
- Simple and explicit over clever abstractions
- One obvious way to do things
- Only log what matters

## The One Way to Log

Use Python's standard logging library directly. No wrappers, no decorators, no magic.

```python
import logging

logger = logging.getLogger(__name__)

# Simple message
logger.info("Processing batch")

# With context (use extra= for structured fields)
logger.info("Download complete", extra={
    "trace_id": task.trace_id,
    "duration_ms": elapsed,
    "bytes_downloaded": file_size,
})

# Error with traceback
try:
    download_file(url)
except Exception as e:
    logger.error("Download failed", extra={
        "trace_id": task.trace_id,
        "error_category": "TRANSIENT",
        "http_status": 500,
    }, exc_info=True)
```

That's it. No helpers, no mixins, no decorators.

## When to Log (and When Not To)

### ERROR - Failures requiring investigation

Log when:
- External API returns non-2xx status
- File download/upload fails
- Database query fails
- Circuit breaker trips
- Authentication fails
- Unhandled exceptions

Don't log:
- Validation errors for bad input (that's expected)
- Retry attempts (use WARNING)
- Errors you're about to re-raise without handling

**Example:**
```python
try:
    response = await api_client.fetch_project(project_id)
except aiohttp.ClientError as e:
    logger.error("API request failed", extra={
        "trace_id": task.trace_id,
        "project_id": project_id,
        "error_category": "TRANSIENT",
    }, exc_info=True)
    raise
```

### WARNING - Degraded operation, self-healing

Log when:
- Retrying a failed operation
- Circuit breaker enters HALF_OPEN state
- Rate limiting activates
- Operation exceeds performance threshold
- Recoverable errors (will retry)

Don't log:
- Every retry attempt (just first and last)
- Normal operation (even if slow)

**Example:**
```python
logger.warning("Retrying download after failure", extra={
    "trace_id": task.trace_id,
    "retry_count": attempt,
    "backoff_seconds": delay,
    "error_category": "TRANSIENT",
})
```

### INFO - Key lifecycle and business events

Log when:
- Worker starts/stops
- Processing batch completes
- Circuit breaker changes state (CLOSED ↔ OPEN ↔ HALF_OPEN)
- Cycle summary (every N seconds)
- Significant state transitions

Don't log:
- Individual record processing
- Internal method calls
- Cache hits/misses
- Helper function calls

**Example:**
```python
logger.info("Batch processing complete", extra={
    "cycle_id": cycle_id,
    "records_processed": 100,
    "records_succeeded": 98,
    "records_failed": 2,
    "duration_ms": elapsed,
})
```

### DEBUG - Detailed troubleshooting flow

Log when:
- Debugging a specific issue (add temporarily)
- Detailed flow needed for troubleshooting
- Cache behavior (hits/misses)
- Context changes

Don't log:
- Production-enabled DEBUG (creates noise)
- Obvious operations ("Starting method X")
- Redundant information already in context

**Example:**
```python
logger.debug("Circuit breaker state checked", extra={
    "circuit_name": "downloads",
    "circuit_state": state.value,
    "failure_count": failures,
})
```

**Default**: DEBUG should be OFF in production. Enable temporarily for troubleshooting.

## Required Context by Log Type

Structured logging (JSON) enables tracing across distributed workers. Always include relevant context.

### Minimum Context for All Logs

Automatically included via context variables (no action needed):
- `domain` - Pipeline domain (verisk, claimx)
- `stage` - Current stage (download, upload, enrichment)
- `worker_id` - Worker instance identifier
- `cycle_id` - Execution cycle ID

### Additional Context by Situation

**Correlation (required for tracing):**
```python
extra={
    "trace_id": task.trace_id,        # Request/message correlation
    "event_id": event.event_id,       # Event identifier
    "project_id": event.project_id,   # Business entity
}
```

**Errors (required):**
```python
extra={
    "trace_id": task.trace_id,
    "error_category": "TRANSIENT",    # TRANSIENT | PERMANENT | AUTH
    "http_status": 500,               # If API error
    "error_message": str(e)[:500],    # Truncated error message
}
```

**Performance (when logging duration):**
```python
extra={
    "trace_id": task.trace_id,
    "duration_ms": 1523.45,           # Always milliseconds, float
    "processing_time_ms": 1200.0,     # Processing time only
}
```

**Batch Processing:**
```python
extra={
    "batch_size": 100,
    "records_processed": 100,
    "records_succeeded": 98,
    "records_failed": 2,
    "records_skipped": 0,
}
```

**Storage Operations:**
```python
extra={
    "blob_path": sanitized_path,      # Sanitized (no SAS tokens)
    "bytes_uploaded": 1024000,
    "bytes_downloaded": 2048000,
}
```

**API Calls:**
```python
extra={
    "api_endpoint": "/projects",
    "api_method": "GET",
    "http_status": 200,
    "duration_ms": 234.5,
}
```

## Field Naming Conventions

Use consistent field names for queryability in ADX:

- **Timing**: Always `duration_ms` or `*_time_ms` as **float**
- **Counts**: `*_count`, `records_*`, `rows_*` as **int**
- **Sizes**: `bytes_*`, `*_size` as **int**
- **IDs**: `*_id` as **string**
- **Status**: `http_status`, `status_code` as **int**
- **Categories**: `error_category`, `event_type` as **string**

**Why it matters**: ADX aggregations require consistent types. `duration_ms: "100"` (string) breaks `avg(duration_ms)`.

## Error Categorization

All errors should be categorized for better debugging and retry logic:

```python
from core.types import ErrorCategory

# TRANSIENT - Retry will likely succeed
# Examples: Network timeout, 503 Service Unavailable, rate limit
error_category = "TRANSIENT"

# PERMANENT - Retry won't help
# Examples: 404 Not Found, invalid data, business logic failure
error_category = "PERMANENT"

# AUTH - Authentication/authorization failure
# Examples: 401 Unauthorized, 403 Forbidden, expired token
error_category = "AUTH"
```

## Security: URL Sanitization

Never log raw URLs with sensitive query parameters (SAS tokens, API keys).

**Automatic**: JSONFormatter sanitizes URLs in specific fields (`download_url`, `blob_path`, `http_url`)

**Manual sanitization** (if logging other URLs):
```python
# DON'T
logger.info("Downloading", extra={"url": presigned_url})  # Leaks SAS token!

# DO - Use field names that auto-sanitize
logger.info("Downloading", extra={"download_url": presigned_url})  # Auto-sanitized

# DO - Manual sanitization for other fields
from urllib.parse import urlparse, parse_qs, urlencode

def sanitize_url(url: str) -> str:
    """Remove sensitive query parameters from URL."""
    parsed = urlparse(url)
    if not parsed.query:
        return url

    params = parse_qs(parsed.query)
    # Remove sensitive params
    safe_params = {k: v for k, v in params.items()
                   if k.lower() not in ['sig', 'token', 'key', 'secret', 'password']}

    safe_query = urlencode(safe_params, doseq=True)
    return parsed._replace(query=safe_query).geturl()
```

## Common Patterns

### Timing Operations

```python
import time

start = time.perf_counter()
try:
    result = await download_file(url)
    duration_ms = (time.perf_counter() - start) * 1000

    logger.info("Download complete", extra={
        "trace_id": task.trace_id,
        "duration_ms": round(duration_ms, 2),
        "bytes_downloaded": len(result),
    })
except Exception as e:
    duration_ms = (time.perf_counter() - start) * 1000

    logger.error("Download failed", extra={
        "trace_id": task.trace_id,
        "duration_ms": round(duration_ms, 2),
        "error_category": "TRANSIENT",
    }, exc_info=True)
    raise
```

### Batch Processing Summary

```python
# Log cycle summary every N seconds (not every record)
if time.monotonic() - last_log_time > 30:
    logger.info(
        f"Cycle {cycle_count}: processed={total_processed} "
        f"(succeeded={succeeded}, failed={failed})",
        extra={
            "cycle_id": cycle_id,
            "cycle_count": cycle_count,
            "records_processed": total_processed,
            "records_succeeded": succeeded,
            "records_failed": failed,
            "duration_ms": cycle_duration,
        }
    )
    last_log_time = time.monotonic()
```

### Retry Logic

```python
# Log first attempt and final outcome, not every retry
for attempt in range(1, max_retries + 1):
    try:
        result = await api_call()

        # Only log if retries were needed
        if attempt > 1:
            logger.info("Retry succeeded", extra={
                "trace_id": task.trace_id,
                "retry_count": attempt - 1,
            })

        return result

    except Exception as e:
        if attempt == max_retries:
            # Final failure
            logger.error("All retries exhausted", extra={
                "trace_id": task.trace_id,
                "retry_count": attempt - 1,
                "error_category": classify_error(e),
            }, exc_info=True)
            raise

        # First failure only
        if attempt == 1:
            logger.warning("Request failed, will retry", extra={
                "trace_id": task.trace_id,
                "max_retries": max_retries - 1,
                "error_category": classify_error(e),
            })

        await asyncio.sleep(backoff_delay(attempt))
```

### Circuit Breaker State Changes

```python
# Only log state transitions, not every check
if new_state != old_state:
    logger.warning("Circuit breaker state changed", extra={
        "circuit_name": circuit_name,
        "old_state": old_state.value,
        "new_state": new_state.value,
        "failure_count": failure_count,
    })
```

## What NOT to Log

### Don't Log Noise

❌ **Every method call:**
```python
def process_batch(self, items):
    logger.debug("Processing batch")  # Obvious, no value
    ...
```

❌ **Redundant success messages:**
```python
try:
    result = operation()
    logger.info("Operation succeeded")  # If no error, assume success
except Exception as e:
    logger.error("Operation failed", exc_info=True)
```

❌ **Variable assignments:**
```python
user_id = get_user_id()
logger.debug(f"user_id = {user_id}")  # Use debugger instead
```

❌ **Before re-raising without handling:**
```python
try:
    result = api_call()
except Exception as e:
    logger.error("API call failed", exc_info=True)  # Don't log here
    raise  # Let caller handle and log
```

### Don't Log Sensitive Data

❌ **Passwords, tokens, API keys:**
```python
logger.debug(f"Using token: {api_token}")  # NEVER
```

❌ **PII (Personally Identifiable Information):**
```python
logger.info("Processing user", extra={"email": user.email})  # Depends on policy
```

❌ **Full URLs with SAS tokens:**
```python
logger.info("Downloading", extra={"url": presigned_url})  # Contains secrets
```

✅ **Use sanitized URLs or identifiers:**
```python
logger.info("Downloading", extra={
    "download_url": presigned_url,  # Auto-sanitized by JSONFormatter
    "blob_path": "domain/project/file.json",  # Path only
})
```

## Anti-Patterns (Don't Do This)

### ❌ Logging in Loops

```python
# DON'T - Creates massive log spam
for item in items:
    logger.debug(f"Processing item {item.id}")
    process(item)

# DO - Log summary
logger.info("Processing batch", extra={"batch_size": len(items)})
for item in items:
    process(item)
logger.info("Batch complete", extra={
    "batch_size": len(items),
    "duration_ms": elapsed,
})
```

### ❌ Logging Implementation Details

```python
# DON'T - Internal implementation
logger.debug("Adding item to cache")
cache[key] = value

# DO - Log outcome that matters
cache[key] = value
logger.debug("Cache updated", extra={"cache_size": len(cache)})
```

### ❌ Logging and Swallowing Errors

```python
# DON'T - Silent failure
try:
    result = api_call()
except Exception as e:
    logger.error("API failed", exc_info=True)
    return None  # Swallows error

# DO - Log and re-raise, or handle properly
try:
    result = api_call()
except Exception as e:
    logger.error("API failed", exc_info=True)
    raise  # Let caller decide
```

### ❌ Logging Instead of Raising

```python
# DON'T - Logging error instead of raising
if not user:
    logger.error("User not found")
    return None

# DO - Raise exception (caller logs if needed)
if not user:
    raise UserNotFoundError(f"User {user_id} not found")
```

## Log Output Formats

### Console (Development)

Human-readable format for local development:
```
2026-02-04 10:30:42 - INFO - [claimx] [download] - [batch:5] [evt_abc1] Processing batch
2026-02-04 10:30:45 - ERROR - [claimx] [download] - [evt_abc1] Download failed
```

### JSON (Production)

Machine-readable for ADX querying:
```json
{
  "ts": "2026-02-04T10:30:42.123Z",
  "level": "INFO",
  "logger": "pipeline.claimx.workers.download_worker",
  "msg": "Processing batch",
  "domain": "claimx",
  "stage": "download",
  "worker_id": "worker-0",
  "trace_id": "evt_abc123def",
  "batch_size": 100,
  "cycle_id": "c-20260204-103042-a3f2"
}
```

## Querying Logs in ADX

Example KQL queries for common scenarios:

**Find all errors for a trace_id:**
```kql
PipelineLogs
| where trace_id == "evt_abc123def"
| where level == "ERROR"
| project ts, stage, msg, error_category, error_message
```

**Average download duration by hour:**
```kql
PipelineLogs
| where stage == "download"
| where msg == "Download complete"
| summarize avg(duration_ms), count() by bin(ts, 1h)
```

**Failed downloads by error category:**
```kql
PipelineLogs
| where stage == "download"
| where level == "ERROR"
| summarize count() by error_category, http_status
```

**Slow operations (>1s):**
```kql
PipelineLogs
| where duration_ms > 1000
| project ts, stage, operation, duration_ms, trace_id
| order by duration_ms desc
```

## Migration from Current State

### Current State (Don't Use)

These patterns exist in the codebase but are being phased out:

❌ **LoggedClass mixin:**
```python
class ApiClient(LoggedClass):
    def fetch(self):
        self._log(logging.INFO, "Fetching")  # "Magic" auto-extraction
```

❌ **Decorators:**
```python
@logged_operation
def process(self):  # Auto-logs start/complete
    ...

@with_api_error_handling
async def fetch(self):  # Auto-logs exceptions
    ...
```

❌ **Helper functions:**
```python
log_with_context(logger, logging.INFO, "Message", trace_id="123")
log_exception(logger, exc, "Failed", trace_id="123")
```

### Target State (Use This)

✅ **Standard logging:**
```python
class ApiClient:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def fetch(self):
        self.logger.info("Fetching", extra={"api_url": self.api_url})

    async def process(self):
        try:
            result = await self.api_call()
            self.logger.info("Request complete", extra={
                "trace_id": task.trace_id,
                "duration_ms": elapsed,
            })
        except Exception as e:
            self.logger.error("Request failed", extra={
                "trace_id": task.trace_id,
                "error_category": "TRANSIENT",
            }, exc_info=True)
            raise
```

## Guidelines Summary

1. **Use standard library**: `logging.getLogger(__name__)` - no wrappers
2. **Be explicit**: Always clear what's being logged and why
3. **Include context**: Use `extra={}` for structured fields
4. **Choose level wisely**: ERROR (action needed), WARNING (degraded), INFO (lifecycle), DEBUG (off by default)
5. **Log outcomes, not flow**: Batch complete (yes), entering function (no)
6. **Sanitize URLs**: Use auto-sanitized fields or manual sanitization
7. **Categorize errors**: TRANSIENT, PERMANENT, AUTH
8. **Timing in milliseconds**: Always `duration_ms` as float
9. **Avoid noise**: No logging in loops, no obvious messages
10. **Correlation IDs**: Always include `trace_id` or `event_id`

## Questions?

When in doubt:
- **Will this help me debug a production issue?** → Log it
- **Is this obvious from the code?** → Don't log it
- **Will I need this for tracing across workers?** → Include correlation context
- **Would 1000 of these logs be noisy?** → Probably don't log it

Follow CLAUDE.md principles: simple, explicit, one obvious way.
