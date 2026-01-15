# Retry Handlers

This package provides a generic base retry handler that can be extended for different task types.

## Overview

All retry handlers follow the same pattern:
1. Check if error is PERMANENT -> send to DLQ immediately
2. Check if retries exhausted -> send to DLQ
3. Otherwise -> send to retry topic with exponential backoff

The `BaseRetryHandler` extracts this common logic so each task type only needs to implement:
- How to copy and increment retry count on tasks
- How to create DLQ messages
- What key to use for Kafka partitioning

## Usage

### Creating a Concrete Handler

```python
from kafka_pipeline.common.retry import BaseRetryHandler
from datetime import datetime, timezone
from core.types import ErrorCategory

class MyTaskRetryHandler(BaseRetryHandler[MyTask, MyFailedMessage]):
    """Retry handler for MyTask."""

    def _create_updated_task(self, task: MyTask) -> MyTask:
        """Create a copy with retry_count incremented."""
        updated = task.model_copy(deep=True)
        updated.retry_count += 1
        return updated

    def _create_dlq_message(
        self,
        task: MyTask,
        error_message: str,
        error_category: ErrorCategory,
    ) -> MyFailedMessage:
        """Create DLQ message with full context."""
        return MyFailedMessage(
            task_id=task.id,
            original_task=task,
            final_error=error_message,
            error_category=error_category.value,
            retry_count=task.retry_count,
            failed_at=datetime.now(timezone.utc),
        )

    def _get_task_key(self, task: MyTask) -> str:
        """Get key for Kafka partitioning."""
        return task.id
```

### Using the Handler

```python
# Initialize
config = KafkaConfig.from_env()
producer = BaseKafkaProducer(config)
await producer.start()
handler = MyTaskRetryHandler(config, producer, domain="my_domain")

# Handle failures
await handler.handle_failure(
    task=my_task,
    error=ConnectionError("Network timeout"),
    error_category=ErrorCategory.TRANSIENT,
)
```

## Customization Hooks

### Override Retry Count Access

If your task stores retry_count differently:

```python
def _get_retry_count(self, task: MyTask) -> int:
    return task.attempts  # Use 'attempts' instead of 'retry_count'
```

### Override Metadata Addition

If your task has different metadata structure:

```python
def _add_error_metadata(self, task, error, error_category, delay_seconds):
    # Add custom fields
    retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
    task.error_info = {
        "message": str(error)[:500],
        "category": error_category.value,
        "retry_at": retry_at.isoformat(),
        "custom_field": "custom_value",
    }
```

### Override Log Context

Add task-specific fields to log messages:

```python
def _get_log_context(self, task: MyTask) -> Dict[str, Any]:
    return {
        "task_id": task.id,
        "task_type": task.type,
        "priority": task.priority,
    }
```

## What the Base Handler Does

The base handler automatically handles:
- **Retry limit checking**: Compares retry_count against max_retries
- **Permanent error detection**: Sends directly to DLQ for PERMANENT errors
- **Retry topic routing**: Uses KafkaConfig to get correct retry topic
- **Exponential backoff**: Calculates retry timestamps based on configured delays
- **Metrics recording**: Records retry attempts, exhaustion, and DLQ messages
- **Error truncation**: Truncates long error messages to prevent huge messages
- **Kafka sending**: Sends to retry topics and DLQ with proper headers
- **Structured logging**: Logs with consistent context throughout retry flow

## Existing Handlers

Examples of concrete handlers using BaseRetryHandler:
- `kafka_pipeline.common.retry.handler.RetryHandler` - Download task retry handler (xact domain)
- `kafka_pipeline.claimx.retry.handler.DeltaRetryHandler` - Delta batch retry handler (claimx domain)
- `kafka_pipeline.claimx.retry.enrichment_handler.EnrichmentRetryHandler` - Enrichment task handler

## Testing

See `tests/kafka_pipeline/common/retry/test_base_handler.py` for comprehensive examples of:
- Testing concrete handlers
- Verifying retry routing
- Verifying DLQ routing
- Testing customization hooks
- Testing edge cases
