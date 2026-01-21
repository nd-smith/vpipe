# Retry Handlers

This package provides retry handling infrastructure for the Kafka pipeline with common utility functions that reduce duplication across domain-specific handlers.

## Overview

All retry handlers follow the same pattern:
1. Check if error is PERMANENT → send to DLQ immediately
2. Check if retries exhausted → send to DLQ
3. Otherwise → send to unified retry topic with exponential backoff

## Architecture

**Common Utilities** (`retry_utils.py`):
- `should_send_to_dlq()` - DLQ routing decision logic
- `calculate_retry_timestamp()` - Retry timing calculation
- `create_retry_headers()` - Standard Kafka headers for retry messages
- `create_dlq_headers()` - Standard Kafka headers for DLQ messages
- `truncate_error_message()` - Error message size control
- `add_error_metadata_to_dict()` - Metadata injection for retry context
- `log_retry_decision()` - Consistent logging format
- `record_retry_metrics()` - Unified metrics recording
- `record_dlq_metrics()` - DLQ metrics with retry exhaustion tracking

**Concrete Handlers**:
Each domain implements task-specific handlers that use the common utilities:
- `kafka_pipeline.common.retry.delta_handler.DeltaRetryHandler` - Delta Lake batch retry
- `kafka_pipeline.xact.retry.download_handler.DownloadRetryHandler` - XACT download retry
- `kafka_pipeline.claimx.retry.download_handler.DownloadRetryHandler` - ClaimX download retry (with URL refresh)
- `kafka_pipeline.claimx.retry.enrichment_handler.EnrichmentRetryHandler` - ClaimX enrichment retry

**Unified Scheduler** (`unified_scheduler.py`):
Consumes from a single retry topic and redelivers messages when their retry timestamp has elapsed.

## Creating a New Retry Handler

When creating a new handler, use the common utilities from `retry_utils` to avoid duplication:

```python
from kafka_pipeline.common.retry import retry_utils
from kafka_pipeline.common.producer import BaseKafkaProducer
from core.types import ErrorCategory
from datetime import datetime, timezone

class MyTaskRetryHandler:
    """Retry handler for MyTask."""

    def __init__(self, config, producer: BaseKafkaProducer):
        self.config = config
        self.producer = producer
        self.max_retries = config.max_retries
        self.retry_topic = config.retry_topic
        self.dlq_topic = config.dlq_topic

    async def handle_failure(
        self,
        task: MyTask,
        error: Exception,
        error_category: ErrorCategory,
    ) -> None:
        """Handle task failure with retry or DLQ routing."""

        # Use utility to check if should go to DLQ
        if retry_utils.should_send_to_dlq(
            error_category=error_category,
            retry_count=task.retry_count,
            max_retries=self.max_retries,
        ):
            await self._send_to_dlq(task, error, error_category)
        else:
            await self._send_to_retry(task, error, error_category)

    async def _send_to_retry(self, task, error, error_category):
        """Send to retry topic with backoff."""
        # Calculate retry timestamp using utility
        retry_timestamp = retry_utils.calculate_retry_timestamp(
            retry_count=task.retry_count,
            base_delay_seconds=self.config.retry_delay_base,
        )

        # Increment retry count
        task.retry_count += 1

        # Add error metadata using utility
        retry_utils.add_error_metadata_to_dict(
            target_dict=task.metadata,  # or create new dict
            error_message=str(error),
            error_category=error_category,
            retry_timestamp=retry_timestamp,
        )

        # Create headers using utility
        headers = retry_utils.create_retry_headers(
            retry_count=task.retry_count,
            max_retries=self.max_retries,
            retry_timestamp=retry_timestamp,
            error_category=error_category,
        )

        # Send to retry topic
        await self.producer.send(
            topic=self.retry_topic,
            key=task.id,
            value=task.model_dump_json().encode("utf-8"),
            headers=headers,
        )

        # Log using utility
        retry_utils.log_retry_decision(
            decision="RETRY",
            task_id=task.id,
            retry_count=task.retry_count,
            max_retries=self.max_retries,
            error_message=str(error),
            error_category=error_category,
        )

        # Record metrics using utility
        retry_utils.record_retry_metrics(
            domain="my_domain",
            task_type="my_task",
            retry_count=task.retry_count,
        )

    async def _send_to_dlq(self, task, error, error_category):
        """Send to dead-letter queue."""
        # Create DLQ message
        dlq_message = MyFailedMessage(
            task_id=task.id,
            original_task=task,
            final_error=retry_utils.truncate_error_message(str(error)),
            error_category=error_category.value,
            retry_count=task.retry_count,
            failed_at=datetime.now(timezone.utc),
        )

        # Create headers using utility
        headers = retry_utils.create_dlq_headers(
            retry_count=task.retry_count,
            max_retries=self.max_retries,
            error_category=error_category,
            final_error=str(error),
        )

        # Send to DLQ
        await self.producer.send(
            topic=self.dlq_topic,
            key=task.id,
            value=dlq_message.model_dump_json().encode("utf-8"),
            headers=headers,
        )

        # Log using utility
        retry_utils.log_retry_decision(
            decision="DLQ",
            task_id=task.id,
            retry_count=task.retry_count,
            max_retries=self.max_retries,
            error_message=str(error),
            error_category=error_category,
        )

        # Record metrics using utility
        retry_utils.record_dlq_metrics(
            domain="my_domain",
            task_type="my_task",
            error_category=error_category,
            retries_exhausted=(task.retry_count >= self.max_retries),
        )
```

## Benefits of Utility-Based Approach

**Reduced Duplication**: Common logic (routing decisions, timestamp calculation, metrics, logging) is centralized in `retry_utils.py`.

**Flexibility**: Handlers can use utilities as needed without being constrained by inheritance hierarchy.

**Consistency**: All handlers use the same logic for core retry operations, ensuring uniform behavior.

**Testability**: Utility functions can be tested independently of handler implementations.

## Unified Retry Topic

All domains use a single unified retry topic (`vpipe-claimx-retry` or `vpipe-xact-retry`). The `UnifiedRetryScheduler` consumes from this topic and redelivers messages when their retry timestamp has elapsed.

## Testing

When testing handlers:
- Mock the producer and verify correct topic routing
- Test both retry and DLQ paths
- Verify retry count increments correctly
- Verify metrics and logging calls
- Test edge cases (max retries, permanent errors, etc.)

See existing handler tests for examples:
- `tests/kafka_pipeline/common/retry/test_delta_handler.py`
- `tests/kafka_pipeline/xact/retry/test_download_handler.py`
- `tests/kafka_pipeline/claimx/retry/test_enrichment_handler.py`
