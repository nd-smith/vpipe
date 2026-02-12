# Cross-Domain Identifier Handling Guide

## Problem: Missing or Unknown Identifiers

When XACT and ClaimX don't know about each other's identifiers, task creation can fail.

## Scenarios

### Scenario 1: Project Not Yet Created in ClaimX

```
Timeline:
  1. XACT estimate created → claim_number: "ABC123456"
  2. XACT event triggers plugin → needs ClaimX project_id
  3. ClaimX project doesn't exist yet → resolution fails (404)
  4. Task creation fails ❌
```

### Scenario 2: Claim Number Not Set in ClaimX

```
Timeline:
  1. ClaimX project created manually → project_id: 12345
  2. Claim number field empty or different format
  3. XACT event arrives with claim_number: "ABC123456"
  4. Resolution API returns 404
  5. Task creation fails ❌
```

### Scenario 3: Format Mismatch

```
XACT claim:  "ABC-123456" or "ABC123456"
ClaimX uses: "123456" (stripped) or "abc123456" (lowercase)

Resolution API returns 404
Task creation fails ❌
```

## Solutions

### Solution 1: Retry with Exponential Backoff

**When to use:** Project will be created soon, just needs time

```python
from pipeline.plugins.shared.base import Plugin, PluginResult, PluginContext

class RetryableXACTTaskPlugin(Plugin):
    """
    Creates ClaimX task from XACT event with retry logic.
    """

    # Configure retry behavior
    default_config = {
        "max_retries": 3,
        "retry_delay_seconds": 30,  # Wait 30s before retry
    }

    async def execute(self, context: PluginContext) -> PluginResult:
        claim_number = context.message.claim_number

        # Try to create task
        # If resolution fails, the action handler will skip
        # But we can detect this and publish to a retry topic

        task_data = {
            "customTaskName": "Review Estimate",
            "customTaskId": 789,
            "notificationType": "COPY_EXTERNAL_LINK_URL",
        }

        # Create multiple actions: try task + publish for retry if needed
        actions = []

        # Action 1: Try to create task (may fail if project doesn't exist)
        actions.append(
            PluginAction(
                action_type=ActionType.CREATE_CLAIMX_TASK,
                params={
                    "claim_number": claim_number,
                    "task_type": "CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
                    "task_data": task_data,
                }
            )
        )

        # Action 2: If this is a retry, log it
        retry_count = context.headers.get("x-retry-count", "0")
        if int(retry_count) > 0:
            actions.append(
                PluginAction(
                    action_type=ActionType.LOG,
                    params={
                        "level": "info",
                        "message": f"Retry {retry_count} for claim {claim_number}"
                    }
                )
            )

        return PluginResult(success=True, actions=actions)
```

**Infrastructure Setup (Kafka):**

```yaml
# Create a retry topic with delayed consumer
topics:
  - name: xact.task-creation.retry
    partitions: 6
    replication: 3

# Consumer configuration
consumers:
  - name: xact_task_retry_consumer
    topic: xact.task-creation.retry
    # Delay processing using poll interval
    poll_interval_ms: 30000  # 30 seconds between attempts
```

### Solution 2: Dead Letter Queue with Manual Review

**When to use:** Persistent failures requiring investigation

```python
class DLQXACTTaskPlugin(Plugin):
    """
    Sends failed task creations to DLQ for manual review.
    """

    async def execute(self, context: PluginContext) -> PluginResult:
        claim_number = context.message.claim_number

        # Try to resolve first (using cache)
        from pipeline.plugins.shared.base import resolve_claimx_project_id

        project_id = await resolve_claimx_project_id(
            claim_number=claim_number,
            connection_manager=self.connection_manager
        )

        if not project_id:
            # Send to DLQ for manual review
            return PluginResult.publish(
                topic="xact.task-creation.dlq",
                payload={
                    "claim_number": claim_number,
                    "event_type": context.event_type,
                    "event_id": context.event_id,
                    "timestamp": context.timestamp.isoformat(),
                    "reason": "ClaimX project not found",
                    "original_message": context.message.dict(),
                },
                headers={
                    "x-dlq-reason": "project-not-found",
                    "x-claim-number": claim_number,
                }
            )

        # Project found, create task
        return PluginResult.create_claimx_task(
            project_id=project_id,
            task_type="CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
            task_data={
                "customTaskName": "Review Estimate",
                "customTaskId": 789,
                "notificationType": "COPY_EXTERNAL_LINK_URL",
            }
        )
```

### Solution 3: Format Normalization

**When to use:** Known format mismatches between systems

```python
class NormalizedXACTTaskPlugin(Plugin):
    """
    Tries multiple claim number formats to find ClaimX project.
    """

    async def execute(self, context: PluginContext) -> PluginResult:
        claim_number_raw = context.message.claim_number

        # Try multiple formats
        formats_to_try = [
            claim_number_raw,                          # As-is
            claim_number_raw.upper(),                  # Uppercase
            claim_number_raw.lower(),                  # Lowercase
            claim_number_raw.replace("-", ""),         # No dashes
            claim_number_raw.replace("-", "").upper(), # No dashes, uppercase
            claim_number_raw.lstrip("0"),              # Strip leading zeros
        ]

        from pipeline.plugins.shared.base import resolve_claimx_project_id

        project_id = None
        format_used = None

        for claim_format in formats_to_try:
            project_id = await resolve_claimx_project_id(
                claim_number=claim_format,
                connection_manager=self.connection_manager
            )

            if project_id:
                format_used = claim_format
                self._logger.info(
                    "Found ClaimX project with normalized format",
                    extra={
                        "original_claim": claim_number_raw,
                        "normalized_claim": claim_format,
                        "project_id": project_id,
                    },
                )
                break

        if not project_id:
            return PluginResult.skip(
                f"ClaimX project not found for claim {claim_number_raw} (tried {len(formats_to_try)} formats)"
            )

        # Create task with resolved project_id
        return PluginResult.create_claimx_task(
            project_id=project_id,
            task_type="CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
            task_data={
                "customTaskName": "Review Estimate",
                "customTaskId": 789,
                "notificationType": "COPY_EXTERNAL_LINK_URL",
            }
        )
```

### Solution 4: Event-Driven Synchronization

**When to use:** Need reliable eventual consistency

```
Architecture:

  XACT Domain                ClaimX Domain
  ┌──────────┐              ┌──────────┐
  │ Estimate │              │ Project  │
  │ Created  │              │ Created  │
  └─────┬────┘              └─────┬────┘
        │                         │
        ├─► Kafka Topic ◄──────────┤
        │   "project.sync"         │
        │                         │
  ┌─────▼────────────────────────▼────┐
  │   Sync Worker (watches both)      │
  │   - Matches claim_number           │
  │   - Updates mapping table          │
  │   - Retries pending tasks          │
  └───────────────────────────────────┘
                  │
          ┌───────▼────────┐
          │  Mapping Table  │
          │  claim → project│
          └────────────────┘
```

**Implementation:**

```python
# Mapping table (Delta Lake or PostgreSQL)
CREATE TABLE claim_project_mapping (
    claim_number VARCHAR(50) PRIMARY KEY,
    project_id BIGINT NOT NULL,
    source VARCHAR(20),  -- 'xact', 'claimx', 'manual'
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    INDEX idx_project_id (project_id)
);

# Sync worker
class ClaimProjectSyncWorker:
    """
    Syncs claim_number ↔ project_id mappings and retries pending tasks.
    """

    async def handle_xact_project_created(self, message):
        """XACT project created - store claim number."""
        claim_number = message.claim_number

        # Check if ClaimX project already exists
        project_id = await self.db.get_project_id(claim_number)

        if not project_id:
            # Store as pending
            await self.db.store_pending_mapping(
                claim_number=claim_number,
                source='xact'
            )
        else:
            # Already synced - retry any pending tasks
            await self.retry_pending_tasks(claim_number)

    async def handle_claimx_project_created(self, message):
        """ClaimX project created - link to claim number if available."""
        project_id = message.project_id
        claim_number = message.metadata.get('claim_number')  # If available

        if claim_number:
            # Store mapping
            await self.db.store_mapping(
                claim_number=claim_number,
                project_id=project_id,
                source='claimx'
            )

            # Invalidate cache to force refresh
            cache = get_claim_project_cache()
            await cache.invalidate(claim_number)

            # Retry any pending tasks for this claim
            await self.retry_pending_tasks(claim_number)
```

### Solution 5: Conditional Task Creation

**When to use:** Tasks only needed when both systems are synced

```python
class ConditionalXACTTaskPlugin(Plugin):
    """
    Only creates task if ClaimX project definitely exists.
    """

    async def execute(self, context: PluginContext) -> PluginResult:
        claim_number = context.message.claim_number

        # Check if mapping exists (from sync worker)
        project_id = await self.mapping_service.get_project_id(claim_number)

        if not project_id:
            # Not synced yet - log and skip
            return PluginResult(
                success=True,
                message=f"Skipping task creation - ClaimX project not synced for {claim_number}",
                actions=[
                    PluginAction(
                        action_type=ActionType.LOG,
                        params={
                            "level": "info",
                            "message": f"ClaimX project not synced for claim {claim_number}"
                        }
                    ),
                    # Optionally: publish to metrics
                    PluginAction(
                        action_type=ActionType.METRIC,
                        params={
                            "name": "claimx_task_skipped_not_synced",
                            "labels": {"claim_number": claim_number}
                        }
                    )
                ]
            )

        # Synced - create task
        return PluginResult.create_claimx_task(
            project_id=project_id,
            task_type="CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
            task_data={...}
        )
```

## Recommended Hybrid Approach

Combine multiple strategies:

```python
class RobustXACTTaskPlugin(Plugin):
    """
    Robust task creation with multiple fallback strategies.
    """

    async def execute(self, context: PluginContext) -> PluginResult:
        claim_number = context.message.claim_number
        retry_count = int(context.headers.get("x-retry-count", "0"))

        # Strategy 1: Try cache first (fastest)
        from pipeline.plugins.shared.base import resolve_claimx_project_id
        project_id = await resolve_claimx_project_id(
            claim_number=claim_number,
            connection_manager=self.connection_manager
        )

        if not project_id:
            # Strategy 2: Try normalized formats
            for normalized in self._get_normalized_formats(claim_number):
                project_id = await resolve_claimx_project_id(
                    claim_number=normalized,
                    connection_manager=self.connection_manager
                )
                if project_id:
                    break

        if not project_id:
            # Strategy 3: Retry logic (up to 3 times, 30s apart)
            if retry_count < 3:
                return PluginResult.publish(
                    topic="xact.task-creation.retry",
                    payload=context.message.dict(),
                    headers={
                        "x-retry-count": str(retry_count + 1),
                        "x-claim-number": claim_number,
                        "x-retry-after": "30",  # seconds
                    }
                )
            else:
                # Strategy 4: Send to DLQ after max retries
                return PluginResult.publish(
                    topic="xact.task-creation.dlq",
                    payload={
                        "claim_number": claim_number,
                        "event": context.message.dict(),
                        "reason": "ClaimX project not found after 3 retries",
                    }
                )

        # Success - create task
        return PluginResult.create_claimx_task(
            project_id=project_id,
            task_type="CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
            task_data={
                "customTaskName": "Review Estimate",
                "customTaskId": 789,
                "notificationType": "COPY_EXTERNAL_LINK_URL",
            }
        )

    def _get_normalized_formats(self, claim_number: str) -> List[str]:
        """Generate common format variations."""
        return [
            claim_number.upper(),
            claim_number.lower(),
            claim_number.replace("-", ""),
            claim_number.replace("-", "").upper(),
            claim_number.lstrip("0"),
        ]
```

## Monitoring & Alerting

Track these metrics:

```python
# In plugin or worker
metrics.counter("claimx_task_creation_attempts_total", labels={"domain": "xact"})
metrics.counter("claimx_task_creation_failures_total", labels={"reason": "project_not_found"})
metrics.counter("claimx_project_resolution_cache_hits_total")
metrics.counter("claimx_project_resolution_cache_misses_total")
metrics.histogram("claimx_project_resolution_duration_seconds")
```

Alert on:
- High rate of DLQ messages
- Low cache hit rate (< 80%)
- Increasing retry queue depth

## Configuration

```yaml
# config/plugins/xact/task_creation/config.yaml
plugins:
  - name: xact_task_creator
    class: RobustXACTTaskPlugin
    enabled: true
    config:
      max_retries: 3
      retry_delay_seconds: 30
      enable_format_normalization: true
      dlq_topic: xact.task-creation.dlq
      retry_topic: xact.task-creation.retry

      # Format normalization rules
      normalization_rules:
        - uppercase
        - remove_dashes
        - strip_leading_zeros
```

## Testing

```python
async def test_project_not_found_retries():
    """Test retry behavior when project doesn't exist."""

    # Mock: project doesn't exist initially
    mock_cm = AsyncMock()
    mock_cm.request.return_value.status = 404

    plugin = RobustXACTTaskPlugin(config={"max_retries": 3})
    result = await plugin.execute(context)

    # Should publish to retry topic
    assert len(result.actions) == 1
    assert result.actions[0].action_type == ActionType.PUBLISH_TO_TOPIC
    assert result.actions[0].params["topic"] == "xact.task-creation.retry"

    # Check retry count incremented
    assert result.actions[0].params["headers"]["x-retry-count"] == "1"
```
