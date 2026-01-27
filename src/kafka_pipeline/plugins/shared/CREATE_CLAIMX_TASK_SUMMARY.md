# CREATE_CLAIMX_TASK Action - Implementation Summary

## Overview

Added a new `CREATE_CLAIMX_TASK` action type to the plugin system that allows plugins to create ClaimX tasks via the `/import/project/actions` API endpoint. Supports both ClaimX and XACT domain events through automatic claim number resolution.

## Files Modified

### 1. `kafka_pipeline/plugins/shared/base.py`

**Added:**
- `ActionType.CREATE_CLAIMX_TASK` enum value
- `PluginResult.create_claimx_task()` factory method
- `resolve_claimx_project_id()` helper function for manual resolution

**Key Features:**
- Accepts either `project_id` (for ClaimX domain) or `claim_number` (for XACT domain)
- Validates that at least one identifier is provided
- Returns `PluginResult` with `CREATE_CLAIMX_TASK` action

### 2. `kafka_pipeline/plugins/shared/registry.py`

**Added:**
- Action routing for `CREATE_CLAIMX_TASK` in `ActionExecutor.execute()`
- `ActionExecutor._create_claimx_task()` handler method

**Key Features:**
- Automatically resolves ClaimX project ID from claim number if needed
- Makes API call: `GET /export/project/projectId?projectNumber={claim_number}`
- Builds proper API payload structure
- Creates task via `POST /import/project/actions`
- Comprehensive error handling and logging

### 3. `kafka_pipeline/claimx/api_client.py`

**Added:**
- `ClaimXApiClient.get_project_id_by_claim_number()` method

**Purpose:**
- Provides a typed method for resolving project IDs in the API client
- Used by other parts of the system that need this functionality

### 4. Documentation Files

**Created:**
- `CREATE_CLAIMX_TASK_EXAMPLE.md` - Usage examples and reference
- `CREATE_CLAIMX_TASK_SUMMARY.md` - This file

## API Structure

### Input (to action)
```python
PluginResult.create_claimx_task(
    claim_number="ABC123456",  # OR project_id=12345
    task_type="CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
    task_data={
        "customTaskName": "Review Documentation",
        "customTaskId": 456,
        "notificationType": "COPY_EXTERNAL_LINK_URL",
    },
    use_primary_contact_as_sender=True,  # optional, default True
    sender_username="system@company.com",  # optional
    connection="claimx_api",  # optional, default "claimx_api"
)
```

### Output (API payload)
```json
{
    "projectId": 12345,
    "usePrimaryContactAsSender": true,
    "senderUserName": "system@company.com",
    "type": "CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
    "data": {
        "customTaskName": "Review Documentation",
        "customTaskId": 456,
        "notificationType": "COPY_EXTERNAL_LINK_URL"
    }
}
```

## Cross-Domain Support

### Terminology
- **Internally (our codebase):** Use `claim_number`
- **API calls:** Use `projectNumber` parameter (API's naming convention)
- **Result:** ClaimX `project_id` (integer)

### Resolution Flow

When `claim_number` is provided instead of `project_id`:

1. Action handler detects `claim_number` parameter
2. Makes API call: `GET /export/project/projectId?projectNumber={claim_number}`
3. Parses response (may be int or dict with "projectId" or "id" key)
4. Uses resolved `project_id` for task creation
5. Proceeds with `POST /import/project/actions`

### Use Cases

**ClaimX Domain Events:**
- Have `project_id` directly available in context
- Pass `project_id` directly to action

**XACT Domain Events:**
- Have `claim_number` available in context
- Pass `claim_number` to action
- Action automatically resolves to `project_id`

## Usage Patterns

### Pattern 1: Direct (ClaimX Domain)
```python
return PluginResult.create_claimx_task(
    project_id=context.project_id,
    task_type="CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
    task_data={...}
)
```

### Pattern 2: Cross-Domain (XACT → ClaimX)
```python
return PluginResult.create_claimx_task(
    claim_number=context.message.claim_number,
    task_type="CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
    task_data={...}
)
```

### Pattern 3: Manual Resolution
```python
project_id = await resolve_claimx_project_id(
    claim_number=context.message.claim_number,
    connection_manager=self.connection_manager
)

if not project_id:
    return PluginResult.skip("ClaimX project not found")

return PluginResult.create_claimx_task(
    project_id=project_id,
    task_type="CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
    task_data={...}
)
```

## Configuration

Uses existing `claimx_api` connection from `config/plugins/shared/connections/claimx.yaml`:

```yaml
connections:
  claimx_api:
    name: claimx_api
    base_url: ${CLAIMX_API_BASE_PATH}
    auth_type: basic
    auth_token: ${CLAIMX_API_TOKEN}
    timeout_seconds: 30
    max_retries: 3
    retry_backoff_base: 2
    retry_backoff_max: 60
```

Environment variables required:
- `CLAIMX_API_BASE_PATH`: ClaimX API base URL
- `CLAIMX_API_TOKEN`: Basic auth token

## Error Handling

### Validation Errors
- Missing connection manager → ERROR log, action skipped
- Missing project_id/claim_number → ERROR log, action skipped
- Missing task_type → ERROR log, action skipped
- Missing task_data → ERROR log, action skipped

### Resolution Errors
- Claim number not found → ERROR log, action skipped
- API error during resolution → ERROR log, exception raised
- Invalid response format → ERROR log, action skipped

### Task Creation Errors
- HTTP 4xx/5xx status → ERROR log with response body
- Connection errors → ERROR log, exception raised
- Timeout → ERROR log, exception raised

## Logging

### INFO Level
- Starting project ID resolution
- Successfully resolved project ID
- Creating ClaimX task
- Task created successfully

### ERROR Level
- Configuration issues (missing connection manager)
- Validation failures (missing parameters)
- API errors (HTTP errors, connection errors)
- Resolution failures (claim number not found)

### DEBUG Level
- (via ConnectionManager) Request/response details
- Retry attempts
- Circuit breaker status

## Testing Considerations

### Unit Tests
- Mock ConnectionManager
- Test both project_id and claim_number paths
- Test error scenarios (not found, API errors, etc.)
- Verify API payload structure

### Integration Tests
- Test with actual ClaimX API (dev/staging environment)
- Verify claim number resolution works
- Verify task creation succeeds
- Test retry behavior on transient failures

### Example Test
```python
from unittest.mock import AsyncMock, MagicMock

async def test_create_claimx_task_with_claim_number():
    mock_cm = AsyncMock()

    # Mock resolution response
    resolve_response = AsyncMock()
    resolve_response.status = 200
    resolve_response.json = AsyncMock(return_value={"projectId": 12345})

    # Mock task creation response
    create_response = AsyncMock()
    create_response.status = 200

    mock_cm.request.side_effect = [resolve_response, create_response]

    executor = ActionExecutor(connection_manager=mock_cm)

    action = PluginAction(
        action_type=ActionType.CREATE_CLAIMX_TASK,
        params={
            "claim_number": "ABC123456",
            "task_type": "CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
            "task_data": {"customTaskName": "Test", "customTaskId": 456}
        }
    )

    await executor.execute(action, mock_context)

    # Verify resolution call
    assert mock_cm.request.call_count == 2
    assert mock_cm.request.call_args_list[0][1]["path"] == "/export/project/projectId"
    assert mock_cm.request.call_args_list[0][1]["params"]["projectNumber"] == "ABC123456"

    # Verify task creation call
    assert mock_cm.request.call_args_list[1][1]["path"] == "/import/project/actions"
    assert mock_cm.request.call_args_list[1][1]["json"]["projectId"] == 12345
```

## Future Enhancements

1. **Caching:** Cache claim number → project ID mappings to reduce API calls
2. **Bulk Operations:** Support creating multiple tasks in a single action
3. **Task Templates:** Pre-defined task configurations by name
4. **Validation:** Validate task_data structure against task_type requirements
5. **Response Parsing:** Parse and return task ID from creation response

## Related Files

- `kafka_pipeline/plugins/shared/base.py` - Core action types and factories
- `kafka_pipeline/plugins/shared/registry.py` - Action execution
- `kafka_pipeline/plugins/shared/connections.py` - Connection management
- `kafka_pipeline/claimx/api_client.py` - ClaimX API client
- `config/plugins/shared/connections/claimx.yaml` - Connection config
- `CREATE_CLAIMX_TASK_EXAMPLE.md` - Usage examples
