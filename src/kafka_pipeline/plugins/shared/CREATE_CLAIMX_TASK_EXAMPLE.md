# CREATE_CLAIMX_TASK Action Example

This document shows how to use the `CREATE_CLAIMX_TASK` action type to create ClaimX tasks from plugins.

## API Endpoint

- **Endpoint:** `POST /import/project/actions`
- **Connection:** Uses the `claimx_api` connection from `config/plugins/shared/connections/claimx.yaml`
- **Authentication:** Basic auth with token from `CLAIMX_API_TOKEN` env var

## Required Payload Structure

```json
{
    "projectId": 12345,
    "usePrimaryContactAsSender": true,
    "senderUserName": "Allstate",
    "type": "CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
    "data": {
        "customTaskName": "Review Documentation",
        "customTaskId": 456,
        "notificationType": "COPY_EXTERNAL_LINK_URL"
    }
}
```

## Usage in Plugin

### Example 1: Basic Task Creation (ClaimX Domain)

When triggered by ClaimX events, the project ID is available in context:

```python
from kafka_pipeline.plugins.shared.base import Plugin, PluginResult, PluginContext, Domain

class MyTaskCreatorPlugin(Plugin):
    name = "my_task_creator"
    domains = [Domain.CLAIMX]

    async def execute(self, context: PluginContext) -> PluginResult:
        # Extract project ID from context
        project_id = context.project_id

        # Build task data
        task_data = {
            "customTaskName": "Review Photos",
            "customTaskId": 456,
            "notificationType": "COPY_EXTERNAL_LINK_URL",
        }

        # Create the task with project_id
        return PluginResult.create_claimx_task(
            project_id=int(project_id),
            task_type="CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
            task_data=task_data,
        )
```

### Example 1b: Cross-Domain Task Creation (XACT → ClaimX)

When triggered by XACT events, use claim_number instead of project_id:

```python
from kafka_pipeline.plugins.shared.base import Plugin, PluginResult, PluginContext, Domain

class MyXACTTaskCreatorPlugin(Plugin):
    name = "xact_task_creator"
    domains = [Domain.XACT]

    async def execute(self, context: PluginContext) -> PluginResult:
        # Extract claim number from XACT message
        claim_number = context.message.claim_number

        # Build task data
        task_data = {
            "customTaskName": "Review Estimate",
            "customTaskId": 789,
            "notificationType": "COPY_EXTERNAL_LINK_URL",
        }

        # Create the task with claim_number (automatically resolves to project_id)
        return PluginResult.create_claimx_task(
            claim_number=claim_number,
            task_type="CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
            task_data=task_data,
        )
```

**How it works:** The action handler automatically calls `GET /export/project/projectId?projectNumber={claim_number}` to resolve the ClaimX project ID before creating the task. Note that internally we use `claim_number`, but the API parameter is `projectNumber`.

### Example 2: Manual Project ID Resolution

For advanced use cases, you can manually resolve the project ID:

```python
from kafka_pipeline.plugins.shared.base import (
    Plugin, PluginResult, PluginContext, Domain, resolve_claimx_project_id
)

class MyAdvancedXACTPlugin(Plugin):
    name = "advanced_xact_plugin"
    domains = [Domain.XACT]

    def __init__(self, config=None):
        super().__init__(config)
        # Assume connection_manager is injected or available
        self.connection_manager = None  # Set in on_load()

    async def execute(self, context: PluginContext) -> PluginResult:
        claim_number = context.message.claim_number

        # Manually resolve the project ID
        project_id = await resolve_claimx_project_id(
            claim_number=claim_number,
            connection_manager=self.connection_manager
        )

        if not project_id:
            return PluginResult.skip(
                f"ClaimX project not found for claim {claim_number}"
            )

        # Do additional logic with the resolved project_id
        # ...

        # Then create the task
        task_data = {
            "customTaskName": "Custom Task",
            "customTaskId": 123,
            "notificationType": "COPY_EXTERNAL_LINK_URL",
        }

        return PluginResult.create_claimx_task(
            project_id=project_id,  # Use the resolved ID
            task_type="CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
            task_data=task_data,
        )
```

### Example 3: With Custom Sender

```python
async def execute(self, context: PluginContext) -> PluginResult:
    task_data = {
        "customTaskName": "Complete Inspection",
        "customTaskId": 789,
        "notificationType": "COPY_EXTERNAL_LINK_URL",
    }

    return PluginResult.create_claimx_task(
        project_id=12345,
        task_type="CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
        task_data=task_data,
        use_primary_contact_as_sender=False,
        sender_username="system@company.com",
    )
```

### Example 4: Multiple Actions (Task + Webhook)

```python
async def execute(self, context: PluginContext) -> PluginResult:
    task_data = {
        "customTaskName": "Review Documentation",
        "customTaskId": 456,
        "notificationType": "COPY_EXTERNAL_LINK_URL",
    }

    # Create multiple actions
    actions = [
        # Create the task
        PluginAction(
            action_type=ActionType.CREATE_CLAIMX_TASK,
            params={
                "connection": "claimx_api",
                "project_id": 12345,
                "task_type": "CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
                "task_data": task_data,
            }
        ),
        # Send notification webhook
        PluginAction(
            action_type=ActionType.HTTP_WEBHOOK,
            params={
                "connection": "notification_service",
                "path": "/notify",
                "method": "POST",
                "body": {
                    "event": "task_created",
                    "project_id": 12345,
                    "task_name": "Review Documentation",
                }
            }
        ),
    ]

    return PluginResult(
        success=True,
        actions=actions,
    )
```

## Available Task Types

The `type` field in the API can be any ClaimX action type. Common examples:

- `CUSTOM_TASK_ASSIGN_EXTERNAL_LINK` - Assign task with external link
- `CUSTOM_TASK_ASSIGN` - Assign custom task
- `CUSTOM_TASK_COMPLETE` - Mark task as complete
- _(Add other types as discovered)_

## Task Data Fields

The `data` object contains task-specific fields. Required fields vary by task type:

### For CUSTOM_TASK_ASSIGN_EXTERNAL_LINK:
- `customTaskName` (string): Display name of the task
- `customTaskId` (int): ID of the custom task definition
- `notificationType` (string): How to notify (e.g., "COPY_EXTERNAL_LINK_URL")

### Additional Fields (task-type dependent):
- `assigneeEmail` (string): Email of person to assign to
- `dueDate` (string): ISO 8601 date string
- `priority` (string): Task priority
- `description` (string): Task description
- `externalUrl` (string): External link URL
- _(Add other fields as discovered)_

## Error Handling

The action will:
1. Validate required parameters (`project_id`, `task_type`, `task_data`)
2. Log warnings for missing connection manager
3. Make the API request with retry logic (from connection config)
4. Log success/failure with status codes
5. Raise exceptions on failure (plugin orchestrator will handle)

## Configuration

Ensure `claimx_api` connection is configured in `config/plugins/shared/connections/claimx.yaml`:

```yaml
connections:
  claimx_api:
    name: claimx_api
    base_url: ${CLAIMX_API_BASE_PATH}
    auth_type: basic
    auth_token: ${CLAIMX_API_TOKEN}
    timeout_seconds: 30
    max_retries: 3
```

Environment variables:
```bash
export CLAIMX_API_BASE_PATH="https://www.claimxperience.com/service/cxedirest"
export CLAIMX_API_TOKEN="your-basic-auth-token"
```

## Logging

The action logs at different levels:

- **INFO**: Task creation started, task created successfully
- **ERROR**: Missing parameters, API errors, connection failures
- **DEBUG**: (via connection manager) Request/response details

Example log output:
```
INFO: Plugin action: create ClaimX task | connection=claimx_api project_id=12345 task_type=CUSTOM_TASK_ASSIGN_EXTERNAL_LINK
INFO: ClaimX task created successfully | status=200 connection=claimx_api project_id=12345
```

## Testing

To test without making real API calls:
1. Use a mock ConnectionManager in tests
2. Check that the payload structure is correct
3. Verify required fields are present

```python
# In tests
from unittest.mock import AsyncMock, MagicMock

action_executor = ActionExecutor(
    connection_manager=mock_connection_manager
)

# Verify the request was called with correct payload
mock_connection_manager.request.assert_called_once_with(
    connection_name="claimx_api",
    method="POST",
    path="/import/project/actions",
    json={
        "projectId": 12345,
        "usePrimaryContactAsSender": True,
        "type": "CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
        "data": {...}
    }
)
```
