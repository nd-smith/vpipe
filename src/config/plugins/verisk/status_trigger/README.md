# XACT Status Trigger Plugin

## Overview

The XACT Status Trigger Plugin monitors XACT assignment events and triggers actions when specific status changes occur. This enables downstream systems to react to important status transitions in real-time.

**Supported Actions (all fully implemented):**
- **Kafka Publishing** - Send notifications to Kafka topics
- **HTTP Webhooks** - Call external APIs (direct URLs or named connections)
- **Logging** - Write structured log messages
- **Headers** - Add headers to context for downstream plugins
- **Metrics** - Emit custom metrics
- **Filter** - Terminate pipeline processing

## Use Cases

- **Payment Processing Integration**: Trigger external payment workflows when status reaches "paymentProcessorAssigned"
- **Document Processing**: Notify document processing systems when "documentsReceived"
- **Workflow Automation**: Trigger custom workflows based on any status change
- **External System Integration**: Send notifications to third-party systems for specific statuses
- **Analytics & Monitoring**: Stream status change events to analytics pipelines

## How It Works

1. The EventIngester creates enrichment tasks for all XACT events
2. The EnrichmentWorker processes the task and executes plugins at the ENRICHMENT_COMPLETE stage
3. This plugin checks the event's `status_subtype` field
4. If the status matches a configured trigger, the plugin publishes a notification to the specified Kafka topic
5. The enrichment continues normally, creating download tasks for any attachments

## Configuration

Edit `config.yaml` to add or modify status triggers. Each trigger can specify multiple actions:

```yaml
plugin:
  enabled: true
  config:
    triggers:
      # Example with multiple actions
      - status: "paymentProcessorAssigned"
        publish_to_topic: "xact.notifications.payment-processor-assigned"
        webhook:
          url: "https://api.example.com/notify"
          method: "POST"
        log:
          level: info
          message: "Payment processor assigned"

      # Example with single action
      - status: "estimateCreated"
        publish_to_topic: "xact.notifications.estimate-created"
```

## Action Types

### 1. Kafka Publishing (`publish_to_topic`)

Publish a notification message to a Kafka topic:

```yaml
- status: "paymentProcessorAssigned"
  publish_to_topic: "xact.notifications.payment-assigned"
```

The plugin will publish a JSON payload with event details to the specified topic.

### 2. HTTP Webhook (`webhook`)

Call an external HTTP endpoint:

```yaml
- status: "estimateCreated"
  webhook:
    url: "https://api.example.com/webhooks/estimate"
    method: "POST"  # Optional, defaults to POST
    headers:        # Optional
      Authorization: "Bearer YOUR_TOKEN"
      Content-Type: "application/json"
    timeout: 30     # Optional, defaults to 30 seconds
```

Shorthand for simple webhooks:
```yaml
- status: "estimateCreated"
  webhook: "https://api.example.com/webhooks/estimate"
```

### 3. Logging (`log`)

Write a structured log message:

```yaml
- status: "documentsReceived"
  log:
    level: info  # debug, info, warning, error
    message: "Documents received for assignment {assignment_id}"
```

Log messages support variable substitution:
- `{status}` - Status subtype
- `{event_id}` - Event ID
- `{trace_id}` - Trace ID
- `{assignment_id}` - Assignment ID
- `{task_status}` - Task status

Shorthand for simple log messages:
```yaml
- status: "documentsReceived"
  log: "Documents received"
```

### 4. Other Action Types

The plugin also supports additional action types available in the base infrastructure:

**Add Header:**
```yaml
- status: "statusChange"
  add_header:
    key: "x-custom-header"
    value: "custom-value"
```

**Emit Metric:**
```yaml
- status: "statusChange"
  metric:
    name: "xact_status_changes"
    value: 1
    labels:
      status: "{status}"
```

**Filter (Terminate Pipeline):**
```yaml
- status: "doNotProcess"
  filter: true  # Stops pipeline processing for this event
```

### Combining Multiple Actions

You can combine any number of actions in a single trigger:

```yaml
- status: "paymentProcessorAssigned"
  # Notify via Kafka
  publish_to_topic: "xact.notifications.ppa"

  # Call external API
  webhook: "https://api.example.com/notify"

  # Log for monitoring
  log:
    level: info
    message: "PPA notification sent | assignment={assignment_id}"
```

### Adding New Triggers

To trigger on a new status:

1. Add a new entry to the `triggers` list in `config.yaml`
2. Specify the exact `status_subtype` value to match (case-sensitive)
3. Define one or more actions (Kafka, webhook, log, or custom)
4. Restart the enrichment worker to load the new configuration

Example:
```yaml
triggers:
  - status: "inspectionScheduled"
    publish_to_topic: "xact.notifications.inspection-scheduled"
    webhook: "https://api.example.com/inspection"
    log:
      level: info
      message: "Inspection scheduled for {trace_id}"
```

## Notification Payload

When a trigger matches, the plugin publishes a message to the target Kafka topic with the following structure:

```json
{
  "event_id": "evt_12345",
  "trace_id": "abc123-def456",
  "event_type": "xact",
  "status_subtype": "paymentProcessorAssigned",
  "assignment_id": "A12345",
  "estimate_version": "1.0",
  "attachment_count": 3,
  "timestamp": "2024-12-25T10:30:00Z",
  "domain": "xact",
  "stage": "enrichment_complete",
  "metadata": {
    "plugin_name": "xact_status_trigger",
    "plugin_version": "1.0.0",
    "triggered_at": "2024-12-25T10:30:01Z"
  }
}
```

## Common Status Values

Here are common XACT status subtypes you might want to trigger on:

- `paymentProcessorAssigned` - Payment processor has been assigned
- `estimateCreated` - Estimate has been created
- `documentsReceived` - Documents have been received
- `inspectionScheduled` - Inspection has been scheduled
- `inspectionCompleted` - Inspection has been completed
- `estimateApproved` - Estimate has been approved
- `assignmentCompleted` - Assignment is complete

**Note:** Status values are case-sensitive and must match exactly as they appear in the events.

## Testing

To test this plugin:

1. Configure a trigger for a status you expect to see
2. Create the target Kafka topic if it doesn't exist
3. Start the enrichment worker
4. Monitor the enrichment worker logs for plugin execution:
   ```
   Status trigger matched: paymentProcessorAssigned -> xact.notifications.payment-processor-assigned
   ```
5. Consume from the target Kafka topic to see the notifications

## Disabling the Plugin

To disable this plugin without removing it:

Set `enabled: false` in `config.yaml`:

```yaml
plugin:
  enabled: false
```

## Architecture

```
EventIngester → enrichment.pending → EnrichmentWorker
                                           ↓
                                    [Execute Plugins]
                                           ↓
                                    XACTStatusTriggerPlugin
                                           ↓
                                    (if status matches)
                                           ↓
                                    Publish to target topic
                                           ↓
                                    Continue pipeline → downloads.pending
```

## Plugin Priority

This plugin runs at priority `100` (standard priority) during the `ENRICHMENT_COMPLETE` stage. Plugins with lower priority numbers run first.

To change the execution order, modify the `priority` field in the plugin class:

```python
priority = 50  # Run earlier (lower number = earlier)
priority = 150  # Run later (higher number = later)
```

## Advanced Usage

### Filtering by Event Type

The plugin currently matches all event types (`event_types = ["*"]`). To restrict to specific event types:

```python
event_types = ["xact"]  # Only XACT events
```

### Custom Payload Transformation

To customize the notification payload, modify the `_build_notification_payload` method in the plugin class.

### Conditional Logic

To add more complex matching logic, override the `should_run` method:

```python
def should_run(self, context: PluginContext) -> bool:
    if not super().should_run(context):
        return False

    # Only run for assignments with attachments
    if context.data.get("attachment_count", 0) == 0:
        return False

    return True
```

## Troubleshooting

### Plugin not triggering

1. Check that the plugin is enabled in `config.yaml`
2. Verify the status value matches exactly (case-sensitive)
3. Check enrichment worker logs for plugin loading messages
4. Ensure the enrichment worker is running and consuming from `xact.enrichment.pending`

### Notifications not appearing in target topic

1. Verify the target Kafka topic exists
2. Check for producer errors in the enrichment worker logs
3. Verify Kafka connectivity and permissions

### Plugin errors

Check the enrichment worker logs for plugin execution errors:
```
Plugin execution failed | event_id=evt_12345 | error=...
```

## License

This plugin is part of the pcesdopodappv1 project.
