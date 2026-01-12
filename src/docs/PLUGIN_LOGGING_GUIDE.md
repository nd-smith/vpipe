# Plugin Execution Logging Guide

## Overview

This guide explains the enhanced logging for plugin execution to help trace message flow through the pipeline.

## Changes Made

### 1. Enhanced PluginOrchestrator Logging

**Before:** Only logged "Executing plugin" at DEBUG level without event_id
**After:** Complete execution trace with full context at INFO level

### 2. Enhanced ActionExecutor Logging

**Before:** Minimal logging for action execution
**After:** Detailed logging for each action with success/failure tracking

### 3. Enhanced TaskTriggerPlugin Logging

**Before:** Missing assignment_id and task_status
**After:** Full task context including assignment_id, task_status, event_id

## Expected Log Sequence for iTel Cabinet Events

When a `CUSTOM_TASK_COMPLETED` event for task_id 32513 is processed, you should see this exact sequence:

```
1. Processing enrichment task
   event_id=405d39... | event_type=CUSTOM_TASK_COMPLETED | project_id=5395115

2. Executing plugin
   plugin_name=itel_cabinet_api | stage=enrichment_complete |
   event_id=405d39... | event_type=CUSTOM_TASK_COMPLETED | project_id=5395115

3. Task trigger matched
   plugin_name=task_trigger | trigger_name="iTel Cabinet Repair Form Task" |
   task_id=32513 | assignment_id=5423878 | task_name="ITEL Cabinet Repair Form" |
   task_status=COMPLETED | event_id=405d39... | event_type=CUSTOM_TASK_COMPLETED |
   project_id=5395115 | action_count=2

4. Plugin executed
   plugin_name=itel_cabinet_api | success=True |
   message="Triggered: iTel Cabinet Repair Form Task (CUSTOM_TASK_COMPLETED)" |
   actions_count=2 | event_id=405d39... | event_type=CUSTOM_TASK_COMPLETED |
   project_id=5395115

5. Executing plugin action
   plugin_name=itel_cabinet_api | action_type=publish_to_topic |
   event_id=405d39... | event_type=CUSTOM_TASK_COMPLETED | project_id=5395115

6. Plugin action: publish to topic
   topic=itel.cabinet.task.tracking | event_id=405d39... | project_id=5395115 |
   payload_keys=[...list of keys...]

7. Plugin message published to topic successfully
   topic=itel.cabinet.task.tracking | event_id=405d39... |
   event_type=CUSTOM_TASK_COMPLETED | project_id=5395115

8. Plugin action executed successfully
   plugin_name=itel_cabinet_api | action_type=publish_to_topic |
   event_id=405d39... | event_type=CUSTOM_TASK_COMPLETED | project_id=5395115

9. Executing plugin action
   plugin_name=itel_cabinet_api | action_type=log |
   event_id=405d39... | event_type=CUSTOM_TASK_COMPLETED | project_id=5395115

10. Plugin action executed successfully
    plugin_name=itel_cabinet_api | action_type=log |
    event_id=405d39... | event_type=CUSTOM_TASK_COMPLETED | project_id=5395115
```

## Diagnostic Commands

### Find all plugin executions
```bash
grep "Executing plugin" <enrichment_worker_log>
```

### Trace a specific event through plugin execution
```bash
grep "405d39191f29c87710ef18917e41d4ea72be5ff588029727f837ab897394ad3f" <enrichment_worker_log> | grep -E "plugin|Plugin"
```

### Check if plugins are loading at startup
```bash
grep -E "Plugin orchestrator initialized|Loaded plugins from directory|Registered plugin" <enrichment_worker_log>
```

### Find plugin publish actions
```bash
grep "Plugin action: publish to topic" <enrichment_worker_log>
```

### Check for producer issues
```bash
grep "No producer configured" <enrichment_worker_log>
```

### Check for plugin failures
```bash
grep -E "Plugin execution failed|Plugin action execution failed" <enrichment_worker_log>
```

## Troubleshooting Decision Tree

### ❌ No "Executing plugin" messages
**Issue:** Plugins not loading or enrichment worker not running
**Check:**
- Search for "Plugin orchestrator initialized"
- Verify enrichment worker is running
- Check plugins_dir configuration

### ❌ "Executing plugin" but no "Task trigger matched"
**Issue:** Plugin runs but doesn't match the task
**Check:**
- Verify task_id=32513 in entities.tasks
- Check event_type matches exactly "CUSTOM_TASK_COMPLETED"
- Verify plugin trigger configuration

### ❌ "Task trigger matched" but no "Executing plugin action"
**Issue:** Plugin executes but doesn't create actions
**Check:**
- Verify trigger configuration has on_any or on_completed
- Check for exceptions between "Plugin executed" and action execution

### ❌ "Executing plugin action" but no "Plugin action: publish to topic"
**Issue:** Action executor fails before reaching publish
**Check:**
- Look for exceptions between these log lines
- Check action_type is publish_to_topic

### ⚠️ "No producer configured - publish action logged only"
**Issue:** Producer not initialized in enrichment worker
**Check:**
- Search for producer initialization errors at worker startup
- Verify BaseKafkaProducer starts successfully
- Check ActionExecutor receives producer in constructor

### ❌ "Plugin action: publish to topic" but no "published successfully"
**Issue:** Producer.send() fails
**Check:**
- Look for exceptions after publish attempt
- Check Kafka connectivity
- Verify topic exists

### ✅ "Plugin message published to topic successfully"
**Issue:** Plugin works, but messages not reaching consumers
**Check:**
- Verify topic exists: `kafka-topics --list`
- Check messages in topic: `kafka-console-consumer --topic itel.cabinet.task.tracking`
- Verify consumer is reading from correct offset

## Log Levels

- **INFO:** Normal plugin execution flow (all the logs above)
- **WARNING:** "No producer configured" - critical issue
- **ERROR:** Plugin execution failures, action execution failures
- **DEBUG:** (Not used for plugin flow after enhancement)

## Key Fields for Tracing

Every log message now includes:
- `event_id` - Unique identifier for end-to-end tracing
- `event_type` - Type of event (CUSTOM_TASK_COMPLETED, etc.)
- `project_id` - Project identifier
- `plugin_name` - Which plugin is executing
- `task_id` - Task template ID (32513 for iTel)
- `assignment_id` - Task assignment ID
- `action_type` - Type of action being executed

## Example: Full Trace

To see the complete flow for your event:

```bash
grep "405d39191f29c87710ef18917e41d4ea72be5ff588029727f837ab897394ad3f" <enrichment_worker_log> \
  | grep -v "DEBUG"
```

This will show all INFO/WARNING/ERROR logs for that event, including:
1. Event ingestion
2. Handler processing
3. Plugin execution
4. Action execution
5. Entity row production

## Next Steps After Adding Logging

1. **Restart enrichment worker** to pick up the logging changes
2. **Trigger a new test event** for task 32513
3. **Search logs** for your event_id
4. **Identify exactly where** the flow stops

The enhanced logging will pinpoint whether:
- Plugins aren't loading
- Plugin doesn't match the event
- Actions aren't created
- Producer isn't configured
- Kafka publish fails
- Messages published but not consumed
