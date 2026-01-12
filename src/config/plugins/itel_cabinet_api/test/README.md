# iTel Cabinet API Test Output

This directory contains test output files generated when the API worker runs in **test mode**.

## Purpose

When `test_mode: true` is enabled in the API worker configuration, instead of sending payloads to the actual iTel Cabinet API, the worker writes them to JSON files in this directory.

This allows you to:
- ✅ Test the complete pipeline end-to-end without needing the actual iTel API
- ✅ Inspect the exact payload structure being generated
- ✅ Validate that form parsing and payload building are working correctly
- ✅ Debug issues before going to production

## File Naming

Files are named with the pattern:
```
payload_{assignmentId}_{timestamp}.json
```

Example:
```
payload_5423723_20260110_153045.json
```

## Enabling Test Mode

In `config/plugins/itel_cabinet_api/workers.yaml`:

```yaml
itel_cabinet_api_worker:
  enrichment_handlers:
    - type: kafka_pipeline.plugins.itel_cabinet_api.handlers.itel_api_sender:ItelApiSender
      config:
        test_mode: true                                           # Enable test mode
        test_output_dir: "config/plugins/itel_cabinet_api/test"  # Output directory
```

## Log Output

When test mode is active, you'll see logs like:

```
[TEST MODE] iTel API request successful (written to file) |
  status=200 |
  assignmentId=5423723 |
  projectId=4895140 |
  file=config/plugins/itel_cabinet_api/test/payload_5423723_20260110_153045.json
```

## Production Mode

To send to actual iTel API, set:

```yaml
test_mode: false
```

## Git Ignore

Test payload files (`*.json`) are ignored by git to avoid committing test data.
