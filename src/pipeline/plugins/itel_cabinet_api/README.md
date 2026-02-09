# iTel Cabinet API Plugin

Simple, explicit code for processing iTel Cabinet repair form tasks.

## Overview

This plugin processes iTel Cabinet Repair Form (task 32513) events from ClaimX:

1. **Tracking Worker** - Enriches tasks, writes to Delta tables, publishes to API worker
2. **API Worker** - Transforms data and sends to iTel Cabinet API

## Quick Start

### 1. Set Environment Variables

```bash
export CLAIMX_API_URL="https://api.claimx.com"
export CLAIMX_API_TOKEN="your-claimx-token"
export ITEL_CABINET_API_BASE_URL="https://api.itelcabinet.com"
export ITEL_CABINET_API_TOKEN="your-itel-token"
export KAFKA_BOOTSTRAP_SERVERS="localhost:9094"
```

### 2. Start Tracking Worker

```bash
python -m pipeline.plugins.itel_cabinet_api.itel_cabinet_tracking_worker
```

### 3. Start API Worker

Production mode (sends to real API):
```bash
python -m pipeline.plugins.itel_cabinet_api.itel_cabinet_api_worker
```

Dev mode (writes to files):
```bash
python -m pipeline.plugins.itel_cabinet_api.itel_cabinet_api_worker --dev
```

## Architecture

```
ClaimX Plugin
     ↓
Kafka Topic: itel.cabinet.task.tracking
     ↓
Tracking Worker
     ├→ Enriches COMPLETED tasks (fetches from ClaimX API)
     ├→ Parses cabinet form data
     ├→ Writes to Delta tables (always)
     └→ Publishes to itel.cabinet.completed (if COMPLETED)
          ↓
     API Worker
          ├→ Transforms to iTel API format
          └→ Sends to iTel Cabinet API
```

## Code Structure

### Core Files

- **`models.py`** - Typed data structures (`TaskEvent`, `CabinetSubmission`, `CabinetAttachment`)
- **`parsers.py`** - Form parsing functions (`parse_cabinet_form()`, `parse_cabinet_attachments()`)
- **`pipeline.py`** - Main processing logic (`ItelCabinetPipeline`)
- **`delta.py`** - Delta table writer (`ItelCabinetDeltaWriter`)
- **`itel_cabinet_tracking_worker.py`** - Tracking worker implementation
- **`itel_cabinet_api_worker.py`** - API worker implementation

### Configuration

- **`config/plugins/claimx/itel_cabinet_api/workers.yaml`** - Worker configuration (topics, tables, settings)
- **`config/plugins/shared/connections/claimx.yaml`** - ClaimX API connection
- **`config/plugins/shared/connections/app.itel.yaml`** - iTel API connection

## How to Understand the Code

### Read These Files in Order:

1. **`models.py`** - See what data structures we use
2. **`pipeline.py:process()`** - Read the main flow top-to-bottom (20 lines)
3. **`parsers.py`** - See how we parse ClaimX responses
4. **`itel_cabinet_tracking_worker.py:main()`** - See how the worker is wired up

### Key Methods to Know:

**Tracking Worker:**
- `ItelCabinetPipeline.process()` - Main entry point (start here!)
- `ItelCabinetPipeline._validate_event()` - Business rules
- `ItelCabinetPipeline._enrich_completed_task()` - Fetch from ClaimX
- `ItelCabinetPipeline._write_to_delta()` - Write to tables
- `ItelCabinetPipeline._publish_for_api()` - Publish to API worker

**API Worker:**
- `ItelCabinetApiWorker.run()` - Main consumer loop
- `ItelCabinetApiWorker._transform_to_api_format()` - Build API payload
- `ItelCabinetApiWorker._send_to_api()` - Send to iTel API

## Debugging

### Set Breakpoints:

```python
# In pipeline.py
async def process(self, raw_message: dict) -> ProcessedTask:
    event = TaskEvent.from_kafka_message(raw_message)  # ← Set breakpoint here
    # Step through to see entire flow
```

### Check Logs:

```bash
# Tracking worker logs
tail -f logs/itel_cabinet_api/*/itel_cabinet_tracking_*.log

# API worker logs
tail -f logs/itel_cabinet_api/*/itel_cabinet_api_*.log
```

### Test Mode:

Run API worker in dev mode to see generated payloads:

```bash
python -m pipeline.plugins.itel_cabinet_api.itel_cabinet_api_worker --dev
# Payloads written to: config/plugins/claimx/itel_cabinet_api/test/
```

## Common Tasks

### Change Validation Rules

Edit `pipeline.py:_validate_event()`:

```python
def _validate_event(self, event: TaskEvent) -> None:
    # Add your validation here
    if event.project_id < 0:
        raise ValueError("Invalid project_id")
```

### Add New Form Fields

1. Add field to `CabinetSubmission` in `models.py`
2. Add mapping to `_question_to_field_name()` in `parsers.py`
3. Add field extraction in `parse_cabinet_form()` in `parsers.py`

### Change API Format

Edit `itel_cabinet_api_worker.py:_transform_to_api_format()`:

```python
def _transform_to_api_format(self, payload: dict) -> dict:
    # Modify API payload structure here
    return {
        "assignmentId": submission.get("assignment_id"),
        # Add/change fields
    }
```

### Skip Enrichment for Certain Tasks

Edit `pipeline.py:process()`:

```python
# Skip enrichment if project_id is in test range
if event.task_status == 'COMPLETED' and event.project_id > 1000:
    submission, attachments = await self._enrich_completed_task(event)
else:
    submission, attachments = None, []
```

## Testing

### Unit Test a Parser:

```python
from pipeline.plugins.itel_cabinet_api.parsers import parse_cabinet_form

def test_parse_cabinet_form():
    task_data = {
        "assignmentId": 12345,
        "projectId": 42,
        # ... full task response
    }

    submission = parse_cabinet_form(task_data, "evt-123")

    assert submission.assignment_id == 12345
    assert submission.project_id == 42
```

### Unit Test the Pipeline:

```python
from pipeline.plugins.itel_cabinet_api.pipeline import ItelCabinetPipeline

async def test_pipeline():
    # Mock dependencies
    mock_connections = Mock()
    mock_delta = Mock()
    mock_kafka = Mock()

    pipeline = ItelCabinetPipeline(
        connection_manager=mock_connections,
        delta_writer=mock_delta,
        kafka_producer=mock_kafka,
        config={},
    )

    # Test with fake message
    message = {
        "event_id": "evt-123",
        "task_id": 32513,
        "task_status": "COMPLETED",
        # ... full message
    }

    result = await pipeline.process(message)

    # Assert expectations
    mock_delta.write_submission.assert_called_once()
```

## Troubleshooting

### "No module named 'pipeline'"

Make sure you're running from the `src/` directory.

### "Configuration file not found"

Check that `config/plugins/claimx/itel_cabinet_api/workers.yaml` exists.

### "Connection 'claimx_api' not found"

Check that `config/plugins/shared/connections/claimx.yaml` exists and has a connection named `claimx_api`.

### Worker isn't processing messages

1. Check Kafka is running: `docker ps | grep kafka`
2. Check topic exists: `kafka-topics --list --bootstrap-server localhost:9094`
3. Check consumer group: `kafka-consumer-groups --describe --group itel_cabinet_tracking_group --bootstrap-server localhost:9094`

### ClaimX API errors

Check your API token:
```bash
echo $CLAIMX_API_TOKEN
curl -H "Authorization: Bearer $CLAIMX_API_TOKEN" https://api.claimx.com/api/v1/tasks/32513
```

## Performance Tuning

### Increase Batch Size

Edit `workers.yaml`:
```yaml
kafka:
  batch_size: 500  # Process more messages per batch
```

### Concurrent Processing

Run multiple tracking worker instances (same consumer group):
```bash
# Terminal 1
python -m pipeline.plugins.itel_cabinet_api.itel_cabinet_tracking_worker

# Terminal 2
python -m pipeline.plugins.itel_cabinet_api.itel_cabinet_tracking_worker
```

Kafka will distribute partitions across workers automatically.

## Migration from Old System

The old handler-based system has been completely removed from this plugin.

If you have references to old handlers like:
- `itel_api_sender.py`
- `form_parser.py`
- `conditional_completed.py`

These are gone. All functionality is now in:
- `pipeline.py`
- `parsers.py`
- `itel_cabinet_api_worker.py`

See `REFACTOR_SUMMARY.md` for full details on what changed.

## Questions?

Read the code! It's designed to be self-documenting:
1. Start with `pipeline.py:process()` - read it like a recipe
2. Follow function calls to see implementation details
3. Check `models.py` to see data structures

No magic, no frameworks, just Python.
