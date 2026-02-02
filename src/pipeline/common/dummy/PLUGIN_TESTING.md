# Plugin Profile Testing Guide

This guide explains how to use the dummy data producer with plugin profiles to test specific plugin workflows with production-like data.

## Overview

Plugin profiles allow you to generate dummy data that's specifically tailored to trigger and test particular plugin workflows. Instead of generating generic ClaimX or XACT events, you can generate events that match the exact requirements of your plugins.

## Available Profiles

### 1. itel Cabinet API (`itel_cabinet_api`)

Generates ClaimX events for itel Cabinet Repair Form tasks (task_id=32513).

**What it generates:**
- `CUSTOM_TASK_ASSIGNED` events (30%)
- `CUSTOM_TASK_COMPLETED` events (70%)
- Full form data including:
  - Customer information
  - Damage descriptions
  - Cabinet sections (lower, upper, full height, island)
  - Linear feet measurements
  - Damage details (detached, face frames, end panels)
  - Counter types
  - Photo attachments grouped by question

**Triggers:**
- itel Cabinet API plugin
- Worker: `itel_cabinet_tracking_worker`
- Topic: `itel.cabinet.task.tracking`

### 2. Standard ClaimX (`standard_claimx` or `null`)

Default profile. Generates standard ClaimX events with various event types.

### 3. Standard XACT (`standard_xact`)

Generates standard XACT domain events.

## Quick Start

### Testing itel Cabinet API Workflow

1. **Start Kafka** (if not already running):
   ```bash
   docker-compose up -d kafka
   ```

2. **Start the itel Cabinet worker**:
   ```bash
   python -m pipeline.plugins.itel_cabinet_api.itel_cabinet_tracking_worker
   ```

3. **Start dummy data producer with itel profile**:

   First, add the itel Cabinet config to your `config.yaml`:
   ```yaml
   dummy:
     generator:
       plugin_profile: "itel_cabinet_api"
     domains:
       - claimx
     events_per_minute: 10.0
     max_events: 100
   ```

   Then run:
   ```bash
   python -m pipeline --worker dummy-source --dev
   ```

4. **Watch the logs**:
   ```bash
   # Dummy source logs
   tail -f logs/dummy_source.log

   # Worker logs
   tail -f logs/itel/tracking_worker.log

   # Kafka topic
   kafka-console-consumer --topic itel.cabinet.task.tracking --from-beginning
   ```

## Configuration

### Using YAML Config

```yaml
# config/dummy/my_test.yaml
generator:
  plugin_profile: "itel_cabinet_api"  # Specify profile
  seed: 42  # Optional: for reproducible data

domains:
  - claimx  # Must include the domain for your profile

events_per_minute: 10.0
max_events: 100
```

### Using Python API

```python
from pipeline.common.dummy import DummyDataSource, DummySourceConfig
from pipeline.common.dummy.generators import GeneratorConfig
from config.config import KafkaConfig

# Create config with itel Cabinet profile
kafka_config = KafkaConfig(...)
generator_config = GeneratorConfig(
    plugin_profile="itel_cabinet_api",
    seed=42,  # Optional: for reproducible data
)

dummy_config = DummySourceConfig(
    kafka=kafka_config,
    generator=generator_config,
    domains=["claimx"],
    events_per_minute=10.0,
    max_events=100,
)

# Run dummy source
async with DummyDataSource(dummy_config) as source:
    await source.run()
```

## Generated Event Structure

### itel Cabinet Event Example

```json
{
  "event_id": "evt_abc123",
  "event_type": "CUSTOM_TASK_COMPLETED",
  "project_id": "proj_XYZ789",
  "task_assignment_id": "CLM-12345",
  "ingested_at": "2026-01-11T10:30:00Z",
  "raw_data": {
    "taskId": 32513,
    "taskName": "iTel Cabinet Repair Form",
    "taskAssignmentId": "CLM-12345",
    "projectId": "proj_XYZ789",
    "assignee": "John Mitchell",
    "completedAt": "2026-01-11T10:30:00Z",

    "claimx_task_details": {
      "id": "CLM-12345",
      "task_id": 32513,
      "status": "completed",
      "form_response": {
        "responses": [
          {
            "question_key": "customer_first_name",
            "question_text": "Customer First Name",
            "answer": "John"
          },
          {
            "question_key": "lower_cabinets_damaged",
            "question_text": "Are lower cabinets damaged?",
            "answer": "Yes"
          },
          {
            "question_key": "lower_cabinets_lf",
            "question_text": "Lower Cabinets Linear Feet",
            "answer": "12.5"
          }
        ],
        "attachments": [
          {
            "media_id": "media_123456",
            "question_key": "overview_photos",
            "question_text": "Overview Photos of Kitchen",
            "file_name": "overview_1.jpg",
            "content_type": "image/jpeg",
            "download_url": "https://api.claimx.com/media/media_123456/download"
          }
        ]
      }
    }
  }
}
```

## Verifying the Data

### Check Delta Tables

```sql
-- View itel form submissions
SELECT
  project_id,
  assignment_id,
  status,
  customer_first_name,
  customer_last_name,
  damage_description,
  lower_cabinets_lf,
  upper_cabinets_lf,
  ingested_at
FROM claimx_itel_forms
ORDER BY ingested_at DESC
LIMIT 10;

-- View itel attachments
SELECT
  project_id,
  assignment_id,
  question_key,
  claim_media_id,
  blob_path,
  ingested_at
FROM claimx_itel_attachments
ORDER BY ingested_at DESC
LIMIT 20;
```

### Check Kafka Topics

```bash
# View events on tracking topic
kafka-console-consumer \
  --topic itel.cabinet.task.tracking \
  --from-beginning \
  --max-messages 10

# View success confirmations
kafka-console-consumer \
  --topic itel.cabinet.task.tracking.success \
  --from-beginning

# View any errors
kafka-console-consumer \
  --topic itel.cabinet.task.tracking.errors \
  --from-beginning
```

### Check API Payloads (Test Mode)

When `test_mode: true` in worker config, payloads are written to files:

```bash
ls -lah config/plugins/claimx/itel_cabinet_api/test/
cat config/plugins/claimx/itel_cabinet_api/test/payload_CLM-12345_20260111_103000.json
```

## Advanced Usage

### Burst Mode Testing

Test high-volume scenarios:

```yaml
generator:
  plugin_profile: "itel_cabinet_api"

burst_mode: true
burst_size: 100  # Generate 100 events at once
burst_interval_seconds: 60  # Every 60 seconds
```

### Reproducible Test Data

Use a seed for deterministic data generation:

```yaml
generator:
  plugin_profile: "itel_cabinet_api"
  seed: 42  # Same seed = same data every time
```

### Custom Event Mix

Generate specific event types:

```python
# In your test script
generator = RealisticDataGenerator(GeneratorConfig(plugin_profile="itel_cabinet_api"))

# Generate only completed tasks
for _ in range(10):
    event = generator.generate_itel_cabinet_event(event_type="CUSTOM_TASK_COMPLETED")
    # Send to Kafka...

# Generate only assigned tasks
for _ in range(5):
    event = generator.generate_itel_cabinet_event(event_type="CUSTOM_TASK_ASSIGNED")
    # Send to Kafka...
```

## Creating Your Own Profile

To add a new plugin profile:

1. **Create profile generator** in `pipeline/common/dummy/plugin_profiles.py`:

```python
class MyPluginDataGenerator:
    """Generates data for my custom plugin."""

    MY_TASK_ID = 99999

    def generate_form_data(self, claim):
        # Build your custom data structure
        return MyCustomFormData(...)

    def build_claimx_task_details(self, form_data, claim):
        # Build mock ClaimX API response
        return {...}
```

2. **Add profile to PluginProfile enum**:

```python
class PluginProfile(Enum):
    MY_PLUGIN = "my_plugin"
```

3. **Integrate in generators.py**:

```python
# In RealisticDataGenerator.__init__
self._my_plugin_generator = MyPluginDataGenerator(self._rng)

# Add generation method
def generate_my_plugin_event(self, claim=None, event_type=None):
    # Use your generator
    form_data = self._my_plugin_generator.generate_form_data(claim)
    # Build event structure
    return event
```

4. **Handle in source.py**:

```python
if plugin_profile == "my_plugin":
    event_data = self._generator.generate_my_plugin_event()
```

5. **Create test config** in `config/dummy/my_plugin_test.yaml`:

```yaml
generator:
  plugin_profile: "my_plugin"
domains:
  - claimx
events_per_minute: 10.0
```

## Troubleshooting

### Events Not Triggering Plugin

- Verify `plugin_profile` matches profile name exactly
- Check plugin is enabled in plugin config
- Ensure domain is correct (`claimx` for itel Cabinet)
- Check task_id matches what plugin expects

### Wrong Event Structure

- Review `build_claimx_task_details()` method
- Compare with actual ClaimX API response
- Check form_response structure matches expectations

### Worker Not Processing

- Verify worker is running
- Check consumer group is correct
- Ensure topic exists and has messages
- Review worker logs for errors

### No Data in Delta Tables

- Check worker enrichment pipeline is complete
- Verify Delta table exists
- Review handler configurations
- Check for validation failures in logs

## Best Practices

1. **Start with small batches**: Use `max_events: 10` for initial testing
2. **Use test mode**: Set `test_mode: true` in worker config to write payloads to files
3. **Monitor topics**: Watch Kafka topics to verify event flow
4. **Check logs**: Review both dummy source and worker logs
5. **Reproducible tests**: Use `seed` for consistent test data
6. **Incremental testing**: Test one component at a time (plugin → worker → Delta)

## Examples

See `config/dummy/itel_cabinet_test.yaml` for a complete working example.
