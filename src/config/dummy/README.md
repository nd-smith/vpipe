# Plugin-Specific Dummy Data Generation

This directory contains configurations for generating dummy data that triggers specific plugin workflows.

## Quick Start

### itel Cabinet API Testing

Generate events that trigger the itel Cabinet API plugin workflow:

**Option 1: Add to config.yaml (recommended)**

1. Add the configuration from `itel_cabinet_test.yaml` to your main `config.yaml` under the `dummy:` key
2. Run: `python -m kafka_pipeline --worker dummy-source --dev`

**Option 2: Use test script**

Create and run `scripts/test_itel_producer.py` (see QUICKSTART.md for example)

This will:
1. Generate ClaimX events with `task_id=32513` (itel Cabinet Repair Form)
2. Include complete form data with customer info, cabinet details, and photos
3. Trigger the itel Cabinet plugin
4. Publish to `itel.cabinet.task.tracking` topic
5. Process through the enrichment worker pipeline

## Configuration Files

### `itel_cabinet_test.yaml`
Complete configuration for testing the itel Cabinet API plugin workflow.

**Key settings:**
- `generator.plugin_profile: "itel_cabinet_api"` - Use itel Cabinet profile
- `domains: ["claimx"]` - Generate ClaimX events only
- `events_per_minute: 10.0` - Generate 10 events per minute
- `max_events: 100` - Stop after 100 events

**What it generates:**
- Event type: `CUSTOM_TASK_ASSIGNED` (30%) or `CUSTOM_TASK_COMPLETED` (70%)
- Task ID: `32513` (itel Cabinet Repair Form)
- Complete form data matching ClaimX API structure
- Realistic cabinet damage scenarios
- Photo attachments grouped by question

## Usage Examples

### Command Line

```bash
# Start with default config
python -m kafka_pipeline --mode dummy --config config/dummy/itel_cabinet_test.yaml

# Customize event rate
python -m kafka_pipeline --mode dummy --config config/dummy/itel_cabinet_test.yaml --events-per-minute 30

# Generate specific number of events
python -m kafka_pipeline --mode dummy --config config/dummy/itel_cabinet_test.yaml --max-events 50
```

### Programmatic

```python
from kafka_pipeline.common.dummy import DummyDataSource, DummySourceConfig
from kafka_pipeline.common.dummy.generators import GeneratorConfig
from config.config import KafkaConfig

# Create config
kafka_config = KafkaConfig(...)
generator_config = GeneratorConfig(plugin_profile="itel_cabinet_api")

config = DummySourceConfig(
    kafka=kafka_config,
    generator=generator_config,
    domains=["claimx"],
    events_per_minute=10.0,
    max_events=100,
)

# Run
async with DummyDataSource(config) as source:
    await source.run()
```

### Generate Single Event

```python
from kafka_pipeline.common.dummy.generators import RealisticDataGenerator, GeneratorConfig

config = GeneratorConfig(plugin_profile="itel_cabinet_api", seed=42)
generator = RealisticDataGenerator(config)

# Generate completed task
event = generator.generate_itel_cabinet_event(event_type="CUSTOM_TASK_COMPLETED")

# Generate assigned task
event = generator.generate_itel_cabinet_event(event_type="CUSTOM_TASK_ASSIGNED")
```

## Testing Workflow

### Full End-to-End Test

1. **Start Kafka**:
   ```bash
   docker-compose up -d kafka
   ```

2. **Start itel Cabinet Worker**:
   ```bash
   python -m kafka_pipeline.plugins.itel_cabinet_api.itel_cabinet_tracking_worker
   ```

3. **Start Dummy Data Producer**:
   ```bash
   python -m kafka_pipeline --mode dummy --config config/dummy/itel_cabinet_test.yaml
   ```

4. **Monitor Progress**:
   ```bash
   # Watch worker logs
   tail -f logs/itel/tracking_worker.log

   # Check Kafka topic
   kafka-console-consumer --topic itel.cabinet.task.tracking --from-beginning

   # Check Delta tables
   spark-sql -e "SELECT * FROM claimx_itel_forms ORDER BY ingested_at DESC LIMIT 10"
   ```

### Quick Structure Verification

```bash
# Verify code structure
python scripts/verify_itel_structure.py
```

## Generated Event Structure

Example itel Cabinet event:

```json
{
  "event_id": "evt_abc123",
  "event_type": "CUSTOM_TASK_COMPLETED",
  "project_id": "proj_XYZ789",
  "task_assignment_id": "CLM-12345",
  "raw_data": {
    "taskId": 32513,
    "taskName": "iTel Cabinet Repair Form",
    "claimx_task_details": {
      "task_id": 32513,
      "status": "completed",
      "form_response": {
        "responses": [
          {
            "question_key": "customer_first_name",
            "answer": "John"
          },
          {
            "question_key": "lower_cabinets_damaged",
            "answer": "Yes"
          },
          {
            "question_key": "lower_cabinets_lf",
            "answer": "12.5"
          }
        ],
        "attachments": [
          {
            "media_id": "media_123456",
            "question_key": "overview_photos",
            "file_name": "overview_1.jpg",
            "download_url": "https://api.claimx.com/media/media_123456/download"
          }
        ]
      }
    }
  }
}
```

## Available Plugin Profiles

### `itel_cabinet_api`
- Generates itel Cabinet Repair Form events
- Task ID: 32513
- Event types: CUSTOM_TASK_ASSIGNED, CUSTOM_TASK_COMPLETED
- Includes complete form data and attachments

### `standard_claimx` (default)
- Generates standard ClaimX events
- All event types
- Generic project and file data

### `standard_xact`
- Generates XACT domain events
- Standard document processing workflow

## Configuration Options

### Generator Settings

```yaml
generator:
  # Plugin profile to use
  plugin_profile: "itel_cabinet_api"  # or null for standard

  # Optional seed for reproducible data
  seed: 42

  # Base URL for file server
  base_url: "http://localhost:8765"

  # Include failure scenarios
  include_failures: false
  failure_rate: 0.05
```

### Event Generation

```yaml
# Which domains to generate for
domains:
  - claimx

# Event rate (per domain)
events_per_minute: 10.0

# Burst mode for load testing
burst_mode: false
burst_size: 50
burst_interval_seconds: 60

# Runtime limits
max_events: 100           # Stop after N events
max_runtime_seconds: 1800  # Stop after N seconds
```

## Troubleshooting

### Events Not Triggering Plugin

**Problem**: Generated events aren't triggering the itel Cabinet plugin.

**Solutions**:
- Verify `plugin_profile: "itel_cabinet_api"` in config
- Check plugin is enabled: `config/plugins/itel_cabinet_api/config.yaml`
- Ensure domain includes `claimx`
- Verify task_id is 32513 in generated events

### Wrong Data Structure

**Problem**: Generated data doesn't match expected format.

**Solutions**:
- Review `kafka_pipeline/common/dummy/plugin_profiles.py`
- Check `build_claimx_task_details()` method
- Compare with actual ClaimX API response
- Generate example events: `python scripts/test_itel_dummy_data.py`

### Worker Not Processing

**Problem**: Worker isn't consuming generated events.

**Solutions**:
- Verify worker is running
- Check Kafka connectivity
- Ensure topic exists: `itel.cabinet.task.tracking`
- Review worker logs for errors
- Check consumer group offsets

## Documentation

- **Plugin Testing Guide**: `kafka_pipeline/common/dummy/PLUGIN_TESTING.md`
- **Worker Configuration**: `config/plugins/itel_cabinet_api/workers.yaml`
- **Plugin Configuration**: `config/plugins/itel_cabinet_api/config.yaml`

## Support

For questions or issues:
1. Check the PLUGIN_TESTING.md guide
2. Review example configurations
3. Run verification script: `python scripts/verify_itel_structure.py`
4. Check worker and plugin logs
