# itel Cabinet API Testing - Quick Start

## The Problem You Had

You tried: `python -m kafka_pipeline --mode dummy --config config/dummy/itel_cabinet_test.yaml`

This doesn't work because the CLI doesn't support a `--config` parameter. The `dummy-source` worker loads its configuration from the main `config.yaml` file.

## Solution: Three Methods

### Method 1: Use Ready-Made Script (Easiest!)

**Just run this:**
```bash
python scripts/run_itel_dummy_producer.py
```

That's it! The script is pre-configured for itel Cabinet testing and will:
- Generate 10 itel Cabinet events
- Use task_id=32513
- Send to claimx.events.raw topic
- Stop automatically after 10 events

To generate more events or run continuously, edit `max_events` in the script.

### Method 2: Add to config.yaml

1. **Copy the dummy configuration** from `config/dummy/itel_cabinet_test.yaml`

2. **Add it to your main config.yaml** under the `dummy:` key:

```yaml
# In config.yaml
dummy:
  # Generator configuration
  generator:
    plugin_profile: "itel_cabinet_api"  # itel Cabinet profile
    base_url: "http://localhost:8765"
    include_failures: false

  # File server configuration
  file_server:
    host: "0.0.0.0"
    port: 8765

  # Which domains to generate events for
  domains:
    - claimx

  # Event generation settings
  events_per_minute: 10.0
  burst_mode: false
  max_events: 100

  # Other settings...
  include_all_event_types: true
  max_active_claims: 100
```

3. **Run the dummy source**:
```bash
python -m kafka_pipeline --worker dummy-source --dev
```

### Method 3: Custom Python Script

If you need more control, create your own test script:

```python
# scripts/test_itel_producer.py
import asyncio
from config.config import KafkaConfig
from kafka_pipeline.common.dummy import DummyDataSource, DummySourceConfig
from kafka_pipeline.common.dummy.generators import GeneratorConfig

async def main():
    # Simple Kafka config for local testing
    # Use port 9094 for external connections (from host machine)
    kafka_config = KafkaConfig(
        bootstrap_servers="localhost:9094",
        security_protocol="PLAINTEXT",
        claimx={"topics": {"events": "claimx.events.raw"}},
    )

    # Generator with itel Cabinet profile
    generator_config = GeneratorConfig(
        plugin_profile="itel_cabinet_api",
        base_url="http://localhost:8765",
    )

    # Dummy source config
    config = DummySourceConfig(
        kafka=kafka_config,
        generator=generator_config,
        domains=["claimx"],
        events_per_minute=10.0,
        max_events=10,  # Just 10 for testing
    )

    # Run the source
    async with DummyDataSource(config) as source:
        await source.run()

if __name__ == "__main__":
    asyncio.run(main())
```

Then run:
```bash
python scripts/test_itel_producer.py
```

## Complete Testing Workflow

### Step 1: Start Kafka
```bash
docker-compose up -d kafka
```

### Step 2: Start the itel Cabinet Worker
```bash
python -m kafka_pipeline.plugins.itel_cabinet_api.itel_cabinet_tracking_worker
```

### Step 3: Start Dummy Producer

**Easiest way (Method 1 - ready-made script):**
```bash
python scripts/run_itel_dummy_producer.py
```

**Alternative (Method 2 - using config.yaml):**
```bash
python -m kafka_pipeline --worker dummy-source --dev
```

**Alternative (Method 3 - custom script):**
```bash
python scripts/test_itel_producer.py  # If you created this
```

### Step 4: Monitor Progress

```bash
# Watch worker logs
tail -f logs/itel/tracking_worker.log

# Check Kafka topic
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic itel.cabinet.task.tracking \
  --from-beginning

# Check generated events
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic claimx.events.raw \
  --from-beginning
```

## What Gets Generated

Each event will have:
- `event_type`: "CUSTOM_TASK_ASSIGNED" or "CUSTOM_TASK_COMPLETED"
- `raw_data.taskId`: 32513 (itel Cabinet Repair Form)
- `raw_data.claimx_task_details`: Complete form data including:
  - Customer information
  - Cabinet damage details (lower, upper cabinets)
  - Linear feet measurements
  - Photo attachments grouped by question

## Troubleshooting

### "No module named 'coolname'"
You need to install dependencies:
```bash
pip install coolname  # Or install from requirements.txt
```

### "Connection refused" to Kafka
Make sure Kafka is running:
```bash
docker-compose up -d kafka
```

### No events generated
Check that:
1. `plugin_profile: "itel_cabinet_api"` is set in config
2. `domains` includes `"claimx"`
3. Dummy source worker is actually running

### Events not triggering plugin
Verify:
1. Plugin is enabled: `config/plugins/claimx/itel_cabinet_api/config.yaml`
2. Worker is running: `python -m kafka_pipeline.plugins.itel_cabinet_api.itel_cabinet_tracking_worker`
3. Topic exists and has messages

## Quick Verification

Want to just verify the code structure works? Run:
```bash
python scripts/verify_itel_structure.py
```

This checks that all the itel Cabinet profile code is properly configured (should show 12/12 checks passed).
