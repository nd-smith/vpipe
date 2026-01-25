# VPipe Simulation Mode - Docker Setup

Complete containerized simulation environment for local testing without production dependencies.

## Quick Start

```bash
# Start everything (first time will build images)
./scripts/start_simulation.sh

# View logs
docker-compose -f docker-compose.simulation.yml logs -f

# Stop everything
./scripts/stop_simulation.sh

# Clean and restart
./scripts/stop_simulation.sh --clean
./scripts/start_simulation.sh
```

## What's Included

### Infrastructure
- **Kafka** - Local Kafka broker (no Eventhouse/Event Hub needed)
- **Kafka UI** - Web interface at http://localhost:8080
- **File Server** - Serves test attachments at http://localhost:8765

### Data Generation
- **Dummy Producer** - Generates 1000 events/min for both domains
  - ClaimX events (PROJECT_CREATED, PROJECT_FILE_ADDED, CUSTOM_TASK_COMPLETED, etc.)
  - XACT events
  - Realistic fake data (insurance claims, media files, tasks)

### Workers (All in Simulation Mode)
**ClaimX Pipeline:**
- `claimx-ingester` (1) - Ingests from raw topic to enrichment topics
- `claimx-enricher` (3 replicas) - Enriches events with mock API data
- `claimx-downloader` (4 replicas) - Downloads from local file server
- `claimx-uploader` (4 replicas) - Uploads to local filesystem
- `claimx-delta-writer` (2 replicas) - Writes to local Delta tables
- `claimx-entity-writer` (2 replicas) - Writes entity data
- `claimx-retry-scheduler` (1) - Manages retries

**XACT Pipeline:**
- `xact-local-ingester` (1) - Ingests from raw topic to enrichment topics
- `xact-enricher` (1) - Runs enrichment plugins
- `xact-download` (4 replicas) - Downloads attachments
- `xact-upload` (4 replicas) - Uploads to local storage
- `xact-delta-writer` (2 replicas) - Writes to local Delta tables
- `xact-retry-scheduler` (1) - Manages retries

## Configuration

### Event Generation Rate

Default: **1000 events/min per domain** (2000 total/min)

To change:

```yaml
# Edit docker-compose.simulation.yml
dummy-producer:
  command:
    - "--events-per-minute"
    - "2000"  # Change this value
```

Or rebuild with environment variable:
```bash
# Set in docker-compose.simulation.yml environment section
- EVENTS_PER_MINUTE=2000
```

### Scaling Workers

Scale up workers to handle higher load:

```bash
# Scale download workers to 8
docker-compose -f docker-compose.simulation.yml up -d --scale xact-download=8 --scale claimx-downloader=8

# Scale all I/O workers
docker-compose -f docker-compose.simulation.yml up -d \
  --scale xact-download=8 \
  --scale xact-upload=8 \
  --scale claimx-downloader=8 \
  --scale claimx-uploader=8
```

### Memory Limits

Adjust if you have more/less resources:

```yaml
# In docker-compose.simulation.yml
deploy:
  resources:
    limits:
      memory: 2G      # Max memory
    reservations:
      memory: 512M    # Minimum guaranteed
```

## Monitoring

### Kafka UI
Web interface: http://localhost:8080
- View topics
- Inspect messages
- Monitor consumer groups
- Check lag

### Logs

View all logs:
```bash
docker-compose -f docker-compose.simulation.yml logs -f
```

View specific worker:
```bash
docker-compose -f docker-compose.simulation.yml logs -f claimx-enricher
```

View producer:
```bash
docker-compose -f docker-compose.simulation.yml logs -f dummy-producer
```

View errors only:
```bash
docker-compose -f docker-compose.simulation.yml logs -f | grep ERROR
```

### Container Status

```bash
docker-compose -f docker-compose.simulation.yml ps
```

### Resource Usage

```bash
docker stats
```

## Data Storage

### Kafka Data
- **Location**: Docker volume `kafka_simulation_data`
- **Retention**: 2 hours
- **Cleanup**: `docker-compose -f docker-compose.simulation.yml down -v`

### Simulation Files
- **Location**: `/tmp/vpipe_simulation/`
- **Contents**: Downloaded files, uploaded files, Delta tables
- **Cleanup**: `rm -rf /tmp/vpipe_simulation`

### Application Logs
- **Location**: `./logs/`
- **Format**: JSON (one per worker)
- **Cleanup**: `rm -rf logs/`

## Troubleshooting

### Port Conflicts

If ports 8080, 8765, or 9092/9094 are in use:

```bash
# Check what's using the port
lsof -i :8080
lsof -i :9092

# Stop conflicting services or change ports in docker-compose.simulation.yml
```

### Out of Memory

Reduce replicas or scale down:

```bash
# Scale down to minimum
docker-compose -f docker-compose.simulation.yml up -d \
  --scale xact-download=1 \
  --scale xact-upload=1 \
  --scale claimx-downloader=1 \
  --scale claimx-uploader=1
```

### Kafka Not Starting

```bash
# View Kafka logs
docker-compose -f docker-compose.simulation.yml logs kafka

# Restart Kafka
docker-compose -f docker-compose.simulation.yml restart kafka
```

### Workers Crashing

Check logs for specific worker:
```bash
docker-compose -f docker-compose.simulation.yml logs claimx-enricher
```

Common issues:
- Kafka not ready (wait for health check)
- Missing environment variables
- Port conflicts

### Reset Everything

```bash
# Stop and remove all data
./scripts/stop_simulation.sh --clean

# Rebuild images
docker-compose -f docker-compose.simulation.yml build --no-cache

# Start fresh
./scripts/start_simulation.sh --clean
```

## Performance Testing

### Load Testing Scenarios

**Low Load** (100 events/min):
```yaml
dummy-producer:
  command: ["--events-per-minute", "100"]
```

**Medium Load** (1000 events/min - default):
```yaml
dummy-producer:
  command: ["--events-per-minute", "1000"]
```

**High Load** (5000 events/min):
```yaml
dummy-producer:
  command: ["--events-per-minute", "5000"]
```
Requires scaling workers proportionally.

**Burst Testing**:
```bash
# Generate 10,000 events as fast as possible
docker-compose -f docker-compose.simulation.yml run --rm dummy-producer \
  python -m kafka_pipeline.simulation.dummy_producer \
  --domains claimx,xact \
  --max-events 10000 \
  --burst-mode \
  --burst-size 1000
```

### Metrics to Monitor

1. **Consumer Lag** (Kafka UI)
   - Should stay low (<100 under normal load)
   - High lag = workers can't keep up

2. **Processing Time** (logs)
   - Check `duration_ms` in worker logs
   - Should be <500ms for most operations

3. **Memory Usage** (`docker stats`)
   - Workers should stay within limits
   - Spikes indicate issues

4. **Disk Usage**
   - Monitor `/tmp/vpipe_simulation` size
   - Delta tables grow over time

## Advanced Usage

### Custom Producer Configuration

Edit `dummy_producer` service in `docker-compose.simulation.yml`:

```yaml
dummy-producer:
  command:
    - "python"
    - "-m"
    - "kafka_pipeline.simulation.dummy_producer"
    - "--domains"
    - "claimx"              # Only ClaimX
    - "--events-per-minute"
    - "500"                 # Lower rate
    - "--max-events"
    - "10000"               # Stop after 10k
    - "--plugin-profile"
    - "itel_cabinet_api"    # iTel-only events
```

### Running Individual Workers

```bash
# Run only ClaimX enricher
docker-compose -f docker-compose.simulation.yml up claimx-enricher

# Run with custom command
docker-compose -f docker-compose.simulation.yml run --rm claimx-enricher \
  --worker claimx-enricher --log-level DEBUG
```

### Accessing Container

```bash
# Shell into running container
docker exec -it claimx-enricher-sim bash

# Run Python commands
docker exec -it claimx-enricher-sim python -c "from kafka_pipeline import __version__; print(__version__)"
```

### Network Debugging

```bash
# Test file server from worker container
docker exec -it xact-download-sim curl http://file-server-simulation:8765/health

# Check Kafka connectivity
docker exec -it xact-enricher-sim python -c "
from kafka import KafkaAdminClient
admin = KafkaAdminClient(bootstrap_servers='kafka:9092')
print(admin.list_topics())
"
```

## Architecture

```
┌─────────────────┐
│ Dummy Producer  │ ──> Generates 1000 events/min
│ (ClaimX, XACT)  │
└────────┬────────┘
         │
         v
┌─────────────────┐
│  Kafka Broker   │ ──> topics: *.events.raw
│   (PLAINTEXT)   │
└────────┬────────┘
         │
         v
┌─────────────────┐
│    Ingesters    │ ──> Read .raw, write to enrichment topics
└────────┬────────┘
         │
         v
┌─────────────────┐
│   Enrichers     │ ──> Use MockClaimXAPIClient
└────────┬────────┘
         │
         v
┌─────────────────┐     ┌─────────────────┐
│   Downloaders   │ ──> │  File Server    │
└────────┬────────┘     │  (localhost)    │
         │              └─────────────────┘
         v
┌─────────────────┐
│    Uploaders    │ ──> Local filesystem
└────────┬────────┘
         │
         v
┌─────────────────┐
│  Delta Writers  │ ──> /tmp/vpipe_simulation/delta/
└─────────────────┘
```

## See Also

- [Simulation Testing Guide](src/kafka_pipeline/simulation/docs/SIMULATION_TESTING.md)
- [Simulation Configuration](config/README_SIMULATION_CONFIG.md)
- [Main README](README.md)
