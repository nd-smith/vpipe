# Docker TLDR - VPipe Kafka Pipeline

Quick reference for building, running, and managing the VPipe Kafka pipeline with Docker.

## Quick Start

### Production (external Kafka/Event Hub)
```bash
docker build -t kafka-pipeline:latest -f Dockerfile .
docker-compose -f scripts/docker/docker-compose.yml up -d
docker-compose -f scripts/docker/docker-compose.yml ps
```

### Local Development (with local Kafka)
```bash
docker-compose -f scripts/docker/docker-compose.kafka.yml up -d
docker-compose -f scripts/docker/docker-compose.kafka.yml --profile ui up -d  # With UI
docker inspect -f '{{.State.Health.Status}}' kafka-pipeline-local  # Check health
```

### Simulation Mode (complete test environment)
```bash
./scripts/start_simulation.sh                    # Recommended
./scripts/start_simulation.sh --build            # Rebuild first
./scripts/start_simulation.sh --clean            # Clean start

# Manual start with observability
docker-compose -f scripts/docker/docker-compose.simulation.yml \
  --profile simulation --profile observability up -d
```

**Simulation URLs:**
- Kafka UI: http://localhost:8080
- File Server: http://localhost:8765/health
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin)
- Jaeger: http://localhost:16686

---

## Building Images

```bash
# Standard build
docker build -t kafka-pipeline:latest -f Dockerfile .

# Clean rebuild
docker build --no-cache -t kafka-pipeline:latest -f Dockerfile .

# Specific platform
docker build --platform linux/amd64 -t kafka-pipeline:latest -f Dockerfile .

# Monitoring service
docker build -t kafka-pipeline-monitoring:latest -f Dockerfile.monitoring .

# Using compose
docker-compose -f scripts/docker/docker-compose.yml build
docker-compose -f scripts/docker/docker-compose.yml build --parallel
```

---

## Running the Pipeline

```bash
# All workers
docker-compose -f scripts/docker/docker-compose.yml up -d

# Specific workers
docker-compose -f scripts/docker/docker-compose.yml up -d xact-event-ingester xact-enricher

# With local Kafka
docker-compose -f scripts/docker/docker-compose.yml --profile kafka up -d

# Run single worker
docker run --rm -it --env-file .env kafka-pipeline:latest --worker xact-event-ingester

# With custom env/volumes
docker run --rm -it --env-file .env \
  -e LOG_LEVEL=DEBUG \
  -v $(pwd)/logs:/app/logs \
  kafka-pipeline:latest --worker xact-event-ingester
```

---

## Scaling Workers

### Dynamic Scaling

```bash
# Scale specific worker
docker-compose -f scripts/docker/docker-compose.yml up -d --scale xact-download=10

# Scale multiple workers
docker-compose -f scripts/docker/docker-compose.yml up -d \
  --scale xact-download=10 \
  --scale xact-upload=8 \
  --scale claimx-downloader=10

# Check running replicas
docker-compose -f scripts/docker/docker-compose.yml ps

# Scale down
docker-compose -f scripts/docker/docker-compose.yml up -d --scale xact-download=2
```

### Resource Limits

Workers have predefined limits in docker-compose.yml:
- Light workers (ingesters, schedulers): 256M-512M RAM
- Heavy workers (downloaders, uploaders): 1G-2G RAM
- Kafka: 2G RAM limit, 1G reservation

```bash
docker stats  # Monitor resource usage
```

---

## Monitoring & Logs

```bash
# View logs
docker-compose -f scripts/docker/docker-compose.yml logs -f
docker-compose -f scripts/docker/docker-compose.yml logs -f xact-event-ingester
docker-compose -f scripts/docker/docker-compose.yml logs --tail=100 --since 2h xact-event-ingester
docker logs -f <container_id>

# Health & status
docker inspect -f '{{.State.Health.Status}}' kafka-pipeline-local
docker ps --format "table {{.Names}}\t{{.Status}}"
docker stats
docker top xact-event-ingester
```

### Kafka Monitoring

```bash
# List topics
docker exec kafka-pipeline-local \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Topic details
docker exec kafka-pipeline-local \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic xact.downloads.pending

# Consumer groups
docker exec kafka-pipeline-local \
  /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# Group details
docker exec kafka-pipeline-local \
  /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group xact-download-consumer-group
```

---

## Managing Containers

```bash
# Start/stop/restart
docker-compose -f scripts/docker/docker-compose.yml start [service]
docker-compose -f scripts/docker/docker-compose.yml stop [service]
docker-compose -f scripts/docker/docker-compose.yml restart [service]

# Recreate (apply config changes)
docker-compose -f scripts/docker/docker-compose.yml up -d --force-recreate [service]

# Execute commands
docker exec -it xact-event-ingester /bin/bash
docker exec xact-event-ingester python --version
```

---

## Troubleshooting

```bash
# Kafka issues
docker logs kafka-pipeline-local
docker inspect -f '{{.State.Health.Status}}' kafka-pipeline-local
docker-compose -f scripts/docker/docker-compose.kafka.yml restart kafka
docker-compose -f scripts/docker/docker-compose.kafka.yml down -v  # Hard reset

# Worker issues
docker logs --tail=200 xact-event-ingester
docker inspect -f '{{.State.ExitCode}}' xact-event-ingester
docker stats xact-event-ingester
docker run --rm -it --env-file .env -e LOG_LEVEL=DEBUG kafka-pipeline:latest --worker xact-event-ingester

# Network/volume issues
docker network inspect pipeline_network
docker volume ls | grep kafka
docker volume prune

# Port conflicts
sudo lsof -i :9092

# Wait for healthy service
timeout 120 bash -c 'until [ "$(docker inspect -f "{{.State.Health.Status}}" kafka-pipeline-local)" = "healthy" ]; do sleep 2; done'
```

---

## Cleanup

```bash
# Stop services
docker-compose -f scripts/docker/docker-compose.yml down
docker-compose -f scripts/docker/docker-compose.simulation.yml down
./scripts/stop_simulation.sh

# Remove volumes (DATA LOSS!)
docker-compose -f scripts/docker/docker-compose.yml down -v
./scripts/start_simulation.sh --clean

# Remove images
docker rmi kafka-pipeline:latest kafka-pipeline-monitoring:latest
docker image prune -a

# Complete cleanup (NUCLEAR)
docker-compose -f scripts/docker/docker-compose.yml down -v
rm -rf logs/ /tmp/vpipe_simulation/
docker system prune -a --volumes
```

---

## Docker Compose Files

### Available Files

**`scripts/docker/docker-compose.yml`** - Production
- All workers (XACT, ClaimX, plugins)
- Optional local Kafka (`--profile kafka`)
- Optional Kafka UI (`--profile ui`)

**`scripts/docker/docker-compose.kafka.yml`** - Local Kafka only
- Kafka broker (KRaft mode)
- Topic initialization
- Kafka UI (`--profile ui`)

**`scripts/docker/docker-compose.simulation.yml`** - Simulation
- Local Kafka + workers
- Dummy producer (1000 events/min)
- File server
- Observability stack (`--profile observability`)

**`scripts/docker/docker-compose.obs.yml`** - Observability stack

### Using Profiles

```bash
# Single profile
docker-compose -f scripts/docker/docker-compose.yml --profile kafka up -d

# Multiple profiles
docker-compose -f scripts/docker/docker-compose.simulation.yml \
  --profile simulation --profile observability up -d

# List profiles
grep -A5 "profiles:" scripts/docker/docker-compose.yml
```

---

## Environment Files

### Required Variables

**Production (`.env`):**
- `KAFKA_BOOTSTRAP_SERVERS` - Kafka/Event Hub connection
- `KAFKA_SECURITY_PROTOCOL` - PLAINTEXT or SASL_SSL
- `KAFKA_SASL_*` - SASL auth (Event Hub)
- Domain API credentials (XACT_*, CLAIMX_*)
- Storage credentials (DELTA_*, AZURE_*)

**Simulation (`.env.simulation`):**
- `SIMULATION_MODE=true`
- `KAFKA_BOOTSTRAP_SERVERS=kafka:9092`
- Failure injection settings

### Loading Environment

```bash
# With compose (in docker-compose.yml)
env_file:
  - .env

# With docker run
docker run --rm -it --env-file .env kafka-pipeline:latest --worker xact-event-ingester

# Override variables
docker run --rm -it --env-file .env \
  -e LOG_LEVEL=DEBUG \
  -e KAFKA_BOOTSTRAP_SERVERS=my-kafka:9092 \
  kafka-pipeline:latest --worker xact-event-ingester
```

---

## Quick Reference

```bash
# Build
docker build -t kafka-pipeline:latest -f Dockerfile .

# Run production
docker-compose -f scripts/docker/docker-compose.yml up -d

# Run simulation
./scripts/start_simulation.sh

# View logs
docker-compose -f scripts/docker/docker-compose.yml logs -f

# Scale workers
docker-compose -f scripts/docker/docker-compose.yml up -d --scale xact-download=10

# Stop
docker-compose -f scripts/docker/docker-compose.yml down

# Clean (with volumes)
docker-compose -f scripts/docker/docker-compose.yml down -v

# Shell into container
docker exec -it xact-event-ingester /bin/bash

# Check status
docker-compose -f scripts/docker/docker-compose.yml ps

# Resource usage
docker stats

# Kafka health
docker inspect -f '{{.State.Health.Status}}' kafka-pipeline-local
```

---

## Common Workflows

```bash
# Deploy new version
docker build -t kafka-pipeline:latest -f Dockerfile .
docker-compose -f scripts/docker/docker-compose.yml up -d --force-recreate

# Scale for high load
docker-compose -f scripts/docker/docker-compose.yml up -d \
  --scale xact-download=15 --scale xact-upload=15
docker stats

# Debug worker
docker logs --tail=200 xact-event-ingester
docker run --rm -it --env-file .env -e LOG_LEVEL=DEBUG kafka-pipeline:latest --worker xact-event-ingester
docker exec xact-event-ingester ping kafka
```

---

**For more details, see individual compose files in `scripts/docker/` and scripts in `scripts/` directory.**
