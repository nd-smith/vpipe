# VPipe Quickstart Guide

Quick reference for launching all workers and plugins in the VPipe data pipeline.

## Table of Contents
- [Prerequisites](#prerequisites)
- [Environment Setup](#environment-setup)
- [Core Workers](#core-workers)
  - [Verisk (XACT) Workers](#verisk-xact-workers)
  - [ClaimX Workers](#claimx-workers)
- [Plugin Workers](#plugin-workers)
- [Common Launch Patterns](#common-launch-patterns)
- [Troubleshooting](#troubleshooting)

---

## Prerequisites

**Required:**
- Python 3.8+
- Kafka cluster access (or local Kafka for dev)
- Azure credentials (for production)
- Environment variables configured

**Optional:**
- Docker (for local Kafka)
- Delta Lake access

---

## Environment Setup

### Minimal Required Variables

```bash
# Kafka Connection
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_SECURITY_PROTOCOL=PLAINTEXT

# ClaimX API
export CLAIMX_API_BASE_PATH=https://www.claimxperience.com/service/cxedirest
export CLAIMX_API_TOKEN=<your-token>

# Azure (Production only)
export AZURE_TENANT_ID=<tenant-id>
export AZURE_CLIENT_ID=<client-id>
export AZURE_CLIENT_SECRET=<client-secret>
export AZURE_WORKSPACE_ID=<workspace-id>
export AZURE_LAKEHOUSE_ID=<lakehouse-id>

# Eventhouse
export EVENTHOUSE_CLUSTER_URL=https://trd-...
export XACT_EVENTHOUSE_DATABASE=VERISK_XACTANALYSIS_DB
export CLAIMX_EVENTHOUSE_DATABASE=VERISK_CLAIMXPERIENCE_DB
```

### Event Hub Transport (Optional)

```bash
export PIPELINE_TRANSPORT=eventhub
export EVENTHUB_NAMESPACE_CONNECTION_STRING="Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=..."
export EVENTHUB_DEFAULT_CONSUMER_GROUP=$Default
```

### Plugin-Specific Variables

```bash
# iTEL Cabinet API Plugin
export ITEL_CABINET_API_BASE_URL=https://api.itelcabinet.com
export ITEL_CABINET_API_TOKEN=<api-token>
export ITEL_DELTA_FORMS_TABLE=abfss://...
export ITEL_DELTA_ATTACHMENTS_TABLE=abfss://...
```

---

## Core Workers

All workers are launched via the main pipeline module from the `src/` directory.

### Main Launcher Syntax

```bash
cd src
python -m pipeline [OPTIONS]
```

**Common Options:**
- `--worker {name|all}` - Worker to run (default: all)
- `--dev` - Development mode (local Kafka only)
- `--no-delta` - Disable Delta Lake writes
- `--log-level {DEBUG|INFO|WARNING|ERROR}` - Log level (default: INFO)
- `--log-dir {path}` - Log directory (default: ./logs)
- `-c, --count {n}` - Number of worker instances (default: 1)
- `--metrics-port {port}` - Prometheus metrics port (default: 8000)
- `--log-to-stdout` - Send logs to stdout only
- `--simulation-mode` - Enable mock dependencies

---

## Verisk (XACT) Workers

### Overview
Processes XACT assignment events from Eventhouse through enrichment, download, and upload stages.

| Worker | Purpose | Launch Command |
|--------|---------|----------------|
| **xact-poller** | Poll Eventhouse for XACT events | `python -m pipeline --worker xact-poller` |
| **xact-json-poller** | JSON variant of Eventhouse poller | `python -m pipeline --worker xact-json-poller` |
| **xact-event-ingester** | Route Event Hub → local Kafka | `python -m pipeline --worker xact-event-ingester` |
| **xact-delta-writer** | Write events to Delta Lake | `python -m pipeline --worker xact-delta-writer` |
| **xact-retry-scheduler** | Handle retry logic for failed events | `python -m pipeline --worker xact-retry-scheduler` |
| **xact-enricher** | Enrich events with ClaimX data | `python -m pipeline --worker xact-enricher` |
| **xact-download** | Download attachments | `python -m pipeline --worker xact-download` |
| **xact-upload** | Upload processed data | `python -m pipeline --worker xact-upload` |
| **xact-result-processor** | Process final results | `python -m pipeline --worker xact-result-processor` |

### Example: Run XACT Download Worker with 3 Instances

```bash
cd src
python -m pipeline --worker xact-download -c 3 --log-level DEBUG
```

### Example: Development Mode (No Delta Writes)

```bash
python -m pipeline --worker xact-enricher --dev --no-delta
```

---

## ClaimX Workers

### Overview
Processes ClaimX events for entities, tasks, projects, and contacts.

| Worker | Purpose | Launch Command |
|--------|---------|----------------|
| **claimx-poller** | Poll Eventhouse for ClaimX events | `python -m pipeline --worker claimx-poller` |
| **claimx-ingester** | Ingest events into local Kafka | `python -m pipeline --worker claimx-ingester` |
| **claimx-enricher** | Enrich ClaimX events | `python -m pipeline --worker claimx-enricher` |
| **claimx-downloader** | Download ClaimX attachments | `python -m pipeline --worker claimx-downloader` |
| **claimx-uploader** | Upload ClaimX data | `python -m pipeline --worker claimx-uploader` |
| **claimx-result-processor** | Process ClaimX results | `python -m pipeline --worker claimx-result-processor` |
| **claimx-delta-writer** | Write events to Delta tables | `python -m pipeline --worker claimx-delta-writer` |
| **claimx-retry-scheduler** | ClaimX retry scheduling | `python -m pipeline --worker claimx-retry-scheduler` |
| **claimx-entity-writer** | Write entities to Delta Lake | `python -m pipeline --worker claimx-entity-writer` |

### Example: Run ClaimX Enricher

```bash
cd src
python -m pipeline --worker claimx-enricher
```

### Example: Multiple ClaimX Workers in Parallel

```bash
# Terminal 1
python -m pipeline --worker claimx-ingester

# Terminal 2
python -m pipeline --worker claimx-enricher

# Terminal 3
python -m pipeline --worker claimx-delta-writer
```

---

## Plugin Workers

Plugins are independent modules with their own workers. They are launched separately from core workers.

### Plugin 1: iTEL Cabinet API

**Purpose**: Track iTEL Cabinet Repair Form tasks (task_id 32513) from creation to completion, enrich with ClaimX data, and send to iTEL API.

#### Worker 1: iTEL Cabinet Tracking Worker

**Purpose**: Process task events, enrich COMPLETED tasks, write to Delta tables

```bash
cd src
python -m pipeline.plugins.itel_cabinet_api.itel_cabinet_tracking_worker
```

**Configuration:**
- Input Topic: `pcesdopodappv1-itel-cabinet-pending`
- Consumer Group: `itel_cabinet_tracking_group`
- Output Topic: `pcesdopodappv1-itel-cabinet-completed`
- Delta Tables: `claimx_itel_forms`, `claimx_itel_attachments`

**Required Environment Variables:**
```bash
CLAIMX_API_BASE_PATH=https://www.claimxperience.com/service/cxedirest
CLAIMX_API_TOKEN=<token>
ITEL_DELTA_FORMS_TABLE=abfss://...
ITEL_DELTA_ATTACHMENTS_TABLE=abfss://...
```

#### Worker 2: iTEL Cabinet API Worker

**Purpose**: Transform completed tasks and send to iTEL API

```bash
# Production mode
cd src
python -m pipeline.plugins.itel_cabinet_api.itel_cabinet_api_worker

# Development mode (writes to files instead of API)
python -m pipeline.plugins.itel_cabinet_api.itel_cabinet_api_worker --dev
```

**Configuration:**
- Input Topic: `pcesdopodappv1-itel-cabinet-completed`
- Consumer Group: `itel_cabinet_api_group`
- API Endpoint: `POST /api/v1/cabinet/repairs`
- Test Output Dir: `config/plugins/claimx/itel_cabinet_api/test`

**Required Environment Variables:**
```bash
ITEL_CABINET_API_BASE_URL=https://api.itelcabinet.com
ITEL_CABINET_API_TOKEN=<api-token>
```

**Documentation**: `src/pipeline/plugins/itel_cabinet_api/README.md`

---

### Plugin 2: ClaimX Mitigation Task

**Purpose**: Track completion of mitigation tasks (task_id 25367, 24454) and publish to Kafka for processing.

#### Mitigation Task Tracking Worker

```bash
cd src
python -m pipeline.plugins.claimx_mitigation_task.mitigation_tracking_worker
```

**Configuration:**
- Input Topic: `pcesdopodappv1-ghrn-mitigation-pending`
- Consumer Group: `claimx_mitigation_tracking_group`
- Output Topic: `pcesdopodappv1-ghrn-mitigation-completed`

**Required Environment Variables:**
```bash
CLAIMX_API_BASE_PATH=https://www.claimxperience.com/service/cxedirest
CLAIMX_API_TOKEN=<token>
```

---

### Plugin 3: Verisk Status Trigger

**Purpose**: Monitor XACT assignment status changes and trigger webhooks, Kafka publishing, or custom actions.

**Configuration File**: `src/config/plugins/verisk/status_trigger/config.yaml`

**Trigger Configuration Example:**

```yaml
triggers:
  - status: "inspectionCompleted"
    publish_to_topic: "inspection-notifications"
    webhook:
      url: "https://api.example.com/webhook"
      method: "POST"
      headers:
        Authorization: "Bearer TOKEN"
      timeout: 30
    log:
      level: info
      message: "Inspection completed for assignment {assignment_id}"
```

**Supported Actions:**
- Kafka publishing
- HTTP webhooks
- Structured logging
- Custom metrics
- Pipeline filtering

**Documentation**: `src/config/plugins/verisk/status_trigger/README.md`

---

## Common Launch Patterns

### Pattern 1: Start All Core Workers

```bash
cd src
python -m pipeline
```

### Pattern 2: Start Specific Worker with Custom Settings

```bash
cd src
python -m pipeline \
  --worker claimx-enricher \
  --log-level DEBUG \
  --metrics-port 9090 \
  -c 2
```

### Pattern 3: Development Mode (Local Kafka, No Delta)

```bash
cd src
python -m pipeline --dev --no-delta --log-to-stdout
```

### Pattern 4: Run Plugin Worker with Full Stack

```bash
# Terminal 1: Core ClaimX workers
cd src
python -m pipeline --worker claimx-ingester

# Terminal 2: ClaimX enricher
python -m pipeline --worker claimx-enricher

# Terminal 3: iTEL Cabinet tracking worker
python -m pipeline.plugins.itel_cabinet_api.itel_cabinet_tracking_worker

# Terminal 4: iTEL Cabinet API worker (dev mode)
python -m pipeline.plugins.itel_cabinet_api.itel_cabinet_api_worker --dev
```

### Pattern 5: Simulation Mode (Mock Dependencies)

```bash
cd src
python -m pipeline --worker xact-download --simulation-mode
```

### Pattern 6: View Worker Logs in Real-Time

```bash
# Core worker logs
tail -f logs/claimx/claimx-enricher_*.log

# Plugin worker logs
tail -f logs/itel_cabinet_api/*/itel_cabinet_tracking_*.log
```

---

## Troubleshooting

### Worker Won't Start

**Check environment variables:**
```bash
env | grep -E "(KAFKA|CLAIMX|AZURE|ITEL)"
```

**Check Kafka connectivity:**
```bash
nc -zv localhost 9092
```

**Run with debug logging:**
```bash
python -m pipeline --worker {worker-name} --log-level DEBUG --log-to-stdout
```

### No Messages Being Processed

**Check Kafka consumer lag:**
```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group {consumer-group}
```

**Verify topic exists:**
```bash
kafka-topics.sh --bootstrap-server localhost:9092 --list | grep {topic-name}
```

### Delta Lake Write Failures

**Check Azure credentials:**
```bash
az login
az account show
```

**Disable Delta writes temporarily:**
```bash
python -m pipeline --worker {worker-name} --no-delta
```

### Plugin Not Receiving Events

**Verify plugin is enabled:**
```bash
cat config/plugins/{domain}/{plugin-name}/config.yaml | grep enabled
```

**Check trigger configuration:**
```bash
cat config/plugins/{domain}/{plugin-name}/config.yaml | grep -A 10 triggers
```

**Test with simulation mode:**
```bash
python -m pipeline.simulation.dummy_producer --profile {plugin-name}
```

### API Connection Issues

**Test ClaimX API:**
```bash
curl -H "Authorization: Bearer $CLAIMX_API_TOKEN" \
  $CLAIMX_API_BASE_PATH/projects/123
```

**Test iTEL API:**
```bash
curl -H "Authorization: Bearer $ITEL_CABINET_API_TOKEN" \
  $ITEL_CABINET_API_BASE_URL/health
```

**Use dev mode to skip API calls:**
```bash
python -m pipeline.plugins.itel_cabinet_api.itel_cabinet_api_worker --dev
```

---

## Quick Reference

### All Worker Names

**Verisk (XACT):**
- `xact-poller`
- `xact-json-poller`
- `xact-event-ingester`
- `xact-delta-writer`
- `xact-retry-scheduler`
- `xact-enricher`
- `xact-download`
- `xact-upload`
- `xact-result-processor`

**ClaimX:**
- `claimx-poller`
- `claimx-ingester`
- `claimx-enricher`
- `claimx-downloader`
- `claimx-uploader`
- `claimx-result-processor`
- `claimx-delta-writer`
- `claimx-retry-scheduler`
- `claimx-entity-writer`

**Plugin Workers (separate launch):**
- `pipeline.plugins.itel_cabinet_api.itel_cabinet_tracking_worker`
- `pipeline.plugins.itel_cabinet_api.itel_cabinet_api_worker`
- `pipeline.plugins.claimx_mitigation_task.mitigation_tracking_worker`

### Configuration Files

| Component | Config File |
|-----------|-------------|
| Main pipeline | `config/config.yaml` |
| iTEL Cabinet plugin | `config/plugins/claimx/itel_cabinet_api/config.yaml` |
| iTEL Cabinet workers | `config/plugins/claimx/itel_cabinet_api/workers.yaml` |
| Mitigation Task plugin | `config/plugins/claimx/claimx_mitigation_task/config.yaml` |
| Mitigation Task workers | `config/plugins/claimx/claimx_mitigation_task/workers.yaml` |
| Status Trigger plugin | `config/plugins/verisk/status_trigger/config.yaml` |
| ClaimX connections | `config/plugins/shared/connections/claimx.yaml` |
| iTEL connections | `config/plugins/shared/connections/app.itel.yaml` |

### Documentation Files

| Topic | Documentation |
|-------|---------------|
| Event Hub transport | `src/pipeline/common/eventhub/README.md` |
| Retry mechanism | `src/pipeline/common/retry/README.md` |
| iTEL Cabinet plugin | `src/pipeline/plugins/itel_cabinet_api/README.md` |
| Status Trigger plugin | `src/config/plugins/verisk/status_trigger/README.md` |
| Plugin testing | `src/pipeline/common/dummy/PLUGIN_TESTING.md` |

---

## Getting Help

For more detailed information, see:
- Individual plugin README files
- Configuration file comments
- Worker source code in `src/pipeline/{domain}/workers/`
- Plugin source code in `src/pipeline/plugins/{plugin-name}/`

For issues or questions, contact the VPipe development team.
