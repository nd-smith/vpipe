# Complete Worker Deployment List

## Total: 17 Workers (6 XACT + 8 ClaimX + 3 Plugin)

---

## Architecture Overview

```
Event Sources (Source EventHub)
         ↓
    Event Ingestion
         ↓
    ┌────┴────┐
    ↓         ↓
  XACT     ClaimX
Pipeline  Pipeline
    ↓         ↓
  ┌─┴─┐    ┌─┴─┐
  │   │    │   │
Enrichers  Enrichers
(Load     (Load
Plugins)   Plugins)
  │   │    │   │
  ↓   ↓    ↓   ↓
Downloads  Downloads
  ↓   ↓    ↓   ↓
  Uploads  Uploads
  ↓   ↓    ↓   ↓
Delta    Delta
Writers  Writers
  ↓        ↓
  └────┬───┘
       ↓
  Plugin Workers
  (Kafka topics)
       ↓
  External APIs
```

---

## 1. XACT Workers (6)

### xact-event-ingester
**Launch**: `python -m pipeline --worker xact-event-ingester`
**Purpose**: Ingests raw XACT events from source EventHub
**Input**: Source EventHub topic `verisk_events`
**Output**: EventHub topic `xact.enrichment.pending`

### xact-enricher
**Launch**: `python -m pipeline --worker xact-enricher`
**Purpose**: Enriches XACT events, executes plugins
**Plugins Loaded**: `status_trigger` plugin
**Input**: Kafka topic `xact.enrichment.pending`
**Output**: Kafka topic `xact.downloads.pending`

### xact-download
**Launch**: `python -m pipeline --worker xact-download`
**Purpose**: Downloads attachments from URLs
**Input**: Kafka topic `xact.downloads.pending`
**Output**: Kafka topic `xact.downloads.cached`

### xact-upload
**Launch**: `python -m pipeline --worker xact-upload`
**Purpose**: Uploads cached files to OneLake
**Input**: Kafka topic `xact.downloads.cached`
**Output**: OneLake storage + Kafka topic `xact.downloads.results`

### xact-delta-writer
**Launch**: `python -m pipeline --worker xact-delta-writer`
**Purpose**: Writes XACT events to Delta Lake
**Input**: Kafka topic `xact.events.ingested`
**Output**: Delta table `xact_events`

### xact-result-processor
**Launch**: `python -m pipeline --worker xact-result-processor`
**Purpose**: Processes download results
**Input**: Kafka topic `xact.downloads.results`
**Output**: Delta table `xact_attachments`

### xact-retry-scheduler
**Launch**: `python -m pipeline --worker xact-retry-scheduler`
**Purpose**: Handles failed event retries
**Input**: Kafka topic `xact.retry`
**Output**: Retries to pipeline or DLQ

---

## 2. ClaimX Workers (8)

### claimx-ingester
**Launch**: `python -m pipeline --worker claimx-ingester`
**Purpose**: Ingests raw ClaimX events from source EventHub
**Input**: Source EventHub topic `claimx_events`
**Output**: EventHub topic `claimx.enrichment.pending`

### claimx-enricher
**Launch**: `python -m pipeline --worker claimx-enricher`
**Purpose**: Enriches ClaimX events, executes plugins
**Plugins Loaded**:
- `itel_cabinet_api` plugin → publishes to `itel.cabinet.task.tracking`
- `claimx_mitigation_task` plugin
**Input**: Kafka topic `claimx.enrichment.pending`
**Output**: Kafka topics `claimx.downloads.pending` + `claimx.entities.rows`

### claimx-downloader
**Launch**: `python -m pipeline --worker claimx-downloader`
**Purpose**: Downloads ClaimX attachments
**Input**: Kafka topic `claimx.downloads.pending`
**Output**: Kafka topic `claimx.downloads.cached`

### claimx-uploader
**Launch**: `python -m pipeline --worker claimx-uploader`
**Purpose**: Uploads ClaimX files to OneLake
**Input**: Kafka topic `claimx.downloads.cached`
**Output**: OneLake storage + Kafka topic `claimx.downloads.results`

### claimx-result-processor
**Launch**: `python -m pipeline --worker claimx-result-processor`
**Purpose**: Processes ClaimX download results
**Input**: Kafka topic `claimx.downloads.results`
**Output**: Delta table `claimx_attachments`

### claimx-delta-writer
**Launch**: `python -m pipeline --worker claimx-delta-writer`
**Purpose**: Writes ClaimX events to Delta Lake
**Input**: Kafka topic `claimx.events.ingested`
**Output**: Delta table `claimx_events`

### claimx-retry-scheduler
**Launch**: `python -m pipeline --worker claimx-retry-scheduler`
**Purpose**: Handles ClaimX failed retries
**Input**: Kafka topic `claimx.retry`
**Output**: Retries to pipeline or DLQ

### claimx-entity-writer
**Launch**: `python -m pipeline --worker claimx-entity-writer`
**Purpose**: Writes to 7 ClaimX entity tables
**Input**: Kafka topic `claimx.entities.rows`
**Output**: 7 Delta tables:
- `claimx_projects`
- `claimx_contacts`
- `claimx_attachments_metadata`
- `claimx_tasks`
- `claimx_task_templates`
- `claimx_external_link`
- `claimx_video_collab`

---

## 3. Plugin Workers (3)

### itel-cabinet-tracking
**Launch**: `python -m pipeline.plugins.itel_cabinet_api.itel_cabinet_tracking_worker`
**Purpose**: Tracks iTel Cabinet task lifecycle
**Triggered By**: `claimx-enricher` (when task_id=32513 events occur)
**Input**: Kafka topic `itel.cabinet.task.tracking`
**Actions**:
1. Enriches task data via ClaimX API
2. Writes to Delta tables: `claimx_itel_forms`, `claimx_itel_attachments`
3. Publishes completed tasks to `itel.cabinet.completed`

### itel-cabinet-api
**Launch**: `python -m pipeline.plugins.itel_cabinet_api.itel_cabinet_api_worker`
**Purpose**: Sends completed iTel Cabinet data to iTel API
**Input**: Kafka topic `itel.cabinet.completed`
**Output**: HTTP POST to iTel Cabinet API

### claimx-mitigation-tracking
**Launch**: `python -m pipeline.plugins.claimx_mitigation_task.mitigation_tracking_worker`
**Purpose**: Tracks ClaimX mitigation tasks
**Triggered By**: `claimx-enricher` (for specific mitigation task events)
**Input**: Kafka topic (TBD - check config)
**Output**: Delta table or external system

---

## Plugin Execution Flow

### Example: iTel Cabinet Task 32513

```
1. ClaimX Event arrives (task_id=32513)
   ↓
2. claimx-ingester reads from source EventHub
   ↓
3. claimx-ingester publishes to enrichment.pending
   ↓
4. claimx-enricher processes event
   - Loads itel_cabinet_api plugin
   - Plugin condition matches (task_id=32513)
   - Plugin publishes to itel.cabinet.task.tracking
   ↓
5. itel-cabinet-tracking worker consumes
   - Enriches via ClaimX API
   - Writes to claimx_itel_forms Delta table
   - Publishes to itel.cabinet.completed
   ↓
6. itel-cabinet-api worker consumes
   - Transforms data to iTel format
   - Sends to iTel Cabinet API
```

---

## Deployment Configuration

### Jenkinsfile Worker Arrays:

```groovy
def XACT_WORKERS = [
    'xact-event-ingester',
    'xact-enricher',
    'xact-download',
    'xact-upload',
    'xact-delta-writer',
    'xact-result-processor',
    'xact-retry-scheduler'
]

def CLAIMX_WORKERS = [
    'claimx-ingester',
    'claimx-enricher',
    'claimx-downloader',
    'claimx-uploader',
    'claimx-result-processor',
    'claimx-delta-writer',
    'claimx-retry-scheduler',
    'claimx-entity-writer'
]

def PLUGIN_WORKERS = [
    'itel-cabinet-tracking',
    'itel-cabinet-api',
    'claimx-mitigation-tracking'
]
```

### Procfile Launch Logic:

**Standard Workers** (`WORKER_TYPE=standard`):
```bash
python -m pipeline --worker ${WORKER_NAME} --metrics-port ${PORT}
```

**Plugin Workers** (`WORKER_TYPE=plugin`):
```bash
# itel-cabinet-tracking
python -m pipeline.plugins.itel_cabinet_api.itel_cabinet_tracking_worker

# itel-cabinet-api
python -m pipeline.plugins.itel_cabinet_api.itel_cabinet_api_worker

# claimx-mitigation-tracking
python -m pipeline.plugins.claimx_mitigation_task.mitigation_tracking_worker
```

---

## Required Secrets (Jenkins Credentials)

1. `eventhub-connection-string`
2. `claimx-api-token`
3. `azure-tenant-id`
4. `azure-client-id`
5. `azure-client-secret`
6. `itel-cabinet-api-url`
7. `itel-cabinet-api-token`

---

## Scaling Recommendations

### Heavy Load Workers (Scale to 3-5 instances):
- `xact-download`
- `xact-upload`
- `claimx-downloader`
- `claimx-uploader`

### Single Instance Workers:
- Plugin workers (unless high volume)

### Moderate Scaling (2-3 instances):
- Enrichers (if plugin processing is slow)
- Delta writers (if write volume is high)
