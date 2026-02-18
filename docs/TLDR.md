# Pipeline Overview (TL;DR)

Event-driven pipeline that ingests insurance claims events from Azure Event Hub, enriches them with external API data, downloads and uploads file attachments to OneLake (ADLS Gen2), and persists structured data to Delta Lake tables. Two parallel domains: **Verisk (XACT)** for XactAnalysis estimate events, and **ClaimX** for ClaimX project/task/media events. Everything runs as async Python workers connected by Kafka topics.

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.12, asyncio |
| Messaging | aiokafka, Azure Event Hub (AMQP/WebSocket) |
| Storage | OneLake (ADLS Gen2), Delta Lake |
| Data | Polars, deltalake-python |
| HTTP | aiohttp (downloads), FastAPI (health) |
| Config/Validation | Pydantic, YAML |
| Observability | Prometheus, structured JSON logging, EventHub log sink |
| Deployment | Docker |

## Architecture

### High-Level View

```mermaid
graph LR
    subgraph Sources
        EH_V[Event Hub<br>xactanalysis]
        EH_C[Event Hub<br>claimx-events]
    end

    subgraph Verisk Pipeline
        V_ING[Ingester] --> V_ENR[Enricher] --> V_DL[Downloader] --> V_UL[Uploader] --> V_RP[Result<br>Processor]
        V_ING -.-> V_DEV[Delta Events<br>Writer]
    end

    subgraph ClaimX Pipeline
        C_ING[Ingester] --> C_ENR[Enricher] --> C_DL[Downloader] --> C_UL[Uploader] --> C_RP[Result<br>Processor]
        C_ENR --> C_ENT[Entity Delta<br>Writer]
        C_ING -.-> C_DEV[Delta Events<br>Writer]
    end

    subgraph Storage
        DL[(Delta Lake<br>Tables)]
        OL[(OneLake<br>Files)]
    end

    EH_V --> V_ING
    EH_C --> C_ING
    V_UL --> OL
    C_UL --> OL
    V_RP --> DL
    V_DEV --> DL
    C_ENT --> DL
    C_DEV --> DL
    C_RP --> DL
```

### Verisk (XACT) Pipeline

```mermaid
graph TD
    EH[Event Hub: xactanalysis] --> ING[EventIngesterWorker<br>24-hr hybrid dedup]
    EH --> DEV[DeltaEventsWorker<br>separate consumer group]

    ING -->|verisk-enrichment-pending| ENR[XACTEnrichmentWorker<br>plugin execution]
    ENR -->|verisk-downloads-pending| DL[DownloadWorker<br>10 concurrent]
    DL -->|verisk-downloads-cached| UL[UploadWorker<br>10 concurrent]
    UL -->|verisk-downloads-results| RP[ResultProcessor<br>batch 100 / 5s]

    DEV --> T_EVT[(xact_events)]
    RP --> T_ATT[(xact_attachments)]

    DL -.->|failure| RETRY{Retry / DLQ}
    UL -.->|failure| RETRY
    RETRY -.->|verisk-retry| DL
```

### ClaimX Pipeline

```mermaid
graph TD
    EH[Event Hub: claimx-events] --> ING[ClaimXEventIngesterWorker<br>dedup]
    EH --> DEV[ClaimXDeltaEventsWorker<br>separate consumer group]

    ING -->|claimx-enrichment-pending| ENR[ClaimXEnrichmentWorker<br>handler registry + ClaimX API]

    ENR -->|claimx-enriched| ENT[ClaimXEntityDeltaWorker<br>merge-write to 7 tables]
    ENR -->|claimx-downloads-pending| DL[ClaimXDownloadWorker<br>S3 presigned URLs]

    DL -->|claimx-downloads-cached| UL[ClaimXUploadWorker]
    UL -->|claimx-downloads-results| RP[ClaimXResultProcessor]

    DEV --> T_EVT[(claimx_events)]
    RP --> T_ATT[(claimx_attachments)]

    ENT --> T_PRJ[(claimx_projects)]
    ENT --> T_CON[(claimx_contacts)]
    ENT --> T_MED[(claimx_media)]
    ENT --> T_TSK[(claimx_tasks)]
    ENT --> T_TT[(claimx_task_templates)]
    ENT --> T_EL[(claimx_external_links)]
    ENT --> T_VC[(claimx_video_collab)]

    ENR -.->|failure| RETRY{Retry / DLQ}
    DL -.->|failure| RETRY
    RETRY -.->|claimx-retry| ENR & DL
```

### File Download & Upload Flow (Detail)

```mermaid
graph TD
    subgraph Download Stage
        TASK[DownloadTaskMessage<br>attachment URL + blob path] --> VAL[URL Validation<br>SSRF check, expiry check]
        VAL --> SIZE{File > 50MB?}
        SIZE -->|yes| STREAM[Streaming download]
        SIZE -->|no| MEM[In-memory download]
        STREAM --> CACHE[Local file cache]
        MEM --> CACHE
    end

    subgraph Upload Stage
        CACHE --> TOKEN[Generate WriteOperation<br>idempotency token]
        TOKEN --> UPLOAD[Upload to OneLake<br>ADLS Gen2 DFS API]
        UPLOAD --> CLEANUP[Delete local cache]
    end

    subgraph Result Processing
        CLEANUP --> RESULT{Status?}
        RESULT -->|completed| INV[(Inventory Delta table)]
        RESULT -->|failed transient| RETRY_Q
        RESULT -->|failed permanent| DLQ_Q
    end

    subgraph Retry Circuit
        RETRY_Q[Retry Topic] --> DELAY[Delay Queue<br>min-heap + disk persist]
        DELAY -->|5m → 10m → 20m → 40m| TASK
        RETRY_Q -->|max retries exceeded| DLQ_Q[Dead Letter Queue]
    end
```

### Error Handling & Retry

```mermaid
graph TD
    ERR[Error occurs] --> CLASS[Classify error]

    CLASS --> T[TRANSIENT<br>timeout, connection,<br>throttle, 5xx]
    CLASS --> A[AUTH<br>401, token expired]
    CLASS --> P[PERMANENT<br>400, 403, 404,<br>schema mismatch]
    CLASS --> U[UNKNOWN<br>unclassified]

    T --> RETRY_CHK{Retries<br>remaining?}
    A --> RETRY_CHK
    U --> RETRY_CHK

    P --> DLQ[Dead Letter Queue<br>reason: permanent]

    RETRY_CHK -->|yes| SCHED[Unified Retry Scheduler<br>delay queue with backoff]
    RETRY_CHK -->|no| DLQ_EX[Dead Letter Queue<br>reason: exhausted]

    SCHED -->|300s → 600s → 1200s → 2400s| TARGET[Back to origin topic]
```

### Plugin System

```mermaid
graph TD
    REG[Plugin Registry] --> ORCH[Plugin Orchestrator<br>priority-ordered execution]

    ORCH --> STAGE{Pipeline Stage}
    STAGE --> EI[EVENT_INGEST]
    STAGE --> EC[ENRICHMENT_COMPLETE]
    STAGE --> DC[DOWNLOAD_COMPLETE]
    STAGE --> UC[UPLOAD_COMPLETE]
    STAGE --> EW[ENTITY_WRITE]

    ORCH --> ACTIONS[Action Executor]
    ACTIONS --> PUB[Publish to Topic]
    ACTIONS --> WH[HTTP Webhook]
    ACTIONS --> EMAIL[Send Email]
    ACTIONS --> CT[Create ClaimX Task]

    subgraph Concrete Plugins
        ITEL[iTel Cabinet<br>cabinet task tracking +<br>form parsing]
        MIT[Mitigation Tracking<br>water/fire damage tasks]
        TT[Task Trigger<br>configurable task-based actions]
    end

    REG --> ITEL & MIT & TT
```

## Project Structure

```
src/
├── pipeline/
│   ├── __main__.py              # CLI entry point
│   ├── verisk/                  # XACT domain
│   │   ├── workers/             # 6 workers + retry handler
│   │   ├── schemas/             # Pydantic message models
│   │   └── writers/             # Delta Lake writers
│   ├── claimx/                  # ClaimX domain
│   │   ├── workers/             # 7 workers + retry handlers
│   │   ├── handlers/            # 5 event handlers + registry
│   │   ├── schemas/             # Pydantic message models
│   │   ├── writers/             # Delta Lake writers (entities + events)
│   │   └── api_client.py        # ClaimX API (circuit breaker + rate limit)
│   ├── common/                  # Shared infrastructure
│   │   ├── transport/           # Kafka/EventHub abstraction
│   │   ├── storage/             # OneLake client
│   │   ├── retry/               # Delay queue + unified scheduler
│   │   ├── dlq/                 # Dead letter queue producer
│   │   └── metrics.py           # Prometheus metrics
│   ├── plugins/                 # Plugin framework
│   │   ├── shared/              # Registry, base classes, loader
│   │   ├── itel_cabinet_api/    # iTel Cabinet plugin
│   │   └── claimx_mitigation_task/  # Mitigation plugin
│   └── runners/                 # Worker registry & launch
├── core/                        # Core utilities
│   ├── download/                # HTTP downloader + streaming
│   ├── auth/                    # Azure authentication
│   ├── security/                # URL/file validation (SSRF prevention)
│   ├── errors/                  # Error classification + exceptions
│   └── resilience/              # Circuit breaker
├── config/                      # Config loading (YAML + env)
└── scripts/
tests/                           # Mirrors src/ structure
```

## Worker Reference

| CLI Name | Domain | Purpose |
|----------|--------|---------|
| `xact-event-ingester` | Verisk | Consume EventHub events, 24-hr dedup, produce enrichment tasks |
| `xact-enricher` | Verisk | Execute plugins, create download tasks from attachments |
| `xact-download` | Verisk | Download attachments (10 concurrent), cache locally |
| `xact-upload` | Verisk | Upload cached files to OneLake, idempotent |
| `xact-result-processor` | Verisk | Batch results, write to xact_attachments Delta table |
| `xact-delta-writer` | Verisk | Write raw events to xact_events Delta table |
| `xact-retry-scheduler` | Verisk | Unified retry routing with exponential backoff |
| `claimx-ingester` | ClaimX | Consume EventHub events, dedup, produce enrichment tasks |
| `claimx-enricher` | ClaimX | Route to handlers, call ClaimX API, produce entities + downloads |
| `claimx-downloader` | ClaimX | Download media from S3 presigned URLs |
| `claimx-uploader` | ClaimX | Upload cached files to OneLake |
| `claimx-result-processor` | ClaimX | Write results to claimx_attachments Delta table |
| `claimx-entity-writer` | ClaimX | Merge-write to 7 entity Delta tables |
| `claimx-delta-writer` | ClaimX | Write raw events to claimx_events Delta table |
| `claimx-retry-scheduler` | ClaimX | Unified retry routing with exponential backoff |
| `itel-cabinet-tracking` | Plugin | Track iTel cabinet task assignments and submissions |
| `itel-cabinet-api` | Plugin | iTel cabinet API integration worker |
| `claimx-mitigation-tracking` | Plugin | Track water/fire mitigation task completions |

## How to Run

```bash
# Run a specific worker
python -m pipeline --worker xact-download

# Run with options
python -m pipeline --worker claimx-enricher --log-level DEBUG --dev --no-delta

# Multiple instances (partition distribution)
python -m pipeline --worker xact-download --count 4
```

| Flag | Effect |
|------|--------|
| `--worker NAME` | Run a specific worker (default: all) |
| `--dev` | Development mode (EventHub only, no Eventhouse) |
| `--no-delta` | Skip Delta Lake writes |
| `--count N` | Number of worker instances |
| `--log-level` | DEBUG, INFO, WARNING, ERROR |
| `--log-to-stdout` | Console logging (for containers) |
| `--metrics-port` | Prometheus metrics port (default: 8000) |
