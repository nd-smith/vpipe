# ClaimX Pipeline Architecture

This document describes the ClaimX event processing pipeline, showing the complete flow from event ingestion through enrichment, download, upload, and storage in Delta Lake.

## Overview

The ClaimX pipeline processes events from a source EventHub namespace containing ClaimX project management events. The event ingester consumes raw events, then events are enriched with API data and associated media files are downloaded. Unlike the Verisk pipeline, ClaimX requires API enrichment to fetch entity data before any downloads can occur.

## Pipeline Flow Diagram

```mermaid
flowchart TB
    %% Event Source
    SourceEventHub[("Source EventHub<br/>Namespace")]

    %% Main Pipeline Workers
    Ingester[Event Ingester Worker]
    Enricher[Enrichment Worker]
    Downloader[Download Worker]
    Uploader[Upload Worker]
    ResultProc[Result Processor]

    %% Delta Writers (Parallel Processing)
    subgraph DeltaWriters["Delta Writers (Parallel Consumer Group)"]
        EntityWriter[Entity Delta Writer<br/>7 Entity Tables]
        EventWriter[Events Delta Writer<br/>1 Events Table]
    end

    %% Topics
    EventsTopic("(claimx.events)")
    EnrichPending("(claimx.enrichment_pending)")
    Enriched("(claimx.enriched)")
    DownloadsPending("(claimx.downloads_pending)")
    DownloadsCached("(claimx.downloads_cached)")
    DownloadsResults("(claimx.downloads_results)")

    %% Delta Tables
    EventsTable[("claimx_events")]
    ProjectsTable[("claimx_projects")]
    ContactsTable[("claimx_contacts")]
    MediaTable[("claimx_media")]
    TasksTable[("claimx_tasks")]
    TaskTemplatesTable[("claimx_task_templates")]
    ExternalLinksTable[("claimx_external_links")]
    VideoCollabTable[("claimx_video_collaboration")]
    AttachmentsTable[("claimx_attachments")]

    %% Error Handling
    subgraph ErrorHandling["Error Handling & Retries"]
        RetryTopics("(claimx.*.retry.{1,2,3,4})<br/>Exponential Backoff")
        DLQ("(claimx.dlq)<br/>Dead Letter Queue")
        RetryScheduler[Unified Retry Scheduler]
    end

    %% ClaimX API
    API[ClaimX API<br/>Handler Registry<br/>7+ Event Handlers]

    %% Source Flow
    SourceEventHub --> EventsTopic

    %% Main Flow
    EventsTopic --> Ingester
    Ingester -->|24h Dedup Cache| EnrichPending
    EnrichPending --> Enricher
    Enricher -->|API Enrichment| API
    API -->|Entity Data| Enricher

    %% Parallel Paths from Enricher
    Enricher -->|Entity Rows| Enriched
    Enricher -->|Download Tasks| DownloadsPending

    %% Delta Writers Path (Parallel)
    Enriched --> EntityWriter
    Enriched --> EventWriter
    EntityWriter --> ProjectsTable
    EntityWriter --> ContactsTable
    EntityWriter --> MediaTable
    EntityWriter --> TasksTable
    EntityWriter --> TaskTemplatesTable
    EntityWriter --> ExternalLinksTable
    EntityWriter --> VideoCollabTable
    EventWriter --> EventsTable

    %% Download/Upload Path
    DownloadsPending --> Downloader
    Downloader -->|10 Parallel<br/>Local Cache| DownloadsCached
    DownloadsCached --> Uploader
    Uploader -->|10 Parallel<br/>OneLake| DownloadsResults
    DownloadsResults --> ResultProc
    ResultProc -->|Batch: 2000 rows<br/>Timeout: 5s| AttachmentsTable

    %% Error Flows
    Enricher -.->|Transient Error| RetryTopics
    Enricher -.->|Permanent Error| DLQ
    Downloader -.->|Error| RetryTopics
    Uploader -.->|Error| RetryTopics
    RetryTopics --> RetryScheduler
    RetryScheduler -.->|Route Back| EnrichPending
    RetryScheduler -.->|Route Back| DownloadsPending

    %% Styling
    classDef ingestion fill:#4A90E2,stroke:#333,stroke-width:2px,color:#fff
    classDef enrichment fill:#50C878,stroke:#333,stroke-width:2px,color:#fff
    classDef download fill:#FF8C42,stroke:#333,stroke-width:2px,color:#fff
    classDef upload fill:#9B59B6,stroke:#333,stroke-width:2px,color:#fff
    classDef storage fill:#95A5A6,stroke:#333,stroke-width:2px,color:#fff
    classDef error fill:#E74C3C,stroke:#333,stroke-width:2px,color:#fff
    classDef api fill:#F39C12,stroke:#333,stroke-width:2px,color:#fff

    class SourceEventHub source
    class Ingester ingestion
    class Enricher enrichment
    class Downloader download
    class Uploader upload
    class EntityWriter,EventWriter,ResultProc storage
    class RetryTopics,DLQ,RetryScheduler error
    class API api
```

## Component Details

### Event Ingester Worker
- **Consumes from**: `claimx.events` topic (source EventHub namespace)
- **Produces to**: `claimx.enrichment_pending`
- **Function**: Ingests raw events and creates enrichment tasks
- **Deduplication**: 24-hour hybrid cache (in-memory + blob storage) based on event_id
- **Key Logic**: All events trigger enrichment (not just file events)
- **File**: `src/pipeline/claimx/workers/event_ingester.py`

### Enrichment Worker
- **Consumes from**: `claimx.enrichment_pending`
- **Produces to**:
  - `claimx.enriched` (entity rows for Delta)
  - `claimx.downloads_pending` (download tasks)
- **Function**: Enriches events with ClaimX API data
- **Handler Registry**: Routes events by type to 7+ specialized handlers
- **Concurrency**: Single-task processing (Delta writer handles batching)
- **API Client**: Circuit breaker pattern with configurable timeout
- **File**: `src/pipeline/claimx/workers/enrichment_worker.py`

### Handler System
- **Base Handler**: `src/pipeline/claimx/handlers/base.py`
- **Registry**: Dynamic handler registration by event type
- **Handlers Include**:
  - Project handlers (create, update)
  - Contact handlers
  - Media handlers
  - Task handlers
  - Video collaboration handlers
  - External link handlers
- **Output**: EntityRowsMessage containing rows for all 7 entity types

### Delta Writers (Parallel Processing)
- **Consumer Group**: Separate from enrichment worker
- **Entity Writer**:
  - **File**: `src/pipeline/claimx/writers/delta_entities.py`
  - **Tables**: 7 entity tables (projects, contacts, media, tasks, task_templates, external_links, video_collaboration)
  - **Function**: Writes entity data with upsert logic
- **Events Writer**:
  - **File**: `src/pipeline/claimx/writers/delta_events.py`
  - **Table**: claimx_events
  - **Function**: Writes raw event records

### Download Worker
- **Consumes from**: `claimx.downloads_pending`
- **Produces to**: `claimx.downloads_cached`
- **Function**: Downloads media files from ClaimX URLs
- **Concurrency**: 10 parallel downloads
- **Caching**: Local filesystem cache before upload
- **File**: `src/pipeline/claimx/workers/download_worker.py`

### Upload Worker
- **Consumes from**: `claimx.downloads_cached`
- **Produces to**: `claimx.downloads_results`
- **Function**: Uploads files to OneLake storage
- **Concurrency**: 10 parallel uploads
- **File**: `src/pipeline/claimx/workers/upload_worker.py`

### Result Processor
- **Consumes from**: `claimx.downloads_results`
- **Writes to**: Delta table `claimx_attachments`
- **Function**: Batches download results for Delta Lake
- **Batching**: 2000 records or 5-second timeout
- **Consumer Group**: Separate from other workers

## Error Handling

### Retry Strategy
- **Retry Topics**: `claimx.*.retry.{1,2,3,4}`
- **Backoff**: Exponential with delays configured per domain
- **Max Retries**: 4 attempts (configurable)
- **Classification**:
  - `TRANSIENT` → Retry with backoff
  - `PERMANENT` → Send to DLQ immediately
  - `CIRCUIT_OPEN` → Reprocess when circuit closes
  - `AUTH` → Retry (may recover if token refreshes)

### Dead Letter Queue
- **Topic**: `claimx.dlq`
- **Purpose**: Permanent failures after max retries
- **Handling**: Manual review and reprocessing via DLQ CLI
- **File**: `src/pipeline/claimx/dlq/handler.py`

### Unified Retry Scheduler
- **Function**: Routes retry messages back to pending topics
- **Logic**: Inspects retry count and routes accordingly
- **Files**: `src/pipeline/claimx/retry/`

## Data Flow Summary

1. **Ingestion**: Event Ingester consumes from source EventHub claimx.events topic → Deduplication → Produces to enrichment_pending
3. **Enrichment**: Enrichment Worker calls ClaimX API via handler registry → Produces entity rows and download tasks
4. **Parallel Delta Writes**: Entity and Events writers (separate consumer groups) consume from enriched topic → Write to 8 Delta tables
5. **Download**: Download Worker fetches media files → Caches locally
6. **Upload**: Upload Worker sends files to OneLake → Produces results
7. **Result Storage**: Result Processor batches results → Writes to attachments table
8. **Error Recovery**: Failed tasks route through retry topics → Back to pending or DLQ

## Key Differences from Verisk Pipeline

- **API Enrichment Required**: ClaimX requires API calls to get entity data before downloads
- **Multiple Entity Tables**: 7 entity tables vs Verisk's 2 tables (events + attachments)
- **Handler Registry**: Event type routing system for specialized enrichment logic
- **No Plugin System**: Direct handler execution instead of extensible plugins
- **Single Domain**: No multi-domain routing in upload worker

## Configuration

### Event Source
```bash
# Source EventHub namespace (where raw events originate)
SOURCE_EVENTHUB_NAMESPACE_CONNECTION_STRING="Endpoint=sb://..."
```

### Key Environment Variables
- `PIPELINE_DOMAIN=claimx`
- `SOURCE_EVENTHUB_NAMESPACE_CONNECTION_STRING`: Source EventHub namespace connection string
- `EVENTHUB_NAMESPACE_CONNECTION_STRING`: Internal EventHub namespace connection string
- `CLAIMX_EVENTS_TABLE_PATH`: Path to claimx_events Delta table (for dedup)
- `CLAIMX_API_URL`: ClaimX API endpoint
- `CLAIMX_API_TOKEN`: API authentication token
- `CLAIMX_API_CONCURRENCY`: Max concurrent API calls
- Delta table paths: `CLAIMX_{ENTITY}_TABLE_PATH`

See `src/config/pipeline_config.py` for complete configuration options.

## Operational Characteristics

### Scaling
- **Event Ingester**: Horizontal scaling supported (shares events topic consumption)
- **Enrichment Worker**: Horizontal scaling supported (separate instances)
- **Delta Writers**: Independent scaling (different consumer groups)
- **Download/Upload Workers**: Horizontal scaling with partition distribution

### Performance
- **Deduplication**: In-memory cache with blob storage persistence
- **API Calls**: Circuit breaker prevents cascading failures
- **Concurrency**: 10 parallel downloads/uploads per worker instance
- **Batching**: Delta writers handle batching internally (larger batches: 2000 rows vs Verisk's 100)
- **Caching**: Local file cache reduces re-download overhead

### Monitoring
- Health check servers on all workers (configurable ports)
- Prometheus metrics for processing rates and errors
- Cycle logging every 30 seconds with success/failure counts
- Structured logging with trace context propagation
