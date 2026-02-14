"""ClaimX domain worker runners.

Contains all runner functions for ClaimX pipeline workers:
- Event ingestion (Event Hub)
- Enrichment with entity extraction
- Download/Upload
- Delta writes (events and entities)
- Result processing
- Retry scheduling
"""

import asyncio
import logging
from pathlib import Path

from pipeline.runners.common import (
    execute_worker_with_producer,
    execute_worker_with_shutdown,
)

logger = logging.getLogger(__name__)


async def run_claimx_event_ingester(
    kafka_config,
    shutdown_event: asyncio.Event,
    instance_id: int | None = None,
):
    """ClaimX event ingester worker."""
    from pipeline.claimx.workers.event_ingester import ClaimXEventIngesterWorker

    worker = ClaimXEventIngesterWorker(
        config=kafka_config,
        domain="claimx",
        instance_id=instance_id,
    )
    await execute_worker_with_shutdown(
        worker,
        stage_name="claimx-ingester",
        shutdown_event=shutdown_event,
        instance_id=instance_id,
    )


async def run_claimx_enrichment_worker(
    kafka_config,
    shutdown_event: asyncio.Event,
    enable_delta_writes: bool = True,
    claimx_projects_table_path: str = "",
    instance_id: int | None = None,
):
    """ClaimX enrichment worker with entity extraction."""
    from pipeline.claimx.workers.enrichment_worker import (
        ClaimXEnrichmentWorker,
    )

    worker = ClaimXEnrichmentWorker(
        config=kafka_config,
        domain="claimx",
        enable_delta_writes=enable_delta_writes,
        projects_table_path=claimx_projects_table_path,
        instance_id=instance_id,
    )

    await execute_worker_with_shutdown(
        worker,
        stage_name="claimx-enricher",
        shutdown_event=shutdown_event,
        instance_id=instance_id,
    )


async def run_claimx_download_worker(
    kafka_config,
    shutdown_event: asyncio.Event,
    instance_id: int | None = None,
):
    """ClaimX download worker."""
    from pipeline.claimx.workers.download_worker import ClaimXDownloadWorker

    worker = ClaimXDownloadWorker(
        config=kafka_config,
        domain="claimx",
        temp_dir=Path(kafka_config.temp_dir),
        instance_id=instance_id,
    )
    await execute_worker_with_shutdown(
        worker,
        stage_name="claimx-downloader",
        shutdown_event=shutdown_event,
        instance_id=instance_id,
    )


async def run_claimx_upload_worker(
    kafka_config,
    shutdown_event: asyncio.Event,
    instance_id: int | None = None,
):
    """ClaimX upload worker."""
    from pipeline.claimx.workers.upload_worker import ClaimXUploadWorker

    worker = ClaimXUploadWorker(config=kafka_config, domain="claimx", instance_id=instance_id)

    await execute_worker_with_shutdown(
        worker,
        stage_name="claimx-uploader",
        shutdown_event=shutdown_event,
        instance_id=instance_id,
    )


async def run_claimx_result_processor(
    kafka_config,
    pipeline_config,
    shutdown_event: asyncio.Event,
    instance_id: int | None = None,
):
    """ClaimX result processor."""
    from pipeline.claimx.workers.result_processor import ClaimXResultProcessor

    processor = ClaimXResultProcessor(
        config=kafka_config,
        inventory_table_path=pipeline_config.claimx_inventory_table_path,
        instance_id=instance_id,
    )
    await execute_worker_with_shutdown(
        processor,
        "claimx-result-processor",
        shutdown_event,
        instance_id=instance_id,
    )


async def run_claimx_delta_events_worker(
    kafka_config,
    events_table_path: str,
    shutdown_event: asyncio.Event,
    instance_id: int | None = None,
):
    """Consumes events from claimx events topic and writes to claimx_events Delta table.
    Runs independently of ClaimXEventIngesterWorker with its own consumer group."""
    from pipeline.claimx.workers.delta_events_worker import (
        ClaimXDeltaEventsWorker,
    )
    from pipeline.common.producer import MessageProducer

    await execute_worker_with_producer(
        worker_class=ClaimXDeltaEventsWorker,
        producer_class=MessageProducer,
        kafka_config=kafka_config,
        domain="claimx",
        stage_name="claimx-delta-writer",
        shutdown_event=shutdown_event,
        worker_kwargs={"events_table_path": events_table_path},
        producer_worker_name="delta_events_writer",
        instance_id=instance_id,
    )


async def run_claimx_retry_scheduler(
    kafka_config,
    shutdown_event: asyncio.Event,
    instance_id: int | None = None,
):
    """Unified retry scheduler for all ClaimX retry types.
    Routes messages from claimx.retry topic to target topics based on headers."""
    from pipeline.common.retry.unified_scheduler import UnifiedRetryScheduler

    scheduler = UnifiedRetryScheduler(
        config=kafka_config,
        domain="claimx",
        target_topic_keys=["downloads_pending", "enrichment_pending", "downloads_results"],
        persistence_dir=kafka_config.retry_persistence_dir,
    )
    await execute_worker_with_shutdown(
        scheduler,
        stage_name="claimx-retry-scheduler",
        shutdown_event=shutdown_event,
        instance_id=instance_id,
    )


async def run_claimx_entity_delta_worker(
    kafka_config,
    projects_table_path: str,
    contacts_table_path: str,
    media_table_path: str,
    tasks_table_path: str,
    task_templates_table_path: str,
    external_links_table_path: str,
    video_collab_table_path: str,
    shutdown_event: asyncio.Event,
    instance_id: int | None = None,
):
    """Consumes EntityRowsMessage from claimx.entities.rows and writes to Delta tables."""
    from pipeline.claimx.workers.entity_delta_worker import (
        ClaimXEntityDeltaWorker,
    )

    worker = ClaimXEntityDeltaWorker(
        config=kafka_config,
        domain="claimx",
        projects_table_path=projects_table_path,
        contacts_table_path=contacts_table_path,
        media_table_path=media_table_path,
        tasks_table_path=tasks_table_path,
        task_templates_table_path=task_templates_table_path,
        external_links_table_path=external_links_table_path,
        video_collab_table_path=video_collab_table_path,
        instance_id=instance_id,
    )
    await execute_worker_with_shutdown(
        worker,
        "claimx-entity-writer",
        shutdown_event,
        instance_id=instance_id,
    )
