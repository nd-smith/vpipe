# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
#
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""ClaimX domain worker runners.

Contains all runner functions for ClaimX pipeline workers:
- Event ingestion from Eventhouse
- Enrichment with entity extraction
- Download/Upload
- Delta writes (events and entities)
- Result processing
- Retry scheduling
"""

import asyncio
import logging
import os
from typing import Optional

from kafka_pipeline.runners.common import (
    execute_poller_with_shutdown,
    execute_worker_with_producer,
    execute_worker_with_shutdown,
)

logger = logging.getLogger(__name__)


async def run_claimx_eventhouse_poller(
    pipeline_config,
    shutdown_event: asyncio.Event,
):
    """Polls Eventhouse for claimx events and produces to claimx.events.raw topic.
    Deduplication handled by daily Fabric maintenance job."""
    from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
    from kafka_pipeline.common.eventhouse.kql_client import EventhouseConfig
    from kafka_pipeline.common.eventhouse.poller import KQLEventPoller, PollerConfig

    claimx_eventhouse = pipeline_config.claimx_eventhouse
    if not claimx_eventhouse:
        raise ValueError(
            "ClaimX Eventhouse configuration required for claimx-poller worker. "
            "Set in config.yaml under 'claimx_eventhouse:' or via CLAIMX_EVENTHOUSE_* env vars."
        )

    eventhouse_config = EventhouseConfig(
        cluster_url=claimx_eventhouse.cluster_url,
        database=claimx_eventhouse.database,
        query_timeout_seconds=claimx_eventhouse.query_timeout_seconds,
    )

    local_kafka_config = pipeline_config.local_kafka.to_kafka_config()
    claimx_kafka_config = local_kafka_config
    if "claimx" not in claimx_kafka_config.claimx or not claimx_kafka_config.claimx:
        claimx_kafka_config.claimx = {"topics": {}}
    if "topics" not in claimx_kafka_config.claimx:
        claimx_kafka_config.claimx["topics"] = {}
    claimx_kafka_config.claimx["topics"]["events"] = claimx_eventhouse.events_topic

    poller_config = PollerConfig(
        eventhouse=eventhouse_config,
        kafka=claimx_kafka_config,
        event_schema_class=ClaimXEventMessage,
        domain="claimx",
        poll_interval_seconds=claimx_eventhouse.poll_interval_seconds,
        batch_size=claimx_eventhouse.batch_size,
        source_table=claimx_eventhouse.source_table,
        column_mapping={
            "event_type": "event_type",
            "event_subtype": "event_subtype",
            "timestamp": "timestamp",
            "source_system": "source_system",
            "payload": "payload",
            "attachments": "attachments",
        },
        events_table_path=claimx_eventhouse.claimx_events_table_path,
        backfill_start_stamp=claimx_eventhouse.backfill_start_stamp,
        backfill_stop_stamp=claimx_eventhouse.backfill_stop_stamp,
        bulk_backfill=claimx_eventhouse.bulk_backfill,
    )

    await execute_poller_with_shutdown(
        KQLEventPoller,
        poller_config,
        stage_name="claimx-poller",
        shutdown_event=shutdown_event,
    )


async def run_claimx_event_ingester(
    kafka_config,
    shutdown_event: asyncio.Event,
    instance_id: Optional[int] = None,
):
    """ClaimX event ingester worker."""
    from kafka_pipeline.claimx.workers.event_ingester import ClaimXEventIngesterWorker

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
    pipeline_config,
    shutdown_event: asyncio.Event,
    instance_id: Optional[int] = None,
    simulation_mode: bool = False,
    simulation_config=None,
):
    """ClaimX enrichment worker with entity extraction.

    Args:
        kafka_config: Kafka configuration
        pipeline_config: Pipeline configuration
        shutdown_event: Shutdown event for graceful shutdown
        instance_id: Optional instance ID for parallel workers
        simulation_mode: Enable simulation mode with mock API client
        simulation_config: Simulation configuration (required if simulation_mode is True)
    """
    if simulation_mode:
        from kafka_pipeline.simulation import create_simulation_enrichment_worker

        if simulation_config is None:
            raise ValueError("simulation_config is required when simulation_mode=True")

        logger.info("Starting ClaimX enrichment worker in SIMULATION MODE")
        logger.info(f"Simulation storage path: {simulation_config.local_storage_path}")

        # Use factory to create worker with mock dependencies
        worker = create_simulation_enrichment_worker(
            config=kafka_config,
            simulation_config=simulation_config,
            domain="claimx",
        )
    else:
        from kafka_pipeline.claimx.workers.enrichment_worker import ClaimXEnrichmentWorker

        worker = ClaimXEnrichmentWorker(
            config=kafka_config,
            domain="claimx",
            enable_delta_writes=pipeline_config.enable_delta_writes,
            projects_table_path=pipeline_config.claimx_projects_table_path,
            instance_id=instance_id,
        )

    await execute_worker_with_shutdown(
        worker,
        stage_name="claimx-enricher",
        shutdown_event=shutdown_event,
        stop_method="stop",
        instance_id=instance_id,
    )


async def run_claimx_download_worker(
    kafka_config,
    shutdown_event: asyncio.Event,
    instance_id: Optional[int] = None,
    simulation_mode: bool = False,
    simulation_config=None,
):
    """ClaimX download worker.

    Args:
        kafka_config: Kafka configuration
        shutdown_event: Shutdown event for graceful shutdown
        instance_id: Optional instance ID for parallel workers
        simulation_mode: Enable simulation mode with mock dependencies
        simulation_config: Simulation configuration (required if simulation_mode is True)
    """
    from kafka_pipeline.claimx.workers.download_worker import ClaimXDownloadWorker

    if simulation_mode and simulation_config is None:
        raise ValueError("simulation_config is required when simulation_mode=True")

    if simulation_mode:
        logger.info("Starting ClaimX download worker in SIMULATION MODE")
        logger.info(f"Simulation config: {simulation_config}")

    worker = ClaimXDownloadWorker(
        config=kafka_config,
        domain="claimx",
        instance_id=instance_id,
        simulation_config=simulation_config if simulation_mode else None,
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
    instance_id: Optional[int] = None,
    simulation_mode: bool = False,
    simulation_config=None,
):
    """ClaimX upload worker.

    Args:
        kafka_config: Kafka configuration
        shutdown_event: Shutdown event for graceful shutdown
        instance_id: Optional instance ID for parallel workers
        simulation_mode: Enable simulation mode with local storage
        simulation_config: Simulation configuration (required if simulation_mode is True)
    """
    if simulation_mode:
        from kafka_pipeline.simulation import create_simulation_upload_worker

        if simulation_config is None:
            raise ValueError("simulation_config is required when simulation_mode=True")

        logger.info("Starting ClaimX upload worker in SIMULATION MODE")
        logger.info(f"Simulation storage path: {simulation_config.local_storage_path}")

        # Use factory to create worker with local storage
        worker = create_simulation_upload_worker(
            config=kafka_config,
            simulation_config=simulation_config,
            domain="claimx",
        )
    else:
        from kafka_pipeline.claimx.workers.upload_worker import ClaimXUploadWorker

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
    instance_id: Optional[int] = None,
):
    """ClaimX result processor."""
    from kafka_pipeline.claimx.workers.result_processor import ClaimXResultProcessor
    from core.logging.context import set_log_context

    set_log_context(stage="claimx-result-processor")
    logger.info("Starting ClaimX Result Processor...")

    processor = ClaimXResultProcessor(
        config=kafka_config,
        inventory_table_path=pipeline_config.claimx_inventory_table_path,
        instance_id=instance_id,
    )

    async def shutdown_watcher():
        """Wait for shutdown signal and stop processor gracefully."""
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping claimx result processor...")
        await processor.stop()

    # Start shutdown watcher alongside processor
    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await processor.start()
    finally:
        # Guard against event loop being closed during shutdown
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            # RuntimeError occurs if event loop is closed
            pass
        # Clean up resources after processor exits
        await processor.stop()


async def run_claimx_delta_events_worker(
    kafka_config,
    events_table_path: str,
    shutdown_event: asyncio.Event,
    instance_id: Optional[int] = None,
):
    """Consumes events from claimx events topic and writes to claimx_events Delta table.
    Runs independently of ClaimXEventIngesterWorker with its own consumer group."""
    from kafka_pipeline.common.producer import BaseKafkaProducer
    from kafka_pipeline.claimx.workers.delta_events_worker import ClaimXDeltaEventsWorker

    await execute_worker_with_producer(
        worker_class=ClaimXDeltaEventsWorker,
        producer_class=BaseKafkaProducer,
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
    instance_id: Optional[int] = None,
):
    """Unified retry scheduler for all ClaimX retry types.
    Routes messages from claimx.retry topic to target topics based on headers."""
    from kafka_pipeline.common.producer import BaseKafkaProducer
    from kafka_pipeline.common.retry.unified_scheduler import UnifiedRetryScheduler

    await execute_worker_with_producer(
        worker_class=UnifiedRetryScheduler,
        producer_class=BaseKafkaProducer,
        kafka_config=kafka_config,
        domain="claimx",
        stage_name="claimx-retry-scheduler",
        shutdown_event=shutdown_event,
        producer_worker_name="unified_retry_scheduler",
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
    instance_id: Optional[int] = None,
):
    """Consumes EntityRowsMessage from claimx.entities.rows and writes to Delta tables."""
    from kafka_pipeline.claimx.workers.entity_delta_worker import ClaimXEntityDeltaWorker

    from core.logging.context import set_log_context

    set_log_context(stage="claimx-entity-writer")
    logger.info("Starting ClaimX Entity Delta worker...")

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

    async def shutdown_watcher():
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping claimx entity delta worker...")
        await worker.stop()

    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await worker.start()
    finally:
        # Guard against event loop being closed during shutdown
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            pass
        await worker.stop()
