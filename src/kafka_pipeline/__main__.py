"""
Entry point for running Kafka pipeline workers.

Usage:
    # Run all workers
    python -m kafka_pipeline

    # Run specific xact worker
    python -m kafka_pipeline --worker xact-poller
    python -m kafka_pipeline --worker xact-event-ingester
    python -m kafka_pipeline --worker xact-local-ingester
    python -m kafka_pipeline --worker xact-delta-writer
    python -m kafka_pipeline --worker xact-delta-retry
    python -m kafka_pipeline --worker xact-download
    python -m kafka_pipeline --worker xact-upload
    python -m kafka_pipeline --worker xact-result-processor

    # Run specific claimx worker
    python -m kafka_pipeline --worker claimx-poller
    python -m kafka_pipeline --worker claimx-ingester
    python -m kafka_pipeline --worker claimx-enricher
    python -m kafka_pipeline --worker claimx-downloader
    python -m kafka_pipeline --worker claimx-uploader
    python -m kafka_pipeline --worker claimx-uploader
    python -m kafka_pipeline --worker claimx-result-processor
    python -m kafka_pipeline --worker claimx-delta-writer
    python -m kafka_pipeline --worker claimx-delta-retry
    python -m kafka_pipeline --worker claimx-entity-writer

    # Run dummy data source for testing (generates synthetic events)
    python -m kafka_pipeline --worker dummy-source --dev

    # Run multiple worker instances (for horizontal scaling)
    python -m kafka_pipeline --worker xact-download --count 4
    python -m kafka_pipeline --worker xact-upload -c 3

    # Run with metrics server
    python -m kafka_pipeline --metrics-port 8000

    # Run in development mode (local Kafka only, no Event Hub/Eventhouse)
    python -m kafka_pipeline --dev

Event Source Configuration:
    Set EVENT_SOURCE environment variable:
    - eventhub (default): Use Azure Event Hub via Kafka protocol
    - eventhouse: Poll Microsoft Fabric Eventhouse
    - dummy: Generate synthetic test data (use --worker dummy-source)

Architecture:
    xact Domain:
        Event Source → events.raw topic
            → xact-event-ingester → downloads.pending → download worker →
              downloads.cached → upload worker → downloads.results → result processor
            → xact-delta-writer → xact_events Delta table (parallel)
              ↓ (on failure)
            → delta-events.retry.* topics → xact-delta-retry → retry write or DLQ

    claimx Domain:
        Eventhouse → claimx-poller → claimx.events.raw → claimx-ingester →
        enrichment.pending → enrichment worker → entity tables + downloads.pending →
        download worker → downloads.cached → upload worker → downloads.results
            → claimx-delta-writer → claimx_events Delta table (parallel)
              ↓ (on failure)
            → claimx-delta-events.retry.* topics → claimx-delta-retry → retry write or DLQ
"""

import argparse
import asyncio
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Any, Callable, Coroutine, Optional

from dotenv import load_dotenv
from prometheus_client import start_http_server

from core.logging.context import set_log_context
from core.logging.setup import get_logger, setup_logging, setup_multi_worker_logging

# Project root directory (where .env file is located)
# __main__.py is at src/kafka_pipeline/__main__.py, so root is 3 levels up
PROJECT_ROOT = Path(__file__).parent.parent.parent

# Worker stages for multi-worker logging
WORKER_STAGES = [
    "xact-poller", "xact-event-ingester", "xact-local-ingester", "xact-delta-writer", "xact-delta-retry", "xact-download", "xact-upload", "xact-result-processor",
    "claimx-poller", "claimx-ingester", "claimx-enricher", "claimx-downloader", "claimx-uploader", "claimx-result-processor",
    "claimx-delta-writer", "claimx-delta-retry", "claimx-entity-writer",
    "dummy-source",
]

# Placeholder logger until setup_logging() is called in main()
# This allows module-level logging before full initialization
logger = logging.getLogger(__name__)

# Global shutdown event for graceful batch completion
# Set by signal handlers, checked by workers to finish current batch before exiting
_shutdown_event: Optional[asyncio.Event] = None


def get_shutdown_event() -> asyncio.Event:
    global _shutdown_event
    if _shutdown_event is None:
        _shutdown_event = asyncio.Event()
    return _shutdown_event


async def run_worker_pool(
    worker_fn: Callable[..., Coroutine[Any, Any, None]],
    count: int,
    worker_name: str,
    *args: Any,
    **kwargs: Any,
) -> None:
    """Run multiple instances of a worker concurrently.
    Each instance joins the same consumer group for automatic partition distribution."""
    logger.info(f"Starting {count} instances of {worker_name}...")

    tasks = []
    for i in range(count):
        task = asyncio.create_task(
            worker_fn(*args, **kwargs),
            name=f"{worker_name}-{i}",
        )
        tasks.append(task)

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info(f"Worker pool {worker_name} cancelled, shutting down...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Kafka pipeline workers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run all workers (Event Hub → Local Kafka pipeline)
    python -m kafka_pipeline

    # Run specific xact worker
    python -m kafka_pipeline --worker xact-download

    # Run specific claimx worker
    python -m kafka_pipeline --worker claimx-enricher

    # Run in development mode (local Kafka only)
    python -m kafka_pipeline --dev

    # Run with custom metrics port
    python -m kafka_pipeline --metrics-port 9090
        """,
    )

    parser.add_argument(
        "--worker",
        choices=[
            "xact-poller", "xact-event-ingester", "xact-local-ingester", "xact-delta-writer", "xact-delta-retry", "xact-download", "xact-upload", "xact-result-processor",
            "claimx-poller", "claimx-ingester", "claimx-delta-writer", "claimx-delta-retry", "claimx-entity-writer", "claimx-enricher", "claimx-downloader", "claimx-uploader", "claimx-result-processor",
            "dummy-source",
            "all"
        ],
        default="all",
        help="Which worker(s) to run (default: all)",
    )

    parser.add_argument(
        "--metrics-port",
        type=int,
        default=8000,
        help="Port for Prometheus metrics server (default: 8000)",
    )

    parser.add_argument(
        "--dev",
        action="store_true",
        help="Development mode: use local Kafka only (no Event Hub/Eventhouse credentials required)",
    )

    parser.add_argument(
        "--no-delta",
        action="store_true",
        help="Disable Delta Lake writes (for testing)",
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )

    parser.add_argument(
        "--log-dir",
        type=str,
        default=None,
        help="Log directory path (default: from LOG_DIR env var or ./logs)",
    )

    parser.add_argument(
        "--count", "-c",
        type=int,
        default=1,
        help="Number of worker instances to run concurrently (default: 1). "
             "Multiple instances share the same consumer group for automatic partition distribution.",
    )

    return parser.parse_args()


async def run_event_ingester(
    eventhub_config,
    local_kafka_config,
    domain: str = "xact",
):
    """Reads events from Event Hub and produces download tasks to local Kafka.
    Delta Lake writes are handled by a separate DeltaEventsWorker."""
    from kafka_pipeline.xact.workers.event_ingester import EventIngesterWorker

    set_log_context(stage="xact-event-ingester")
    logger.info("Starting xact Event Ingester worker...")

    worker = EventIngesterWorker(
        config=eventhub_config,
        domain=domain,
        producer_config=local_kafka_config,
    )
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping event ingester...")
        await worker.stop()

    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await worker.start()
    finally:
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            pass
        await worker.stop()


async def run_eventhouse_poller(pipeline_config):
    """Polls Microsoft Fabric Eventhouse for events and produces to events.raw topic.
    Deduplication handled by daily Fabric maintenance job."""
    from kafka_pipeline.common.eventhouse.kql_client import EventhouseConfig
    from kafka_pipeline.common.eventhouse.poller import KQLEventPoller, PollerConfig

    set_log_context(stage="xact-poller")
    logger.info("Starting xact Eventhouse Poller...")

    eventhouse_source = pipeline_config.xact_eventhouse
    if not eventhouse_source:
        raise ValueError("Xact Eventhouse configuration required for EVENT_SOURCE=eventhouse")

    eventhouse_config = EventhouseConfig(
        cluster_url=eventhouse_source.cluster_url,
        database=eventhouse_source.database,
        query_timeout_seconds=eventhouse_source.query_timeout_seconds,
    )

    poller_config = PollerConfig(
        eventhouse=eventhouse_config,
        kafka=pipeline_config.local_kafka.to_kafka_config(),
        poll_interval_seconds=eventhouse_source.poll_interval_seconds,
        batch_size=eventhouse_source.batch_size,
        source_table=eventhouse_source.source_table,
        events_table_path=eventhouse_source.xact_events_table_path,
        backfill_start_stamp=eventhouse_source.backfill_start_stamp,
        backfill_stop_stamp=eventhouse_source.backfill_stop_stamp,
        bulk_backfill=eventhouse_source.bulk_backfill,
    )

    shutdown_event = get_shutdown_event()

    async def shutdown_watcher(poller: KQLEventPoller):
        """Wait for shutdown signal and stop poller gracefully."""
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping eventhouse poller...")
        await poller.stop()

    async with KQLEventPoller(poller_config) as poller:
        # Start shutdown watcher alongside poller
        watcher_task = asyncio.create_task(shutdown_watcher(poller))

        try:
            await poller.run()
        finally:
            # Guard against event loop being closed during shutdown
            try:
                watcher_task.cancel()
                await watcher_task
            except (asyncio.CancelledError, RuntimeError):
                # RuntimeError occurs if event loop is closed
                pass


async def run_delta_events_worker(kafka_config, events_table_path: str):
    """Consumes events from events.raw and writes to xact_events Delta table.
    Runs independently of EventIngesterWorker with its own consumer group."""
    from kafka_pipeline.common.producer import BaseKafkaProducer
    from kafka_pipeline.xact.workers.delta_events_worker import DeltaEventsWorker

    set_log_context(stage="xact-delta-writer")
    logger.info("Starting xact Delta Events worker...")

    producer = BaseKafkaProducer(
        config=kafka_config,
        domain="xact",
        worker_name="delta_events_writer",
    )
    await producer.start()

    worker = DeltaEventsWorker(
        config=kafka_config,
        producer=producer,
        events_table_path=events_table_path,
        domain="xact",
    )
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping delta events worker...")
        await worker.stop()

    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await worker.start()
    finally:
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            pass
        await worker.stop()
        await producer.stop()


async def run_delta_retry_scheduler(kafka_config, events_table_path: str):
    """Consumes failed batches from retry topics and attempts to write to Delta table after delay.
    Routes permanently failed batches to DLQ."""
    from kafka_pipeline.common.producer import BaseKafkaProducer
    from kafka_pipeline.xact.retry.scheduler import DeltaBatchRetryScheduler

    set_log_context(stage="xact-delta-retry")
    logger.info("Starting xact Delta Retry Scheduler...")

    producer = BaseKafkaProducer(
        config=kafka_config,
        domain="xact",
        worker_name="delta_retry_scheduler",
    )
    await producer.start()

    scheduler = DeltaBatchRetryScheduler(
        config=kafka_config,
        producer=producer,
        table_path=events_table_path,
    )
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        """Wait for shutdown signal and stop scheduler gracefully."""
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping delta retry scheduler...")
        await scheduler.stop()

    # Start shutdown watcher alongside scheduler
    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await scheduler.start()
    finally:
        # Guard against event loop being closed during shutdown
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            # RuntimeError occurs if event loop is closed
            pass
        # Clean up resources after scheduler exits
        await scheduler.stop()
        await producer.stop()


async def run_download_worker(kafka_config):
    from kafka_pipeline.xact.workers.download_worker import DownloadWorker

    set_log_context(stage="xact-download")
    logger.info("Starting xact Download worker...")

    worker = DownloadWorker(config=kafka_config, domain="xact")
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping download worker after current batch...")
        await worker.request_shutdown()

    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await worker.start()
    finally:
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            pass
        await worker.stop()


async def run_upload_worker(kafka_config):
    from kafka_pipeline.xact.workers.upload_worker import UploadWorker

    set_log_context(stage="xact-upload")
    logger.info("Starting xact Upload worker...")

    worker = UploadWorker(config=kafka_config, domain="xact")
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping upload worker after current batch...")
        await worker.request_shutdown()

    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await worker.start()
    finally:
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            pass
        await worker.stop()


async def run_result_processor(
    kafka_config,
    enable_delta_writes: bool = True,
    inventory_table_path: str = "",
    failed_table_path: str = "",
):
    """Reads download results and writes to Delta Lake tables.
    On Delta write failure, batches are routed to retry topics."""
    from kafka_pipeline.common.producer import BaseKafkaProducer
    from kafka_pipeline.xact.workers.result_processor import ResultProcessor

    set_log_context(stage="xact-result-processor")
    logger.info("Starting xact Result Processor worker...")

    if enable_delta_writes and not inventory_table_path:
        logger.error(
            "inventory_table_path is required for xact-result-processor "
            "when enable_delta_writes=True. Configure via DELTA_INVENTORY_TABLE_PATH "
            "environment variable or delta.xact.inventory_table_path in config.yaml."
        )
        raise ValueError("inventory_table_path is required when delta writes are enabled")

    producer = BaseKafkaProducer(
        config=kafka_config,
        domain="xact",
        worker_name="result_processor",
    )
    await producer.start()

    worker = ResultProcessor(
        config=kafka_config,
        producer=producer,
        inventory_table_path=inventory_table_path if enable_delta_writes else None,
        failed_table_path=failed_table_path if enable_delta_writes and failed_table_path else None,
        batch_size=2000,
        batch_timeout_seconds=5.0,
    )
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping result processor...")
        await worker.stop()

    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await worker.start()
    finally:
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            pass
        await worker.stop()
        await producer.stop()


async def run_claimx_eventhouse_poller(pipeline_config):
    """Polls Eventhouse for claimx events and produces to claimx.events.raw topic.
    Deduplication handled by daily Fabric maintenance job."""
    from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
    from kafka_pipeline.common.eventhouse.kql_client import EventhouseConfig
    from kafka_pipeline.common.eventhouse.poller import KQLEventPoller, PollerConfig

    set_log_context(stage="claimx-poller")
    logger.info("Starting ClaimX Eventhouse Poller...")

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

    shutdown_event = get_shutdown_event()

    async def shutdown_watcher(poller: KQLEventPoller):
        """Wait for shutdown signal and stop poller gracefully."""
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping claimx eventhouse poller...")
        await poller.stop()

    async with KQLEventPoller(poller_config) as poller:
        # Start shutdown watcher alongside poller
        watcher_task = asyncio.create_task(shutdown_watcher(poller))

        try:
            await poller.run()
        finally:
            # Guard against event loop being closed during shutdown
            try:
                watcher_task.cancel()
                await watcher_task
            except (asyncio.CancelledError, RuntimeError):
                # RuntimeError occurs if event loop is closed
                pass


async def run_local_event_ingester(
    local_kafka_config,
    domain: str = "xact",
):
    """Consumes from local Kafka events.raw topic and processes events to downloads.pending.
    Used in Eventhouse mode. Delta Lake writes handled by separate DeltaEventsWorker."""
    from kafka_pipeline.xact.workers.event_ingester import EventIngesterWorker

    set_log_context(stage="xact-event-ingester")
    logger.info("Starting xact Event Ingester (local Kafka mode)...")

    worker = EventIngesterWorker(
        config=local_kafka_config,
        domain=domain,
    )
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping event ingester...")
        await worker.stop()

    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await worker.start()
    finally:
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            pass
        await worker.stop()


async def run_claimx_event_ingester(
    kafka_config,
):
    from kafka_pipeline.claimx.workers.event_ingester import ClaimXEventIngesterWorker

    set_log_context(stage="claimx-ingester")
    logger.info("Starting ClaimX Event Ingester worker...")

    worker = ClaimXEventIngesterWorker(
        config=kafka_config,
        domain="claimx",
    )
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping claimx event ingester...")
        await worker.stop()

    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await worker.start()
    finally:
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            pass
        await worker.stop()


async def run_claimx_enrichment_worker(
    kafka_config,
    pipeline_config,
):
    from kafka_pipeline.claimx.workers.enrichment_worker import ClaimXEnrichmentWorker

    set_log_context(stage="claimx-enricher")
    logger.info("Starting ClaimX Enrichment worker...")


    worker = ClaimXEnrichmentWorker(
        config=kafka_config,
        domain="claimx",
        enable_delta_writes=pipeline_config.enable_delta_writes,
        projects_table_path=pipeline_config.claimx_projects_table_path,
    )
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping enrichment worker...")
        await worker.request_shutdown()

    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await worker.start()
    finally:
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            pass
        await worker.stop()


async def run_claimx_download_worker(kafka_config):
    from kafka_pipeline.claimx.workers.download_worker import ClaimXDownloadWorker

    set_log_context(stage="claimx-downloader")
    logger.info("Starting ClaimX Download worker...")

    worker = ClaimXDownloadWorker(config=kafka_config, domain="claimx")
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping claimx download worker...")
        await worker.stop()

    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await worker.start()
    finally:
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            pass
        await worker.stop()


async def run_claimx_upload_worker(kafka_config):
    from kafka_pipeline.claimx.workers.upload_worker import ClaimXUploadWorker

    set_log_context(stage="claimx-uploader")
    logger.info("Starting ClaimX Upload worker...")

    worker = ClaimXUploadWorker(config=kafka_config, domain="claimx")
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping claimx upload worker...")
        await worker.stop()

    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await worker.start()
    finally:
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            pass
        await worker.stop()


async def run_claimx_result_processor(
    kafka_config,
    pipeline_config,
):
    from kafka_pipeline.claimx.workers.result_processor import ClaimXResultProcessor

    set_log_context(stage="claimx-result-processor")
    logger.info("Starting ClaimX Result Processor...")

    processor = ClaimXResultProcessor(
        config=kafka_config,
        inventory_table_path=pipeline_config.claimx_inventory_table_path,
    )
    shutdown_event = get_shutdown_event()

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


async def run_all_workers(
    pipeline_config,
    enable_delta_writes: bool = True,
):
    """Run all pipeline workers concurrently.
    Architecture: events.raw → EventIngester → downloads.pending → DownloadWorker → ...
                  events.raw → DeltaEventsWorker → Delta table (parallel)"""
    from config.pipeline_config import EventSourceType

    logger.info("Starting all pipeline workers...")

    local_kafka_config = pipeline_config.local_kafka.to_kafka_config()

    tasks = []

    if pipeline_config.event_source == EventSourceType.EVENTHOUSE:
        events_table_path = (
            pipeline_config.xact_eventhouse.xact_events_table_path
            or pipeline_config.events_table_path
        )
    else:
        events_table_path = pipeline_config.events_table_path

    if pipeline_config.event_source == EventSourceType.EVENTHOUSE:
        tasks.append(
            asyncio.create_task(
                run_eventhouse_poller(pipeline_config),
                name="eventhouse-poller",
            )
        )
        tasks.append(
            asyncio.create_task(
                run_local_event_ingester(
                    local_kafka_config,
                    domain=pipeline_config.domain,
                ),
                name="xact-event-ingester",
            )
        )
        logger.info("Using Eventhouse as event source")
    else:
        eventhub_config = pipeline_config.eventhub.to_kafka_config()
        tasks.append(
            asyncio.create_task(
                run_event_ingester(
                    eventhub_config,
                    local_kafka_config,
                    domain=pipeline_config.domain,
                ),
                name="xact-event-ingester",
            )
        )
        logger.info("Using Event Hub as event source")

    if enable_delta_writes and events_table_path:
        tasks.append(
            asyncio.create_task(
                run_delta_events_worker(local_kafka_config, events_table_path),
                name="xact-delta-writer",
            )
        )
        logger.info("Delta events writer enabled")

        tasks.append(
            asyncio.create_task(
                run_delta_retry_scheduler(local_kafka_config, events_table_path),
                name="xact-delta-retry",
            )
        )
        logger.info("Delta retry scheduler enabled")

    tasks.extend([
        asyncio.create_task(
            run_download_worker(local_kafka_config),
            name="xact-download",
        ),
        asyncio.create_task(
            run_upload_worker(local_kafka_config),
            name="xact-upload",
        ),
        asyncio.create_task(
            run_result_processor(
                local_kafka_config,
                enable_delta_writes,
                inventory_table_path=pipeline_config.inventory_table_path,
                failed_table_path=pipeline_config.failed_table_path,
            ),
            name="xact-result-processor",
        ),
    ])

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Workers cancelled, shutting down...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def start_metrics_server(preferred_port: int) -> int:
    """Start Prometheus metrics server with automatic port fallback.
    Returns actual port number that the server is listening on."""
    import socket

    try:
        start_http_server(preferred_port)
        return preferred_port
    except OSError as e:
        if e.errno == 98:
            logger.info(f"Port {preferred_port} already in use, finding available port...")

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('', 0))
                s.listen(1)
                available_port = s.getsockname()[1]

            start_http_server(available_port)
            return available_port
        else:
            raise


def setup_signal_handlers(loop: asyncio.AbstractEventLoop):
    """Set up signal handlers for graceful shutdown.

    First CTRL+C: Sets shutdown event - workers finish current batch, flush data, commit offsets.
    Second CTRL+C: Forces immediate shutdown by cancelling all tasks.
    Note: Signal handlers not supported on Windows - KeyboardInterrupt used instead."""

    def handle_signal(sig):
        logger.info(f"Received signal {sig.name}, initiating graceful shutdown...")
        shutdown_event = get_shutdown_event()
        if not shutdown_event.is_set():
            shutdown_event.set()
        else:
            logger.warning("Received second signal, forcing immediate shutdown...")
            for task in asyncio.all_tasks(loop):
                task.cancel()

    if sys.platform == "win32":
        logger.debug("Signal handlers not supported on Windows, using KeyboardInterrupt")
        return

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: handle_signal(s))


def main():
    load_dotenv(PROJECT_ROOT / ".env")

    global logger
    args = parse_args()

    log_level = getattr(logging, args.log_level)

    json_logs = os.getenv("JSON_LOGS", "true").lower() in ("true", "1", "yes")

    log_dir_str = args.log_dir or os.getenv("LOG_DIR", "logs")
    log_dir = Path(log_dir_str)

    worker_id = os.getenv("WORKER_ID", f"kafka-{args.worker}")

    domain = "kafka"
    if args.worker != "all" and "-" in args.worker:
        domain_prefix = args.worker.split("-")[0]
        if domain_prefix in ("xact", "claimx"):
            domain = domain_prefix

    if args.worker == "all":
        setup_multi_worker_logging(
            workers=WORKER_STAGES,
            domain="kafka",
            log_dir=log_dir,
            json_format=json_logs,
            console_level=log_level,
        )
    else:
        setup_logging(
            name="kafka_pipeline",
            stage=args.worker,
            domain=domain,
            log_dir=log_dir,
            json_format=json_logs,
            console_level=log_level,
            worker_id=worker_id,
        )

    logger = get_logger(__name__)

    _debug_token_file = os.getenv("AZURE_TOKEN_FILE")
    if _debug_token_file:
        _debug_token_exists = Path(_debug_token_file).exists()
        logger.debug(
            "Auth configuration detected",
            extra={
                "project_root": str(PROJECT_ROOT),
                "token_file": _debug_token_file,
                "token_file_exists": _debug_token_exists,
            }
        )
        if not _debug_token_exists:
            _resolved = PROJECT_ROOT / _debug_token_file
            logger.debug(
                "Attempting to resolve token file path relative to project root",
                extra={
                    "resolved_path": str(_resolved),
                    "resolved_exists": _resolved.exists(),
                }
            )

    actual_port = start_metrics_server(args.metrics_port)
    if actual_port != args.metrics_port:
        logger.info(f"Metrics server started on port {actual_port} (fallback from {args.metrics_port})")
    else:
        logger.info(f"Metrics server started on port {actual_port}")

    if args.dev:
        logger.info("Running in DEVELOPMENT mode (local Kafka only)")
        from config.pipeline_config import (
            EventSourceType,
            LocalKafkaConfig,
            PipelineConfig,
        )

        local_config = LocalKafkaConfig.load_config()
        kafka_config = local_config.to_kafka_config()

        pipeline_config = PipelineConfig(
            event_source=EventSourceType.EVENTHUB,
            local_kafka=local_config,
        )

        eventhub_config = kafka_config
        local_kafka_config = kafka_config
    else:
        from config.pipeline_config import EventSourceType, get_pipeline_config

        try:
            pipeline_config = get_pipeline_config()
            local_kafka_config = pipeline_config.local_kafka.to_kafka_config()

            if pipeline_config.event_source == EventSourceType.EVENTHOUSE:
                logger.info("Running in PRODUCTION mode (Eventhouse + local Kafka)")
                eventhub_config = None
            else:
                logger.info("Running in PRODUCTION mode (Event Hub + local Kafka)")
                eventhub_config = pipeline_config.eventhub.to_kafka_config()
        except ValueError as e:
            logger.error(f"Configuration error: {e}")
            logger.error("Use --dev flag for local development without Event Hub/Eventhouse")
            sys.exit(1)

    enable_delta_writes = not args.no_delta

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    setup_signal_handlers(loop)

    try:
        if args.worker == "xact-poller":
            if pipeline_config.event_source != EventSourceType.EVENTHOUSE:
                logger.error("xact-poller requires EVENT_SOURCE=eventhouse")
                sys.exit(1)
            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_eventhouse_poller, args.count, "xact-poller",
                        pipeline_config,
                    )
                )
            else:
                loop.run_until_complete(run_eventhouse_poller(pipeline_config))
        elif args.worker == "xact-event-ingester":
            if pipeline_config.event_source == EventSourceType.EVENTHOUSE:
                if args.count > 1:
                    loop.run_until_complete(
                        run_worker_pool(
                            run_local_event_ingester, args.count, "xact-event-ingester",
                            local_kafka_config,
                            domain=pipeline_config.domain,
                        )
                    )
                else:
                    loop.run_until_complete(
                        run_local_event_ingester(
                            local_kafka_config,
                            domain=pipeline_config.domain,
                        )
                    )
            else:
                if args.count > 1:
                    loop.run_until_complete(
                        run_worker_pool(
                            run_event_ingester, args.count, "xact-event-ingester",
                            eventhub_config,
                            local_kafka_config,
                            domain=pipeline_config.domain,
                        )
                    )
                else:
                    loop.run_until_complete(
                        run_event_ingester(
                            eventhub_config,
                            local_kafka_config,
                            domain=pipeline_config.domain,
                        )
                    )
        elif args.worker == "xact-local-ingester":
            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_local_event_ingester, args.count, "xact-local-ingester",
                        local_kafka_config,
                        domain=pipeline_config.domain,
                    )
                )
            else:
                loop.run_until_complete(
                    run_local_event_ingester(
                        local_kafka_config,
                        domain=pipeline_config.domain,
                    )
                )
        elif args.worker == "xact-delta-writer":
            events_table_path = pipeline_config.events_table_path
            if pipeline_config.event_source == EventSourceType.EVENTHOUSE:
                events_table_path = (
                    pipeline_config.xact_eventhouse.xact_events_table_path
                    or events_table_path
                )
            if not events_table_path:
                logger.error("DELTA_EVENTS_TABLE_PATH is required for xact-delta-writer")
                sys.exit(1)
            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_delta_events_worker, args.count, "xact-delta-writer",
                        local_kafka_config, events_table_path,
                    )
                )
            else:
                loop.run_until_complete(
                    run_delta_events_worker(local_kafka_config, events_table_path)
                )
        elif args.worker == "xact-delta-retry":
            events_table_path = pipeline_config.events_table_path
            if pipeline_config.event_source == EventSourceType.EVENTHOUSE:
                events_table_path = (
                    pipeline_config.xact_eventhouse.xact_events_table_path
                    or events_table_path
                )
            if not events_table_path:
                logger.error("DELTA_EVENTS_TABLE_PATH is required for xact-delta-retry")
                sys.exit(1)
            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_delta_retry_scheduler, args.count, "xact-delta-retry",
                        local_kafka_config, events_table_path,
                    )
                )
            else:
                loop.run_until_complete(
                    run_delta_retry_scheduler(local_kafka_config, events_table_path)
                )
        elif args.worker == "xact-download":
            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_download_worker, args.count, "xact-download",
                        local_kafka_config,
                    )
                )
            else:
                loop.run_until_complete(run_download_worker(local_kafka_config))
        elif args.worker == "xact-upload":
            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_upload_worker, args.count, "xact-upload",
                        local_kafka_config,
                    )
                )
            else:
                loop.run_until_complete(run_upload_worker(local_kafka_config))
        elif args.worker == "xact-result-processor":
            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_result_processor, args.count, "xact-result-processor",
                        local_kafka_config, enable_delta_writes,
                        inventory_table_path=pipeline_config.inventory_table_path,
                        failed_table_path=pipeline_config.failed_table_path,
                    )
                )
            else:
                loop.run_until_complete(
                    run_result_processor(
                        local_kafka_config,
                        enable_delta_writes,
                        inventory_table_path=pipeline_config.inventory_table_path,
                        failed_table_path=pipeline_config.failed_table_path,
                    )
                )
        elif args.worker == "claimx-poller":
            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_claimx_eventhouse_poller, args.count, "claimx-poller",
                        pipeline_config,
                    )
                )
            else:
                loop.run_until_complete(run_claimx_eventhouse_poller(pipeline_config))
        elif args.worker == "claimx-delta-writer":
            claimx_events_table_path = os.getenv("CLAIMX_EVENTS_TABLE_PATH", "")
            if not claimx_events_table_path and pipeline_config.claimx_eventhouse:
                claimx_events_table_path = pipeline_config.claimx_eventhouse.claimx_events_table_path

            if not claimx_events_table_path:
                logger.error("CLAIMX_EVENTS_TABLE_PATH is required for claimx-delta-writer")
                sys.exit(1)

            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_claimx_delta_events_worker, args.count, "claimx-delta-writer",
                        local_kafka_config, claimx_events_table_path,
                    )
                )
            else:
                loop.run_until_complete(
                    run_claimx_delta_events_worker(local_kafka_config, claimx_events_table_path)
                )
        elif args.worker == "claimx-delta-retry":
            claimx_events_table_path = os.getenv("CLAIMX_EVENTS_TABLE_PATH", "")
            if not claimx_events_table_path and pipeline_config.claimx_eventhouse:
                claimx_events_table_path = pipeline_config.claimx_eventhouse.claimx_events_table_path

            if not claimx_events_table_path:
                logger.error("CLAIMX_EVENTS_TABLE_PATH is required for claimx-delta-retry")
                sys.exit(1)

            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_claimx_delta_retry_scheduler, args.count, "claimx-delta-retry",
                        local_kafka_config, claimx_events_table_path,
                    )
                )
            else:
                loop.run_until_complete(
                    run_claimx_delta_retry_scheduler(local_kafka_config, claimx_events_table_path)
                )
        elif args.worker == "claimx-ingester":
            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_claimx_event_ingester, args.count, "claimx-ingester",
                        local_kafka_config,
                    )
                )
            else:
                loop.run_until_complete(
                    run_claimx_event_ingester(
                        local_kafka_config,
                    )
                )
        elif args.worker == "claimx-enricher":
            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_claimx_enrichment_worker, args.count, "claimx-enricher",
                        local_kafka_config, pipeline_config,
                    )
                )
            else:
                loop.run_until_complete(run_claimx_enrichment_worker(local_kafka_config, pipeline_config))
        elif args.worker == "claimx-downloader":
            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_claimx_download_worker, args.count, "claimx-downloader",
                        local_kafka_config,
                    )
                )
            else:
                loop.run_until_complete(run_claimx_download_worker(local_kafka_config))
        elif args.worker == "claimx-uploader":
            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_claimx_upload_worker, args.count, "claimx-uploader",
                        local_kafka_config,
                    )
                )
            else:
                loop.run_until_complete(run_claimx_upload_worker(local_kafka_config))
        elif args.worker == "claimx-result-processor":
            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_claimx_result_processor, args.count, "claimx-result-processor",
                        local_kafka_config, pipeline_config,
                    )
                )
            else:
                loop.run_until_complete(run_claimx_result_processor(local_kafka_config, pipeline_config))
        elif args.worker == "claimx-entity-writer":
            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_claimx_entity_delta_worker, args.count, "claimx-entity-writer",
                        local_kafka_config, pipeline_config,
                    )
                )
            else:
                loop.run_until_complete(
                    run_claimx_entity_delta_worker(local_kafka_config, pipeline_config)
                )
        elif args.worker == "dummy-source":
            import yaml
            from config.pipeline_config import DEFAULT_CONFIG_DIR

            dummy_config = {}
            config_file = DEFAULT_CONFIG_DIR / "shared.yaml"
            if config_file.exists():
                with open(config_file) as f:
                    full_config = yaml.safe_load(f)
                    dummy_config = full_config.get("dummy", {})
                logger.info(
                    f"Loaded dummy source config from {config_file}",
                    extra={
                        "domains": dummy_config.get("domains", ["xact", "claimx"]),
                        "events_per_minute": dummy_config.get("events_per_minute", 10.0),
                        "burst_mode": dummy_config.get("burst_mode", False),
                    }
                )
            else:
                logger.warning(
                    f"Config file not found at {config_file}, using defaults (10 events/min)"
                )
            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_dummy_source, args.count, "dummy-source",
                        local_kafka_config, dummy_config,
                    )
                )
            else:
                loop.run_until_complete(run_dummy_source(local_kafka_config, dummy_config))
        else:
            loop.run_until_complete(run_all_workers(pipeline_config, enable_delta_writes))
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        loop.close()
        logger.info("Pipeline shutdown complete")



async def run_claimx_delta_events_worker(
    kafka_config,
    events_table_path: str,
):
    """Consumes events from claimx events topic and writes to claimx_events Delta table.
    Runs independently of ClaimXEventIngesterWorker with its own consumer group."""
    from kafka_pipeline.common.producer import BaseKafkaProducer
    from kafka_pipeline.claimx.workers.delta_events_worker import ClaimXDeltaEventsWorker

    set_log_context(stage="claimx-delta-writer")
    logger.info("Starting ClaimX Delta Events worker...")

    producer = BaseKafkaProducer(
        config=kafka_config,
        domain="claimx",
        worker_name="delta_events_writer",
    )
    await producer.start()

    worker = ClaimXDeltaEventsWorker(
        config=kafka_config,
        producer=producer,
        events_table_path=events_table_path,
        domain="claimx",
    )
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping claimx delta events worker...")
        await worker.stop()

    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await worker.start()
    finally:
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            pass
        await worker.stop()
        await producer.stop()


async def run_claimx_delta_retry_scheduler(kafka_config, events_table_path: str):
    """Consumes failed batches from retry topics and attempts to write to claimx_events Delta table.
    Routes permanently failed batches to DLQ."""
    from kafka_pipeline.common.producer import BaseKafkaProducer
    from kafka_pipeline.claimx.retry.scheduler import DeltaBatchRetryScheduler

    set_log_context(stage="claimx-delta-retry")
    logger.info("Starting ClaimX Delta Retry Scheduler...")

    producer = BaseKafkaProducer(
        config=kafka_config,
        domain="claimx",
        worker_name="delta_retry_scheduler",
    )
    await producer.start()

    scheduler = DeltaBatchRetryScheduler(
        config=kafka_config,
        producer=producer,
        table_path=events_table_path,
    )
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        """Wait for shutdown signal and stop scheduler gracefully."""
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping claimx delta retry scheduler...")
        await scheduler.stop()

    # Start shutdown watcher alongside scheduler
    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await scheduler.start()
    finally:
        # Guard against event loop being closed during shutdown
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            pass
        # Clean up resources after scheduler exits
        await scheduler.stop()
        await producer.stop()


async def run_claimx_entity_delta_worker(
    kafka_config,
    pipeline_config,
):
    """Consumes EntityRowsMessage from claimx.entities.rows and writes to Delta tables."""
    from kafka_pipeline.claimx.workers.entity_delta_worker import ClaimXEntityDeltaWorker

    set_log_context(stage="claimx-entity-writer")
    logger.info("Starting ClaimX Entity Delta worker...")

    worker = ClaimXEntityDeltaWorker(
        config=kafka_config,
        domain="claimx",
        projects_table_path=pipeline_config.claimx_projects_table_path,
        contacts_table_path=pipeline_config.claimx_contacts_table_path,
        media_table_path=pipeline_config.claimx_media_table_path,
        tasks_table_path=pipeline_config.claimx_tasks_table_path,
        task_templates_table_path=pipeline_config.claimx_task_templates_table_path,
        external_links_table_path=pipeline_config.claimx_external_links_table_path,
        video_collab_table_path=pipeline_config.claimx_video_collab_table_path,
    )
    shutdown_event = get_shutdown_event()

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


async def run_dummy_source(kafka_config, dummy_config: dict):
    """Generates synthetic insurance claim data for testing.
    Includes file server, realistic data generators for XACT/ClaimX, and configurable event rates."""
    from kafka_pipeline.common.dummy.source import DummyDataSource, load_dummy_source_config

    set_log_context(stage="dummy-source")
    logger.info("Starting Dummy Data Source...")

    source_config = load_dummy_source_config(kafka_config, dummy_config)

    source = DummyDataSource(source_config)
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping dummy data source...")
        await source.stop()

    await source.start()
    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await source.run()
    finally:
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            pass
        await source.stop()
        logger.info(f"Dummy source stopped. Stats: {source.stats}")


if __name__ == "__main__":
    main()
