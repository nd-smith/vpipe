"""Verisk domain worker runners.

Contains all runner functions for XACT pipeline workers:
- Event ingestion (Event Hub and Eventhouse)
- Enrichment
- Download/Upload
- Delta writes
- Result processing
- Retry scheduling
"""

import asyncio
import logging

from pipeline.runners.common import (
    execute_poller_with_shutdown,
    execute_worker_with_producer,
    execute_worker_with_shutdown,
)

logger = logging.getLogger(__name__)


async def run_event_ingester(
    eventhub_config,
    local_kafka_config,
    shutdown_event: asyncio.Event,
    domain: str = "xact",
    instance_id: int | None = None,
):
    """Reads events from Event Hub and produces download tasks to local Kafka.
    Delta Lake writes are handled by a separate DeltaEventsWorker."""
    from pipeline.verisk.workers.event_ingester import EventIngesterWorker

    worker = EventIngesterWorker(
        config=eventhub_config,
        domain=domain,
        producer_config=local_kafka_config,
        instance_id=instance_id,
    )
    await execute_worker_with_shutdown(
        worker,
        stage_name="xact-event-ingester",
        shutdown_event=shutdown_event,
        instance_id=instance_id,
    )


async def run_eventhouse_poller(
    pipeline_config, shutdown_event: asyncio.Event, local_kafka_config
):
    """Polls Microsoft Fabric Eventhouse for events and produces to events.raw topic.
    Deduplication handled by daily Fabric maintenance job."""
    from pipeline.common.eventhouse.kql_client import EventhouseConfig
    from pipeline.common.eventhouse.poller import KQLEventPoller, PollerConfig

    print("\n[XACT-POLLER] Initializing Eventhouse poller configuration")

    eventhouse_source = pipeline_config.verisk_eventhouse
    if not eventhouse_source:
        raise ValueError(
            "Xact Eventhouse configuration required for EVENT_SOURCE=eventhouse"
        )

    print(f"[XACT-POLLER] Eventhouse configuration:")
    print(f"[XACT-POLLER]   - Cluster URL: {eventhouse_source.cluster_url}")
    print(f"[XACT-POLLER]   - Database: {eventhouse_source.database}")
    print(f"[XACT-POLLER]   - Source table: {eventhouse_source.source_table}")
    print(f"[XACT-POLLER]   - Query timeout: {eventhouse_source.query_timeout_seconds}s")
    print(f"[XACT-POLLER]   - Poll interval: {eventhouse_source.poll_interval_seconds}s")
    print(f"[XACT-POLLER]   - Batch size: {eventhouse_source.batch_size}")
    print(f"[XACT-POLLER]   - Bulk backfill: {eventhouse_source.bulk_backfill}")
    if eventhouse_source.backfill_start_stamp:
        print(f"[XACT-POLLER]   - Backfill start: {eventhouse_source.backfill_start_stamp}")
    if eventhouse_source.backfill_stop_stamp:
        print(f"[XACT-POLLER]   - Backfill stop: {eventhouse_source.backfill_stop_stamp}")

    eventhouse_config = EventhouseConfig(
        cluster_url=eventhouse_source.cluster_url,
        database=eventhouse_source.database,
        query_timeout_seconds=eventhouse_source.query_timeout_seconds,
    )

    print(f"[XACT-POLLER] Local Kafka configuration: Loaded")
    print(f"[XACT-POLLER]   - Output topic: events.raw")

    poller_config = PollerConfig(
        eventhouse=eventhouse_config,
        kafka=local_kafka_config,
        poll_interval_seconds=eventhouse_source.poll_interval_seconds,
        batch_size=eventhouse_source.batch_size,
        source_table=eventhouse_source.source_table,
        events_table_path=eventhouse_source.verisk_events_table_path,
        backfill_start_stamp=eventhouse_source.backfill_start_stamp,
        backfill_stop_stamp=eventhouse_source.backfill_stop_stamp,
        bulk_backfill=eventhouse_source.bulk_backfill,
    )

    print("[XACT-POLLER] Configuration complete, starting poller...\n")

    await execute_poller_with_shutdown(
        KQLEventPoller,
        poller_config,
        stage_name="xact-poller",
        shutdown_event=shutdown_event,
    )


async def run_eventhouse_json_poller(
    pipeline_config,
    shutdown_event: asyncio.Event,
    output_path: str = "output/xact_events.jsonl",
    rotate_size_mb: float = 100.0,
    pretty_print: bool = False,
    include_metadata: bool = True,
):
    """Polls Microsoft Fabric Eventhouse for events and writes to JSON file.

    This is a Kafka-free version of the poller for debugging, testing, or
    exporting events to JSON format for external processing.

    Args:
        pipeline_config: Pipeline configuration with eventhouse settings
        shutdown_event: Shutdown event for graceful shutdown
        output_path: Path to output JSON Lines file (default: output/xact_events.jsonl)
        rotate_size_mb: Rotate file when it reaches this size in MB (default: 100)
        pretty_print: Format JSON with indentation (default: False)
        include_metadata: Include _key, _timestamp, _headers in output (default: True)
    """

    from pipeline.common.eventhouse.kql_client import EventhouseConfig
    from pipeline.common.eventhouse.poller import KQLEventPoller, PollerConfig
    from pipeline.common.eventhouse.sinks import create_json_sink

    eventhouse_source = pipeline_config.verisk_eventhouse
    if not eventhouse_source:
        raise ValueError("Xact Eventhouse configuration required")

    eventhouse_config = EventhouseConfig(
        cluster_url=eventhouse_source.cluster_url,
        database=eventhouse_source.database,
        query_timeout_seconds=eventhouse_source.query_timeout_seconds,
    )

    # Create JSON sink
    json_sink = create_json_sink(
        output_path=output_path,
        rotate_size_mb=rotate_size_mb,
        pretty_print=pretty_print,
        include_metadata=include_metadata,
    )

    poller_config = PollerConfig(
        eventhouse=eventhouse_config,
        kafka=None,  # Not using Kafka
        sink=json_sink,  # Use JSON file sink
        poll_interval_seconds=eventhouse_source.poll_interval_seconds,
        batch_size=eventhouse_source.batch_size,
        source_table=eventhouse_source.source_table,
        events_table_path=eventhouse_source.verisk_events_table_path,
        backfill_start_stamp=eventhouse_source.backfill_start_stamp,
        backfill_stop_stamp=eventhouse_source.backfill_stop_stamp,
        bulk_backfill=eventhouse_source.bulk_backfill,
    )

    await execute_poller_with_shutdown(
        KQLEventPoller,
        poller_config,
        stage_name="xact-json-poller",
        shutdown_event=shutdown_event,
    )


async def run_delta_events_worker(
    kafka_config,
    events_table_path: str,
    shutdown_event: asyncio.Event,
    instance_id: int | None = None,
):
    """Consumes events from events.raw and writes to xact_events Delta table.
    Runs independently of EventIngesterWorker with its own consumer group."""
    from pipeline.common.producer import BaseKafkaProducer
    from pipeline.verisk.workers.delta_events_worker import DeltaEventsWorker

    await execute_worker_with_producer(
        worker_class=DeltaEventsWorker,
        producer_class=BaseKafkaProducer,
        kafka_config=kafka_config,
        domain="verisk",
        stage_name="xact-delta-writer",
        shutdown_event=shutdown_event,
        worker_kwargs={"events_table_path": events_table_path},
        producer_worker_name="delta_events_writer",
        instance_id=instance_id,
    )


async def run_xact_retry_scheduler(
    kafka_config,
    shutdown_event: asyncio.Event,
    instance_id: int | None = None,
):
    """Unified retry scheduler for all XACT retry types.
    Routes messages from xact.retry topic to target topics based on headers."""
    from pipeline.common.producer import BaseKafkaProducer
    from pipeline.common.retry.unified_scheduler import UnifiedRetryScheduler

    await execute_worker_with_producer(
        worker_class=UnifiedRetryScheduler,
        producer_class=BaseKafkaProducer,
        kafka_config=kafka_config,
        domain="verisk",
        stage_name="xact-retry-scheduler",
        shutdown_event=shutdown_event,
        producer_worker_name="unified_retry_scheduler",
        instance_id=instance_id,
    )


async def run_xact_enrichment_worker(
    kafka_config,
    shutdown_event: asyncio.Event,
    instance_id: int | None = None,
):
    """Run XACT enrichment worker with plugin-based enrichment.

    Args:
        kafka_config: Kafka configuration
        shutdown_event: Shutdown event for graceful shutdown
        instance_id: Optional instance ID for parallel workers
    """
    from pipeline.verisk.workers.enrichment_worker import XACTEnrichmentWorker

    worker = XACTEnrichmentWorker(
        config=kafka_config,
        domain="verisk",
        instance_id=instance_id,
    )

    await execute_worker_with_shutdown(
        worker,
        stage_name="xact-enricher",
        shutdown_event=shutdown_event,
        instance_id=instance_id,
    )


async def run_download_worker(
    kafka_config,
    shutdown_event: asyncio.Event,
    instance_id: int | None = None,
):
    """Download files from external sources.

    Args:
        kafka_config: Kafka configuration
        shutdown_event: Shutdown event for graceful shutdown
        instance_id: Optional instance ID for parallel workers
    """
    from pipeline.verisk.workers.download_worker import DownloadWorker

    worker = DownloadWorker(
        config=kafka_config, domain="verisk", instance_id=instance_id
    )
    await execute_worker_with_shutdown(
        worker,
        stage_name="xact-download",
        shutdown_event=shutdown_event,
        instance_id=instance_id,
    )


async def run_upload_worker(
    kafka_config,
    shutdown_event: asyncio.Event,
    instance_id: int | None = None,
):
    """Upload cached files to storage.

    Args:
        kafka_config: Kafka configuration
        shutdown_event: Shutdown event for graceful shutdown
        instance_id: Optional instance ID for parallel workers
    """
    from pipeline.verisk.workers.upload_worker import UploadWorker

    worker = UploadWorker(config=kafka_config, domain="verisk", instance_id=instance_id)

    await execute_worker_with_shutdown(
        worker,
        stage_name="xact-upload",
        shutdown_event=shutdown_event,
        instance_id=instance_id,
    )


async def run_result_processor(
    kafka_config,
    shutdown_event: asyncio.Event,
    enable_delta_writes: bool = True,
    inventory_table_path: str = "",
    failed_table_path: str = "",
    instance_id: int | None = None,
):
    """Reads download results and writes to Delta Lake tables.
    On Delta write failure, batches are routed to retry topics."""
    from core.logging.context import set_log_context
    from pipeline.common.producer import BaseKafkaProducer
    from pipeline.verisk.workers.result_processor import ResultProcessor

    set_log_context(stage="xact-result-processor")
    logger.info("Starting xact Result Processor worker...")

    if enable_delta_writes and not inventory_table_path:
        logger.error(
            "inventory_table_path is required for xact-result-processor "
            "when enable_delta_writes=True. Configure via DELTA_INVENTORY_TABLE_PATH "
            "environment variable or delta.xact.inventory_table_path in config.yaml."
        )
        raise ValueError(
            "inventory_table_path is required when delta writes are enabled"
        )

    await execute_worker_with_producer(
        worker_class=ResultProcessor,
        producer_class=BaseKafkaProducer,
        kafka_config=kafka_config,
        domain="verisk",
        stage_name="xact-result-processor",
        shutdown_event=shutdown_event,
        worker_kwargs={
            "inventory_table_path": (
                inventory_table_path if enable_delta_writes else None
            ),
            "failed_table_path": (
                failed_table_path if enable_delta_writes and failed_table_path else None
            ),
            "batch_size": 2000,
            "batch_timeout_seconds": 5.0,
        },
        producer_worker_name="result_processor",
        instance_id=instance_id,
    )


async def run_local_event_ingester(
    local_kafka_config,
    shutdown_event: asyncio.Event,
    domain: str = "xact",
    instance_id: int | None = None,
):
    """Consumes from local Kafka events.raw topic and processes events to downloads.pending.
    Used in Eventhouse mode. Delta Lake writes handled by separate DeltaEventsWorker."""
    from pipeline.verisk.workers.event_ingester import EventIngesterWorker

    worker = EventIngesterWorker(
        config=local_kafka_config,
        domain=domain,
        instance_id=instance_id,
    )
    await execute_worker_with_shutdown(
        worker,
        stage_name="xact-event-ingester",
        shutdown_event=shutdown_event,
        instance_id=instance_id,
    )


async def run_dummy_source(
    kafka_config,
    dummy_config: dict,
    shutdown_event: asyncio.Event,
):
    """Generates synthetic insurance claim data for testing.
    Includes file server, realistic data generators for XACT/ClaimX, and configurable event rates.
    """
    from core.logging.context import set_log_context
    from pipeline.common.dummy.source import (
        DummyDataSource,
        load_dummy_source_config,
    )

    set_log_context(stage="dummy-source")
    logger.info("Starting Dummy Data Source...")

    source_config = load_dummy_source_config(kafka_config, dummy_config)

    source = DummyDataSource(source_config)

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
        logger.info("Dummy source stopped", extra={"stats": source.stats})
