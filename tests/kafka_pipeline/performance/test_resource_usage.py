"""
Resource usage benchmark tests for Kafka pipeline.

Tests pipeline resource usage against NFR requirements:
- NFR-1.3: Download concurrency 50 parallel downloads
- NFR-1.4: Consumer lag recovery < 10 minutes for 100k backlog
- Memory target: < 512MB per worker
- CPU target: Reasonable under load

Validates:
- Memory usage under load
- CPU usage under load
- Download concurrency limits
- Consumer lag recovery performance
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict
from unittest.mock import patch

import pytest
from aiokafka import AIOKafkaProducer

from kafka_pipeline.config import KafkaConfig

from tests.kafka_pipeline.integration.helpers import wait_for_condition, start_worker_background, stop_worker_gracefully
from tests.kafka_pipeline.performance.conftest import PerformanceMetrics, ResourceMonitor, monitor_performance, save_performance_report

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.performance
async def test_memory_usage_under_load(
    test_kafka_config: KafkaConfig,
    event_ingester_worker,
    download_worker,
    result_processor,
    mock_storage: Dict,
    load_generator: Dict,
    performance_metrics: PerformanceMetrics,
    resource_monitor: ResourceMonitor,
    performance_report_dir: Path,
    tmp_path: Path,
):
    """
    Measure memory usage under sustained load.

    Tests that workers stay within memory limits.

    Target: < 512MB per worker
    Test: Process 20,000 events and monitor memory usage
    """
    mock_delta_inventory = mock_storage["delta_inventory"]
    generate_events = load_generator["generate_events"]

    # Generate test events
    event_count = 20_000
    test_events = await generate_events(
        count=event_count,
        prefix="memory-load",
        attachments_per_event=1,
    )

    # Create test file
    test_file_path = tmp_path / "document.pdf"
    test_file_content = b"PDF content for testing"
    test_file_path.write_bytes(test_file_content)

    # Mock downloads
    with patch("core.download.downloader.AttachmentDownloader.download") as mock_download:
        async def mock_download_impl(task):
            from core.download.models import DownloadOutcome
            return DownloadOutcome.success_outcome(
                file_path=test_file_path,
                bytes_downloaded=len(test_file_content),
                content_type="application/pdf",
                status_code=200,
            )

        mock_download.side_effect = mock_download_impl

        # Start all workers
        ingester_task = await start_worker_background(event_ingester_worker)
        download_task = await start_worker_background(download_worker)
        processor_task = await start_worker_background(result_processor)

        try:
            async with monitor_performance(performance_metrics, resource_monitor):
                # Produce events
                producer = AIOKafkaProducer(
                    bootstrap_servers=test_kafka_config.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    compression_type=None,  # lz4 disabled due to Python 3.13 compatibility
                )
                await producer.start()

                try:
                    logger.info(f"Producing {event_count} events for memory monitoring")

                    # Produce in batches
                    batch_size = 1000
                    for i in range(0, len(test_events), batch_size):
                        batch = test_events[i:i + batch_size]
                        for event in batch:
                            await producer.send(
                                test_kafka_config.events_topic,
                                value=event.model_dump(mode="json"),
                            )
                        await producer.flush()

                    # Wait for completion
                    success = await wait_for_condition(
                        lambda: len(mock_delta_inventory.inventory_records) >= event_count,
                        timeout_seconds=180.0,
                        description=f"{event_count} events completed",
                    )

                    assert success, f"Only {len(mock_delta_inventory.inventory_records)} events completed"
                    performance_metrics.messages_processed = event_count

                finally:
                    await producer.stop()

        finally:
            await stop_worker_gracefully(event_ingester_worker, ingester_task)
            await stop_worker_gracefully(download_worker, download_task)
            await stop_worker_gracefully(result_processor, processor_task)

    # Finalize and save metrics
    performance_metrics.finalize()
    report_path = save_performance_report(performance_metrics, performance_report_dir)

    # Log resource usage
    logger.info(f"Memory usage statistics:")
    logger.info(f"  Peak memory: {performance_metrics.peak_memory_mb:.2f} MB")
    logger.info(f"  Mean memory: {performance_metrics.mean_memory_mb:.2f} MB")
    logger.info(f"  Peak CPU: {performance_metrics.peak_cpu_percent:.2f}%")
    logger.info(f"  Mean CPU: {performance_metrics.mean_cpu_percent:.2f}%")

    # Assert memory target (< 512MB per worker)
    # Note: This is for all workers combined, so allow 1536MB (3 * 512MB)
    assert performance_metrics.peak_memory_mb < 1536, (
        f"Peak memory {performance_metrics.peak_memory_mb:.2f} MB exceeds target (1536 MB for 3 workers)"
    )


@pytest.mark.asyncio
@pytest.mark.performance
async def test_download_concurrency(
    test_kafka_config: KafkaConfig,
    download_worker,
    mock_storage: Dict,
    load_generator: Dict,
    performance_metrics: PerformanceMetrics,
    resource_monitor: ResourceMonitor,
    performance_report_dir: Path,
    tmp_path: Path,
):
    """
    Test download concurrency limits.

    Tests that download worker can handle concurrent downloads.

    Target (NFR-1.3): 50 parallel downloads
    Test: Process 100 download tasks with simulated delays
    """
    mock_onelake = mock_storage["onelake"]
    generate_events = load_generator["generate_events"]

    # Generate test events
    event_count = 100
    test_events = await generate_events(
        count=event_count,
        prefix="concurrency-test",
        attachments_per_event=1,
    )

    # Create test file
    test_file_path = tmp_path / "document.pdf"
    test_file_content = b"PDF content for testing"
    test_file_path.write_bytes(test_file_content)

    # Track concurrent downloads
    max_concurrent = 0
    current_concurrent = 0

    # Mock downloads with delay to simulate network I/O
    with patch("core.download.downloader.AttachmentDownloader.download") as mock_download:
        async def mock_download_impl(task):
            nonlocal max_concurrent, current_concurrent

            current_concurrent += 1
            max_concurrent = max(max_concurrent, current_concurrent)

            # Simulate download delay
            await asyncio.sleep(0.1)

            current_concurrent -= 1

            from core.download.models import DownloadOutcome
            return DownloadOutcome.success_outcome(
                file_path=test_file_path,
                bytes_downloaded=len(test_file_content),
                content_type="application/pdf",
                status_code=200,
            )

        mock_download.side_effect = mock_download_impl

        # Start download worker
        download_task = await start_worker_background(download_worker)

        try:
            async with monitor_performance(performance_metrics, resource_monitor):
                # Produce download tasks
                producer = AIOKafkaProducer(
                    bootstrap_servers=test_kafka_config.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                )
                await producer.start()

                try:
                    logger.info(f"Producing {event_count} download tasks for concurrency test")

                    # Convert events to download tasks
                    from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage

                    for event in test_events:
                        for attachment_url in event.attachments:
                            task = DownloadTaskMessage(
                                trace_id=event.trace_id,
                                attachment_url=attachment_url,
                                blob_path=f"downloads/{event.trace_id}/{attachment_url.split('/')[-1]}",
                                event_type=event.event_type,
                                event_subtype=event.event_subtype,
                                event_timestamp=event.event_timestamp,
                                retry_count=0,
                                metadata={"claim_id": event.payload.get("claim_id")},
                            )
                            await producer.send(
                                test_kafka_config.downloads_pending_topic,
                                value=task.model_dump(mode="json"),
                            )

                    await producer.flush()

                    # Wait for completion
                    success = await wait_for_condition(
                        lambda: mock_onelake.upload_count >= event_count,
                        timeout_seconds=60.0,
                        description=f"{event_count} downloads completed",
                    )

                    assert success, f"Only {mock_onelake.upload_count} downloads completed"
                    performance_metrics.messages_processed = event_count

                finally:
                    await producer.stop()

        finally:
            await stop_worker_gracefully(download_worker, download_task)

    # Finalize and save metrics
    performance_metrics.finalize()
    report_path = save_performance_report(performance_metrics, performance_report_dir)

    # Log concurrency metrics
    logger.info(f"Download concurrency statistics:")
    logger.info(f"  Max concurrent downloads: {max_concurrent}")
    logger.info(f"  Peak memory: {performance_metrics.peak_memory_mb:.2f} MB")
    logger.info(f"  Peak CPU: {performance_metrics.peak_cpu_percent:.2f}%")

    # Assert NFR-1.3 target (50 parallel downloads)
    # Allow some margin as actual concurrency depends on timing
    assert max_concurrent >= 40, (
        f"Max concurrent downloads {max_concurrent} is below target (50)"
    )


@pytest.mark.asyncio
@pytest.mark.performance
@pytest.mark.slow
async def test_consumer_lag_recovery(
    test_kafka_config: KafkaConfig,
    event_ingester_worker,
    download_worker,
    result_processor,
    mock_storage: Dict,
    load_generator: Dict,
    performance_metrics: PerformanceMetrics,
    resource_monitor: ResourceMonitor,
    performance_report_dir: Path,
    tmp_path: Path,
):
    """
    Test consumer lag recovery performance.

    Tests pipeline's ability to clear message backlog.

    Target (NFR-1.4): Clear 100k message backlog in < 10 minutes
    Test: Produce 100k messages, then start consumers and measure recovery time
    """
    mock_delta_inventory = mock_storage["delta_inventory"]
    generate_events = load_generator["generate_events"]

    # Generate large backlog
    event_count = 100_000
    logger.info(f"Generating {event_count} events for lag recovery test...")
    test_events = await generate_events(
        count=event_count,
        prefix="lag-recovery",
        attachments_per_event=1,
    )

    # Create test file
    test_file_path = tmp_path / "document.pdf"
    test_file_content = b"PDF content for testing"
    test_file_path.write_bytes(test_file_content)

    # Mock downloads
    with patch("core.download.downloader.AttachmentDownloader.download") as mock_download:
        async def mock_download_impl(task):
            from core.download.models import DownloadOutcome
            return DownloadOutcome.success_outcome(
                file_path=test_file_path,
                bytes_downloaded=len(test_file_content),
                content_type="application/pdf",
                status_code=200,
            )

        mock_download.side_effect = mock_download_impl

        # Step 1: Produce all events BEFORE starting workers (create backlog)
        producer = AIOKafkaProducer(
            bootstrap_servers=test_kafka_config.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            compression_type=None,  # lz4 disabled due to Python 3.13 compatibility
            linger_ms=10,
        )
        await producer.start()

        try:
            logger.info(f"Producing {event_count} events to create backlog...")
            produce_start = datetime.now(timezone.utc)

            batch_size = 1000
            for i in range(0, len(test_events), batch_size):
                batch = test_events[i:i + batch_size]
                for event in batch:
                    await producer.send(
                        test_kafka_config.events_topic,
                        value=event.model_dump(mode="json"),
                    )
                await producer.flush()

                if (i + batch_size) % 10000 == 0:
                    logger.info(f"Produced {i + batch_size}/{event_count} events")

            produce_end = datetime.now(timezone.utc)
            logger.info(f"Backlog created in {(produce_end - produce_start).total_seconds():.2f}s")

        finally:
            await producer.stop()

        # Step 2: Start workers and measure lag recovery
        logger.info("Starting workers to process backlog...")
        ingester_task = await start_worker_background(event_ingester_worker)
        download_task = await start_worker_background(download_worker)
        processor_task = await start_worker_background(result_processor)

        try:
            async with monitor_performance(performance_metrics, resource_monitor):
                recovery_start = datetime.now(timezone.utc)

                # Wait for all events to be processed
                success = await wait_for_condition(
                    lambda: len(mock_delta_inventory.inventory_records) >= event_count,
                    timeout_seconds=600.0,  # 10 minutes max (NFR target)
                    description=f"{event_count} backlog cleared",
                )

                recovery_end = datetime.now(timezone.utc)
                recovery_duration = (recovery_end - recovery_start).total_seconds()

                assert success, f"Only {len(mock_delta_inventory.inventory_records)}/{event_count} processed"
                performance_metrics.messages_processed = event_count

        finally:
            await stop_worker_gracefully(event_ingester_worker, ingester_task)
            await stop_worker_gracefully(download_worker, download_task)
            await stop_worker_gracefully(result_processor, processor_task)

    # Finalize and save metrics
    performance_metrics.finalize()
    report_path = save_performance_report(performance_metrics, performance_report_dir)

    # Log recovery metrics
    logger.info(f"Consumer lag recovery statistics:")
    logger.info(f"  Backlog size: {event_count} messages")
    logger.info(f"  Recovery time: {performance_metrics.duration_seconds:.2f}s ({performance_metrics.duration_seconds / 60:.2f}min)")
    logger.info(f"  Recovery rate: {performance_metrics.messages_per_second:.2f} msg/sec")
    logger.info(f"  Peak memory: {performance_metrics.peak_memory_mb:.2f} MB")
    logger.info(f"  Peak CPU: {performance_metrics.peak_cpu_percent:.2f}%")

    # Assert NFR-1.4 target (< 10 minutes for 100k backlog)
    max_recovery_seconds = 600.0  # 10 minutes
    assert performance_metrics.duration_seconds < max_recovery_seconds, (
        f"Recovery time {performance_metrics.duration_seconds:.2f}s exceeds target ({max_recovery_seconds}s)"
    )


@pytest.mark.asyncio
@pytest.mark.performance
async def test_cpu_usage_under_load(
    test_kafka_config: KafkaConfig,
    event_ingester_worker,
    download_worker,
    result_processor,
    mock_storage: Dict,
    load_generator: Dict,
    performance_metrics: PerformanceMetrics,
    resource_monitor: ResourceMonitor,
    performance_report_dir: Path,
    tmp_path: Path,
):
    """
    Measure CPU usage under sustained load.

    Tests CPU utilization during high-throughput processing.

    Target: Reasonable CPU usage (< 80% mean)
    Test: Process 15,000 events and monitor CPU usage
    """
    mock_delta_inventory = mock_storage["delta_inventory"]
    generate_events = load_generator["generate_events"]

    # Generate test events
    event_count = 15_000
    test_events = await generate_events(
        count=event_count,
        prefix="cpu-load",
        attachments_per_event=1,
    )

    # Create test file
    test_file_path = tmp_path / "document.pdf"
    test_file_content = b"PDF content for testing"
    test_file_path.write_bytes(test_file_content)

    # Mock downloads
    with patch("core.download.downloader.AttachmentDownloader.download") as mock_download:
        async def mock_download_impl(task):
            from core.download.models import DownloadOutcome
            return DownloadOutcome.success_outcome(
                file_path=test_file_path,
                bytes_downloaded=len(test_file_content),
                content_type="application/pdf",
                status_code=200,
            )

        mock_download.side_effect = mock_download_impl

        # Start all workers
        ingester_task = await start_worker_background(event_ingester_worker)
        download_task = await start_worker_background(download_worker)
        processor_task = await start_worker_background(result_processor)

        try:
            async with monitor_performance(performance_metrics, resource_monitor):
                # Produce events
                producer = AIOKafkaProducer(
                    bootstrap_servers=test_kafka_config.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    compression_type=None,  # lz4 disabled due to Python 3.13 compatibility
                )
                await producer.start()

                try:
                    logger.info(f"Producing {event_count} events for CPU monitoring")

                    # Produce continuously
                    batch_size = 500
                    for i in range(0, len(test_events), batch_size):
                        batch = test_events[i:i + batch_size]
                        for event in batch:
                            await producer.send(
                                test_kafka_config.events_topic,
                                value=event.model_dump(mode="json"),
                            )
                        await producer.flush()

                    # Wait for completion
                    success = await wait_for_condition(
                        lambda: len(mock_delta_inventory.inventory_records) >= event_count,
                        timeout_seconds=180.0,
                        description=f"{event_count} events completed",
                    )

                    assert success, f"Only {len(mock_delta_inventory.inventory_records)} events completed"
                    performance_metrics.messages_processed = event_count

                finally:
                    await producer.stop()

        finally:
            await stop_worker_gracefully(event_ingester_worker, ingester_task)
            await stop_worker_gracefully(download_worker, download_task)
            await stop_worker_gracefully(result_processor, processor_task)

    # Finalize and save metrics
    performance_metrics.finalize()
    report_path = save_performance_report(performance_metrics, performance_report_dir)

    # Log CPU usage
    logger.info(f"CPU usage statistics:")
    logger.info(f"  Peak CPU: {performance_metrics.peak_cpu_percent:.2f}%")
    logger.info(f"  Mean CPU: {performance_metrics.mean_cpu_percent:.2f}%")
    logger.info(f"  Peak memory: {performance_metrics.peak_memory_mb:.2f} MB")

    # Assert reasonable CPU usage target
    # Allow high peak but check mean usage
    assert performance_metrics.mean_cpu_percent < 80.0, (
        f"Mean CPU usage {performance_metrics.mean_cpu_percent:.2f}% exceeds target (80%)"
    )
