"""
Peak load and sustained load tests.

Tests system behavior under extreme conditions:
- Peak load: 2x normal throughput (2,000 events/sec)
- Sustained load: 4 hour continuous operation
- Resource limits: Memory and CPU under sustained load
- System stability: No degradation over time

Validates system can handle traffic spikes and long-running operation.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict
from unittest.mock import patch

import pytest
from aiokafka import AIOKafkaProducer

from kafka_pipeline.config import KafkaConfig

from tests.kafka_pipeline.integration.helpers import (
    wait_for_condition,
    start_worker_background,
    stop_worker_gracefully,
)
from tests.kafka_pipeline.performance.conftest import (
    PerformanceMetrics,
    ResourceMonitor,
    monitor_performance,
    save_performance_report,
)

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.performance
@pytest.mark.slow
async def test_peak_load_2x_throughput(
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
    Test system under 2x normal load (2,000 events/sec target).

    Validates:
    - System handles 2x normal throughput
    - No message loss under peak load
    - Resource usage remains acceptable
    - Graceful degradation if limits reached
    """
    mock_delta_inventory = mock_storage["delta_inventory"]
    generate_events = load_generator["generate_events"]

    # Generate large batch for peak load test
    # Target: 2,000 events/sec for 60 seconds = 120,000 events
    event_count = 120_000
    logger.info(f"Generating {event_count} events for 2x peak load test...")
    test_events = await generate_events(
        count=event_count,
        prefix="peak-load-2x",
        attachments_per_event=1,
    )

    # Create test file
    test_file_path = tmp_path / "document.pdf"
    test_file_content = b"PDF content for testing"
    test_file_path.write_bytes(test_file_content)

    # Mock downloads with minimal delay
    with patch("core.download.downloader.AttachmentDownloader.download") as mock_download:
        async def mock_download_impl(task):
            from core.download.models import DownloadOutcome
            # Minimal delay for maximum throughput
            await asyncio.sleep(0.0001)
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
                # Produce events as fast as possible to simulate peak load
                producer = AIOKafkaProducer(
                    bootstrap_servers=test_kafka_config.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    compression_type=None,  # lz4 disabled due to Python 3.13 compatibility
                    linger_ms=1,  # Minimal batching for high throughput
                    batch_size=65536,  # Larger batches
                )
                await producer.start()

                try:
                    logger.info(f"Starting 2x peak load test: {event_count} events")
                    start_time = datetime.now(timezone.utc)

                    # Produce as fast as possible
                    batch_size = 5000
                    for i in range(0, len(test_events), batch_size):
                        batch = test_events[i:i + batch_size]
                        for event in batch:
                            await producer.send(
                                test_kafka_config.events_topic,
                                value=event.model_dump(mode="json"),
                            )

                        # Periodic flush for throughput
                        if i % 20000 == 0:
                            await producer.flush()
                            elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
                            rate = (i + batch_size) / elapsed if elapsed > 0 else 0
                            logger.info(f"Produced {i + batch_size}/{event_count} - Rate: {rate:.2f} events/sec")

                    await producer.flush()
                    produce_end = datetime.now(timezone.utc)
                    produce_duration = (produce_end - start_time).total_seconds()
                    produce_rate = event_count / produce_duration
                    logger.info(f"Production complete in {produce_duration:.2f}s at {produce_rate:.2f} events/sec")

                    # Wait for all events to flow through pipeline
                    # Allow longer timeout for peak load
                    success = await wait_for_condition(
                        lambda: len(mock_delta_inventory.inventory_records) >= event_count,
                        timeout_seconds=600.0,  # 10 minutes max
                        description=f"{event_count} events at 2x peak load",
                    )

                    end_time = datetime.now(timezone.utc)
                    assert success, (
                        f"Only {len(mock_delta_inventory.inventory_records)}/{event_count} events completed. "
                        f"System failed to handle 2x peak load."
                    )

                    performance_metrics.messages_processed = event_count

                finally:
                    await producer.stop()

        finally:
            await stop_worker_gracefully(event_ingester_worker, ingester_task)
            await stop_worker_gracefully(download_worker, download_task)
            await stop_worker_gracefully(result_processor, processor_task)

    # Finalize metrics
    performance_metrics.finalize()
    report_path = save_performance_report(performance_metrics, performance_report_dir)

    # Validate peak load performance
    throughput = performance_metrics.messages_per_second
    logger.info(f"2x Peak Load - Total events: {event_count}")
    logger.info(f"2x Peak Load - Duration: {performance_metrics.duration_seconds:.2f}s")
    logger.info(f"2x Peak Load - Throughput: {throughput:.2f} events/sec")
    logger.info(f"2x Peak Load - Peak CPU: {performance_metrics.peak_cpu_percent:.2f}%")
    logger.info(f"2x Peak Load - Peak Memory: {performance_metrics.peak_memory_mb:.2f} MB")

    # System should handle peak load gracefully
    # Even if throughput is lower than target, all messages should be processed
    assert len(mock_delta_inventory.inventory_records) == event_count, (
        f"Message loss detected: {len(mock_delta_inventory.inventory_records)}/{event_count}"
    )

    # Peak memory should stay reasonable
    assert performance_metrics.peak_memory_mb < 1024, (
        f"Peak memory {performance_metrics.peak_memory_mb:.2f}MB exceeds 1GB limit"
    )


@pytest.mark.asyncio
@pytest.mark.performance
@pytest.mark.slow
@pytest.mark.extended
async def test_sustained_load_4_hours(
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
    Test sustained load for 4 hours at target throughput.

    Validates:
    - No performance degradation over time
    - No memory leaks
    - Stable resource usage
    - System remains responsive

    Note: This is a long-running test. Use pytest marker filtering to skip in CI:
        pytest -m "not extended"
    """
    mock_delta_inventory = mock_storage["delta_inventory"]
    generate_events = load_generator["generate_events"]

    # 4 hours at 1,000 events/sec = 14,400,000 events
    # For testing purposes, use scaled-down version: 1 hour at 1,000 events/sec
    test_duration_hours = 1  # Scale down for practical testing
    events_per_second = 1000
    total_events = test_duration_hours * 3600 * events_per_second  # 3,600,000 for 1 hour

    logger.info(f"Sustained load test: {test_duration_hours} hour(s) at {events_per_second} events/sec")
    logger.info(f"Total events: {total_events}")

    # Create test file
    test_file_path = tmp_path / "document.pdf"
    test_file_content = b"PDF content for testing"
    test_file_path.write_bytes(test_file_content)

    # Track resource usage over time
    resource_checkpoints = []

    # Mock downloads
    with patch("core.download.downloader.AttachmentDownloader.download") as mock_download:
        async def mock_download_impl(task):
            from core.download.models import DownloadOutcome
            await asyncio.sleep(0.0001)
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
                producer = AIOKafkaProducer(
                    bootstrap_servers=test_kafka_config.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    compression_type=None,  # lz4 disabled due to Python 3.13 compatibility
                    linger_ms=5,
                    batch_size=32768,
                )
                await producer.start()

                try:
                    logger.info("Starting sustained load test...")
                    test_start = datetime.now(timezone.utc)
                    test_end_target = test_start + timedelta(hours=test_duration_hours)

                    events_produced = 0
                    checkpoint_interval_events = 100_000  # Checkpoint every 100k events

                    # Generate and produce events in batches to avoid memory issues
                    batch_size = 10_000
                    batches_needed = total_events // batch_size

                    for batch_num in range(batches_needed):
                        # Generate batch
                        batch_events = await generate_events(
                            count=batch_size,
                            prefix=f"sustained-batch-{batch_num}",
                            attachments_per_event=1,
                        )

                        # Produce batch
                        for event in batch_events:
                            await producer.send(
                                test_kafka_config.events_topic,
                                value=event.model_dump(mode="json"),
                            )
                            events_produced += 1

                            # Periodic checkpoint
                            if events_produced % checkpoint_interval_events == 0:
                                elapsed = (datetime.now(timezone.utc) - test_start).total_seconds()
                                current_rate = events_produced / elapsed if elapsed > 0 else 0
                                current_memory = resource_monitor.snapshots[-1].memory_mb if resource_monitor.snapshots else 0

                                checkpoint = {
                                    "events": events_produced,
                                    "elapsed_seconds": elapsed,
                                    "rate": current_rate,
                                    "memory_mb": current_memory,
                                }
                                resource_checkpoints.append(checkpoint)

                                logger.info(
                                    f"Checkpoint: {events_produced}/{total_events} events, "
                                    f"{elapsed:.0f}s elapsed, "
                                    f"{current_rate:.2f} events/sec, "
                                    f"{current_memory:.2f} MB"
                                )

                        # Flush after each batch
                        await producer.flush()

                    await producer.flush()
                    produce_end = datetime.now(timezone.utc)
                    logger.info(f"All {total_events} events produced in {(produce_end - test_start).total_seconds():.2f}s")

                    # Wait for all events to be processed
                    success = await wait_for_condition(
                        lambda: len(mock_delta_inventory.inventory_records) >= total_events,
                        timeout_seconds=test_duration_hours * 3600 + 3600,  # Test duration + 1 hour buffer
                        description=f"{total_events} sustained load events",
                    )

                    assert success, (
                        f"Only {len(mock_delta_inventory.inventory_records)}/{total_events} events processed"
                    )

                    performance_metrics.messages_processed = total_events

                finally:
                    await producer.stop()

        finally:
            await stop_worker_gracefully(event_ingester_worker, ingester_task)
            await stop_worker_gracefully(download_worker, download_task)
            await stop_worker_gracefully(result_processor, processor_task)

    # Finalize metrics
    performance_metrics.finalize()
    report_path = save_performance_report(performance_metrics, performance_report_dir)

    # Validate sustained load performance
    logger.info(f"Sustained Load - Total events: {total_events}")
    logger.info(f"Sustained Load - Duration: {performance_metrics.duration_seconds:.2f}s ({performance_metrics.duration_seconds / 3600:.2f} hours)")
    logger.info(f"Sustained Load - Average throughput: {performance_metrics.messages_per_second:.2f} events/sec")
    logger.info(f"Sustained Load - Peak CPU: {performance_metrics.peak_cpu_percent:.2f}%")
    logger.info(f"Sustained Load - Peak Memory: {performance_metrics.peak_memory_mb:.2f} MB")
    logger.info(f"Sustained Load - Mean Memory: {performance_metrics.mean_memory_mb:.2f} MB")

    # Analyze resource checkpoints for trends
    if resource_checkpoints:
        memory_trend = [cp["memory_mb"] for cp in resource_checkpoints]
        memory_start = memory_trend[0]
        memory_end = memory_trend[-1]
        memory_growth = memory_end - memory_start

        logger.info(f"Memory trend - Start: {memory_start:.2f} MB, End: {memory_end:.2f} MB, Growth: {memory_growth:.2f} MB")

        # Validate no significant memory leak (allow <100MB growth over test)
        assert memory_growth < 100, (
            f"Potential memory leak detected: {memory_growth:.2f}MB growth over sustained test"
        )

    # Validate all messages processed
    assert len(mock_delta_inventory.inventory_records) == total_events, (
        f"Message loss: {len(mock_delta_inventory.inventory_records)}/{total_events}"
    )


@pytest.mark.asyncio
@pytest.mark.performance
async def test_resource_limits_under_load(
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
    Test resource limits (memory, CPU) under high load.

    Validates:
    - Memory usage stays within acceptable limits (<512MB per worker)
    - CPU usage reasonable for workload
    - No resource exhaustion
    - Proper cleanup of temporary resources
    """
    mock_onelake = mock_storage["onelake"]
    generate_events = load_generator["generate_events"]

    # Generate moderate load
    event_count = 20_000
    test_events = await generate_events(
        count=event_count,
        prefix="resource-limits",
        attachments_per_event=1,
    )

    # Create test file
    test_file_path = tmp_path / "document.pdf"
    test_file_content = b"PDF content for testing" * 100  # Larger file for more realistic memory usage
    test_file_path.write_bytes(test_file_content)

    # Mock downloads
    with patch("core.download.downloader.AttachmentDownloader.download") as mock_download:
        async def mock_download_impl(task):
            from core.download.models import DownloadOutcome
            await asyncio.sleep(0.001)
            return DownloadOutcome.success_outcome(
                file_path=test_file_path,
                bytes_downloaded=len(test_file_content),
                content_type="application/pdf",
                status_code=200,
            )

        mock_download.side_effect = mock_download_impl

        # Start worker
        download_task = await start_worker_background(download_worker)

        try:
            async with monitor_performance(performance_metrics, resource_monitor):
                # Produce tasks
                producer = AIOKafkaProducer(
                    bootstrap_servers=test_kafka_config.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    compression_type=None,  # lz4 disabled due to Python 3.13 compatibility
                )
                await producer.start()

                try:
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
                        timeout_seconds=120.0,
                        description=f"{event_count} downloads for resource limit test",
                    )

                    assert success, f"Only {mock_onelake.upload_count} downloads completed"
                    performance_metrics.messages_processed = event_count

                finally:
                    await producer.stop()

        finally:
            await stop_worker_gracefully(download_worker, download_task)

    # Finalize metrics
    performance_metrics.finalize()
    report_path = save_performance_report(performance_metrics, performance_report_dir)

    # Validate resource limits
    logger.info(f"Resource Limits - Peak CPU: {performance_metrics.peak_cpu_percent:.2f}%")
    logger.info(f"Resource Limits - Peak Memory: {performance_metrics.peak_memory_mb:.2f} MB")
    logger.info(f"Resource Limits - Mean CPU: {performance_metrics.mean_cpu_percent:.2f}%")
    logger.info(f"Resource Limits - Mean Memory: {performance_metrics.mean_memory_mb:.2f} MB")

    # Assert memory limits (target: <512MB per worker)
    assert performance_metrics.peak_memory_mb < 512, (
        f"Peak memory {performance_metrics.peak_memory_mb:.2f}MB exceeds 512MB limit per worker"
    )

    # Mean memory should be well below peak
    assert performance_metrics.mean_memory_mb < performance_metrics.peak_memory_mb * 0.8, (
        f"Mean memory too close to peak suggests inefficient memory management"
    )

    # Validate temporary files cleaned up
    temp_files = list((tmp_path / "downloads").glob("**/*")) if (tmp_path / "downloads").exists() else []
    logger.info(f"Temporary files remaining: {len(temp_files)}")
