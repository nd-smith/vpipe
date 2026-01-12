"""
Throughput benchmark tests for Kafka pipeline.

Tests pipeline throughput against NFR-1.2:
- Target: 1,000 events/second sustained
- Current: 100 events/cycle

Validates:
- Event ingestion throughput
- Download worker throughput
- Result processor throughput
- End-to-end pipeline throughput
- Sustained load handling
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
async def test_event_ingestion_throughput(
    test_kafka_config: KafkaConfig,
    event_ingester_worker,
    mock_storage: Dict,
    load_generator: Dict,
    performance_metrics: PerformanceMetrics,
    resource_monitor: ResourceMonitor,
    performance_report_dir: Path,
):
    """
    Measure event ingestion throughput.

    Tests the Event Ingester's ability to consume events and produce download tasks.

    Target: Process >1,000 events/second
    Test: Send 10,000 events and measure processing rate
    """
    mock_delta_events = mock_storage["delta_events"]
    generate_events = load_generator["generate_events"]

    # Generate test events
    event_count = 10_000
    test_events = await generate_events(
        count=event_count,
        prefix="throughput-ingestion",
        attachments_per_event=1,
    )

    # Start event ingester
    ingester_task = await start_worker_background(event_ingester_worker)

    try:
        async with monitor_performance(performance_metrics, resource_monitor):
            # Produce events to raw topic
            producer = AIOKafkaProducer(
                bootstrap_servers=test_kafka_config.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                compression_type=None,  # lz4 disabled due to Python 3.13 compatibility  # Enable compression for better throughput
            )
            await producer.start()

            try:
                logger.info(f"Producing {event_count} events...")
                produce_start = datetime.now(timezone.utc)

                # Batch produce for better throughput
                batch_size = 100
                for i in range(0, len(test_events), batch_size):
                    batch = test_events[i:i + batch_size]
                    for event in batch:
                        await producer.send(
                            test_kafka_config.events_topic,
                            value=event.model_dump(mode="json"),
                        )

                await producer.flush()
                produce_end = datetime.now(timezone.utc)
                produce_duration = (produce_end - produce_start).total_seconds()
                logger.info(f"Produced {event_count} events in {produce_duration:.2f}s")

                # Wait for all events to be processed
                process_start = datetime.now(timezone.utc)
                success = await wait_for_condition(
                    lambda: len(mock_delta_events.written_events) >= event_count,
                    timeout_seconds=60.0,
                    description=f"{event_count} events processed",
                )
                process_end = datetime.now(timezone.utc)

                assert success, f"Only {len(mock_delta_events.written_events)} events processed"

                # Record metrics
                processing_duration = (process_end - process_start).total_seconds()
                performance_metrics.messages_processed = event_count

            finally:
                await producer.stop()

    finally:
        await stop_worker_gracefully(event_ingester_worker, ingester_task)

    # Finalize and save metrics
    performance_metrics.finalize()
    report_path = save_performance_report(performance_metrics, performance_report_dir)

    # Validate throughput target
    throughput = performance_metrics.messages_per_second
    logger.info(f"Event ingestion throughput: {throughput:.2f} events/sec")
    logger.info(f"Peak CPU: {performance_metrics.peak_cpu_percent:.2f}%")
    logger.info(f"Peak Memory: {performance_metrics.peak_memory_mb:.2f} MB")

    # Assert NFR-1.2 target (1,000 events/second)
    # Allow some margin for test environment variability
    assert throughput >= 800, (
        f"Throughput {throughput:.2f} events/sec is below target (1,000 events/sec)"
    )


@pytest.mark.asyncio
@pytest.mark.performance
async def test_download_worker_throughput(
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
    Measure download worker throughput.

    Tests the Download Worker's ability to process download tasks.

    Target: Process >1,000 downloads/second (mocked downloads)
    Test: Send 5,000 download tasks and measure processing rate
    """
    mock_onelake = mock_storage["onelake"]
    generate_events = load_generator["generate_events"]

    # Generate test events (will be converted to download tasks)
    event_count = 5_000
    test_events = await generate_events(
        count=event_count,
        prefix="throughput-download",
        attachments_per_event=1,
    )

    # Create a temporary file to simulate download
    test_file_path = tmp_path / "document.pdf"
    test_file_content = b"PDF content for testing"
    test_file_path.write_bytes(test_file_content)

    # Mock the download to return immediately
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

        # Start download worker
        download_task = await start_worker_background(download_worker)

        try:
            async with monitor_performance(performance_metrics, resource_monitor):
                # Produce download tasks to pending topic
                producer = AIOKafkaProducer(
                    bootstrap_servers=test_kafka_config.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    compression_type=None,  # lz4 disabled due to Python 3.13 compatibility
                )
                await producer.start()

                try:
                    logger.info(f"Producing {event_count} download tasks...")
                    produce_start = datetime.now(timezone.utc)

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
                    produce_end = datetime.now(timezone.utc)
                    produce_duration = (produce_end - produce_start).total_seconds()
                    logger.info(f"Produced {event_count} download tasks in {produce_duration:.2f}s")

                    # Wait for all downloads to complete
                    process_start = datetime.now(timezone.utc)
                    success = await wait_for_condition(
                        lambda: mock_onelake.upload_count >= event_count,
                        timeout_seconds=60.0,
                        description=f"{event_count} downloads processed",
                    )
                    process_end = datetime.now(timezone.utc)

                    assert success, f"Only {mock_onelake.upload_count} downloads completed"

                    # Record metrics
                    processing_duration = (process_end - process_start).total_seconds()
                    performance_metrics.messages_processed = event_count

                finally:
                    await producer.stop()

        finally:
            await stop_worker_gracefully(download_worker, download_task)

    # Finalize and save metrics
    performance_metrics.finalize()
    report_path = save_performance_report(performance_metrics, performance_report_dir)

    # Validate throughput target
    throughput = performance_metrics.messages_per_second
    logger.info(f"Download worker throughput: {throughput:.2f} downloads/sec")
    logger.info(f"Peak CPU: {performance_metrics.peak_cpu_percent:.2f}%")
    logger.info(f"Peak Memory: {performance_metrics.peak_memory_mb:.2f} MB")

    # Assert NFR-1.2 target (1,000 events/second)
    assert throughput >= 800, (
        f"Throughput {throughput:.2f} downloads/sec is below target (1,000/sec)"
    )


@pytest.mark.asyncio
@pytest.mark.performance
async def test_result_processor_throughput(
    test_kafka_config: KafkaConfig,
    result_processor,
    mock_storage: Dict,
    load_generator: Dict,
    performance_metrics: PerformanceMetrics,
    resource_monitor: ResourceMonitor,
    performance_report_dir: Path,
):
    """
    Measure result processor throughput.

    Tests the Result Processor's ability to batch and write results.

    Target: Process >1,000 results/second
    Test: Send 10,000 result messages and measure processing rate
    """
    mock_delta_inventory = mock_storage["delta_inventory"]
    generate_events = load_generator["generate_events"]

    # Generate test events
    event_count = 10_000
    test_events = await generate_events(
        count=event_count,
        prefix="throughput-results",
        attachments_per_event=1,
    )

    # Start result processor
    processor_task = await start_worker_background(result_processor)

    try:
        async with monitor_performance(performance_metrics, resource_monitor):
            # Produce result messages to results topic
            producer = AIOKafkaProducer(
                bootstrap_servers=test_kafka_config.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                compression_type=None,  # lz4 disabled due to Python 3.13 compatibility
            )
            await producer.start()

            try:
                logger.info(f"Producing {event_count} result messages...")
                produce_start = datetime.now(timezone.utc)

                # Convert events to result messages
                from kafka_pipeline.xact.schemas.results import DownloadResultMessage

                for event in test_events:
                    for attachment_url in event.attachments:
                        filename = attachment_url.split('/')[-1]
                        file_type = filename.rsplit('.', 1)[-1] if '.' in filename else 'pdf'
                        result = DownloadResultMessage(
                            trace_id=event.trace_id,
                            attachment_url=attachment_url,
                            blob_path=f"downloads/{event.trace_id}/{filename}",
                            status_subtype="documentsReceived",
                            file_type=file_type,
                            assignment_id=event.payload.get("claim_id", "A12345"),
                            status="completed",
                            http_status=200,
                            bytes_downloaded=1024,
                            created_at=datetime.now(timezone.utc),
                        )
                        await producer.send(
                            test_kafka_config.downloads_results_topic,
                            value=result.model_dump(mode="json"),
                        )

                await producer.flush()
                produce_end = datetime.now(timezone.utc)
                produce_duration = (produce_end - produce_start).total_seconds()
                logger.info(f"Produced {event_count} results in {produce_duration:.2f}s")

                # Wait for all results to be processed
                process_start = datetime.now(timezone.utc)
                success = await wait_for_condition(
                    lambda: len(mock_delta_inventory.inventory_records) >= event_count,
                    timeout_seconds=60.0,
                    description=f"{event_count} results processed",
                )
                process_end = datetime.now(timezone.utc)

                assert success, f"Only {len(mock_delta_inventory.inventory_records)} results processed"

                # Record metrics
                processing_duration = (process_end - process_start).total_seconds()
                performance_metrics.messages_processed = event_count

            finally:
                await producer.stop()

    finally:
        await stop_worker_gracefully(result_processor, processor_task)

    # Finalize and save metrics
    performance_metrics.finalize()
    report_path = save_performance_report(performance_metrics, performance_report_dir)

    # Validate throughput target
    throughput = performance_metrics.messages_per_second
    logger.info(f"Result processor throughput: {throughput:.2f} results/sec")
    logger.info(f"Peak CPU: {performance_metrics.peak_cpu_percent:.2f}%")
    logger.info(f"Peak Memory: {performance_metrics.peak_memory_mb:.2f} MB")

    # Assert NFR-1.2 target (1,000 events/second)
    assert throughput >= 800, (
        f"Throughput {throughput:.2f} results/sec is below target (1,000/sec)"
    )


@pytest.mark.asyncio
@pytest.mark.performance
@pytest.mark.slow
async def test_sustained_throughput(
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
    Measure sustained end-to-end throughput over extended duration.

    Tests full pipeline under sustained load.

    Target: Maintain >1,000 events/second for 60 seconds
    Test: Process 60,000 events continuously
    """
    mock_delta_inventory = mock_storage["delta_inventory"]
    generate_events = load_generator["generate_events"]

    # Generate large batch of test events
    event_count = 60_000
    logger.info(f"Generating {event_count} events...")
    test_events = await generate_events(
        count=event_count,
        prefix="sustained-load",
        attachments_per_event=1,
    )

    # Create test file for downloads
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
                # Produce events continuously
                producer = AIOKafkaProducer(
                    bootstrap_servers=test_kafka_config.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    compression_type=None,  # lz4 disabled due to Python 3.13 compatibility
                    linger_ms=10,  # Batch for 10ms for better throughput
                )
                await producer.start()

                try:
                    logger.info(f"Starting sustained load test: {event_count} events")
                    start_time = datetime.now(timezone.utc)

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

                        # Log progress
                        if (i + batch_size) % 10000 == 0:
                            logger.info(f"Produced {i + batch_size}/{event_count} events")

                    produce_end = datetime.now(timezone.utc)
                    logger.info(f"All events produced in {(produce_end - start_time).total_seconds():.2f}s")

                    # Wait for all events to flow through entire pipeline
                    success = await wait_for_condition(
                        lambda: len(mock_delta_inventory.inventory_records) >= event_count,
                        timeout_seconds=300.0,  # 5 minutes max
                        description=f"{event_count} events processed end-to-end",
                    )

                    end_time = datetime.now(timezone.utc)
                    assert success, f"Only {len(mock_delta_inventory.inventory_records)} events completed"

                    # Record metrics
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

    # Validate throughput target
    throughput = performance_metrics.messages_per_second
    logger.info(f"Sustained throughput: {throughput:.2f} events/sec over {performance_metrics.duration_seconds:.2f}s")
    logger.info(f"Peak CPU: {performance_metrics.peak_cpu_percent:.2f}%")
    logger.info(f"Peak Memory: {performance_metrics.peak_memory_mb:.2f} MB")

    # Assert NFR-1.2 target (1,000 events/second sustained)
    # Allow margin for test environment variability
    assert throughput >= 800, (
        f"Sustained throughput {throughput:.2f} events/sec is below target (1,000 events/sec)"
    )
