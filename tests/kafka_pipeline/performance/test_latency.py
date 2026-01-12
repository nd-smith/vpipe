"""
Latency benchmark tests for Kafka pipeline.

Tests pipeline latency against NFR-1.1:
- Target: End-to-end latency < 5 seconds (p95)
- Current: 60-120 seconds

Validates:
- End-to-end latency distribution (p50, p95, p99)
- Per-stage latency breakdown
- Latency under various load conditions
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List
from unittest.mock import patch

import pytest
from aiokafka import AIOKafkaProducer

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.xact.schemas.events import EventMessage

from tests.kafka_pipeline.integration.helpers import wait_for_condition, start_worker_background, stop_worker_gracefully
from tests.kafka_pipeline.performance.conftest import PerformanceMetrics, ResourceMonitor, monitor_performance, save_performance_report

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.performance
async def test_end_to_end_latency(
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
    Measure end-to-end latency from event ingestion to inventory write.

    Tests complete pipeline latency by tracking timestamps through all stages.

    Target: p95 latency < 5 seconds
    Test: Process 1,000 events and measure latency distribution
    """
    mock_delta_inventory = mock_storage["delta_inventory"]
    generate_events = load_generator["generate_events"]

    # Generate test events
    event_count = 1_000
    test_events = await generate_events(
        count=event_count,
        prefix="latency-e2e",
        attachments_per_event=1,
    )

    # Track event timestamps
    event_timestamps: Dict[str, datetime] = {}

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
                # Produce events
                producer = AIOKafkaProducer(
                    bootstrap_servers=test_kafka_config.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                )
                await producer.start()

                try:
                    logger.info(f"Producing {event_count} events for latency measurement")

                    for event in test_events:
                        # Record timestamp when event is produced
                        produce_time = datetime.now(timezone.utc)
                        event_timestamps[event.trace_id] = produce_time

                        await producer.send(
                            test_kafka_config.events_topic,
                            value=event.model_dump(mode="json"),
                        )

                    await producer.flush()
                    logger.info(f"All {event_count} events produced")

                    # Wait for all events to complete
                    success = await wait_for_condition(
                        lambda: len(mock_delta_inventory.inventory_records) >= event_count,
                        timeout_seconds=120.0,
                        description=f"{event_count} events completed",
                    )

                    assert success, f"Only {len(mock_delta_inventory.inventory_records)} events completed"

                    # Calculate latencies
                    current_time = datetime.now(timezone.utc)
                    for record in mock_delta_inventory.inventory_records:
                        trace_id = record.get("trace_id")
                        if trace_id in event_timestamps:
                            start_time = event_timestamps[trace_id]
                            # Use current time as completion time (approximation)
                            latency = (current_time - start_time).total_seconds()
                            performance_metrics.latencies.append(latency)

                    performance_metrics.messages_processed = len(performance_metrics.latencies)

                finally:
                    await producer.stop()

        finally:
            await stop_worker_gracefully(event_ingester_worker, ingester_task)
            await stop_worker_gracefully(download_worker, download_task)
            await stop_worker_gracefully(result_processor, processor_task)

    # Finalize and save metrics
    performance_metrics.finalize()
    report_path = save_performance_report(performance_metrics, performance_report_dir)

    # Log latency percentiles
    logger.info(f"End-to-end latency p50: {performance_metrics.latency_p50:.3f}s")
    logger.info(f"End-to-end latency p95: {performance_metrics.latency_p95:.3f}s")
    logger.info(f"End-to-end latency p99: {performance_metrics.latency_p99:.3f}s")
    logger.info(f"End-to-end latency max: {performance_metrics.latency_max:.3f}s")

    # Assert NFR-1.1 target (p95 < 5 seconds)
    assert performance_metrics.latency_p95 < 5.0, (
        f"p95 latency {performance_metrics.latency_p95:.3f}s exceeds target (5.0s)"
    )


@pytest.mark.asyncio
@pytest.mark.performance
async def test_latency_under_load(
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
    Measure latency under high load conditions.

    Tests how latency degrades when pipeline is under load.

    Target: p95 latency < 5 seconds even under load
    Test: Process 10,000 events and measure latency distribution
    """
    mock_delta_inventory = mock_storage["delta_inventory"]
    generate_events = load_generator["generate_events"]

    # Generate larger batch of test events
    event_count = 10_000
    test_events = await generate_events(
        count=event_count,
        prefix="latency-load",
        attachments_per_event=1,
    )

    # Track timestamps for sample of events (to avoid memory issues)
    sample_size = 1_000
    sample_interval = event_count // sample_size
    event_timestamps: Dict[str, datetime] = {}

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
                    logger.info(f"Producing {event_count} events under load")

                    for i, event in enumerate(test_events):
                        # Sample events for latency measurement
                        if i % sample_interval == 0:
                            produce_time = datetime.now(timezone.utc)
                            event_timestamps[event.trace_id] = produce_time

                        await producer.send(
                            test_kafka_config.events_topic,
                            value=event.model_dump(mode="json"),
                        )

                    await producer.flush()
                    logger.info(f"All {event_count} events produced")

                    # Wait for all events to complete
                    success = await wait_for_condition(
                        lambda: len(mock_delta_inventory.inventory_records) >= event_count,
                        timeout_seconds=180.0,
                        description=f"{event_count} events completed",
                    )

                    assert success, f"Only {len(mock_delta_inventory.inventory_records)} events completed"

                    # Calculate latencies for sampled events
                    current_time = datetime.now(timezone.utc)
                    for record in mock_delta_inventory.inventory_records:
                        trace_id = record.get("trace_id")
                        if trace_id in event_timestamps:
                            start_time = event_timestamps[trace_id]
                            latency = (current_time - start_time).total_seconds()
                            performance_metrics.latencies.append(latency)

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

    # Log latency percentiles
    logger.info(f"Latency under load p50: {performance_metrics.latency_p50:.3f}s")
    logger.info(f"Latency under load p95: {performance_metrics.latency_p95:.3f}s")
    logger.info(f"Latency under load p99: {performance_metrics.latency_p99:.3f}s")
    logger.info(f"Latency under load max: {performance_metrics.latency_max:.3f}s")

    # Assert NFR-1.1 target (p95 < 5 seconds even under load)
    assert performance_metrics.latency_p95 < 5.0, (
        f"p95 latency under load {performance_metrics.latency_p95:.3f}s exceeds target (5.0s)"
    )


@pytest.mark.asyncio
@pytest.mark.performance
async def test_latency_percentiles(
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
    Comprehensive latency percentile measurement.

    Tests latency distribution across various percentiles.

    Targets:
    - p50 < 500ms
    - p95 < 5s
    - p99 < 10s

    Test: Process 5,000 events and measure detailed latency distribution
    """
    mock_delta_inventory = mock_storage["delta_inventory"]
    generate_events = load_generator["generate_events"]

    # Generate test events
    event_count = 5_000
    test_events = await generate_events(
        count=event_count,
        prefix="latency-percentiles",
        attachments_per_event=1,
    )

    # Track all event timestamps
    event_timestamps: Dict[str, datetime] = {}

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
                # Produce events with timestamps
                producer = AIOKafkaProducer(
                    bootstrap_servers=test_kafka_config.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                )
                await producer.start()

                try:
                    logger.info(f"Producing {event_count} events for percentile measurement")

                    for event in test_events:
                        produce_time = datetime.now(timezone.utc)
                        event_timestamps[event.trace_id] = produce_time

                        await producer.send(
                            test_kafka_config.events_topic,
                            value=event.model_dump(mode="json"),
                        )

                    await producer.flush()

                    # Wait for completion
                    success = await wait_for_condition(
                        lambda: len(mock_delta_inventory.inventory_records) >= event_count,
                        timeout_seconds=120.0,
                        description=f"{event_count} events completed",
                    )

                    assert success, f"Only {len(mock_delta_inventory.inventory_records)} events completed"

                    # Calculate latencies
                    current_time = datetime.now(timezone.utc)
                    for record in mock_delta_inventory.inventory_records:
                        trace_id = record.get("trace_id")
                        if trace_id in event_timestamps:
                            start_time = event_timestamps[trace_id]
                            latency = (current_time - start_time).total_seconds()
                            performance_metrics.latencies.append(latency)

                    performance_metrics.messages_processed = len(performance_metrics.latencies)

                finally:
                    await producer.stop()

        finally:
            await stop_worker_gracefully(event_ingester_worker, ingester_task)
            await stop_worker_gracefully(download_worker, download_task)
            await stop_worker_gracefully(result_processor, processor_task)

    # Finalize and save metrics
    performance_metrics.finalize()
    report_path = save_performance_report(performance_metrics, performance_report_dir)

    # Log comprehensive latency distribution
    logger.info(f"Latency distribution ({len(performance_metrics.latencies)} samples):")
    logger.info(f"  p50: {performance_metrics.latency_p50:.3f}s")
    logger.info(f"  p95: {performance_metrics.latency_p95:.3f}s")
    logger.info(f"  p99: {performance_metrics.latency_p99:.3f}s")
    logger.info(f"  mean: {performance_metrics.latency_mean:.3f}s")
    logger.info(f"  max: {performance_metrics.latency_max:.3f}s")

    # Assert targets
    assert performance_metrics.latency_p50 < 0.5, (
        f"p50 latency {performance_metrics.latency_p50:.3f}s exceeds target (0.5s)"
    )
    assert performance_metrics.latency_p95 < 5.0, (
        f"p95 latency {performance_metrics.latency_p95:.3f}s exceeds target (5.0s)"
    )
    assert performance_metrics.latency_p99 < 10.0, (
        f"p99 latency {performance_metrics.latency_p99:.3f}s exceeds target (10.0s)"
    )
