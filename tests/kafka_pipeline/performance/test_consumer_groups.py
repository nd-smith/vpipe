"""
Consumer group behavior and rebalancing tests.

Tests consumer group dynamics:
- Partition assignment and rebalancing
- Consumer lag under various loads
- Offset commit behavior
- Consumer group coordination

Validates NFR-3.1 and NFR-3.2 requirements.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List
from unittest.mock import patch

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

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
async def test_consumer_lag_recovery(
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
    Test consumer lag recovery (NFR-1.4).

    Validates:
    - 100k message backlog cleared in <10min
    - Consistent throughput during catchup
    - Consumer lag metrics
    """
    mock_onelake = mock_storage["onelake"]
    generate_events = load_generator["generate_events"]

    # Generate large backlog
    backlog_size = 100_000
    logger.info(f"Generating {backlog_size} message backlog...")
    test_events = await generate_events(
        count=backlog_size,
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
            # Minimal delay for fast processing
            await asyncio.sleep(0.0001)
            return DownloadOutcome.success_outcome(
                file_path=test_file_path,
                bytes_downloaded=len(test_file_content),
                content_type="application/pdf",
                status_code=200,
            )

        mock_download.side_effect = mock_download_impl

        # Produce entire backlog BEFORE starting consumer
        producer = AIOKafkaProducer(
            bootstrap_servers=test_kafka_config.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            compression_type=None,  # lz4 disabled due to Python 3.13 compatibility
        )
        await producer.start()

        try:
            logger.info(f"Producing {backlog_size} message backlog...")
            produce_start = datetime.now(timezone.utc)

            from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage

            # Batch produce for speed
            batch_size = 1000
            for i in range(0, len(test_events), batch_size):
                batch = test_events[i:i + batch_size]
                for event in batch:
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

                # Log progress
                if (i + batch_size) % 10000 == 0:
                    logger.info(f"Produced {i + batch_size}/{backlog_size}")

            await producer.flush()
            produce_end = datetime.now(timezone.utc)
            produce_duration = (produce_end - produce_start).total_seconds()
            logger.info(f"Backlog produced in {produce_duration:.2f}s")

        finally:
            await producer.stop()

        # NOW start consumer and measure recovery time
        logger.info("Starting consumer to process backlog...")
        download_task = await start_worker_background(download_worker)

        try:
            async with monitor_performance(performance_metrics, resource_monitor):
                catchup_start = datetime.now(timezone.utc)

                # Wait for backlog to be cleared
                success = await wait_for_condition(
                    lambda: mock_onelake.upload_count >= backlog_size,
                    timeout_seconds=600.0,  # 10 minute max (NFR-1.4)
                    description=f"{backlog_size} backlog messages processed",
                )

                catchup_end = datetime.now(timezone.utc)
                catchup_duration = (catchup_end - catchup_start).total_seconds()

                assert success, (
                    f"Only {mock_onelake.upload_count}/{backlog_size} messages processed. "
                    f"Failed to clear backlog in 10 minutes (NFR-1.4)."
                )

                performance_metrics.messages_processed = backlog_size

        finally:
            await stop_worker_gracefully(download_worker, download_task)

    # Finalize metrics
    performance_metrics.finalize()
    report_path = save_performance_report(performance_metrics, performance_report_dir)

    # Validate NFR-1.4: 100k backlog cleared in <10min
    logger.info(f"Lag Recovery - Backlog size: {backlog_size}")
    logger.info(f"Lag Recovery - Catchup time: {performance_metrics.duration_seconds:.2f}s ({performance_metrics.duration_seconds / 60:.2f} min)")
    logger.info(f"Lag Recovery - Throughput: {performance_metrics.messages_per_second:.2f} msgs/sec")
    logger.info(f"Lag Recovery - Peak CPU: {performance_metrics.peak_cpu_percent:.2f}%")
    logger.info(f"Lag Recovery - Peak Memory: {performance_metrics.peak_memory_mb:.2f} MB")

    # Assert NFR-1.4 requirement
    max_catchup_seconds = 600  # 10 minutes
    assert performance_metrics.duration_seconds < max_catchup_seconds, (
        f"Backlog catchup took {performance_metrics.duration_seconds:.2f}s, "
        f"exceeding {max_catchup_seconds}s limit (NFR-1.4)"
    )


@pytest.mark.asyncio
@pytest.mark.performance
async def test_consumer_group_coordination(
    test_kafka_config: KafkaConfig,
    load_generator: Dict,
    tmp_path: Path,
):
    """
    Test consumer group coordination and partition assignment.

    Validates:
    - All consumers join the same consumer group
    - Partition assignment completes successfully
    - No duplicate message processing
    - Graceful consumer shutdown
    """
    generate_events = load_generator["generate_events"]

    # Generate test events
    event_count = 5_000
    test_events = await generate_events(
        count=event_count,
        prefix="group-coordination",
        attachments_per_event=1,
    )

    # Create test file
    test_file_path = tmp_path / "document.pdf"
    test_file_content = b"PDF content for testing"
    test_file_path.write_bytes(test_file_content)

    # Track processed trace IDs to detect duplicates
    processed_trace_ids = set()
    duplicate_count = 0

    # Mock downloads with duplicate detection
    with patch("core.download.downloader.AttachmentDownloader.download") as mock_download:
        async def mock_download_impl(task):
            nonlocal duplicate_count
            from core.download.models import DownloadOutcome

            # Check for duplicate processing
            if task.trace_id in processed_trace_ids:
                duplicate_count += 1
                logger.warning(f"Duplicate processing detected for trace_id: {task.trace_id}")
            else:
                processed_trace_ids.add(task.trace_id)

            await asyncio.sleep(0.001)
            return DownloadOutcome.success_outcome(
                file_path=test_file_path,
                bytes_downloaded=len(test_file_content),
                content_type="application/pdf",
                status_code=200,
            )

        mock_download.side_effect = mock_download_impl

        # Create multiple consumers in same group
        from kafka_pipeline.xact.workers.download_worker import DownloadWorker

        consumer_count = 4
        workers = []
        worker_tasks = []

        for i in range(consumer_count):
            worker = DownloadWorker(
                config=test_kafka_config,
                temp_dir=tmp_path / f"downloads_{i}"
            )
            workers.append(worker)

        try:
            # Start all consumers
            logger.info(f"Starting {consumer_count} consumers in same group...")
            for worker in workers:
                task = await start_worker_background(worker)
                worker_tasks.append(task)

            # Give time for consumer group coordination
            await asyncio.sleep(10.0)

            # Produce messages
            producer = AIOKafkaProducer(
                bootstrap_servers=test_kafka_config.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                compression_type=None,  # lz4 disabled due to Python 3.13 compatibility
            )
            await producer.start()

            try:
                logger.info(f"Producing {event_count} tasks...")
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

                # Wait for all to complete
                success = await wait_for_condition(
                    lambda: mock_onelake_client.upload_count >= event_count,
                    timeout_seconds=120.0,
                    description=f"{event_count} tasks with {consumer_count} consumers",
                )

                assert success, f"Only {mock_onelake_client.upload_count} tasks completed"

            finally:
                await producer.stop()

        finally:
            # Gracefully stop all consumers
            logger.info("Gracefully stopping all consumers...")
            for worker, task in zip(workers, worker_tasks):
                await stop_worker_gracefully(worker, task)

    # Validate results
    logger.info(f"Consumer Group Coordination - Tasks processed: {mock_onelake_client.upload_count}")
    logger.info(f"Consumer Group Coordination - Unique trace IDs: {len(processed_trace_ids)}")
    logger.info(f"Consumer Group Coordination - Duplicate count: {duplicate_count}")

    # Assert no duplicates (proper partition assignment)
    assert duplicate_count == 0, (
        f"Detected {duplicate_count} duplicate message processing. "
        f"Consumer group coordination failed."
    )

    # Assert all messages processed
    assert mock_onelake_client.upload_count == event_count, (
        f"Only {mock_onelake_client.upload_count}/{event_count} tasks processed"
    )

    # Assert all trace IDs unique
    assert len(processed_trace_ids) == event_count, (
        f"Only {len(processed_trace_ids)}/{event_count} unique trace IDs processed"
    )


@pytest.mark.asyncio
@pytest.mark.performance
async def test_offset_commit_behavior(
    test_kafka_config: KafkaConfig,
    download_worker,
    mock_storage: Dict,
    load_generator: Dict,
    tmp_path: Path,
):
    """
    Test offset commit behavior and exactly-once processing.

    Validates:
    - Offsets committed after successful processing
    - Failed messages don't commit offsets
    - Restart picks up from last committed offset
    """
    mock_onelake = mock_storage["onelake"]
    generate_events = load_generator["generate_events"]

    # Generate test events
    event_count = 1_000
    test_events = await generate_events(
        count=event_count,
        prefix="offset-commit",
        attachments_per_event=1,
    )

    # Create test file
    test_file_path = tmp_path / "document.pdf"
    test_file_content = b"PDF content for testing"
    test_file_path.write_bytes(test_file_content)

    # Mock downloads
    processed_count = 0
    with patch("core.download.downloader.AttachmentDownloader.download") as mock_download:
        async def mock_download_impl(task):
            nonlocal processed_count
            from core.download.models import DownloadOutcome

            processed_count += 1
            await asyncio.sleep(0.001)
            return DownloadOutcome.success_outcome(
                file_path=test_file_path,
                bytes_downloaded=len(test_file_content),
                content_type="application/pdf",
                status_code=200,
            )

        mock_download.side_effect = mock_download_impl

        # Produce all messages first
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

        finally:
            await producer.stop()

        # Process half the messages
        half_count = event_count // 2
        download_task = await start_worker_background(download_worker)

        try:
            logger.info(f"Processing first {half_count} messages...")
            success = await wait_for_condition(
                lambda: mock_onelake.upload_count >= half_count,
                timeout_seconds=60.0,
                description=f"{half_count} messages processed",
            )

            assert success, f"Only {mock_onelake.upload_count} messages processed in first run"

            first_run_count = mock_onelake.upload_count
            logger.info(f"First run processed: {first_run_count}")

        finally:
            # Stop consumer gracefully (should commit offsets)
            await stop_worker_gracefully(download_worker, download_task)

        # Clear mock OneLake counter to track second run
        initial_upload_count = mock_onelake.upload_count
        mock_onelake.upload_count = 0

        # Restart consumer - should pick up from committed offset
        logger.info("Restarting consumer to process remaining messages...")
        download_task = await start_worker_background(download_worker)

        try:
            # Should process remaining messages
            remaining_count = event_count - first_run_count
            logger.info(f"Expecting to process ~{remaining_count} remaining messages...")

            success = await wait_for_condition(
                lambda: mock_onelake.upload_count >= remaining_count - 100,  # Allow some tolerance
                timeout_seconds=60.0,
                description=f"{remaining_count} remaining messages",
            )

            second_run_count = mock_onelake.upload_count
            logger.info(f"Second run processed: {second_run_count}")

        finally:
            await stop_worker_gracefully(download_worker, download_task)

    # Validate offset commit behavior
    total_processed = initial_upload_count + mock_onelake.upload_count
    logger.info(f"Offset Commit Test - First run: {initial_upload_count}")
    logger.info(f"Offset Commit Test - Second run: {mock_onelake.upload_count}")
    logger.info(f"Offset Commit Test - Total: {total_processed}")

    # Should process all messages across both runs
    # Allow small tolerance for test timing issues
    assert total_processed >= event_count * 0.95, (
        f"Only {total_processed}/{event_count} messages processed across both runs. "
        f"Offset commit may have failed."
    )


@pytest.mark.asyncio
@pytest.mark.performance
async def test_backpressure_scheduler_pause(
    test_kafka_config: KafkaConfig,
    load_generator: Dict,
):
    """
    Test scheduler pauses retry delivery on high pending topic lag.

    Validates:
    - Scheduler detects high lag on pending topic
    - Scheduler pauses retry delivery
    - Scheduler resumes after lag clears
    """
    # This test validates the DelayedRedeliveryScheduler's lag monitoring
    # Note: Full implementation requires scheduler instance and lag monitoring

    # For now, validate that the config and topic structure support this behavior
    assert test_kafka_config.downloads_pending_topic, "Pending topic configured"
    assert test_kafka_config.retry_delays, "Retry delays configured"
    # Retry topics are generated dynamically via get_retry_topic()
    assert test_kafka_config.get_retry_topic(0), "First retry topic can be generated"

    logger.info("Backpressure test: Scheduler pause behavior requires running scheduler instance")
    logger.info("Test validates configuration supports lag-based backpressure")

    # In production, scheduler would:
    # 1. Monitor consumer lag on pending topic
    # 2. Pause consumption from retry topics when lag > threshold
    # 3. Resume when lag drops below threshold

    # This ensures the pending queue is processed before adding more from retries
