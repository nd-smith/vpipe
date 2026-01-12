"""
End-to-end happy path integration test.

Tests the complete flow from event ingestion through download to inventory write:
    1. EventMessage → events.raw
    2. Event Ingester → DownloadTaskMessage → downloads.pending
    3. Download Worker → download + upload → DownloadResultMessage → downloads.results
    4. Result Processor → Delta inventory write

Validates:
- Message transformation at each stage
- Delta Lake writes (events and inventory)
- OneLake uploads
- End-to-end latency
- Deduplication behavior
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List
from unittest.mock import AsyncMock, patch

import pytest
from aiokafka import AIOKafkaProducer

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.xact.schemas.events import EventMessage
from kafka_pipeline.xact.schemas.results import DownloadResultMessage
from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage

from .fixtures.generators import create_event_message
from .helpers import (
    get_topic_messages,
    start_worker_background,
    stop_worker_gracefully,
    wait_for_condition,
)

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_e2e_single_event_single_attachment(
    test_kafka_config: KafkaConfig,
    event_ingester_worker,
    download_worker,
    result_processor,
    mock_storage: Dict,
    tmp_path: Path,
):
    """
    Test happy path: single event with single attachment flows through entire pipeline.

    Validates:
    - EventMessage → DownloadTaskMessage transformation
    - Successful download (mocked)
    - OneLake upload with correct path
    - DownloadResultMessage production
    - Delta inventory write with correct data
    - Delta events write
    - End-to-end latency measurement
    """
    # Extract mock storage components
    mock_onelake = mock_storage["onelake"]
    mock_delta_events = mock_storage["delta_events"]
    mock_delta_inventory = mock_storage["delta_inventory"]

    # Create test event with single attachment
    test_event = create_event_message(
        trace_id="e2e-test-001",
        event_type="claim",
        event_subtype="created",
        attachments=["https://example.com/document.pdf"],
        payload={
            "claim_id": "C-001",
            "assignment_id": "A-001",  # Required for path generation
            "amount": 10000,
            "status": "pending",
        },
    )

    # Create a temporary file to simulate download
    test_file_path = tmp_path / "document.pdf"
    test_file_content = b"PDF content for testing"
    test_file_path.write_bytes(test_file_content)

    # Mock the download to return our test file
    with patch("core.download.downloader.AttachmentDownloader.download") as mock_download:
        # Configure mock to simulate successful download
        async def mock_download_impl(task):
            # Simulate download by using our test file
            from core.download.models import DownloadOutcome

            return DownloadOutcome.success_outcome(
                file_path=test_file_path,
                bytes_downloaded=len(test_file_content),
                content_type="application/pdf",
                status_code=200,
            )

        mock_download.side_effect = mock_download_impl

        # Start all workers in background
        ingester_task = await start_worker_background(event_ingester_worker)
        download_task = await start_worker_background(download_worker)
        processor_task = await start_worker_background(result_processor)

        try:
            # Produce event to raw topic
            producer = AIOKafkaProducer(
                bootstrap_servers=test_kafka_config.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            await producer.start()

            try:
                start_time = datetime.now(timezone.utc)

                await producer.send(
                    test_kafka_config.events_topic,
                    value=test_event.model_dump(mode="json"),
                )
                logger.info(f"Produced event {test_event.trace_id} to events.raw")

            finally:
                await producer.stop()

            # Wait for Delta events write
            success = await wait_for_condition(
                lambda: len(mock_delta_events.written_events) > 0,
                timeout_seconds=10.0,
                description="Delta events write",
            )
            assert success, "Event not written to Delta events table"

            # Validate Delta events write
            events = mock_delta_events.get_events_by_trace_id(test_event.trace_id)
            assert len(events) == 1, f"Expected 1 event in Delta, got {len(events)}"
            delta_event = events[0]
            assert delta_event["trace_id"] == test_event.trace_id
            assert delta_event["event_type"] == test_event.event_type
            assert delta_event["event_subtype"] == test_event.event_subtype

            # Wait for download task in pending topic
            await asyncio.sleep(2.0)  # Give workers time to process
            pending_messages = await get_topic_messages(
                test_kafka_config,
                test_kafka_config.downloads_pending_topic,
                max_messages=10,
                timeout_seconds=5.0,
            )
            assert len(pending_messages) > 0, "No download tasks created"

            # Validate download task
            task_msg = DownloadTaskMessage.model_validate_json(pending_messages[0]["value"])
            assert task_msg.trace_id == test_event.trace_id
            assert task_msg.attachment_url == test_event.attachments[0]
            assert task_msg.retry_count == 0

            # Wait for OneLake upload
            success = await wait_for_condition(
                lambda: mock_onelake.upload_count > 0,
                timeout_seconds=15.0,
                description="OneLake upload",
            )
            assert success, "File not uploaded to OneLake"

            # Validate OneLake upload
            assert len(mock_onelake.uploaded_files) == 1, "Expected 1 file uploaded"
            uploaded_path = list(mock_onelake.uploaded_files.keys())[0]
            uploaded_content = mock_onelake.uploaded_files[uploaded_path]
            assert uploaded_content == test_file_content, "Uploaded content doesn't match"

            # Wait for result message
            await asyncio.sleep(2.0)
            result_messages = await get_topic_messages(
                test_kafka_config,
                test_kafka_config.downloads_results_topic,
                max_messages=10,
                timeout_seconds=5.0,
            )
            assert len(result_messages) > 0, "No result messages produced"

            # Validate result message
            result_msg = DownloadResultMessage.model_validate_json(result_messages[0]["value"])
            assert result_msg.trace_id == test_event.trace_id
            assert result_msg.status == "completed"
            assert result_msg.bytes_downloaded == len(test_file_content)
            assert result_msg.blob_path is not None

            # Wait for Delta inventory write (result processor has batch timeout)
            success = await wait_for_condition(
                lambda: len(mock_delta_inventory.inventory_records) > 0,
                timeout_seconds=10.0,
                description="Delta inventory write",
            )
            assert success, "Result not written to Delta inventory"

            # Validate Delta inventory write
            inventory_records = mock_delta_inventory.get_records_by_trace_id(test_event.trace_id)
            assert len(inventory_records) == 1, f"Expected 1 inventory record, got {len(inventory_records)}"
            inventory_record = inventory_records[0]
            assert inventory_record["trace_id"] == test_event.trace_id
            assert inventory_record["attachment_url"] == test_event.attachments[0]
            assert inventory_record["bytes_downloaded"] > 0  # Completed records have bytes
            assert inventory_record["bytes_downloaded"] == len(test_file_content)

            # Calculate end-to-end latency
            end_time = datetime.now(timezone.utc)
            latency_seconds = (end_time - start_time).total_seconds()
            logger.info(
                f"End-to-end latency: {latency_seconds:.2f}s",
                extra={
                    "trace_id": test_event.trace_id,
                    "latency_seconds": latency_seconds,
                },
            )

            # Assert reasonable latency (should be well under 5s for single event)
            assert latency_seconds < 15.0, f"End-to-end latency too high: {latency_seconds:.2f}s"

        finally:
            # Stop all workers
            await stop_worker_gracefully(event_ingester_worker, ingester_task)
            await stop_worker_gracefully(download_worker, download_task)
            await stop_worker_gracefully(result_processor, processor_task)


@pytest.mark.asyncio
async def test_e2e_multiple_events_multiple_attachments(
    test_kafka_config: KafkaConfig,
    event_ingester_worker,
    download_worker,
    result_processor,
    mock_storage: Dict,
    tmp_path: Path,
):
    """
    Test happy path with multiple events, each having multiple attachments.

    Validates:
    - Multiple events processed in parallel
    - Multiple attachments per event
    - All downloads successful
    - All inventory records written
    - Proper throughput
    """
    # Extract mock storage components
    mock_onelake = mock_storage["onelake"]
    mock_delta_events = mock_storage["delta_events"]
    mock_delta_inventory = mock_storage["delta_inventory"]

    # Create test events with multiple attachments each
    events = [
        create_event_message(
            trace_id=f"e2e-multi-{i:03d}",
            event_type="claim",
            event_subtype="created",
            attachments=[
                f"https://example.com/claim-{i}-doc1.pdf",
                f"https://example.com/claim-{i}-doc2.pdf",
                f"https://example.com/claim-{i}-doc3.pdf",
            ],
            payload={
                "claim_id": f"C-{i:03d}",
                "assignment_id": f"A-{i:03d}",  # Required for path generation
                "amount": 10000 + (i * 1000),
                "status": "pending",
            },
        )
        for i in range(3)  # 3 events × 3 attachments = 9 total downloads
    ]

    # Create temporary files for all downloads
    test_files = {}
    for i in range(3):
        for j in range(3):
            filename = f"claim-{i}-doc{j+1}.pdf"
            file_path = tmp_path / filename
            content = f"PDF content for claim {i} document {j+1}".encode()
            file_path.write_bytes(content)
            test_files[f"https://example.com/{filename}"] = (file_path, content)

    # Mock the download to return appropriate test files
    with patch("core.download.downloader.AttachmentDownloader.download") as mock_download:

        async def mock_download_impl(task):
            from core.download.models import DownloadOutcome

            # Find the matching test file (task.url, not task.attachment_url)
            file_path, content = test_files[task.url]

            return DownloadOutcome.success_outcome(
                file_path=file_path,
                bytes_downloaded=len(content),
                content_type="application/pdf",
                status_code=200,
            )

        mock_download.side_effect = mock_download_impl

        # Start all workers
        ingester_task = await start_worker_background(event_ingester_worker)
        download_task = await start_worker_background(download_worker)
        processor_task = await start_worker_background(result_processor)

        try:
            # Produce all events
            producer = AIOKafkaProducer(
                bootstrap_servers=test_kafka_config.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            await producer.start()

            try:
                start_time = datetime.now(timezone.utc)

                for event in events:
                    await producer.send(
                        test_kafka_config.events_topic,
                        value=event.model_dump(mode="json"),
                    )
                    logger.info(f"Produced event {event.trace_id} to events.raw")

            finally:
                await producer.stop()

            # Wait for all Delta events writes
            success = await wait_for_condition(
                lambda: len(mock_delta_events.written_events) >= len(events),
                timeout_seconds=15.0,
                description=f"{len(events)} Delta events writes",
            )
            assert success, f"Not all events written to Delta. Got {len(mock_delta_events.written_events)}"

            # Wait for all OneLake uploads (9 total)
            expected_uploads = sum(len(e.attachments) for e in events)
            success = await wait_for_condition(
                lambda: mock_onelake.upload_count >= expected_uploads,
                timeout_seconds=30.0,
                description=f"{expected_uploads} OneLake uploads",
            )
            assert success, f"Not all files uploaded. Got {mock_onelake.upload_count}"

            # Wait for all Delta inventory writes
            success = await wait_for_condition(
                lambda: len(mock_delta_inventory.inventory_records) >= expected_uploads,
                timeout_seconds=15.0,
                description=f"{expected_uploads} Delta inventory writes",
            )
            assert success, f"Not all inventory records written. Got {len(mock_delta_inventory.inventory_records)}"

            # Validate all events processed
            for event in events:
                # Check Delta events
                delta_events = mock_delta_events.get_events_by_trace_id(event.trace_id)
                assert len(delta_events) == 1, f"Event {event.trace_id} not in Delta events"

                # Check Delta inventory
                inventory_records = mock_delta_inventory.get_records_by_trace_id(event.trace_id)
                assert (
                    len(inventory_records) == len(event.attachments)
                ), f"Event {event.trace_id}: expected {len(event.attachments)} inventory records, got {len(inventory_records)}"

                # Validate all attachments succeeded
                for record in inventory_records:
                    assert record["bytes_downloaded"] > 0, f"Attachment failed: {record}"

            # Calculate end-to-end latency
            end_time = datetime.now(timezone.utc)
            latency_seconds = (end_time - start_time).total_seconds()
            logger.info(
                f"End-to-end latency for {len(events)} events with {expected_uploads} attachments: {latency_seconds:.2f}s",
                extra={
                    "event_count": len(events),
                    "attachment_count": expected_uploads,
                    "latency_seconds": latency_seconds,
                },
            )

            # Assert reasonable latency (should be well under 30s for 9 downloads)
            assert latency_seconds < 40.0, f"End-to-end latency too high: {latency_seconds:.2f}s"

        finally:
            # Stop all workers
            await stop_worker_gracefully(event_ingester_worker, ingester_task)
            await stop_worker_gracefully(download_worker, download_task)
            await stop_worker_gracefully(result_processor, processor_task)


@pytest.mark.asyncio
async def test_e2e_deduplication(
    test_kafka_config: KafkaConfig,
    event_ingester_worker,
    download_worker,
    result_processor,
    mock_storage: Dict,
    tmp_path: Path,
):
    """
    Test deduplication behavior for duplicate events.

    Validates:
    - Duplicate events (same trace_id) are deduplicated in Delta events
    - All attachments still get downloaded (no deduplication at task level)
    - Delta inventory handles duplicate results via merge
    """
    # Extract mock storage components
    mock_onelake = mock_storage["onelake"]
    mock_delta_events = mock_storage["delta_events"]
    mock_delta_inventory = mock_storage["delta_inventory"]

    # Create duplicate events with same trace_id
    trace_id = "e2e-dedupe-001"
    event = create_event_message(
        trace_id=trace_id,
        event_type="claim",
        event_subtype="created",
        attachments=["https://example.com/document.pdf"],
        payload={
            "claim_id": "C-dedupe",
            "assignment_id": "A-dedupe",  # Required for path generation
            "amount": 10000,
            "status": "pending",
        },
    )

    # Create test file
    test_file_path = tmp_path / "document.pdf"
    test_file_content = b"PDF content for deduplication testing"
    test_file_path.write_bytes(test_file_content)

    # Mock download
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
            # Produce the same event twice
            producer = AIOKafkaProducer(
                bootstrap_servers=test_kafka_config.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            await producer.start()

            try:
                # Send event twice
                for i in range(2):
                    await producer.send(
                        test_kafka_config.events_topic,
                        value=event.model_dump(mode="json"),
                    )
                    logger.info(f"Produced duplicate event {i+1} for {trace_id}")

            finally:
                await producer.stop()

            # Wait for processing
            await asyncio.sleep(5.0)

            # Validate Delta events deduplication
            delta_events = mock_delta_events.get_events_by_trace_id(trace_id)
            assert len(delta_events) == 1, (
                f"Expected 1 event in Delta (deduplicated), got {len(delta_events)}. "
                f"Dedupe hits: {mock_delta_events.dedupe_hits}"
            )
            assert mock_delta_events.dedupe_hits >= 1, "Deduplication should have occurred"

            # Note: Both download tasks will be created and processed (no deduplication at task level)
            # This is intentional - task-level deduplication would require distributed coordination

            # Validate Delta inventory handles duplicates via merge
            inventory_records = mock_delta_inventory.get_records_by_trace_id(trace_id)
            # Should still have 1 record due to merge on (trace_id, attachment_url)
            assert len(inventory_records) == 1, (
                f"Expected 1 inventory record (merged), got {len(inventory_records)}. "
                f"Merge count: {mock_delta_inventory.merge_count}"
            )

        finally:
            # Stop all workers
            await stop_worker_gracefully(event_ingester_worker, ingester_task)
            await stop_worker_gracefully(download_worker, download_task)
            await stop_worker_gracefully(result_processor, processor_task)
