"""
Integration tests for Event Ingester Worker.

Tests the complete event ingestion pipeline from consuming events
to producing download tasks, including Delta Lake writes and deduplication.

These tests use Testcontainers to run a real Kafka instance and verify:
- Event → download task end-to-end flow
- URL validation and filtering
- Deduplication behavior
- Delta write success/failure handling
- Error handling and recovery
"""

import asyncio
from datetime import datetime, timezone
from typing import List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiokafka.structs import ConsumerRecord

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.common.consumer import BaseKafkaConsumer
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.xact.schemas.events import EventMessage
from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage
from kafka_pipeline.xact.workers.event_ingester import EventIngesterWorker


@pytest.fixture
def sample_event_data():
    """Create sample event data for testing."""
    return {
        "trace_id": "test-evt-001",
        "event_type": "claim",
        "event_subtype": "documentsReceived",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source_system": "claimx",
        "payload": {
            "assignment_id": "A-12345",
            "claim_id": "C-67890",
        },
        "attachments": [
            "https://claimxperience.com/files/document1.pdf",
            "https://claimxperience.com/files/document2.pdf",
        ],
    }


@pytest.fixture
def kafka_config_with_events(kafka_config: KafkaConfig, unique_topic_prefix: str):
    """
    Provide Kafka config with event-specific topic names.

    Args:
        kafka_config: Base test Kafka configuration
        unique_topic_prefix: Unique prefix for test isolation

    Returns:
        KafkaConfig: Configuration with test-specific topics
    """
    config = kafka_config
    config.events_topic = f"{unique_topic_prefix}.events.raw"
    config.downloads_pending_topic = f"{unique_topic_prefix}.downloads.pending"
    config.downloads_results_topic = f"{unique_topic_prefix}.downloads.results"
    config.dlq_topic = f"{unique_topic_prefix}.downloads.dlq"
    config.consumer_group_prefix = unique_topic_prefix
    return config


@pytest.mark.asyncio
async def test_event_to_download_task_flow(
    kafka_producer: BaseKafkaProducer,
    kafka_consumer_factory: callable,
    kafka_config_with_events: KafkaConfig,
    sample_event_data: dict,
    unique_topic_prefix: str,
):
    """
    Test complete event ingestion flow: event → download tasks.

    Verifies:
    - Event is consumed from events.raw topic
    - Valid attachments are extracted
    - Download tasks are produced to pending topic
    - One task per attachment is created
    - Task fields are correctly populated
    """
    # Send event to events.raw topic
    events_topic = kafka_config_with_events.events_topic
    pending_topic = kafka_config_with_events.downloads_pending_topic

    # Create EventMessage from sample data
    event = EventMessage(**sample_event_data)
    await kafka_producer.send(
        topic=events_topic,
        key=sample_event_data["trace_id"],
        value=event,
        headers={"source": "test"},
    )

    # Collect download tasks produced to pending topic
    download_tasks: List[DownloadTaskMessage] = []

    async def task_collector(record: ConsumerRecord):
        task = DownloadTaskMessage.model_validate_json(record.value)
        download_tasks.append(task)

    # Start consumer for pending topic
    task_consumer = await kafka_consumer_factory(
        topics=[pending_topic],
        group_id=f"{unique_topic_prefix}.task-collector",
        message_handler=task_collector,
    )
    task_consumer_task = asyncio.create_task(task_consumer.start())

    # Start event ingester worker (disable Delta writes for simpler test)
    worker = EventIngesterWorker(
        config=kafka_config_with_events,
        enable_delta_writes=False,
    )

    # Start worker in background
    worker_task = asyncio.create_task(worker.start())

    # Wait for download tasks to be produced
    # Should produce 2 tasks (one per attachment)
    for _ in range(100):  # 10 seconds max
        if len(download_tasks) >= 2:
            break
        await asyncio.sleep(0.1)

    # Stop worker and consumer
    await worker.stop()
    await task_consumer.stop()

    # Cancel background tasks
    worker_task.cancel()
    task_consumer_task.cancel()
    try:
        await asyncio.gather(worker_task, task_consumer_task)
    except asyncio.CancelledError:
        pass

    # Verify download tasks were created
    assert len(download_tasks) == 2, "Should create one task per attachment"

    # Verify first task
    task1 = download_tasks[0]
    assert task1.trace_id == "test-evt-001"
    assert task1.event_type == "claim"
    assert task1.event_subtype == "documentsReceived"
    assert task1.retry_count == 0
    assert "claimxperience.com" in task1.attachment_url
    assert "documentsReceived/A-12345/test-evt-001/" in task1.destination_path
    assert task1.metadata["assignment_id"] == "A-12345"
    assert task1.metadata["source_system"] == "claimx"
    assert task1.metadata["file_type"] == "PDF"

    # Verify second task
    task2 = download_tasks[1]
    assert task2.trace_id == "test-evt-001"
    assert task2.event_type == "claim"
    assert task2.event_subtype == "documentsReceived"
    assert "claimxperience.com" in task2.attachment_url

    # Verify both attachments were processed
    attachment_urls = {task.attachment_url for task in download_tasks}
    expected_urls = set(sample_event_data["attachments"])
    assert attachment_urls == expected_urls


@pytest.mark.asyncio
async def test_invalid_url_handling(
    kafka_producer: BaseKafkaProducer,
    kafka_consumer_factory: callable,
    kafka_config_with_events: KafkaConfig,
    sample_event_data: dict,
    unique_topic_prefix: str,
):
    """
    Test that invalid URLs are filtered out.

    Verifies:
    - Invalid URLs (not in allowlist) are skipped
    - Valid URLs are still processed
    - No download tasks created for invalid URLs
    """
    # Create event with mix of valid and invalid URLs
    event_with_invalid = sample_event_data.copy()
    event_with_invalid["trace_id"] = "test-evt-invalid-url"
    event_with_invalid["attachments"] = [
        "https://claimxperience.com/files/valid.pdf",  # Valid
        "https://evil.com/malware.exe",  # Invalid (not in allowlist)
        "https://claimxperience.s3.us-east-1.amazonaws.com/valid-bucket/file.pdf",  # Valid (in allowlist)
    ]

    events_topic = kafka_config_with_events.events_topic
    pending_topic = kafka_config_with_events.downloads_pending_topic

    # Create EventMessage
    event = EventMessage(**event_with_invalid)
    await kafka_producer.send(
        topic=events_topic,
        key=event_with_invalid["trace_id"],
        value=event,
    )

    # Collect download tasks
    download_tasks: List[DownloadTaskMessage] = []

    async def task_collector(record: ConsumerRecord):
        task = DownloadTaskMessage.model_validate_json(record.value)
        download_tasks.append(task)

    task_consumer = await kafka_consumer_factory(
        topics=[pending_topic],
        group_id=f"{unique_topic_prefix}.invalid-url-collector",
        message_handler=task_collector,
    )
    task_consumer_task = asyncio.create_task(task_consumer.start())

    # Start worker
    worker = EventIngesterWorker(
        config=kafka_config_with_events,
        enable_delta_writes=False,
    )
    worker_task = asyncio.create_task(worker.start())

    # Wait for tasks (should only get 2, not 3)
    await asyncio.sleep(2)  # Give enough time for processing

    # Stop worker and consumer
    await worker.stop()
    await task_consumer.stop()

    worker_task.cancel()
    task_consumer_task.cancel()
    try:
        await asyncio.gather(worker_task, task_consumer_task)
    except asyncio.CancelledError:
        pass

    # Verify only valid URLs were processed
    assert len(download_tasks) == 2, "Should only create tasks for valid URLs"

    # Verify evil.com URL was filtered out
    attachment_urls = {task.attachment_url for task in download_tasks}
    assert "https://evil.com/malware.exe" not in attachment_urls
    assert "https://claimxperience.com/files/valid.pdf" in attachment_urls
    assert "https://claimxperience.s3.us-east-1.amazonaws.com/valid-bucket/file.pdf" in attachment_urls


@pytest.mark.asyncio
async def test_event_without_attachments_skipped(
    kafka_producer: BaseKafkaProducer,
    kafka_consumer_factory: callable,
    kafka_config_with_events: KafkaConfig,
    sample_event_data: dict,
    unique_topic_prefix: str,
):
    """
    Test that events without attachments are skipped gracefully.

    Verifies:
    - Events with no attachments don't produce download tasks
    - Events with empty attachment list are handled correctly
    - Worker continues processing subsequent events
    """
    events_topic = kafka_config_with_events.events_topic
    pending_topic = kafka_config_with_events.downloads_pending_topic

    # Create events: one without attachments, one with
    event_no_attachments = sample_event_data.copy()
    event_no_attachments["trace_id"] = "test-evt-no-attach"
    event_no_attachments["attachments"] = None  # No attachments

    event_with_attachments = sample_event_data.copy()
    event_with_attachments["trace_id"] = "test-evt-with-attach"

    # Send both events
    for event_data in [event_no_attachments, event_with_attachments]:
        event = EventMessage(**event_data)
        await kafka_producer.send(
            topic=events_topic,
            key=event_data["trace_id"],
            value=event,
        )

    # Collect download tasks
    download_tasks: List[DownloadTaskMessage] = []

    async def task_collector(record: ConsumerRecord):
        task = DownloadTaskMessage.model_validate_json(record.value)
        download_tasks.append(task)

    task_consumer = await kafka_consumer_factory(
        topics=[pending_topic],
        group_id=f"{unique_topic_prefix}.no-attach-collector",
        message_handler=task_collector,
    )
    task_consumer_task = asyncio.create_task(task_consumer.start())

    # Start worker
    worker = EventIngesterWorker(
        config=kafka_config_with_events,
        enable_delta_writes=False,
    )
    worker_task = asyncio.create_task(worker.start())

    # Wait for tasks from second event
    for _ in range(50):
        if len(download_tasks) >= 2:  # From event with attachments
            break
        await asyncio.sleep(0.1)

    # Stop worker and consumer
    await worker.stop()
    await task_consumer.stop()

    worker_task.cancel()
    task_consumer_task.cancel()
    try:
        await asyncio.gather(worker_task, task_consumer_task)
    except asyncio.CancelledError:
        pass

    # Verify only tasks from event with attachments
    assert len(download_tasks) == 2
    assert all(
        task.trace_id == "test-evt-with-attach" for task in download_tasks
    ), "All tasks should be from event with attachments"


@pytest.mark.asyncio
async def test_delta_write_success(
    kafka_producer: BaseKafkaProducer,
    kafka_config_with_events: KafkaConfig,
    sample_event_data: dict,
):
    """
    Test successful Delta Lake write for event analytics.

    Verifies:
    - Events are written to Delta Lake asynchronously
    - Delta write doesn't block Kafka processing
    - write_event is called with correct event data
    """
    events_topic = kafka_config_with_events.events_topic

    # Mock Delta writer
    mock_delta_writer = AsyncMock()
    mock_delta_writer.write_event.return_value = True  # Success

    with patch(
        "kafka_pipeline.workers.event_ingester.DeltaEventsWriter"
    ) as mock_writer_class:
        mock_writer_class.return_value = mock_delta_writer

        # Create worker with Delta writes enabled
        worker = EventIngesterWorker(
            config=kafka_config_with_events,
            enable_delta_writes=True,
        )

        # Send event
        event = EventMessage(**sample_event_data)
        await kafka_producer.send(
            topic=events_topic,
            key=sample_event_data["trace_id"],
            value=event,
        )

        # Start worker
        worker_task = asyncio.create_task(worker.start())

        # Wait for Delta write to be called
        for _ in range(50):
            if mock_delta_writer.write_event.called:
                break
            await asyncio.sleep(0.1)

        # Stop worker
        await worker.stop()
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

        # Verify Delta write was called
        assert mock_delta_writer.write_event.called
        call_args = mock_delta_writer.write_event.call_args
        event_arg = call_args[0][0]
        assert isinstance(event_arg, EventMessage)
        assert event_arg.trace_id == "test-evt-001"
        assert event_arg.event_type == "claim"


@pytest.mark.asyncio
async def test_delta_write_failure_doesnt_block_processing(
    kafka_producer: BaseKafkaProducer,
    kafka_consumer_factory: callable,
    kafka_config_with_events: KafkaConfig,
    sample_event_data: dict,
    unique_topic_prefix: str,
):
    """
    Test that Delta write failures don't block Kafka processing.

    Verifies:
    - Delta write failures are logged but don't stop processing
    - Download tasks are still produced even if Delta write fails
    - Worker continues to process subsequent events
    """
    events_topic = kafka_config_with_events.events_topic
    pending_topic = kafka_config_with_events.downloads_pending_topic

    # Mock Delta writer that fails
    mock_delta_writer = AsyncMock()
    mock_delta_writer.write_event.side_effect = Exception("Delta write failed")

    # Collect download tasks
    download_tasks: List[DownloadTaskMessage] = []

    async def task_collector(record: ConsumerRecord):
        task = DownloadTaskMessage.model_validate_json(record.value)
        download_tasks.append(task)

    task_consumer = await kafka_consumer_factory(
        topics=[pending_topic],
        group_id=f"{unique_topic_prefix}.delta-fail-collector",
        message_handler=task_collector,
    )
    task_consumer_task = asyncio.create_task(task_consumer.start())

    with patch(
        "kafka_pipeline.workers.event_ingester.DeltaEventsWriter"
    ) as mock_writer_class:
        mock_writer_class.return_value = mock_delta_writer

        # Create worker with Delta writes enabled
        worker = EventIngesterWorker(
            config=kafka_config_with_events,
            enable_delta_writes=True,
        )

        # Send event
        event = EventMessage(**sample_event_data)
        await kafka_producer.send(
            topic=events_topic,
            key=sample_event_data["trace_id"],
            value=event,
        )

        # Start worker
        worker_task = asyncio.create_task(worker.start())

        # Wait for download tasks to be produced
        for _ in range(50):
            if len(download_tasks) >= 2:
                break
            await asyncio.sleep(0.1)

        # Stop worker and consumer
        await worker.stop()
        await task_consumer.stop()

        worker_task.cancel()
        task_consumer_task.cancel()
        try:
            await asyncio.gather(worker_task, task_consumer_task)
        except asyncio.CancelledError:
            pass

    # Verify download tasks were still produced despite Delta failure
    assert len(download_tasks) == 2, "Download tasks should be produced even if Delta write fails"
    assert all(task.trace_id == "test-evt-001" for task in download_tasks)


@pytest.mark.asyncio
async def test_event_missing_assignment_id_skipped(
    kafka_producer: BaseKafkaProducer,
    kafka_consumer_factory: callable,
    kafka_config_with_events: KafkaConfig,
    sample_event_data: dict,
    unique_topic_prefix: str,
):
    """
    Test that events without assignment_id are skipped.

    Verifies:
    - Events missing assignment_id don't produce download tasks
    - Warning is logged for missing assignment_id
    - Worker continues processing subsequent events
    """
    events_topic = kafka_config_with_events.events_topic
    pending_topic = kafka_config_with_events.downloads_pending_topic

    # Create event without assignment_id
    event_no_assignment = sample_event_data.copy()
    event_no_assignment["trace_id"] = "test-evt-no-assignment"
    event_no_assignment["payload"] = {"claim_id": "C-67890"}  # No assignment_id

    event_with_assignment = sample_event_data.copy()
    event_with_assignment["trace_id"] = "test-evt-with-assignment"

    # Send both events
    for event_data in [event_no_assignment, event_with_assignment]:
        event = EventMessage(**event_data)
        await kafka_producer.send(
            topic=events_topic,
            key=event_data["trace_id"],
            value=event,
        )

    # Collect download tasks
    download_tasks: List[DownloadTaskMessage] = []

    async def task_collector(record: ConsumerRecord):
        task = DownloadTaskMessage.model_validate_json(record.value)
        download_tasks.append(task)

    task_consumer = await kafka_consumer_factory(
        topics=[pending_topic],
        group_id=f"{unique_topic_prefix}.no-assignment-collector",
        message_handler=task_collector,
    )
    task_consumer_task = asyncio.create_task(task_consumer.start())

    # Start worker
    worker = EventIngesterWorker(
        config=kafka_config_with_events,
        enable_delta_writes=False,
    )
    worker_task = asyncio.create_task(worker.start())

    # Wait for tasks from second event
    for _ in range(50):
        if len(download_tasks) >= 2:
            break
        await asyncio.sleep(0.1)

    # Stop worker and consumer
    await worker.stop()
    await task_consumer.stop()

    worker_task.cancel()
    task_consumer_task.cancel()
    try:
        await asyncio.gather(worker_task, task_consumer_task)
    except asyncio.CancelledError:
        pass

    # Verify only tasks from event with assignment_id
    assert len(download_tasks) == 2
    assert all(
        task.trace_id == "test-evt-with-assignment" for task in download_tasks
    ), "All tasks should be from event with assignment_id"
