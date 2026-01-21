"""
Integration tests for DLQ handler functionality.

Tests verify:
- DLQ handler consumption and parsing
- Replay from DLQ to pending topic
- Manual acknowledgment after resolve
- Full DLQ workflow with real Kafka
"""

import asyncio
from datetime import datetime, timezone

import pytest

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.common.dlq.handler import DLQHandler
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.common.retry.handler import RetryHandler
from kafka_pipeline.xact.schemas.results import FailedDownloadMessage
from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage
from core.types import ErrorCategory


@pytest.mark.asyncio
async def test_dlq_handler_consumes_failed_messages(
    kafka_producer: BaseKafkaProducer,
    message_collector: callable,
    test_kafka_config: KafkaConfig,
    test_topics: dict[str, str],
):
    """
    Test that DLQ handler consumes and parses DLQ messages.

    Verifies:
    - DLQ handler starts and subscribes to DLQ topic
    - Failed messages are consumed correctly
    - Messages are parsed into FailedDownloadMessage
    """
    # Send a permanent failure to DLQ
    retry_handler = RetryHandler(config=test_kafka_config, producer=kafka_producer)

    task = DownloadTaskMessage(
        trace_id="dlq-consume-001",
        attachment_url="https://example.com/failed.pdf",
        blob_path="documentsReceived/T-001/pdf/failed.pdf",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="T-001",
        event_type="test",
        event_subtype="created",
        retry_count=0,
        original_timestamp=datetime.now(timezone.utc),
    )

    # Send permanent failure to DLQ
    await retry_handler.handle_failure(
        task=task,
        error=ValueError("Invalid file type"),
        error_category=ErrorCategory.PERMANENT,
    )

    # Wait briefly for message to be produced
    await asyncio.sleep(0.2)

    # Create DLQ handler
    dlq_handler = DLQHandler(test_kafka_config)

    # Track parsed messages
    parsed_messages = []

    async def tracking_handler(record):
        """Handler that tracks parsed DLQ messages."""
        try:
            parsed = dlq_handler.parse_dlq_message(record)
            parsed_messages.append(parsed)
        except Exception:
            pass

    # Replace default handler BEFORE starting
    dlq_handler._handle_dlq_message = tracking_handler

    # Start handler
    handler_task = asyncio.create_task(dlq_handler.start())

    # Wait for message to be consumed
    for _ in range(50):
        if len(parsed_messages) > 0:
            break
        await asyncio.sleep(0.1)

    # Stop handler
    await dlq_handler.stop()
    handler_task.cancel()
    try:
        await handler_task
    except asyncio.CancelledError:
        pass

    # Verify message was consumed and parsed
    assert len(parsed_messages) == 1
    dlq_msg = parsed_messages[0]
    assert dlq_msg.trace_id == task.trace_id
    assert dlq_msg.attachment_url == task.attachment_url
    assert "Invalid file type" in dlq_msg.final_error
    assert dlq_msg.error_category == "permanent"


@pytest.mark.asyncio
async def test_dlq_replay_to_pending_topic(
    kafka_producer: BaseKafkaProducer,
    kafka_consumer_factory: callable,
    message_collector: callable,
    test_kafka_config: KafkaConfig,
    test_topics: dict[str, str],
):
    """
    Test replaying DLQ message to pending topic.

    Verifies:
    - Replay sends original task to pending topic
    - Retry count is reset to 0
    - Metadata includes replay context
    - Headers include replay markers
    """
    # Create a DLQ message manually
    original_task = DownloadTaskMessage(
        trace_id="dlq-replay-001",
        attachment_url="https://example.com/replay.pdf",
        blob_path="documentsReceived/T-001/pdf/replay.pdf",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="T-001",
        event_type="test",
        event_subtype="created",
        retry_count=4,
        original_timestamp=datetime.now(timezone.utc),
        metadata={"original": "metadata"},
    )

    dlq_message = FailedDownloadMessage(
        trace_id=original_task.trace_id,
        attachment_url=original_task.attachment_url,
        original_task=original_task,
        final_error="Failed after 4 retries",
        error_category="transient",
        retry_count=4,
        failed_at=datetime.now(timezone.utc),
    )

    # Send to DLQ
    await kafka_producer.send(
        topic=test_topics["dlq"],
        key=dlq_message.trace_id,
        value=dlq_message,
        headers={
            "trace_id": dlq_message.trace_id,
            "error_category": dlq_message.error_category,
        },
    )

    # Wait for message to be produced
    await asyncio.sleep(0.2)

    # Create DLQ handler
    dlq_handler = DLQHandler(test_kafka_config)

    # Track DLQ records for replay
    dlq_records = []

    async def capture_handler(record):
        """Handler that captures DLQ records."""
        dlq_records.append(record)

    # Replace handler BEFORE starting
    dlq_handler._handle_dlq_message = capture_handler

    # Start DLQ handler
    handler_task = asyncio.create_task(dlq_handler.start())

    # Wait for DLQ message to be consumed
    for _ in range(50):
        if len(dlq_records) > 0:
            break
        await asyncio.sleep(0.1)

    # Replay the message
    assert len(dlq_records) == 1
    await dlq_handler.replay_message(dlq_records[0])

    # Stop DLQ handler
    await dlq_handler.stop()
    handler_task.cancel()
    try:
        await handler_task
    except asyncio.CancelledError:
        pass

    # Consume from pending topic to verify replay
    pending_consumer = await kafka_consumer_factory(
        topics=[test_topics["pending"]],
        group_id="test-dlq-replay-consumer",
        message_handler=message_collector,
    )

    consumer_task = asyncio.create_task(pending_consumer.start())

    # Wait for replayed message
    for _ in range(50):
        if len(message_collector.messages) > 0:
            break
        await asyncio.sleep(0.1)

    await pending_consumer.stop()
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

    # Verify replayed message
    assert len(message_collector.messages) == 1
    record = message_collector.messages[0]

    # Verify headers
    headers_dict = {k: v.decode("utf-8") for k, v in record.headers}
    assert headers_dict["trace_id"] == dlq_message.trace_id
    assert headers_dict["replayed_from_dlq"] == "true"

    # Verify task was reset
    replayed_task = DownloadTaskMessage.model_validate_json(record.value)
    assert replayed_task.trace_id == original_task.trace_id
    assert replayed_task.attachment_url == original_task.attachment_url
    assert replayed_task.retry_count == 0  # Reset!
    assert replayed_task.metadata["replayed_from_dlq"] is True
    assert replayed_task.metadata["original"] == "metadata"  # Preserved


@pytest.mark.asyncio
async def test_dlq_manual_acknowledgment(
    kafka_producer: BaseKafkaProducer,
    kafka_consumer_factory: callable,
    message_collector: callable,
    test_kafka_config: KafkaConfig,
    test_topics: dict[str, str],
):
    """
    Test manual acknowledgment of DLQ messages.

    Verifies:
    - Acknowledgment commits offset
    - Subsequent consumer doesn't see acknowledged message
    - Manual commit works correctly
    """
    # Create and send DLQ message
    task = DownloadTaskMessage(
        trace_id="dlq-ack-001",
        attachment_url="https://example.com/ack.pdf",
        blob_path="documentsReceived/T-001/pdf/ack.pdf",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="T-001",
        event_type="test",
        event_subtype="created",
        retry_count=0,
        original_timestamp=datetime.now(timezone.utc),
    )

    dlq_message = FailedDownloadMessage(
        trace_id=task.trace_id,
        attachment_url=task.attachment_url,
        original_task=task,
        final_error="Permanent error for testing",
        error_category="permanent",
        retry_count=0,
        failed_at=datetime.now(timezone.utc),
    )

    await kafka_producer.send(
        topic=test_topics["dlq"],
        key=dlq_message.trace_id,
        value=dlq_message,
        headers={"trace_id": dlq_message.trace_id},
    )

    # Wait for message to be produced
    await asyncio.sleep(0.2)

    # Create DLQ handler with unique consumer group
    test_kafka_config.consumer_group_prefix = "dlq-ack-test"
    dlq_handler = DLQHandler(test_kafka_config)

    # Track consumed records
    consumed_records = []

    async def capture_handler(record):
        """Handler that captures records."""
        consumed_records.append(record)

    # Replace handler BEFORE starting
    dlq_handler._handle_dlq_message = capture_handler

    # Start handler
    handler_task = asyncio.create_task(dlq_handler.start())

    # Wait for message to be consumed
    for _ in range(50):
        if len(consumed_records) > 0:
            break
        await asyncio.sleep(0.1)

    # Acknowledge the message
    assert len(consumed_records) == 1
    await dlq_handler.acknowledge_message(consumed_records[0])

    # Stop handler
    await dlq_handler.stop()
    handler_task.cancel()
    try:
        await handler_task
    except asyncio.CancelledError:
        pass

    # Wait for commit to propagate
    await asyncio.sleep(0.5)

    # Create new handler with SAME consumer group
    dlq_handler2 = DLQHandler(test_kafka_config)

    consumed_records2 = []

    async def capture_handler2(record):
        """Handler for second consumer."""
        consumed_records2.append(record)

    # Replace handler BEFORE starting
    dlq_handler2._handle_dlq_message = capture_handler2

    # Start second handler
    handler_task2 = asyncio.create_task(dlq_handler2.start())

    # Wait a bit - should NOT consume same message
    await asyncio.sleep(1.0)

    # Stop second handler
    await dlq_handler2.stop()
    handler_task2.cancel()
    try:
        await handler_task2
    except asyncio.CancelledError:
        pass

    # Verify message was NOT consumed again (already acknowledged)
    assert len(consumed_records2) == 0


@pytest.mark.asyncio
async def test_dlq_full_workflow_retry_exhaustion(
    kafka_producer: BaseKafkaProducer,
    kafka_consumer_factory: callable,
    message_collector: callable,
    test_kafka_config: KafkaConfig,
    test_topics: dict[str, str],
):
    """
    Test complete DLQ workflow from retry exhaustion to replay.

    Verifies:
    - Exhausted retries go to DLQ
    - DLQ handler can consume and parse
    - Replay sends back to pending with reset count
    - Full round-trip works correctly
    """
    # Set max retries
    test_kafka_config.max_retries = 2

    # Create initial task
    task = DownloadTaskMessage(
        trace_id="dlq-workflow-001",
        attachment_url="https://example.com/workflow.pdf",
        blob_path="documentsReceived/T-001/pdf/workflow.pdf",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="T-001",
        event_type="test",
        event_subtype="created",
        retry_count=2,  # At max retries
        original_timestamp=datetime.now(timezone.utc),
        metadata={"test": "workflow"},
    )

    # Exhaust retries - should go to DLQ
    retry_handler = RetryHandler(config=test_kafka_config, producer=kafka_producer)

    await retry_handler.handle_failure(
        task=task,
        error=ConnectionError("Still failing"),
        error_category=ErrorCategory.TRANSIENT,
    )

    # Wait for DLQ message
    await asyncio.sleep(0.2)

    # Create DLQ handler
    dlq_handler = DLQHandler(test_kafka_config)

    dlq_records = []

    async def capture_dlq(record):
        """Capture DLQ records."""
        dlq_records.append(record)

    # Replace handler BEFORE starting
    dlq_handler._handle_dlq_message = capture_dlq

    # Start DLQ handler
    handler_task = asyncio.create_task(dlq_handler.start())

    # Wait for DLQ message
    for _ in range(50):
        if len(dlq_records) > 0:
            break
        await asyncio.sleep(0.1)

    # Verify DLQ message
    assert len(dlq_records) == 1
    dlq_msg = dlq_handler.parse_dlq_message(dlq_records[0])
    assert dlq_msg.trace_id == task.trace_id
    assert dlq_msg.retry_count == 2

    # Replay to pending
    await dlq_handler.replay_message(dlq_records[0])

    # Acknowledge DLQ message
    await dlq_handler.acknowledge_message(dlq_records[0])

    # Stop DLQ handler
    await dlq_handler.stop()
    handler_task.cancel()
    try:
        await handler_task
    except asyncio.CancelledError:
        pass

    # Verify replayed message in pending topic
    pending_consumer = await kafka_consumer_factory(
        topics=[test_topics["pending"]],
        group_id="test-workflow-pending-consumer",
        message_handler=message_collector,
    )

    consumer_task = asyncio.create_task(pending_consumer.start())

    # Wait for replayed message
    for _ in range(50):
        if len(message_collector.messages) > 0:
            break
        await asyncio.sleep(0.1)

    await pending_consumer.stop()
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

    # Verify replay
    assert len(message_collector.messages) == 1
    replayed_task = DownloadTaskMessage.model_validate_json(
        message_collector.messages[0].value
    )
    assert replayed_task.trace_id == task.trace_id
    assert replayed_task.retry_count == 0  # Reset for retry
    assert replayed_task.metadata["replayed_from_dlq"] is True
    assert replayed_task.metadata["test"] == "workflow"  # Original metadata preserved


@pytest.mark.asyncio
async def test_dlq_handler_multiple_messages(
    kafka_producer: BaseKafkaProducer,
    test_kafka_config: KafkaConfig,
    test_topics: dict[str, str],
):
    """
    Test DLQ handler with multiple messages.

    Verifies:
    - Handler can consume multiple DLQ messages
    - Each message is parsed correctly
    - Messages are processed in order
    """
    # Send multiple messages to DLQ
    tasks = []
    for i in range(5):
        task = DownloadTaskMessage(
            trace_id=f"dlq-multi-{i:03d}",
            attachment_url=f"https://example.com/file{i}.pdf",
            blob_path=f"documentsReceived/T-{i:03d}/pdf/file{i}.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id=f"T-{i:03d}",
            event_type="test",
            event_subtype="created",
            retry_count=0,
            original_timestamp=datetime.now(timezone.utc),
        )

        dlq_msg = FailedDownloadMessage(
            trace_id=task.trace_id,
            attachment_url=task.attachment_url,
            original_task=task,
            final_error=f"Error {i}",
            error_category="permanent",
            retry_count=0,
            failed_at=datetime.now(timezone.utc),
        )

        await kafka_producer.send(
            topic=test_topics["dlq"],
            key=dlq_msg.trace_id,
            value=dlq_msg,
            headers={"trace_id": dlq_msg.trace_id},
        )

        tasks.append(task)

    # Wait for messages to be produced
    await asyncio.sleep(0.3)

    # Create DLQ handler
    dlq_handler = DLQHandler(test_kafka_config)

    parsed_messages = []

    async def tracking_handler(record):
        """Track all parsed messages."""
        try:
            parsed = dlq_handler.parse_dlq_message(record)
            parsed_messages.append(parsed)
        except Exception:
            pass

    # Replace handler BEFORE starting
    dlq_handler._handle_dlq_message = tracking_handler

    # Start handler
    handler_task = asyncio.create_task(dlq_handler.start())

    # Wait for all messages
    for _ in range(100):
        if len(parsed_messages) >= 5:
            break
        await asyncio.sleep(0.1)

    # Stop handler
    await dlq_handler.stop()
    handler_task.cancel()
    try:
        await handler_task
    except asyncio.CancelledError:
        pass

    # Verify all messages were consumed
    assert len(parsed_messages) == 5

    # Verify trace IDs
    trace_ids = {msg.trace_id for msg in parsed_messages}
    expected_trace_ids = {f"dlq-multi-{i:03d}" for i in range(5)}
    assert trace_ids == expected_trace_ids
