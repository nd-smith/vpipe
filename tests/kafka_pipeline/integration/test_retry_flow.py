"""
Integration tests for Kafka retry flow.

Tests verify:
- RetryHandler routes failures to retry topics
- Retry count is incremented correctly
- DLQ routing after max retries
- Error context preservation
- Exponential backoff topic selection
"""

import asyncio
from datetime import datetime, timezone

import pytest

from core.types import ErrorCategory
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.common.consumer import BaseKafkaConsumer
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.common.retry.handler import RetryHandler
from kafka_pipeline.xact.schemas.results import FailedDownloadMessage
from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage


@pytest.mark.asyncio
async def test_transient_error_routes_to_retry_topic(
    kafka_producer: BaseKafkaProducer,
    kafka_consumer_factory: callable,
    message_collector: callable,
    test_kafka_config: KafkaConfig,
    test_topics: dict[str, str],
):
    """
    Test that transient errors route to retry topic.

    Verifies:
    - RetryHandler sends failed task to retry topic
    - Retry count is incremented
    - Error context is preserved in metadata
    - retry_at timestamp is added
    """
    # Create retry handler
    retry_handler = RetryHandler(config=test_kafka_config, producer=kafka_producer)

    # Create initial task
    task = DownloadTaskMessage(
        trace_id="test-retry-001",
        attachment_url="https://example.com/transient-fail.pdf",
        blob_path="documentsReceived/T-001/pdf/transient-fail.pdf",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="T-001",
        event_type="test_event",
        event_subtype="created",
        retry_count=0,
        original_timestamp=datetime.now(timezone.utc),
    )

    # Simulate transient error
    error = ConnectionError("Network timeout")

    # Handle failure
    await retry_handler.handle_failure(
        task=task,
        error=error,
        error_category=ErrorCategory.TRANSIENT,
    )

    # Consumer to check retry topic
    retry_topic = test_topics["retry_5m"]
    consumer = await kafka_consumer_factory(
        topics=[retry_topic],
        group_id="test-retry-consumer",
        message_handler=message_collector,
    )

    consumer_task = asyncio.create_task(consumer.start())

    # Wait for retry message
    for _ in range(50):
        if len(message_collector.messages) > 0:
            break
        await asyncio.sleep(0.1)

    await consumer.stop()
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

    # Verify retry message
    assert len(message_collector.messages) == 1
    record = message_collector.messages[0]

    # Verify headers
    headers_dict = {k: v.decode("utf-8") for k, v in record.headers}
    assert headers_dict["retry_count"] == "1"
    assert headers_dict["error_category"] == "transient"

    # Verify message content
    retry_task = DownloadTaskMessage.model_validate_json(record.value)
    assert retry_task.trace_id == task.trace_id
    assert retry_task.retry_count == 1
    assert "Network timeout" in retry_task.metadata["last_error"]
    assert retry_task.metadata["error_category"] == "transient"
    assert "retry_at" in retry_task.metadata


@pytest.mark.asyncio
async def test_permanent_error_routes_to_dlq(
    kafka_producer: BaseKafkaProducer,
    kafka_consumer_factory: callable,
    message_collector: callable,
    test_kafka_config: KafkaConfig,
    test_topics: dict[str, str],
):
    """
    Test that permanent errors route directly to DLQ.

    Verifies:
    - Permanent errors skip retry queue
    - DLQ message contains full context
    - Original task is preserved for replay
    """
    # Config already has test-specific DLQ topic
    retry_handler = RetryHandler(config=test_kafka_config, producer=kafka_producer)

    # Create task
    task = DownloadTaskMessage(
        trace_id="test-dlq-001",
        attachment_url="https://example.com/permanent-fail.pdf",
        blob_path="documentsReceived/T-001/pdf/permanent-fail.pdf",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="T-001",
        event_type="test_event",
        event_subtype="created",
        retry_count=0,
        original_timestamp=datetime.now(timezone.utc),
    )

    # Simulate permanent error
    error = ValueError("Invalid file type")

    # Handle failure
    await retry_handler.handle_failure(
        task=task,
        error=error,
        error_category=ErrorCategory.PERMANENT,
    )

    # Consumer for DLQ
    dlq_topic = test_topics["dlq"]
    consumer = await kafka_consumer_factory(
        topics=[dlq_topic],
        group_id="test-dlq-consumer",
        message_handler=message_collector,
    )

    consumer_task = asyncio.create_task(consumer.start())

    # Wait for DLQ message
    for _ in range(50):
        if len(message_collector.messages) > 0:
            break
        await asyncio.sleep(0.1)

    await consumer.stop()
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

    # Verify DLQ message
    assert len(message_collector.messages) == 1
    record = message_collector.messages[0]

    # Verify headers
    headers_dict = {k: v.decode("utf-8") for k, v in record.headers}
    assert headers_dict["error_category"] == "permanent"
    assert headers_dict["failed"] == "true"

    # Verify DLQ message structure
    dlq_message = FailedDownloadMessage.model_validate_json(record.value)
    assert dlq_message.trace_id == task.trace_id
    assert dlq_message.attachment_url == task.attachment_url
    assert "Invalid file type" in dlq_message.final_error
    assert dlq_message.error_category == "permanent"
    assert dlq_message.retry_count == 0

    # Verify original task is preserved
    assert dlq_message.original_task.trace_id == task.trace_id
    assert dlq_message.original_task.attachment_url == task.attachment_url


@pytest.mark.asyncio
async def test_exhausted_retries_route_to_dlq(
    kafka_producer: BaseKafkaProducer,
    kafka_consumer_factory: callable,
    message_collector: callable,
    test_kafka_config: KafkaConfig,
    test_topics: dict[str, str],
):
    """
    Test that exhausted retries route to DLQ.

    Verifies:
    - After max retries, task goes to DLQ
    - DLQ message reflects retry exhaustion
    """
    # Config already has test-specific DLQ topic, just set max retries
    test_kafka_config.max_retries = 4

    retry_handler = RetryHandler(config=test_kafka_config, producer=kafka_producer)

    # Create task that has already been retried max times
    task = DownloadTaskMessage(
        trace_id="test-exhausted-001",
        attachment_url="https://example.com/exhausted.pdf",
        blob_path="documentsReceived/T-001/pdf/exhausted.pdf",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="T-001",
        event_type="test_event",
        event_subtype="created",
        retry_count=4,  # Already at max
        original_timestamp=datetime.now(timezone.utc),
    )

    # Simulate another transient error
    error = ConnectionError("Still failing after retries")

    # Handle failure (should go to DLQ)
    await retry_handler.handle_failure(
        task=task,
        error=error,
        error_category=ErrorCategory.TRANSIENT,
    )

    # Consumer for DLQ
    dlq_topic = test_topics["dlq"]
    consumer = await kafka_consumer_factory(
        topics=[dlq_topic],
        group_id="test-exhausted-dlq-consumer",
        message_handler=message_collector,
    )

    consumer_task = asyncio.create_task(consumer.start())

    # Wait for DLQ message
    for _ in range(50):
        if len(message_collector.messages) > 0:
            break
        await asyncio.sleep(0.1)

    await consumer.stop()
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

    # Verify DLQ message
    assert len(message_collector.messages) == 1
    record = message_collector.messages[0]

    dlq_message = FailedDownloadMessage.model_validate_json(record.value)
    assert dlq_message.retry_count == 4
    assert "Still failing after retries" in dlq_message.final_error


@pytest.mark.asyncio
async def test_retry_progression_through_backoff_topics(
    kafka_producer: BaseKafkaProducer,
    kafka_consumer_factory: callable,
    test_kafka_config: KafkaConfig,
    test_topics: dict[str, str],
):
    """
    Test retry progression through exponential backoff topics.

    Verifies:
    - First retry goes to 5m topic
    - Second retry goes to 10m topic
    - Third retry goes to 20m topic
    - Fourth retry goes to 40m topic
    - Fifth attempt goes to DLQ
    """
    # Config already has test-specific topics, just set max retries
    test_kafka_config.max_retries = 4

    retry_handler = RetryHandler(config=test_kafka_config, producer=kafka_producer)

    # Track which topics messages appear in
    topic_sequence = []

    async def topic_recorder(record):
        topic_sequence.append(record.topic)

    # Initial task
    task = DownloadTaskMessage(
        trace_id="test-progression-001",
        attachment_url="https://example.com/progression.pdf",
        blob_path="documentsReceived/T-001/pdf/progression.pdf",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="T-001",
        event_type="test_event",
        event_subtype="created",
        retry_count=0,
        original_timestamp=datetime.now(timezone.utc),
    )

    error = ConnectionError("Persistent network issue")

    # Simulate retry progression
    for attempt in range(5):
        # Handle failure at current retry count
        await retry_handler.handle_failure(
            task=task,
            error=error,
            error_category=ErrorCategory.TRANSIENT,
        )

        # Determine expected topic
        if attempt < 4:
            # Should go to retry topic
            expected_topic = test_kafka_config.get_retry_topic(attempt)

            # Consume from retry topic
            consumer = await kafka_consumer_factory(
                topics=[expected_topic],
                group_id=f"test-progression-consumer-{attempt}",
                message_handler=topic_recorder,
            )

            consumer_task = asyncio.create_task(consumer.start())

            # Wait for message
            messages_before = len(topic_sequence)
            for _ in range(50):
                if len(topic_sequence) > messages_before:
                    break
                await asyncio.sleep(0.1)

            await consumer.stop()
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass

            # Get updated task from retry topic for next iteration
            # (In real system, this would be consumed and reprocessed)
            task = task.model_copy(deep=True)
            task.retry_count += 1

        else:
            # Should go to DLQ
            dlq_topic = test_topics["dlq"]

            consumer = await kafka_consumer_factory(
                topics=[dlq_topic],
                group_id="test-progression-dlq-consumer",
                message_handler=topic_recorder,
            )

            consumer_task = asyncio.create_task(consumer.start())

            # Wait for DLQ message
            messages_before = len(topic_sequence)
            for _ in range(50):
                if len(topic_sequence) > messages_before:
                    break
                await asyncio.sleep(0.1)

            await consumer.stop()
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass

    # Verify topic progression
    assert len(topic_sequence) == 5

    # Verify retry topics in correct order
    assert test_topics["retry_5m"] in topic_sequence
    assert test_topics["retry_10m"] in topic_sequence
    assert test_topics["retry_20m"] in topic_sequence
    assert test_topics["retry_40m"] in topic_sequence
    # Final message should be DLQ
    assert topic_sequence[-1] == test_topics["dlq"]


@pytest.mark.asyncio
async def test_error_metadata_preserved_through_retries(
    kafka_producer: BaseKafkaProducer,
    kafka_consumer_factory: callable,
    message_collector: callable,
    test_kafka_config: KafkaConfig,
    test_topics: dict[str, str],
):
    """
    Test that error context is preserved through retry chain.

    Verifies:
    - Each retry preserves error information
    - Metadata accumulates context
    - Original timestamp is preserved
    """
    retry_handler = RetryHandler(config=test_kafka_config, producer=kafka_producer)

    # Initial task
    original_timestamp = datetime.now(timezone.utc)
    task = DownloadTaskMessage(
        trace_id="test-metadata-001",
        attachment_url="https://example.com/metadata.pdf",
        blob_path="documentsReceived/T-001/pdf/metadata.pdf",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="T-001",
        event_type="test_event",
        event_subtype="created",
        retry_count=0,
        original_timestamp=original_timestamp,
        metadata={"initial": "value"},
    )

    # First failure
    error1 = ConnectionError("First error")
    await retry_handler.handle_failure(
        task=task,
        error=error1,
        error_category=ErrorCategory.TRANSIENT,
    )

    # Consume from first retry topic
    retry_topic_1 = test_kafka_config.get_retry_topic(0)
    consumer1 = await kafka_consumer_factory(
        topics=[retry_topic_1],
        group_id="test-metadata-consumer-1",
        message_handler=message_collector,
    )

    consumer_task1 = asyncio.create_task(consumer1.start())

    for _ in range(50):
        if len(message_collector.messages) > 0:
            break
        await asyncio.sleep(0.1)

    await consumer1.stop()
    consumer_task1.cancel()
    try:
        await consumer_task1
    except asyncio.CancelledError:
        pass

    # Verify first retry
    assert len(message_collector.messages) == 1
    retry_task_1 = DownloadTaskMessage.model_validate_json(
        message_collector.messages[0].value
    )

    assert retry_task_1.retry_count == 1
    assert "First error" in retry_task_1.metadata["last_error"]
    assert retry_task_1.metadata["error_category"] == "transient"
    assert retry_task_1.metadata["initial"] == "value"  # Original metadata preserved
    assert retry_task_1.original_timestamp == original_timestamp  # Preserved

    # Second failure
    message_collector.messages.clear()
    error2 = ConnectionError("Second error")
    await retry_handler.handle_failure(
        task=retry_task_1,
        error=error2,
        error_category=ErrorCategory.TRANSIENT,
    )

    # Consume from second retry topic
    retry_topic_2 = test_kafka_config.get_retry_topic(1)
    consumer2 = await kafka_consumer_factory(
        topics=[retry_topic_2],
        group_id="test-metadata-consumer-2",
        message_handler=message_collector,
    )

    consumer_task2 = asyncio.create_task(consumer2.start())

    for _ in range(50):
        if len(message_collector.messages) > 0:
            break
        await asyncio.sleep(0.1)

    await consumer2.stop()
    consumer_task2.cancel()
    try:
        await consumer_task2
    except asyncio.CancelledError:
        pass

    # Verify second retry
    assert len(message_collector.messages) == 1
    retry_task_2 = DownloadTaskMessage.model_validate_json(
        message_collector.messages[0].value
    )

    assert retry_task_2.retry_count == 2
    assert "Second error" in retry_task_2.metadata["last_error"]
    assert retry_task_2.metadata["initial"] == "value"  # Still preserved
    assert retry_task_2.original_timestamp == original_timestamp  # Still preserved
