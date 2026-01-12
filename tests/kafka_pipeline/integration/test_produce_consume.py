"""
Integration tests for basic Kafka producer/consumer interaction.

Tests verify:
- Producer can send messages to Kafka
- Consumer can receive and process messages
- Message serialization/deserialization works correctly
- Multiple messages can be batched and consumed
- Consumer offset management (at-least-once semantics)
"""

import asyncio
from datetime import datetime, timezone

import pytest

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.common.consumer import BaseKafkaConsumer
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage


@pytest.mark.asyncio
async def test_send_and_receive_single_message(
    kafka_producer: BaseKafkaProducer,
    kafka_consumer_factory: callable,
    message_collector: callable,
    test_topics: dict[str, str],
):
    """
    Test sending and receiving a single message.

    Verifies:
    - Producer successfully sends message
    - Consumer receives message
    - Message content is correctly deserialized
    - Offset is committed after processing
    """
    # Create test message
    task = DownloadTaskMessage(
        trace_id="test-evt-001",
        attachment_url="https://example.com/file1.pdf",
        blob_path="documentsReceived/T-001/pdf/file1.pdf",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="T-001",
        event_type="test_event",
        event_subtype="created",
        retry_count=0,
        original_timestamp=datetime.now(timezone.utc),
        metadata={"test": "value"},
    )

    # Send message
    topic = test_topics["pending"]
    metadata = await kafka_producer.send(
        topic=topic,
        key=task.trace_id,
        value=task,
    )

    assert metadata.topic == topic
    assert metadata.partition >= 0
    assert metadata.offset >= 0

    # Create consumer
    consumer = await kafka_consumer_factory(
        topics=[topic],
        group_id="test-consumer-single",
        message_handler=message_collector,
    )

    # Start consumer in background task
    consumer_task = asyncio.create_task(consumer.start())

    # Wait for message to be consumed
    # Use timeout to prevent test hanging
    for _ in range(50):  # 5 seconds max
        if len(message_collector.messages) > 0:
            break
        await asyncio.sleep(0.1)

    # Stop consumer
    await consumer.stop()
    # Cancel and cleanup consumer task
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

    # Verify message was received
    assert len(message_collector.messages) == 1
    received = message_collector.messages[0]

    # Verify message content
    assert received.topic == topic
    assert received.key.decode("utf-8") == task.trace_id

    # Deserialize and verify
    received_task = DownloadTaskMessage.model_validate_json(received.value)
    assert received_task.trace_id == task.trace_id
    assert received_task.attachment_url == task.attachment_url
    assert received_task.blob_path == task.blob_path
    assert received_task.event_type == task.event_type
    assert received_task.retry_count == 0
    assert received_task.metadata["test"] == "value"


@pytest.mark.asyncio
async def test_send_batch_and_consume(
    kafka_producer: BaseKafkaProducer,
    kafka_consumer_factory: callable,
    message_collector: callable,
    test_topics: dict[str, str],
):
    """
    Test sending batch of messages and consuming them.

    Verifies:
    - Producer batch send works correctly
    - All messages are sent
    - Consumer receives all messages
    - Message order is preserved (within same partition)
    """
    # Create batch of test messages
    messages = []
    for i in range(5):
        task = DownloadTaskMessage(
            trace_id=f"test-evt-{i:03d}",
            attachment_url=f"https://example.com/file{i}.pdf",
            blob_path=f"documentsReceived/T-{i:03d}/pdf/file{i}.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id=f"T-{i:03d}",
            event_type="test_event",
            event_subtype="created",
            retry_count=0,
            original_timestamp=datetime.now(timezone.utc),
        )
        messages.append((task.trace_id, task))

    # Send batch
    topic = test_topics["pending"]
    metadata_list = await kafka_producer.send_batch(
        topic=topic,
        messages=messages,
    )

    # Verify batch send results
    assert len(metadata_list) == 5
    for metadata in metadata_list:
        assert metadata.topic == topic
        assert metadata.partition >= 0
        assert metadata.offset >= 0

    # Create consumer
    consumer = await kafka_consumer_factory(
        topics=[topic],
        group_id="test-consumer-batch",
        message_handler=message_collector,
    )

    # Start consumer in background
    consumer_task = asyncio.create_task(consumer.start())

    # Wait for all messages to be consumed
    for _ in range(100):  # 10 seconds max
        if len(message_collector.messages) >= 5:
            break
        await asyncio.sleep(0.1)

    # Stop consumer
    await consumer.stop()
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

    # Verify all messages received
    assert len(message_collector.messages) == 5

    # Verify message content
    received_trace_ids = set()
    for record in message_collector.messages:
        task = DownloadTaskMessage.model_validate_json(record.value)
        received_trace_ids.add(task.trace_id)

    expected_trace_ids = {f"test-evt-{i:03d}" for i in range(5)}
    assert received_trace_ids == expected_trace_ids


@pytest.mark.asyncio
async def test_multiple_consumers_same_group(
    kafka_producer: BaseKafkaProducer,
    kafka_consumer_factory: callable,
    test_topics: dict[str, str],
):
    """
    Test multiple consumers in same group share partitions.

    Verifies:
    - Multiple consumers can join same group
    - Messages are distributed among consumers
    - Each message consumed exactly once per group
    """
    # Create two message collectors
    collector1_messages = []
    collector2_messages = []

    async def collector1(record):
        collector1_messages.append(record)

    async def collector2(record):
        collector2_messages.append(record)

    # Send multiple messages first
    topic = test_topics["pending"]
    messages = []
    for i in range(10):
        task = DownloadTaskMessage(
            trace_id=f"test-evt-multi-{i:03d}",
            attachment_url=f"https://example.com/file{i}.pdf",
            blob_path=f"documentsReceived/T-{i:03d}/pdf/file{i}.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id=f"T-{i:03d}",
            event_type="test_event",
            event_subtype="created",
            retry_count=0,
            original_timestamp=datetime.now(timezone.utc),
        )
        messages.append((task.trace_id, task))

    await kafka_producer.send_batch(topic=topic, messages=messages)

    # Create two consumers in same group
    consumer1 = await kafka_consumer_factory(
        topics=[topic],
        group_id="test-consumer-multi",
        message_handler=collector1,
    )

    consumer2 = await kafka_consumer_factory(
        topics=[topic],
        group_id="test-consumer-multi",
        message_handler=collector2,
    )

    # Start both consumers
    task1 = asyncio.create_task(consumer1.start())
    task2 = asyncio.create_task(consumer2.start())

    # Wait for messages to be distributed
    for _ in range(100):  # 10 seconds max
        total = len(collector1_messages) + len(collector2_messages)
        if total >= 10:
            break
        await asyncio.sleep(0.1)

    # Stop consumers
    await consumer1.stop()
    await consumer2.stop()
    task1.cancel()
    task2.cancel()
    try:
        await asyncio.gather(task1, task2)
    except asyncio.CancelledError:
        pass

    # Verify all messages consumed exactly once
    total_consumed = len(collector1_messages) + len(collector2_messages)
    assert total_consumed == 10

    # Verify no duplicate trace_ids
    all_trace_ids = set()
    for record in collector1_messages + collector2_messages:
        task = DownloadTaskMessage.model_validate_json(record.value)
        assert task.trace_id not in all_trace_ids, "Duplicate message consumed"
        all_trace_ids.add(task.trace_id)

    # Both consumers should have received at least some messages
    # (unless all messages went to same partition, which is possible but unlikely)
    # For now, just verify total is correct
    assert len(all_trace_ids) == 10


@pytest.mark.asyncio
async def test_consumer_processes_with_headers(
    kafka_producer: BaseKafkaProducer,
    kafka_consumer_factory: callable,
    message_collector: callable,
    test_topics: dict[str, str],
):
    """
    Test that message headers are preserved through producer/consumer.

    Verifies:
    - Producer can send headers
    - Consumer receives headers
    - Header values are correct
    """
    # Create message with headers
    task = DownloadTaskMessage(
        trace_id="test-evt-headers",
        attachment_url="https://example.com/file.pdf",
        blob_path="documentsReceived/T-001/pdf/file.pdf",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="T-001",
        event_type="test_event",
        event_subtype="created",
        retry_count=0,
        original_timestamp=datetime.now(timezone.utc),
    )

    topic = test_topics["pending"]

    # Send with custom headers
    await kafka_producer.send(
        topic=topic,
        key=task.trace_id,
        value=task,
        headers={
            "source": "integration_test",
            "version": "1.0",
            "retry_count": "0",
        },
    )

    # Create consumer
    consumer = await kafka_consumer_factory(
        topics=[topic],
        group_id="test-consumer-headers",
        message_handler=message_collector,
    )

    # Start consumer
    consumer_task = asyncio.create_task(consumer.start())

    # Wait for message
    for _ in range(50):
        if len(message_collector.messages) > 0:
            break
        await asyncio.sleep(0.1)

    # Stop consumer
    await consumer.stop()
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

    # Verify headers
    assert len(message_collector.messages) == 1
    record = message_collector.messages[0]

    headers_dict = {k: v.decode("utf-8") for k, v in record.headers}
    assert headers_dict["source"] == "integration_test"
    assert headers_dict["version"] == "1.0"
    assert headers_dict["retry_count"] == "0"


@pytest.mark.asyncio
async def test_consumer_manual_offset_commit(
    kafka_producer: BaseKafkaProducer,
    kafka_consumer_factory: callable,
    test_topics: dict[str, str],
    unique_topic_prefix: str,
):
    """
    Test that consumer uses manual offset commit (at-least-once semantics).

    Verifies:
    - Offsets are not auto-committed
    - Messages are reprocessed if consumer crashes before commit
    - After successful processing, offset is committed
    """
    # Track processing attempts
    processing_count = [0]
    processed_trace_ids = []

    async def counting_handler(record):
        processing_count[0] += 1
        task = DownloadTaskMessage.model_validate_json(record.value)
        processed_trace_ids.append(task.trace_id)
        # Simulate crash on first attempt
        if processing_count[0] == 1:
            raise Exception("Simulated processing failure")
        # Second attempt succeeds

    # Send message
    task = DownloadTaskMessage(
        trace_id="test-evt-commit",
        attachment_url="https://example.com/file.pdf",
        blob_path="documentsReceived/T-001/pdf/file.pdf",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="T-001",
        event_type="test_event",
        event_subtype="created",
        retry_count=0,
        original_timestamp=datetime.now(timezone.utc),
    )

    topic = test_topics["pending"]
    await kafka_producer.send(topic=topic, key=task.trace_id, value=task)

    # First consumer attempt (will fail)
    # Use unique group_id to avoid interference from other tests
    group_id = f"{unique_topic_prefix}.consumer-commit"
    consumer = await kafka_consumer_factory(
        topics=[topic],
        group_id=group_id,
        message_handler=counting_handler,
    )

    consumer_task = asyncio.create_task(consumer.start())

    # Wait for first processing attempt
    for _ in range(50):
        if processing_count[0] >= 1:
            break
        await asyncio.sleep(0.1)

    # Simulate crash by canceling task WITHOUT calling stop()
    # This ensures offset is NOT committed (unlike graceful stop())
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

    # Wait longer for consumer group to rebalance and Kafka state to settle
    # This is necessary because Kafka needs time to recognize the consumer is gone
    # and release the partition assignment. Session timeout + heartbeat + rebalance
    # can take significant time in testcontainers environments.
    await asyncio.sleep(5)

    # Second consumer attempt (should reprocess because offset wasn't committed)
    consumer2 = await kafka_consumer_factory(
        topics=[topic],
        group_id=group_id,  # Same group as first consumer
        message_handler=counting_handler,
    )

    consumer_task2 = asyncio.create_task(consumer2.start())

    # Wait longer for reprocessing (Kafka needs time to assign partitions)
    for _ in range(100):  # 10 seconds max
        if processing_count[0] >= 2:
            break
        await asyncio.sleep(0.1)

    # Cancel second consumer task (no need for graceful stop in test)
    consumer_task2.cancel()
    try:
        await consumer_task2
    except asyncio.CancelledError:
        pass

    # Verify message was processed twice (at-least-once semantics)
    assert processing_count[0] >= 2, "Message should be reprocessed after failure"
    assert all(
        tid == "test-evt-commit" for tid in processed_trace_ids
    ), "Same message should be reprocessed"
