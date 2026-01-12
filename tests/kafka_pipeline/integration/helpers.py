"""
Helper utilities for integration testing.

Provides utility functions for:
- Kafka topic inspection and message retrieval
- Async operation waiting with timeouts
- Worker lifecycle management
- Test data verification
"""

import asyncio
import logging
from typing import Any, Callable, Dict, List, Optional, TypeVar

from aiokafka import AIOKafkaConsumer, TopicPartition
from kafka_pipeline.config import KafkaConfig

logger = logging.getLogger(__name__)

T = TypeVar("T")


async def wait_for_condition(
    condition: Callable[[], bool],
    timeout_seconds: float = 5.0,
    poll_interval: float = 0.1,
    description: str = "condition",
) -> bool:
    """
    Wait for a condition to become true with timeout.

    Polls the condition function at regular intervals until it returns True
    or the timeout is reached.

    Args:
        condition: Callable that returns True when condition is met
        timeout_seconds: Maximum time to wait (default: 5.0)
        poll_interval: Time between condition checks (default: 0.1)
        description: Human-readable description for logging

    Returns:
        bool: True if condition met, False if timeout reached

    Example:
        >>> messages = []
        >>> await wait_for_condition(
        ...     lambda: len(messages) > 0,
        ...     timeout_seconds=10.0,
        ...     description="message received"
        ... )
    """
    start_time = asyncio.get_event_loop().time()
    end_time = start_time + timeout_seconds

    while asyncio.get_event_loop().time() < end_time:
        if condition():
            logger.debug(f"Condition met: {description}")
            return True
        await asyncio.sleep(poll_interval)

    logger.warning(
        f"Timeout waiting for {description}",
        extra={"timeout_seconds": timeout_seconds},
    )
    return False


async def wait_for_messages(
    messages_list: List,
    expected_count: int,
    timeout_seconds: float = 10.0,
    description: str = "messages",
) -> bool:
    """
    Wait for a specific number of messages to be collected.

    Convenience wrapper around wait_for_condition for message collection.

    Args:
        messages_list: List that accumulates messages
        expected_count: Number of messages to wait for
        timeout_seconds: Maximum time to wait (default: 10.0)
        description: Description for logging

    Returns:
        bool: True if messages received, False if timeout

    Example:
        >>> collector_messages = []
        >>> success = await wait_for_messages(
        ...     collector_messages,
        ...     expected_count=5,
        ...     timeout_seconds=10.0
        ... )
    """
    return await wait_for_condition(
        condition=lambda: len(messages_list) >= expected_count,
        timeout_seconds=timeout_seconds,
        description=f"{expected_count} {description}",
    )


async def get_topic_messages(
    config: KafkaConfig,
    topic: str,
    max_messages: int = 100,
    timeout_seconds: float = 5.0,
) -> List[Dict[str, Any]]:
    """
    Retrieve messages from a Kafka topic for inspection.

    Creates a temporary consumer to read messages from the beginning of the topic.
    Useful for verifying messages were produced correctly.

    Args:
        config: Kafka configuration
        topic: Topic name to read from
        max_messages: Maximum messages to retrieve (default: 100)
        timeout_seconds: Read timeout (default: 5.0)

    Returns:
        List[Dict]: List of message dictionaries with keys:
            - key: Message key (bytes)
            - value: Message value (bytes)
            - topic: Topic name
            - partition: Partition number
            - offset: Message offset
            - timestamp: Message timestamp
            - headers: Message headers

    Example:
        >>> messages = await get_topic_messages(
        ...     config,
        ...     topic="downloads.pending",
        ...     max_messages=10
        ... )
        >>> assert len(messages) > 0
    """
    consumer = AIOKafkaConsumer(
        bootstrap_servers=config.bootstrap_servers,
        security_protocol=config.security_protocol,
        sasl_mechanism=config.sasl_mechanism,
        group_id=f"test-inspector-{topic}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=int(timeout_seconds * 1000),
    )

    await consumer.start()
    messages = []

    try:
        # Subscribe and wait for assignment
        consumer.subscribe([topic])

        # Poll until we get messages or timeout
        start_time = asyncio.get_event_loop().time()
        end_time = start_time + timeout_seconds

        while len(messages) < max_messages:
            if asyncio.get_event_loop().time() >= end_time:
                break

            # Poll for messages
            records = await consumer.getmany(
                timeout_ms=1000,
                max_records=max_messages - len(messages)
            )

            # Extract messages from partitions
            for partition_records in records.values():
                for record in partition_records:
                    messages.append({
                        "key": record.key,
                        "value": record.value,
                        "topic": record.topic,
                        "partition": record.partition,
                        "offset": record.offset,
                        "timestamp": record.timestamp,
                        "headers": record.headers,
                    })

            if not records:
                # No more messages available
                break

    finally:
        await consumer.stop()

    logger.debug(
        f"Retrieved {len(messages)} messages from topic",
        extra={"topic": topic, "message_count": len(messages)},
    )

    return messages


async def get_topic_message_count(
    config: KafkaConfig,
    topic: str,
    timeout_seconds: float = 5.0,
) -> int:
    """
    Get the total number of messages in a topic.

    Reads the high watermark for all partitions to determine message count.

    Args:
        config: Kafka configuration
        topic: Topic name
        timeout_seconds: Timeout for operation (default: 5.0)

    Returns:
        int: Total number of messages across all partitions

    Example:
        >>> count = await get_topic_message_count(config, "downloads.pending")
        >>> assert count > 0
    """
    consumer = AIOKafkaConsumer(
        bootstrap_servers=config.bootstrap_servers,
        security_protocol=config.security_protocol,
        sasl_mechanism=config.sasl_mechanism,
        group_id=f"test-counter-{topic}",
        enable_auto_commit=False,
    )

    await consumer.start()
    total_count = 0

    try:
        # Get partitions for topic
        partitions = consumer.partitions_for_topic(topic)

        if partitions:
            # Get end offsets for all partitions
            topic_partitions = [TopicPartition(topic, p) for p in partitions]
            end_offsets = await consumer.end_offsets(topic_partitions)

            # Sum up messages across partitions
            for tp, end_offset in end_offsets.items():
                # End offset is the next offset to be written, so current count is end_offset
                total_count += end_offset

    finally:
        await consumer.stop()

    logger.debug(
        f"Topic message count",
        extra={"topic": topic, "message_count": total_count},
    )

    return total_count


async def drain_topic(
    config: KafkaConfig,
    topic: str,
    timeout_seconds: float = 2.0,
) -> int:
    """
    Drain all messages from a topic (read and discard).

    Useful for cleaning up between tests.

    Args:
        config: Kafka configuration
        topic: Topic name to drain
        timeout_seconds: Read timeout (default: 2.0)

    Returns:
        int: Number of messages drained

    Example:
        >>> drained = await drain_topic(config, "downloads.pending")
        >>> # Topic is now empty for next test
    """
    messages = await get_topic_messages(
        config,
        topic,
        max_messages=10000,
        timeout_seconds=timeout_seconds,
    )

    logger.debug(
        f"Drained topic",
        extra={"topic": topic, "drained_count": len(messages)},
    )

    return len(messages)


async def start_worker_background(worker: Any) -> asyncio.Task:
    """
    Start a worker in a background task.

    Creates an asyncio Task that runs the worker's start() method.
    Caller is responsible for stopping the worker and cleaning up the task.

    Args:
        worker: Worker instance with start() method

    Returns:
        asyncio.Task: Background task running the worker

    Example:
        >>> task = await start_worker_background(download_worker)
        >>> # Do some testing...
        >>> await download_worker.stop()
        >>> task.cancel()
        >>> try:
        ...     await task
        ... except asyncio.CancelledError:
        ...     pass
    """
    task = asyncio.create_task(worker.start())
    # Give worker time to fully start
    await asyncio.sleep(0.1)
    return task


async def stop_worker_gracefully(worker: Any, task: asyncio.Task) -> None:
    """
    Stop a worker and clean up its background task.

    Calls worker's stop() method and cancels the background task.

    Args:
        worker: Worker instance with stop() method
        task: Background task running the worker

    Example:
        >>> await stop_worker_gracefully(download_worker, worker_task)
    """
    # Stop worker
    await worker.stop()

    # Cancel and wait for task
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def assert_message_count(
    messages: List,
    expected: int,
    description: str = "messages",
) -> None:
    """
    Assert message count with helpful error message.

    Args:
        messages: List of messages
        expected: Expected count
        description: Description for error message

    Raises:
        AssertionError: If count doesn't match

    Example:
        >>> assert_message_count(collector.messages, 5, "download tasks")
    """
    actual = len(messages)
    assert actual == expected, (
        f"Expected {expected} {description}, got {actual}"
    )


def assert_message_field(
    message: Dict,
    field: str,
    expected_value: Any,
) -> None:
    """
    Assert a message field has expected value.

    Args:
        message: Message dictionary
        field: Field name
        expected_value: Expected value

    Raises:
        AssertionError: If field value doesn't match

    Example:
        >>> assert_message_field(msg, "status", "success")
    """
    actual_value = message.get(field)
    assert actual_value == expected_value, (
        f"Expected {field}={expected_value}, got {actual_value}"
    )
