"""
End-to-end retry flow integration test.

Tests the complete retry flow from transient failure through retry topics
to eventual success or DLQ routing:
    1. DownloadWorker encounters transient failure
    2. RetryHandler routes to appropriate retry topic with exponential backoff
    3. DelayedRedeliveryScheduler redelivers after delay
    4. Successful download on retry OR retry exhaustion → DLQ

Validates:
- Message routing to retry topics with correct delays
- retry_count incrementation
- Error metadata preservation through retry chain
- Scheduler redelivery after delay elapsed
- Successful download on retry attempt
- Retry exhaustion routing to DLQ
- Permanent failure direct routing to DLQ
- Metrics: retry_count, error_category labels
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict
from unittest.mock import AsyncMock, patch

import pytest
from aiokafka import AIOKafkaProducer

from core.download.models import DownloadOutcome
from core.types import ErrorCategory
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.common.retry.scheduler import DelayedRedeliveryScheduler
from kafka_pipeline.xact.schemas.results import DownloadResultMessage, FailedDownloadMessage
from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage

from .fixtures.generators import create_download_task_message
from .helpers import (
    get_topic_messages,
    start_worker_background,
    stop_worker_gracefully,
    wait_for_condition,
)

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_transient_failure_routes_to_retry_topic(
    test_kafka_config: KafkaConfig,
    download_worker,
    mock_storage: Dict,
    tmp_path: Path,
):
    """
    Test transient download failure routes to first retry topic.

    Validates:
    - Transient error (timeout, 503) routes to retry topic
    - retry_count incremented from 0 to 1
    - Error metadata preserved in task
    - retry_at timestamp calculated correctly
    - Result message produced with failed_transient status
    """
    # Create test download task
    test_task = create_download_task_message(
        trace_id="retry-test-001",
        attachment_url="https://example.com/timeout-file.pdf",
        retry_count=0,
    )

    # Mock download to simulate transient timeout failure
    with patch("core.download.downloader.AttachmentDownloader.download") as mock_download:

        async def mock_transient_failure(task):
            # Simulate timeout error (transient)
            return DownloadOutcome.download_failure(
                error_message="Connection timeout after 60 seconds",
                error_category=ErrorCategory.TRANSIENT,
                status_code=None,
            )

        mock_download.side_effect = mock_transient_failure

        # Start download worker
        worker_task = await start_worker_background(download_worker)

        try:
            # Produce task to pending topic
            producer = AIOKafkaProducer(
                bootstrap_servers=test_kafka_config.bootstrap_servers,
                value_serializer=lambda v: v.model_dump_json().encode("utf-8"),
            )
            await producer.start()

            try:
                await producer.send(
                    test_kafka_config.downloads_pending_topic,
                    value=test_task,
                )
                logger.info(f"Produced task {test_task.trace_id} to pending topic")

            finally:
                await producer.stop()

            # Wait for processing and routing to retry topic
            await asyncio.sleep(3.0)

            # Verify message routed to first retry topic (5m delay)
            retry_topic = test_kafka_config.get_retry_topic(0)  # First retry topic
            retry_messages = await get_topic_messages(
                test_kafka_config,
                retry_topic,
                max_messages=10,
                timeout_seconds=5.0,
            )

            assert len(retry_messages) > 0, f"No messages in retry topic {retry_topic}"

            # Validate retry task message
            retry_task = DownloadTaskMessage.model_validate_json(retry_messages[0]["value"])
            assert retry_task.trace_id == test_task.trace_id
            assert retry_task.retry_count == 1, f"Expected retry_count=1, got {retry_task.retry_count}"

            # Validate error metadata preserved
            assert "last_error" in retry_task.metadata, "Error metadata missing"
            assert "Connection timeout" in retry_task.metadata["last_error"]
            assert retry_task.metadata["error_category"] == ErrorCategory.TRANSIENT.value

            # Validate retry_at timestamp exists and is in future
            assert "retry_at" in retry_task.metadata, "retry_at timestamp missing"
            retry_at_str = retry_task.metadata["retry_at"]
            retry_at = datetime.fromisoformat(retry_at_str)

            # Should be ~5 minutes in future (with tolerance for test execution time)
            expected_delay = test_kafka_config.retry_delays[0]  # 300 seconds (5 minutes)
            now = datetime.now(timezone.utc)
            time_diff = (retry_at - now).total_seconds()

            # Allow for test execution time: should be between 250-310 seconds in future
            assert 250 < time_diff < 310, f"retry_at timestamp incorrect: {time_diff}s in future"

            # Verify result message produced
            await asyncio.sleep(1.0)
            result_messages = await get_topic_messages(
                test_kafka_config,
                test_kafka_config.downloads_results_topic,
                max_messages=10,
                timeout_seconds=5.0,
            )

            assert len(result_messages) > 0, "No result messages produced"

            result_msg = DownloadResultMessage.model_validate_json(result_messages[0]["value"])
            assert result_msg.trace_id == test_task.trace_id
            assert result_msg.status == "failed_transient"
            assert result_msg.error_message is not None
            assert result_msg.error_category == ErrorCategory.TRANSIENT.value

        finally:
            await stop_worker_gracefully(download_worker, worker_task)


@pytest.mark.asyncio
async def test_scheduler_redelivers_after_delay(
    test_kafka_config: KafkaConfig,
    download_worker,
    mock_storage: Dict,
    tmp_path: Path,
):
    """
    Test DelayedRedeliveryScheduler redelivers message after delay elapsed.

    Validates:
    - Scheduler consumes from retry topics
    - Message not redelivered before delay elapsed
    - Message redelivered to pending topic after delay
    - Successful download on retry attempt

    Note: This test directly produces to the retry topic to isolate scheduler behavior.
    Uses longer delays to account for Kafka consumer polling intervals.
    """
    # Create test file for successful download
    test_file_path = tmp_path / "success-on-retry.pdf"
    test_file_content = b"PDF content for retry success test"
    test_file_path.write_bytes(test_file_content)

    # Create test task with retry metadata (simulating it came from retry handler)
    # Set retry_at to 3 seconds in future (accounting for Kafka polling delays)
    retry_at = datetime.now(timezone.utc) + timedelta(seconds=3)
    test_task = create_download_task_message(
        trace_id="retry-test-002",
        attachment_url="https://example.com/retry-success.pdf",
        retry_count=1,
        metadata={
            "last_error": "Connection timeout",
            "error_category": ErrorCategory.TRANSIENT.value,
            "retry_at": retry_at.isoformat(),
        },
    )

    # Mock download to succeed
    with patch("core.download.downloader.AttachmentDownloader.download") as mock_download:

        async def mock_download_success(task):
            return DownloadOutcome.success_outcome(
                file_path=test_file_path,
                bytes_downloaded=len(test_file_content),
                content_type="application/pdf",
                status_code=200,
            )

        mock_download.side_effect = mock_download_success

        # Start download worker
        worker_task = await start_worker_background(download_worker)

        # Create and start scheduler
        from kafka_pipeline.common.producer import BaseKafkaProducer

        producer = BaseKafkaProducer(test_kafka_config)
        await producer.start()

        scheduler = DelayedRedeliveryScheduler(
            config=test_kafka_config,
            producer=producer,
            lag_threshold=1000,
            check_interval_seconds=1,
        )

        scheduler_task = await start_worker_background(scheduler)

        try:
            # Produce task DIRECTLY to retry topic (bypassing download worker)
            # This ensures our retry_at timestamp is preserved
            retry_topic = test_kafka_config.get_retry_topic(0)

            kafka_producer = AIOKafkaProducer(
                bootstrap_servers=test_kafka_config.bootstrap_servers,
                value_serializer=lambda v: v.model_dump_json().encode("utf-8"),
            )
            await kafka_producer.start()

            try:
                await kafka_producer.send(
                    retry_topic,
                    value=test_task,
                )
                logger.info(f"Produced task {test_task.trace_id} to retry topic {retry_topic}")

            finally:
                await kafka_producer.stop()

            # Wait a bit (less than delay) - message should NOT be redelivered yet
            await asyncio.sleep(1.5)

            # Check pending topic - should be empty or have other messages, but not our task
            pending_messages = await get_topic_messages(
                test_kafka_config,
                test_kafka_config.downloads_pending_topic,
                max_messages=10,
                timeout_seconds=2.0,
            )

            # Our task should not be redelivered yet
            redelivered_early = any(
                DownloadTaskMessage.model_validate_json(msg["value"]).trace_id == test_task.trace_id
                for msg in pending_messages
            )
            assert not redelivered_early, "Task redelivered before delay elapsed"

            # Wait for delay to elapse plus extra time for scheduler to repoll and redeliver
            # Need to wait: remaining delay (~1.5s) + Kafka poll interval + scheduler processing
            await asyncio.sleep(7.0)

            # Now check that message was redelivered to pending topic
            pending_messages = await get_topic_messages(
                test_kafka_config,
                test_kafka_config.downloads_pending_topic,
                max_messages=10,
                timeout_seconds=5.0,
            )

            redelivered_tasks = [
                DownloadTaskMessage.model_validate_json(msg["value"])
                for msg in pending_messages
                if DownloadTaskMessage.model_validate_json(msg["value"]).trace_id == test_task.trace_id
            ]

            assert len(redelivered_tasks) > 0, "Task not redelivered to pending topic after delay"
            redelivered_task = redelivered_tasks[0]
            assert redelivered_task.retry_count == 1, "retry_count should be preserved"

            # Wait for successful download processing
            mock_onelake = mock_storage["onelake"]
            success = await wait_for_condition(
                lambda: mock_onelake.upload_count > 0,
                timeout_seconds=10.0,
                description="OneLake upload after retry",
            )
            assert success, "File not uploaded after successful retry"

            # Verify success result message
            await asyncio.sleep(2.0)
            result_messages = await get_topic_messages(
                test_kafka_config,
                test_kafka_config.downloads_results_topic,
                max_messages=10,
                timeout_seconds=5.0,
            )

            # Find our success result
            success_results = [
                DownloadResultMessage.model_validate_json(msg["value"])
                for msg in result_messages
                if DownloadResultMessage.model_validate_json(msg["value"]).trace_id == test_task.trace_id
                and DownloadResultMessage.model_validate_json(msg["value"]).status == "completed"
            ]

            assert len(success_results) > 0, "No success result message found"
            success_result = success_results[0]
            assert success_result.bytes_downloaded == len(test_file_content)

        finally:
            await stop_worker_gracefully(scheduler, scheduler_task)
            await stop_worker_gracefully(download_worker, worker_task)
            await producer.stop()


@pytest.mark.asyncio
async def test_retry_exhaustion_routes_to_dlq(
    test_kafka_config: KafkaConfig,
    download_worker,
    mock_storage: Dict,
    tmp_path: Path,
):
    """
    Test retry exhaustion after max retries routes to DLQ.

    Validates:
    - Task retries up to max_retries
    - After max retries exhausted, routes to DLQ
    - FailedDownloadMessage contains complete context
    - original_task preserved in DLQ message
    - Retry history preserved in metadata
    """
    # Create test task at max retry count
    max_retries = test_kafka_config.max_retries
    test_task = create_download_task_message(
        trace_id="retry-test-003",
        attachment_url="https://example.com/always-fails.pdf",
        retry_count=max_retries,  # Already at max retries
        metadata={
            "last_error": f"Connection timeout (attempt {max_retries})",
            "error_category": ErrorCategory.TRANSIENT.value,
        },
    )

    # Mock download to always fail with transient error
    with patch("core.download.downloader.AttachmentDownloader.download") as mock_download:

        async def mock_always_fails(task):
            return DownloadOutcome.download_failure(
                error_message="Connection timeout - persistent network issue",
                error_category=ErrorCategory.TRANSIENT,
                status_code=None,
            )

        mock_download.side_effect = mock_always_fails

        # Start download worker
        worker_task = await start_worker_background(download_worker)

        try:
            # Produce task to pending topic
            producer = AIOKafkaProducer(
                bootstrap_servers=test_kafka_config.bootstrap_servers,
                value_serializer=lambda v: v.model_dump_json().encode("utf-8"),
            )
            await producer.start()

            try:
                await producer.send(
                    test_kafka_config.downloads_pending_topic,
                    value=test_task,
                )
                logger.info(f"Produced task {test_task.trace_id} to pending (at max retries)")

            finally:
                await producer.stop()

            # Wait for processing and routing to DLQ
            await asyncio.sleep(3.0)

            # Verify message routed to DLQ (not another retry topic)
            dlq_messages = await get_topic_messages(
                test_kafka_config,
                test_kafka_config.dlq_topic,
                max_messages=10,
                timeout_seconds=5.0,
            )

            assert len(dlq_messages) > 0, "No messages in DLQ topic"

            # Validate DLQ message
            dlq_msg = FailedDownloadMessage.model_validate_json(dlq_messages[0]["value"])
            assert dlq_msg.trace_id == test_task.trace_id
            assert dlq_msg.retry_count == max_retries
            assert dlq_msg.error_category == ErrorCategory.TRANSIENT.value
            assert "Connection timeout" in dlq_msg.final_error

            # Validate original_task preserved
            assert dlq_msg.original_task is not None
            assert dlq_msg.original_task.trace_id == test_task.trace_id
            assert dlq_msg.original_task.attachment_url == test_task.attachment_url

            # Verify NO messages in further retry topics (exhausted)
            for i in range(max_retries):
                retry_topic = test_kafka_config.get_retry_topic(i)
                retry_msgs = await get_topic_messages(
                    test_kafka_config,
                    retry_topic,
                    max_messages=10,
                    timeout_seconds=2.0,
                )

                # Filter to our trace_id
                our_msgs = [
                    msg for msg in retry_msgs
                    if DownloadTaskMessage.model_validate_json(msg["value"]).trace_id == test_task.trace_id
                ]

                # Should not be in retry topics (went straight to DLQ)
                assert len(our_msgs) == 0, f"Task found in retry topic {retry_topic} after exhaustion"

        finally:
            await stop_worker_gracefully(download_worker, worker_task)


@pytest.mark.asyncio
async def test_permanent_failure_routes_directly_to_dlq(
    test_kafka_config: KafkaConfig,
    download_worker,
    mock_storage: Dict,
    tmp_path: Path,
):
    """
    Test permanent failure routes directly to DLQ without retry.

    Validates:
    - PERMANENT error category skips retry topics
    - Routes directly to DLQ
    - retry_count remains 0
    - FailedDownloadMessage has correct error_category
    """
    # Create test task
    test_task = create_download_task_message(
        trace_id="retry-test-004",
        attachment_url="https://example.com/not-found.pdf",
        retry_count=0,
    )

    # Mock download to fail with permanent error (404)
    with patch("core.download.downloader.AttachmentDownloader.download") as mock_download:

        async def mock_permanent_failure(task):
            return DownloadOutcome.download_failure(
                error_message="404 Not Found - Resource does not exist",
                error_category=ErrorCategory.PERMANENT,
                status_code=404,
            )

        mock_download.side_effect = mock_permanent_failure

        # Start download worker
        worker_task = await start_worker_background(download_worker)

        try:
            # Produce task to pending topic
            producer = AIOKafkaProducer(
                bootstrap_servers=test_kafka_config.bootstrap_servers,
                value_serializer=lambda v: v.model_dump_json().encode("utf-8"),
            )
            await producer.start()

            try:
                await producer.send(
                    test_kafka_config.downloads_pending_topic,
                    value=test_task,
                )
                logger.info(f"Produced task {test_task.trace_id} to pending (permanent failure)")

            finally:
                await producer.stop()

            # Wait for processing and routing to DLQ
            await asyncio.sleep(3.0)

            # Verify message routed directly to DLQ
            dlq_messages = await get_topic_messages(
                test_kafka_config,
                test_kafka_config.dlq_topic,
                max_messages=10,
                timeout_seconds=5.0,
            )

            assert len(dlq_messages) > 0, "No messages in DLQ topic"

            # Validate DLQ message
            dlq_msg = FailedDownloadMessage.model_validate_json(dlq_messages[0]["value"])
            assert dlq_msg.trace_id == test_task.trace_id
            assert dlq_msg.retry_count == 0, "Permanent failure should not increment retry_count"
            assert dlq_msg.error_category == ErrorCategory.PERMANENT.value
            assert "404 Not Found" in dlq_msg.final_error

            # Verify NO messages in any retry topics (skipped retry)
            for i in range(test_kafka_config.max_retries):
                retry_topic = test_kafka_config.get_retry_topic(i)
                retry_msgs = await get_topic_messages(
                    test_kafka_config,
                    retry_topic,
                    max_messages=10,
                    timeout_seconds=2.0,
                )

                # Filter to our trace_id
                our_msgs = [
                    msg for msg in retry_msgs
                    if DownloadTaskMessage.model_validate_json(msg["value"]).trace_id == test_task.trace_id
                ]

                assert len(our_msgs) == 0, f"Permanent failure found in retry topic {retry_topic}"

            # Verify result message has failed_permanent status
            await asyncio.sleep(1.0)
            result_messages = await get_topic_messages(
                test_kafka_config,
                test_kafka_config.downloads_results_topic,
                max_messages=10,
                timeout_seconds=5.0,
            )

            result_msgs = [
                DownloadResultMessage.model_validate_json(msg["value"])
                for msg in result_messages
                if DownloadResultMessage.model_validate_json(msg["value"]).trace_id == test_task.trace_id
            ]

            assert len(result_msgs) > 0, "No result message found"
            result_msg = result_msgs[0]
            assert result_msg.status == "failed_permanent"
            assert result_msg.error_category == ErrorCategory.PERMANENT.value

        finally:
            await stop_worker_gracefully(download_worker, worker_task)


@pytest.mark.asyncio
async def test_multiple_retries_then_success(
    test_kafka_config: KafkaConfig,
    download_worker,
    mock_storage: Dict,
    tmp_path: Path,
):
    """
    Test multiple retry attempts (1st fails, 2nd fails, 3rd succeeds).

    Validates:
    - First failure routes to retry.5m
    - Second failure routes to retry.10m
    - Third attempt succeeds
    - retry_count increments correctly through chain
    - Error metadata updated on each retry
    """
    # Create test file for eventual success
    test_file_path = tmp_path / "multi-retry-success.pdf"
    test_file_content = b"PDF content after multiple retries"
    test_file_path.write_bytes(test_file_content)

    # Mock download: fail twice, then succeed
    call_count = [0]

    with patch("core.download.downloader.AttachmentDownloader.download") as mock_download:

        async def mock_multi_retry(task):
            call_count[0] += 1

            if call_count[0] <= 2:
                # First two calls fail
                return DownloadOutcome.download_failure(
                    error_message=f"Connection timeout (attempt {call_count[0]})",
                    error_category=ErrorCategory.TRANSIENT,
                    status_code=None,
                )
            else:
                # Third call succeeds
                return DownloadOutcome.success_outcome(
                    file_path=test_file_path,
                    bytes_downloaded=len(test_file_content),
                    content_type="application/pdf",
                    status_code=200,
                )

        mock_download.side_effect = mock_multi_retry

        # Start download worker (we'll manually trigger retries instead of using scheduler)
        worker_task = await start_worker_background(download_worker)

        try:
            # Create and produce initial task
            test_task = create_download_task_message(
                trace_id="retry-test-005",
                attachment_url="https://example.com/multi-retry.pdf",
                retry_count=0,
            )

            producer = AIOKafkaProducer(
                bootstrap_servers=test_kafka_config.bootstrap_servers,
                value_serializer=lambda v: v.model_dump_json().encode("utf-8"),
            )
            await producer.start()

            try:
                # First attempt (retry_count=0)
                await producer.send(
                    test_kafka_config.downloads_pending_topic,
                    value=test_task,
                )
                logger.info(f"Produced task {test_task.trace_id} attempt 1 (retry_count=0)")

                await asyncio.sleep(2.0)

                # Verify routed to first retry topic
                retry_topic_1 = test_kafka_config.get_retry_topic(0)
                retry_msgs_1 = await get_topic_messages(
                    test_kafka_config,
                    retry_topic_1,
                    max_messages=10,
                    timeout_seconds=5.0,
                )

                assert len(retry_msgs_1) > 0, f"No message in {retry_topic_1}"
                retry_task_1 = DownloadTaskMessage.model_validate_json(retry_msgs_1[0]["value"])
                assert retry_task_1.retry_count == 1, "retry_count should be 1 after first failure"
                assert "attempt 1" in retry_task_1.metadata["last_error"]

                # Second attempt (simulate scheduler redelivery with retry_count=1)
                await producer.send(
                    test_kafka_config.downloads_pending_topic,
                    value=retry_task_1,
                )
                logger.info(f"Produced task {test_task.trace_id} attempt 2 (retry_count=1)")

                await asyncio.sleep(2.0)

                # Verify routed to second retry topic
                retry_topic_2 = test_kafka_config.get_retry_topic(1)
                retry_msgs_2 = await get_topic_messages(
                    test_kafka_config,
                    retry_topic_2,
                    max_messages=10,
                    timeout_seconds=5.0,
                )

                assert len(retry_msgs_2) > 0, f"No message in {retry_topic_2}"
                retry_task_2 = DownloadTaskMessage.model_validate_json(retry_msgs_2[0]["value"])
                assert retry_task_2.retry_count == 2, "retry_count should be 2 after second failure"
                assert "attempt 2" in retry_task_2.metadata["last_error"]

                # Third attempt (simulate scheduler redelivery with retry_count=2)
                await producer.send(
                    test_kafka_config.downloads_pending_topic,
                    value=retry_task_2,
                )
                logger.info(f"Produced task {test_task.trace_id} attempt 3 (retry_count=2)")

                # Wait for successful processing
                mock_onelake = mock_storage["onelake"]
                success = await wait_for_condition(
                    lambda: mock_onelake.upload_count > 0,
                    timeout_seconds=10.0,
                    description="OneLake upload after 3rd attempt",
                )
                assert success, "File not uploaded after third attempt"

                # Verify success result message
                await asyncio.sleep(2.0)
                result_messages = await get_topic_messages(
                    test_kafka_config,
                    test_kafka_config.downloads_results_topic,
                    max_messages=20,
                    timeout_seconds=5.0,
                )

                # Find success result (there will also be 2 failure results)
                success_results = [
                    DownloadResultMessage.model_validate_json(msg["value"])
                    for msg in result_messages
                    if DownloadResultMessage.model_validate_json(msg["value"]).trace_id == test_task.trace_id
                    and DownloadResultMessage.model_validate_json(msg["value"]).status == "completed"
                ]

                assert len(success_results) > 0, "No success result message found"
                success_result = success_results[0]
                assert success_result.bytes_downloaded == len(test_file_content)

            finally:
                await producer.stop()

        finally:
            await stop_worker_gracefully(download_worker, worker_task)
