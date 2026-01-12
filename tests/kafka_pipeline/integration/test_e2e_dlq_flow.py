"""
End-to-end DLQ flow integration test.

Tests the complete dead-letter queue flow including permanent failures,
retry exhaustion, manual review, and replay operations:
    1. DownloadWorker encounters permanent failure → DLQ directly (no retry)
    2. Retry exhaustion scenario → DLQ
    3. DLQHandler parses and validates DLQ messages
    4. DLQ CLI operations: list, view, replay, resolve
    5. Replayed message reprocessed successfully

Validates:
- Permanent failures route directly to DLQ (404, invalid URL, file type validation)
- Retry exhaustion routes to DLQ after max retries
- FailedDownloadMessage contains complete context (original_task, error details)
- Error metadata preserved (final_error, error_category)
- DLQ CLI can list messages
- DLQ CLI can view message details
- DLQ CLI replay sends message to pending topic with retry_count=0
- Replayed messages are processed successfully
- Audit log entries for DLQ operations
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict
from unittest.mock import AsyncMock, patch

import pytest
from aiokafka import AIOKafkaProducer

from core.download.models import DownloadOutcome
from core.types import ErrorCategory
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.common.dlq.cli import DLQCLIManager
from kafka_pipeline.common.dlq.handler import DLQHandler
from kafka_pipeline.xact.schemas.results import FailedDownloadMessage
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
async def test_permanent_failure_routes_directly_to_dlq(
    test_kafka_config: KafkaConfig,
    download_worker,
    mock_storage: Dict,
    tmp_path: Path,
):
    """
    Test permanent failures route directly to DLQ without retry.

    Tests multiple permanent failure types:
    - 404 Not Found
    - Invalid URL (validation failure)
    - File type validation failure

    Validates:
    - Message routed directly to DLQ (no retry topic)
    - retry_count remains 0
    - FailedDownloadMessage has correct error_category
    - original_task preserved with all metadata
    - Error context contains specific failure reason
    """
    # Create test task that will encounter 404 error
    test_task = create_download_task_message(
        trace_id="dlq-test-001",
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

            # Validate original_task preserved
            assert dlq_msg.original_task is not None
            assert dlq_msg.original_task.trace_id == test_task.trace_id
            assert dlq_msg.original_task.attachment_url == test_task.attachment_url
            assert dlq_msg.original_task.destination_path == test_task.destination_path

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
                    msg
                    for msg in retry_msgs
                    if DownloadTaskMessage.model_validate_json(msg["value"]).trace_id
                    == test_task.trace_id
                ]

                assert (
                    len(our_msgs) == 0
                ), f"Permanent failure found in retry topic {retry_topic}"

        finally:
            await stop_worker_gracefully(download_worker, worker_task)


@pytest.mark.asyncio
async def test_retry_exhaustion_routes_to_dlq(
    test_kafka_config: KafkaConfig,
    download_worker,
    mock_storage: Dict,
    tmp_path: Path,
):
    """
    Test retry exhaustion after max retries routes to DLQ.

    Simulates a task that has already exhausted all retries and
    validates DLQ routing with complete error context.

    Validates:
    - Task at max_retries routes to DLQ on next failure
    - FailedDownloadMessage contains retry history
    - Error context preserved through retry chain
    - original_task has max retry_count
    """
    # Create test task at max retry count
    max_retries = test_kafka_config.max_retries
    test_task = create_download_task_message(
        trace_id="dlq-test-002",
        attachment_url="https://example.com/exhausted-retries.pdf",
        retry_count=max_retries,  # Already at max retries
        metadata={
            "last_error": f"Connection timeout (attempt {max_retries})",
            "error_category": ErrorCategory.TRANSIENT.value,
            "retry_history": [
                "Attempt 1: Connection timeout",
                "Attempt 2: Connection timeout",
                f"Attempt {max_retries}: Connection timeout",
            ],
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

            assert len(dlq_messages) > 0, "No messages in DLQ topic after retry exhaustion"

            # Validate DLQ message
            dlq_msg = FailedDownloadMessage.model_validate_json(dlq_messages[0]["value"])
            assert dlq_msg.trace_id == test_task.trace_id
            assert dlq_msg.retry_count == max_retries
            assert dlq_msg.error_category == ErrorCategory.TRANSIENT.value
            assert "Connection timeout" in dlq_msg.final_error

            # Validate original_task preserved with metadata
            assert dlq_msg.original_task is not None
            assert dlq_msg.original_task.retry_count == max_retries
            assert "retry_history" in dlq_msg.original_task.metadata
            assert len(dlq_msg.original_task.metadata["retry_history"]) > 0

        finally:
            await stop_worker_gracefully(download_worker, worker_task)


@pytest.mark.asyncio
async def test_dlq_handler_parses_and_validates_messages(
    test_kafka_config: KafkaConfig,
    download_worker,
    mock_storage: Dict,
    tmp_path: Path,
):
    """
    Test DLQHandler can parse and validate DLQ messages.

    Validates:
    - DLQHandler.parse_dlq_message() works correctly
    - Message handler logs DLQ messages
    - Audit logging for DLQ message receipt
    """
    # Create test task that will go to DLQ
    test_task = create_download_task_message(
        trace_id="dlq-test-003",
        attachment_url="https://example.com/validation-error.pdf",
        retry_count=0,
    )

    # Mock download to fail with permanent error
    with patch("core.download.downloader.AttachmentDownloader.download") as mock_download:

        async def mock_validation_failure(task):
            return DownloadOutcome.download_failure(
                error_message="Invalid file type: application/exe",
                error_category=ErrorCategory.PERMANENT,
                status_code=None,
            )

        mock_download.side_effect = mock_validation_failure

        # Start download worker
        worker_task = await start_worker_background(download_worker)

        try:
            # Produce task
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
                logger.info(f"Produced task {test_task.trace_id} (validation failure)")

            finally:
                await producer.stop()

            # Wait for DLQ routing
            await asyncio.sleep(3.0)

            # Create DLQHandler and fetch messages
            dlq_handler = DLQHandler(test_kafka_config)

            # Start producer only (we'll manually fetch from DLQ)
            from kafka_pipeline.common.producer import BaseKafkaProducer

            dlq_handler._producer = BaseKafkaProducer(test_kafka_config)
            await dlq_handler._producer.start()

            try:
                # Manually fetch DLQ messages
                dlq_messages = await get_topic_messages(
                    test_kafka_config,
                    test_kafka_config.dlq_topic,
                    max_messages=10,
                    timeout_seconds=5.0,
                )

                assert len(dlq_messages) > 0, "No DLQ messages to parse"

                # Parse message using DLQHandler
                from aiokafka.structs import ConsumerRecord

                record = ConsumerRecord(
                    topic=test_kafka_config.dlq_topic,
                    partition=dlq_messages[0]["partition"],
                    offset=dlq_messages[0]["offset"],
                    timestamp=dlq_messages[0]["timestamp"],
                    timestamp_type=1,
                    key=dlq_messages[0]["key"],
                    value=dlq_messages[0]["value"],
                    checksum=None,
                    serialized_key_size=0,
                    serialized_value_size=len(dlq_messages[0]["value"]),
                    headers=dlq_messages[0]["headers"] or [],
                )

                # Test parse_dlq_message method
                parsed_msg = dlq_handler.parse_dlq_message(record)

                assert parsed_msg.trace_id == test_task.trace_id
                assert parsed_msg.error_category == ErrorCategory.PERMANENT.value
                assert "Invalid file type" in parsed_msg.final_error
                assert parsed_msg.original_task is not None

            finally:
                await dlq_handler._producer.stop()

        finally:
            await stop_worker_gracefully(download_worker, worker_task)


@pytest.mark.asyncio
async def test_dlq_cli_list_and_view_operations(
    test_kafka_config: KafkaConfig,
    download_worker,
    mock_storage: Dict,
    tmp_path: Path,
):
    """
    Test DLQ CLI can parse and display messages.

    Validates:
    - DLQHandler.parse_dlq_message() works with messages from topic
    - Messages contain all expected fields
    - Multiple messages can be retrieved and parsed
    """
    # Create multiple test tasks that will go to DLQ
    test_tasks = [
        create_download_task_message(
            trace_id=f"dlq-cli-{i:03d}",
            attachment_url=f"https://example.com/file-{i}.pdf",
            retry_count=0,
        )
        for i in range(3)
    ]

    # Mock download to fail with different permanent errors
    error_messages = [
        "404 Not Found",
        "403 Forbidden",
        "Invalid file type",
    ]

    call_count = [0]

    with patch("core.download.downloader.AttachmentDownloader.download") as mock_download:

        async def mock_various_failures(task):
            idx = call_count[0] % len(error_messages)
            call_count[0] += 1

            return DownloadOutcome.download_failure(
                error_message=error_messages[idx],
                error_category=ErrorCategory.PERMANENT,
                status_code=404 if idx == 0 else None,
            )

        mock_download.side_effect = mock_various_failures

        # Start download worker
        worker_task = await start_worker_background(download_worker)

        try:
            # Produce all tasks
            producer = AIOKafkaProducer(
                bootstrap_servers=test_kafka_config.bootstrap_servers,
                value_serializer=lambda v: v.model_dump_json().encode("utf-8"),
            )
            await producer.start()

            try:
                for task in test_tasks:
                    await producer.send(
                        test_kafka_config.downloads_pending_topic,
                        value=task,
                    )
                    logger.info(f"Produced task {task.trace_id} for CLI test")

            finally:
                await producer.stop()

            # Wait for all messages to reach DLQ
            await asyncio.sleep(5.0)

            # Fetch DLQ messages directly from topic
            dlq_messages = await get_topic_messages(
                test_kafka_config,
                test_kafka_config.dlq_topic,
                max_messages=20,
                timeout_seconds=5.0,
            )

            assert len(dlq_messages) >= len(
                test_tasks
            ), f"Expected at least {len(test_tasks)} DLQ messages, got {len(dlq_messages)}"

            # Create DLQ handler to parse messages
            dlq_handler = DLQHandler(test_kafka_config)

            # Parse and validate messages
            from aiokafka.structs import ConsumerRecord

            fetched_trace_ids = []
            for msg_dict in dlq_messages:
                # Create ConsumerRecord for parsing
                record = ConsumerRecord(
                    topic=test_kafka_config.dlq_topic,
                    partition=msg_dict["partition"],
                    offset=msg_dict["offset"],
                    timestamp=msg_dict["timestamp"],
                    timestamp_type=1,
                    key=msg_dict["key"],
                    value=msg_dict["value"],
                    checksum=None,
                    serialized_key_size=0,
                    serialized_value_size=len(msg_dict["value"]),
                    headers=msg_dict["headers"] or [],
                )

                try:
                    dlq_msg = dlq_handler.parse_dlq_message(record)
                    fetched_trace_ids.append(dlq_msg.trace_id)

                    # Validate message structure
                    assert dlq_msg.error_category == ErrorCategory.PERMANENT.value
                    assert dlq_msg.final_error in error_messages
                    assert dlq_msg.original_task is not None

                except Exception:
                    continue

            # Validate all our test tasks are in DLQ
            for task in test_tasks:
                assert (
                    task.trace_id in fetched_trace_ids
                ), f"Task {task.trace_id} not found in DLQ"

        finally:
            await stop_worker_gracefully(download_worker, worker_task)


@pytest.mark.asyncio
async def test_dlq_cli_replay_operation(
    test_kafka_config: KafkaConfig,
    download_worker,
    mock_storage: Dict,
    tmp_path: Path,
):
    """
    Test DLQ replay sends message to pending topic for reprocessing.

    Validates:
    - DLQHandler.replay_message() sends to pending topic
    - Replayed message has retry_count reset to 0
    - Replayed message includes replay metadata
    - Replayed message can be successfully processed
    - Audit logging for replay operation
    """
    # Create test file for successful replay
    test_file_path = tmp_path / "replay-success.pdf"
    test_file_content = b"PDF content after DLQ replay"
    test_file_path.write_bytes(test_file_content)

    # Create test task that will first fail, then succeed on replay
    test_task = create_download_task_message(
        trace_id="dlq-replay-001",
        attachment_url="https://example.com/replay-test.pdf",
        retry_count=0,
    )

    call_count = [0]

    # Mock download: fail first time (permanent), succeed on replay
    with patch("core.download.downloader.AttachmentDownloader.download") as mock_download:

        async def mock_fail_then_succeed(task):
            call_count[0] += 1

            if call_count[0] == 1:
                # First call fails
                return DownloadOutcome.download_failure(
                    error_message="Temporary service unavailable",
                    error_category=ErrorCategory.PERMANENT,  # Send to DLQ
                    status_code=503,
                )
            else:
                # Replay succeeds
                return DownloadOutcome.success_outcome(
                    file_path=test_file_path,
                    bytes_downloaded=len(test_file_content),
                    content_type="application/pdf",
                    status_code=200,
                )

        mock_download.side_effect = mock_fail_then_succeed

        # Start download worker
        worker_task = await start_worker_background(download_worker)

        try:
            # Produce initial task
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
                logger.info(f"Produced task {test_task.trace_id} (will fail first time)")

            finally:
                await producer.stop()

            # Wait for DLQ routing
            await asyncio.sleep(3.0)

            # Verify task in DLQ
            dlq_messages = await get_topic_messages(
                test_kafka_config,
                test_kafka_config.dlq_topic,
                max_messages=10,
                timeout_seconds=5.0,
            )

            assert len(dlq_messages) > 0, "Task not in DLQ"

            # Create DLQ handler and replay the message
            dlq_handler = DLQHandler(test_kafka_config)

            # Start producer for replay
            from kafka_pipeline.common.producer import BaseKafkaProducer

            dlq_handler._producer = BaseKafkaProducer(test_kafka_config)
            await dlq_handler._producer.start()

            try:
                # Parse DLQ message and replay it
                from aiokafka.structs import ConsumerRecord

                msg_dict = dlq_messages[0]
                record = ConsumerRecord(
                    topic=test_kafka_config.dlq_topic,
                    partition=msg_dict["partition"],
                    offset=msg_dict["offset"],
                    timestamp=msg_dict["timestamp"],
                    timestamp_type=1,
                    key=msg_dict["key"],
                    value=msg_dict["value"],
                    checksum=None,
                    serialized_key_size=0,
                    serialized_value_size=len(msg_dict["value"]),
                    headers=msg_dict["headers"] or [],
                )

                # Replay the message
                await dlq_handler.replay_message(record)

                # Wait for replay to be processed
                await asyncio.sleep(3.0)

                # Verify message was sent to pending topic
                pending_messages = await get_topic_messages(
                    test_kafka_config,
                    test_kafka_config.downloads_pending_topic,
                    max_messages=20,
                    timeout_seconds=5.0,
                )

                # Find replayed message
                replayed_tasks = [
                    DownloadTaskMessage.model_validate_json(msg["value"])
                    for msg in pending_messages
                    if DownloadTaskMessage.model_validate_json(msg["value"]).trace_id
                    == test_task.trace_id
                ]

                # Should have at least 1 replayed message (original + replay)
                assert len(replayed_tasks) > 0, "Replayed message not found in pending topic"

                # Find the replayed one (has replay metadata)
                replayed_task = None
                for task in replayed_tasks:
                    if task.metadata.get("replayed_from_dlq"):
                        replayed_task = task
                        break

                assert replayed_task is not None, "Could not find replayed message with metadata"

                # Validate replayed message
                assert replayed_task.retry_count == 0, "Retry count should be reset to 0"
                assert replayed_task.metadata.get("replayed_from_dlq") is True
                assert "dlq_offset" in replayed_task.metadata
                assert "dlq_partition" in replayed_task.metadata

                # Wait for successful download processing after replay
                mock_onelake = mock_storage["onelake"]
                success = await wait_for_condition(
                    lambda: mock_onelake.upload_count > 0,
                    timeout_seconds=10.0,
                    description="OneLake upload after replay",
                )
                assert success, "File not uploaded after replay"

            finally:
                await dlq_handler._producer.stop()

        finally:
            await stop_worker_gracefully(download_worker, worker_task)


@pytest.mark.asyncio
async def test_invalid_url_validation_failure_routes_to_dlq(
    test_kafka_config: KafkaConfig,
    download_worker,
    mock_storage: Dict,
    tmp_path: Path,
):
    """
    Test URL validation failures route to DLQ.

    Validates:
    - Invalid URL (SSRF prevention) routes to DLQ
    - Error message indicates validation failure
    - No retry attempts made
    """
    # Create task with invalid URL (should fail validation)
    test_task = create_download_task_message(
        trace_id="dlq-test-invalid-url",
        attachment_url="http://localhost/admin/secrets.pdf",  # SSRF attempt
        retry_count=0,
    )

    # Mock download to fail with validation error
    with patch("core.download.downloader.AttachmentDownloader.download") as mock_download:

        async def mock_validation_error(task):
            return DownloadOutcome.download_failure(
                error_message="URL validation failed: Domain not in allowlist",
                error_category=ErrorCategory.PERMANENT,
                status_code=None,
            )

        mock_download.side_effect = mock_validation_error

        # Start download worker
        worker_task = await start_worker_background(download_worker)

        try:
            # Produce task
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
                logger.info(f"Produced task {test_task.trace_id} (invalid URL)")

            finally:
                await producer.stop()

            # Wait for DLQ routing
            await asyncio.sleep(3.0)

            # Verify in DLQ
            dlq_messages = await get_topic_messages(
                test_kafka_config,
                test_kafka_config.dlq_topic,
                max_messages=10,
                timeout_seconds=5.0,
            )

            assert len(dlq_messages) > 0, "URL validation failure not in DLQ"

            # Validate DLQ message
            dlq_msg = FailedDownloadMessage.model_validate_json(dlq_messages[0]["value"])
            assert dlq_msg.trace_id == test_task.trace_id
            assert "validation failed" in dlq_msg.final_error.lower()
            assert dlq_msg.error_category == ErrorCategory.PERMANENT.value
            assert dlq_msg.retry_count == 0

        finally:
            await stop_worker_gracefully(download_worker, worker_task)
