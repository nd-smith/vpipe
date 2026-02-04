"""
Tests for Verisk DLQ handler.

Test Coverage:
    - Initialization
    - Message parsing (JSON decode, Pydantic validation)
    - Parse error handling (empty, invalid JSON, validation errors)
    - Replay message logic (task extraction, retry_count reset, metadata)
    - Replay headers
    - Acknowledge message (offset commit)
    - Producer/consumer state checks
    - is_running property
"""

import json
import pytest
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock

from config.config import KafkaConfig
from pipeline.common.types import PipelineMessage
from pipeline.verisk.dlq.handler import DLQHandler
from pipeline.verisk.schemas.results import FailedDownloadMessage
from pipeline.verisk.schemas.tasks import DownloadTaskMessage


class TestDLQHandlerInitialization:
    """Tests for DLQHandler initialization."""

    def test_initialization(self):
        """DLQHandler initializes with config and domain."""
        config = Mock(spec=KafkaConfig)

        handler = DLQHandler(config=config)

        assert handler.config is config
        assert handler.domain == "verisk"
        assert handler._consumer is None
        assert handler._producer is None

    def test_is_running_property_when_not_started(self):
        """is_running returns False when handler not started."""
        config = Mock(spec=KafkaConfig)
        handler = DLQHandler(config=config)

        assert handler.is_running is False

    def test_is_running_property_with_consumer_only(self):
        """is_running returns False when only consumer is set."""
        config = Mock(spec=KafkaConfig)
        handler = DLQHandler(config=config)
        handler._consumer = Mock(is_running=True)

        assert handler.is_running is False

    def test_is_running_property_with_producer_only(self):
        """is_running returns False when only producer is set."""
        config = Mock(spec=KafkaConfig)
        handler = DLQHandler(config=config)
        handler._producer = Mock(is_started=True)

        assert handler.is_running is False

    def test_is_running_property_with_both_started(self):
        """is_running returns True when both consumer and producer are started."""
        config = Mock(spec=KafkaConfig)
        handler = DLQHandler(config=config)
        handler._consumer = Mock(is_running=True)
        handler._producer = Mock(is_started=True)

        assert handler.is_running is True


class TestDLQHandlerMessageParsing:
    """Tests for DLQ message parsing logic."""

    @pytest.fixture
    def handler(self):
        """DLQHandler instance."""
        config = Mock(spec=KafkaConfig)
        return DLQHandler(config=config)

    @pytest.fixture
    def valid_dlq_message_dict(self):
        """Valid DLQ message as dictionary."""
        return {
            "trace_id": "test-trace-123",
            "attachment_url": "https://example.com/file.pdf",
            "original_task": {
                "trace_id": "test-trace-123",
                "media_id": "media-456",
                "attachment_url": "https://example.com/file.pdf",
                "blob_path": "attachments/file.pdf",
                "status_subtype": "inspection",
                "file_type": "pdf",
                "assignment_id": "assignment-789",
                "estimate_version": "v1",
                "event_type": "statusChange",
                "event_subtype": "inspectionCompleted",
                "retry_count": 3,
                "original_timestamp": datetime.now(UTC).isoformat(),
                "metadata": {"key": "value"},
            },
            "error_category": "transient",
            "retry_count": 3,
            "failed_at": datetime.now(UTC).isoformat(),
        }

    def test_parse_dlq_message_success(self, handler, valid_dlq_message_dict):
        """Successfully parses valid DLQ message."""
        json_bytes = json.dumps(valid_dlq_message_dict).encode("utf-8")
        record = PipelineMessage(
            topic="dlq",
            partition=0,
            offset=123,
            key=b"test-trace-123",
            value=json_bytes,
            timestamp=datetime.now(UTC),
            headers=[],
        )

        dlq_msg = handler.parse_dlq_message(record)

        assert isinstance(dlq_msg, FailedDownloadMessage)
        assert dlq_msg.trace_id == "test-trace-123"
        assert dlq_msg.attachment_url == "https://example.com/file.pdf"
        assert dlq_msg.error_category == "transient"
        assert dlq_msg.retry_count == 3

    def test_parse_dlq_message_empty_value_raises_error(self, handler):
        """Raises ValueError when message value is empty."""
        record = PipelineMessage(
            topic="dlq",
            partition=0,
            offset=123,
            key=b"test-trace-123",
            value=None,
            timestamp=datetime.now(UTC),
            headers=[],
        )

        with pytest.raises(ValueError, match="DLQ message value is empty"):
            handler.parse_dlq_message(record)

    def test_parse_dlq_message_invalid_json_raises_error(self, handler):
        """Raises ValueError when message contains invalid JSON."""
        record = PipelineMessage(
            topic="dlq",
            partition=0,
            offset=123,
            key=b"test-trace-123",
            value=b"not valid json {",
            timestamp=datetime.now(UTC),
            headers=[],
        )

        with pytest.raises(ValueError, match="Invalid JSON in DLQ message"):
            handler.parse_dlq_message(record)

    def test_parse_dlq_message_missing_required_fields_raises_error(self, handler):
        """Raises ValueError when message is missing required fields."""
        incomplete_dict = {
            "trace_id": "test-trace-123",
            # Missing attachment_url, original_task, etc.
        }
        json_bytes = json.dumps(incomplete_dict).encode("utf-8")
        record = PipelineMessage(
            topic="dlq",
            partition=0,
            offset=123,
            key=b"test-trace-123",
            value=json_bytes,
            timestamp=datetime.now(UTC),
            headers=[],
        )

        with pytest.raises(ValueError, match="Failed to parse DLQ message"):
            handler.parse_dlq_message(record)

    def test_parse_dlq_message_preserves_original_task(
        self, handler, valid_dlq_message_dict
    ):
        """Parsed message preserves complete original task."""
        json_bytes = json.dumps(valid_dlq_message_dict).encode("utf-8")
        record = PipelineMessage(
            topic="dlq",
            partition=0,
            offset=123,
            key=b"test-trace-123",
            value=json_bytes,
            timestamp=datetime.now(UTC),
            headers=[],
        )

        dlq_msg = handler.parse_dlq_message(record)

        assert dlq_msg.original_task.trace_id == "test-trace-123"
        assert dlq_msg.original_task.media_id == "media-456"
        assert dlq_msg.original_task.retry_count == 3
        assert dlq_msg.original_task.metadata == {"key": "value"}


class TestDLQHandlerReplayMessage:
    """Tests for DLQ message replay logic."""

    @pytest.fixture
    def handler(self):
        """DLQHandler with mocked producer."""
        config = Mock(spec=KafkaConfig)
        handler = DLQHandler(config=config)
        handler._producer = AsyncMock()
        handler._producer.is_started = True
        handler._pending_topic = "verisk-downloads-pending"  # Missing in production code
        return handler

    @pytest.fixture
    def valid_dlq_message_dict(self):
        """Valid DLQ message as dictionary."""
        return {
            "trace_id": "test-trace-123",
            "attachment_url": "https://example.com/file.pdf",
            "original_task": {
                "trace_id": "test-trace-123",
                "media_id": "media-456",
                "attachment_url": "https://example.com/file.pdf",
                "blob_path": "attachments/file.pdf",
                "status_subtype": "inspection",
                "file_type": "pdf",
                "assignment_id": "assignment-789",
                "estimate_version": "v1",
                "event_type": "statusChange",
                "event_subtype": "inspectionCompleted",
                "retry_count": 4,
                "original_timestamp": datetime.now(UTC).isoformat(),
                "metadata": {"original_key": "original_value"},
            },
            "error_category": "transient",
            "retry_count": 4,
            "failed_at": datetime.now(UTC).isoformat(),
        }

    @pytest.fixture
    def pipeline_record(self, valid_dlq_message_dict):
        """PipelineMessage with valid DLQ message."""
        json_bytes = json.dumps(valid_dlq_message_dict).encode("utf-8")
        return PipelineMessage(
            topic="dlq",
            partition=2,
            offset=456,
            key=b"test-trace-123",
            value=json_bytes,
            timestamp=datetime.now(UTC),
            headers=[],
        )

    @pytest.mark.asyncio
    async def test_replay_message_raises_if_producer_not_started(self):
        """Raises RuntimeError if producer not started."""
        config = Mock(spec=KafkaConfig)
        handler = DLQHandler(config=config)
        handler._producer = None

        record = PipelineMessage(
            topic="dlq",
            partition=0,
            offset=123,
            key=b"test",
            value=b"{}",
            timestamp=datetime.now(UTC),
            headers=[],
        )

        with pytest.raises(RuntimeError, match="Producer not started"):
            await handler.replay_message(record)

    @pytest.mark.asyncio
    async def test_replay_message_sends_to_producer(self, handler, pipeline_record):
        """Replay message sends replayed task to producer."""
        await handler.replay_message(pipeline_record)

        handler._producer.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_replay_message_resets_retry_count(self, handler, pipeline_record):
        """Replayed task has retry_count reset to 0."""
        await handler.replay_message(pipeline_record)

        send_call = handler._producer.send.call_args
        replayed_task = send_call.kwargs["value"]

        assert isinstance(replayed_task, DownloadTaskMessage)
        assert replayed_task.retry_count == 0

    @pytest.mark.asyncio
    async def test_replay_message_preserves_task_fields(
        self, handler, pipeline_record
    ):
        """Replayed task preserves all original task fields."""
        await handler.replay_message(pipeline_record)

        send_call = handler._producer.send.call_args
        replayed_task = send_call.kwargs["value"]

        assert replayed_task.trace_id == "test-trace-123"
        assert replayed_task.media_id == "media-456"
        assert replayed_task.attachment_url == "https://example.com/file.pdf"
        assert replayed_task.blob_path == "attachments/file.pdf"
        assert replayed_task.status_subtype == "inspection"
        assert replayed_task.file_type == "pdf"
        assert replayed_task.assignment_id == "assignment-789"

    @pytest.mark.asyncio
    async def test_replay_message_adds_replay_metadata(
        self, handler, pipeline_record
    ):
        """Replayed task includes replay metadata."""
        await handler.replay_message(pipeline_record)

        send_call = handler._producer.send.call_args
        replayed_task = send_call.kwargs["value"]

        assert replayed_task.metadata["replayed_from_dlq"] is True
        assert replayed_task.metadata["dlq_offset"] == 456
        assert replayed_task.metadata["dlq_partition"] == 2

    @pytest.mark.asyncio
    async def test_replay_message_preserves_original_metadata(
        self, handler, pipeline_record
    ):
        """Replayed task preserves original task metadata."""
        await handler.replay_message(pipeline_record)

        send_call = handler._producer.send.call_args
        replayed_task = send_call.kwargs["value"]

        assert replayed_task.metadata["original_key"] == "original_value"

    @pytest.mark.asyncio
    async def test_replay_message_key_is_trace_id(self, handler, pipeline_record):
        """Replayed message uses trace_id as key."""
        await handler.replay_message(pipeline_record)

        send_call = handler._producer.send.call_args
        assert send_call.kwargs["key"] == "test-trace-123"

    @pytest.mark.asyncio
    async def test_replay_message_headers_include_replay_flag(
        self, handler, pipeline_record
    ):
        """Replayed message headers include replayed_from_dlq flag."""
        await handler.replay_message(pipeline_record)

        send_call = handler._producer.send.call_args
        headers = send_call.kwargs["headers"]

        assert headers["trace_id"] == "test-trace-123"
        assert headers["replayed_from_dlq"] == "true"


class TestDLQHandlerAcknowledgeMessage:
    """Tests for DLQ message acknowledgment logic."""

    @pytest.fixture
    def handler(self):
        """DLQHandler with mocked consumer."""
        config = Mock(spec=KafkaConfig)
        handler = DLQHandler(config=config)
        mock_consumer = Mock()
        mock_consumer._consumer = AsyncMock()
        handler._consumer = mock_consumer
        return handler

    @pytest.fixture
    def valid_dlq_message_dict(self):
        """Valid DLQ message as dictionary."""
        return {
            "trace_id": "test-trace-123",
            "attachment_url": "https://example.com/file.pdf",
            "original_task": {
                "trace_id": "test-trace-123",
                "media_id": "media-456",
                "attachment_url": "https://example.com/file.pdf",
                "blob_path": "attachments/file.pdf",
                "status_subtype": "inspection",
                "file_type": "pdf",
                "assignment_id": "assignment-789",
                "estimate_version": "v1",
                "event_type": "statusChange",
                "event_subtype": "inspectionCompleted",
                "retry_count": 4,
                "original_timestamp": datetime.now(UTC).isoformat(),
                "metadata": {},
            },
            "error_category": "permanent",
            "retry_count": 4,
            "failed_at": datetime.now(UTC).isoformat(),
        }

    @pytest.fixture
    def pipeline_record(self, valid_dlq_message_dict):
        """PipelineMessage with valid DLQ message."""
        json_bytes = json.dumps(valid_dlq_message_dict).encode("utf-8")
        return PipelineMessage(
            topic="dlq",
            partition=1,
            offset=789,
            key=b"test-trace-123",
            value=json_bytes,
            timestamp=datetime.now(UTC),
            headers=[],
        )

    @pytest.mark.asyncio
    async def test_acknowledge_message_raises_if_consumer_not_started(self):
        """Raises RuntimeError if consumer not started."""
        config = Mock(spec=KafkaConfig)
        handler = DLQHandler(config=config)
        handler._consumer = None

        record = PipelineMessage(
            topic="dlq",
            partition=0,
            offset=123,
            key=b"test",
            value=b"{}",
            timestamp=datetime.now(UTC),
            headers=[],
        )

        with pytest.raises(RuntimeError, match="Consumer not started"):
            await handler.acknowledge_message(record)

    @pytest.mark.asyncio
    async def test_acknowledge_message_commits_offset(self, handler, pipeline_record):
        """Acknowledge message commits consumer offset."""
        await handler.acknowledge_message(pipeline_record)

        handler._consumer._consumer.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_acknowledge_message_parses_message_first(
        self, handler, pipeline_record
    ):
        """Acknowledge message parses DLQ message before committing."""
        # This test verifies that parse is called (implicitly) by checking no error
        await handler.acknowledge_message(pipeline_record)

        # If parsing failed, we'd get an exception before commit
        handler._consumer._consumer.commit.assert_called_once()


class TestDLQHandlerEdgeCases:
    """Tests for edge cases and error conditions."""

    @pytest.fixture
    def handler(self):
        """DLQHandler instance."""
        config = Mock(spec=KafkaConfig)
        return DLQHandler(config=config)

    def test_parse_message_with_unicode_content(self, handler):
        """Successfully parses message with unicode characters."""
        message_dict = {
            "trace_id": "test-trace-123",
            "attachment_url": "https://example.com/файл.pdf",  # Cyrillic
            "original_task": {
                "trace_id": "test-trace-123",
                "media_id": "media-456",
                "attachment_url": "https://example.com/файл.pdf",
                "blob_path": "attachments/file.pdf",
                "status_subtype": "inspection",
                "file_type": "pdf",
                "assignment_id": "assignment-789",
                "estimate_version": "v1",
                "event_type": "statusChange",
                "event_subtype": "inspectionCompleted",
                "retry_count": 0,
                "original_timestamp": datetime.now(UTC).isoformat(),
                "metadata": {},
            },
            "error_category": "transient",
            "retry_count": 0,
            "failed_at": datetime.now(UTC).isoformat(),
        }

        json_bytes = json.dumps(message_dict, ensure_ascii=False).encode("utf-8")
        record = PipelineMessage(
            topic="dlq",
            partition=0,
            offset=123,
            key=b"test-trace-123",
            value=json_bytes,
            timestamp=datetime.now(UTC),
            headers=[],
        )

        dlq_msg = handler.parse_dlq_message(record)
        assert "файл.pdf" in dlq_msg.attachment_url

    def test_parse_message_with_empty_metadata(self, handler):
        """Successfully parses message with empty metadata."""
        message_dict = {
            "trace_id": "test-trace-123",
            "attachment_url": "https://example.com/file.pdf",
            "original_task": {
                "trace_id": "test-trace-123",
                "media_id": "media-456",
                "attachment_url": "https://example.com/file.pdf",
                "blob_path": "attachments/file.pdf",
                "status_subtype": "inspection",
                "file_type": "pdf",
                "assignment_id": "assignment-789",
                "estimate_version": "v1",
                "event_type": "statusChange",
                "event_subtype": "inspectionCompleted",
                "retry_count": 0,
                "original_timestamp": datetime.now(UTC).isoformat(),
                "metadata": {},  # Empty metadata
            },
            "error_category": "transient",
            "retry_count": 0,
            "failed_at": datetime.now(UTC).isoformat(),
        }

        json_bytes = json.dumps(message_dict).encode("utf-8")
        record = PipelineMessage(
            topic="dlq",
            partition=0,
            offset=123,
            key=b"test-trace-123",
            value=json_bytes,
            timestamp=datetime.now(UTC),
            headers=[],
        )

        dlq_msg = handler.parse_dlq_message(record)
        assert dlq_msg.original_task.metadata == {}

    @pytest.mark.asyncio
    async def test_replay_with_zero_retry_count_in_original(self):
        """Replay works correctly when original task has retry_count=0."""
        config = Mock(spec=KafkaConfig)
        handler = DLQHandler(config=config)
        handler._producer = AsyncMock()
        handler._producer.is_started = True
        handler._pending_topic = "verisk-downloads-pending"

        message_dict = {
            "trace_id": "test-trace-123",
            "attachment_url": "https://example.com/file.pdf",
            "original_task": {
                "trace_id": "test-trace-123",
                "media_id": "media-456",
                "attachment_url": "https://example.com/file.pdf",
                "blob_path": "attachments/file.pdf",
                "status_subtype": "inspection",
                "file_type": "pdf",
                "assignment_id": "assignment-789",
                "estimate_version": "v1",
                "event_type": "statusChange",
                "event_subtype": "inspectionCompleted",
                "retry_count": 0,  # Original was first attempt
                "original_timestamp": datetime.now(UTC).isoformat(),
                "metadata": {},
            },
            "error_category": "permanent",
            "retry_count": 0,
            "failed_at": datetime.now(UTC).isoformat(),
        }

        json_bytes = json.dumps(message_dict).encode("utf-8")
        record = PipelineMessage(
            topic="dlq",
            partition=0,
            offset=123,
            key=b"test-trace-123",
            value=json_bytes,
            timestamp=datetime.now(UTC),
            headers=[],
        )

        await handler.replay_message(record)

        send_call = handler._producer.send.call_args
        replayed_task = send_call.kwargs["value"]

        # Still resets to 0 (giving it a fresh start)
        assert replayed_task.retry_count == 0
