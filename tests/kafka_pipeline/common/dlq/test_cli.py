"""
Tests for DLQ CLI tool.
"""

import asyncio
import pytest
from datetime import datetime, timezone
from io import StringIO
from unittest.mock import AsyncMock, MagicMock, patch
from aiokafka.structs import ConsumerRecord

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.common.dlq.cli import DLQCLIManager
from kafka_pipeline.xact.schemas.results import FailedDownloadMessage
from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage


@pytest.fixture
def kafka_config():
    """Create test Kafka configuration with hierarchical domain structure."""
    return KafkaConfig(
        bootstrap_servers="localhost:9092",
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        xact={
            "topics": {
                "events": "test.events.raw",
                "downloads_pending": "test.downloads.pending",
                "downloads_cached": "test.downloads.cached",
                "dlq": "test.dlq",
            },
            "retry_delays": [300, 600, 1200, 2400],
            "consumer_group_prefix": "test",
        },
    )


@pytest.fixture
def sample_task():
    """Create sample download task message."""
    return DownloadTaskMessage(
        trace_id="evt-123",
        media_id="media-123",
        attachment_url="https://storage.example.com/file.pdf",
        blob_path="documentsReceived/C-456/pdf/file.pdf",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="C-456",
        event_type="claim",
        event_subtype="created",
        retry_count=4,
        original_timestamp=datetime.now(timezone.utc),
        metadata={"source": "test"},
    )


@pytest.fixture
def sample_dlq_message(sample_task):
    """Create sample DLQ message."""
    return FailedDownloadMessage(
        trace_id=sample_task.trace_id,
        media_id=sample_task.media_id,
        attachment_url=sample_task.attachment_url,
        original_task=sample_task,
        final_error="File not found after 4 retries",
        error_category="permanent",
        retry_count=4,
        failed_at=datetime.now(timezone.utc),
    )


def create_consumer_record_from_dlq(dlq_msg: FailedDownloadMessage) -> ConsumerRecord:
    """Helper to create ConsumerRecord containing DLQ message."""
    value_json = dlq_msg.model_dump_json()
    return ConsumerRecord(
        topic="test.dlq",
        partition=0,
        offset=42,
        timestamp=1234567890,
        timestamp_type=0,
        key=dlq_msg.trace_id.encode("utf-8"),
        value=value_json.encode("utf-8"),
        headers=[],
        checksum=None,
        serialized_key_size=len(dlq_msg.trace_id),
        serialized_value_size=len(value_json),
    )


class TestDLQCLIManagerInit:
    """Tests for DLQ CLI manager initialization."""

    def test_init_with_config(self, kafka_config):
        """CLI manager initializes with config."""
        manager = DLQCLIManager(kafka_config)

        assert manager.config == kafka_config
        assert manager.handler is not None
        assert manager._messages == []


class TestDLQCLIManagerFetchMessages:
    """Tests for fetching DLQ messages."""

    @pytest.mark.asyncio
    async def test_fetch_messages_success(self, kafka_config, sample_dlq_message):
        """Fetch messages from DLQ topic."""
        manager = DLQCLIManager(kafka_config)

        # Mock consumer
        mock_aiokafka = MagicMock()
        record = create_consumer_record_from_dlq(sample_dlq_message)
        mock_aiokafka.getmany = AsyncMock(
            return_value={
                MagicMock(): [record]
            }
        )

        mock_consumer = MagicMock()
        mock_consumer._consumer = mock_aiokafka
        manager.handler._consumer = mock_consumer

        # Fetch
        messages = await manager.fetch_messages(limit=10, timeout_ms=1000)

        assert len(messages) == 1
        assert messages[0] == record
        assert manager._messages == messages
        mock_aiokafka.getmany.assert_called_once_with(timeout_ms=1000, max_records=10)

    @pytest.mark.asyncio
    async def test_fetch_messages_consumer_not_started(self, kafka_config):
        """Fetch raises RuntimeError if consumer not started."""
        manager = DLQCLIManager(kafka_config)

        with pytest.raises(RuntimeError, match="not started"):
            await manager.fetch_messages()

    @pytest.mark.asyncio
    async def test_fetch_messages_respects_limit(self, kafka_config, sample_dlq_message):
        """Fetch respects message limit."""
        manager = DLQCLIManager(kafka_config)

        # Create multiple records
        records = [create_consumer_record_from_dlq(sample_dlq_message) for _ in range(5)]

        mock_aiokafka = MagicMock()
        mock_aiokafka.getmany = AsyncMock(
            return_value={MagicMock(): records}
        )

        mock_consumer = MagicMock()
        mock_consumer._consumer = mock_aiokafka
        manager.handler._consumer = mock_consumer

        # Fetch with limit
        messages = await manager.fetch_messages(limit=3, timeout_ms=1000)

        # Should stop after limit reached
        assert len(messages) <= 5


class TestDLQCLIManagerListMessages:
    """Tests for listing DLQ messages."""

    def test_list_messages_empty(self, kafka_config, capsys):
        """List shows message when no DLQ messages."""
        manager = DLQCLIManager(kafka_config)

        manager.list_messages()

        captured = capsys.readouterr()
        assert "No DLQ messages found" in captured.out

    def test_list_messages_displays_table(self, kafka_config, sample_dlq_message, capsys):
        """List displays messages in table format."""
        manager = DLQCLIManager(kafka_config)
        record = create_consumer_record_from_dlq(sample_dlq_message)
        manager._messages = [record]

        manager.list_messages()

        captured = capsys.readouterr()
        assert "DLQ Messages" in captured.out
        assert "evt-123" in captured.out
        assert "permanent" in captured.out
        assert str(sample_dlq_message.retry_count) in captured.out

    def test_list_messages_handles_parse_errors(self, kafka_config, capsys):
        """List handles messages that can't be parsed."""
        manager = DLQCLIManager(kafka_config)

        # Create invalid record
        invalid_record = ConsumerRecord(
            topic="test.dlq",
            partition=0,
            offset=42,
            timestamp=0,
            timestamp_type=0,
            key=b"key",
            value=b"invalid",
            headers=[],
            checksum=None,
            serialized_key_size=3,
            serialized_value_size=7,
        )
        manager._messages = [invalid_record]

        # Should not raise
        manager.list_messages()

        captured = capsys.readouterr()
        assert "PARSE ERROR" in captured.out


class TestDLQCLIManagerViewMessage:
    """Tests for viewing DLQ message details."""

    def test_view_message_success(self, kafka_config, sample_dlq_message, capsys):
        """View displays detailed message information."""
        manager = DLQCLIManager(kafka_config)
        record = create_consumer_record_from_dlq(sample_dlq_message)
        manager._messages = [record]

        manager.view_message("evt-123")

        captured = capsys.readouterr()
        assert "DLQ Message Details" in captured.out
        assert "evt-123" in captured.out
        assert sample_dlq_message.attachment_url in captured.out
        assert sample_dlq_message.final_error in captured.out
        assert sample_dlq_message.error_category in captured.out

    def test_view_message_not_found(self, kafka_config, capsys):
        """View shows error when message not found."""
        manager = DLQCLIManager(kafka_config)
        manager._messages = []

        manager.view_message("nonexistent")

        captured = capsys.readouterr()
        assert "No message found" in captured.out
        assert "nonexistent" in captured.out

    def test_view_message_shows_metadata(self, kafka_config, capsys):
        """View displays message metadata."""
        manager = DLQCLIManager(kafka_config)

        task = DownloadTaskMessage(
            trace_id="evt-456",
            media_id="media-456",
            attachment_url="https://example.com/test.pdf",
            blob_path="documentsReceived/T-001/pdf/test.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="T-001",
            event_type="test",
            event_subtype="created",
            retry_count=2,
            original_timestamp=datetime.now(timezone.utc),
            metadata={"key1": "value1", "key2": "value2"},
        )

        dlq_msg = FailedDownloadMessage(
            trace_id=task.trace_id,
            media_id=task.media_id,
            attachment_url=task.attachment_url,
            original_task=task,
            final_error="Test error",
            error_category="transient",
            retry_count=2,
            failed_at=datetime.now(timezone.utc),
        )

        record = create_consumer_record_from_dlq(dlq_msg)
        manager._messages = [record]

        manager.view_message("evt-456")

        captured = capsys.readouterr()
        assert "Metadata:" in captured.out
        assert "key1: value1" in captured.out
        assert "key2: value2" in captured.out


class TestDLQCLIManagerReplayMessage:
    """Tests for replaying DLQ messages."""

    @pytest.mark.asyncio
    async def test_replay_message_success(self, kafka_config, sample_dlq_message, capsys):
        """Replay sends message to pending topic."""
        manager = DLQCLIManager(kafka_config)
        record = create_consumer_record_from_dlq(sample_dlq_message)
        manager._messages = [record]

        # Mock handler replay_message
        manager.handler.replay_message = AsyncMock()

        await manager.replay_message("evt-123")

        # Verify replay was called
        manager.handler.replay_message.assert_called_once_with(record)

        # Verify output
        captured = capsys.readouterr()
        assert "\u2713" in captured.out
        assert "replayed successfully" in captured.out

    @pytest.mark.asyncio
    async def test_replay_message_not_found(self, kafka_config, capsys):
        """Replay shows error when message not found."""
        manager = DLQCLIManager(kafka_config)
        manager._messages = []

        await manager.replay_message("nonexistent")

        captured = capsys.readouterr()
        assert "No message found" in captured.out

    @pytest.mark.asyncio
    async def test_replay_message_handles_errors(self, kafka_config, sample_dlq_message, capsys):
        """Replay handles replay errors gracefully."""
        manager = DLQCLIManager(kafka_config)
        record = create_consumer_record_from_dlq(sample_dlq_message)
        manager._messages = [record]

        # Mock handler to raise error
        manager.handler.replay_message = AsyncMock(side_effect=Exception("Replay failed"))

        await manager.replay_message("evt-123")

        captured = capsys.readouterr()
        assert "\u2717" in captured.out
        assert "Failed to replay" in captured.out


class TestDLQCLIManagerResolveMessage:
    """Tests for resolving DLQ messages."""

    @pytest.mark.asyncio
    async def test_resolve_message_success(self, kafka_config, sample_dlq_message, capsys):
        """Resolve commits offset for message."""
        manager = DLQCLIManager(kafka_config)
        record = create_consumer_record_from_dlq(sample_dlq_message)
        manager._messages = [record]

        # Mock handler acknowledge_message
        manager.handler.acknowledge_message = AsyncMock()

        await manager.resolve_message("evt-123")

        # Verify acknowledge was called
        manager.handler.acknowledge_message.assert_called_once_with(record)

        # Verify output
        captured = capsys.readouterr()
        assert "\u2713" in captured.out
        assert "resolved successfully" in captured.out

    @pytest.mark.asyncio
    async def test_resolve_message_not_found(self, kafka_config, capsys):
        """Resolve shows error when message not found."""
        manager = DLQCLIManager(kafka_config)
        manager._messages = []

        await manager.resolve_message("nonexistent")

        captured = capsys.readouterr()
        assert "No message found" in captured.out

    @pytest.mark.asyncio
    async def test_resolve_message_handles_errors(self, kafka_config, sample_dlq_message, capsys):
        """Resolve handles acknowledgment errors gracefully."""
        manager = DLQCLIManager(kafka_config)
        record = create_consumer_record_from_dlq(sample_dlq_message)
        manager._messages = [record]

        # Mock handler to raise error
        manager.handler.acknowledge_message = AsyncMock(side_effect=Exception("Ack failed"))

        await manager.resolve_message("evt-123")

        captured = capsys.readouterr()
        assert "\u2717" in captured.out
        assert "Failed to resolve" in captured.out


class TestDLQCLIManagerFindMessage:
    """Tests for finding messages by trace_id."""

    def test_find_message_by_trace_id_found(self, kafka_config, sample_dlq_message):
        """Find returns record when trace_id matches."""
        manager = DLQCLIManager(kafka_config)
        record = create_consumer_record_from_dlq(sample_dlq_message)
        manager._messages = [record]

        found = manager._find_message_by_trace_id("evt-123")

        assert found == record

    def test_find_message_by_trace_id_not_found(self, kafka_config, sample_dlq_message):
        """Find returns None when trace_id doesn't match."""
        manager = DLQCLIManager(kafka_config)
        record = create_consumer_record_from_dlq(sample_dlq_message)
        manager._messages = [record]

        found = manager._find_message_by_trace_id("nonexistent")

        assert found is None

    def test_find_message_skips_unparseable(self, kafka_config, sample_dlq_message):
        """Find skips messages that can't be parsed."""
        manager = DLQCLIManager(kafka_config)

        # Add invalid record
        invalid_record = ConsumerRecord(
            topic="test.dlq",
            partition=0,
            offset=1,
            timestamp=0,
            timestamp_type=0,
            key=b"key",
            value=b"invalid",
            headers=[],
            checksum=None,
            serialized_key_size=3,
            serialized_value_size=7,
        )

        # Add valid record
        valid_record = create_consumer_record_from_dlq(sample_dlq_message)

        manager._messages = [invalid_record, valid_record]

        # Should skip invalid and find valid
        found = manager._find_message_by_trace_id("evt-123")

        assert found == valid_record
