"""Tests for ClaimX DLQ handler.

Tests DLQ message parsing, replay logic, and acknowledgment for both
download and enrichment DLQ types without requiring Kafka infrastructure.
"""

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock, patch

import pytest

from config.config import MessageConfig
from pipeline.claimx.dlq.handler import ClaimXDLQHandler
from pipeline.claimx.schemas.results import (
    FailedDownloadMessage,
    FailedEnrichmentMessage,
)
from pipeline.claimx.schemas.tasks import (
    ClaimXDownloadTask,
    ClaimXEnrichmentTask,
)
from pipeline.common.types import PipelineMessage


@pytest.fixture
def mock_config():
    """Create a mock Kafka configuration."""
    config = Mock(spec=MessageConfig)
    config.bootstrap_servers = "localhost:9092"
    config.sasl_username = "test_user"
    config.sasl_password = "test_pass"
    config.security_protocol = "SASL_SSL"
    config.sasl_mechanism = "PLAIN"
    return config


@pytest.fixture
def sample_download_task():
    """Create a sample download task for testing."""
    return ClaimXDownloadTask(
        media_id="media_123",
        project_id="proj_456",
        download_url="https://example.com/file.pdf",
        blob_path="claimx/proj_456/media/file.pdf",
        file_type="pdf",
        file_name="file.pdf",
        source_event_id="evt_789",
        retry_count=3,
        metadata={"last_error": "Connection timeout"},
    )


@pytest.fixture
def sample_enrichment_task():
    """Create a sample enrichment task for testing."""
    return ClaimXEnrichmentTask(
        event_id="evt_123",
        event_type="PROJECT_CREATED",
        project_id="proj_456",
        retry_count=2,
        created_at=datetime(2024, 12, 25, 10, 0, 0, tzinfo=timezone.utc),
        metadata={"last_error": "API timeout"},
    )


@pytest.fixture
def valid_download_dlq_message_dict(sample_download_task):
    """Create a valid download DLQ message as a dict."""
    return {
        "media_id": sample_download_task.media_id,
        "project_id": sample_download_task.project_id,
        "download_url": sample_download_task.download_url,
        "original_task": {
            "media_id": sample_download_task.media_id,
            "project_id": sample_download_task.project_id,
            "download_url": sample_download_task.download_url,
            "blob_path": sample_download_task.blob_path,
            "file_type": sample_download_task.file_type,
            "file_name": sample_download_task.file_name,
            "source_event_id": sample_download_task.source_event_id,
            "retry_count": sample_download_task.retry_count,
            "metadata": sample_download_task.metadata,
        },
        "error_category": "transient",
        "retry_count": 3,
        "failed_at": "2024-12-25T12:30:45Z",
    }


@pytest.fixture
def valid_enrichment_dlq_message_dict(sample_enrichment_task):
    """Create a valid enrichment DLQ message as a dict."""
    return {
        "event_id": sample_enrichment_task.event_id,
        "event_type": sample_enrichment_task.event_type,
        "project_id": sample_enrichment_task.project_id,
        "original_task": {
            "event_id": sample_enrichment_task.event_id,
            "event_type": sample_enrichment_task.event_type,
            "project_id": sample_enrichment_task.project_id,
            "retry_count": sample_enrichment_task.retry_count,
            "created_at": sample_enrichment_task.created_at.isoformat(),
            "metadata": sample_enrichment_task.metadata,
        },
        "final_error": "API call failed",
        "error_category": "transient",
        "retry_count": 2,
        "failed_at": "2024-12-25T12:30:45Z",
    }


class TestClaimXDLQHandlerInitialization:
    """Tests for ClaimX DLQ handler initialization."""

    def test_initialization_with_download_type(self, mock_config):
        """Test handler initializes with download DLQ type."""
        handler = ClaimXDLQHandler(config=mock_config, dlq_type="download")

        assert handler.config == mock_config
        assert handler.domain == "claimx"
        assert handler.dlq_type == "download"
        assert handler._consumer is None
        assert handler._producer is None

    def test_initialization_with_enrichment_type(self, mock_config):
        """Test handler initializes with enrichment DLQ type."""
        handler = ClaimXDLQHandler(config=mock_config, dlq_type="enrichment")

        assert handler.config == mock_config
        assert handler.domain == "claimx"
        assert handler.dlq_type == "enrichment"
        assert handler._consumer is None
        assert handler._producer is None

    def test_initialization_with_invalid_type_raises_error(self, mock_config):
        """Test handler raises ValueError for invalid DLQ type."""
        with pytest.raises(ValueError) as exc_info:
            ClaimXDLQHandler(config=mock_config, dlq_type="invalid")

        assert "Invalid dlq_type: invalid" in str(exc_info.value)
        assert "Must be 'download' or 'enrichment'" in str(exc_info.value)

    def test_initialization_with_empty_type_raises_error(self, mock_config):
        """Test handler raises ValueError for empty DLQ type."""
        with pytest.raises(ValueError):
            ClaimXDLQHandler(config=mock_config, dlq_type="")


class TestClaimXDLQHandlerParsingDownload:
    """Tests for parsing download DLQ messages."""

    @pytest.fixture
    def download_handler(self, mock_config):
        """Create a download DLQ handler for testing."""
        return ClaimXDLQHandler(config=mock_config, dlq_type="download")

    def test_parse_download_dlq_message_success(
        self, download_handler, valid_download_dlq_message_dict
    ):
        """Test successful parsing of download DLQ message."""
        json_bytes = json.dumps(valid_download_dlq_message_dict).encode("utf-8")
        record = PipelineMessage(
            topic="claimx-downloads-dlq",
            partition=0,
            offset=123,
            key=b"evt_789",
            value=json_bytes,
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        dlq_msg = download_handler.parse_dlq_message(record)

        assert isinstance(dlq_msg, FailedDownloadMessage)
        assert dlq_msg.media_id == "media_123"
        assert dlq_msg.project_id == "proj_456"
        assert dlq_msg.download_url == "https://example.com/file.pdf"
        assert dlq_msg.error_category == "transient"
        assert dlq_msg.retry_count == 3

    def test_parse_download_dlq_message_with_empty_value_raises_error(
        self, download_handler
    ):
        """Test parsing fails with empty message value."""
        record = PipelineMessage(
            topic="claimx-downloads-dlq",
            partition=0,
            offset=123,
            key=b"evt_789",
            value=b"",
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        with pytest.raises(ValueError) as exc_info:
            download_handler.parse_dlq_message(record)

        assert "DLQ message value is empty" in str(exc_info.value)

    def test_parse_download_dlq_message_with_none_value_raises_error(
        self, download_handler
    ):
        """Test parsing fails with None message value."""
        record = PipelineMessage(
            topic="claimx-downloads-dlq",
            partition=0,
            offset=123,
            key=b"evt_789",
            value=None,
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        with pytest.raises(ValueError) as exc_info:
            download_handler.parse_dlq_message(record)

        assert "DLQ message value is empty" in str(exc_info.value)

    def test_parse_download_dlq_message_with_invalid_json_raises_error(
        self, download_handler
    ):
        """Test parsing fails with malformed JSON."""
        record = PipelineMessage(
            topic="claimx-downloads-dlq",
            partition=0,
            offset=123,
            key=b"evt_789",
            value=b"{invalid json}",
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        with pytest.raises(ValueError) as exc_info:
            download_handler.parse_dlq_message(record)

        assert "Invalid JSON in DLQ message" in str(exc_info.value)

    def test_parse_download_dlq_message_with_missing_required_fields_raises_error(
        self, download_handler
    ):
        """Test parsing fails when required fields are missing."""
        incomplete_message = {
            "media_id": "media_123",
            # Missing project_id, download_url, etc.
        }
        json_bytes = json.dumps(incomplete_message).encode("utf-8")
        record = PipelineMessage(
            topic="claimx-downloads-dlq",
            partition=0,
            offset=123,
            key=b"evt_789",
            value=json_bytes,
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        with pytest.raises(ValueError) as exc_info:
            download_handler.parse_dlq_message(record)

        assert "Failed to parse DLQ message" in str(exc_info.value)

    def test_parse_download_dlq_message_with_unicode_content(
        self, download_handler, valid_download_dlq_message_dict
    ):
        """Test parsing handles unicode characters correctly."""
        valid_download_dlq_message_dict["original_task"]["file_name"] = "file_émojis_😀.pdf"
        json_bytes = json.dumps(valid_download_dlq_message_dict).encode("utf-8")
        record = PipelineMessage(
            topic="claimx-downloads-dlq",
            partition=0,
            offset=123,
            key=b"evt_789",
            value=json_bytes,
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        dlq_msg = download_handler.parse_dlq_message(record)

        assert dlq_msg.original_task.file_name == "file_émojis_😀.pdf"


class TestClaimXDLQHandlerParsingEnrichment:
    """Tests for parsing enrichment DLQ messages."""

    @pytest.fixture
    def enrichment_handler(self, mock_config):
        """Create an enrichment DLQ handler for testing."""
        return ClaimXDLQHandler(config=mock_config, dlq_type="enrichment")

    def test_parse_enrichment_dlq_message_success(
        self, enrichment_handler, valid_enrichment_dlq_message_dict
    ):
        """Test successful parsing of enrichment DLQ message."""
        json_bytes = json.dumps(valid_enrichment_dlq_message_dict).encode("utf-8")
        record = PipelineMessage(
            topic="claimx-enrichment-dlq",
            partition=0,
            offset=123,
            key=b"evt_123",
            value=json_bytes,
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        dlq_msg = enrichment_handler.parse_dlq_message(record)

        assert isinstance(dlq_msg, FailedEnrichmentMessage)
        assert dlq_msg.event_id == "evt_123"
        assert dlq_msg.event_type == "PROJECT_CREATED"
        assert dlq_msg.project_id == "proj_456"
        assert dlq_msg.final_error == "API call failed"
        assert dlq_msg.error_category == "transient"
        assert dlq_msg.retry_count == 2

    def test_parse_enrichment_dlq_message_with_empty_value_raises_error(
        self, enrichment_handler
    ):
        """Test parsing fails with empty message value."""
        record = PipelineMessage(
            topic="claimx-enrichment-dlq",
            partition=0,
            offset=123,
            key=b"evt_123",
            value=b"",
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        with pytest.raises(ValueError) as exc_info:
            enrichment_handler.parse_dlq_message(record)

        assert "DLQ message value is empty" in str(exc_info.value)

    def test_parse_enrichment_dlq_message_with_invalid_json_raises_error(
        self, enrichment_handler
    ):
        """Test parsing fails with malformed JSON."""
        record = PipelineMessage(
            topic="claimx-enrichment-dlq",
            partition=0,
            offset=123,
            key=b"evt_123",
            value=b"not json",
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        with pytest.raises(ValueError) as exc_info:
            enrichment_handler.parse_dlq_message(record)

        assert "Invalid JSON in DLQ message" in str(exc_info.value)

    def test_parse_enrichment_dlq_message_with_missing_required_fields_raises_error(
        self, enrichment_handler
    ):
        """Test parsing fails when required fields are missing."""
        incomplete_message = {
            "event_id": "evt_123",
            # Missing event_type, project_id, etc.
        }
        json_bytes = json.dumps(incomplete_message).encode("utf-8")
        record = PipelineMessage(
            topic="claimx-enrichment-dlq",
            partition=0,
            offset=123,
            key=b"evt_123",
            value=json_bytes,
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        with pytest.raises(ValueError) as exc_info:
            enrichment_handler.parse_dlq_message(record)

        assert "Failed to parse DLQ message" in str(exc_info.value)


class TestClaimXDLQHandlerReplayDownload:
    """Tests for replaying download DLQ messages."""

    @pytest.fixture
    def download_handler(self, mock_config):
        """Create a download DLQ handler with mocked producer."""
        handler = ClaimXDLQHandler(config=mock_config, dlq_type="download")
        handler._producer = AsyncMock()
        handler._producer.is_started = True
        return handler

    @pytest.mark.asyncio
    async def test_replay_download_message_success(
        self, download_handler, valid_download_dlq_message_dict
    ):
        """Test successful replay of download DLQ message."""
        json_bytes = json.dumps(valid_download_dlq_message_dict).encode("utf-8")
        record = PipelineMessage(
            topic="claimx-downloads-dlq",
            partition=2,
            offset=456,
            key=b"evt_789",
            value=json_bytes,
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        await download_handler.replay_message(record)

        # Verify producer.send was called once
        download_handler._producer.send.assert_called_once()

        # Extract the call arguments
        call_args = download_handler._producer.send.call_args

        # Verify the replayed task
        replayed_task = call_args.kwargs["value"]
        assert isinstance(replayed_task, ClaimXDownloadTask)
        assert replayed_task.media_id == "media_123"
        assert replayed_task.project_id == "proj_456"
        assert replayed_task.retry_count == 0  # Reset to 0
        assert replayed_task.metadata["replayed_from_dlq"] is True
        assert replayed_task.metadata["dlq_offset"] == 456
        assert replayed_task.metadata["dlq_partition"] == 2

        # Verify message key uses source_event_id
        assert call_args.kwargs["key"] == "evt_789"

        # Verify headers
        headers = call_args.kwargs["headers"]
        assert headers["source_event_id"] == "evt_789"
        assert headers["replayed_from_dlq"] == "true"

    @pytest.mark.asyncio
    async def test_replay_download_message_preserves_original_metadata(
        self, download_handler, valid_download_dlq_message_dict
    ):
        """Test replay preserves original metadata fields."""
        valid_download_dlq_message_dict["original_task"]["metadata"] = {
            "last_error": "Connection timeout",
            "error_category": "transient",
            "custom_field": "custom_value",
        }
        json_bytes = json.dumps(valid_download_dlq_message_dict).encode("utf-8")
        record = PipelineMessage(
            topic="claimx-downloads-dlq",
            partition=0,
            offset=123,
            key=b"evt_789",
            value=json_bytes,
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        await download_handler.replay_message(record)

        # Verify original metadata is preserved
        call_args = download_handler._producer.send.call_args
        replayed_task = call_args.kwargs["value"]
        assert replayed_task.metadata["last_error"] == "Connection timeout"
        assert replayed_task.metadata["error_category"] == "transient"
        assert replayed_task.metadata["custom_field"] == "custom_value"
        assert replayed_task.metadata["replayed_from_dlq"] is True

    @pytest.mark.asyncio
    async def test_replay_download_message_with_none_metadata(
        self, download_handler, valid_download_dlq_message_dict
    ):
        """Test replay handles None metadata correctly."""
        valid_download_dlq_message_dict["original_task"]["metadata"] = None
        json_bytes = json.dumps(valid_download_dlq_message_dict).encode("utf-8")
        record = PipelineMessage(
            topic="claimx-downloads-dlq",
            partition=0,
            offset=123,
            key=b"evt_789",
            value=json_bytes,
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        await download_handler.replay_message(record)

        # Verify metadata is created with replay fields
        call_args = download_handler._producer.send.call_args
        replayed_task = call_args.kwargs["value"]
        assert replayed_task.metadata["replayed_from_dlq"] is True
        assert replayed_task.metadata["dlq_offset"] == 123
        assert replayed_task.metadata["dlq_partition"] == 0

    @pytest.mark.asyncio
    async def test_replay_download_message_without_producer_raises_error(
        self, mock_config, valid_download_dlq_message_dict
    ):
        """Test replay fails when producer not started."""
        handler = ClaimXDLQHandler(config=mock_config, dlq_type="download")
        # Producer not started
        json_bytes = json.dumps(valid_download_dlq_message_dict).encode("utf-8")
        record = PipelineMessage(
            topic="claimx-downloads-dlq",
            partition=0,
            offset=123,
            key=b"evt_789",
            value=json_bytes,
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        with pytest.raises(RuntimeError) as exc_info:
            await handler.replay_message(record)

        assert "Producer not started" in str(exc_info.value)
        assert "Call start() first" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_replay_download_message_with_invalid_message_raises_error(
        self, download_handler
    ):
        """Test replay fails with invalid DLQ message."""
        record = PipelineMessage(
            topic="claimx-downloads-dlq",
            partition=0,
            offset=123,
            key=b"evt_789",
            value=b"invalid json",
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        with pytest.raises(ValueError):
            await download_handler.replay_message(record)

        # Verify producer.send was NOT called
        download_handler._producer.send.assert_not_called()


class TestClaimXDLQHandlerReplayEnrichment:
    """Tests for replaying enrichment DLQ messages."""

    @pytest.fixture
    def enrichment_handler(self, mock_config):
        """Create an enrichment DLQ handler with mocked producer."""
        handler = ClaimXDLQHandler(config=mock_config, dlq_type="enrichment")
        handler._producer = AsyncMock()
        handler._producer.is_started = True
        return handler

    @pytest.mark.asyncio
    async def test_replay_enrichment_message_success(
        self, enrichment_handler, valid_enrichment_dlq_message_dict
    ):
        """Test successful replay of enrichment DLQ message."""
        json_bytes = json.dumps(valid_enrichment_dlq_message_dict).encode("utf-8")
        record = PipelineMessage(
            topic="claimx-enrichment-dlq",
            partition=1,
            offset=789,
            key=b"evt_123",
            value=json_bytes,
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        await enrichment_handler.replay_message(record)

        # Verify producer.send was called once
        enrichment_handler._producer.send.assert_called_once()

        # Extract the call arguments
        call_args = enrichment_handler._producer.send.call_args

        # Verify the replayed task
        replayed_task = call_args.kwargs["value"]
        assert isinstance(replayed_task, ClaimXEnrichmentTask)
        assert replayed_task.event_id == "evt_123"
        assert replayed_task.event_type == "PROJECT_CREATED"
        assert replayed_task.project_id == "proj_456"
        assert replayed_task.retry_count == 0  # Reset to 0
        assert replayed_task.metadata["replayed_from_dlq"] is True
        assert replayed_task.metadata["dlq_offset"] == 789
        assert replayed_task.metadata["dlq_partition"] == 1

        # Verify message key uses event_id
        assert call_args.kwargs["key"] == "evt_123"

        # Verify headers
        headers = call_args.kwargs["headers"]
        assert headers["event_id"] == "evt_123"
        assert headers["replayed_from_dlq"] == "true"

    @pytest.mark.asyncio
    async def test_replay_enrichment_message_preserves_original_metadata(
        self, enrichment_handler, valid_enrichment_dlq_message_dict
    ):
        """Test replay preserves original metadata fields."""
        valid_enrichment_dlq_message_dict["original_task"]["metadata"] = {
            "last_error": "API timeout",
            "error_category": "transient",
            "custom_field": "custom_value",
        }
        json_bytes = json.dumps(valid_enrichment_dlq_message_dict).encode("utf-8")
        record = PipelineMessage(
            topic="claimx-enrichment-dlq",
            partition=0,
            offset=123,
            key=b"evt_123",
            value=json_bytes,
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        await enrichment_handler.replay_message(record)

        # Verify original metadata is preserved
        call_args = enrichment_handler._producer.send.call_args
        replayed_task = call_args.kwargs["value"]
        assert replayed_task.metadata["last_error"] == "API timeout"
        assert replayed_task.metadata["error_category"] == "transient"
        assert replayed_task.metadata["custom_field"] == "custom_value"
        assert replayed_task.metadata["replayed_from_dlq"] is True

    @pytest.mark.asyncio
    async def test_replay_enrichment_message_with_none_metadata(
        self, enrichment_handler, valid_enrichment_dlq_message_dict
    ):
        """Test replay handles None metadata correctly."""
        valid_enrichment_dlq_message_dict["original_task"]["metadata"] = None
        json_bytes = json.dumps(valid_enrichment_dlq_message_dict).encode("utf-8")
        record = PipelineMessage(
            topic="claimx-enrichment-dlq",
            partition=0,
            offset=123,
            key=b"evt_123",
            value=json_bytes,
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        await enrichment_handler.replay_message(record)

        # Verify metadata is created with replay fields
        call_args = enrichment_handler._producer.send.call_args
        replayed_task = call_args.kwargs["value"]
        assert replayed_task.metadata["replayed_from_dlq"] is True
        assert replayed_task.metadata["dlq_offset"] == 123
        assert replayed_task.metadata["dlq_partition"] == 0

    @pytest.mark.asyncio
    async def test_replay_enrichment_message_without_producer_raises_error(
        self, mock_config, valid_enrichment_dlq_message_dict
    ):
        """Test replay fails when producer not started."""
        handler = ClaimXDLQHandler(config=mock_config, dlq_type="enrichment")
        # Producer not started
        json_bytes = json.dumps(valid_enrichment_dlq_message_dict).encode("utf-8")
        record = PipelineMessage(
            topic="claimx-enrichment-dlq",
            partition=0,
            offset=123,
            key=b"evt_123",
            value=json_bytes,
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        with pytest.raises(RuntimeError) as exc_info:
            await handler.replay_message(record)

        assert "Producer not started" in str(exc_info.value)
        assert "Call start() first" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_replay_enrichment_message_with_invalid_message_raises_error(
        self, enrichment_handler
    ):
        """Test replay fails with invalid DLQ message."""
        record = PipelineMessage(
            topic="claimx-enrichment-dlq",
            partition=0,
            offset=123,
            key=b"evt_123",
            value=b"{malformed",
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        with pytest.raises(ValueError):
            await enrichment_handler.replay_message(record)

        # Verify producer.send was NOT called
        enrichment_handler._producer.send.assert_not_called()


class TestClaimXDLQHandlerAcknowledgment:
    """Tests for acknowledging DLQ messages."""

    @pytest.fixture
    def download_handler_with_consumer(self, mock_config):
        """Create a download DLQ handler with mocked consumer."""
        handler = ClaimXDLQHandler(config=mock_config, dlq_type="download")
        handler._consumer = AsyncMock()
        handler._consumer._consumer = AsyncMock()
        handler._consumer._consumer.commit = AsyncMock()
        return handler

    @pytest.fixture
    def enrichment_handler_with_consumer(self, mock_config):
        """Create an enrichment DLQ handler with mocked consumer."""
        handler = ClaimXDLQHandler(config=mock_config, dlq_type="enrichment")
        handler._consumer = AsyncMock()
        handler._consumer._consumer = AsyncMock()
        handler._consumer._consumer.commit = AsyncMock()
        return handler

    @pytest.mark.asyncio
    async def test_acknowledge_download_message_success(
        self, download_handler_with_consumer, valid_download_dlq_message_dict
    ):
        """Test successful acknowledgment of download DLQ message."""
        json_bytes = json.dumps(valid_download_dlq_message_dict).encode("utf-8")
        record = PipelineMessage(
            topic="claimx-downloads-dlq",
            partition=0,
            offset=123,
            key=b"evt_789",
            value=json_bytes,
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        await download_handler_with_consumer.acknowledge_message(record)

        # Verify commit was called
        download_handler_with_consumer._consumer._consumer.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_acknowledge_enrichment_message_success(
        self, enrichment_handler_with_consumer, valid_enrichment_dlq_message_dict
    ):
        """Test successful acknowledgment of enrichment DLQ message."""
        json_bytes = json.dumps(valid_enrichment_dlq_message_dict).encode("utf-8")
        record = PipelineMessage(
            topic="claimx-enrichment-dlq",
            partition=0,
            offset=123,
            key=b"evt_123",
            value=json_bytes,
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        await enrichment_handler_with_consumer.acknowledge_message(record)

        # Verify commit was called
        enrichment_handler_with_consumer._consumer._consumer.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_acknowledge_download_message_without_consumer_raises_error(
        self, mock_config, valid_download_dlq_message_dict
    ):
        """Test acknowledge fails when consumer not started."""
        handler = ClaimXDLQHandler(config=mock_config, dlq_type="download")
        # Consumer not started
        json_bytes = json.dumps(valid_download_dlq_message_dict).encode("utf-8")
        record = PipelineMessage(
            topic="claimx-downloads-dlq",
            partition=0,
            offset=123,
            key=b"evt_789",
            value=json_bytes,
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        with pytest.raises(RuntimeError) as exc_info:
            await handler.acknowledge_message(record)

        assert "Consumer not started" in str(exc_info.value)
        assert "Call start() first" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_acknowledge_enrichment_message_without_consumer_raises_error(
        self, mock_config, valid_enrichment_dlq_message_dict
    ):
        """Test acknowledge fails when consumer not started."""
        handler = ClaimXDLQHandler(config=mock_config, dlq_type="enrichment")
        # Consumer not started
        json_bytes = json.dumps(valid_enrichment_dlq_message_dict).encode("utf-8")
        record = PipelineMessage(
            topic="claimx-enrichment-dlq",
            partition=0,
            offset=123,
            key=b"evt_123",
            value=json_bytes,
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        with pytest.raises(RuntimeError) as exc_info:
            await handler.acknowledge_message(record)

        assert "Consumer not started" in str(exc_info.value)
        assert "Call start() first" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_acknowledge_download_message_with_invalid_message_raises_error(
        self, download_handler_with_consumer
    ):
        """Test acknowledge fails with invalid DLQ message."""
        record = PipelineMessage(
            topic="claimx-downloads-dlq",
            partition=0,
            offset=123,
            key=b"evt_789",
            value=b"invalid",
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        with pytest.raises(ValueError):
            await download_handler_with_consumer.acknowledge_message(record)

        # Verify commit was NOT called
        download_handler_with_consumer._consumer._consumer.commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_acknowledge_enrichment_message_with_invalid_message_raises_error(
        self, enrichment_handler_with_consumer
    ):
        """Test acknowledge fails with invalid DLQ message."""
        record = PipelineMessage(
            topic="claimx-enrichment-dlq",
            partition=0,
            offset=123,
            key=b"evt_123",
            value=b"not json",
            headers={},
            timestamp=datetime.now(timezone.utc),
        )

        with pytest.raises(ValueError):
            await enrichment_handler_with_consumer.acknowledge_message(record)

        # Verify commit was NOT called
        enrichment_handler_with_consumer._consumer._consumer.commit.assert_not_called()


class TestClaimXDLQHandlerStartStop:
    """Tests for starting and stopping the DLQ handler."""

    @pytest.mark.asyncio
    @patch("pipeline.claimx.dlq.handler.MessageProducer")
    async def test_start_creates_producer(self, mock_producer_class, mock_config):
        """Test start() initializes producer."""
        # Create mock producer instance
        mock_producer_instance = AsyncMock()
        mock_producer_instance.start = AsyncMock()
        mock_producer_instance.is_started = True
        mock_producer_class.return_value = mock_producer_instance

        handler = ClaimXDLQHandler(config=mock_config, dlq_type="download")

        await handler.start()

        # Verify producer was created with correct parameters
        mock_producer_class.assert_called_once_with(
            config=mock_config,
            domain="claimx",
            worker_name="download_dlq_handler",
        )

        # Verify producer.start was called
        mock_producer_instance.start.assert_called_once()

        # Verify producer was set on handler
        assert handler._producer is mock_producer_instance

    @pytest.mark.asyncio
    async def test_stop_cleans_up_resources(self, mock_config):
        """Test stop() properly cleans up consumer and producer."""
        handler = ClaimXDLQHandler(config=mock_config, dlq_type="download")

        # Set up mock resources
        mock_consumer = AsyncMock()
        mock_consumer.stop = AsyncMock()
        mock_producer = AsyncMock()
        mock_producer.stop = AsyncMock()

        handler._consumer = mock_consumer
        handler._producer = mock_producer

        await handler.stop()

        # Verify cleanup was called
        mock_consumer.stop.assert_called_once()
        mock_producer.stop.assert_called_once()

        # Verify resources are set to None
        assert handler._consumer is None
        assert handler._producer is None

    @pytest.mark.asyncio
    async def test_stop_without_resources_is_safe(self, mock_config):
        """Test stop() can be called safely when resources not initialized."""
        handler = ClaimXDLQHandler(config=mock_config, dlq_type="download")

        # No resources set
        assert handler._consumer is None
        assert handler._producer is None

        # Should not raise
        await handler.stop()

        assert handler._consumer is None
        assert handler._producer is None
