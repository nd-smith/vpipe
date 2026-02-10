"""Tests for ClaimX DLQ Management CLI.

Tests the argparse-based CLI commands (list, inspect, replay, purge),
the DLQManager class, and the main() entry point. All Kafka and config
dependencies are mocked.
"""

import argparse
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest

from pipeline.claimx.dlq.cli import (
    DLQManager,
    cmd_inspect,
    cmd_list,
    cmd_purge,
    cmd_replay,
    main,
)

SECURITY_PATCH = "pipeline.common.kafka_config.build_kafka_security_config"


# ============================================================================
# Helpers
# ============================================================================


def _make_kafka_consumer_mock():
    """Build a consumer mock where sync methods stay sync and async methods are async."""
    consumer = Mock()
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()
    consumer.end_offsets = AsyncMock()
    consumer.beginning_offsets = AsyncMock()
    consumer.seek_to_end = AsyncMock()
    consumer.commit = AsyncMock()
    # partitions_for_topic is sync in aiokafka
    consumer.partitions_for_topic = Mock()
    return consumer


def _make_kafka_producer_mock():
    """Build a producer mock with async start/stop/send/flush."""
    producer = Mock()
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    producer.send = AsyncMock()
    producer.flush = AsyncMock()
    return producer


def _make_async_iterator(records):
    """Make a mock object behave as an async iterator over records."""

    class _AsyncIter:
        def __init__(self):
            self._items = list(records)
            self._index = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self._index >= len(self._items):
                raise StopAsyncIteration
            item = self._items[self._index]
            self._index += 1
            return item

    return _AsyncIter()


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def mock_config():
    """Mock MessageConfig for DLQManager."""
    config = Mock()
    config.bootstrap_servers = "localhost:9092"
    config.get_topic = Mock(side_effect=lambda domain, key: f"{domain}.{key}")
    return config


@pytest.fixture
def manager(mock_config):
    """DLQManager with mocked config."""
    return DLQManager(mock_config)


# ============================================================================
# DLQManager initialization
# ============================================================================


class TestDLQManagerInit:
    """Tests for DLQManager initialization."""

    def test_topics_are_set_from_config(self, mock_config):
        mgr = DLQManager(mock_config)

        assert mgr.enrichment_dlq_topic == "claimx.enrichment.dlq"
        assert mgr.enrichment_pending_topic == "claimx.enrichment.pending"
        assert mgr.download_dlq_topic == "claimx.downloads.dlq"
        assert mgr.download_pending_topic == "claimx.downloads.pending"

    def test_config_is_stored(self, mock_config):
        mgr = DLQManager(mock_config)
        assert mgr.config is mock_config


# ============================================================================
# DLQManager._get_consumer_config / _get_producer_config
# ============================================================================


class TestDLQManagerKafkaConfig:
    """Tests for Kafka config builder methods."""

    @patch(SECURITY_PATCH, return_value={"security_protocol": "SASL_SSL"})
    def test_get_consumer_config(self, mock_security, manager):
        config = manager._get_consumer_config()

        assert config["bootstrap_servers"] == "localhost:9092"
        assert config["group_id"] == "claimx-dlq-cli"
        assert config["enable_auto_commit"] is False
        assert config["auto_offset_reset"] == "earliest"
        assert config["security_protocol"] == "SASL_SSL"

    @patch(SECURITY_PATCH, return_value={"security_protocol": "SASL_SSL"})
    def test_get_producer_config(self, mock_security, manager):
        config = manager._get_producer_config()

        assert config["bootstrap_servers"] == "localhost:9092"
        assert config["security_protocol"] == "SASL_SSL"


# ============================================================================
# DLQManager.list_dlq_counts
# ============================================================================


class TestListDLQCounts:
    """Tests for the list_dlq_counts method."""

    @patch(SECURITY_PATCH, return_value={})
    @patch("pipeline.claimx.dlq.cli.AIOKafkaConsumer")
    async def test_list_dlq_counts_with_messages(self, mock_consumer_cls, mock_security, manager):
        consumer = _make_kafka_consumer_mock()
        mock_consumer_cls.return_value = consumer

        consumer.partitions_for_topic.side_effect = lambda t: {0, 1} if "enrichment" in t else {0}
        consumer.end_offsets.side_effect = lambda tps: dict.fromkeys(tps, 100)
        consumer.beginning_offsets.side_effect = lambda tps: dict.fromkeys(tps, 50)

        counts = await manager.list_dlq_counts()

        assert counts["Enrichment DLQ"]["count"] == 100  # 2 partitions * 50
        assert counts["Enrichment DLQ"]["partitions"] == 2
        assert counts["Download DLQ"]["count"] == 50  # 1 partition * 50
        assert counts["Download DLQ"]["partitions"] == 1
        consumer.start.assert_called_once()
        consumer.stop.assert_called_once()

    @patch(SECURITY_PATCH, return_value={})
    @patch("pipeline.claimx.dlq.cli.AIOKafkaConsumer")
    async def test_list_dlq_counts_topic_not_found(self, mock_consumer_cls, mock_security, manager):
        consumer = _make_kafka_consumer_mock()
        mock_consumer_cls.return_value = consumer
        consumer.partitions_for_topic.return_value = None

        counts = await manager.list_dlq_counts()

        assert counts["Enrichment DLQ"]["count"] == 0
        assert counts["Enrichment DLQ"]["error"] == "Topic not found"
        assert counts["Download DLQ"]["count"] == 0
        assert counts["Download DLQ"]["error"] == "Topic not found"

    @patch(SECURITY_PATCH, return_value={})
    @patch("pipeline.claimx.dlq.cli.AIOKafkaConsumer")
    async def test_list_dlq_counts_stops_consumer_on_error(
        self, mock_consumer_cls, mock_security, manager
    ):
        consumer = _make_kafka_consumer_mock()
        mock_consumer_cls.return_value = consumer
        consumer.partitions_for_topic.side_effect = Exception("Kafka error")

        with pytest.raises(Exception, match="Kafka error"):
            await manager.list_dlq_counts()

        consumer.stop.assert_called_once()

    @patch(SECURITY_PATCH, return_value={})
    @patch("pipeline.claimx.dlq.cli.AIOKafkaConsumer")
    async def test_list_dlq_counts_zero_messages(self, mock_consumer_cls, mock_security, manager):
        consumer = _make_kafka_consumer_mock()
        mock_consumer_cls.return_value = consumer
        consumer.partitions_for_topic.return_value = {0}
        consumer.end_offsets.side_effect = lambda tps: dict.fromkeys(tps, 0)
        consumer.beginning_offsets.side_effect = lambda tps: dict.fromkeys(tps, 0)

        counts = await manager.list_dlq_counts()

        assert counts["Enrichment DLQ"]["count"] == 0
        assert counts["Download DLQ"]["count"] == 0


# ============================================================================
# DLQManager.inspect_dlq
# ============================================================================


class TestInspectDLQ:
    """Tests for the inspect_dlq method."""

    async def test_inspect_invalid_type_raises_value_error(self, manager):
        with pytest.raises(ValueError, match="Invalid DLQ type"):
            await manager.inspect_dlq("bogus")

    @patch("pipeline.claimx.dlq.cli.FailedEnrichmentMessage")
    @patch(SECURITY_PATCH, return_value={})
    @patch("pipeline.claimx.dlq.cli.AIOKafkaConsumer")
    async def test_inspect_enrichment_dlq_empty(
        self, mock_consumer_cls, mock_security, mock_schema, manager
    ):
        consumer = _make_async_iterator([])
        consumer.start = AsyncMock()
        consumer.stop = AsyncMock()
        mock_consumer_cls.return_value = consumer

        messages = await manager.inspect_dlq("enrichment", limit=10)

        assert messages == []
        consumer.stop.assert_called_once()

    @patch("pipeline.claimx.dlq.cli.FailedEnrichmentMessage")
    @patch(SECURITY_PATCH, return_value={})
    @patch("pipeline.claimx.dlq.cli.AIOKafkaConsumer")
    async def test_inspect_enrichment_dlq_with_messages(
        self, mock_consumer_cls, mock_security, mock_schema_cls, manager
    ):
        record = Mock()
        record.partition = 0
        record.offset = 42
        record.timestamp = 1700000000000
        record.key = b"evt_123"
        record.value = b'{"some": "json"}'

        mock_parsed = Mock()
        mock_parsed.event_id = "evt_123"
        mock_parsed.event_type = "PROJECT_CREATED"
        mock_parsed.project_id = "proj_456"
        mock_parsed.error_category = "transient"
        mock_parsed.final_error = "API timeout"
        mock_parsed.retry_count = 3
        mock_parsed.failed_at = datetime(2024, 1, 1, tzinfo=UTC)
        mock_schema_cls.model_validate_json.return_value = mock_parsed

        consumer = _make_async_iterator([record])
        consumer.start = AsyncMock()
        consumer.stop = AsyncMock()
        mock_consumer_cls.return_value = consumer

        messages = await manager.inspect_dlq("enrichment", limit=10)

        assert len(messages) == 1
        assert messages[0]["event_id"] == "evt_123"
        assert messages[0]["event_type"] == "PROJECT_CREATED"
        assert messages[0]["project_id"] == "proj_456"
        assert messages[0]["error_category"] == "transient"
        assert messages[0]["final_error"] == "API timeout"
        assert messages[0]["retry_count"] == 3
        assert messages[0]["partition"] == 0
        assert messages[0]["offset"] == 42
        assert messages[0]["key"] == "evt_123"

    @patch("pipeline.claimx.dlq.cli.FailedDownloadMessage")
    @patch(SECURITY_PATCH, return_value={})
    @patch("pipeline.claimx.dlq.cli.AIOKafkaConsumer")
    async def test_inspect_download_dlq_with_messages(
        self, mock_consumer_cls, mock_security, mock_schema_cls, manager
    ):
        record = Mock()
        record.partition = 1
        record.offset = 99
        record.timestamp = 1700000000000
        record.key = b"media_789"
        record.value = b'{"some": "json"}'

        mock_parsed = Mock()
        mock_parsed.media_id = "media_789"
        mock_parsed.project_id = "proj_456"
        mock_parsed.download_url = "https://example.com/" + "x" * 200
        mock_parsed.blob_path = "claimx/proj_456/media/file.pdf"
        mock_parsed.error_category = "permanent"
        mock_parsed.final_error = "404 Not Found"
        mock_parsed.retry_count = 5
        mock_parsed.url_refresh_attempted = True
        mock_parsed.failed_at = datetime(2024, 1, 1, tzinfo=UTC)
        mock_schema_cls.model_validate_json.return_value = mock_parsed

        consumer = _make_async_iterator([record])
        consumer.start = AsyncMock()
        consumer.stop = AsyncMock()
        mock_consumer_cls.return_value = consumer

        messages = await manager.inspect_dlq("download", limit=10)

        assert len(messages) == 1
        assert messages[0]["media_id"] == "media_789"
        assert messages[0]["project_id"] == "proj_456"
        assert messages[0]["download_url"].endswith("...")  # Truncated
        assert messages[0]["blob_path"] == "claimx/proj_456/media/file.pdf"
        assert messages[0]["error_category"] == "permanent"
        assert messages[0]["url_refresh_attempted"] is True

    @patch("pipeline.claimx.dlq.cli.FailedEnrichmentMessage")
    @patch(SECURITY_PATCH, return_value={})
    @patch("pipeline.claimx.dlq.cli.AIOKafkaConsumer")
    async def test_inspect_respects_limit(
        self, mock_consumer_cls, mock_security, mock_schema_cls, manager
    ):
        records = []
        for i in range(5):
            r = Mock()
            r.partition = 0
            r.offset = i
            r.timestamp = 1700000000000
            r.key = f"evt_{i}".encode()
            r.value = b'{"some": "json"}'
            records.append(r)

        mock_parsed = Mock()
        mock_parsed.event_id = "evt_x"
        mock_parsed.event_type = "PROJECT_CREATED"
        mock_parsed.project_id = "proj_456"
        mock_parsed.error_category = "transient"
        mock_parsed.final_error = "err"
        mock_parsed.retry_count = 1
        mock_parsed.failed_at = datetime(2024, 1, 1, tzinfo=UTC)
        mock_schema_cls.model_validate_json.return_value = mock_parsed

        consumer = _make_async_iterator(records)
        consumer.start = AsyncMock()
        consumer.stop = AsyncMock()
        mock_consumer_cls.return_value = consumer

        messages = await manager.inspect_dlq("enrichment", limit=2)

        assert len(messages) == 2

    @patch("pipeline.claimx.dlq.cli.FailedEnrichmentMessage")
    @patch(SECURITY_PATCH, return_value={})
    @patch("pipeline.claimx.dlq.cli.AIOKafkaConsumer")
    async def test_inspect_skips_unparseable_messages(
        self, mock_consumer_cls, mock_security, mock_schema_cls, manager
    ):
        record = Mock()
        record.partition = 0
        record.offset = 0
        record.timestamp = 1700000000000
        record.key = b"evt_bad"
        record.value = b"bad json"

        mock_schema_cls.model_validate_json.side_effect = Exception("parse error")

        consumer = _make_async_iterator([record])
        consumer.start = AsyncMock()
        consumer.stop = AsyncMock()
        mock_consumer_cls.return_value = consumer

        messages = await manager.inspect_dlq("enrichment", limit=10)

        assert messages == []

    @patch("pipeline.claimx.dlq.cli.FailedEnrichmentMessage")
    @patch(SECURITY_PATCH, return_value={})
    @patch("pipeline.claimx.dlq.cli.AIOKafkaConsumer")
    async def test_inspect_decodes_record_key(
        self, mock_consumer_cls, mock_security, mock_schema_cls, manager
    ):
        record = Mock()
        record.partition = 0
        record.offset = 0
        record.timestamp = 1700000000000
        record.key = None  # No key
        record.value = b"{}"

        mock_parsed = Mock()
        mock_parsed.event_id = "evt_1"
        mock_parsed.event_type = "X"
        mock_parsed.project_id = "p1"
        mock_parsed.error_category = "transient"
        mock_parsed.final_error = "err"
        mock_parsed.retry_count = 0
        mock_parsed.failed_at = datetime(2024, 1, 1, tzinfo=UTC)
        mock_schema_cls.model_validate_json.return_value = mock_parsed

        consumer = _make_async_iterator([record])
        consumer.start = AsyncMock()
        consumer.stop = AsyncMock()
        mock_consumer_cls.return_value = consumer

        messages = await manager.inspect_dlq("enrichment", limit=10)

        assert messages[0]["key"] is None


# ============================================================================
# DLQManager.replay_messages
# ============================================================================


class TestReplayMessages:
    """Tests for the replay_messages method."""

    async def test_replay_invalid_type_raises_value_error(self, manager):
        with pytest.raises(ValueError, match="Invalid DLQ type"):
            await manager.replay_messages("bogus")

    @patch("pipeline.claimx.dlq.cli.FailedEnrichmentMessage")
    @patch(SECURITY_PATCH, return_value={})
    @patch("pipeline.claimx.dlq.cli.AIOKafkaProducer")
    @patch("pipeline.claimx.dlq.cli.AIOKafkaConsumer")
    async def test_replay_all_enrichment_messages(
        self, mock_consumer_cls, mock_producer_cls, mock_security, mock_schema_cls, manager
    ):
        record = Mock()
        record.partition = 0
        record.offset = 10
        record.value = b'{"some": "json"}'

        mock_original_task = Mock()
        mock_original_task.model_copy.return_value = mock_original_task
        mock_original_task.model_dump_json.return_value = '{"replayed": true}'

        mock_parsed = Mock()
        mock_parsed.event_id = "evt_123"
        mock_parsed.original_task = mock_original_task
        mock_schema_cls.model_validate_json.return_value = mock_parsed

        consumer = _make_async_iterator([record])
        consumer.start = AsyncMock()
        consumer.stop = AsyncMock()
        mock_consumer_cls.return_value = consumer

        producer = _make_kafka_producer_mock()
        mock_producer_cls.return_value = producer

        count = await manager.replay_messages("enrichment", replay_all=True)

        assert count == 1
        producer.send.assert_called_once()
        producer.flush.assert_called_once()
        mock_original_task.model_copy.assert_called_once_with(deep=True)
        assert mock_original_task.retry_count == 0

    @patch("pipeline.claimx.dlq.cli.FailedDownloadMessage")
    @patch(SECURITY_PATCH, return_value={})
    @patch("pipeline.claimx.dlq.cli.AIOKafkaProducer")
    @patch("pipeline.claimx.dlq.cli.AIOKafkaConsumer")
    async def test_replay_download_with_event_id_filter(
        self, mock_consumer_cls, mock_producer_cls, mock_security, mock_schema_cls, manager
    ):
        # Two records: one matching, one not
        record_match = Mock(partition=0, offset=10, value=b"{}")
        record_skip = Mock(partition=0, offset=11, value=b"{}")

        task_match = Mock()
        task_match.model_copy.return_value = task_match
        task_match.model_dump_json.return_value = "{}"

        task_skip = Mock()
        task_skip.model_copy.return_value = task_skip
        task_skip.model_dump_json.return_value = "{}"

        parsed_match = Mock(media_id="media_match", original_task=task_match)
        parsed_skip = Mock(media_id="media_skip", original_task=task_skip)

        mock_schema_cls.model_validate_json.side_effect = [parsed_match, parsed_skip]

        consumer = _make_async_iterator([record_match, record_skip])
        consumer.start = AsyncMock()
        consumer.stop = AsyncMock()
        mock_consumer_cls.return_value = consumer

        producer = _make_kafka_producer_mock()
        mock_producer_cls.return_value = producer

        count = await manager.replay_messages(
            "download", event_ids=["media_match"], replay_all=False
        )

        assert count == 1
        producer.send.assert_called_once()

    @patch("pipeline.claimx.dlq.cli.FailedEnrichmentMessage")
    @patch(SECURITY_PATCH, return_value={})
    @patch("pipeline.claimx.dlq.cli.AIOKafkaProducer")
    @patch("pipeline.claimx.dlq.cli.AIOKafkaConsumer")
    async def test_replay_skips_failed_messages(
        self, mock_consumer_cls, mock_producer_cls, mock_security, mock_schema_cls, manager
    ):
        record = Mock(partition=0, offset=10, value=b"bad")
        mock_schema_cls.model_validate_json.side_effect = Exception("parse error")

        consumer = _make_async_iterator([record])
        consumer.start = AsyncMock()
        consumer.stop = AsyncMock()
        mock_consumer_cls.return_value = consumer

        producer = _make_kafka_producer_mock()
        mock_producer_cls.return_value = producer

        count = await manager.replay_messages("enrichment", replay_all=True)

        assert count == 0
        producer.send.assert_not_called()

    @patch(SECURITY_PATCH, return_value={})
    @patch("pipeline.claimx.dlq.cli.AIOKafkaProducer")
    @patch("pipeline.claimx.dlq.cli.AIOKafkaConsumer")
    async def test_replay_stops_consumer_and_producer_on_error(
        self, mock_consumer_cls, mock_producer_cls, mock_security, manager
    ):
        consumer = _make_kafka_consumer_mock()
        consumer.start.side_effect = Exception("Connection refused")
        mock_consumer_cls.return_value = consumer

        producer = _make_kafka_producer_mock()
        mock_producer_cls.return_value = producer

        with pytest.raises(Exception, match="Connection refused"):
            await manager.replay_messages("enrichment", replay_all=True)

        producer.stop.assert_called_once()
        consumer.stop.assert_called_once()

    @patch("pipeline.claimx.dlq.cli.FailedEnrichmentMessage")
    @patch(SECURITY_PATCH, return_value={})
    @patch("pipeline.claimx.dlq.cli.AIOKafkaProducer")
    @patch("pipeline.claimx.dlq.cli.AIOKafkaConsumer")
    async def test_replay_sends_to_correct_pending_topic(
        self, mock_consumer_cls, mock_producer_cls, mock_security, mock_schema_cls, manager
    ):
        record = Mock(partition=0, offset=10, value=b"{}")

        mock_original_task = Mock()
        mock_original_task.model_copy.return_value = mock_original_task
        mock_original_task.model_dump_json.return_value = "{}"

        mock_parsed = Mock(event_id="evt_123", original_task=mock_original_task)
        mock_schema_cls.model_validate_json.return_value = mock_parsed

        consumer = _make_async_iterator([record])
        consumer.start = AsyncMock()
        consumer.stop = AsyncMock()
        mock_consumer_cls.return_value = consumer

        producer = _make_kafka_producer_mock()
        mock_producer_cls.return_value = producer

        await manager.replay_messages("enrichment", replay_all=True)

        send_call = producer.send.call_args
        assert send_call[0][0] == "claimx.enrichment.pending"

    @patch("pipeline.claimx.dlq.cli.FailedEnrichmentMessage")
    @patch(SECURITY_PATCH, return_value={})
    @patch("pipeline.claimx.dlq.cli.AIOKafkaProducer")
    @patch("pipeline.claimx.dlq.cli.AIOKafkaConsumer")
    async def test_replay_sends_message_key_as_event_id(
        self, mock_consumer_cls, mock_producer_cls, mock_security, mock_schema_cls, manager
    ):
        record = Mock(partition=0, offset=10, value=b"{}")

        mock_original_task = Mock()
        mock_original_task.model_copy.return_value = mock_original_task
        mock_original_task.model_dump_json.return_value = "{}"

        mock_parsed = Mock(event_id="evt_abc", original_task=mock_original_task)
        mock_schema_cls.model_validate_json.return_value = mock_parsed

        consumer = _make_async_iterator([record])
        consumer.start = AsyncMock()
        consumer.stop = AsyncMock()
        mock_consumer_cls.return_value = consumer

        producer = _make_kafka_producer_mock()
        mock_producer_cls.return_value = producer

        await manager.replay_messages("enrichment", replay_all=True)

        send_kwargs = producer.send.call_args
        assert send_kwargs.kwargs["key"] == b"evt_abc"

    @patch("pipeline.claimx.dlq.cli.FailedEnrichmentMessage")
    @patch(SECURITY_PATCH, return_value={})
    @patch("pipeline.claimx.dlq.cli.AIOKafkaProducer")
    @patch("pipeline.claimx.dlq.cli.AIOKafkaConsumer")
    async def test_replay_empty_dlq_returns_zero(
        self, mock_consumer_cls, mock_producer_cls, mock_security, mock_schema_cls, manager
    ):
        consumer = _make_async_iterator([])
        consumer.start = AsyncMock()
        consumer.stop = AsyncMock()
        mock_consumer_cls.return_value = consumer

        producer = _make_kafka_producer_mock()
        mock_producer_cls.return_value = producer

        count = await manager.replay_messages("enrichment", replay_all=True)

        assert count == 0
        producer.send.assert_not_called()


# ============================================================================
# DLQManager.purge_dlq
# ============================================================================


class TestPurgeDLQ:
    """Tests for the purge_dlq method."""

    async def test_purge_invalid_type_raises_value_error(self, manager):
        with pytest.raises(ValueError, match="Invalid DLQ type"):
            await manager.purge_dlq("bogus")

    @patch(SECURITY_PATCH, return_value={})
    @patch("pipeline.claimx.dlq.cli.AIOKafkaConsumer")
    async def test_purge_with_confirm_skips_prompt(self, mock_consumer_cls, mock_security, manager):
        consumer = _make_kafka_consumer_mock()
        consumer.partitions_for_topic.return_value = {0, 1}
        mock_consumer_cls.return_value = consumer

        result = await manager.purge_dlq("enrichment", confirm=True)

        assert result is True
        consumer.commit.assert_called_once()

    @patch("builtins.input", return_value="no")
    async def test_purge_without_confirm_prompts_user_and_cancels(self, mock_input, manager):
        result = await manager.purge_dlq("enrichment", confirm=False)

        assert result is False
        mock_input.assert_called_once()

    @patch("builtins.input", return_value="yes")
    @patch(SECURITY_PATCH, return_value={})
    @patch("pipeline.claimx.dlq.cli.AIOKafkaConsumer")
    async def test_purge_with_user_confirmation_yes(
        self, mock_consumer_cls, mock_security, mock_input, manager
    ):
        consumer = _make_kafka_consumer_mock()
        consumer.partitions_for_topic.return_value = {0}
        mock_consumer_cls.return_value = consumer

        result = await manager.purge_dlq("enrichment", confirm=False)

        assert result is True

    @patch(SECURITY_PATCH, return_value={})
    @patch("pipeline.claimx.dlq.cli.AIOKafkaConsumer")
    async def test_purge_returns_false_when_topic_not_found(
        self, mock_consumer_cls, mock_security, manager
    ):
        consumer = _make_kafka_consumer_mock()
        consumer.partitions_for_topic.return_value = None
        mock_consumer_cls.return_value = consumer

        result = await manager.purge_dlq("download", confirm=True)

        assert result is False

    @patch(SECURITY_PATCH, return_value={})
    @patch("pipeline.claimx.dlq.cli.AIOKafkaConsumer")
    async def test_purge_seeks_to_end_and_commits(self, mock_consumer_cls, mock_security, manager):
        consumer = _make_kafka_consumer_mock()
        consumer.partitions_for_topic.return_value = {0, 1}
        mock_consumer_cls.return_value = consumer

        result = await manager.purge_dlq("download", confirm=True)

        assert result is True
        assert consumer.seek_to_end.call_count == 2
        consumer.commit.assert_called_once()
        consumer.stop.assert_called_once()

    @patch("builtins.input", return_value="y")
    @patch(SECURITY_PATCH, return_value={})
    @patch("pipeline.claimx.dlq.cli.AIOKafkaConsumer")
    async def test_purge_accepts_y_as_confirmation(
        self, mock_consumer_cls, mock_security, mock_input, manager
    ):
        consumer = _make_kafka_consumer_mock()
        consumer.partitions_for_topic.return_value = {0}
        mock_consumer_cls.return_value = consumer

        result = await manager.purge_dlq("enrichment", confirm=False)

        assert result is True

    @patch(SECURITY_PATCH, return_value={})
    @patch("pipeline.claimx.dlq.cli.AIOKafkaConsumer")
    async def test_purge_returns_false_when_partitions_empty(
        self, mock_consumer_cls, mock_security, manager
    ):
        consumer = _make_kafka_consumer_mock()
        consumer.partitions_for_topic.return_value = set()
        mock_consumer_cls.return_value = consumer

        result = await manager.purge_dlq("enrichment", confirm=True)

        # Empty set is falsy, same as None path
        assert result is False


# ============================================================================
# CLI command functions
# ============================================================================


class TestCmdList:
    """Tests for the cmd_list command function."""

    @patch("pipeline.claimx.dlq.cli.DLQManager")
    @patch("pipeline.claimx.dlq.cli.MessageConfig")
    async def test_cmd_list_prints_counts(self, mock_config_cls, mock_manager_cls, capsys):
        mock_config_cls.from_env.return_value = Mock()
        mock_manager = AsyncMock()
        mock_manager.list_dlq_counts.return_value = {
            "Enrichment DLQ": {
                "topic": "claimx.enrichment.dlq",
                "count": 42,
                "partitions": 3,
            },
            "Download DLQ": {
                "topic": "claimx.downloads.dlq",
                "count": 7,
                "partitions": 1,
            },
        }
        mock_manager_cls.return_value = mock_manager

        args = argparse.Namespace()
        exit_code = await cmd_list(args)

        assert exit_code == 0
        output = capsys.readouterr().out
        assert "42" in output
        assert "7" in output

    @patch("pipeline.claimx.dlq.cli.DLQManager")
    @patch("pipeline.claimx.dlq.cli.MessageConfig")
    async def test_cmd_list_prints_errors(self, mock_config_cls, mock_manager_cls, capsys):
        mock_config_cls.from_env.return_value = Mock()
        mock_manager = AsyncMock()
        mock_manager.list_dlq_counts.return_value = {
            "Enrichment DLQ": {
                "topic": "claimx.enrichment.dlq",
                "count": 0,
                "error": "Topic not found",
            },
        }
        mock_manager_cls.return_value = mock_manager

        args = argparse.Namespace()
        exit_code = await cmd_list(args)

        assert exit_code == 0
        output = capsys.readouterr().out
        assert "Topic not found" in output


class TestCmdInspect:
    """Tests for the cmd_inspect command function."""

    @patch("pipeline.claimx.dlq.cli.DLQManager")
    @patch("pipeline.claimx.dlq.cli.MessageConfig")
    async def test_cmd_inspect_with_messages(self, mock_config_cls, mock_manager_cls, capsys):
        mock_config_cls.from_env.return_value = Mock()
        mock_manager = AsyncMock()
        mock_manager.inspect_dlq.return_value = [
            {"event_id": "evt_123", "error_category": "transient"},
        ]
        mock_manager_cls.return_value = mock_manager

        args = argparse.Namespace(dlq_type="enrichment", limit=10)
        exit_code = await cmd_inspect(args)

        assert exit_code == 0
        output = capsys.readouterr().out
        assert "evt_123" in output
        assert "Total messages inspected: 1" in output

    @patch("pipeline.claimx.dlq.cli.DLQManager")
    @patch("pipeline.claimx.dlq.cli.MessageConfig")
    async def test_cmd_inspect_no_messages(self, mock_config_cls, mock_manager_cls, capsys):
        mock_config_cls.from_env.return_value = Mock()
        mock_manager = AsyncMock()
        mock_manager.inspect_dlq.return_value = []
        mock_manager_cls.return_value = mock_manager

        args = argparse.Namespace(dlq_type="download", limit=5)
        exit_code = await cmd_inspect(args)

        assert exit_code == 0
        output = capsys.readouterr().out
        assert "No messages found" in output


class TestCmdReplay:
    """Tests for the cmd_replay command function."""

    @patch("pipeline.claimx.dlq.cli.DLQManager")
    @patch("pipeline.claimx.dlq.cli.MessageConfig")
    async def test_cmd_replay_all(self, mock_config_cls, mock_manager_cls, capsys):
        mock_config_cls.from_env.return_value = Mock()
        mock_manager = AsyncMock()
        mock_manager.replay_messages.return_value = 5
        mock_manager_cls.return_value = mock_manager

        args = argparse.Namespace(dlq_type="enrichment", all=True, event_ids=None)
        exit_code = await cmd_replay(args)

        assert exit_code == 0
        mock_manager.replay_messages.assert_called_once_with(
            dlq_type="enrichment", event_ids=None, replay_all=True
        )
        output = capsys.readouterr().out
        assert "5 message(s)" in output

    @patch("pipeline.claimx.dlq.cli.DLQManager")
    @patch("pipeline.claimx.dlq.cli.MessageConfig")
    async def test_cmd_replay_specific_ids(self, mock_config_cls, mock_manager_cls, capsys):
        mock_config_cls.from_env.return_value = Mock()
        mock_manager = AsyncMock()
        mock_manager.replay_messages.return_value = 2
        mock_manager_cls.return_value = mock_manager

        args = argparse.Namespace(dlq_type="download", all=False, event_ids="media_1,media_2")
        exit_code = await cmd_replay(args)

        assert exit_code == 0
        mock_manager.replay_messages.assert_called_once_with(
            dlq_type="download",
            event_ids=["media_1", "media_2"],
            replay_all=False,
        )

    @patch("pipeline.claimx.dlq.cli.DLQManager")
    @patch("pipeline.claimx.dlq.cli.MessageConfig")
    async def test_cmd_replay_no_ids_and_not_all_returns_error(
        self, mock_config_cls, mock_manager_cls, capsys
    ):
        mock_config_cls.from_env.return_value = Mock()
        mock_manager_cls.return_value = AsyncMock()

        args = argparse.Namespace(dlq_type="enrichment", all=False, event_ids=None)
        exit_code = await cmd_replay(args)

        assert exit_code == 1
        output = capsys.readouterr().out
        assert "Must specify either --all or --event-ids" in output


class TestCmdPurge:
    """Tests for the cmd_purge command function."""

    @patch("pipeline.claimx.dlq.cli.DLQManager")
    @patch("pipeline.claimx.dlq.cli.MessageConfig")
    async def test_cmd_purge_success(self, mock_config_cls, mock_manager_cls, capsys):
        mock_config_cls.from_env.return_value = Mock()
        mock_manager = AsyncMock()
        mock_manager.purge_dlq.return_value = True
        mock_manager_cls.return_value = mock_manager

        args = argparse.Namespace(dlq_type="enrichment", yes=True)
        exit_code = await cmd_purge(args)

        assert exit_code == 0
        output = capsys.readouterr().out
        assert "purged successfully" in output

    @patch("pipeline.claimx.dlq.cli.DLQManager")
    @patch("pipeline.claimx.dlq.cli.MessageConfig")
    async def test_cmd_purge_cancelled(self, mock_config_cls, mock_manager_cls, capsys):
        mock_config_cls.from_env.return_value = Mock()
        mock_manager = AsyncMock()
        mock_manager.purge_dlq.return_value = False
        mock_manager_cls.return_value = mock_manager

        args = argparse.Namespace(dlq_type="download", yes=False)
        exit_code = await cmd_purge(args)

        assert exit_code == 1
        output = capsys.readouterr().out
        assert "cancelled or failed" in output


# ============================================================================
# main() entry point
# ============================================================================


class TestMain:
    """Tests for the main() CLI entry point."""

    @patch("pipeline.claimx.dlq.cli.load_dotenv")
    def test_main_no_command_prints_help(self, mock_dotenv, capsys):
        with patch("sys.argv", ["cli.py"]):
            exit_code = main()

        assert exit_code == 1

    @patch("pipeline.claimx.dlq.cli.load_dotenv")
    @patch("pipeline.claimx.dlq.cli.asyncio.run")
    def test_main_list_command(self, mock_asyncio_run, mock_dotenv):
        mock_asyncio_run.return_value = 0

        with patch("sys.argv", ["cli.py", "list"]):
            exit_code = main()

        assert exit_code == 0
        mock_asyncio_run.assert_called_once()

    @patch("pipeline.claimx.dlq.cli.load_dotenv")
    @patch("pipeline.claimx.dlq.cli.asyncio.run")
    def test_main_inspect_command(self, mock_asyncio_run, mock_dotenv):
        mock_asyncio_run.return_value = 0

        with patch("sys.argv", ["cli.py", "inspect", "enrichment", "--limit", "5"]):
            exit_code = main()

        assert exit_code == 0

    @patch("pipeline.claimx.dlq.cli.load_dotenv")
    @patch("pipeline.claimx.dlq.cli.asyncio.run")
    def test_main_replay_all_command(self, mock_asyncio_run, mock_dotenv):
        mock_asyncio_run.return_value = 0

        with patch("sys.argv", ["cli.py", "replay", "download", "--all"]):
            exit_code = main()

        assert exit_code == 0

    @patch("pipeline.claimx.dlq.cli.load_dotenv")
    @patch("pipeline.claimx.dlq.cli.asyncio.run")
    def test_main_replay_with_ids_command(self, mock_asyncio_run, mock_dotenv):
        mock_asyncio_run.return_value = 0

        with patch("sys.argv", ["cli.py", "replay", "enrichment", "--event-ids", "a,b,c"]):
            exit_code = main()

        assert exit_code == 0

    @patch("pipeline.claimx.dlq.cli.load_dotenv")
    @patch("pipeline.claimx.dlq.cli.asyncio.run")
    def test_main_purge_command_with_yes(self, mock_asyncio_run, mock_dotenv):
        mock_asyncio_run.return_value = 0

        with patch("sys.argv", ["cli.py", "purge", "download", "--yes"]):
            exit_code = main()

        assert exit_code == 0

    @patch("pipeline.claimx.dlq.cli.load_dotenv")
    @patch("pipeline.claimx.dlq.cli.asyncio.run")
    def test_main_keyboard_interrupt(self, mock_asyncio_run, mock_dotenv, capsys):
        mock_asyncio_run.side_effect = KeyboardInterrupt

        with patch("sys.argv", ["cli.py", "list"]):
            exit_code = main()

        assert exit_code == 130

    @patch("pipeline.claimx.dlq.cli.load_dotenv")
    @patch("pipeline.claimx.dlq.cli.asyncio.run")
    def test_main_unexpected_exception(self, mock_asyncio_run, mock_dotenv):
        mock_asyncio_run.side_effect = RuntimeError("boom")

        with patch("sys.argv", ["cli.py", "list"]):
            exit_code = main()

        assert exit_code == 1

    @patch("pipeline.claimx.dlq.cli.load_dotenv")
    def test_main_loads_dotenv(self, mock_dotenv):
        with patch("sys.argv", ["cli.py"]):
            main()

        mock_dotenv.assert_called_once()
