"""
Unit tests for ClaimXEntityDeltaWorker.

Tests entity row consumption, batch processing, and Delta Lake writes.
"""

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiokafka.structs import ConsumerRecord

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage
from kafka_pipeline.claimx.workers.entity_delta_worker import ClaimXEntityDeltaWorker


@pytest.fixture
def kafka_config():
    """Create test Kafka configuration."""
    return KafkaConfig(
        bootstrap_servers="localhost:9092",
        security_protocol="PLAINTEXT",
        claimx={
            "topics": {
                "events": "test.claimx.events.raw",
                "enrichment_pending": "test.claimx.enrichment.pending",
                "downloads_pending": "test.claimx.downloads.pending",
                "downloads_cached": "test.claimx.downloads.cached",
                "downloads_results": "test.claimx.downloads.results",
                "entities_rows": "test.claimx.entities.rows",
            },
            "consumer_group_prefix": "test-claimx",
            "retry_delays": [300, 600, 1200],
            "entity_delta_writer": {
                "consumer": {"max_poll_records": 100},
                "processing": {
                    "batch_size": 50,
                    "batch_timeout_seconds": 30.0,
                    "max_retries": 3,
                },
            },
        },
        onelake_base_path="abfss://test@onelake.dfs.fabric.microsoft.com/lakehouse",
    )


@pytest.fixture
def sample_entity_rows():
    """Create sample EntityRowsMessage for testing."""
    return EntityRowsMessage(
        projects=[
            {
                "project_id": "proj-12345",
                "project_name": "Insurance Claim 2024",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "status": "active",
            }
        ],
        contacts=[
            {
                "contact_id": "contact-111",
                "project_id": "proj-12345",
                "name": "John Doe",
                "email": "john@example.com",
            }
        ],
        media=[
            {
                "media_id": "media-222",
                "project_id": "proj-12345",
                "file_name": "photo.jpg",
                "file_type": "jpg",
                "file_size": 1024000,
            }
        ],
        tasks=[],
        task_templates=[],
        external_links=[],
        video_collab=[],
    )


@pytest.fixture
def sample_consumer_record(sample_entity_rows):
    """Create sample ConsumerRecord with EntityRowsMessage."""
    return ConsumerRecord(
        topic="test.claimx.entities.rows",
        partition=0,
        offset=10,
        timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
        timestamp_type=0,
        key=b"proj-12345",
        value=sample_entity_rows.model_dump_json().encode("utf-8"),
        headers=[],
        checksum=None,
        serialized_key_size=10,
        serialized_value_size=len(sample_entity_rows.model_dump_json()),
    )


@pytest.mark.asyncio
class TestClaimXEntityDeltaWorker:
    """Test suite for ClaimXEntityDeltaWorker."""

    async def test_initialization(self, kafka_config):
        """Test worker initialization with correct configuration."""
        with patch(
            "kafka_pipeline.claimx.workers.entity_delta_worker.ClaimXEntityWriter"
        ) as mock_writer:
            mock_writer.return_value = MagicMock()

            worker = ClaimXEntityDeltaWorker(
                kafka_config,
                projects_table_path="/tmp/tables/projects",
                contacts_table_path="/tmp/tables/contacts",
                media_table_path="/tmp/tables/media",
                tasks_table_path="/tmp/tables/tasks",
                task_templates_table_path="/tmp/tables/task_templates",
                external_links_table_path="/tmp/tables/external_links",
                video_collab_table_path="/tmp/tables/video_collab",
            )

            assert worker.domain == "claimx"
            assert worker.batch_size == 50
            assert worker.batch_timeout_seconds == 30.0
            assert worker.max_retries == 3
            assert worker.topics == ["test.claimx.entities.rows"]

    async def test_initialization_default_topic(self, kafka_config):
        """Test worker uses config topic if not provided."""
        with patch(
            "kafka_pipeline.claimx.workers.entity_delta_worker.ClaimXEntityWriter"
        ) as mock_writer:
            mock_writer.return_value = MagicMock()

            worker = ClaimXEntityDeltaWorker(kafka_config)

            assert "test.claimx.entities.rows" in worker.topics

    async def test_handle_message_adds_to_batch(
        self, kafka_config, sample_consumer_record
    ):
        """Test that messages are added to batch."""
        with patch(
            "kafka_pipeline.claimx.workers.entity_delta_worker.ClaimXEntityWriter"
        ) as mock_writer:
            mock_writer.return_value = MagicMock()

            worker = ClaimXEntityDeltaWorker(kafka_config)
            worker._batch = []
            worker._batch_lock = asyncio.Lock()

            await worker._handle_message(sample_consumer_record)

            assert len(worker._batch) == 1
            assert worker._batch[0].projects[0]["project_id"] == "proj-12345"

    async def test_handle_message_triggers_flush_on_batch_size(
        self, kafka_config, sample_entity_rows
    ):
        """Test that flush is triggered when batch size is reached."""
        with patch(
            "kafka_pipeline.claimx.workers.entity_delta_worker.ClaimXEntityWriter"
        ) as mock_writer:
            mock_writer.return_value = MagicMock()

            worker = ClaimXEntityDeltaWorker(kafka_config)
            worker._batch = []
            worker._batch_lock = asyncio.Lock()
            worker.batch_size = 3  # Small batch for testing
            worker._flush_batch = AsyncMock()
            worker._reset_batch_timer = MagicMock()

            # Add messages up to batch size
            for i in range(3):
                record = ConsumerRecord(
                    topic="test.claimx.entities.rows",
                    partition=0,
                    offset=i,
                    timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
                    timestamp_type=0,
                    key=f"proj-{i}".encode("utf-8"),
                    value=sample_entity_rows.model_dump_json().encode("utf-8"),
                    headers=[],
                    checksum=None,
                    serialized_key_size=6,
                    serialized_value_size=len(sample_entity_rows.model_dump_json()),
                )
                await worker._handle_message(record)

            # Verify flush was triggered
            worker._flush_batch.assert_called_once()
            worker._reset_batch_timer.assert_called()

    async def test_handle_message_invalid_json(self, kafka_config):
        """Test handling invalid JSON gracefully."""
        with patch(
            "kafka_pipeline.claimx.workers.entity_delta_worker.ClaimXEntityWriter"
        ) as mock_writer:
            mock_writer.return_value = MagicMock()

            worker = ClaimXEntityDeltaWorker(kafka_config)
            worker._batch = []
            worker._batch_lock = asyncio.Lock()

            invalid_record = ConsumerRecord(
                topic="test.claimx.entities.rows",
                partition=0,
                offset=10,
                timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
                timestamp_type=0,
                key=b"proj-123",
                value=b"invalid json{{{",
                headers=[],
                checksum=None,
                serialized_key_size=8,
                serialized_value_size=16,
            )

            # Should not raise, just log error
            await worker._handle_message(invalid_record)

            # Batch should still be empty (invalid message not added)
            assert len(worker._batch) == 0

    async def test_flush_batch_writes_to_delta(self, kafka_config, sample_entity_rows):
        """Test batch flush writes to Delta Lake."""
        with patch(
            "kafka_pipeline.claimx.workers.entity_delta_worker.ClaimXEntityWriter"
        ) as mock_writer_class:
            mock_writer = MagicMock()
            mock_writer.write_all = AsyncMock(
                return_value={
                    "projects": 1,
                    "contacts": 1,
                    "media": 1,
                }
            )
            mock_writer_class.return_value = mock_writer

            worker = ClaimXEntityDeltaWorker(kafka_config)
            worker._batch = [sample_entity_rows]
            worker._batch_lock = asyncio.Lock()
            worker.commit = AsyncMock()

            await worker._flush_batch()

            # Verify write_all was called
            mock_writer.write_all.assert_called_once()

            # Verify commit was called
            worker.commit.assert_called_once()

            # Verify batch was cleared
            assert len(worker._batch) == 0

            # Verify counters updated
            assert worker._batches_written == 1
            assert worker._records_succeeded == 3  # 1 project + 1 contact + 1 media

    async def test_flush_batch_empty_batch_noop(self, kafka_config):
        """Test that empty batch flush is a no-op."""
        with patch(
            "kafka_pipeline.claimx.workers.entity_delta_worker.ClaimXEntityWriter"
        ) as mock_writer_class:
            mock_writer = MagicMock()
            mock_writer_class.return_value = mock_writer

            worker = ClaimXEntityDeltaWorker(kafka_config)
            worker._batch = []
            worker._batch_lock = asyncio.Lock()

            await worker._flush_batch()

            # write_all should not be called
            mock_writer.write_all.assert_not_called()

    async def test_flush_batch_empty_entity_rows(self, kafka_config):
        """Test batch with no actual entity data logs warning."""
        with patch(
            "kafka_pipeline.claimx.workers.entity_delta_worker.ClaimXEntityWriter"
        ) as mock_writer_class:
            mock_writer = MagicMock()
            mock_writer_class.return_value = mock_writer

            worker = ClaimXEntityDeltaWorker(kafka_config)
            # Add empty EntityRowsMessage
            worker._batch = [EntityRowsMessage()]
            worker._batch_lock = asyncio.Lock()

            await worker._flush_batch()

            # write_all should not be called for empty entity rows
            mock_writer.write_all.assert_not_called()

            # Batch should be cleared
            assert len(worker._batch) == 0

    async def test_flush_batch_merges_messages(self, kafka_config):
        """Test that multiple messages are merged before write."""
        with patch(
            "kafka_pipeline.claimx.workers.entity_delta_worker.ClaimXEntityWriter"
        ) as mock_writer_class:
            mock_writer = MagicMock()
            mock_writer.write_all = AsyncMock(return_value={"projects": 2, "media": 2})
            mock_writer_class.return_value = mock_writer

            worker = ClaimXEntityDeltaWorker(kafka_config)
            worker._batch_lock = asyncio.Lock()
            worker.commit = AsyncMock()

            # Add two messages with different data
            worker._batch = [
                EntityRowsMessage(
                    projects=[{"project_id": "proj-1"}],
                    media=[{"media_id": "media-1"}],
                ),
                EntityRowsMessage(
                    projects=[{"project_id": "proj-2"}],
                    media=[{"media_id": "media-2"}],
                ),
            ]

            await worker._flush_batch()

            # Verify write_all was called with merged data
            mock_writer.write_all.assert_called_once()
            call_args = mock_writer.write_all.call_args[0][0]
            assert len(call_args.projects) == 2
            assert len(call_args.media) == 2

    async def test_flush_batch_handles_write_error(self, kafka_config, sample_entity_rows):
        """Test batch flush handles write errors gracefully."""
        with patch(
            "kafka_pipeline.claimx.workers.entity_delta_worker.ClaimXEntityWriter"
        ) as mock_writer_class:
            mock_writer = MagicMock()
            mock_writer.write_all = AsyncMock(
                side_effect=Exception("Delta write failed")
            )
            mock_writer_class.return_value = mock_writer

            worker = ClaimXEntityDeltaWorker(kafka_config)
            worker._batch = [sample_entity_rows]
            worker._batch_lock = asyncio.Lock()
            worker.commit = AsyncMock()

            # Should not raise, error is logged
            await worker._flush_batch()

            # Commit should not be called on error
            worker.commit.assert_not_called()

    async def test_periodic_flush(self, kafka_config, sample_entity_rows):
        """Test periodic flush triggers on timeout."""
        with patch(
            "kafka_pipeline.claimx.workers.entity_delta_worker.ClaimXEntityWriter"
        ) as mock_writer_class:
            mock_writer = MagicMock()
            mock_writer.write_all = AsyncMock(return_value={"projects": 1})
            mock_writer_class.return_value = mock_writer

            worker = ClaimXEntityDeltaWorker(kafka_config)
            worker.batch_timeout_seconds = 0.1  # Short timeout for testing
            worker._batch = [sample_entity_rows]
            worker._batch_lock = asyncio.Lock()
            worker.commit = AsyncMock()

            # Start periodic flush task
            task = asyncio.create_task(worker._periodic_flush())

            # Wait for timeout to trigger
            await asyncio.sleep(0.15)

            # Cancel the task
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

            # Verify flush was triggered
            mock_writer.write_all.assert_called()

    async def test_start_initializes_producer_and_retry_handler(self, kafka_config):
        """Test start initializes producer and retry handler."""
        with patch(
            "kafka_pipeline.claimx.workers.entity_delta_worker.ClaimXEntityWriter"
        ) as mock_writer_class, patch(
            "kafka_pipeline.claimx.workers.entity_delta_worker.BaseKafkaProducer"
        ) as mock_producer_class, patch(
            "kafka_pipeline.claimx.workers.entity_delta_worker.DeltaRetryHandler"
        ) as mock_retry_class, patch.object(
            ClaimXEntityDeltaWorker, "_reset_batch_timer"
        ), patch(
            "kafka_pipeline.common.consumer.BaseKafkaConsumer.start", new_callable=AsyncMock
        ):
            mock_writer_class.return_value = MagicMock()
            mock_producer = AsyncMock()
            mock_producer_class.return_value = mock_producer
            mock_retry_class.return_value = MagicMock()

            worker = ClaimXEntityDeltaWorker(kafka_config)

            await worker.start()

            # Verify producer was started
            mock_producer.start.assert_called_once()

            # Verify retry handler was initialized
            mock_retry_class.assert_called_once()

    async def test_stop_flushes_remaining_batch(self, kafka_config, sample_entity_rows):
        """Test stop flushes remaining batch."""
        with patch(
            "kafka_pipeline.claimx.workers.entity_delta_worker.ClaimXEntityWriter"
        ) as mock_writer_class, patch(
            "kafka_pipeline.common.consumer.BaseKafkaConsumer.stop", new_callable=AsyncMock
        ):
            mock_writer = MagicMock()
            mock_writer.write_all = AsyncMock(return_value={"projects": 1})
            mock_writer_class.return_value = mock_writer

            worker = ClaimXEntityDeltaWorker(kafka_config)
            worker._batch = [sample_entity_rows]
            worker._batch_lock = asyncio.Lock()
            worker._batch_timer = None
            worker.producer = AsyncMock()
            worker.commit = AsyncMock()

            await worker.stop()

            # Verify batch was flushed
            mock_writer.write_all.assert_called_once()

            # Verify producer was stopped
            worker.producer.stop.assert_called_once()


@pytest.mark.asyncio
class TestEntityRowsMessageMerge:
    """Test EntityRowsMessage merging functionality."""

    async def test_merge_combines_all_entity_types(self):
        """Test merge combines all entity types."""
        msg1 = EntityRowsMessage(
            projects=[{"project_id": "p1"}],
            contacts=[{"contact_id": "c1"}],
            media=[{"media_id": "m1"}],
        )
        msg2 = EntityRowsMessage(
            projects=[{"project_id": "p2"}],
            tasks=[{"task_id": "t1"}],
            video_collab=[{"collab_id": "v1"}],
        )

        msg1.merge(msg2)

        assert len(msg1.projects) == 2
        assert len(msg1.contacts) == 1
        assert len(msg1.media) == 1
        assert len(msg1.tasks) == 1
        assert len(msg1.video_collab) == 1

    async def test_is_empty(self):
        """Test is_empty check."""
        empty = EntityRowsMessage()
        assert empty.is_empty() is True

        non_empty = EntityRowsMessage(projects=[{"project_id": "p1"}])
        assert non_empty.is_empty() is False

    async def test_row_count(self):
        """Test row_count calculation."""
        msg = EntityRowsMessage(
            projects=[{"project_id": "p1"}, {"project_id": "p2"}],
            contacts=[{"contact_id": "c1"}],
            media=[{"media_id": "m1"}, {"media_id": "m2"}, {"media_id": "m3"}],
        )

        assert msg.row_count() == 6  # 2 + 1 + 3
