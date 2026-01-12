"""
Unit tests for ResultProcessor.

Tests result consumption, batch accumulation, size-based flushing,
timeout-based flushing, and graceful shutdown.
"""

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest
from aiokafka.structs import ConsumerRecord

from kafka_pipeline.xact.schemas.results import DownloadResultMessage
from kafka_pipeline.xact.workers.result_processor import ResultProcessor


@pytest.fixture
def kafka_config():
    """Create test Kafka configuration using mock."""
    config = MagicMock()
    config.bootstrap_servers = "localhost:9092"
    config.security_protocol = "PLAINTEXT"
    config.sasl_mechanism = "PLAIN"
    config.request_timeout_ms = 120000
    config.metadata_max_age_ms = 300000
    config.connections_max_idle_ms = 540000

    # Configure topics
    def get_topic(domain, topic_key):
        topics = {
            "events": "xact.events.raw",
            "events_ingested": "xact.events.ingested",
            "downloads_pending": "xact.downloads.pending",
            "downloads_cached": "xact.downloads.cached",
            "downloads_results": "xact.downloads.results",
            "dlq": "xact.downloads.dlq",
        }
        return topics.get(topic_key, f"xact.{topic_key}")

    def get_consumer_group(domain, worker_name):
        return f"{domain}-{worker_name}"

    def get_worker_config(domain, worker_name, component):
        return {}

    config.get_topic = MagicMock(side_effect=get_topic)
    config.get_consumer_group = MagicMock(side_effect=get_consumer_group)
    config.get_worker_config = MagicMock(side_effect=get_worker_config)

    return config


@pytest.fixture
def mock_producer():
    """Create mock Kafka producer."""
    producer = AsyncMock()
    producer.send = AsyncMock()
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    return producer


@pytest.fixture
def inventory_table_path():
    """Create test inventory table path."""
    return "abfss://test@storage.dfs.core.windows.net/xact_attachments"


@pytest.fixture
def mock_inventory_writer():
    """Create mock DeltaInventoryWriter."""
    with patch("kafka_pipeline.xact.workers.result_processor.DeltaInventoryWriter") as mock:
        # Mock instance returned when DeltaInventoryWriter() is called
        mock_instance = AsyncMock()
        mock_instance.write_results = AsyncMock(return_value=True)
        mock.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def failed_table_path():
    """Create test failed attachments table path."""
    return "abfss://test@storage.dfs.core.windows.net/xact_attachments_failed"


@pytest.fixture
def mock_failed_writer():
    """Create mock DeltaFailedAttachmentsWriter."""
    with patch("kafka_pipeline.xact.workers.result_processor.DeltaFailedAttachmentsWriter") as mock:
        # Mock instance returned when DeltaFailedAttachmentsWriter() is called
        mock_instance = AsyncMock()
        mock_instance.write_results = AsyncMock(return_value=True)
        mock.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def sample_success_result():
    """Create sample successful DownloadResultMessage."""
    return DownloadResultMessage(
        trace_id="evt-123",
        media_id="media-123",
        attachment_url="https://storage.example.com/file.pdf",
        blob_path="claims/C-456/file.pdf",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="C-456",
        status="completed",
        http_status=200,
        bytes_downloaded=2048576,
        created_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def sample_failed_transient_result():
    """Create sample failed (transient) DownloadResultMessage."""
    return DownloadResultMessage(
        trace_id="evt-456",
        media_id="media-456",
        attachment_url="https://storage.example.com/timeout.pdf",
        blob_path="documentsReceived/C-456/pdf/timeout.pdf",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="C-456",
        status="failed",
        http_status=None,
        bytes_downloaded=0,
        error_message="Connection timeout",
        created_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def sample_failed_permanent_result():
    """Create sample failed (permanent) DownloadResultMessage."""
    return DownloadResultMessage(
        trace_id="evt-789",
        media_id="media-789",
        attachment_url="https://storage.example.com/invalid.exe",
        blob_path="documentsReceived/C-789/exe/invalid.exe",
        status_subtype="documentsReceived",
        file_type="exe",
        assignment_id="C-789",
        status="failed_permanent",
        http_status=403,
        bytes_downloaded=0,
        error_message="File type not allowed",
        created_at=datetime.now(timezone.utc),
    )


def create_consumer_record(result: DownloadResultMessage, offset: int = 0) -> ConsumerRecord:
    """Create ConsumerRecord from DownloadResultMessage."""
    return ConsumerRecord(
        topic="xact.downloads.results",
        partition=0,
        offset=offset,
        timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
        timestamp_type=0,
        key=result.trace_id.encode("utf-8"),
        value=result.model_dump_json().encode("utf-8"),
        headers=[],
        checksum=None,
        serialized_key_size=len(result.trace_id),
        serialized_value_size=len(result.model_dump_json()),
    )


@pytest.mark.asyncio
class TestResultProcessor:
    """Test suite for ResultProcessor."""

    async def test_initialization(
        self, kafka_config, mock_producer, inventory_table_path, mock_inventory_writer
    ):
        """Test processor initialization with correct configuration."""
        processor = ResultProcessor(kafka_config, mock_producer, inventory_table_path)

        assert processor.config == kafka_config
        assert processor.batch_size == 100
        assert processor.batch_timeout_seconds == 5
        assert processor._batch == []
        assert not processor.is_running

    async def test_initialization_custom_batch_config(
        self, kafka_config, mock_producer, inventory_table_path, mock_inventory_writer
    ):
        """Test processor initialization with custom batch configuration."""
        processor = ResultProcessor(
            kafka_config,
            mock_producer,
            inventory_table_path,
            batch_size=50,
            batch_timeout_seconds=10.0,
        )

        assert processor.batch_size == 50
        assert processor.batch_timeout_seconds == 10.0

    async def test_handle_successful_result(
        self, kafka_config, mock_producer, inventory_table_path, mock_inventory_writer, sample_success_result
    ):
        """Test handling successful download result."""
        processor = ResultProcessor(kafka_config, mock_producer, inventory_table_path)
        record = create_consumer_record(sample_success_result)

        await processor._handle_result(record)

        # Verify result added to batch
        assert len(processor._batch) == 1
        assert processor._batch[0].trace_id == "evt-123"
        assert processor._batch[0].status == "completed"

    async def test_filter_failed_transient_result(
        self, kafka_config, mock_producer, inventory_table_path, mock_inventory_writer, sample_failed_transient_result
    ):
        """Test that failed_transient results are filtered out."""
        processor = ResultProcessor(kafka_config, mock_producer, inventory_table_path)
        record = create_consumer_record(sample_failed_transient_result)

        await processor._handle_result(record)

        # Verify result NOT added to batch
        assert len(processor._batch) == 0

    async def test_filter_failed_permanent_result(
        self, kafka_config, mock_producer, inventory_table_path, mock_inventory_writer, sample_failed_permanent_result
    ):
        """Test that failed_permanent results are filtered out."""
        processor = ResultProcessor(kafka_config, mock_producer, inventory_table_path)
        record = create_consumer_record(sample_failed_permanent_result)

        await processor._handle_result(record)

        # Verify result NOT added to batch
        assert len(processor._batch) == 0

    async def test_batch_size_flush(
        self, kafka_config, mock_producer, inventory_table_path, mock_inventory_writer, sample_success_result
    ):
        """Test that batch flushes when size threshold reached."""
        processor = ResultProcessor(kafka_config, mock_producer, inventory_table_path, batch_size=3)

        # Add results up to batch size
        for i in range(3):
            result = DownloadResultMessage(
                trace_id=f"evt-{i}",
                media_id=f"media-{i}",
                attachment_url=f"https://storage.example.com/file{i}.pdf",
                blob_path=f"claims/C-{i}/file{i}.pdf",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id=f"C-{i}",
                status="completed",
                http_status=200,
                bytes_downloaded=1024 * i,
                created_at=datetime.now(timezone.utc),
            )
            record = create_consumer_record(result, offset=i)
            await processor._handle_result(record)

        # Batch should be empty after automatic flush
        assert len(processor._batch) == 0

    async def test_batch_accumulation_below_threshold(
        self, kafka_config, mock_producer, inventory_table_path, mock_inventory_writer, sample_success_result
    ):
        """Test that batch accumulates when below size threshold."""
        processor = ResultProcessor(kafka_config, mock_producer, inventory_table_path, batch_size=10)

        # Add 5 results (below threshold)
        for i in range(5):
            result = DownloadResultMessage(
                trace_id=f"evt-{i}",
                media_id=f"media-{i}",
                attachment_url=f"https://storage.example.com/file{i}.pdf",
                blob_path=f"claims/C-{i}/file{i}.pdf",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id=f"C-{i}",
                status="completed",
                http_status=200,
                bytes_downloaded=1024 * i,
                created_at=datetime.now(timezone.utc),
            )
            record = create_consumer_record(result, offset=i)
            await processor._handle_result(record)

        # Batch should contain 5 results (no flush yet)
        assert len(processor._batch) == 5

    async def test_periodic_flush_timeout(
        self, kafka_config, mock_producer, inventory_table_path, mock_inventory_writer, sample_success_result
    ):
        """Test that periodic flush triggers after timeout."""
        processor = ResultProcessor(kafka_config, mock_producer, inventory_table_path, batch_timeout_seconds=0.5)

        # Add one result to batch
        record = create_consumer_record(sample_success_result)
        await processor._handle_result(record)

        # Verify batch has 1 result
        assert len(processor._batch) == 1

        # Start periodic flush task
        flush_task = asyncio.create_task(processor._periodic_flush())
        processor._running = True

        try:
            # Wait for timeout + buffer
            await asyncio.sleep(1.5)

            # Batch should be flushed
            assert len(processor._batch) == 0
        finally:
            processor._running = False
            flush_task.cancel()
            try:
                await flush_task
            except asyncio.CancelledError:
                pass

    async def test_graceful_shutdown_flushes_pending_batch(
        self, kafka_config, mock_producer, inventory_table_path, mock_inventory_writer, sample_success_result
    ):
        """Test that graceful shutdown flushes pending batch."""
        processor = ResultProcessor(kafka_config, mock_producer, inventory_table_path)

        # Mock the consumer
        mock_consumer = AsyncMock()
        mock_consumer.is_running = False
        processor._consumer = mock_consumer
        processor._running = True

        # Add results to batch
        for i in range(3):
            result = DownloadResultMessage(
                trace_id=f"evt-{i}",
                media_id=f"media-{i}",
                attachment_url=f"https://storage.example.com/file{i}.pdf",
                blob_path=f"claims/C-{i}/file{i}.pdf",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id=f"C-{i}",
                status="completed",
                http_status=200,
                bytes_downloaded=1024 * i,
                created_at=datetime.now(timezone.utc),
            )
            record = create_consumer_record(result, offset=i)
            await processor._handle_result(record)

        # Verify batch has 3 results
        assert len(processor._batch) == 3

        # Stop processor (should flush)
        await processor.stop()

        # Batch should be empty after shutdown flush
        assert len(processor._batch) == 0
        assert not processor.is_running

    async def test_empty_batch_no_flush(
        self, kafka_config, mock_producer, inventory_table_path, mock_inventory_writer
    ):
        """Test that empty batch doesn't trigger flush."""
        processor = ResultProcessor(kafka_config, mock_producer, inventory_table_path)

        # Call flush with empty batch
        async with processor._batch_lock:
            await processor._flush_batch()

        # Should complete without error
        assert len(processor._batch) == 0

    async def test_invalid_message_raises_exception(
        self, kafka_config, mock_producer, inventory_table_path, mock_inventory_writer
    ):
        """Test that invalid message parsing raises exception."""
        processor = ResultProcessor(kafka_config, mock_producer, inventory_table_path)

        # Create invalid consumer record
        invalid_record = ConsumerRecord(
            topic="xact.downloads.results",
            partition=0,
            offset=0,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"evt-invalid",
            value=b'{"invalid": "json structure"}',
            headers=[],
            checksum=None,
            serialized_key_size=11,
            serialized_value_size=30,
        )

        # Should raise validation error
        with pytest.raises(Exception):
            await processor._handle_result(invalid_record)

    async def test_thread_safe_batch_accumulation(
        self, kafka_config, mock_producer, inventory_table_path, mock_inventory_writer
    ):
        """Test that concurrent batch accumulation is thread-safe."""
        processor = ResultProcessor(kafka_config, mock_producer, inventory_table_path, batch_size=1000)

        # Create multiple results
        async def add_result(i):
            result = DownloadResultMessage(
                trace_id=f"evt-{i}",
                media_id=f"media-{i}",
                attachment_url=f"https://storage.example.com/file{i}.pdf",
                blob_path=f"claims/C-{i}/file{i}.pdf",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id=f"C-{i}",
                status="completed",
                http_status=200,
                bytes_downloaded=1024 * i,
                created_at=datetime.now(timezone.utc),
            )
            record = create_consumer_record(result, offset=i)
            await processor._handle_result(record)

        # Add 50 results concurrently
        tasks = [add_result(i) for i in range(50)]
        await asyncio.gather(*tasks)

        # Batch should contain exactly 50 results
        assert len(processor._batch) == 50

    async def test_flush_batch_converts_to_inventory_records(
        self, kafka_config, mock_producer, inventory_table_path, mock_inventory_writer, sample_success_result
    ):
        """Test that flush_batch converts results to inventory record format."""
        processor = ResultProcessor(kafka_config, mock_producer, inventory_table_path)

        # Add result to batch
        record = create_consumer_record(sample_success_result)
        await processor._handle_result(record)

        # Verify batch has 1 result before flush
        assert len(processor._batch) == 1

        # Flush batch
        async with processor._batch_lock:
            await processor._flush_batch()

        # Batch should be empty after flush
        assert len(processor._batch) == 0

    async def test_multiple_flush_cycles(
        self, kafka_config, mock_producer, inventory_table_path, mock_inventory_writer
    ):
        """Test that processor handles multiple flush cycles correctly."""
        processor = ResultProcessor(kafka_config, mock_producer, inventory_table_path, batch_size=2)

        # First batch
        for i in range(2):
            result = DownloadResultMessage(
                trace_id=f"evt-batch1-{i}",
                media_id=f"media-batch1-{i}",
                attachment_url=f"https://storage.example.com/file{i}.pdf",
                blob_path=f"claims/C-{i}/file{i}.pdf",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id=f"C-{i}",
                status="completed",
                http_status=200,
                bytes_downloaded=1024 * i,
                created_at=datetime.now(timezone.utc),
            )
            record = create_consumer_record(result, offset=i)
            await processor._handle_result(record)

        # Batch should be flushed
        assert len(processor._batch) == 0

        # Second batch
        for i in range(2):
            result = DownloadResultMessage(
                trace_id=f"evt-batch2-{i}",
                media_id=f"media-batch2-{i}",
                attachment_url=f"https://storage.example.com/file{i}.pdf",
                blob_path=f"claims/C-{i}/file{i}.pdf",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id=f"C-{i}",
                status="completed",
                http_status=200,
                bytes_downloaded=1024 * i,
                created_at=datetime.now(timezone.utc),
            )
            record = create_consumer_record(result, offset=i + 10)
            await processor._handle_result(record)

        # Second batch should also be flushed
        assert len(processor._batch) == 0

    async def test_is_running_property(
        self, kafka_config, mock_producer, inventory_table_path, mock_inventory_writer
    ):
        """Test is_running property reflects processor state."""
        processor = ResultProcessor(kafka_config, mock_producer, inventory_table_path)

        # Initially not running
        assert not processor.is_running

        # Mock consumer as running
        mock_consumer = MagicMock()
        mock_consumer.is_running = True
        processor._consumer = mock_consumer
        processor._running = True

        assert processor.is_running

        # Stop processor
        processor._running = False
        mock_consumer.is_running = False

        assert not processor.is_running

    async def test_delta_writer_called_on_flush(
        self, kafka_config, mock_producer, inventory_table_path, mock_inventory_writer, sample_success_result
    ):
        """Test that Delta writer is called when batch is flushed."""
        processor = ResultProcessor(kafka_config, mock_producer, inventory_table_path, batch_size=2)

        # Add 2 results to trigger flush
        for i in range(2):
            result = DownloadResultMessage(
                trace_id=f"evt-{i}",
                media_id=f"media-{i}",
                attachment_url=f"https://storage.example.com/file{i}.pdf",
                blob_path=f"claims/C-{i}/file{i}.pdf",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id=f"C-{i}",
                status="completed",
                http_status=200,
                bytes_downloaded=1024 * i,
                created_at=datetime.now(timezone.utc),
            )
            record = create_consumer_record(result, offset=i)
            await processor._handle_result(record)

        # Verify Delta writer was called
        mock_inventory_writer.write_results.assert_called_once()

        # Verify batch was passed to writer
        call_args = mock_inventory_writer.write_results.call_args[0][0]
        assert len(call_args) == 2
        assert call_args[0].trace_id == "evt-0"
        assert call_args[1].trace_id == "evt-1"

    async def test_delta_write_failure_logged(
        self, kafka_config, mock_producer, inventory_table_path, mock_inventory_writer
    ):
        """Test that Delta write failures are logged but don't crash processor."""
        # Configure mock to return False (failure)
        mock_inventory_writer.write_results = AsyncMock(return_value=False)

        processor = ResultProcessor(kafka_config, mock_producer, inventory_table_path, batch_size=1)

        # Add result to trigger flush
        result = DownloadResultMessage(
            trace_id="evt-fail",
            media_id="media-fail",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="claims/C-123/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-123",
            status="completed",
            http_status=200,
            bytes_downloaded=1024,
            created_at=datetime.now(timezone.utc),
        )
        record = create_consumer_record(result)

        # Should not raise exception even though write failed
        await processor._handle_result(record)

        # Verify batch was still cleared
        assert len(processor._batch) == 0

    async def test_duplicate_start_warning(
        self, kafka_config, mock_producer, inventory_table_path, mock_inventory_writer
    ):
        """Test that starting already-running processor logs warning."""
        processor = ResultProcessor(kafka_config, mock_producer, inventory_table_path)

        # Mock as already running
        processor._running = True

        # Calling start again should return immediately
        await processor.start()

        # Should not have created new consumer
        assert processor._running

    async def test_stop_already_stopped_processor(
        self, kafka_config, mock_producer, inventory_table_path, mock_inventory_writer
    ):
        """Test that stopping already-stopped processor is safe."""
        processor = ResultProcessor(kafka_config, mock_producer, inventory_table_path)

        # Processor not running
        assert not processor._running

        # Calling stop should be safe
        await processor.stop()

        # Should still not be running
        assert not processor._running

    async def test_stop_with_error_in_consumer_stop(
        self, kafka_config, mock_producer, inventory_table_path, mock_inventory_writer
    ):
        """Test that errors during consumer stop are propagated."""
        processor = ResultProcessor(kafka_config, mock_producer, inventory_table_path)

        # Mock the consumer to raise exception on stop
        mock_consumer = AsyncMock()
        mock_consumer.is_running = True
        mock_consumer.stop = AsyncMock(side_effect=Exception("Consumer stop failed"))
        processor._consumer = mock_consumer
        processor._running = True

        # Should raise the exception
        with pytest.raises(Exception, match="Consumer stop failed"):
            await processor.stop()

        # Processor should still be marked as not running
        assert not processor._running

    async def test_initialization_with_failed_table_path(
        self, kafka_config, mock_producer, inventory_table_path, failed_table_path,
        mock_inventory_writer, mock_failed_writer
    ):
        """Test that failed writer is initialized when failed_table_path is provided."""
        processor = ResultProcessor(
            kafka_config, mock_producer, inventory_table_path, failed_table_path=failed_table_path
        )

        # Both writers should be initialized
        assert processor._inventory_writer is not None
        assert processor._failed_writer is not None

    async def test_failed_permanent_result_with_writer(
        self, kafka_config, mock_producer, inventory_table_path, failed_table_path,
        mock_inventory_writer, mock_failed_writer, sample_failed_permanent_result
    ):
        """Test that failed_permanent results are added to failed batch when writer is configured."""
        processor = ResultProcessor(
            kafka_config, mock_producer, inventory_table_path, failed_table_path=failed_table_path
        )
        record = create_consumer_record(sample_failed_permanent_result)

        await processor._handle_result(record)

        # Should be in failed batch, not success batch
        assert len(processor._batch) == 0
        assert len(processor._failed_batch) == 1
        assert processor._failed_batch[0].trace_id == sample_failed_permanent_result.trace_id

    async def test_failed_batch_size_flush(
        self, kafka_config, mock_producer, inventory_table_path, failed_table_path,
        mock_inventory_writer, mock_failed_writer
    ):
        """Test that failed batch flushes when size threshold reached."""
        processor = ResultProcessor(
            kafka_config, mock_producer, inventory_table_path, failed_table_path=failed_table_path, batch_size=3
        )

        # Add failed results up to batch size
        for i in range(3):
            result = DownloadResultMessage(
                trace_id=f"evt-fail-{i}",
                media_id=f"media-fail-{i}",
                attachment_url=f"https://storage.example.com/invalid{i}.pdf",
                blob_path=f"documentsReceived/C-fail-{i}/pdf/invalid{i}.pdf",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id=f"C-fail-{i}",
                status="failed_permanent",
                http_status=403,
                bytes_downloaded=0,
                error_message="File type not allowed",
                created_at=datetime.now(timezone.utc),
            )
            record = create_consumer_record(result, offset=i)
            await processor._handle_result(record)

        # Batch should be empty after automatic flush
        assert len(processor._failed_batch) == 0

        # Verify failed writer was called
        mock_failed_writer.write_results.assert_called_once()

    async def test_failed_writer_called_on_flush(
        self, kafka_config, mock_producer, inventory_table_path, failed_table_path,
        mock_inventory_writer, mock_failed_writer
    ):
        """Test that failed writer is called when failed batch is flushed."""
        processor = ResultProcessor(
            kafka_config, mock_producer, inventory_table_path, failed_table_path=failed_table_path, batch_size=2
        )

        # Add 2 failed results to trigger flush
        for i in range(2):
            result = DownloadResultMessage(
                trace_id=f"evt-fail-{i}",
                media_id=f"media-fail-{i}",
                attachment_url=f"https://storage.example.com/invalid{i}.pdf",
                blob_path=f"documentsReceived/C-fail-{i}/pdf/invalid{i}.pdf",
                status_subtype="documentsReceived",
                file_type="pdf",
                assignment_id=f"C-fail-{i}",
                status="failed_permanent",
                http_status=403,
                bytes_downloaded=0,
                error_message=f"Error {i}",
                created_at=datetime.now(timezone.utc),
            )
            record = create_consumer_record(result, offset=i)
            await processor._handle_result(record)

        # Verify failed writer was called
        mock_failed_writer.write_results.assert_called_once()

        # Verify batch was passed to writer
        call_args = mock_failed_writer.write_results.call_args[0][0]
        assert len(call_args) == 2
        assert call_args[0].trace_id == "evt-fail-0"
        assert call_args[1].trace_id == "evt-fail-1"

    async def test_graceful_shutdown_flushes_failed_batch(
        self, kafka_config, mock_producer, inventory_table_path, failed_table_path,
        mock_inventory_writer, mock_failed_writer
    ):
        """Test that graceful shutdown flushes pending failed batch."""
        processor = ResultProcessor(
            kafka_config, mock_producer, inventory_table_path, failed_table_path=failed_table_path, batch_size=100
        )

        # Add a failed result but don't reach batch size
        result = DownloadResultMessage(
            trace_id="evt-fail-shutdown",
            media_id="media-fail-shutdown",
            attachment_url="https://storage.example.com/invalid.pdf",
            blob_path="documentsReceived/C-shutdown/pdf/invalid.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-shutdown",
            status="failed_permanent",
            http_status=403,
            bytes_downloaded=0,
            error_message="File type not allowed",
            created_at=datetime.now(timezone.utc),
        )
        record = create_consumer_record(result)
        await processor._handle_result(record)

        # Verify failed batch has one item
        assert len(processor._failed_batch) == 1

        # Mock consumer for stop
        processor._consumer = AsyncMock()
        processor._running = True

        # Stop processor (should flush failed batch)
        await processor.stop()

        # Verify failed writer was called
        mock_failed_writer.write_results.assert_called_once()

        # Verify batch was passed to writer
        call_args = mock_failed_writer.write_results.call_args[0][0]
        assert len(call_args) == 1
        assert call_args[0].trace_id == "evt-fail-shutdown"

    async def test_separate_batches_for_success_and_failed(
        self, kafka_config, mock_producer, inventory_table_path, failed_table_path,
        mock_inventory_writer, mock_failed_writer
    ):
        """Test that success and failed results use separate batches."""
        processor = ResultProcessor(
            kafka_config, mock_producer, inventory_table_path, failed_table_path=failed_table_path, batch_size=100
        )

        # Add success result
        success_result = DownloadResultMessage(
            trace_id="evt-success",
            media_id="media-success",
            attachment_url="https://storage.example.com/file.pdf",
            blob_path="claims/C-123/file.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-123",
            status="completed",
            http_status=200,
            bytes_downloaded=1024,
            created_at=datetime.now(timezone.utc),
        )
        await processor._handle_result(create_consumer_record(success_result, offset=0))

        # Add failed result
        failed_result = DownloadResultMessage(
            trace_id="evt-failed",
            media_id="media-failed",
            attachment_url="https://storage.example.com/invalid.pdf",
            blob_path="documentsReceived/C-fail/pdf/invalid.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="C-fail",
            status="failed_permanent",
            http_status=403,
            bytes_downloaded=0,
            error_message="File type not allowed",
            created_at=datetime.now(timezone.utc),
        )
        await processor._handle_result(create_consumer_record(failed_result, offset=1))

        # Verify separate batches
        assert len(processor._batch) == 1
        assert processor._batch[0].trace_id == "evt-success"
        assert len(processor._failed_batch) == 1
        assert processor._failed_batch[0].trace_id == "evt-failed"
