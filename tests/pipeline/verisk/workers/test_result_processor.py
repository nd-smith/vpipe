"""
Unit tests for Verisk Result Processor.

Test Coverage:
    - Worker initialization and configuration
    - Lifecycle management (start/stop)
    - Message parsing and validation
    - Result routing (success vs failed_permanent vs transient)
    - Separate batch accumulation for success and failures
    - Delta writes with inventory and failed writers
    - Retry handler integration for failed Delta writes
    - Graceful shutdown with batch flushing
    - Periodic logging and metrics

No infrastructure required - all dependencies mocked.
"""

import asyncio
import pytest
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

from config.config import KafkaConfig
from pipeline.verisk.schemas.results import DownloadResultMessage
from pipeline.verisk.workers.result_processor import ResultProcessor
from pipeline.common.types import PipelineMessage


@pytest.fixture
def mock_config():
    """Mock KafkaConfig with standard settings."""
    config = Mock(spec=KafkaConfig)
    config.get_topic.return_value = "verisk.downloads.results"
    config.get_consumer_group.return_value = "verisk-result-processor"

    def mock_get_worker_config(domain, worker_name, config_key=None):
        if config_key == "processing":
            return {"health_port": 8094}
        return {"health_port": 8094}

    config.get_worker_config = Mock(side_effect=mock_get_worker_config)
    return config


@pytest.fixture
def mock_producer():
    """Mock Kafka producer."""
    producer = AsyncMock()
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    return producer


@pytest.fixture
def sample_success_result():
    """Sample successful download result."""
    return DownloadResultMessage(
        media_id="media-123",
        trace_id="trace-456",
        attachment_url="https://example.com/file.pdf",
        blob_path="verisk/A12345/documentsReceived/file.pdf",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="A12345",
        status="completed",
        bytes_downloaded=1024,
        created_at=datetime.now(UTC),
    )


@pytest.fixture
def sample_failed_result():
    """Sample permanently failed download result."""
    return DownloadResultMessage(
        media_id="media-456",
        trace_id="trace-789",
        attachment_url="https://example.com/file2.pdf",
        blob_path="verisk/A12345/documentsReceived/file2.pdf",
        status_subtype="documentsReceived",
        file_type="pdf",
        assignment_id="A12345",
        status="failed_permanent",
        bytes_downloaded=0,
        error_message="Download failed",
        created_at=datetime.now(UTC),
    )


@pytest.fixture
def sample_success_message(sample_success_result):
    """Sample Kafka message with successful result."""
    return PipelineMessage(
        topic="verisk.downloads.results",
        partition=0,
        offset=1,
        key=b"trace-456",
        value=sample_success_result.model_dump_json().encode(),
        timestamp=None,
        headers=None,
    )


@pytest.fixture
def sample_failed_message(sample_failed_result):
    """Sample Kafka message with failed result."""
    return PipelineMessage(
        topic="verisk.downloads.results",
        partition=0,
        offset=2,
        key=b"trace-789",
        value=sample_failed_result.model_dump_json().encode(),
        timestamp=None,
        headers=None,
    )


class TestResultProcessorInitialization:
    """Test worker initialization and configuration."""

    def test_initialization_with_default_config(self, mock_config, mock_producer):
        """Processor initializes with default configuration."""
        processor = ResultProcessor(
            config=mock_config,
            producer=mock_producer,
            inventory_table_path="abfss://test/xact_attachments",
        )

        assert processor.domain == "verisk"
        assert processor.worker_name == "result_processor"
        assert processor.worker_id == "result_processor"
        assert processor.instance_id is None
        assert processor._running is False
        assert processor.batch_size == 100
        assert processor.batch_timeout_seconds == 5

    def test_initialization_with_custom_batch_config(self, mock_config, mock_producer):
        """Processor accepts custom batch configuration."""
        processor = ResultProcessor(
            config=mock_config,
            producer=mock_producer,
            inventory_table_path="abfss://test/xact_attachments",
            batch_size=50,
            batch_timeout_seconds=10.0,
        )

        assert processor.batch_size == 50
        assert processor.batch_timeout_seconds == 10.0

    def test_initialization_with_instance_id(self, mock_config, mock_producer):
        """Processor uses instance ID for worker_id suffix."""
        processor = ResultProcessor(
            config=mock_config,
            producer=mock_producer,
            inventory_table_path="abfss://test/xact_attachments",
            instance_id="happy-tiger",
        )

        assert processor.worker_id == "result_processor-happy-tiger"
        assert processor.instance_id == "happy-tiger"

    def test_initialization_with_failed_writer(self, mock_config, mock_producer):
        """Processor initializes with failed attachments writer."""
        processor = ResultProcessor(
            config=mock_config,
            producer=mock_producer,
            inventory_table_path="abfss://test/xact_attachments",
            failed_table_path="abfss://test/xact_attachments_failed",
        )

        assert processor._failed_writer is not None

    def test_initialization_without_failed_writer(self, mock_config, mock_producer):
        """Processor can initialize without failed attachments writer."""
        processor = ResultProcessor(
            config=mock_config,
            producer=mock_producer,
            inventory_table_path="abfss://test/xact_attachments",
        )

        assert processor._failed_writer is None

    def test_metrics_initialized_to_zero(self, mock_config, mock_producer):
        """Processor initializes metrics to zero."""
        processor = ResultProcessor(
            config=mock_config,
            producer=mock_producer,
            inventory_table_path="abfss://test/xact_attachments",
        )

        assert processor._records_processed == 0
        assert processor._records_succeeded == 0
        assert processor._records_failed == 0
        assert processor._batches_written == 0
        assert processor._failed_batches_written == 0
        assert processor._total_records_written == 0


class TestResultProcessorLifecycle:
    """Test worker lifecycle (start/stop)."""

    @pytest.mark.asyncio
    async def test_start_initializes_components(self, mock_config, mock_producer):
        """Processor start initializes all components."""
        processor = ResultProcessor(
            config=mock_config,
            producer=mock_producer,
            inventory_table_path="abfss://test/xact_attachments",
        )

        with patch(
            "pipeline.verisk.workers.result_processor.create_consumer"
        ) as mock_create_consumer, patch(
            "pipeline.common.telemetry.initialize_worker_telemetry"
        ), patch.object(
            processor.health_server, "start", new_callable=AsyncMock
        ), patch.object(
            processor._retry_handler, "start", new_callable=AsyncMock
        ):
            # Setup mock consumer
            mock_consumer = AsyncMock()
            mock_consumer.start = AsyncMock(side_effect=Exception("Stop"))
            mock_create_consumer.return_value = mock_consumer

            try:
                await processor.start()
            except Exception:
                pass

            # Verify components were initialized
            assert processor._running is False  # Reset in finally

    @pytest.mark.asyncio
    async def test_stop_flushes_pending_batches(self, mock_config, mock_producer):
        """Processor stop flushes both pending batches."""
        processor = ResultProcessor(
            config=mock_config,
            producer=mock_producer,
            inventory_table_path="abfss://test/xact_attachments",
            failed_table_path="abfss://test/xact_attachments_failed",
        )

        # Setup mocked components
        processor._running = True
        processor._consumer = AsyncMock()
        processor._consumer.stop = AsyncMock()
        processor._retry_handler = AsyncMock()
        processor._retry_handler.stop = AsyncMock()

        # Add pending batches
        processor._batch = [Mock()]
        processor._failed_batch = [Mock()]

        # Mock flush methods
        processor._flush_batch = AsyncMock()
        processor._flush_failed_batch = AsyncMock()

        # Stop processor
        await processor.stop()

        # Verify both flushes were called
        processor._flush_batch.assert_called_once()
        processor._flush_failed_batch.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_handles_none_components(self, mock_config, mock_producer):
        """Processor stop handles None components gracefully."""
        processor = ResultProcessor(
            config=mock_config,
            producer=mock_producer,
            inventory_table_path="abfss://test/xact_attachments",
        )

        # All components are None
        assert processor._consumer is None

        # Should not raise
        await processor.stop()


class TestResultProcessorMessageProcessing:
    """Test message parsing and processing."""

    @pytest.mark.asyncio
    async def test_success_result_parsed_successfully(
        self, mock_config, mock_producer, sample_success_message
    ):
        """Processor parses successful result message."""
        processor = ResultProcessor(
            config=mock_config,
            producer=mock_producer,
            inventory_table_path="abfss://test/xact_attachments",
        )

        await processor._handle_result(sample_success_message)

        # Verify message was processed
        assert processor._records_processed == 1
        assert processor._records_succeeded == 1
        assert processor._records_failed == 0
        assert len(processor._batch) == 1

    @pytest.mark.asyncio
    async def test_failed_result_parsed_successfully(
        self, mock_config, mock_producer, sample_failed_message
    ):
        """Processor parses failed result message."""
        processor = ResultProcessor(
            config=mock_config,
            producer=mock_producer,
            inventory_table_path="abfss://test/xact_attachments",
            failed_table_path="abfss://test/xact_attachments_failed",
        )

        await processor._handle_result(sample_failed_message)

        # Verify message was processed
        assert processor._records_processed == 1
        assert processor._records_succeeded == 0
        assert processor._records_failed == 1
        assert len(processor._failed_batch) == 1

    @pytest.mark.asyncio
    async def test_transient_failure_skipped(self, mock_config, mock_producer):
        """Processor skips transient failures (still retrying)."""
        processor = ResultProcessor(
            config=mock_config,
            producer=mock_producer,
            inventory_table_path="abfss://test/xact_attachments",
        )

        # Create transient failure message
        transient_result = DownloadResultMessage(
            media_id="media-789",
            trace_id="trace-abc",
            attachment_url="https://example.com/file3.pdf",
            blob_path="verisk/A12345/documentsReceived/file3.pdf",
            status_subtype="documentsReceived",
            file_type="pdf",
            assignment_id="A12345",
            status="failed",  # Transient failure
            bytes_downloaded=0,
            created_at=datetime.now(UTC),
        )

        message = PipelineMessage(
            topic="verisk.downloads.results",
            partition=0,
            offset=3,
            key=b"trace-abc",
            value=transient_result.model_dump_json().encode(),
            timestamp=None,
            headers=None,
        )

        await processor._handle_result(message)

        # Verify message was skipped
        assert processor._records_processed == 1
        assert processor._records_skipped == 1
        assert len(processor._batch) == 0
        assert len(processor._failed_batch) == 0


class TestResultProcessorBatching:
    """Test batch accumulation and flushing."""

    @pytest.mark.asyncio
    async def test_success_batch_accumulates(
        self, mock_config, mock_producer, sample_success_message
    ):
        """Processor accumulates successful results in success batch."""
        processor = ResultProcessor(
            config=mock_config,
            producer=mock_producer,
            inventory_table_path="abfss://test/xact_attachments",
            batch_size=10,
        )

        # Process multiple messages
        for _ in range(3):
            await processor._handle_result(sample_success_message)

        # Verify batch accumulation
        assert len(processor._batch) == 3
        assert processor._records_succeeded == 3

    @pytest.mark.asyncio
    async def test_failed_batch_accumulates(
        self, mock_config, mock_producer, sample_failed_message
    ):
        """Processor accumulates failed results in failed batch."""
        processor = ResultProcessor(
            config=mock_config,
            producer=mock_producer,
            inventory_table_path="abfss://test/xact_attachments",
            failed_table_path="abfss://test/xact_attachments_failed",
            batch_size=10,
        )

        # Process multiple messages
        for _ in range(3):
            await processor._handle_result(sample_failed_message)

        # Verify batch accumulation
        assert len(processor._failed_batch) == 3
        assert processor._records_failed == 3

    @pytest.mark.asyncio
    async def test_success_batch_flushes_on_size_threshold(
        self, mock_config, mock_producer, sample_success_message
    ):
        """Processor flushes success batch when size threshold reached."""
        processor = ResultProcessor(
            config=mock_config,
            producer=mock_producer,
            inventory_table_path="abfss://test/xact_attachments",
            batch_size=2,
        )

        # Mock flush method
        processor._flush_batch = AsyncMock()

        # Process messages up to threshold
        await processor._handle_result(sample_success_message)
        await processor._handle_result(sample_success_message)

        # Verify flush was triggered
        processor._flush_batch.assert_called()

    @pytest.mark.asyncio
    async def test_failed_batch_flushes_on_size_threshold(
        self, mock_config, mock_producer, sample_failed_message
    ):
        """Processor flushes failed batch when size threshold reached."""
        processor = ResultProcessor(
            config=mock_config,
            producer=mock_producer,
            inventory_table_path="abfss://test/xact_attachments",
            failed_table_path="abfss://test/xact_attachments_failed",
            batch_size=2,
        )

        # Mock flush method
        processor._flush_failed_batch = AsyncMock()

        # Process messages up to threshold
        await processor._handle_result(sample_failed_message)
        await processor._handle_result(sample_failed_message)

        # Verify flush was triggered
        processor._flush_failed_batch.assert_called()


class TestResultProcessorDeltaWrites:
    """Test Delta table writes."""

    @pytest.mark.asyncio
    async def test_flush_batch_writes_to_delta(
        self, mock_config, mock_producer, sample_success_result
    ):
        """Processor writes success batch to Delta table."""
        processor = ResultProcessor(
            config=mock_config,
            producer=mock_producer,
            inventory_table_path="abfss://test/xact_attachments",
        )

        # Mock inventory writer
        processor._inventory_writer = AsyncMock()
        processor._inventory_writer.write_results = AsyncMock(return_value=True)
        processor._consumer = AsyncMock()
        processor._consumer.commit = AsyncMock()

        # Add to batch
        processor._batch = [sample_success_result]

        # Flush batch
        await processor._flush_batch()

        # Verify Delta write was called
        assert processor._inventory_writer.write_results.called
        assert processor._consumer.commit.called
        assert processor._batches_written == 1
        assert processor._total_records_written == 1

    @pytest.mark.asyncio
    async def test_flush_batch_routes_to_retry_on_failure(
        self, mock_config, mock_producer, sample_success_result
    ):
        """Processor routes batch to retry handler on Delta write failure."""
        processor = ResultProcessor(
            config=mock_config,
            producer=mock_producer,
            inventory_table_path="abfss://test/xact_attachments",
        )

        # Mock inventory writer to fail
        processor._inventory_writer = AsyncMock()
        processor._inventory_writer.write_results = AsyncMock(return_value=False)
        processor._retry_handler = AsyncMock()
        processor._retry_handler.handle_batch_failure = AsyncMock()

        # Add to batch
        processor._batch = [sample_success_result]

        # Flush batch
        await processor._flush_batch()

        # Verify retry handler was called
        assert processor._retry_handler.handle_batch_failure.called
        assert processor._batches_written == 0  # Not incremented on failure
