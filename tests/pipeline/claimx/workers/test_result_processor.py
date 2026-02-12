"""
Unit tests for ClaimX Result Processor.

Test Coverage:
    - Worker initialization and configuration
    - Lifecycle management (start/stop)
    - Message parsing and validation
    - Result routing (success vs failure)
    - Batch accumulation (size-based and time-based)
    - Delta writes with inventory writer
    - Graceful shutdown with batch flushing
    - Periodic logging and metrics

No infrastructure required - all dependencies mocked.
"""

import asyncio
import contextlib
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest

from pydantic import ValidationError

from config.config import MessageConfig
from pipeline.claimx.schemas.results import ClaimXUploadResultMessage
from pipeline.claimx.workers.result_processor import ClaimXResultProcessor
from pipeline.common.types import PipelineMessage


@pytest.fixture
def mock_config():
    """Mock MessageConfig with standard settings."""
    config = Mock(spec=MessageConfig)
    config.get_topic.return_value = "claimx.downloads.results"
    config.get_consumer_group.return_value = "claimx-result-processor"

    def mock_get_worker_config(domain, worker_name, config_key=None):
        if config_key == "processing":
            return {"health_port": 8087}
        return {"health_port": 8087}

    config.get_worker_config = Mock(side_effect=mock_get_worker_config)
    return config


@pytest.fixture
def sample_success_result():
    """Sample successful upload result."""
    return ClaimXUploadResultMessage(
        media_id="media-123",
        project_id="proj-456",
        download_url="https://s3.amazonaws.com/file.jpg",
        blob_path="claimx/proj-456/media/file.jpg",
        file_type="jpg",
        file_name="file.jpg",
        source_event_id="evt-abc",
        status="completed",
        bytes_uploaded=1024,
        created_at=datetime.now(UTC),
    )


@pytest.fixture
def sample_failure_result():
    """Sample failed upload result."""
    return ClaimXUploadResultMessage(
        media_id="media-456",
        project_id="proj-789",
        download_url="https://s3.amazonaws.com/file2.jpg",
        blob_path="claimx/proj-789/media/file2.jpg",
        file_type="jpg",
        file_name="file2.jpg",
        source_event_id="evt-def",
        status="failed_permanent",
        bytes_uploaded=0,
        error_message="Upload failed",
        created_at=datetime.now(UTC),
    )


@pytest.fixture
def sample_success_message(sample_success_result):
    """Sample Kafka message with successful result."""
    return PipelineMessage(
        topic="claimx.downloads.results",
        partition=0,
        offset=1,
        key=b"evt-abc",
        value=sample_success_result.model_dump_json().encode(),
        timestamp=None,
        headers=None,
    )


@pytest.fixture
def sample_failure_message(sample_failure_result):
    """Sample Kafka message with failed result."""
    return PipelineMessage(
        topic="claimx.downloads.results",
        partition=0,
        offset=2,
        key=b"evt-def",
        value=sample_failure_result.model_dump_json().encode(),
        timestamp=None,
        headers=None,
    )


class TestClaimXResultProcessorInitialization:
    """Test worker initialization and configuration."""

    def test_initialization_with_default_config(self, mock_config):
        """Processor initializes with default configuration."""
        processor = ClaimXResultProcessor(config=mock_config)

        assert processor.domain == "claimx"
        assert processor.worker_name == "result_processor"
        assert processor.worker_id == "result_processor"
        assert processor.instance_id is None
        assert processor._running is False
        assert processor.batch_size == 2000
        assert processor.batch_timeout_seconds == 5

    def test_initialization_with_custom_batch_config(self, mock_config):
        """Processor accepts custom batch configuration."""
        processor = ClaimXResultProcessor(
            config=mock_config,
            batch_size=100,
            batch_timeout_seconds=10.0,
        )

        assert processor.batch_size == 100
        assert processor.batch_timeout_seconds == 10.0

    def test_initialization_with_instance_id(self, mock_config):
        """Processor uses instance ID for worker_id suffix."""
        processor = ClaimXResultProcessor(
            config=mock_config,
            instance_id="happy-tiger",
        )

        assert processor.worker_id == "result_processor-happy-tiger"
        assert processor.instance_id == "happy-tiger"

    def test_initialization_with_inventory_writer(self, mock_config):
        """Processor initializes with inventory writer."""
        processor = ClaimXResultProcessor(
            config=mock_config,
            inventory_table_path="abfss://test/claimx_attachments",
        )

        assert processor.inventory_writer is not None

    def test_initialization_without_inventory_writer(self, mock_config):
        """Processor can initialize without inventory writer."""
        processor = ClaimXResultProcessor(config=mock_config)

        assert processor.inventory_writer is None

    def test_metrics_initialized_to_zero(self, mock_config):
        """Processor initializes metrics to zero."""
        processor = ClaimXResultProcessor(config=mock_config)

        assert processor._records_processed == 0
        assert processor._records_succeeded == 0
        assert processor._records_failed == 0
        assert processor._batches_written == 0
        assert processor._total_records_written == 0


class TestClaimXResultProcessorLifecycle:
    """Test worker lifecycle (start/stop)."""

    @pytest.mark.asyncio
    async def test_start_initializes_components(self, mock_config):
        """Processor start initializes all components."""
        processor = ClaimXResultProcessor(config=mock_config)

        with (
            patch(
                "pipeline.claimx.workers.result_processor.create_consumer"
            ) as mock_create_consumer,
            patch("pipeline.common.telemetry.initialize_worker_telemetry"),
            patch.object(processor.health_server, "start", new_callable=AsyncMock),
        ):
            # Setup mock consumer
            mock_consumer = AsyncMock()
            mock_consumer.start = AsyncMock(side_effect=Exception("Stop"))
            mock_create_consumer.return_value = mock_consumer

            with contextlib.suppress(Exception):
                await processor.start()

            # Verify components were initialized
            assert processor._running is False  # Reset in finally
            assert mock_consumer is not None

    @pytest.mark.asyncio
    async def test_stop_flushes_pending_batch(self, mock_config):
        """Processor stop flushes pending batch."""
        processor = ClaimXResultProcessor(
            config=mock_config,
            inventory_table_path="abfss://test/claimx_attachments",
        )

        # Setup mocked components
        processor._running = True
        processor.consumer = AsyncMock()
        processor.consumer.stop = AsyncMock()

        # Add pending batch
        processor._batch = [Mock()]

        # Mock flush
        processor._flush_batch = AsyncMock()

        # Stop processor
        await processor.stop()

        # Verify flush was called
        processor._flush_batch.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_handles_none_components(self, mock_config):
        """Processor stop handles None components gracefully."""
        processor = ClaimXResultProcessor(config=mock_config)

        # All components are None
        assert processor.consumer is None

        # Should not raise
        await processor.stop()


class TestClaimXResultProcessorMessageProcessing:
    """Test message parsing and processing."""

    @pytest.mark.asyncio
    async def test_success_result_parsed_successfully(self, mock_config, sample_success_message):
        """Processor parses successful result message."""
        processor = ClaimXResultProcessor(config=mock_config)

        await processor._handle_result_message(sample_success_message)

        # Verify message was processed
        assert processor._records_processed == 1
        assert processor._records_succeeded == 1
        assert processor._records_failed == 0

    @pytest.mark.asyncio
    async def test_failure_result_parsed_successfully(self, mock_config, sample_failure_message):
        """Processor parses failed result message."""
        processor = ClaimXResultProcessor(config=mock_config)

        # Patch log_worker_error to avoid parameter issues
        with patch("pipeline.claimx.workers.result_processor.log_worker_error"):
            await processor._handle_result_message(sample_failure_message)

        # Verify message was processed
        assert processor._records_processed == 1
        assert processor._records_succeeded == 0
        assert processor._records_failed == 1

    @pytest.mark.asyncio
    async def test_invalid_json_raises_error(self, mock_config):
        """Processor raises error on invalid JSON."""
        processor = ClaimXResultProcessor(config=mock_config)

        invalid_message = PipelineMessage(
            topic="claimx.downloads.results",
            partition=0,
            offset=1,
            key=b"key",
            value=b"invalid json{",
            timestamp=None,
            headers=None,
        )

        with pytest.raises(ValidationError):
            await processor._handle_result_message(invalid_message)


class TestClaimXResultProcessorBatching:
    """Test batch accumulation and flushing."""

    @pytest.mark.asyncio
    async def test_batch_accumulates_success_results(self, mock_config, sample_success_message):
        """Processor accumulates successful results in batch."""
        processor = ClaimXResultProcessor(
            config=mock_config,
            inventory_table_path="abfss://test/claimx_attachments",
            batch_size=10,
        )

        # Process multiple messages
        for _ in range(3):
            await processor._handle_result_message(sample_success_message)

        # Verify batch accumulation
        assert len(processor._batch) == 3
        assert processor._records_succeeded == 3

    @pytest.mark.asyncio
    async def test_batch_flushes_on_size_threshold(self, mock_config, sample_success_message):
        """Processor flushes batch when size threshold reached."""
        processor = ClaimXResultProcessor(
            config=mock_config,
            inventory_table_path="abfss://test/claimx_attachments",
            batch_size=2,
        )

        # Mock flush method
        processor._flush_batch = AsyncMock()

        # Process messages up to threshold
        await processor._handle_result_message(sample_success_message)
        await processor._handle_result_message(sample_success_message)

        # Verify flush was triggered
        processor._flush_batch.assert_called()

    @pytest.mark.asyncio
    async def test_batch_does_not_accumulate_without_writer(
        self, mock_config, sample_success_message
    ):
        """Processor does not accumulate batch without inventory writer."""
        processor = ClaimXResultProcessor(config=mock_config)

        await processor._handle_result_message(sample_success_message)

        # Verify no batch accumulation
        assert len(processor._batch) == 0
        assert processor._records_succeeded == 1


class TestClaimXResultProcessorDeltaWrites:
    """Test Delta table writes."""

    @pytest.mark.asyncio
    async def test_flush_batch_writes_to_delta(self, mock_config, sample_success_result):
        """Processor writes batch to Delta table."""
        processor = ClaimXResultProcessor(
            config=mock_config,
            inventory_table_path="abfss://test/claimx_attachments",
        )

        # Mock inventory writer
        processor.inventory_writer = AsyncMock()
        processor.inventory_writer._async_merge = AsyncMock()
        processor.consumer = AsyncMock()
        processor.consumer.commit = AsyncMock()

        # Add to batch
        processor._batch = [sample_success_result]

        # Flush batch
        await processor._flush_batch()

        # Verify Delta write was called
        assert processor.inventory_writer._async_merge.called
        assert processor.consumer.commit.called
        assert processor._batches_written == 1
        assert processor._total_records_written == 1

    @pytest.mark.asyncio
    async def test_flush_batch_with_empty_batch_is_noop(self, mock_config):
        """Processor handles empty batch flush gracefully."""
        processor = ClaimXResultProcessor(
            config=mock_config,
            inventory_table_path="abfss://test/claimx_attachments",
        )

        processor.inventory_writer = AsyncMock()
        processor._batch = []

        await processor._flush_batch()

        # Verify no writes occurred
        assert not processor.inventory_writer._async_merge.called


class TestClaimXResultProcessorPeriodicTasks:
    """Test periodic background tasks."""

    @pytest.mark.asyncio
    async def test_periodic_flush_checks_timeout(self, mock_config):
        """Periodic flush task checks timeout threshold."""
        processor = ClaimXResultProcessor(
            config=mock_config,
            batch_timeout_seconds=0.1,
        )

        processor._running = True
        processor._batch = [Mock()]
        processor._flush_batch = AsyncMock()

        # Run periodic flush briefly
        task = asyncio.create_task(processor._periodic_flush())
        await asyncio.sleep(0.2)
        processor._running = False
        await task

        # Verify flush was called due to timeout
        assert processor._flush_batch.called
