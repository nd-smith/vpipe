"""
Integration tests for result processing flow.

Tests the complete flow from Kafka consumption through batching to Delta writes,
using real Kafka (Testcontainers) and mocked Delta storage.
"""

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from kafka_pipeline.xact.schemas.results import DownloadResultMessage
from kafka_pipeline.xact.workers.result_processor import ResultProcessor


@pytest.mark.asyncio
class TestResultProcessingIntegration:
    """Integration tests for result processor with real Kafka."""

    async def test_result_to_batch_to_delta_write(
        self, test_kafka_config, kafka_producer, unique_topic_prefix
    ):
        """Test complete flow: Kafka result → batch → Delta write."""
        inventory_table_path = "abfss://test@storage/xact_attachments"

        # Mock Delta writer to avoid Azure dependencies
        with patch("kafka_pipeline.workers.result_processor.DeltaInventoryWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer.write_results = AsyncMock(return_value=True)
            mock_writer_class.return_value = mock_writer

            # Create processor with small batch size for faster testing
            processor = ResultProcessor(
                config=test_kafka_config,
                inventory_table_path=inventory_table_path,
                batch_size=3,
                batch_timeout_seconds=1.0,
            )

            # Start processor in background
            processor_task = asyncio.create_task(processor.start())

            try:
                # Give processor time to start consuming
                await asyncio.sleep(2)

                # Produce 3 successful results to trigger batch flush
                for i in range(3):
                    result = DownloadResultMessage(
                        trace_id=f"evt-{i}",
                        attachment_url=f"https://storage.example.com/file{i}.pdf",
                        blob_path=f"claims/C-{i}/file{i}.pdf",
                        status_subtype="documentsReceived",
                        file_type="pdf",
                        assignment_id=f"C-{i}",
                        status="completed",
                        http_status=200,
                        bytes_downloaded=1024 * (i + 1),
                        created_at=datetime.now(timezone.utc),
                    )

                    await kafka_producer.send(
                        topic=test_kafka_config.downloads_results_topic,
                        key=result.trace_id,
                        value=result,
                    )

                # Wait for batch to be processed
                await asyncio.sleep(3)

                # Verify Delta writer was called with batch
                assert mock_writer.write_results.called
                call_args = mock_writer.write_results.call_args[0][0]
                assert len(call_args) == 3
                assert call_args[0].trace_id == "evt-0"
                assert call_args[1].trace_id == "evt-1"
                assert call_args[2].trace_id == "evt-2"

            finally:
                # Stop processor
                await processor.stop()
                processor_task.cancel()
                try:
                    await processor_task
                except asyncio.CancelledError:
                    pass

    async def test_idempotency_duplicate_results(
        self, test_kafka_config, kafka_producer, unique_topic_prefix
    ):
        """Test that duplicate results are passed to Delta writer for merge handling."""
        inventory_table_path = "abfss://test@storage/xact_attachments"

        with patch("kafka_pipeline.workers.result_processor.DeltaInventoryWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer.write_results = AsyncMock(return_value=True)
            mock_writer_class.return_value = mock_writer

            processor = ResultProcessor(
                config=test_kafka_config,
                inventory_table_path=inventory_table_path,
                batch_size=2,
                batch_timeout_seconds=1.0,
            )

            processor_task = asyncio.create_task(processor.start())

            try:
                await asyncio.sleep(2)

                # Produce same result twice (duplicate)
                for _ in range(2):
                    result = DownloadResultMessage(
                        trace_id="evt-duplicate",
                        attachment_url="https://storage.example.com/duplicate.pdf",
                        blob_path="claims/C-999/duplicate.pdf",
                        status_subtype="documentsReceived",
                        file_type="pdf",
                        assignment_id="C-999",
                        status="completed",
                        http_status=200,
                        bytes_downloaded=2048,
                        created_at=datetime.now(timezone.utc),
                    )

                    await kafka_producer.send(
                        topic=test_kafka_config.downloads_results_topic,
                        key=result.trace_id,
                        value=result,
                    )

                # Wait for batch to be processed
                await asyncio.sleep(3)

                # Verify both duplicates were included in batch
                # Delta merge logic will handle deduplication
                assert mock_writer.write_results.called
                call_args = mock_writer.write_results.call_args[0][0]
                assert len(call_args) == 2
                assert call_args[0].trace_id == "evt-duplicate"
                assert call_args[1].trace_id == "evt-duplicate"

            finally:
                await processor.stop()
                processor_task.cancel()
                try:
                    await processor_task
                except asyncio.CancelledError:
                    pass

    async def test_graceful_shutdown_with_pending_batch(
        self, test_kafka_config, kafka_producer, unique_topic_prefix
    ):
        """Test that graceful shutdown flushes pending batch."""
        inventory_table_path = "abfss://test@storage/xact_attachments"

        with patch("kafka_pipeline.workers.result_processor.DeltaInventoryWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer.write_results = AsyncMock(return_value=True)
            mock_writer_class.return_value = mock_writer

            # Large batch size so results stay in batch
            processor = ResultProcessor(
                config=test_kafka_config,
                inventory_table_path=inventory_table_path,
                batch_size=100,
                batch_timeout_seconds=60.0,
            )

            processor_task = asyncio.create_task(processor.start())

            try:
                await asyncio.sleep(2)

                # Produce 2 results (below batch threshold)
                for i in range(2):
                    result = DownloadResultMessage(
                        trace_id=f"evt-pending-{i}",
                        attachment_url=f"https://storage.example.com/pending{i}.pdf",
                        blob_path=f"claims/C-{i}/pending{i}.pdf",
                        status_subtype="documentsReceived",
                        file_type="pdf",
                        assignment_id=f"C-{i}",
                        status="completed",
                        http_status=200,
                        bytes_downloaded=1024,
                        created_at=datetime.now(timezone.utc),
                    )

                    await kafka_producer.send(
                        topic=test_kafka_config.downloads_results_topic,
                        key=result.trace_id,
                        value=result,
                    )

                # Wait for messages to be consumed (but not flushed)
                await asyncio.sleep(2)

                # Verify no flush yet (batch not full, timeout not reached)
                assert not mock_writer.write_results.called

                # Gracefully stop processor
                await processor.stop()

                # Verify shutdown flush occurred
                assert mock_writer.write_results.called
                call_args = mock_writer.write_results.call_args[0][0]
                assert len(call_args) == 2
                assert call_args[0].trace_id == "evt-pending-0"
                assert call_args[1].trace_id == "evt-pending-1"

            finally:
                processor_task.cancel()
                try:
                    await processor_task
                except asyncio.CancelledError:
                    pass

    async def test_filtering_non_success_results(
        self, test_kafka_config, kafka_producer, unique_topic_prefix
    ):
        """Test that failed results are filtered out and not written to Delta."""
        inventory_table_path = "abfss://test@storage/xact_attachments"

        with patch("kafka_pipeline.workers.result_processor.DeltaInventoryWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer.write_results = AsyncMock(return_value=True)
            mock_writer_class.return_value = mock_writer

            processor = ResultProcessor(
                config=test_kafka_config,
                inventory_table_path=inventory_table_path,
                batch_size=10,
                batch_timeout_seconds=1.0,
            )

            processor_task = asyncio.create_task(processor.start())

            try:
                await asyncio.sleep(2)

                # Produce mix of success and failed results
                results = [
                    DownloadResultMessage(
                        trace_id="evt-success",
                        attachment_url="https://storage.example.com/success.pdf",
                        blob_path="claims/C-1/success.pdf",
                        status_subtype="documentsReceived",
                        file_type="pdf",
                        assignment_id="C-1",
                        status="completed",
                        http_status=200,
                        bytes_downloaded=2048,
                        created_at=datetime.now(timezone.utc),
                    ),
                    DownloadResultMessage(
                        trace_id="evt-failed-transient",
                        attachment_url="https://storage.example.com/timeout.pdf",
                        blob_path="documentsReceived/C-2/pdf/timeout.pdf",
                        status_subtype="documentsReceived",
                        file_type="pdf",
                        assignment_id="C-2",
                        status="failed",
                        bytes_downloaded=0,
                        error_message="Connection timeout",
                        created_at=datetime.now(timezone.utc),
                    ),
                    DownloadResultMessage(
                        trace_id="evt-failed-permanent",
                        attachment_url="https://storage.example.com/invalid.exe",
                        blob_path="documentsReceived/C-3/exe/invalid.exe",
                        status_subtype="documentsReceived",
                        file_type="exe",
                        assignment_id="C-3",
                        status="failed_permanent",
                        http_status=403,
                        bytes_downloaded=0,
                        error_message="File type not allowed",
                        created_at=datetime.now(timezone.utc),
                    ),
                ]

                for result in results:
                    await kafka_producer.send(
                        topic=test_kafka_config.downloads_results_topic,
                        key=result.trace_id,
                        value=result,
                    )

                # Wait for processing
                await asyncio.sleep(3)

                # Stop to flush pending batch
                await processor.stop()

                # Verify only successful result was written
                assert mock_writer.write_results.called
                call_args = mock_writer.write_results.call_args[0][0]
                assert len(call_args) == 1
                assert call_args[0].trace_id == "evt-success"
                assert call_args[0].status == "completed"

            finally:
                processor_task.cancel()
                try:
                    await processor_task
                except asyncio.CancelledError:
                    pass

    async def test_timeout_based_flush(
        self, test_kafka_config, kafka_producer, unique_topic_prefix
    ):
        """Test that batch flushes after timeout even if not full."""
        inventory_table_path = "abfss://test@storage/xact_attachments"

        with patch("kafka_pipeline.workers.result_processor.DeltaInventoryWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer.write_results = AsyncMock(return_value=True)
            mock_writer_class.return_value = mock_writer

            # Large batch size, short timeout
            processor = ResultProcessor(
                config=test_kafka_config,
                inventory_table_path=inventory_table_path,
                batch_size=100,
                batch_timeout_seconds=2.0,
            )

            processor_task = asyncio.create_task(processor.start())

            try:
                await asyncio.sleep(2)

                # Produce single result
                result = DownloadResultMessage(
                    trace_id="evt-timeout",
                    attachment_url="https://storage.example.com/timeout.pdf",
                    blob_path="claims/C-1/timeout.pdf",
                    status_subtype="documentsReceived",
                    file_type="pdf",
                    assignment_id="C-1",
                    status="completed",
                    http_status=200,
                    bytes_downloaded=1024,
                    created_at=datetime.now(timezone.utc),
                )

                await kafka_producer.send(
                    topic=test_kafka_config.downloads_results_topic,
                    key=result.trace_id,
                    value=result,
                )

                # Wait for timeout flush (2s timeout + buffer)
                await asyncio.sleep(4)

                # Verify timeout flush occurred
                assert mock_writer.write_results.called
                call_args = mock_writer.write_results.call_args[0][0]
                assert len(call_args) == 1
                assert call_args[0].trace_id == "evt-timeout"

            finally:
                await processor.stop()
                processor_task.cancel()
                try:
                    await processor_task
                except asyncio.CancelledError:
                    pass
