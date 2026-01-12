"""
Integration tests for ClaimX result processing flow.

Tests the complete flow from result consumption through logging and metrics,
using real Kafka (via shared test infrastructure) with ClaimXUploadResultMessage.

Tests:
- Successful upload result processing
- Failed (transient) upload result processing
- Permanently failed upload result processing
- Statistics tracking and calculation
- Metrics emission
"""

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

import pytest

from kafka_pipeline.claimx.schemas.results import ClaimXUploadResultMessage
from kafka_pipeline.claimx.workers.result_processor import ClaimXResultProcessor
from tests.kafka_pipeline.integration.claimx.generators import create_upload_result_message


@pytest.mark.asyncio
class TestClaimXResultProcessingIntegration:
    """Integration tests for ClaimX result processor with real Kafka."""

    async def test_successful_result_processing(
        self,
        test_claimx_config,
        kafka_producer,
    ):
        """Test that successful upload results are logged and tracked correctly."""
        # Create result processor
        processor = ClaimXResultProcessor(
            config=test_claimx_config,
            results_topic=test_claimx_config.claimx_downloads_results_topic,
        )

        # Start processor in background
        processor_task = asyncio.create_task(processor.start())

        try:
            # Give processor time to start consuming
            await asyncio.sleep(2)

            # Produce successful result
            result = create_upload_result_message(
                media_id=12345,
                project_id=98765,
                status="completed",
                file_name="test_image.jpg",
                file_type="image/jpeg",
                bytes_uploaded=2048,
                blob_path="project_98765/media_12345.jpg",
                source_event_id="evt-001",
            )

            await kafka_producer.send(
                topic=test_claimx_config.claimx_downloads_results_topic,
                key=result.media_id,
                value=result,
            )

            # Wait for processing
            await asyncio.sleep(3)

            # Verify statistics were updated
            async with processor._stats_lock:
                assert processor._stats["total_processed"] == 1
                assert processor._stats["completed"] == 1
                assert processor._stats["failed"] == 0
                assert processor._stats["failed_permanent"] == 0
                assert processor._stats["bytes_uploaded_total"] == 2048

        finally:
            # Stop processor
            await processor.stop()
            processor_task.cancel()
            try:
                await processor_task
            except asyncio.CancelledError:
                pass

    async def test_failed_transient_result_processing(
        self,
        test_claimx_config,
        kafka_producer,
    ):
        """Test that transient failures are logged as warnings and tracked."""
        processor = ClaimXResultProcessor(
            config=test_claimx_config,
            results_topic=test_claimx_config.claimx_downloads_results_topic,
        )

        processor_task = asyncio.create_task(processor.start())

        try:
            await asyncio.sleep(2)

            # Produce failed (transient) result
            result = create_upload_result_message(
                media_id=12346,
                project_id=98765,
                status="failed",
                file_name="test_image.jpg",
                error_message="Network timeout during upload",
                source_event_id="evt-002",
            )

            await kafka_producer.send(
                topic=test_claimx_config.claimx_downloads_results_topic,
                key=result.media_id,
                value=result,
            )

            await asyncio.sleep(3)

            # Verify statistics
            async with processor._stats_lock:
                assert processor._stats["total_processed"] == 1
                assert processor._stats["completed"] == 0
                assert processor._stats["failed"] == 1
                assert processor._stats["failed_permanent"] == 0

        finally:
            await processor.stop()
            processor_task.cancel()
            try:
                await processor_task
            except asyncio.CancelledError:
                pass

    async def test_failed_permanent_result_processing(
        self,
        test_claimx_config,
        kafka_producer,
    ):
        """Test that permanent failures are logged as errors and tracked."""
        processor = ClaimXResultProcessor(
            config=test_claimx_config,
            results_topic=test_claimx_config.claimx_downloads_results_topic,
        )

        processor_task = asyncio.create_task(processor.start())

        try:
            await asyncio.sleep(2)

            # Produce permanently failed result
            result = create_upload_result_message(
                media_id=12347,
                project_id=98765,
                status="failed_permanent",
                file_name="test_image.jpg",
                error_message="File validation failed: corrupted data",
                source_event_id="evt-003",
            )

            await kafka_producer.send(
                topic=test_claimx_config.claimx_downloads_results_topic,
                key=result.media_id,
                value=result,
            )

            await asyncio.sleep(3)

            # Verify statistics
            async with processor._stats_lock:
                assert processor._stats["total_processed"] == 1
                assert processor._stats["completed"] == 0
                assert processor._stats["failed"] == 0
                assert processor._stats["failed_permanent"] == 1

        finally:
            await processor.stop()
            processor_task.cancel()
            try:
                await processor_task
            except asyncio.CancelledError:
                pass

    async def test_mixed_results_statistics(
        self,
        test_claimx_config,
        kafka_producer,
    ):
        """Test statistics calculation with a mix of successful and failed results."""
        processor = ClaimXResultProcessor(
            config=test_claimx_config,
            results_topic=test_claimx_config.claimx_downloads_results_topic,
        )

        processor_task = asyncio.create_task(processor.start())

        try:
            await asyncio.sleep(2)

            # Produce multiple results with different statuses
            results = [
                # 2 successful
                create_upload_result_message(
                    media_id=1000 + i,
                    project_id=98765,
                    status="completed",
                    file_name=f"success_{i}.jpg",
                    bytes_uploaded=1024 * (i + 1),
                    blob_path=f"claimx/project_98765/media_{1000 + i}.jpg",
                )
                for i in range(2)
            ] + [
                # 1 transient failure
                create_upload_result_message(
                    media_id=2000,
                    project_id=98765,
                    status="failed",
                    file_name="failed_temp.jpg",
                    error_message="Temporary network error",
                ),
                # 1 permanent failure
                create_upload_result_message(
                    media_id=3000,
                    project_id=98765,
                    status="failed_permanent",
                    file_name="failed_perm.jpg",
                    error_message="Invalid file format",
                ),
            ]

            # Send all results
            for result in results:
                await kafka_producer.send(
                    topic=test_claimx_config.claimx_downloads_results_topic,
                    key=result.media_id,
                    value=result,
                )

            # Wait for all messages to be processed
            await asyncio.sleep(5)

            # Verify aggregated statistics
            async with processor._stats_lock:
                assert processor._stats["total_processed"] == 4
                assert processor._stats["completed"] == 2
                assert processor._stats["failed"] == 1
                assert processor._stats["failed_permanent"] == 1
                # Verify byte counting (only successful uploads)
                expected_bytes = 1024 + 2048  # From 2 successful uploads
                assert processor._stats["bytes_uploaded_total"] == expected_bytes

        finally:
            await processor.stop()
            processor_task.cancel()
            try:
                await processor_task
            except asyncio.CancelledError:
                pass

    @pytest.mark.skip(reason="Invalid message handling test needs raw producer access - deferred")
    async def test_invalid_message_handling(
        self,
        test_claimx_config,
        kafka_producer,
    ):
        """Test that invalid messages are logged and raise errors appropriately."""
        # NOTE: This test is skipped because it requires direct access to raw producer
        # to send invalid JSON. Error handling is tested at the unit test level.
        # The integration tests focus on valid message flows.
        pass

    async def test_metrics_emission(
        self,
        test_claimx_config,
        kafka_producer,
    ):
        """Test that metrics are emitted for processed results."""
        with patch("kafka_pipeline.claimx.workers.result_processor.record_message_consumed") as mock_metric:
            processor = ClaimXResultProcessor(
                config=test_claimx_config,
                results_topic=test_claimx_config.claimx_downloads_results_topic,
            )

            processor_task = asyncio.create_task(processor.start())

            try:
                await asyncio.sleep(2)

                # Produce a successful result
                result = create_upload_result_message(
                    media_id=5000,
                    project_id=98765,
                    status="completed",
                    file_name="metric_test.jpg",
                    bytes_uploaded=512,
                    blob_path="project_98765/media_5000.jpg",
                )

                await kafka_producer.send(
                    topic=test_claimx_config.claimx_downloads_results_topic,
                    key=result.media_id,
                    value=result,
                )

                await asyncio.sleep(3)

                # Verify metric was recorded
                assert mock_metric.called
                call_args = mock_metric.call_args
                assert call_args[0][0] == test_claimx_config.claimx_downloads_results_topic
                assert call_args[1]["success"] is True

            finally:
                await processor.stop()
                processor_task.cancel()
                try:
                    await processor_task
                except asyncio.CancelledError:
                    pass
