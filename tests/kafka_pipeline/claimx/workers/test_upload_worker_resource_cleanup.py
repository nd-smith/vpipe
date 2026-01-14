"""
Tests for ClaimX upload worker resource cleanup and initialization failure handling.

Tests cover:
- OneLake client initialization failures
- Kafka consumer initialization failures
- Normal shutdown with proper cleanup
- Error handling during cleanup
- In-flight task timeout handling
- Multiple stop() calls (idempotency)
"""

import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.claimx.workers.upload_worker import ClaimXUploadWorker


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
            "upload_worker": {
                "processing": {
                    "concurrency": 10,
                    "batch_size": 20,
                    "health_port": 8083,
                },
            },
        },
        onelake_base_path="abfss://test@onelake.dfs.fabric.microsoft.com/lakehouse",
        onelake_domain_paths={
            "claimx": "abfss://claimx@onelake.dfs.fabric.microsoft.com/lakehouse"
        },
    )


@pytest.mark.asyncio
class TestClaimXUploadWorkerResourceCleanup:
    """Test suite for resource cleanup and initialization failure handling."""

    async def test_onelake_client_init_failure_cleans_up_producer(self, kafka_config):
        """Test that OneLake client initialization failure cleans up producer and health server."""
        worker = ClaimXUploadWorker(kafka_config)

        # Track if producer.stop and health.stop were called
        producer_stop_called = False
        health_stop_called = False

        async def track_producer_stop():
            nonlocal producer_stop_called
            producer_stop_called = True

        async def track_health_stop():
            nonlocal health_stop_called
            health_stop_called = True

        # Replace methods with tracking functions
        worker.producer.stop = track_producer_stop
        worker.health_server.stop = track_health_stop

        # Mock OneLake client to fail on __aenter__
        mock_onelake = AsyncMock()
        mock_onelake.__aenter__.side_effect = Exception("OneLake connection failed")

        with patch(
            "kafka_pipeline.claimx.workers.upload_worker.OneLakeClient"
        ) as mock_onelake_class:
            mock_onelake_class.return_value = mock_onelake

            # Prevent the consume loop from running
            worker._consume_batch_loop = AsyncMock()

            # start() should fail and clean up resources
            with pytest.raises(Exception, match="OneLake connection failed"):
                await worker.start()

            # Verify cleanup was called
            assert producer_stop_called, "Producer stop should have been called"
            assert health_stop_called, "Health server stop should have been called"

    async def test_consumer_init_failure_cleans_up_all_resources(self, kafka_config):
        """Test that consumer initialization failure cleans up OneLake client, producer, and health server."""
        worker = ClaimXUploadWorker(kafka_config)

        # Track cleanup calls
        onelake_close_called = False
        producer_stop_called = False
        health_stop_called = False

        async def track_onelake_close():
            nonlocal onelake_close_called
            onelake_close_called = True

        async def track_producer_stop():
            nonlocal producer_stop_called
            producer_stop_called = True

        async def track_health_stop():
            nonlocal health_stop_called
            health_stop_called = True

        # Replace methods with tracking functions
        worker.producer.stop = track_producer_stop
        worker.health_server.stop = track_health_stop

        # Mock OneLake client to succeed initialization but track close
        mock_onelake = AsyncMock()
        mock_onelake.__aenter__.return_value = mock_onelake
        mock_onelake.close = track_onelake_close

        with patch(
            "kafka_pipeline.claimx.workers.upload_worker.OneLakeClient"
        ) as mock_onelake_class, patch.object(
            worker, "_create_consumer", new_callable=AsyncMock
        ) as mock_create_consumer:
            mock_onelake_class.return_value = mock_onelake
            mock_create_consumer.side_effect = Exception("Kafka connection failed")

            # start() should fail and clean up all resources
            with pytest.raises(Exception, match="Kafka connection failed"):
                await worker.start()

            # Verify OneLake client was initialized then cleaned up
            mock_onelake.__aenter__.assert_called_once()
            assert onelake_close_called, "OneLake client close should have been called"

            # Verify producer was stopped
            assert producer_stop_called, "Producer stop should have been called"

            # Verify health server was stopped
            assert health_stop_called, "Health server stop should have been called"

            # Verify onelake_client is set to None after cleanup
            assert worker.onelake_client is None

    async def test_normal_shutdown_cleans_up_resources(self, kafka_config):
        """Test normal shutdown properly cleans up all resources."""
        worker = ClaimXUploadWorker(kafka_config)

        # Initialize state as if worker was running
        worker._running = True
        worker._shutdown_event = asyncio.Event()
        worker._in_flight_tasks = set()
        worker._cycle_task = None

        # Mock consumer
        mock_consumer = AsyncMock()
        worker._consumer = mock_consumer

        # Mock producer
        mock_producer = AsyncMock()
        worker.producer = mock_producer

        # Mock health server
        worker.health_server.stop = AsyncMock()

        # Mock OneLake client
        mock_onelake = AsyncMock()
        worker.onelake_client = mock_onelake

        await worker.stop()

        # Verify all resources cleaned up
        mock_consumer.stop.assert_called_once()
        mock_producer.stop.assert_called_once()
        worker.health_server.stop.assert_called_once()
        mock_onelake.close.assert_called_once()

        # Verify resources set to None
        assert worker._consumer is None
        assert worker.onelake_client is None

    async def test_onelake_close_error_during_shutdown_is_handled(self, kafka_config):
        """Test that OneLake client close errors during shutdown are handled gracefully."""
        worker = ClaimXUploadWorker(kafka_config)

        # Initialize state
        worker._running = True
        worker._shutdown_event = asyncio.Event()
        worker._in_flight_tasks = set()
        worker._cycle_task = None

        # Mock consumer
        mock_consumer = AsyncMock()
        worker._consumer = mock_consumer

        # Mock producer
        mock_producer = AsyncMock()
        worker.producer = mock_producer

        # Mock health server
        worker.health_server.stop = AsyncMock()

        # Mock OneLake client to fail on close
        mock_onelake = AsyncMock()
        mock_onelake.close.side_effect = Exception("Close failed")
        worker.onelake_client = mock_onelake

        # Should not raise, but log warning
        await worker.stop()

        # Verify close was attempted
        mock_onelake.close.assert_called_once()

        # Verify other resources still cleaned up
        mock_consumer.stop.assert_called_once()
        mock_producer.stop.assert_called_once()
        worker.health_server.stop.assert_called_once()

        # Verify onelake_client set to None despite close error
        assert worker.onelake_client is None

    async def test_stop_with_in_flight_tasks_waits_then_times_out(self, kafka_config):
        """Test stop waits for in-flight tasks but times out after 30 seconds."""
        worker = ClaimXUploadWorker(kafka_config)

        # Initialize state with in-flight tasks
        worker._running = True
        worker._shutdown_event = asyncio.Event()
        worker._in_flight_tasks = {"media-1", "media-2", "media-3"}  # Simulate stuck tasks
        worker._cycle_task = None

        # Mock consumer
        mock_consumer = AsyncMock()
        worker._consumer = mock_consumer

        # Mock producer
        mock_producer = AsyncMock()
        worker.producer = mock_producer

        # Mock health server
        worker.health_server.stop = AsyncMock()

        # Mock OneLake client
        mock_onelake = AsyncMock()
        worker.onelake_client = mock_onelake

        start_time = time.time()

        # stop() should wait briefly but not the full 30 seconds in test
        # (tasks never clear, so it should log warning and force shutdown)
        await worker.stop()

        elapsed = time.time() - start_time

        # Should have waited at least briefly but forced shutdown
        # In real scenario would wait up to 30s, but for test we just verify it completes
        assert elapsed < 35  # Should not hang indefinitely

        # Verify resources cleaned up despite in-flight tasks
        mock_consumer.stop.assert_called_once()
        mock_producer.stop.assert_called_once()
        worker.health_server.stop.assert_called_once()
        mock_onelake.close.assert_called_once()

    async def test_multiple_stop_calls_are_safe(self, kafka_config):
        """Test that calling stop() multiple times is safe and idempotent."""
        worker = ClaimXUploadWorker(kafka_config)

        # Initialize state
        worker._running = True
        worker._shutdown_event = asyncio.Event()
        worker._in_flight_tasks = set()
        worker._cycle_task = None

        # Mock consumer
        mock_consumer = AsyncMock()
        worker._consumer = mock_consumer

        # Mock producer
        mock_producer = AsyncMock()
        worker.producer = mock_producer

        # Mock health server
        worker.health_server.stop = AsyncMock()

        # Mock OneLake client
        mock_onelake = AsyncMock()
        worker.onelake_client = mock_onelake

        # First stop
        await worker.stop()

        # Resources cleaned up
        assert worker._consumer is None
        assert worker.onelake_client is None

        # Second stop should be safe (no-op)
        await worker.stop()

        # Should not raise or cause issues
        # Verify stop was only called once on mocks (from first stop)
        mock_consumer.stop.assert_called_once()
        mock_producer.stop.assert_called_once()
        mock_onelake.close.assert_called_once()
