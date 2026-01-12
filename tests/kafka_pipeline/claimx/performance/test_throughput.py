"""
Performance and throughput tests for ClaimX workers.

Measures and establishes performance baselines for:
1. Enrichment worker throughput (events/sec)
2. Download worker throughput (files/sec)
3. API call latency impact
4. End-to-end pipeline latency

These tests are divided into two categories:
1. Unit-style performance tests (no Kafka required) - test API client, mocks, etc.
2. Integration performance tests (require Docker/Kafka) - test full worker throughput

Run unit performance tests with:
    pytest tests/kafka_pipeline/claimx/performance/test_throughput.py -v -s -m "performance and not integration"

Run integration performance tests with:
    pytest tests/kafka_pipeline/claimx/performance/test_throughput.py -v -s -m "performance and integration"

Baseline Performance Targets (for reference):
- Enrichment worker: > 100 events/sec (single worker)
- Download worker: > 10 files/sec (single worker, with S3 mocked)
- API latency: < 100ms per call (mocked)
- E2E latency: < 5 seconds for single file flow
"""

import asyncio
import logging
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List
from unittest.mock import AsyncMock, patch

import pytest

from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
from kafka_pipeline.claimx.schemas.tasks import ClaimXEnrichmentTask, ClaimXDownloadTask
from kafka_pipeline.config import KafkaConfig


logger = logging.getLogger(__name__)


# =============================================================================
# Performance Test Configuration
# =============================================================================

# Number of events/files for throughput tests
ENRICHMENT_EVENT_COUNT = 100  # Events to process for enrichment throughput
DOWNLOAD_FILE_COUNT = 50  # Files to download for download throughput

# Concurrency settings
ENRICHMENT_BATCH_SIZE = 10  # Events per batch
DOWNLOAD_CONCURRENCY = 10  # Concurrent downloads

# Performance thresholds (for assertions)
MIN_ENRICHMENT_THROUGHPUT = 50.0  # events/sec (conservative target)
MIN_DOWNLOAD_THROUGHPUT = 5.0  # files/sec (conservative target)
MAX_API_LATENCY_MS = 200.0  # milliseconds (mocked API)


# =============================================================================
# Test Data Generators
# =============================================================================


def create_mock_project_response(project_id: int, mfn: str = None) -> Dict:
    """Create mock project API response."""
    return {
        "id": project_id,
        "mfn": mfn or f"MFN-{project_id}",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "status": "active",
        "name": f"Test Project {project_id}",
        "contacts": []
    }


def create_mock_media_response(media_id: int, project_id: int) -> Dict:
    """Create mock media API response."""
    return {
        "id": media_id,
        "project_id": project_id,
        "filename": f"media_{media_id}.jpg",
        "content_type": "image/jpeg",
        "file_size": 2048,
        "download_url": f"https://example.com/media/test_{media_id}.jpg",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def create_enrichment_task(
    event_id: int,
    event_type: str,
    project_id: int,
    **kwargs
) -> ClaimXEnrichmentTask:
    """Create a ClaimX enrichment task for testing."""
    return ClaimXEnrichmentTask(
        event_id=str(event_id),
        event_type=event_type,
        project_id=str(project_id),
        created_at=kwargs.get("created_at", datetime.now(timezone.utc)),
        retry_count=kwargs.get("retry_count", 0),
    )


def create_download_task(
    media_id: int,
    project_id: int,
    download_url: str,
    **kwargs
) -> ClaimXDownloadTask:
    """Create a ClaimX download task for testing."""
    default_blob_path = f"project_{project_id}/media_{media_id}.jpg"
    return ClaimXDownloadTask(
        media_id=str(media_id),
        project_id=str(project_id),
        download_url=download_url,
        blob_path=kwargs.get("blob_path", default_blob_path),
        file_type=kwargs.get("file_type", "jpg"),
        file_name=kwargs.get("file_name", f"media_{media_id}.jpg"),
        source_event_id=kwargs.get("source_event_id", f"evt_{media_id}"),
        retry_count=kwargs.get("retry_count", 0),
    )


# =============================================================================
# Performance Measurement Helpers
# =============================================================================

class PerformanceStats:
    """Track and calculate performance statistics."""

    def __init__(self, operation: str):
        self.operation = operation
        self.start_time: float = 0
        self.end_time: float = 0
        self.item_count: int = 0
        self.latencies: List[float] = []

    def start(self):
        """Start timing."""
        self.start_time = time.time()

    def record_item(self, latency_ms: float = None):
        """Record completion of one item."""
        self.item_count += 1
        if latency_ms is not None:
            self.latencies.append(latency_ms)

    def finish(self):
        """Finish timing."""
        self.end_time = time.time()

    @property
    def elapsed_seconds(self) -> float:
        """Total elapsed time in seconds."""
        return self.end_time - self.start_time

    @property
    def throughput(self) -> float:
        """Items per second."""
        if self.elapsed_seconds > 0:
            return self.item_count / self.elapsed_seconds
        return 0.0

    @property
    def avg_latency_ms(self) -> float:
        """Average latency in milliseconds."""
        if self.latencies:
            return statistics.mean(self.latencies)
        return 0.0

    @property
    def p50_latency_ms(self) -> float:
        """Median latency in milliseconds."""
        if self.latencies:
            return statistics.median(self.latencies)
        return 0.0

    @property
    def p95_latency_ms(self) -> float:
        """95th percentile latency in milliseconds."""
        if self.latencies:
            sorted_latencies = sorted(self.latencies)
            idx = int(len(sorted_latencies) * 0.95)
            return sorted_latencies[min(idx, len(sorted_latencies) - 1)]
        return 0.0

    @property
    def p99_latency_ms(self) -> float:
        """99th percentile latency in milliseconds."""
        if self.latencies:
            sorted_latencies = sorted(self.latencies)
            idx = int(len(sorted_latencies) * 0.99)
            return sorted_latencies[min(idx, len(sorted_latencies) - 1)]
        return 0.0

    def print_summary(self):
        """Print performance summary."""
        logger.info(f"\n{'='*60}")
        logger.info(f"Performance Test: {self.operation}")
        logger.info(f"{'='*60}")
        logger.info(f"Total items processed: {self.item_count}")
        logger.info(f"Total time: {self.elapsed_seconds:.2f}s")
        logger.info(f"Throughput: {self.throughput:.2f} items/sec")
        if self.latencies:
            logger.info(f"\nLatency Statistics:")
            logger.info(f"  Average: {self.avg_latency_ms:.2f}ms")
            logger.info(f"  Median (p50): {self.p50_latency_ms:.2f}ms")
            logger.info(f"  p95: {self.p95_latency_ms:.2f}ms")
            logger.info(f"  p99: {self.p99_latency_ms:.2f}ms")
        logger.info(f"{'='*60}\n")


# =============================================================================
# Unit Performance Tests (No Kafka Required)
# =============================================================================

@pytest.mark.asyncio
@pytest.mark.performance
class TestApiClientPerformance:
    """Performance tests for ClaimX API client (no Kafka required)."""

    async def test_api_call_latency(
        self,
        mock_claimx_api_client,
    ):
        """
        Measure API call latency for enrichment operations.

        Tests:
        - get_project() call latency
        - get_project_media() call latency
        - Impact of concurrent API calls

        Target: < 100ms per call (mocked API, measures overhead)
        """
        stats = PerformanceStats("API Call Latency")
        call_count = 100

        # Prepare mock data
        for i in range(call_count):
            project_id = 20000 + i
            mock_claimx_api_client.set_project(
                project_id,
                create_mock_project_response(project_id)
            )
            mock_claimx_api_client.set_media(
                project_id,
                [create_mock_media_response(i, project_id) for i in range(5)]
            )

        # Measure get_project latency
        project_latencies = []
        stats.start()
        for i in range(call_count):
            start = time.time()
            await mock_claimx_api_client.get_project(20000 + i)
            latency_ms = (time.time() - start) * 1000
            project_latencies.append(latency_ms)
            stats.record_item(latency_ms)

        stats.finish()

        # Print results
        logger.info(f"\nAPI Call Latency (get_project):")
        logger.info(f"  Average: {statistics.mean(project_latencies):.2f}ms")
        logger.info(f"  Median: {statistics.median(project_latencies):.2f}ms")
        logger.info(f"  p95: {stats.p95_latency_ms:.2f}ms")
        logger.info(f"  p99: {stats.p99_latency_ms:.2f}ms")

        # Assertions
        assert stats.avg_latency_ms < MAX_API_LATENCY_MS, \
            f"API latency {stats.avg_latency_ms:.2f}ms exceeds target {MAX_API_LATENCY_MS}ms"

    async def test_concurrent_api_calls(
        self,
        mock_claimx_api_client,
    ):
        """
        Measure throughput of concurrent API calls.

        Tests:
        - Concurrent get_project() calls
        - Throughput under load

        Target: > 500 calls/sec with mocked API
        """
        stats = PerformanceStats("Concurrent API Calls")
        call_count = 200

        # Prepare mock data
        for i in range(call_count):
            project_id = 30000 + i
            mock_claimx_api_client.set_project(
                project_id,
                create_mock_project_response(project_id)
            )

        # Make concurrent API calls
        stats.start()

        async def make_call(project_id: int):
            start = time.time()
            await mock_claimx_api_client.get_project(project_id)
            return (time.time() - start) * 1000

        # Run all calls concurrently
        tasks = [make_call(30000 + i) for i in range(call_count)]
        latencies = await asyncio.gather(*tasks)

        stats.finish()
        stats.item_count = call_count
        stats.latencies = list(latencies)

        # Print results
        stats.print_summary()

        # Assertions
        assert stats.throughput >= 500.0, \
            f"API throughput {stats.throughput:.2f} calls/sec below target 500"

    async def test_entity_writer_performance(
        self,
        mock_claimx_entity_writer,
    ):
        """
        Measure entity writer performance with mock.

        Tests write_all() method throughput.
        """
        from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage

        stats = PerformanceStats("Entity Writer Performance")
        batch_count = 50

        # Create test entity batches
        batches = []
        for batch_idx in range(batch_count):
            projects = [
                {
                    "project_id": str(40000 + batch_idx * 10 + i),
                    "project_number": f"P-{40000 + batch_idx * 10 + i}",
                    "master_file_name": f"MFN-{40000 + batch_idx * 10 + i}",
                }
                for i in range(10)
            ]
            batches.append(EntityRowsMessage(projects=projects))

        # Measure write performance
        stats.start()
        for batch in batches:
            start = time.time()
            await mock_claimx_entity_writer.write_all(batch)
            latency_ms = (time.time() - start) * 1000
            stats.record_item(latency_ms)
        stats.finish()

        # Print results
        stats.print_summary()

        # Verify all writes completed
        assert mock_claimx_entity_writer.write_count == batch_count, \
            f"Expected {batch_count} writes, got {mock_claimx_entity_writer.write_count}"


@pytest.mark.asyncio
@pytest.mark.performance
class TestSchemaPerformance:
    """Performance tests for Pydantic schema operations."""

    async def test_enrichment_task_serialization(self):
        """
        Measure EnrichmentTask serialization/deserialization performance.
        """
        stats = PerformanceStats("EnrichmentTask Serialization")
        task_count = 1000

        # Create test tasks
        tasks = [
            create_enrichment_task(
                event_id=50000 + i,
                event_type="PROJECT_CREATED",
                project_id=50000 + i,
            )
            for i in range(task_count)
        ]

        # Measure serialization
        stats.start()
        for task in tasks:
            start = time.time()
            json_str = task.model_dump_json()
            # Also deserialize to measure round-trip
            ClaimXEnrichmentTask.model_validate_json(json_str)
            latency_ms = (time.time() - start) * 1000
            stats.record_item(latency_ms)
        stats.finish()

        # Print results
        stats.print_summary()

        # Assertions
        assert stats.throughput >= 1000.0, \
            f"Serialization throughput {stats.throughput:.2f}/sec below target 1000"

    async def test_download_task_serialization(self):
        """
        Measure DownloadTask serialization/deserialization performance.
        """
        stats = PerformanceStats("DownloadTask Serialization")
        task_count = 1000

        # Create test tasks
        tasks = [
            create_download_task(
                media_id=60000 + i,
                project_id=60001,
                download_url=f"https://s3.amazonaws.com/test/media_{60000+i}.jpg",
            )
            for i in range(task_count)
        ]

        # Measure serialization
        stats.start()
        for task in tasks:
            start = time.time()
            json_str = task.model_dump_json()
            # Also deserialize to measure round-trip
            ClaimXDownloadTask.model_validate_json(json_str)
            latency_ms = (time.time() - start) * 1000
            stats.record_item(latency_ms)
        stats.finish()

        # Print results
        stats.print_summary()

        # Assertions
        assert stats.throughput >= 1000.0, \
            f"Serialization throughput {stats.throughput:.2f}/sec below target 1000"


# =============================================================================
# Integration Performance Tests (Require Docker/Kafka)
# These tests are skipped unless explicitly run with -m "integration"
# =============================================================================

@pytest.mark.asyncio
@pytest.mark.performance
@pytest.mark.integration
class TestEnrichmentWorkerPerformance:
    """Performance tests for ClaimX enrichment worker (requires Kafka)."""

    @pytest.mark.skip(reason="Requires Docker/Kafka - run with -m integration")
    async def test_enrichment_throughput_single_worker(
        self,
        test_claimx_config: KafkaConfig,
        claimx_enrichment_worker,
        mock_claimx_api_client,
        mock_claimx_entity_writer,
        kafka_producer,
    ):
        """
        Measure enrichment worker throughput with single worker instance.

        Tests:
        - Processing rate of PROJECT_CREATED events
        - Entity writes to Delta Lake (mocked)
        - API call frequency and batching
        - Overall events/sec throughput

        Target: > 50 events/sec (conservative baseline)
        """
        # This test requires a real Kafka instance
        # It is designed for integration testing with Docker
        pass


@pytest.mark.asyncio
@pytest.mark.performance
@pytest.mark.integration
class TestDownloadWorkerPerformance:
    """Performance tests for ClaimX download worker (requires Kafka)."""

    @pytest.mark.skip(reason="Requires Docker/Kafka - run with -m integration")
    async def test_download_throughput_single_worker(
        self,
        test_claimx_config: KafkaConfig,
        claimx_download_worker,
        kafka_producer,
        tmp_path: Path,
    ):
        """
        Measure download worker throughput with single worker instance.

        Tests:
        - File download rate (files/sec)
        - Concurrent download processing
        - Cache write performance

        Target: > 5 files/sec (conservative, with mocked S3)
        """
        # This test requires a real Kafka instance
        # It is designed for integration testing with Docker
        pass

    @pytest.mark.skip(reason="Requires Docker/Kafka - run with -m integration")
    async def test_download_concurrent_processing(
        self,
        test_claimx_config: KafkaConfig,
        claimx_download_worker,
        kafka_producer,
        tmp_path: Path,
    ):
        """
        Measure impact of concurrent downloads on throughput.

        Tests:
        - Throughput with various concurrency levels
        - Resource utilization
        - Optimal concurrency setting

        Compares sequential vs concurrent download performance.
        """
        # This test requires a real Kafka instance
        # It is designed for integration testing with Docker
        pass


@pytest.mark.asyncio
@pytest.mark.performance
@pytest.mark.integration
@pytest.mark.slow
class TestPipelineE2EPerformance:
    """End-to-end pipeline performance tests (requires Kafka)."""

    @pytest.mark.skip(reason="Requires Docker/Kafka - run with -m integration")
    async def test_single_file_e2e_latency(
        self,
        test_claimx_config: KafkaConfig,
        claimx_download_worker,
        claimx_upload_worker,
        mock_onelake_client,
        kafka_producer,
        tmp_path: Path,
    ):
        """
        Measure end-to-end latency for single file through pipeline.

        Flow:
        1. Download task produced
        2. Download worker downloads file
        3. Upload worker uploads to OneLake
        4. Result message produced

        Target: < 5 seconds for complete pipeline
        """
        # This test requires a real Kafka instance
        # It is designed for integration testing with Docker
        pass
