"""
Pytest fixtures for Kafka pipeline tests.

Provides fixtures for:
- Mock storage classes (available for all tests)
- In-memory Delta table implementations (for E2E testing without external deps)
- Docker-based Kafka test containers (only for integration tests)
- Test Kafka configuration
- Producer and consumer instances
- Topic management

IMPORTANT: Docker/Kafka fixtures only run when tests are marked with
@pytest.mark.integration. Unit tests will not trigger Docker dependencies.
"""

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncGenerator, Dict, Generator, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from kafka_pipeline.common.storage.inmemory_delta import (
    InMemoryDeltaTable,
    InMemoryDeltaTableWriter,
    InMemoryDeltaRegistry,
)


# =============================================================================
# Mock Storage Classes - Available for ALL tests (unit and integration)
# These are defined FIRST to ensure they're available without Docker
# =============================================================================


class MockOneLakeClient:
    """Mock OneLake client for testing without Azure dependencies."""

    def __init__(self, base_path: str = ""):
        self.base_path = base_path
        self.uploaded_files: Dict[str, bytes] = {}
        self.upload_count = 0
        self.is_open = False

    async def __aenter__(self):
        self.is_open = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.is_open = False
        return False

    async def upload_file(self, relative_path: str, local_path: Path, overwrite: bool = True) -> str:
        if not local_path.exists():
            raise FileNotFoundError(f"Local file not found: {local_path}")
        content = local_path.read_bytes()
        self.uploaded_files[relative_path] = content
        self.upload_count += 1
        return relative_path

    async def exists(self, blob_path: str) -> bool:
        return blob_path in self.uploaded_files

    async def close(self) -> None:
        self.is_open = False

    def get_uploaded_content(self, blob_path: str) -> Optional[bytes]:
        return self.uploaded_files.get(blob_path)

    def clear(self) -> None:
        self.uploaded_files.clear()
        self.upload_count = 0


class MockDeltaEventsWriter:
    """Mock Delta events writer for testing without Delta Lake dependencies."""

    def __init__(self, table_path: str = "", dedupe_window_hours: int = 24):
        self.table_path = table_path
        self.dedupe_window_hours = dedupe_window_hours
        self.written_events: List[Dict] = []
        self.write_count = 0
        self.dedupe_hits = 0
        self._seen_trace_ids = set()

    async def write_event(self, event: Dict) -> None:
        if hasattr(event, "model_dump"):
            event_dict = event.model_dump(mode="json")
            trace_id = event.trace_id
        else:
            event_dict = event
            trace_id = event.get("trace_id")

        if trace_id in self._seen_trace_ids:
            self.dedupe_hits += 1
            return

        self._seen_trace_ids.add(trace_id)
        self.written_events.append(event_dict)
        self.write_count += 1

    def get_events_by_trace_id(self, trace_id: str) -> List[Dict]:
        return [e for e in self.written_events if e.get("trace_id") == trace_id]

    def clear(self) -> None:
        self.written_events.clear()
        self.write_count = 0
        self.dedupe_hits = 0
        self._seen_trace_ids.clear()


class MockDeltaInventoryWriter:
    """Mock Delta inventory writer for testing without Delta Lake dependencies."""

    def __init__(self, table_path: str = ""):
        self.table_path = table_path
        self.inventory_records: List[Dict] = []
        self.write_count = 0
        self.merge_count = 0
        self._record_keys = {}

    async def write_results(self, results: List) -> bool:
        for result in results:
            if hasattr(result, "model_dump"):
                record = result.model_dump(mode="json")
                trace_id = result.trace_id
                attachment_url = result.attachment_url
            else:
                record = result
                trace_id = result.get("trace_id")
                attachment_url = result.get("attachment_url")

            key = (trace_id, attachment_url)

            if key in self._record_keys:
                idx = self._record_keys[key]
                self.inventory_records[idx] = record
                self.merge_count += 1
            else:
                self._record_keys[key] = len(self.inventory_records)
                self.inventory_records.append(record)

        self.write_count += 1
        return True

    async def write_batch(self, results: List) -> bool:
        """Alias for write_results for backward compatibility."""
        return await self.write_results(results)

    def get_records_by_trace_id(self, trace_id: str) -> List[Dict]:
        return [r for r in self.inventory_records if r.get("trace_id") == trace_id]

    def clear(self) -> None:
        self.inventory_records.clear()
        self.write_count = 0
        self.merge_count = 0
        self._record_keys.clear()


# =============================================================================
# Mock Fixtures - Available for ALL tests (no Docker required)
# =============================================================================


@pytest.fixture
def mock_onelake_client() -> MockOneLakeClient:
    """Provide mock OneLake client for testing (no Docker required)."""
    return MockOneLakeClient(base_path="test/attachments")


@pytest.fixture
def mock_delta_events_writer() -> MockDeltaEventsWriter:
    """Provide mock Delta events writer for testing (no Docker required)."""
    return MockDeltaEventsWriter(
        table_path="abfss://test@onelake.dfs.fabric.microsoft.com/lakehouse/Tables/xact_events",
        dedupe_window_hours=24,
    )


@pytest.fixture
def mock_delta_inventory_writer() -> MockDeltaInventoryWriter:
    """Provide mock Delta inventory writer for testing (no Docker required)."""
    return MockDeltaInventoryWriter(
        table_path="abfss://test@onelake.dfs.fabric.microsoft.com/lakehouse/Tables/xact_attachments"
    )


@pytest.fixture
def mock_storage(
    mock_onelake_client: MockOneLakeClient,
    mock_delta_events_writer: MockDeltaEventsWriter,
    mock_delta_inventory_writer: MockDeltaInventoryWriter,
) -> Dict[str, object]:
    """Provide all mock storage components as a dict (no Docker required)."""
    return {
        "onelake": mock_onelake_client,
        "delta_events": mock_delta_events_writer,
        "delta_inventory": mock_delta_inventory_writer,
    }


# =============================================================================
# In-Memory Delta Tables - For E2E testing with full DataFrame support
# =============================================================================


@pytest.fixture
def inmemory_delta_registry() -> InMemoryDeltaRegistry:
    """
    Provide a registry for managing multiple in-memory Delta tables.

    The registry allows creating and accessing multiple tables by name,
    and provides easy reset between tests.

    Usage:
        def test_something(inmemory_delta_registry):
            events = inmemory_delta_registry.get_table("events")
            inventory = inmemory_delta_registry.get_table("inventory")
    """
    registry = InMemoryDeltaRegistry()
    yield registry
    registry.reset()


@pytest.fixture
def inmemory_xact_events() -> InMemoryDeltaTable:
    """
    Provide in-memory Delta table for XACT events.

    Configured to match production xact_events table structure.
    """
    table = InMemoryDeltaTable(
        table_path="inmemory://xact_events",
        timestamp_column="created_at",
        partition_column="event_date",
        z_order_columns=["event_date", "trace_id", "event_id", "type"],
    )
    yield table
    table.clear()


@pytest.fixture
def inmemory_xact_attachments() -> InMemoryDeltaTable:
    """
    Provide in-memory Delta table for XACT attachments/inventory.

    Configured to match production xact_attachments table structure.
    """
    table = InMemoryDeltaTable(
        table_path="inmemory://xact_attachments",
        timestamp_column="created_at",
        partition_column="created_date",
    )
    yield table
    table.clear()


@pytest.fixture
def inmemory_claimx_events() -> InMemoryDeltaTable:
    """
    Provide in-memory Delta table for ClaimX events.

    Configured to match production claimx_events table structure.
    """
    table = InMemoryDeltaTable(
        table_path="inmemory://claimx_events",
        timestamp_column="ingested_at",
        partition_column="event_date",
        z_order_columns=["project_id"],
    )
    yield table
    table.clear()


@pytest.fixture
def inmemory_delta_storage(
    inmemory_xact_events: InMemoryDeltaTable,
    inmemory_xact_attachments: InMemoryDeltaTable,
    inmemory_claimx_events: InMemoryDeltaTable,
    mock_onelake_client: MockOneLakeClient,
) -> Dict[str, object]:
    """
    Provide all in-memory storage components for E2E testing.

    This fixture combines in-memory Delta tables with mock OneLake
    for complete pipeline testing without external dependencies.

    Usage:
        def test_e2e(inmemory_delta_storage):
            xact_events = inmemory_delta_storage["xact_events"]
            xact_attachments = inmemory_delta_storage["xact_attachments"]
            claimx_events = inmemory_delta_storage["claimx_events"]
            onelake = inmemory_delta_storage["onelake"]
    """
    return {
        "xact_events": inmemory_xact_events,
        "xact_attachments": inmemory_xact_attachments,
        "claimx_events": inmemory_claimx_events,
        "onelake": mock_onelake_client,
    }


# =============================================================================
# Helper function to check if running integration tests
# =============================================================================


def _is_integration_test(request) -> bool:
    """Check if the current test is marked as an integration test."""
    # Check if any item in the session has integration marker
    if hasattr(request, "node"):
        # Check if the current test has integration marker
        if request.node.get_closest_marker("integration"):
            return True
        # Also check parent for parametrized tests
        if hasattr(request.node, "parent") and request.node.parent:
            if hasattr(request.node.parent, "get_closest_marker"):
                if request.node.parent.get_closest_marker("integration"):
                    return True
    return False


def _session_has_integration_tests(session) -> bool:
    """Check if the test session contains any integration tests."""
    for item in session.items:
        if item.get_closest_marker("integration"):
            return True
    return False


# =============================================================================
# Docker/Kafka Fixtures - ONLY run for integration tests
# =============================================================================


@pytest.fixture(scope="session")
def kafka_container(request) -> Generator:
    """
    Provide a Kafka container for integration tests.

    Uses Testcontainers to start a real Kafka instance in Docker.
    The container runs for the entire test session and is shared across tests.

    IMPORTANT: This fixture only starts Docker when tests are marked with
    @pytest.mark.integration. Unit tests will not trigger Docker.

    Yields:
        KafkaContainer: Started Kafka container instance, or None for unit tests
    """
    # Check if the session contains any integration tests
    if not _session_has_integration_tests(request.session):
        # No integration tests - skip Docker startup entirely
        yield None
        return

    # Import testcontainers only when needed (avoids import errors if not installed)
    try:
        from testcontainers.kafka import KafkaContainer
    except ImportError:
        pytest.skip("testcontainers not installed - skipping integration tests")
        return

    # Create and start Kafka container
    # Uses Confluent Kafka image which includes Zookeeper
    kafka = KafkaContainer()
    kafka.start()

    # Set environment variable for KafkaConfig.from_env()
    # This allows tests to use the test container's bootstrap server
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = kafka.get_bootstrap_server()
    # Disable auth for local testing
    os.environ["KAFKA_SECURITY_PROTOCOL"] = "PLAINTEXT"
    os.environ["KAFKA_SASL_MECHANISM"] = "PLAIN"
    # Set allowed domains for URL validation in tests
    os.environ["ALLOWED_ATTACHMENT_DOMAINS"] = "example.com,claimxperience.com,www.claimxperience.com,claimxperience.s3.amazonaws.com,claimxperience.s3.us-east-1.amazonaws.com"

    yield kafka

    # Cleanup: stop container after all tests complete
    kafka.stop()

    # Clean up environment variables
    os.environ.pop("KAFKA_BOOTSTRAP_SERVERS", None)
    os.environ.pop("KAFKA_SECURITY_PROTOCOL", None)
    os.environ.pop("KAFKA_SASL_MECHANISM", None)
    os.environ.pop("ALLOWED_ATTACHMENT_DOMAINS", None)


@pytest.fixture
def kafka_config(kafka_container):
    """
    Provide test Kafka configuration.

    Creates configuration that points to the test container with
    simplified security settings for local testing.

    Args:
        kafka_container: Test Kafka container fixture

    Returns:
        KafkaConfig: Configuration for test environment
    """
    if kafka_container is None:
        pytest.skip("Kafka container not available - this fixture requires @pytest.mark.integration")

    from kafka_pipeline.config import KafkaConfig

    # Create config from environment (which includes container bootstrap server)
    config = KafkaConfig.from_env()

    # Override security settings for local testing
    config.security_protocol = "PLAINTEXT"
    config.sasl_mechanism = "PLAIN"

    return config


@pytest.fixture
async def kafka_producer(
    kafka_config,
) -> AsyncGenerator:
    """
    Provide a started Kafka producer for tests.

    Creates and starts a producer instance that connects to the test container.
    Automatically stops and cleans up after the test.

    Args:
        kafka_config: Test Kafka configuration

    Yields:
        BaseKafkaProducer: Started producer instance
    """
    from kafka_pipeline.common.producer import BaseKafkaProducer

    producer = BaseKafkaProducer(config=kafka_config)
    await producer.start()

    yield producer

    # Cleanup: stop producer after test
    await producer.stop()


@pytest.fixture
def unique_topic_prefix(request) -> str:
    """
    Generate unique topic prefix for test isolation.

    Creates a unique prefix based on the test name to ensure
    tests don't interfere with each other by using different topics.

    Args:
        request: Pytest request fixture for test metadata

    Returns:
        str: Unique topic prefix (e.g., "test_produce_consume_123")
    """
    # Use test name to create unique prefix
    test_name = request.node.name
    # Remove special characters that aren't allowed in Kafka topic names
    safe_name = "".join(c if c.isalnum() or c == "_" else "_" for c in test_name)
    return safe_name.lower()[:100]  # Limit length


@pytest.fixture
def test_topics(
    kafka_config, unique_topic_prefix: str
) -> dict[str, str]:
    """
    Provide unique test topic names.

    Creates a mapping of logical topic names to actual unique topic names
    for test isolation. Each test gets its own set of topics.

    Args:
        kafka_config: Test Kafka configuration
        unique_topic_prefix: Unique prefix for this test

    Returns:
        dict: Mapping of logical name to actual topic name
    """
    return {
        "pending": f"{unique_topic_prefix}.downloads.pending",
        "results": f"{unique_topic_prefix}.downloads.results",
        "retry_5m": f"{unique_topic_prefix}.downloads.pending.retry.5m",
        "retry_10m": f"{unique_topic_prefix}.downloads.pending.retry.10m",
        "retry_20m": f"{unique_topic_prefix}.downloads.pending.retry.20m",
        "retry_40m": f"{unique_topic_prefix}.downloads.pending.retry.40m",
        "dlq": f"{unique_topic_prefix}.downloads.dlq",
    }


@pytest.fixture
def test_kafka_config(
    kafka_config, unique_topic_prefix: str
):
    """
    Provide test-specific Kafka configuration with unique topic names.

    Updates the config to use test-specific topic names for proper
    isolation between tests.

    Args:
        kafka_config: Base test Kafka configuration
        unique_topic_prefix: Unique prefix for this test

    Returns:
        KafkaConfig: Configuration with test-specific topic names
    """
    # Create a copy and update topic names
    config = kafka_config
    config.downloads_pending_topic = f"{unique_topic_prefix}.downloads.pending"
    config.downloads_results_topic = f"{unique_topic_prefix}.downloads.results"
    config.dlq_topic = f"{unique_topic_prefix}.downloads.dlq"

    return config


@pytest.fixture
async def kafka_consumer_factory(
    kafka_config,
) -> AsyncGenerator[callable, None]:
    """
    Factory fixture for creating Kafka consumers.

    Provides a factory function that creates and tracks consumer instances.
    All created consumers are automatically stopped and cleaned up after the test.

    Args:
        kafka_config: Test Kafka configuration

    Yields:
        callable: Factory function that creates consumers
    """
    from kafka_pipeline.common.consumer import BaseKafkaConsumer

    created_consumers = []

    async def create_consumer(
        topics: list[str],
        group_id: str,
        message_handler,
    ):
        """
        Create and start a consumer for testing.

        Args:
            topics: List of topics to subscribe to
            group_id: Consumer group ID
            message_handler: Async callback for processing messages

        Returns:
            BaseKafkaConsumer: Started consumer instance
        """
        consumer = BaseKafkaConsumer(
            config=kafka_config,
            topics=topics,
            group_id=group_id,
            message_handler=message_handler,
        )
        # Note: Don't start() here - tests control when to start
        # This allows tests to set up handlers before consumption begins
        created_consumers.append(consumer)
        return consumer

    yield create_consumer

    # Cleanup: stop all created consumers
    for consumer in created_consumers:
        if consumer.is_running:
            await consumer.stop()


@pytest.fixture
async def message_collector() -> callable:
    """
    Provide a message collector for testing.

    Creates a collector that accumulates messages consumed by a consumer.
    Useful for asserting on consumed messages in tests.

    Returns:
        callable: Message handler that collects messages
    """
    messages = []

    async def collect(record):
        """Collect consumed message."""
        messages.append(record)

    # Attach messages list to the function for easy access in tests
    collect.messages = messages

    return collect


@pytest.fixture(scope="session")
def wait_for_kafka_ready(kafka_container):
    """
    Wait for Kafka to be ready before running tests.

    Ensures the Kafka container is fully started and accepting connections.
    NOTE: This is NOT autouse - only runs when explicitly requested or
    when a dependent fixture is used.

    Args:
        kafka_container: Test Kafka container fixture
    """
    if kafka_container is None:
        # Not running integration tests
        return

    # The container's start() method waits for basic readiness
    # No additional wait needed - testcontainers handles this
    pass


# =============================================================================
# Integration Test Worker Fixtures - Require Docker/Kafka
# =============================================================================


@pytest.fixture
def mock_onelake_client_with_config(kafka_config) -> MockOneLakeClient:
    """Provide mock OneLake client with config-based path (requires Kafka)."""
    return MockOneLakeClient(base_path=kafka_config.onelake_base_path)


@pytest.fixture
async def event_ingester_worker(
    test_kafka_config,
    mock_delta_events_writer: MockDeltaEventsWriter,
    monkeypatch,
) -> AsyncGenerator:
    """Provide event ingester worker with mocked Delta writer."""
    from kafka_pipeline.xact.workers.event_ingester import EventIngesterWorker

    monkeypatch.setattr(
        "kafka_pipeline.xact.workers.event_ingester.DeltaEventsWriter",
        lambda *args, **kwargs: mock_delta_events_writer
    )

    worker = EventIngesterWorker(
        config=test_kafka_config,
        enable_delta_writes=True
    )

    yield worker

    if worker.consumer and worker.consumer.is_running:
        await worker.stop()


@pytest.fixture
async def download_worker(
    test_kafka_config,
    tmp_path: Path,
) -> AsyncGenerator:
    """Provide download worker for testing.

    Note: DownloadWorker no longer uses OneLakeClient directly.
    It caches files locally and produces CachedDownloadMessage for upload worker.
    """
    from kafka_pipeline.xact.workers.download_worker import DownloadWorker

    worker = DownloadWorker(
        config=test_kafka_config,
        temp_dir=tmp_path / "downloads"
    )

    yield worker

    if worker.is_running:
        await worker.stop()


@pytest.fixture
async def result_processor(
    test_kafka_config,
    mock_delta_inventory_writer: MockDeltaInventoryWriter,
    monkeypatch,
) -> AsyncGenerator:
    """Provide result processor with mocked Delta writer."""
    from kafka_pipeline.xact.workers.result_processor import ResultProcessor

    monkeypatch.setattr(
        "kafka_pipeline.xact.workers.result_processor.DeltaInventoryWriter",
        lambda *args, **kwargs: mock_delta_inventory_writer
    )

    processor = ResultProcessor(
        config=test_kafka_config,
        inventory_table_path="abfss://test@onelake.dfs.fabric.microsoft.com/lakehouse/Tables/xact_attachments",
        batch_size=10,
        batch_timeout_seconds=1.0,
    )

    yield processor

    if processor._consumer.is_running:
        await processor.stop()


@pytest.fixture
def all_workers(
    event_ingester_worker,
    download_worker,
    result_processor,
) -> Dict[str, object]:
    """Provide all workers as a dict for E2E tests."""
    return {
        "event_ingester": event_ingester_worker,
        "download_worker": download_worker,
        "result_processor": result_processor,
    }
