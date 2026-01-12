"""
Pytest configuration and fixtures for ClaimX performance tests.

Provides all necessary ClaimX fixtures for performance testing, including:
- Mock ClaimX API client
- Mock Delta Lake writers (events and entities)
- ClaimX worker instances (enrichment worker, download worker, upload worker)
- Kafka configuration with ClaimX topics
"""

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncGenerator, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage


# Set environment variables for performance tests
if "TEST_MODE" not in os.environ:
    os.environ["TEST_MODE"] = "true"

if "CLAIMX_API_TOKEN" not in os.environ:
    os.environ["CLAIMX_API_TOKEN"] = "test-api-token"


# =============================================================================
# Mock Classes for ClaimX Performance Tests
# =============================================================================


class MockClaimXApiClient:
    """Mock ClaimX API client for testing without external API dependencies."""

    def __init__(self, api_url: str = "", username: str = "", password: str = "", **kwargs):
        self.api_url = api_url
        self.username = username
        self.password = password
        self.is_closed = False
        self.request_count = 0
        self.failed_requests = 0
        self.is_circuit_open = False

        # Mock response data
        self.mock_projects: Dict[int, Dict] = {}
        self.mock_contacts: Dict[int, Dict] = {}
        self.mock_media: Dict[int, List[Dict]] = {}
        self.mock_tasks: Dict[int, Dict] = {}
        self.mock_video_collab: Dict[int, Dict] = {}
        self.mock_conversations: Dict[int, List[Dict]] = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False

    async def close(self):
        self.is_closed = True

    async def _ensure_session(self):
        """Ensure HTTP session is ready (mock implementation)."""
        pass

    def set_project(self, project_id: int, data: Dict):
        """Set mock data for a project."""
        self.mock_projects[project_id] = data

    def set_contact(self, contact_id: int, data: Dict):
        """Set mock data for a contact."""
        self.mock_contacts[contact_id] = data

    def set_media(self, project_id: int, data: List[Dict]):
        """Set mock data for media items."""
        self.mock_media[project_id] = data

    def set_task(self, task_id: int, data: Dict):
        """Set mock data for a task."""
        self.mock_tasks[task_id] = data

    def set_video_collab(self, project_id: int, data: Dict):
        """Set mock data for video collaboration."""
        self.mock_video_collab[project_id] = data

    def set_conversations(self, project_id: int, data: List[Dict]):
        """Set mock data for conversations."""
        self.mock_conversations[project_id] = data

    async def get_project(self, project_id: int) -> Optional[Dict]:
        """Get project details."""
        self.request_count += 1
        return self.mock_projects.get(project_id)

    async def get_contact(self, contact_id: int) -> Optional[Dict]:
        """Get contact details."""
        self.request_count += 1
        return self.mock_contacts.get(contact_id)

    async def get_media(self, project_id: int) -> List[Dict]:
        """Get media items for a project."""
        self.request_count += 1
        return self.mock_media.get(project_id, [])

    async def get_task(self, task_id: int) -> Optional[Dict]:
        """Get task details."""
        self.request_count += 1
        return self.mock_tasks.get(task_id)

    async def get_video_collaboration(self, project_id: int) -> Optional[Dict]:
        """Get video collaboration details."""
        self.request_count += 1
        return self.mock_video_collab.get(project_id)

    async def get_conversations(self, project_id: int) -> List[Dict]:
        """Get conversations for a project."""
        self.request_count += 1
        return self.mock_conversations.get(project_id, [])

    def clear(self):
        """Clear all mock data."""
        self.mock_projects.clear()
        self.mock_contacts.clear()
        self.mock_media.clear()
        self.mock_tasks.clear()
        self.mock_video_collab.clear()
        self.mock_conversations.clear()
        self.request_count = 0
        self.failed_requests = 0


class MockClaimXEventsDeltaWriter:
    """Mock Delta events writer for ClaimX events."""

    def __init__(self, table_path: str = ""):
        self.table_path = table_path
        self.written_events: List[Dict] = []
        self.write_count = 0
        self._seen_event_ids = set()

    async def write_event(self, event: Dict) -> None:
        """Write a single event."""
        if hasattr(event, "model_dump"):
            event_dict = event.model_dump(mode="json")
            event_id = event.event_id
        else:
            event_dict = event
            event_id = event.get("event_id")

        # Deduplicate by event_id
        if event_id in self._seen_event_ids:
            return

        self._seen_event_ids.add(event_id)
        self.written_events.append(event_dict)
        self.write_count += 1

    async def write_events(self, events: List) -> bool:
        """Write multiple events."""
        for event in events:
            await self.write_event(event)
        return True

    def get_events_by_event_id(self, event_id: int) -> List[Dict]:
        """Get events by event_id."""
        return [e for e in self.written_events if e.get("event_id") == event_id]

    def get_events_by_project_id(self, project_id: int) -> List[Dict]:
        """Get events by project_id."""
        return [e for e in self.written_events if e.get("project_id") == project_id]

    def clear(self):
        """Clear all written events."""
        self.written_events.clear()
        self.write_count = 0
        self._seen_event_ids.clear()


class MockClaimXEntityWriter:
    """Mock entity writer for ClaimX entities (7 tables)."""

    def __init__(self, **kwargs):
        # One list per entity type
        self.projects: List[Dict] = []
        self.contacts: List[Dict] = []
        self.media: List[Dict] = []
        self.tasks: List[Dict] = []
        self.task_templates: List[Dict] = []
        self.external_links: List[Dict] = []
        self.video_collab: List[Dict] = []

        self.write_count = 0
        self.merge_count = 0

    async def write_all(self, entity_rows: EntityRowsMessage) -> Dict[str, int]:
        """Write entity rows to all tables."""
        counts = {}

        if entity_rows.projects:
            self.projects.extend(entity_rows.projects)
            counts["projects"] = len(entity_rows.projects)
        if entity_rows.contacts:
            self.contacts.extend(entity_rows.contacts)
            counts["contacts"] = len(entity_rows.contacts)
        if entity_rows.media:
            self.media.extend(entity_rows.media)
            counts["media"] = len(entity_rows.media)
        if entity_rows.tasks:
            self.tasks.extend(entity_rows.tasks)
            counts["tasks"] = len(entity_rows.tasks)
        if entity_rows.task_templates:
            self.task_templates.extend(entity_rows.task_templates)
            counts["task_templates"] = len(entity_rows.task_templates)
        if entity_rows.external_links:
            self.external_links.extend(entity_rows.external_links)
            counts["external_links"] = len(entity_rows.external_links)
        if entity_rows.video_collab:
            self.video_collab.extend(entity_rows.video_collab)
            counts["video_collab"] = len(entity_rows.video_collab)

        self.write_count += 1
        return counts

    async def write_entities(self, entity_rows: EntityRowsMessage) -> bool:
        """Write entity rows to all tables (alias for compatibility)."""
        await self.write_all(entity_rows)
        return True

    def get_projects_by_id(self, project_id: int) -> List[Dict]:
        """Get projects by project_id."""
        return [p for p in self.projects if p.get("project_id") == project_id]

    def get_contacts_by_project(self, project_id: int) -> List[Dict]:
        """Get contacts by project_id."""
        return [c for c in self.contacts if c.get("project_id") == project_id]

    def get_media_by_project(self, project_id: int) -> List[Dict]:
        """Get media by project_id."""
        return [m for m in self.media if m.get("project_id") == project_id]

    def clear(self):
        """Clear all entity data."""
        self.projects.clear()
        self.contacts.clear()
        self.media.clear()
        self.tasks.clear()
        self.task_templates.clear()
        self.external_links.clear()
        self.video_collab.clear()
        self.write_count = 0
        self.merge_count = 0


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


# =============================================================================
# Pytest Fixtures
# =============================================================================


@pytest.fixture
def mock_claimx_api_client() -> MockClaimXApiClient:
    """Provide mock ClaimX API client for testing."""
    client = MockClaimXApiClient(
        api_url="https://api.test.claimxperience.com",
        username="test_user",
        password="test_password"
    )
    return client


@pytest.fixture
def mock_claimx_events_writer() -> MockClaimXEventsDeltaWriter:
    """Provide mock ClaimX events writer for testing."""
    writer = MockClaimXEventsDeltaWriter(
        table_path="abfss://test@onelake.dfs.fabric.microsoft.com/lakehouse/Tables/claimx_events"
    )
    return writer


@pytest.fixture
def mock_claimx_entity_writer() -> MockClaimXEntityWriter:
    """Provide mock ClaimX entity writer for testing."""
    return MockClaimXEntityWriter()


@pytest.fixture
def mock_onelake_client() -> MockOneLakeClient:
    """Provide mock OneLake client for testing."""
    return MockOneLakeClient(base_path="test/attachments")


@pytest.fixture
def unique_topic_prefix(request) -> str:
    """Generate unique topic prefix for test isolation."""
    test_name = request.node.name
    safe_name = "".join(c if c.isalnum() or c == "_" else "_" for c in test_name)
    return f"perf_{safe_name.lower()[:80]}"


@pytest.fixture
def test_claimx_config(unique_topic_prefix: str) -> KafkaConfig:
    """
    Provide test-specific Kafka configuration with ClaimX topic names.

    Returns:
        KafkaConfig: Configuration with ClaimX-specific topic names for performance testing
    """
    config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        security_protocol="PLAINTEXT",
        claimx={
            "topics": {
                "events": f"{unique_topic_prefix}.claimx.events.raw",
                "enrichment_pending": f"{unique_topic_prefix}.claimx.enrichment.pending",
                "downloads_pending": f"{unique_topic_prefix}.claimx.downloads.pending",
                "downloads_cached": f"{unique_topic_prefix}.claimx.downloads.cached",
                "downloads_results": f"{unique_topic_prefix}.claimx.downloads.results",
                "entities_rows": f"{unique_topic_prefix}.claimx.entities.rows",
            },
            "consumer_group_prefix": f"{unique_topic_prefix}-claimx",
            "retry_delays": [300, 600, 1200],
            "event_ingester": {
                "consumer": {"max_poll_records": 100},
                "processing": {"health_port": 18084},
            },
            "enrichment_worker": {
                "consumer": {"max_poll_records": 100},
                "processing": {"health_port": 18081, "max_poll_records": 100},
            },
            "download_worker": {
                "consumer": {"max_poll_records": 20},
                "processing": {
                    "concurrency": 10,
                    "batch_size": 20,
                    "health_port": 18082,
                },
            },
            "upload_worker": {
                "consumer": {"max_poll_records": 20},
                "processing": {
                    "concurrency": 10,
                    "batch_size": 20,
                    "health_port": 18083,
                },
            },
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
        onelake_domain_paths={
            "claimx": "abfss://claimx@onelake.dfs.fabric.microsoft.com/lakehouse"
        },
        claimx_api_url="https://test.claimxperience.com/api",
        claimx_api_token="test-token",
        claimx_api_timeout_seconds=30,
        claimx_api_concurrency=20,
    )

    # Set dynamic attributes for backward compatibility
    config.claimx_topic_prefix = f"{unique_topic_prefix}.claimx"
    config.claimx_events_topic = f"{unique_topic_prefix}.claimx.events.raw"
    config.claimx_enrichment_pending_topic = f"{unique_topic_prefix}.claimx.enrichment.pending"
    config.claimx_downloads_pending_topic = f"{unique_topic_prefix}.claimx.downloads.pending"
    config.claimx_downloads_cached_topic = f"{unique_topic_prefix}.claimx.downloads.cached"
    config.claimx_downloads_results_topic = f"{unique_topic_prefix}.claimx.downloads.results"
    config.claimx_dlq_topic = f"{unique_topic_prefix}.claimx.dlq"

    return config


@pytest.fixture
async def claimx_enrichment_worker(
    test_claimx_config: KafkaConfig,
    mock_claimx_api_client: MockClaimXApiClient,
    mock_claimx_entity_writer: MockClaimXEntityWriter,
    monkeypatch,
) -> AsyncGenerator:
    """Provide ClaimX enrichment worker with mocked API client and Delta writer."""
    from kafka_pipeline.claimx.workers.enrichment_worker import ClaimXEnrichmentWorker

    # Mock the API client
    monkeypatch.setattr(
        "kafka_pipeline.claimx.workers.enrichment_worker.ClaimXApiClient",
        lambda *args, **kwargs: mock_claimx_api_client
    )

    # Pass the mock entity writer directly to the worker
    worker = ClaimXEnrichmentWorker(
        config=test_claimx_config,
        entity_writer=mock_claimx_entity_writer,
        enable_delta_writes=True,
        enrichment_topic=test_claimx_config.claimx_enrichment_pending_topic,
        download_topic=test_claimx_config.claimx_downloads_pending_topic,
    )

    yield worker

    if hasattr(worker, '_running') and worker._running:
        await worker.stop()


@pytest.fixture
async def claimx_download_worker(
    test_claimx_config: KafkaConfig,
    tmp_path: Path,
) -> AsyncGenerator:
    """Provide ClaimX download worker for testing."""
    from kafka_pipeline.claimx.workers.download_worker import ClaimXDownloadWorker

    worker = ClaimXDownloadWorker(
        config=test_claimx_config,
        temp_dir=tmp_path / "claimx_downloads"
    )

    yield worker

    if hasattr(worker, 'is_running') and worker.is_running:
        await worker.stop()


@pytest.fixture
async def claimx_upload_worker(
    test_claimx_config: KafkaConfig,
    mock_onelake_client: MockOneLakeClient,
    tmp_path: Path,
    monkeypatch,
) -> AsyncGenerator:
    """Provide ClaimX upload worker with mocked OneLake client."""
    from kafka_pipeline.claimx.workers.upload_worker import ClaimXUploadWorker

    # Mock the OneLake client
    monkeypatch.setattr(
        "kafka_pipeline.claimx.workers.upload_worker.OneLakeClient",
        lambda *args, **kwargs: mock_onelake_client
    )

    worker = ClaimXUploadWorker(
        config=test_claimx_config
    )

    yield worker

    if hasattr(worker, 'is_running') and worker.is_running:
        await worker.stop()


@pytest.fixture
async def kafka_producer(test_claimx_config: KafkaConfig) -> AsyncGenerator:
    """Provide a mock Kafka producer for performance tests."""
    # For performance tests that don't need real Kafka, we provide a mock producer
    from unittest.mock import AsyncMock

    class MockKafkaProducer:
        """Mock Kafka producer for performance testing."""

        def __init__(self):
            self.sent_messages: List[Dict] = []
            self.is_started = True

        async def send(self, topic: str, key=None, value=None, headers=None):
            """Mock send method."""
            if hasattr(value, "model_dump"):
                value_dict = value.model_dump(mode="json")
            else:
                value_dict = value

            self.sent_messages.append({
                "topic": topic,
                "key": key,
                "value": value_dict,
                "headers": headers,
            })

            # Return mock metadata
            mock_metadata = MagicMock()
            mock_metadata.partition = 0
            mock_metadata.offset = len(self.sent_messages) - 1
            return mock_metadata

        async def start(self):
            self.is_started = True

        async def stop(self):
            self.is_started = False

        def clear(self):
            self.sent_messages.clear()

    producer = MockKafkaProducer()
    yield producer
    await producer.stop()


# =============================================================================
# Custom Pytest Markers
# =============================================================================


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "performance: marks tests as performance tests"
    )
    config.addinivalue_line(
        "markers", "slow: marks tests as slow running"
    )
