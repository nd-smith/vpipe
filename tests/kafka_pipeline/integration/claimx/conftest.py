"""
Pytest fixtures for ClaimX integration tests.

Provides fixtures for:
- ClaimX worker instances (event ingester, enrichment worker, download worker, upload worker)
- Mock ClaimX API client
- Mock Delta Lake writers (events and entities)
- Worker lifecycle management
"""

import os
from pathlib import Path
from typing import AsyncGenerator, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage


# =============================================================================
# Mock Classes for ClaimX
# =============================================================================


class MockClaimXApiClient:
    """Mock ClaimX API client for testing without external API dependencies."""

    def __init__(self, api_url: str, username: str, password: str):
        self.api_url = api_url
        self.username = username
        self.password = password
        self.is_closed = False
        self.request_count = 0
        self.failed_requests = 0

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

    def is_circuit_open(self) -> bool:
        """Check if circuit breaker is open (mock always returns False)."""
        return False

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

    def __init__(self, table_path: str):
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

    def __init__(self):
        # One list per entity type
        self.projects: List[Dict] = []
        self.contacts: List[Dict] = []
        self.media: List[Dict] = []
        self.tasks: List[Dict] = []
        self.task_templates: List[Dict] = []
        self.task_links: List[Dict] = []
        self.video_collaborations: List[Dict] = []

        self.write_count = 0
        self.merge_count = 0

    async def write_entities(self, entity_rows: EntityRowsMessage) -> bool:
        """Write entity rows to all tables."""
        if hasattr(entity_rows, "model_dump"):
            rows = entity_rows.model_dump(mode="json")
        else:
            rows = entity_rows

        # Append to respective lists (simplified - no merging logic for mock)
        if rows.get("projects"):
            self.projects.extend(rows["projects"])
        if rows.get("contacts"):
            self.contacts.extend(rows["contacts"])
        if rows.get("media"):
            self.media.extend(rows["media"])
        if rows.get("tasks"):
            self.tasks.extend(rows["tasks"])
        if rows.get("task_templates"):
            self.task_templates.extend(rows["task_templates"])
        if rows.get("task_links"):
            self.task_links.extend(rows["task_links"])
        if rows.get("video_collaborations"):
            self.video_collaborations.extend(rows["video_collaborations"])

        self.write_count += 1
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
        self.task_links.clear()
        self.video_collaborations.clear()
        self.write_count = 0
        self.merge_count = 0


# =============================================================================
# Fixtures
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
def test_claimx_config(kafka_config: KafkaConfig, unique_topic_prefix: str) -> KafkaConfig:
    """
    Provide test-specific Kafka configuration with ClaimX topic names.

    Args:
        kafka_config: Base test Kafka configuration
        unique_topic_prefix: Unique prefix for this test

    Returns:
        KafkaConfig: Configuration with ClaimX-specific topic names
    """
    # Create a copy and update ClaimX topic names
    config = kafka_config

    # Set ClaimX domain prefix
    config.claimx_topic_prefix = f"{unique_topic_prefix}.claimx"

    # ClaimX-specific topics
    # Events topic
    events_topic = f"{config.claimx_topic_prefix}.events.raw"
    # Enrichment topics
    enrichment_pending_topic = f"{config.claimx_topic_prefix}.enrichment.pending"
    # Download topics
    downloads_pending_topic = f"{config.claimx_topic_prefix}.downloads.pending"
    downloads_cached_topic = f"{config.claimx_topic_prefix}.downloads.cached"
    downloads_results_topic = f"{config.claimx_topic_prefix}.downloads.results"
    # DLQ topic
    dlq_topic = f"{config.claimx_topic_prefix}.dlq"

    # Store in config (these attributes would need to be added to KafkaConfig)
    # For now, we'll use a simpler approach with get_topic()
    config.events_topic = events_topic  # Used by event ingester
    config.claimx_events_topic = events_topic
    config.claimx_enrichment_pending_topic = enrichment_pending_topic
    config.claimx_downloads_pending_topic = downloads_pending_topic
    config.claimx_downloads_cached_topic = downloads_cached_topic
    config.claimx_downloads_results_topic = downloads_results_topic
    config.claimx_dlq_topic = dlq_topic

    return config


@pytest.fixture
async def claimx_event_ingester(
    test_claimx_config: KafkaConfig,
    mock_claimx_events_writer: MockClaimXEventsDeltaWriter,
    monkeypatch,
) -> AsyncGenerator:
    """Provide ClaimX event ingester worker with mocked Delta writer."""
    from kafka_pipeline.claimx.workers.event_ingester import ClaimXEventIngesterWorker

    # Mock the ClaimXEventsDeltaWriter class before creating the worker
    monkeypatch.setattr(
        "kafka_pipeline.claimx.workers.event_ingester.ClaimXEventsDeltaWriter",
        lambda *args, **kwargs: mock_claimx_events_writer
    )

    worker = ClaimXEventIngesterWorker(
        config=test_claimx_config,
        enable_delta_writes=True,
        events_table_path="abfss://test@onelake.dfs.fabric.microsoft.com/lakehouse/Tables/claimx_events",
        enrichment_topic=test_claimx_config.claimx_enrichment_pending_topic,
    )

    yield worker

    if worker.consumer and hasattr(worker.consumer, 'is_running') and worker.consumer.is_running:
        await worker.stop()


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

    if hasattr(worker, 'is_running') and worker.is_running:
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
    mock_onelake_client,
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
async def claimx_result_processor(
    test_claimx_config: KafkaConfig,
) -> AsyncGenerator:
    """Provide ClaimX result processor worker for testing."""
    from kafka_pipeline.claimx.workers.result_processor import ClaimXResultProcessor

    processor = ClaimXResultProcessor(
        config=test_claimx_config,
        results_topic=test_claimx_config.claimx_downloads_results_topic,
    )

    yield processor

    # Cleanup is handled by the test itself
    # (processor.stop() should be called in test)


@pytest.fixture
def mock_storage_claimx(
    mock_onelake_client,
    mock_claimx_events_writer: MockClaimXEventsDeltaWriter,
    mock_claimx_entity_writer: MockClaimXEntityWriter,
) -> Dict:
    """
    Provide all ClaimX mock storage components in a single dict.

    Useful for tests that need access to multiple mock storage components.
    """
    return {
        "onelake": mock_onelake_client,
        "delta_events": mock_claimx_events_writer,
        "delta_entities": mock_claimx_entity_writer,
    }
