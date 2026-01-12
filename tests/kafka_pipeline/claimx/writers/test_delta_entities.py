"""
Tests for ClaimX Delta entity writer.

Tests cover:
- Initialization with multiple table paths
- Writing to different entity tables
- Schema validation and type conversion
- Merge operations for upserts
- Append operations for contacts/media
"""

import asyncio
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch, AsyncMock

import polars as pl
import pytest

from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage


@pytest.fixture
def mock_base_writer_factory():
    """Factory to create mock BaseDeltaWriter instances."""
    created_writers = {}

    def create_mock_writer(table_path, **kwargs):
        mock = MagicMock()
        mock.table_path = table_path
        mock.partition_column = kwargs.get("partition_column")
        mock._async_append = AsyncMock(return_value=True)
        mock._async_merge = AsyncMock(return_value=True)
        mock.append_call_count = 0
        mock.merge_call_count = 0
        mock.last_df = None

        # Track call counts
        original_append = mock._async_append.side_effect

        async def tracking_append(df):
            mock.append_call_count += 1
            mock.last_df = df
            return True

        async def tracking_merge(df, **merge_kwargs):
            mock.merge_call_count += 1
            mock.last_df = df
            return True

        mock._async_append = AsyncMock(side_effect=tracking_append)
        mock._async_merge = AsyncMock(side_effect=tracking_merge)

        created_writers[table_path] = mock
        return mock

    return create_mock_writer, created_writers


@pytest.fixture
def claimx_entity_writer(mock_base_writer_factory):
    """Create a ClaimXEntityWriter with mocked Delta backends."""
    create_mock, created_writers = mock_base_writer_factory

    with patch(
        "kafka_pipeline.claimx.writers.delta_entities.BaseDeltaWriter",
        side_effect=create_mock,
    ):
        from kafka_pipeline.claimx.writers.delta_entities import ClaimXEntityWriter

        writer = ClaimXEntityWriter(
            projects_table_path="abfss://test@onelake/lakehouse/claimx_projects",
            contacts_table_path="abfss://test@onelake/lakehouse/claimx_contacts",
            media_table_path="abfss://test@onelake/lakehouse/claimx_attachment_metadata",
            tasks_table_path="abfss://test@onelake/lakehouse/claimx_tasks",
            task_templates_table_path="abfss://test@onelake/lakehouse/claimx_task_templates",
            external_links_table_path="abfss://test@onelake/lakehouse/claimx_external_links",
            video_collab_table_path="abfss://test@onelake/lakehouse/claimx_video_collab",
        )
        writer._mock_writers = created_writers
        yield writer


class TestClaimXEntityWriterInit:
    """Test suite for ClaimXEntityWriter initialization."""

    def test_initialization(self, claimx_entity_writer):
        """Test writer creates all expected table writers."""
        expected_tables = [
            "projects",
            "contacts",
            "media",
            "tasks",
            "task_templates",
            "external_links",
            "video_collab",
        ]

        for table in expected_tables:
            assert table in claimx_entity_writer._writers, f"Missing writer for {table}"

    def test_projects_writer_has_partition(self, claimx_entity_writer):
        """Test that projects writer has project_id partition."""
        projects_writer = claimx_entity_writer._writers["projects"]
        assert projects_writer.partition_column == "project_id"

    def test_media_writer_has_partition(self, claimx_entity_writer):
        """Test that media writer has project_id partition."""
        media_writer = claimx_entity_writer._writers["media"]
        assert media_writer.partition_column == "project_id"


class TestClaimXEntityWriterWriteAll:
    """Test suite for write_all method."""

    @pytest.mark.asyncio
    async def test_write_all_projects_only(
        self, claimx_entity_writer, sample_project_row
    ):
        """Test writing only projects."""
        entity_rows = EntityRowsMessage(projects=[sample_project_row])

        counts = await claimx_entity_writer.write_all(entity_rows)

        assert counts.get("projects") == 1
        # Verify merge was called for projects
        projects_writer = claimx_entity_writer._writers["projects"]
        assert projects_writer.merge_call_count == 1

    @pytest.mark.asyncio
    async def test_write_all_contacts_only(
        self, claimx_entity_writer, sample_contact_row
    ):
        """Test writing only contacts."""
        entity_rows = EntityRowsMessage(contacts=[sample_contact_row])

        counts = await claimx_entity_writer.write_all(entity_rows)

        assert counts.get("contacts") == 1
        # Verify append was called for contacts (not merge)
        contacts_writer = claimx_entity_writer._writers["contacts"]
        assert contacts_writer.append_call_count == 1
        assert contacts_writer.merge_call_count == 0

    @pytest.mark.asyncio
    async def test_write_all_media_only(self, claimx_entity_writer, sample_media_row):
        """Test writing only media."""
        entity_rows = EntityRowsMessage(media=[sample_media_row])

        counts = await claimx_entity_writer.write_all(entity_rows)

        assert counts.get("media") == 1
        # Verify append was called for media (not merge)
        media_writer = claimx_entity_writer._writers["media"]
        assert media_writer.append_call_count == 1
        assert media_writer.merge_call_count == 0

    @pytest.mark.asyncio
    async def test_write_all_tasks_only(self, claimx_entity_writer, sample_task_row):
        """Test writing only tasks."""
        entity_rows = EntityRowsMessage(tasks=[sample_task_row])

        counts = await claimx_entity_writer.write_all(entity_rows)

        assert counts.get("tasks") == 1
        # Verify merge was called for tasks
        tasks_writer = claimx_entity_writer._writers["tasks"]
        assert tasks_writer.merge_call_count == 1

    @pytest.mark.asyncio
    async def test_write_all_multiple_tables(
        self,
        claimx_entity_writer,
        sample_project_row,
        sample_contact_row,
        sample_media_row,
    ):
        """Test writing to multiple tables in one call."""
        entity_rows = EntityRowsMessage(
            projects=[sample_project_row],
            contacts=[sample_contact_row],
            media=[sample_media_row],
        )

        counts = await claimx_entity_writer.write_all(entity_rows)

        assert counts.get("projects") == 1
        assert counts.get("contacts") == 1
        assert counts.get("media") == 1

    @pytest.mark.asyncio
    async def test_write_all_empty_entity_rows(self, claimx_entity_writer):
        """Test writing with no data returns empty counts."""
        entity_rows = EntityRowsMessage()

        counts = await claimx_entity_writer.write_all(entity_rows)

        assert counts == {}

    @pytest.mark.asyncio
    async def test_write_all_multiple_rows_per_table(
        self, claimx_entity_writer, sample_project_row
    ):
        """Test writing multiple rows to a single table."""
        projects = [
            {**sample_project_row, "project_id": "111"},
            {**sample_project_row, "project_id": "222"},
            {**sample_project_row, "project_id": "333"},
        ]
        entity_rows = EntityRowsMessage(projects=projects)

        counts = await claimx_entity_writer.write_all(entity_rows)

        assert counts.get("projects") == 3

        # Verify the DataFrame has 3 rows
        projects_writer = claimx_entity_writer._writers["projects"]
        assert len(projects_writer.last_df) == 3


class TestClaimXEntityWriterSchema:
    """Test schema-related functionality."""

    def test_merge_keys_defined(self):
        """Test that MERGE_KEYS has all required tables."""
        from kafka_pipeline.claimx.writers.delta_entities import MERGE_KEYS

        expected_tables = [
            "projects",
            "contacts",
            "media",
            "tasks",
            "task_templates",
            "external_links",
            "video_collab",
        ]

        for table in expected_tables:
            assert table in MERGE_KEYS, f"Missing merge keys for {table}"
            assert len(MERGE_KEYS[table]) > 0, f"Empty merge keys for {table}"

    def test_table_schemas_defined(self):
        """Test that TABLE_SCHEMAS has all required tables."""
        from kafka_pipeline.claimx.writers.delta_entities import TABLE_SCHEMAS

        expected_tables = [
            "projects",
            "contacts",
            "media",
            "tasks",
            "task_templates",
            "external_links",
            "video_collab",
        ]

        for table in expected_tables:
            assert table in TABLE_SCHEMAS, f"Missing schema for {table}"
            assert len(TABLE_SCHEMAS[table]) > 0, f"Empty schema for {table}"

    def test_projects_schema_fields(self):
        """Test projects schema has required fields."""
        from kafka_pipeline.claimx.writers.delta_entities import PROJECTS_SCHEMA

        required_fields = [
            "project_id",
            "project_number",
            "master_file_name",
            "status",
            "event_id",
            "created_at",
            "updated_at",
            "last_enriched_at",
        ]

        for field in required_fields:
            assert field in PROJECTS_SCHEMA, f"Missing projects field: {field}"

    def test_contacts_schema_fields(self):
        """Test contacts schema has required fields."""
        from kafka_pipeline.claimx.writers.delta_entities import CONTACTS_SCHEMA

        required_fields = [
            "project_id",
            "contact_email",
            "contact_type",
            "first_name",
            "last_name",
            "event_id",
            "created_at",
            "last_enriched_at",
        ]

        for field in required_fields:
            assert field in CONTACTS_SCHEMA, f"Missing contacts field: {field}"

    def test_media_schema_fields(self):
        """Test media schema has required fields."""
        from kafka_pipeline.claimx.writers.delta_entities import MEDIA_SCHEMA

        required_fields = [
            "media_id",
            "project_id",
            "file_type",
            "file_name",
            "event_id",
            "created_at",
            "updated_at",
            "last_enriched_at",
        ]

        for field in required_fields:
            assert field in MEDIA_SCHEMA, f"Missing media field: {field}"


class TestClaimXEntityWriterDataFrameCreation:
    """Test DataFrame creation with schema."""

    def test_create_dataframe_with_schema_projects(self):
        """Test DataFrame creation for projects."""
        from kafka_pipeline.claimx.writers.delta_entities import (
            ClaimXEntityWriter,
            TABLE_SCHEMAS,
        )

        # Create a minimal writer instance for testing
        with patch("kafka_pipeline.claimx.writers.delta_entities.BaseDeltaWriter"):
            writer = ClaimXEntityWriter(
                projects_table_path="test",
                contacts_table_path="test",
                media_table_path="test",
                tasks_table_path="test",
                task_templates_table_path="test",
                external_links_table_path="test",
                video_collab_table_path="test",
            )

            rows = [
                {
                    "project_id": "123",
                    "project_number": "P-123",
                    "master_file_name": "MFN-123",
                    "status": "active",
                }
            ]

            df = writer._create_dataframe_with_schema("projects", rows)

            assert len(df) == 1
            assert df["project_id"][0] == "123"
            assert df.schema["project_id"] == pl.Utf8

    def test_create_dataframe_converts_datetime_strings(self):
        """Test that ISO datetime strings are converted to datetime objects."""
        from kafka_pipeline.claimx.writers.delta_entities import ClaimXEntityWriter

        with patch("kafka_pipeline.claimx.writers.delta_entities.BaseDeltaWriter"):
            writer = ClaimXEntityWriter(
                projects_table_path="test",
                contacts_table_path="test",
                media_table_path="test",
                tasks_table_path="test",
                task_templates_table_path="test",
                external_links_table_path="test",
                video_collab_table_path="test",
            )

            rows = [
                {
                    "project_id": "123",
                    "created_at": "2024-01-15T10:30:00Z",
                    "updated_at": "2024-01-15T10:30:00+00:00",
                }
            ]

            df = writer._create_dataframe_with_schema("projects", rows)

            # Should be converted to datetime
            assert df.schema["created_at"] == pl.Datetime("us", "UTC")
            assert df.schema["updated_at"] == pl.Datetime("us", "UTC")

    def test_create_dataframe_converts_date_strings(self):
        """Test that date strings are converted to date objects."""
        from kafka_pipeline.claimx.writers.delta_entities import ClaimXEntityWriter

        with patch("kafka_pipeline.claimx.writers.delta_entities.BaseDeltaWriter"):
            writer = ClaimXEntityWriter(
                projects_table_path="test",
                contacts_table_path="test",
                media_table_path="test",
                tasks_table_path="test",
                task_templates_table_path="test",
                external_links_table_path="test",
                video_collab_table_path="test",
            )

            rows = [
                {
                    "project_id": "123",
                    "contact_email": "test@example.com",
                    "contact_type": "primary",
                    "created_date": "2024-01-15",
                }
            ]

            df = writer._create_dataframe_with_schema("contacts", rows)

            # Should be converted to date
            assert df.schema["created_date"] == pl.Date
            assert df["created_date"][0] == date(2024, 1, 15)

    def test_create_dataframe_empty_rows(self):
        """Test DataFrame creation with empty rows returns empty DataFrame."""
        from kafka_pipeline.claimx.writers.delta_entities import ClaimXEntityWriter

        with patch("kafka_pipeline.claimx.writers.delta_entities.BaseDeltaWriter"):
            writer = ClaimXEntityWriter(
                projects_table_path="test",
                contacts_table_path="test",
                media_table_path="test",
                tasks_table_path="test",
                task_templates_table_path="test",
                external_links_table_path="test",
                video_collab_table_path="test",
            )

            df = writer._create_dataframe_with_schema("projects", [])

            assert len(df) == 0


class TestClaimXEntityWriterMergeKeys:
    """Test merge key configurations."""

    def test_projects_merge_key(self):
        """Test projects uses project_id as merge key."""
        from kafka_pipeline.claimx.writers.delta_entities import MERGE_KEYS

        assert MERGE_KEYS["projects"] == ["project_id"]

    def test_contacts_merge_keys(self):
        """Test contacts uses composite merge key."""
        from kafka_pipeline.claimx.writers.delta_entities import MERGE_KEYS

        assert "project_id" in MERGE_KEYS["contacts"]
        assert "contact_email" in MERGE_KEYS["contacts"]
        assert "contact_type" in MERGE_KEYS["contacts"]

    def test_media_merge_key(self):
        """Test media uses media_id as merge key."""
        from kafka_pipeline.claimx.writers.delta_entities import MERGE_KEYS

        assert MERGE_KEYS["media"] == ["media_id"]

    def test_tasks_merge_key(self):
        """Test tasks uses assignment_id as merge key."""
        from kafka_pipeline.claimx.writers.delta_entities import MERGE_KEYS

        assert MERGE_KEYS["tasks"] == ["assignment_id"]

    def test_task_templates_merge_key(self):
        """Test task_templates uses task_id as merge key."""
        from kafka_pipeline.claimx.writers.delta_entities import MERGE_KEYS

        assert MERGE_KEYS["task_templates"] == ["task_id"]

    def test_external_links_merge_key(self):
        """Test external_links uses link_id as merge key."""
        from kafka_pipeline.claimx.writers.delta_entities import MERGE_KEYS

        assert MERGE_KEYS["external_links"] == ["link_id"]

    def test_video_collab_merge_key(self):
        """Test video_collab uses video_collaboration_id as merge key."""
        from kafka_pipeline.claimx.writers.delta_entities import MERGE_KEYS

        assert MERGE_KEYS["video_collab"] == ["video_collaboration_id"]


@pytest.mark.asyncio
async def test_claimx_entity_writer_integration():
    """Integration test with BaseDeltaWriter mocked."""
    mock_writers = {}

    def create_mock_writer(table_path, **kwargs):
        mock = MagicMock()
        mock.table_path = table_path
        mock.partition_column = kwargs.get("partition_column")
        mock._async_append = AsyncMock(return_value=True)
        mock._async_merge = AsyncMock(return_value=True)
        mock_writers[table_path] = mock
        return mock

    with patch(
        "kafka_pipeline.claimx.writers.delta_entities.BaseDeltaWriter",
        side_effect=create_mock_writer,
    ):
        from kafka_pipeline.claimx.writers.delta_entities import ClaimXEntityWriter

        # Create writer
        writer = ClaimXEntityWriter(
            projects_table_path="abfss://test/projects",
            contacts_table_path="abfss://test/contacts",
            media_table_path="abfss://test/media",
            tasks_table_path="abfss://test/tasks",
            task_templates_table_path="abfss://test/task_templates",
            external_links_table_path="abfss://test/external_links",
            video_collab_table_path="abfss://test/video_collab",
        )

        # Verify all 7 writers were created
        assert len(mock_writers) == 7

        # Write some test data
        entity_rows = EntityRowsMessage(
            projects=[
                {
                    "project_id": "int-test-123",
                    "project_number": "P-123",
                    "master_file_name": "MFN-123",
                    "status": "active",
                }
            ],
            contacts=[
                {
                    "project_id": "int-test-123",
                    "contact_email": "test@example.com",
                    "contact_type": "policyholder",
                    "first_name": "Test",
                    "last_name": "User",
                }
            ],
        )

        counts = await writer.write_all(entity_rows)

        assert counts.get("projects") == 1
        assert counts.get("contacts") == 1

        # Verify merge was called for projects
        projects_mock = mock_writers["abfss://test/projects"]
        projects_mock._async_merge.assert_called_once()

        # Verify append was called for contacts
        contacts_mock = mock_writers["abfss://test/contacts"]
        contacts_mock._async_append.assert_called_once()
