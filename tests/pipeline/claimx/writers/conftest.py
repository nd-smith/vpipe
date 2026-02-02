"""Fixtures for ClaimX writer tests."""

import os
from datetime import datetime, timezone
from typing import Dict, List
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

# Set environment variables for writer tests
if "TEST_MODE" not in os.environ:
    os.environ["TEST_MODE"] = "true"

# Prevent Delta Lake from creating actual tables in tests
if "DELTA_LAKE_TEST_MODE" not in os.environ:
    os.environ["DELTA_LAKE_TEST_MODE"] = "true"


@pytest.fixture
def sample_claimx_event():
    """Create a sample ClaimX event for testing."""
    return {
        "event_id": "evt-12345",
        "event_type": "PROJECT_CREATED",
        "project_id": "123456",
        "media_id": None,
        "task_assignment_id": None,
        "video_collaboration_id": None,
        "master_file_name": "MFN-123456",
        "ingested_at": datetime.now(timezone.utc),
    }


@pytest.fixture
def sample_project_row():
    """Create a sample project row for entity testing."""
    return {
        "project_id": "123456",
        "project_number": "P-123456",
        "master_file_name": "MFN-123456",
        "status": "active",
        "created_date": "2024-01-01",
        "date_of_loss": "2024-01-01",
        "customer_first_name": "John",
        "customer_last_name": "Doe",
        "event_id": "evt-12345",
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
        "last_enriched_at": datetime.now(timezone.utc),
    }


@pytest.fixture
def sample_contact_row():
    """Create a sample contact row for entity testing."""
    return {
        "project_id": "123456",
        "contact_email": "john.doe@example.com",
        "contact_type": "policyholder",
        "first_name": "John",
        "last_name": "Doe",
        "phone_number": "555-1234",
        "is_primary_contact": True,
        "master_file_name": "MFN-123456",
        "event_id": "evt-12345",
        "created_at": datetime.now(timezone.utc),
        "last_enriched_at": datetime.now(timezone.utc),
    }


@pytest.fixture
def sample_media_row():
    """Create a sample media row for entity testing."""
    return {
        "media_id": "media-789",
        "project_id": "123456",
        "file_type": "image/jpeg",
        "file_name": "photo001.jpg",
        "media_description": "Front of house",
        "full_download_link": "https://example.com/media/photo001.jpg",
        "event_id": "evt-12345",
        "created_at": "2024-01-01T12:00:00Z",
        "updated_at": "2024-01-01T12:00:00Z",
        "last_enriched_at": "2024-01-01T12:00:00Z",
    }


@pytest.fixture
def sample_task_row():
    """Create a sample task row for entity testing."""
    return {
        "assignment_id": 12345,
        "task_id": 100,
        "project_id": "123456",
        "assignee_id": 500,
        "assignor_id": 501,
        "date_assigned": "2024-01-01",
        "status": "assigned",
        "stp_enabled": False,
        "mfn": "MFN-123456",
        "event_id": "evt-12345",
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
        "last_enriched_at": datetime.now(timezone.utc),
    }


@pytest.fixture
def mock_delta_writer():
    """Create a mock DeltaTableWriter instance."""
    mock_instance = MagicMock()
    mock_instance.append = MagicMock(return_value=1)
    mock_instance.merge = MagicMock(return_value=1)
    return mock_instance


@pytest.fixture
def mock_base_delta_writer():
    """Create a mock BaseDeltaWriter with all async methods mocked."""

    class MockBaseDeltaWriter:
        def __init__(self, **kwargs):
            self.table_path = kwargs.get("table_path", "")
            self.partition_column = kwargs.get("partition_column")
            self._append_call_count = 0
            self._merge_call_count = 0
            self._last_df = None

        async def _async_append(self, df: pl.DataFrame) -> bool:
            self._append_call_count += 1
            self._last_df = df
            return True

        async def _async_merge(
            self,
            df: pl.DataFrame,
            merge_keys: List[str],
            preserve_columns: List[str] = None,
            update_condition: str = None,
        ) -> bool:
            self._merge_call_count += 1
            self._last_df = df
            return True

    return MockBaseDeltaWriter
