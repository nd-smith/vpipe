"""Shared fixtures for ClaimX handler tests."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from pipeline.claimx.schemas.entities import EntityRowsMessage
from pipeline.claimx.schemas.events import ClaimXEventMessage


@pytest.fixture
def mock_client():
    """Create a mock ClaimXApiClient."""
    client = AsyncMock()
    client.get_project = AsyncMock(return_value={})
    client.get_project_media = AsyncMock(return_value=[])
    client.get_custom_task = AsyncMock(return_value={})
    client.get_video_collaboration = AsyncMock(return_value={})
    return client


@pytest.fixture
def mock_project_cache():
    """Create a mock ProjectCache."""
    cache = MagicMock()
    cache.has = MagicMock(return_value=False)
    cache.add = MagicMock()
    cache.size = MagicMock(return_value=0)
    return cache


def make_event(
    event_type="PROJECT_CREATED",
    project_id="123",
    event_id="evt_001",
    media_id=None,
    task_assignment_id=None,
    video_collaboration_id=None,
    master_file_name=None,
):
    """Create a ClaimXEventMessage for testing."""
    return ClaimXEventMessage(
        event_id=event_id,
        event_type=event_type,
        project_id=project_id,
        ingested_at=datetime(2024, 12, 25, 10, 0, 0, tzinfo=timezone.utc),
        media_id=media_id,
        task_assignment_id=task_assignment_id,
        video_collaboration_id=video_collaboration_id,
        master_file_name=master_file_name,
    )


def make_project_api_response(project_id=123):
    """Create a realistic project API response."""
    return {
        "data": {
            "project": {
                "projectId": project_id,
                "projectNumber": "PN-001",
                "mfn": "MFN-001",
                "secondaryNumber": "SN-001",
                "createdDate": "2024-01-15T10:00:00Z",
                "status": "Active",
                "dateOfLoss": "2024-01-10T00:00:00Z",
                "typeOfLoss": "Water Damage",
                "causeOfLoss": "Pipe Burst",
                "lossDescription": "Kitchen flooding",
                "yearBuilt": 1990,
                "squareFootage": 2500,
                "customerInformation": {
                    "firstName": "John",
                    "lastName": "Doe",
                    "emails": [
                        {"emailAddress": "john@example.com", "primary": True},
                    ],
                    "phones": [
                        {"phoneNumber": "555-0100", "phoneCountryCode": 1},
                    ],
                },
                "address": {
                    "street1": "123 Main St",
                    "city": "Springfield",
                    "stateProvince": "IL",
                    "zipPostcode": "62701",
                    "country": "US",
                },
            },
            "teamMembers": [
                {
                    "userName": "adjuster@example.com",
                    "primaryContact": True,
                    "mfn": "MFN-001",
                },
            ],
            "companyName": "Test Insurance Co",
        },
    }
