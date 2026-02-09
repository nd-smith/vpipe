"""Tests for ClaimX project update event handler."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.types import ErrorCategory
from pipeline.claimx.api_client import ClaimXApiError
from pipeline.claimx.handlers.project_update import ProjectUpdateHandler
from pipeline.claimx.schemas.entities import EntityRowsMessage

from .conftest import make_event, make_project_api_response


# ============================================================================
# POLICYHOLDER_INVITED
# ============================================================================


class TestProjectUpdatePolicyholderInvited:

    async def test_policyholder_invited_with_verification(self, mock_client):
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = ProjectUpdateHandler(mock_client)
        event = make_event(event_type="POLICYHOLDER_INVITED")

        result = await handler.handle_event(event)

        assert result.success is True
        assert result.api_calls == 1
        assert len(result.rows.projects) >= 1
        assert "policyholder_invited_at" in result.rows.projects[0]
        assert isinstance(result.rows.projects[0]["policyholder_invited_at"], datetime)

    async def test_policyholder_invited_fallback_when_no_project_from_api(self, mock_client):
        # API returns data but project has no project_id, so no project row from fetch
        mock_client.get_project = AsyncMock(
            return_value={"data": {"project": {}, "teamMembers": []}}
        )
        handler = ProjectUpdateHandler(mock_client)
        event = make_event(event_type="POLICYHOLDER_INVITED", project_id="456")

        result = await handler.handle_event(event)

        assert result.success is True
        assert len(result.rows.projects) == 1
        assert result.rows.projects[0]["project_id"] == "456"
        assert "policyholder_invited_at" in result.rows.projects[0]


# ============================================================================
# POLICYHOLDER_JOINED
# ============================================================================


class TestProjectUpdatePolicyholderJoined:

    async def test_policyholder_joined_adds_timestamp(self, mock_client):
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = ProjectUpdateHandler(mock_client)
        event = make_event(event_type="POLICYHOLDER_JOINED")

        result = await handler.handle_event(event)

        assert result.success is True
        assert result.api_calls == 1
        assert "policyholder_joined_at" in result.rows.projects[0]
        assert isinstance(result.rows.projects[0]["policyholder_joined_at"], datetime)


# ============================================================================
# PROJECT_AUTO_XA_LINKING_UNSUCCESSFUL
# ============================================================================


class TestProjectUpdateXALinkingFail:

    async def test_xa_linking_fail_creates_minimal_row(self, mock_client):
        handler = ProjectUpdateHandler(mock_client)
        event = make_event(event_type="PROJECT_AUTO_XA_LINKING_UNSUCCESSFUL", project_id="789")

        result = await handler.handle_event(event)

        assert result.success is True
        assert result.api_calls == 0  # No verification needed
        assert len(result.rows.projects) == 1

        project_row = result.rows.projects[0]
        assert project_row["project_id"] == "789"
        assert project_row["xa_autolink_fail"] is True
        assert isinstance(project_row["xa_autolink_fail_at"], datetime)
        assert "updated_at" in project_row

    async def test_xa_linking_fail_does_not_call_api(self, mock_client):
        handler = ProjectUpdateHandler(mock_client)
        event = make_event(event_type="PROJECT_AUTO_XA_LINKING_UNSUCCESSFUL")

        await handler.handle_event(event)

        mock_client.get_project.assert_not_called()


# ============================================================================
# Unknown event type
# ============================================================================


class TestProjectUpdateUnknownEvent:

    async def test_unknown_event_type_returns_failure(self, mock_client):
        handler = ProjectUpdateHandler(mock_client)
        # Bypass the event_types check by constructing event directly
        event = make_event(event_type="UNKNOWN_EVENT")

        result = await handler.handle_event(event)

        assert result.success is False
        assert "Unknown event type" in result.error
        assert result.api_calls == 0


# ============================================================================
# Error handling
# ============================================================================


class TestProjectUpdateErrors:

    async def test_api_error_returns_failure(self, mock_client):
        mock_client.get_project = AsyncMock(
            side_effect=ClaimXApiError(
                "Server error",
                status_code=500,
                category=ErrorCategory.TRANSIENT,
                is_retryable=True,
            )
        )
        handler = ProjectUpdateHandler(mock_client)
        event = make_event(event_type="POLICYHOLDER_INVITED")

        result = await handler.handle_event(event)

        assert result.success is False
        assert result.error_category == ErrorCategory.TRANSIENT
        assert result.is_retryable is True
        assert result.api_calls == 1

    async def test_unexpected_error_returns_transient(self, mock_client):
        mock_client.get_project = AsyncMock(
            side_effect=RuntimeError("unexpected")
        )
        handler = ProjectUpdateHandler(mock_client)
        event = make_event(event_type="POLICYHOLDER_JOINED")

        result = await handler.handle_event(event)

        assert result.success is False
        assert result.error_category == ErrorCategory.TRANSIENT
        assert result.is_retryable is True
        assert "unexpected" in result.error
