"""Tests for ClaimX task event handler."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from core.types import ErrorCategory
from pipeline.claimx.api_client import ClaimXApiError
from pipeline.claimx.handlers.task import TaskHandler
from pipeline.claimx.schemas.entities import EntityRowsMessage

from .conftest import make_event, make_project_api_response


def _make_task_response(
    assignment_id=100,
    project_id=123,
    with_custom_task=True,
    with_link=True,
):
    """Create a realistic task API response."""
    response = {
        "assignmentId": assignment_id,
        "taskId": 200,
        "taskName": "Inspection Report",
        "projectId": project_id,
        "assigneeId": 10,
        "status": "ASSIGNED",
    }
    if with_custom_task:
        response["customTask"] = {
            "taskId": 50,
            "compId": 10,
            "name": "Photo Review",
            "enabled": True,
        }
    if with_link:
        response["externalLinkData"] = {
            "linkId": 300,
            "linkCode": "ABC123",
            "url": "https://example.com/link",
            "email": "recipient@example.com",
            "firstName": "Jane",
            "lastName": "Doe",
        }
    return response


# ============================================================================
# TaskHandler.handle_event - Success cases
# ============================================================================


class TestTaskHandlerSuccess:

    async def test_handle_event_extracts_task_row(self, mock_client):
        mock_client.get_custom_task = AsyncMock(return_value=_make_task_response())
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = TaskHandler(mock_client)
        event = make_event(
            event_type="CUSTOM_TASK_ASSIGNED",
            task_assignment_id="100",
        )

        result = await handler.handle_event(event)

        assert result.success is True
        assert result.api_calls == 2
        assert len(result.rows.tasks) == 1
        assert result.rows.tasks[0]["assignment_id"] == 100

    async def test_handle_event_extracts_template_row(self, mock_client):
        mock_client.get_custom_task = AsyncMock(return_value=_make_task_response())
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = TaskHandler(mock_client)
        event = make_event(
            event_type="CUSTOM_TASK_ASSIGNED",
            task_assignment_id="100",
        )

        result = await handler.handle_event(event)

        assert len(result.rows.task_templates) == 1
        assert result.rows.task_templates[0]["task_id"] == 50

    async def test_handle_event_extracts_external_link(self, mock_client):
        mock_client.get_custom_task = AsyncMock(return_value=_make_task_response())
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = TaskHandler(mock_client)
        event = make_event(
            event_type="CUSTOM_TASK_ASSIGNED",
            task_assignment_id="100",
        )

        result = await handler.handle_event(event)

        assert len(result.rows.external_links) == 1
        assert result.rows.external_links[0]["link_id"] == 300

    async def test_handle_event_extracts_contact_from_link(self, mock_client):
        mock_client.get_custom_task = AsyncMock(return_value=_make_task_response())
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = TaskHandler(mock_client)
        event = make_event(
            event_type="CUSTOM_TASK_ASSIGNED",
            task_assignment_id="100",
        )

        result = await handler.handle_event(event)

        # Contact from link + contacts from project verification
        link_contacts = [
            c for c in result.rows.contacts
            if c.get("contact_type") == "POLICYHOLDER" and c.get("task_assignment_id") is not None
        ]
        assert len(link_contacts) == 1
        assert link_contacts[0]["contact_email"] == "recipient@example.com"

    async def test_handle_event_includes_project_verification(self, mock_client):
        mock_client.get_custom_task = AsyncMock(return_value=_make_task_response())
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = TaskHandler(mock_client)
        event = make_event(
            event_type="CUSTOM_TASK_ASSIGNED",
            task_assignment_id="100",
        )

        result = await handler.handle_event(event)

        assert len(result.rows.projects) >= 1
        mock_client.get_project.assert_called_once()

    async def test_handle_event_without_custom_task(self, mock_client):
        mock_client.get_custom_task = AsyncMock(
            return_value=_make_task_response(with_custom_task=False)
        )
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = TaskHandler(mock_client)
        event = make_event(
            event_type="CUSTOM_TASK_ASSIGNED",
            task_assignment_id="100",
        )

        result = await handler.handle_event(event)

        assert result.success is True
        assert len(result.rows.task_templates) == 0

    async def test_handle_event_without_external_link(self, mock_client):
        mock_client.get_custom_task = AsyncMock(
            return_value=_make_task_response(with_link=False)
        )
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = TaskHandler(mock_client)
        event = make_event(
            event_type="CUSTOM_TASK_ASSIGNED",
            task_assignment_id="100",
        )

        result = await handler.handle_event(event)

        assert result.success is True
        assert len(result.rows.external_links) == 0

    async def test_handle_event_task_completed(self, mock_client):
        mock_client.get_custom_task = AsyncMock(return_value=_make_task_response())
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = TaskHandler(mock_client)
        event = make_event(
            event_type="CUSTOM_TASK_COMPLETED",
            task_assignment_id="100",
        )

        result = await handler.handle_event(event)

        assert result.success is True
        assert len(result.rows.tasks) == 1

    async def test_handle_event_uses_api_project_id(self, mock_client):
        """project_id from API response should be preferred."""
        response = _make_task_response(project_id=999)
        mock_client.get_custom_task = AsyncMock(return_value=response)
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = TaskHandler(mock_client)
        event = make_event(
            event_type="CUSTOM_TASK_ASSIGNED",
            project_id="123",
            task_assignment_id="100",
        )

        result = await handler.handle_event(event)

        assert result.success is True
        # Project verification should use API's project_id
        mock_client.get_project.assert_called_once_with(999)


# ============================================================================
# TaskHandler.handle_event - Validation
# ============================================================================


class TestTaskHandlerValidation:

    async def test_handle_event_missing_task_assignment_id(self, mock_client):
        handler = TaskHandler(mock_client)
        event = make_event(
            event_type="CUSTOM_TASK_ASSIGNED",
            task_assignment_id=None,
        )

        result = await handler.handle_event(event)

        assert result.success is False
        assert "Missing task_assignment_id" in result.error
        assert result.error_category == ErrorCategory.PERMANENT
        assert result.is_retryable is False
        assert result.api_calls == 0

    async def test_handle_event_non_numeric_task_assignment_id(self, mock_client):
        handler = TaskHandler(mock_client)
        event = make_event(
            event_type="CUSTOM_TASK_ASSIGNED",
            task_assignment_id="abc",
        )

        result = await handler.handle_event(event)

        assert result.success is False
        assert "Invalid task_assignment_id" in result.error
        assert result.error_category == ErrorCategory.PERMANENT
        assert result.is_retryable is False


# ============================================================================
# TaskHandler.handle_event - Error handling
# ============================================================================


class TestTaskHandlerErrors:

    async def test_handle_event_api_error(self, mock_client):
        mock_client.get_custom_task = AsyncMock(
            side_effect=ClaimXApiError(
                "Not found",
                status_code=404,
                category=ErrorCategory.PERMANENT,
                is_retryable=False,
            )
        )
        handler = TaskHandler(mock_client)
        event = make_event(
            event_type="CUSTOM_TASK_ASSIGNED",
            task_assignment_id="100",
        )

        result = await handler.handle_event(event)

        assert result.success is False
        assert result.error_category == ErrorCategory.PERMANENT
        assert result.is_retryable is False
        assert result.api_calls == 2

    async def test_handle_event_transient_api_error(self, mock_client):
        mock_client.get_custom_task = AsyncMock(
            side_effect=ClaimXApiError(
                "Server error",
                status_code=500,
                category=ErrorCategory.TRANSIENT,
                is_retryable=True,
            )
        )
        handler = TaskHandler(mock_client)
        event = make_event(
            event_type="CUSTOM_TASK_ASSIGNED",
            task_assignment_id="100",
        )

        result = await handler.handle_event(event)

        assert result.success is False
        assert result.is_retryable is True

    async def test_handle_event_unexpected_error(self, mock_client):
        mock_client.get_custom_task = AsyncMock(
            side_effect=RuntimeError("unexpected")
        )
        handler = TaskHandler(mock_client)
        event = make_event(
            event_type="CUSTOM_TASK_ASSIGNED",
            task_assignment_id="100",
        )

        result = await handler.handle_event(event)

        assert result.success is False
        assert result.error_category == ErrorCategory.TRANSIENT
        assert result.is_retryable is True
        assert "unexpected" in result.error
        assert result.api_calls == 2

    async def test_handle_event_link_without_email_skips_contact(self, mock_client):
        response = _make_task_response()
        response["externalLinkData"] = {
            "linkId": 300,
            "linkCode": "ABC123",
            "url": "https://example.com/link",
            # No email
        }
        mock_client.get_custom_task = AsyncMock(return_value=response)
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = TaskHandler(mock_client)
        event = make_event(
            event_type="CUSTOM_TASK_ASSIGNED",
            task_assignment_id="100",
        )

        result = await handler.handle_event(event)

        assert result.success is True
        # External link should exist but no contact from link
        assert len(result.rows.external_links) == 1
        link_contacts = [
            c for c in result.rows.contacts
            if c.get("task_assignment_id") is not None
        ]
        assert len(link_contacts) == 0
