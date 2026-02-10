"""Tests for ClaimX project event handler."""

from unittest.mock import AsyncMock, MagicMock

from core.types import ErrorCategory
from pipeline.claimx.api_client import ClaimXApiError
from pipeline.claimx.handlers.project import ProjectHandler

from .conftest import make_event, make_project_api_response

# ============================================================================
# ProjectHandler.handle_event - PROJECT_CREATED
# ============================================================================


class TestProjectHandlerProjectCreated:
    async def test_handle_event_project_created_success(self, mock_client):
        mock_client.get_project = AsyncMock(return_value=make_project_api_response(project_id=123))
        handler = ProjectHandler(mock_client)
        event = make_event(event_type="PROJECT_CREATED", project_id="123")

        result = await handler.handle_event(event)

        assert result.success is True
        assert result.api_calls == 1
        assert len(result.rows.projects) == 1
        assert result.rows.projects[0]["project_id"] == "123"
        assert len(result.rows.contacts) > 0
        mock_client.get_project.assert_called_once_with(123)

    async def test_handle_event_project_created_with_contacts(self, mock_client):
        mock_client.get_project = AsyncMock(return_value=make_project_api_response())
        handler = ProjectHandler(mock_client)
        event = make_event(event_type="PROJECT_CREATED")

        result = await handler.handle_event(event)

        assert result.success is True
        # Should have policyholder + team member contacts
        assert len(result.rows.contacts) == 2

    async def test_handle_event_project_created_empty_response(self, mock_client):
        mock_client.get_project = AsyncMock(return_value={})
        handler = ProjectHandler(mock_client)
        event = make_event(event_type="PROJECT_CREATED")

        result = await handler.handle_event(event)

        assert result.success is True
        assert result.api_calls == 1
        # project_id is None from empty response, so no project row appended
        assert len(result.rows.projects) == 0


# ============================================================================
# ProjectHandler.handle_event - PROJECT_MFN_ADDED
# ============================================================================


class TestProjectHandlerMfnAdded:
    async def test_handle_event_mfn_added_overlays_mfn(self, mock_client):
        mock_client.get_project = AsyncMock(return_value=make_project_api_response())
        handler = ProjectHandler(mock_client)
        event = make_event(
            event_type="PROJECT_MFN_ADDED",
            master_file_name="NEW-MFN-999",
        )

        result = await handler.handle_event(event)

        assert result.success is True
        assert len(result.rows.projects) == 1
        assert result.rows.projects[0]["master_file_name"] == "NEW-MFN-999"

    async def test_handle_event_mfn_added_no_mfn_in_event_keeps_api_value(self, mock_client):
        mock_client.get_project = AsyncMock(return_value=make_project_api_response())
        handler = ProjectHandler(mock_client)
        event = make_event(
            event_type="PROJECT_MFN_ADDED",
            master_file_name=None,
        )

        result = await handler.handle_event(event)

        assert result.success is True
        assert len(result.rows.projects) == 1
        # Should keep the API value since event.master_file_name is None
        assert result.rows.projects[0]["master_file_name"] == "MFN-001"

    async def test_handle_event_mfn_added_fallback_when_api_returns_no_project(self, mock_client):
        mock_client.get_project = AsyncMock(
            return_value={"data": {"project": {}, "teamMembers": []}}
        )
        handler = ProjectHandler(mock_client)
        event = make_event(
            event_type="PROJECT_MFN_ADDED",
            project_id="999",
            master_file_name="FALLBACK-MFN",
        )

        result = await handler.handle_event(event)

        assert result.success is True
        # API returned empty project (no project_id), so no project row from API.
        # Fallback row should be appended.
        assert len(result.rows.projects) == 1
        assert result.rows.projects[0]["project_id"] == "999"
        assert result.rows.projects[0]["master_file_name"] == "FALLBACK-MFN"


# ============================================================================
# ProjectHandler.handle_event - Error handling
# ============================================================================


class TestProjectHandlerErrors:
    async def test_handle_event_api_error_returns_failure(self, mock_client):
        mock_client.get_project = AsyncMock(
            side_effect=ClaimXApiError(
                "Not found",
                status_code=404,
                category=ErrorCategory.PERMANENT,
                is_retryable=False,
            )
        )
        handler = ProjectHandler(mock_client)
        event = make_event()

        result = await handler.handle_event(event)

        assert result.success is False
        assert result.error_category == ErrorCategory.PERMANENT
        assert result.is_retryable is False
        assert "Not found" in result.error
        assert result.api_calls == 1

    async def test_handle_event_api_transient_error_is_retryable(self, mock_client):
        mock_client.get_project = AsyncMock(
            side_effect=ClaimXApiError(
                "Server error",
                status_code=500,
                category=ErrorCategory.TRANSIENT,
                is_retryable=True,
            )
        )
        handler = ProjectHandler(mock_client)
        event = make_event()

        result = await handler.handle_event(event)

        assert result.success is False
        assert result.is_retryable is True
        assert result.error_category == ErrorCategory.TRANSIENT

    async def test_handle_event_unexpected_error_returns_transient(self, mock_client):
        mock_client.get_project = AsyncMock(side_effect=RuntimeError("unexpected"))
        handler = ProjectHandler(mock_client)
        event = make_event()

        result = await handler.handle_event(event)

        assert result.success is False
        assert result.error_category == ErrorCategory.TRANSIENT
        assert result.is_retryable is True
        assert "unexpected" in result.error


# ============================================================================
# ProjectHandler.fetch_project_data
# ============================================================================


class TestProjectHandlerFetchProjectData:
    async def test_fetch_project_data_returns_rows(self, mock_client):
        mock_client.get_project = AsyncMock(return_value=make_project_api_response())
        handler = ProjectHandler(mock_client)

        rows = await handler.fetch_project_data(123, source_event_id="evt_001")

        assert len(rows.projects) == 1
        assert len(rows.contacts) > 0
        mock_client.get_project.assert_called_once_with(123)

    async def test_fetch_project_data_uses_cache(self, mock_client, mock_project_cache):
        mock_project_cache.has = MagicMock(return_value=True)
        handler = ProjectHandler(mock_client, project_cache=mock_project_cache)

        rows = await handler.fetch_project_data(123, source_event_id="evt_001")

        # Should return empty rows and skip API call
        assert rows.is_empty()
        mock_client.get_project.assert_not_called()

    async def test_fetch_project_data_adds_to_cache_after_fetch(
        self, mock_client, mock_project_cache
    ):
        mock_project_cache.has = MagicMock(return_value=False)
        mock_client.get_project = AsyncMock(return_value=make_project_api_response())
        handler = ProjectHandler(mock_client, project_cache=mock_project_cache)

        await handler.fetch_project_data(123, source_event_id="evt_001")

        mock_project_cache.add.assert_called_once_with("123")

    async def test_fetch_project_data_without_cache(self, mock_client):
        mock_client.get_project = AsyncMock(return_value=make_project_api_response())
        handler = ProjectHandler(mock_client)

        rows = await handler.fetch_project_data(123, source_event_id="evt_001")

        assert len(rows.projects) == 1
        mock_client.get_project.assert_called_once_with(123)

    async def test_fetch_project_data_skips_project_row_when_no_id(self, mock_client):
        mock_client.get_project = AsyncMock(
            return_value={"data": {"project": {}, "teamMembers": []}}
        )
        handler = ProjectHandler(mock_client)

        rows = await handler.fetch_project_data(123, source_event_id="evt_001")

        assert len(rows.projects) == 0
