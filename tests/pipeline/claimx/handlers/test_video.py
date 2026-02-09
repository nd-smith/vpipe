"""Tests for ClaimX video collaboration event handler."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from core.types import ErrorCategory
from pipeline.claimx.api_client import ClaimXApiError
from pipeline.claimx.handlers.video import VideoCollabHandler
from pipeline.claimx.schemas.entities import EntityRowsMessage

from .conftest import make_event, make_project_api_response


def _make_collab_response(collab_id=700, claim_id=123):
    """Create a video collaboration API response."""
    return {
        "videoCollaborationId": collab_id,
        "claimId": claim_id,
        "claimRepFirstName": "John",
        "claimRepLastName": "Doe",
        "claimRepFullName": "John Doe",
        "numberOfVideos": 3,
        "sessionCount": 1,
        "companyId": 1,
        "companyName": "Test Co",
    }


# ============================================================================
# VideoCollabHandler.handle_event - Success cases
# ============================================================================


class TestVideoCollabHandlerSuccess:

    async def test_handle_event_extracts_video_collab_row(self, mock_client):
        mock_client.get_video_collaboration = AsyncMock(
            return_value=_make_collab_response()
        )
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = VideoCollabHandler(mock_client)
        event = make_event(event_type="VIDEO_COLLABORATION_INVITE_SENT", project_id="123")

        result = await handler.handle_event(event)

        assert result.success is True
        assert result.api_calls == 2
        assert len(result.rows.video_collab) == 1
        assert result.rows.video_collab[0]["video_collaboration_id"] == 700

    async def test_handle_event_includes_project_verification(self, mock_client):
        mock_client.get_video_collaboration = AsyncMock(
            return_value=_make_collab_response()
        )
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = VideoCollabHandler(mock_client)
        event = make_event(event_type="VIDEO_COLLABORATION_COMPLETED", project_id="123")

        result = await handler.handle_event(event)

        assert result.success is True
        assert len(result.rows.projects) >= 1
        mock_client.get_project.assert_called_once()

    async def test_handle_event_no_collab_data_returns_success_with_project(self, mock_client):
        mock_client.get_video_collaboration = AsyncMock(return_value=None)
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = VideoCollabHandler(mock_client)
        event = make_event(event_type="VIDEO_COLLABORATION_INVITE_SENT", project_id="123")

        result = await handler.handle_event(event)

        assert result.success is True
        assert len(result.rows.video_collab) == 0
        # Should still have project rows from verification
        assert len(result.rows.projects) >= 1


# ============================================================================
# VideoCollabHandler._extract_collab_data
# ============================================================================


class TestExtractCollabData:

    def test_extract_from_dict_response(self, mock_client):
        handler = VideoCollabHandler(mock_client)
        response = _make_collab_response(claim_id=123)

        result = handler._extract_collab_data(response, "123")
        assert result is not None
        assert result["videoCollaborationId"] == 700

    def test_extract_from_data_wrapped_response(self, mock_client):
        handler = VideoCollabHandler(mock_client)
        response = {"data": [_make_collab_response(claim_id=123)]}

        result = handler._extract_collab_data(response, "123")
        assert result is not None
        assert result["claimId"] == 123

    def test_extract_from_collaborations_wrapped_response(self, mock_client):
        handler = VideoCollabHandler(mock_client)
        response = {"collaborations": [_make_collab_response(claim_id=123)]}

        result = handler._extract_collab_data(response, "123")
        assert result is not None

    def test_extract_from_video_collaboration_wrapped_response(self, mock_client):
        handler = VideoCollabHandler(mock_client)
        inner = _make_collab_response(claim_id=123)
        response = {"videoCollaboration": inner}

        result = handler._extract_collab_data(response, "123")
        assert result is not None

    def test_extract_from_list_matches_project_id(self, mock_client):
        handler = VideoCollabHandler(mock_client)
        response = [
            _make_collab_response(collab_id=1, claim_id=100),
            _make_collab_response(collab_id=2, claim_id=200),
        ]

        result = handler._extract_collab_data(response, "200")
        assert result is not None
        assert result["videoCollaborationId"] == 2

    def test_extract_from_list_falls_back_to_first(self, mock_client):
        handler = VideoCollabHandler(mock_client)
        response = [
            _make_collab_response(collab_id=1, claim_id=100),
            _make_collab_response(collab_id=2, claim_id=200),
        ]

        result = handler._extract_collab_data(response, "999")
        assert result is not None
        assert result["videoCollaborationId"] == 1

    def test_extract_from_empty_list_returns_none(self, mock_client):
        handler = VideoCollabHandler(mock_client)
        result = handler._extract_collab_data([], "123")
        assert result is None

    def test_extract_from_none_returns_none(self, mock_client):
        handler = VideoCollabHandler(mock_client)
        result = handler._extract_collab_data(None, "123")
        assert result is None

    def test_extract_from_non_dict_non_list_returns_none(self, mock_client):
        handler = VideoCollabHandler(mock_client)
        result = handler._extract_collab_data("unexpected", "123")
        assert result is None

    def test_extract_from_dict_without_known_keys(self, mock_client):
        handler = VideoCollabHandler(mock_client)
        response = {"videoCollaborationId": 700, "claimId": 123}

        result = handler._extract_collab_data(response, "123")
        assert result is not None
        assert result["videoCollaborationId"] == 700


# ============================================================================
# VideoCollabHandler.handle_event - Error handling
# ============================================================================


class TestVideoCollabHandlerErrors:

    async def test_handle_event_api_error(self, mock_client):
        mock_client.get_video_collaboration = AsyncMock(
            side_effect=ClaimXApiError(
                "Server error",
                status_code=500,
                category=ErrorCategory.TRANSIENT,
                is_retryable=True,
            )
        )
        handler = VideoCollabHandler(mock_client)
        event = make_event(event_type="VIDEO_COLLABORATION_INVITE_SENT")

        result = await handler.handle_event(event)

        assert result.success is False
        assert result.error_category == ErrorCategory.TRANSIENT
        assert result.is_retryable is True
        assert result.api_calls == 2

    async def test_handle_event_permanent_api_error(self, mock_client):
        mock_client.get_video_collaboration = AsyncMock(
            side_effect=ClaimXApiError(
                "Not found",
                status_code=404,
                category=ErrorCategory.PERMANENT,
                is_retryable=False,
            )
        )
        handler = VideoCollabHandler(mock_client)
        event = make_event(event_type="VIDEO_COLLABORATION_COMPLETED")

        result = await handler.handle_event(event)

        assert result.success is False
        assert result.error_category == ErrorCategory.PERMANENT
        assert result.is_retryable is False

    async def test_handle_event_unexpected_error(self, mock_client):
        mock_client.get_video_collaboration = AsyncMock(
            side_effect=RuntimeError("unexpected")
        )
        handler = VideoCollabHandler(mock_client)
        event = make_event(event_type="VIDEO_COLLABORATION_INVITE_SENT")

        result = await handler.handle_event(event)

        assert result.success is False
        assert result.error_category == ErrorCategory.TRANSIENT
        assert result.is_retryable is True
        assert "unexpected" in result.error

    async def test_handle_event_video_row_without_id_not_appended(self, mock_client):
        """If video_collaboration_id is None, row should not be appended."""
        mock_client.get_video_collaboration = AsyncMock(
            return_value={"some_field": "value"}  # No videoCollaborationId or id
        )
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = VideoCollabHandler(mock_client)
        event = make_event(event_type="VIDEO_COLLABORATION_INVITE_SENT", project_id="123")

        result = await handler.handle_event(event)

        assert result.success is True
        assert len(result.rows.video_collab) == 0
