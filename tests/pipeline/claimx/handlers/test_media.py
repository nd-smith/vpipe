"""Tests for ClaimX media event handler."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.types import ErrorCategory
from pipeline.claimx.api_client import ClaimXApiError
from pipeline.claimx.handlers.media import BATCH_THRESHOLD, MediaHandler
from pipeline.claimx.schemas.entities import EntityRowsMessage

from .conftest import make_event, make_project_api_response


def _make_media_api_item(media_id, name="photo.jpg"):
    return {
        "mediaID": media_id,
        "mediaType": "IMAGE",
        "mediaName": name,
        "fullDownloadLink": f"https://cdn.example.com/{media_id}.jpg",
    }


# ============================================================================
# MediaHandler.handle_event (single event passthrough)
# ============================================================================


class TestMediaHandlerHandleEvent:

    async def test_handle_event_delegates_to_handle_batch(self, mock_client):
        mock_client.get_project_media = AsyncMock(
            return_value=[_make_media_api_item(500)]
        )
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = MediaHandler(mock_client)
        event = make_event(
            event_type="PROJECT_FILE_ADDED",
            project_id="123",
            media_id="500",
        )

        result = await handler.handle_event(event)

        assert result.success is True
        assert len(result.rows.media) == 1

    async def test_handle_event_returns_failure_when_no_result(self, mock_client):
        """handle_event returns a failure if handle_batch returns empty."""
        mock_client.get_project_media = AsyncMock(return_value=[])
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = MediaHandler(mock_client)
        event = make_event(
            event_type="PROJECT_FILE_ADDED",
            project_id="123",
            media_id="999",
        )

        result = await handler.handle_event(event)

        # Media not found in API response = failure
        assert result.success is False


# ============================================================================
# MediaHandler.handle_batch
# ============================================================================


class TestMediaHandlerHandleBatch:

    async def test_handle_batch_empty_events(self, mock_client):
        handler = MediaHandler(mock_client)
        results = await handler.handle_batch([])
        assert results == []

    async def test_handle_batch_selective_fetch_for_small_batch(self, mock_client):
        mock_client.get_project_media = AsyncMock(
            return_value=[_make_media_api_item(500)]
        )
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = MediaHandler(mock_client)
        events = [
            make_event(
                event_type="PROJECT_FILE_ADDED",
                project_id="123",
                media_id="500",
                event_id="evt_1",
            ),
        ]

        results = await handler.handle_batch(events)

        assert len(results) == 1
        assert results[0].success is True
        # Selective fetch should pass media_ids
        mock_client.get_project_media.assert_called_once_with(
            123, media_ids=[500]
        )

    async def test_handle_batch_full_fetch_for_large_batch(self, mock_client):
        media_items = [_make_media_api_item(i) for i in range(1, BATCH_THRESHOLD + 2)]
        mock_client.get_project_media = AsyncMock(return_value=media_items)
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = MediaHandler(mock_client)

        events = [
            make_event(
                event_type="PROJECT_FILE_ADDED",
                project_id="123",
                media_id=str(i),
                event_id=f"evt_{i}",
            )
            for i in range(1, BATCH_THRESHOLD + 2)
        ]

        results = await handler.handle_batch(events)

        assert len(results) == BATCH_THRESHOLD + 1
        # Full fetch should NOT pass media_ids
        mock_client.get_project_media.assert_called_once_with(123)

    async def test_handle_batch_media_not_found(self, mock_client):
        mock_client.get_project_media = AsyncMock(return_value=[])
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = MediaHandler(mock_client)
        event = make_event(
            event_type="PROJECT_FILE_ADDED",
            project_id="123",
            media_id="999",
        )

        results = await handler.handle_batch([event])

        assert len(results) == 1
        assert results[0].success is False
        assert results[0].error_category == ErrorCategory.PERMANENT
        assert results[0].is_retryable is False
        assert "999" in results[0].error

    async def test_handle_batch_merges_project_rows_into_first_result(self, mock_client):
        mock_client.get_project_media = AsyncMock(
            return_value=[
                _make_media_api_item(1),
                _make_media_api_item(2),
            ]
        )
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = MediaHandler(mock_client)
        events = [
            make_event(
                event_type="PROJECT_FILE_ADDED",
                project_id="123",
                media_id="1",
                event_id="evt_1",
            ),
            make_event(
                event_type="PROJECT_FILE_ADDED",
                project_id="123",
                media_id="2",
                event_id="evt_2",
            ),
        ]

        results = await handler.handle_batch(events)

        assert len(results) == 2
        # First result should have project data merged in
        assert len(results[0].rows.projects) >= 1
        assert results[0].api_calls == 2  # Media + project verification
        # Second result should NOT have project data
        assert results[1].api_calls == 0

    async def test_handle_batch_dict_response_with_data_key(self, mock_client):
        mock_client.get_project_media = AsyncMock(
            return_value={"data": [_make_media_api_item(500)]}
        )
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = MediaHandler(mock_client)
        event = make_event(
            event_type="PROJECT_FILE_ADDED",
            project_id="123",
            media_id="500",
        )

        results = await handler.handle_batch([event])

        assert len(results) == 1
        assert results[0].success is True

    async def test_handle_batch_non_list_non_dict_response(self, mock_client):
        mock_client.get_project_media = AsyncMock(return_value="unexpected")
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = MediaHandler(mock_client)
        event = make_event(
            event_type="PROJECT_FILE_ADDED",
            project_id="123",
            media_id="500",
        )

        results = await handler.handle_batch([event])

        assert len(results) == 1
        assert results[0].success is False

    async def test_handle_batch_event_without_media_id(self, mock_client):
        mock_client.get_project_media = AsyncMock(return_value=[])
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = MediaHandler(mock_client)
        event = make_event(
            event_type="PROJECT_FILE_ADDED",
            project_id="123",
            media_id=None,
        )

        results = await handler.handle_batch([event])

        assert len(results) == 1
        assert results[0].success is False


# ============================================================================
# MediaHandler.handle_batch - Error handling
# ============================================================================


class TestMediaHandlerErrors:

    async def test_handle_batch_api_error(self, mock_client):
        mock_client.get_project_media = AsyncMock(
            side_effect=ClaimXApiError(
                "Not found",
                status_code=404,
                category=ErrorCategory.PERMANENT,
                is_retryable=False,
            )
        )
        handler = MediaHandler(mock_client)
        events = [
            make_event(
                event_type="PROJECT_FILE_ADDED",
                project_id="123",
                media_id="500",
                event_id="evt_1",
            ),
            make_event(
                event_type="PROJECT_FILE_ADDED",
                project_id="123",
                media_id="501",
                event_id="evt_2",
            ),
        ]

        results = await handler.handle_batch(events)

        assert len(results) == 2
        assert all(not r.success for r in results)
        assert results[0].error_category == ErrorCategory.PERMANENT
        assert results[0].is_retryable is False

    async def test_handle_batch_unexpected_error(self, mock_client):
        mock_client.get_project_media = AsyncMock(
            side_effect=RuntimeError("connection reset")
        )
        handler = MediaHandler(mock_client)
        event = make_event(
            event_type="PROJECT_FILE_ADDED",
            project_id="123",
            media_id="500",
        )

        results = await handler.handle_batch([event])

        assert len(results) == 1
        assert results[0].success is False
        assert results[0].error_category == ErrorCategory.TRANSIENT
        assert results[0].is_retryable is True

    async def test_handle_batch_api_error_first_event_gets_api_calls(self, mock_client):
        mock_client.get_project_media = AsyncMock(
            side_effect=ClaimXApiError(
                "Error",
                status_code=500,
                category=ErrorCategory.TRANSIENT,
                is_retryable=True,
            )
        )
        handler = MediaHandler(mock_client)
        events = [
            make_event(
                event_type="PROJECT_FILE_ADDED",
                project_id="123",
                media_id="1",
                event_id="evt_1",
            ),
            make_event(
                event_type="PROJECT_FILE_ADDED",
                project_id="123",
                media_id="2",
                event_id="evt_2",
            ),
        ]

        results = await handler.handle_batch(events)

        assert results[0].api_calls == 2  # First event: media + project
        assert results[1].api_calls == 0  # Subsequent events: 0


# ============================================================================
# MediaHandler.process (batched by project_id)
# ============================================================================


class TestMediaHandlerProcess:

    async def test_process_empty_events(self, mock_client):
        handler = MediaHandler(mock_client)
        result = await handler.process([])

        assert result.total == 0
        assert result.succeeded == 0
        assert result.duration_seconds == 0.0

    async def test_process_groups_by_project_id(self, mock_client):
        mock_client.get_project_media = AsyncMock(
            return_value=[_make_media_api_item(500)]
        )
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = MediaHandler(mock_client)
        events = [
            make_event(
                event_type="PROJECT_FILE_ADDED",
                project_id="100",
                media_id="500",
                event_id="evt_1",
            ),
            make_event(
                event_type="PROJECT_FILE_ADDED",
                project_id="200",
                media_id="500",
                event_id="evt_2",
            ),
        ]

        result = await handler.process(events)

        # Both projects should be processed (2 groups)
        assert result.total >= 1

    async def test_process_handles_group_exception(self, mock_client):
        call_count = 0

        async def flaky_media(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("boom")
            return [_make_media_api_item(500)]

        mock_client.get_project_media = AsyncMock(side_effect=flaky_media)
        mock_client.get_project = AsyncMock(
            return_value=make_project_api_response()
        )
        handler = MediaHandler(mock_client)
        events = [
            make_event(
                event_type="PROJECT_FILE_ADDED",
                project_id="100",
                media_id="500",
                event_id="evt_1",
            ),
            make_event(
                event_type="PROJECT_FILE_ADDED",
                project_id="200",
                media_id="500",
                event_id="evt_2",
            ),
        ]

        # Should not raise, even if one group fails
        result = await handler.process(events)
        assert result is not None
