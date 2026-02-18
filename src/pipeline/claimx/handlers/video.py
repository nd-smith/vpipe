"""
Video collaboration event handler.

Handles: VIDEO_COLLABORATION_INVITE_SENT, VIDEO_COLLABORATION_COMPLETED
"""

import logging
from datetime import UTC, datetime
from typing import Any

from core.types import ErrorCategory
from pipeline.claimx.api_client import ClaimXApiError
from pipeline.claimx.handlers import transformers
from pipeline.claimx.handlers.base import (
    EnrichmentResult,
    EventHandler,
    register_handler,
)
from pipeline.claimx.handlers.utils import (
    LOG_ERROR_TRUNCATE_SHORT,
    elapsed_ms,
)
from pipeline.claimx.schemas.entities import EntityRowsMessage
from pipeline.claimx.schemas.events import ClaimXEventMessage
from pipeline.common.logging import extract_log_context

logger = logging.getLogger(__name__)


@register_handler
class VideoCollabHandler(EventHandler):
    """
    Handler for video collaboration events.

    Fetches video collaboration report and extracts:
    - Video collab row → claimx_video_collab
    """

    event_types = ["VIDEO_COLLABORATION_INVITE_SENT", "VIDEO_COLLABORATION_COMPLETED"]
    HANDLER_NAME = "video"

    async def handle_event(self, event: ClaimXEventMessage) -> EnrichmentResult:
        """Fetch video collaboration data and transform to entity rows."""
        start_time = datetime.now(UTC)
        api_calls = 2  # Video + Project verification

        try:
            # 1. Fetch video collaboration report
            response = await self.client.get_video_collaboration(event.project_id)

            collab_data = self._extract_collab_data(response, event.project_id)

            rows = EntityRowsMessage()

            # 2. In-flight Project Verification
            # We need to ensure the project exists in our warehouse
            project_rows = await self.ensure_project_exists(
                int(event.project_id),
                trace_id=event.trace_id,
            )
            rows.merge(project_rows)

            if not collab_data:
                logger.warning(
                    "No video collaboration data",
                    extra={
                        "handler_name": VideoCollabHandler.HANDLER_NAME,
                        "trace_id": event.trace_id,
                        "project_id": event.project_id,
                    },
                )
                return EnrichmentResult(
                    event=event,
                    success=True,
                    rows=rows,  # Return project rows even if video data missing
                    api_calls=api_calls,
                    duration_ms=elapsed_ms(start_time),
                )

            video_row = transformers.video_collab_to_row(
                collab_data,
                trace_id=event.trace_id,
            )
            if video_row.get("video_collaboration_id") is not None:
                rows.video_collab.append(video_row)

            logger.debug(
                "Video collab extracted",
                extra={
                    "handler_name": VideoCollabHandler.HANDLER_NAME,
                    "video_collab_count": len(rows.video_collab),
                    "project_verification": bool(project_rows.projects),
                    **extract_log_context(event),
                },
            )

            return EnrichmentResult(
                event=event,
                success=True,
                rows=rows,
                api_calls=api_calls,
                duration_ms=elapsed_ms(start_time),
            )

        except ClaimXApiError as e:
            duration_ms = elapsed_ms(start_time)
            logger.warning(
                "API error",
                extra={
                    "handler_name": VideoCollabHandler.HANDLER_NAME,
                    "error_message": str(e)[:LOG_ERROR_TRUNCATE_SHORT],
                    "error_category": e.category.value if e.category else None,
                    "http_status": e.status_code,
                    "duration_ms": duration_ms,
                    **extract_log_context(event),
                },
            )
            return EnrichmentResult(
                event=event,
                success=False,
                error=str(e),
                error_category=e.category,
                is_retryable=e.is_retryable,
                api_calls=api_calls,
                duration_ms=duration_ms,
            )

        except Exception as e:
            duration_ms = elapsed_ms(start_time)
            logger.error(
                "Unexpected error",
                extra={
                    "handler_name": VideoCollabHandler.HANDLER_NAME,
                    "duration_ms": duration_ms,
                    **extract_log_context(event),
                },
                exc_info=True,
            )
            return EnrichmentResult(
                event=event,
                success=False,
                error=str(e),
                error_category=ErrorCategory.TRANSIENT,
                is_retryable=True,
                api_calls=api_calls,
                duration_ms=duration_ms,
            )

    @staticmethod
    def _unwrap_response(response: Any) -> Any:
        """Unwrap dict wrappers to get the collaboration data or list."""
        if not isinstance(response, dict):
            return response
        for key in ("data", "collaborations", "videoCollaboration"):
            if key in response:
                return response[key]
        return response

    def _extract_collab_data(
        self,
        response: Any,
        project_id: str,
    ) -> dict[str, Any] | None:
        """
        Extract video collaboration data from API response.

        API may return:
        - Single object: {...}
        - List: [{...}, {...}]
        - Wrapped: {"data": [...]} or {"collaborations": [...]}

        Args:
            response: Raw API response
            project_id: Project ID to match (maps to claimId in API)

        Returns:
            Video collaboration dict or None
        """
        if response is None:
            return None

        unwrapped = self._unwrap_response(response)

        if isinstance(unwrapped, list):
            if not unwrapped:
                return None
            for item in unwrapped:
                if str(item.get("claimId")) == project_id:
                    return item
            return unwrapped[0]

        return unwrapped if isinstance(unwrapped, dict) else None
