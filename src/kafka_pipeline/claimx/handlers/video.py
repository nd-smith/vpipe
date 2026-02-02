"""
Video collaboration event handler.

Handles: VIDEO_COLLABORATION_INVITE_SENT, VIDEO_COLLABORATION_COMPLETED
"""

import logging
from datetime import datetime
from typing import Any

from core.logging import get_logger, log_with_context
from kafka_pipeline.claimx.handlers import transformers
from kafka_pipeline.claimx.handlers.base import (
    EnrichmentResult,
    EventHandler,
    register_handler,
    with_api_error_handling,
)
from kafka_pipeline.claimx.handlers.utils import (
    elapsed_ms,
)
from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage
from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
from kafka_pipeline.common.logging import extract_log_context

logger = get_logger(__name__)


@register_handler
class VideoCollabHandler(EventHandler):
    """
    Handler for video collaboration events.

    Fetches video collaboration report and extracts:
    - Video collab row → claimx_video_collab
    """

    event_types = ["VIDEO_COLLABORATION_INVITE_SENT", "VIDEO_COLLABORATION_COMPLETED"]
    supports_batching = False
    HANDLER_NAME = "video"

    @with_api_error_handling(
        api_calls=2,  # Video + Project verification
        log_context=lambda e: {"event_id": e.event_id, "project_id": e.project_id},
    )
    async def handle_event(
        self, event: ClaimXEventMessage, start_time: datetime
    ) -> EnrichmentResult:
        """Fetch video collaboration data and transform to entity rows."""
        # 1. Fetch video collaboration report
        response = await self.client.get_video_collaboration(event.project_id)

        collab_data = self._extract_collab_data(response, event.project_id)

        rows = EntityRowsMessage()

        # 2. In-flight Project Verification
        # We need to ensure the project exists in our warehouse
        project_rows = await self.ensure_project_exists(
            int(event.project_id),
            source_event_id=event.event_id,
        )
        rows.merge(project_rows)

        if not collab_data:
            log_with_context(
                logger,
                logging.WARNING,
                "No video collaboration data",
                handler_name=VideoCollabHandler.HANDLER_NAME,
                event_id=event.event_id,
                project_id=event.project_id,
            )
            return EnrichmentResult(
                event=event,
                success=True,
                rows=rows,  # Return project rows even if video data missing
                api_calls=2,
                duration_ms=elapsed_ms(start_time),
            )

        video_row = transformers.video_collab_to_row(
            collab_data,
            event_id=event.event_id,
        )
        if video_row.get("video_collaboration_id") is not None:
            rows.video_collab.append(video_row)

        log_with_context(
            logger,
            logging.DEBUG,
            "Video collab extracted",
            handler_name=VideoCollabHandler.HANDLER_NAME,
            video_collab_count=len(rows.video_collab),
            project_verification=bool(project_rows.projects),
            **extract_log_context(event),
        )

        return EnrichmentResult(
            event=event,
            success=True,
            rows=rows,
            api_calls=2,
            duration_ms=elapsed_ms(start_time),
        )

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

        if isinstance(response, dict):
            if "data" in response:
                response = response["data"]
            elif "collaborations" in response:
                response = response["collaborations"]
            elif "videoCollaboration" in response:
                response = response["videoCollaboration"]

        if isinstance(response, list):
            if not response:
                return None

            for video_collaboration in response:
                if str(video_collaboration.get("claimId")) == project_id:
                    return video_collaboration

            return response[0]

        if isinstance(response, dict):
            return response

        return None
