"""
Video collaboration event handler.

Handles: VIDEO_COLLABORATION_INVITE_SENT, VIDEO_COLLABORATION_COMPLETED
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional

from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage
from kafka_pipeline.claimx.handlers.base import (
    EnrichmentResult,
    EventHandler,
    register_handler,
    with_api_error_handling,
)
from kafka_pipeline.claimx.handlers.utils import (
    safe_int,
    safe_str,
    safe_decimal_str,
    parse_timestamp,
    now_datetime,
    elapsed_ms,
)

from core.logging import get_logger, log_with_context
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
        from kafka_pipeline.claimx.handlers.project import ProjectHandler
        
        project_handler = ProjectHandler(self.client, project_cache=self.project_cache)
        project_rows = await project_handler.fetch_project_data(
            int(event.project_id),
            source_event_id=event.event_id
        )
        rows.merge(project_rows)

        if not collab_data:
            log_with_context(
                logger,
                logging.WARNING,
                "No video collaboration data",
                handler_name="video",
                event_id=event.event_id,
                project_id=event.project_id,
            )
            return EnrichmentResult(
                event=event,
                success=True,
                rows=rows, # Return project rows even if video data missing
                api_calls=2,
                duration_ms=elapsed_ms(start_time),
            )

        video_row = VideoCollabTransformer.to_video_collab_row(
            collab_data,
            event_id=event.event_id,
        )
        if video_row.get("video_collaboration_id") is not None:
            rows.video_collab.append(video_row)

        log_with_context(
            logger,
            logging.DEBUG,
            "Video collab extracted",
            handler_name="video",
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
    ) -> Optional[Dict[str, Any]]:
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

            for item in response:
                if str(item.get("claimId")) == project_id:
                    return item

            return response[0]

        if isinstance(response, dict):
            return response

        return None


class VideoCollabTransformer:
    """
    Transforms ClaimX API video collaboration response to entity rows.

    API response structure (from POST /data with reportType=VIDEO_COLLABORATION):
    {
        "videoCollaborationId": 123,
        "claimId": 456,
        "mfn": "...",
        "emailUserName": "...",
        "numberOfVideos": 5,
        "totalTimeSeconds": 1234.567,
        ...
    }
    """

    @staticmethod
    def to_video_collab_row(
        data: Dict[str, Any],
        event_id: str,
    ) -> Dict[str, Any]:
        """
        Transform API response to video collaboration row.

        Args:
            data: API response dict
            event_id: Event ID for traceability

        Returns:
            Video collaboration row dict
        """
        now = now_datetime()

        first_name = safe_str(data.get("claimRepFirstName"))
        last_name = safe_str(data.get("claimRepLastName"))
        full_name = safe_str(data.get("claimRepFullName"))

        if not full_name and (first_name or last_name):
            parts = [p for p in [first_name, last_name] if p]
            full_name = " ".join(parts) if parts else None

        return {
            "video_collaboration_id": safe_int(
                data.get("videoCollaborationId") or data.get("id")
            ),
            "claim_id": safe_int(data.get("claimId")),
            "mfn": safe_str(data.get("mfn")),
            "claim_number": safe_str(data.get("claimNumber")),
            "policy_number": safe_str(data.get("policyNumber")),
            "email_user_name": safe_str(data.get("emailUserName")),
            "claim_rep_first_name": first_name,
            "claim_rep_last_name": last_name,
            "claim_rep_full_name": full_name,
            "number_of_videos": safe_int(data.get("numberOfVideos")),
            "number_of_photos": safe_int(data.get("numberOfPhotos")),
            "number_of_viewers": safe_int(data.get("numberOfViewers")),
            "session_count": safe_int(data.get("sessionCount")),
            "total_time_seconds": safe_decimal_str(data.get("totalTimeSeconds")),
            "total_time": safe_str(data.get("totalTime")),
            "created_date": parse_timestamp(data.get("createdDate")),
            "live_call_first_session": parse_timestamp(
                data.get("liveCallFirstSession")
            ),
            "live_call_last_session": parse_timestamp(data.get("liveCallLastSession")),
            "company_id": safe_int(data.get("companyId")),
            "company_name": safe_str(data.get("companyName")),
            "guid": safe_str(data.get("guid")),
            "event_id": event_id,
            "created_at": now,
            "updated_at": now,
            "last_enriched_at": now,
        }
