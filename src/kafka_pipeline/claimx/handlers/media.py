"""
Media event handler.

Handles: PROJECT_FILE_ADDED
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from kafka_pipeline.claimx.api_client import ClaimXApiError
from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage
from kafka_pipeline.claimx.handlers.base import (
    EnrichmentResult,
    EventHandler,
    register_handler,
)
from kafka_pipeline.claimx.handlers.utils import (
    safe_int,
    safe_str,
    safe_str_id,
    safe_float,
    now_iso,
    elapsed_ms,
)

from core.types import ErrorCategory
from core.logging import get_logger, log_exception, log_with_context
from kafka_pipeline.common.logging import extract_log_context
from kafka_pipeline.common.utils import extract_expires_at_iso

logger = get_logger(__name__)

BATCH_THRESHOLD = 5


class MediaTransformer:
    """
    Transforms ClaimX API media response to entity rows.

    API response structure (from /export/project/{projectId}/media):
    {
        "data": [
            {
                "mediaID": 123,
                "taskAssignmentID": 456,
                "mediaType": "jpg",
                "mediaName": "photo.jpg",
                "fullDownloadLink": "https://..."
            }
        ]
    }
    """

    @staticmethod
    def to_media_row(
        media: Dict[str, Any],
        project_id: Any,
        event_id: str,
    ) -> Dict[str, Any]:
        """
        Transform media item to row.

        Args:
            media: Media dict from API
            project_id: Project ID (int or string)
            event_id: Event ID for traceability

        Returns:
            Media row dict
        """
        now = now_iso()
        download_link = safe_str(media.get("fullDownloadLink"))

        return {
            "media_id": safe_str_id(media.get("mediaID")),
            "project_id": safe_str_id(project_id),
            "task_assignment_id": safe_str(media.get("taskAssignmentId")),
            "file_type": safe_str(media.get("mediaType")),
            "file_name": safe_str(media.get("mediaName")),
            "media_description": safe_str(media.get("mediaDescription")),
            "media_comment": safe_str(media.get("mediaComment")),
            "latitude": safe_str(media.get("latitude")),
            "longitude": safe_str(media.get("longitude")),
            "gps_source": safe_str(media.get("gpsSource")),
            "taken_date": safe_str(media.get("takenDate")),
            "full_download_link": download_link,
            "expires_at": safe_str(media.get("expiresAt")),
            "event_id": event_id,
            "created_at": now,
            "updated_at": now,
            "last_enriched_at": now,
        }


@register_handler
class MediaHandler(EventHandler):
    """
    Handler for media events.

    Fetches media metadata and extracts:
    - Media row → claimx_media_metadata

    Supports batching by project_id to minimize API calls.
    """

    event_types = ["PROJECT_FILE_ADDED"]
    supports_batching = True
    batch_key = "project_id"

    async def handle_batch(
        self, events: List[ClaimXEventMessage]
    ) -> List[EnrichmentResult]:
        """Process batch of media events for same project."""
        if not events:
            return []

        project_id = events[0].project_id
        media_ids = [event.media_id for event in events if event.media_id]

        start_time = datetime.now(timezone.utc)

        try:
            fetch_strategy = (
                "selective" if len(media_ids) <= BATCH_THRESHOLD else "full"
            )
            log_with_context(
                logger,
                logging.DEBUG,
                "Media fetch strategy selected",
                handler_name="media",
                project_id=project_id,
                media_count=len(media_ids),
                fetch_strategy=fetch_strategy,
                threshold=BATCH_THRESHOLD,
            )

            if len(media_ids) <= BATCH_THRESHOLD:
                response = await self.client.get_project_media(
                    int(project_id),
                    media_ids=[int(m) for m in media_ids if m],
                )
            else:
                response = await self.client.get_project_media(int(project_id))

            # Normalize response to list
            if isinstance(response, list):
                media_list = response
            elif isinstance(response, dict):
                media_list = response.get("data", [])
            else:
                media_list = []

            if not isinstance(media_list, list):
                media_list = [media_list] if media_list else []

            media_by_id: Dict[int, Dict] = {}
            for media in media_list:
                mid = safe_int(media.get("mediaID"))
                if mid is not None:
                    media_by_id[mid] = media

            # In-flight Project Verification
            # Ensure the project exists in our warehouse before processing media
            from kafka_pipeline.claimx.handlers.project import ProjectHandler

            project_handler = ProjectHandler(self.client, project_cache=self.project_cache)
            project_rows = await project_handler.fetch_project_data(
                int(project_id),
                source_event_id=events[0].event_id,
            )

            results = []
            total_media_rows = 0
            for i, event in enumerate(events):
                result = self._process_single_event(
                    event,
                    media_by_id,
                    int(project_id),
                    start_time,
                )
                # Merge project rows into first result only and update api_calls
                if i == 0:
                    if result.rows:
                        result.rows.merge(project_rows)
                    # Update api_calls to include project verification (media=1 + project=1)
                    result = EnrichmentResult(
                        event=result.event,
                        success=result.success,
                        rows=result.rows,
                        error=result.error,
                        error_category=result.error_category,
                        is_retryable=result.is_retryable,
                        api_calls=2,  # Media API + Project verification API
                        duration_ms=result.duration_ms,
                    )
                results.append(result)
                if result.rows:
                    total_media_rows += len(result.rows.media)

            log_with_context(
                logger,
                logging.DEBUG,
                "Handler complete",
                handler_name="media",
                project_id=project_id,
                events_count=len(events),
                media_count=total_media_rows,
                succeeded=sum(1 for r in results if r.success),
                failed=sum(1 for r in results if not r.success),
                project_verification=bool(project_rows.projects),
            )

            return results

        except ClaimXApiError as e:
            duration = elapsed_ms(start_time)
            log_with_context(
                logger,
                logging.WARNING,
                "API error for project media",
                handler_name="media",
                project_id=project_id,
                error_message=str(e)[:200],
                error_category=e.category.value if e.category else None,
                http_status=e.status_code,
                is_retryable=e.is_retryable,
            )
            return [
                EnrichmentResult(
                    event=event,
                    success=False,
                    error=str(e),
                    error_category=e.category,
                    is_retryable=e.is_retryable,
                    api_calls=2 if i == 0 else 0,  # Media + Project verification
                    duration_ms=duration if i == 0 else 0,
                )
                for i, event in enumerate(events)
            ]

        except Exception as e:
            duration = elapsed_ms(start_time)
            log_exception(
                logger,
                e,
                "Unexpected error for project media",
                handler_name="media",
                project_id=project_id,
            )
            return [
                EnrichmentResult(
                    event=event,
                    success=False,
                    error=str(e),
                    error_category=ErrorCategory.TRANSIENT,
                    is_retryable=True,
                    api_calls=2 if i == 0 else 0,  # Media + Project verification
                    duration_ms=duration if i == 0 else 0,
                )
                for i, event in enumerate(events)
            ]

    def _process_single_event(
        self,
        event: ClaimXEventMessage,
        media_by_id: Dict[int, Dict],
        project_id: int,
        batch_start_time: datetime,
    ) -> EnrichmentResult:
        """Process single event using pre-fetched media data."""
        rows = EntityRowsMessage()

        media_id_int = safe_int(event.media_id) if event.media_id else None
        media_data = media_by_id.get(media_id_int) if media_id_int else None

        if media_data:
            media_row = MediaTransformer.to_media_row(
                media_data,
                project_id=project_id,
                event_id=event.event_id,
            )
            if media_row.get("media_id") is not None:
                rows.media.append(media_row)

            return EnrichmentResult(
                event=event,
                success=True,
                rows=rows,
                api_calls=0,
                duration_ms=0,
            )
        else:
            log_with_context(
                logger,
                logging.WARNING,
                "Media not found in API response",
                **extract_log_context(event),
            )
            return EnrichmentResult(
                event=event,
                success=False,
                error=f"Media {event.media_id} not found in API response",
                error_category=ErrorCategory.PERMANENT,
                is_retryable=False,
                api_calls=0,
                duration_ms=0,
            )

    async def handle_event(self, event: ClaimXEventMessage) -> EnrichmentResult:
        """Handle single media event (fallback if not batched)."""
        results = await self.handle_batch([event])
        return (
            results[0]
            if results
            else EnrichmentResult(
                event=event,
                success=False,
                error="No result from batch handler",
                error_category=ErrorCategory.TRANSIENT,
                is_retryable=True,
            )
        )
