"""
Media event handler.

Handles: PROJECT_FILE_ADDED
"""

import asyncio
import logging
from collections import defaultdict
from datetime import UTC, datetime

from core.types import ErrorCategory
from pipeline.claimx.api_client import ClaimXApiError
from pipeline.claimx.handlers import transformers
from pipeline.claimx.handlers.base import (
    EnrichmentResult,
    EventHandler,
    HandlerResult,
    aggregate_results,
    register_handler,
)
from pipeline.claimx.handlers.utils import (
    LOG_ERROR_TRUNCATE_SHORT,
    elapsed_ms,
    safe_int,
)
from pipeline.claimx.schemas.entities import EntityRowsMessage
from pipeline.claimx.schemas.events import ClaimXEventMessage
from pipeline.common.logging import extract_log_context

logger = logging.getLogger(__name__)

# Batch threshold for triggering pre-flight project verification
# Reduces API calls while ensuring projects exist before processing attachments
BATCH_THRESHOLD = 5


@register_handler
class MediaHandler(EventHandler):
    """
    Handler for media events.

    Fetches media metadata and extracts:
    - Media row → claimx_media_metadata

    Supports batching by project_id to minimize API calls.
    """

    event_types = ["PROJECT_FILE_ADDED"]
    HANDLER_NAME = "media"

    async def process(self, events: list[ClaimXEventMessage]) -> HandlerResult:
        """Process media events grouped by project_id for batch optimization."""
        if not events:
            return HandlerResult(
                handler_name=self.name,
                total=0,
                succeeded=0,
                failed=0,
                failed_permanent=0,
                skipped=0,
                rows=EntityRowsMessage(),
                errors=[],
                duration_seconds=0.0,
                api_calls=0,
            )

        start_time = datetime.now(UTC)

        # Group by project_id
        groups: dict[str | None, list[ClaimXEventMessage]] = defaultdict(list)
        for event in events:
            groups[event.project_id].append(event)

        logger.debug(
            "Processing batched media events",
            extra={
                "handler_name": self.name,
                "total_events": len(events),
                "group_count": len(groups),
                "group_sizes": {str(k): len(v) for k, v in groups.items()},
            },
        )

        # Process all project groups concurrently
        async def process_group(key, group_events):
            return key, await self.handle_batch(group_events)

        tasks = [process_group(k, evts) for k, evts in groups.items()]
        group_results = await asyncio.gather(*tasks, return_exceptions=True)

        results = []
        groups_succeeded = 0
        groups_failed = 0

        for group_result in group_results:
            if isinstance(group_result, Exception):
                groups_failed += 1
                logger.error(
                    "Batch group processing failed",
                    extra={"handler_name": self.name},
                    exc_info=True,
                )
                continue
            _key, batch_results = group_result
            groups_succeeded += 1
            results.extend(batch_results)

        logger.debug(
            "Batched processing complete",
            extra={
                "handler_name": self.name,
                "groups_succeeded": groups_succeeded,
                "groups_failed": groups_failed,
                "total_results": len(results),
            },
        )

        handler_result = aggregate_results(self.name, results, start_time)

        logger.debug(
            "Handler processing complete",
            extra={
                "handler_name": self.name,
                "records_processed": handler_result.total,
                "records_succeeded": handler_result.succeeded,
                "records_failed": handler_result.failed,
                "records_failed_permanent": handler_result.failed_permanent,
                "api_calls": handler_result.api_calls,
                "duration_seconds": round(handler_result.duration_seconds, 3),
            },
        )

        return handler_result

    async def handle_batch(self, events: list[ClaimXEventMessage]) -> list[EnrichmentResult]:
        """Process batch of media events for same project."""
        if not events:
            return []

        project_id = events[0].project_id
        media_ids = [event.media_id for event in events if event.media_id]

        start_time = datetime.now(UTC)

        try:
            fetch_strategy = "selective" if len(media_ids) <= BATCH_THRESHOLD else "full"
            logger.debug(
                "Media fetch strategy selected",
                extra={
                    "handler_name": MediaHandler.HANDLER_NAME,
                    "project_id": project_id,
                    "media_count": len(media_ids),
                    "fetch_strategy": fetch_strategy,
                    "threshold": BATCH_THRESHOLD,
                },
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

            media_by_id: dict[int, dict] = {}
            for media in media_list:
                mid = safe_int(media.get("mediaID"))
                if mid is not None:
                    media_by_id[mid] = media

            # In-flight Project Verification
            # Ensure the project exists in our warehouse before processing media
            project_rows = await self.ensure_project_exists(
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

            logger.debug(
                "Handler complete",
                extra={
                    "handler_name": MediaHandler.HANDLER_NAME,
                    "project_id": project_id,
                    "events_count": len(events),
                    "media_count": total_media_rows,
                    "succeeded": sum(1 for r in results if r.success),
                    "failed": sum(1 for r in results if not r.success),
                    "project_verification": bool(project_rows.projects),
                },
            )

            return results

        except ClaimXApiError as e:
            duration = elapsed_ms(start_time)
            logger.warning(
                "API error for project media",
                extra={
                    "handler_name": MediaHandler.HANDLER_NAME,
                    "project_id": project_id,
                    "error_message": str(e)[:LOG_ERROR_TRUNCATE_SHORT],
                    "error_category": e.category.value if e.category else None,
                    "http_status": e.status_code,
                    "is_retryable": e.is_retryable,
                },
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
            logger.error(
                "Unexpected error for project media",
                extra={
                    "handler_name": MediaHandler.HANDLER_NAME,
                    "project_id": project_id,
                },
                exc_info=True,
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
        media_by_id: dict[int, dict],
        project_id: int,
        batch_start_time: datetime,
    ) -> EnrichmentResult:
        """Process single event using pre-fetched media data."""
        rows = EntityRowsMessage()

        media_id_int = safe_int(event.media_id) if event.media_id else None
        media_data = media_by_id.get(media_id_int) if media_id_int else None

        if media_data:
            media_row = transformers.media_to_row(
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
            logger.warning(
                "Media not found in API response",
                extra={
                    **extract_log_context(event),
                },
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
