"""
Policyholder contact event handler.

Handles: POLICYHOLDER_INVITED, POLICYHOLDER_JOINED
"""

import logging
from datetime import datetime, timezone

from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage
from kafka_pipeline.claimx.handlers.base import (
    EnrichmentResult,
    EventHandler,
    register_handler,
    with_api_error_handling,
)
from kafka_pipeline.claimx.handlers.utils import elapsed_ms, now_datetime

from core.logging import get_logger, log_with_context
from kafka_pipeline.common.logging import extract_log_context

logger = get_logger(__name__)


@register_handler
class PolicyholderHandler(EventHandler):
    """
    Handler for policyholder events.

    Updates projects table with invite/join timestamps.
    No contact record created (no reliable email in these events).
    """

    event_types = ["POLICYHOLDER_INVITED", "POLICYHOLDER_JOINED"]
    supports_batching = False

    @with_api_error_handling(
        api_calls=1,  # Project verification
        log_context=lambda e: {"event_id": e.event_id, "project_id": e.project_id},
    )
    async def handle_event(
        self, event: ClaimXEventMessage, start_time: datetime
    ) -> EnrichmentResult:
        """Process policyholder event with in-flight project verification."""
        # In-flight Project Verification
        # Ensure the project exists in our warehouse
        from kafka_pipeline.claimx.handlers.project import ProjectHandler

        project_handler = ProjectHandler(self.client, project_cache=self.project_cache)
        rows = await project_handler.fetch_project_data(
            int(event.project_id),
            source_event_id=event.event_id,
        )

        # Apply policyholder timestamp update to the project row
        now = now_datetime()
        if rows.projects:
            # Update the fetched project row with policyholder timestamp
            if event.event_type == "POLICYHOLDER_INVITED":
                rows.projects[0]["policyholder_invited_at"] = now
            elif event.event_type == "POLICYHOLDER_JOINED":
                rows.projects[0]["policyholder_joined_at"] = now
        else:
            # Fallback: create minimal project row if API didn't return data
            project_row = {
                "project_id": str(event.project_id),
                "policyholder_invited_at": (
                    now if event.event_type == "POLICYHOLDER_INVITED" else None
                ),
                "policyholder_joined_at": (
                    now if event.event_type == "POLICYHOLDER_JOINED" else None
                ),
            }
            rows.projects.append(project_row)

        log_with_context(
            logger,
            logging.DEBUG,
            "Policyholder event processed",
            handler_name="contact",
            project_verification=bool(rows.projects),
            **extract_log_context(event),
        )

        return EnrichmentResult(
            event=event,
            success=True,
            rows=rows,
            api_calls=1,
            duration_ms=elapsed_ms(start_time),
        )
