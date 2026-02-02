"""
Project update event handler.

Handles simple project field updates that don't require complex API calls or transformations.
Consolidates previously separate handlers (PolicyholderHandler, XALinkingHandler) into a
single configurable handler with event-to-field mapping.

Handles:
- POLICYHOLDER_INVITED
- POLICYHOLDER_JOINED
- PROJECT_AUTO_XA_LINKING_UNSUCCESSFUL
"""

import logging
from datetime import datetime
from typing import Any

from core.logging import get_logger, log_with_context
from kafka_pipeline.claimx.handlers.base import (
    EnrichmentResult,
    EventHandler,
    register_handler,
    with_api_error_handling,
)
from kafka_pipeline.claimx.handlers.utils import (
    API_CALLS_SINGLE,
    elapsed_ms,
    now_datetime,
)
from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage
from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
from kafka_pipeline.common.logging import extract_log_context

logger = get_logger(__name__)


# Event-to-field mapping configuration
# Maps event types to the project fields that should be updated
EVENT_FIELD_MAPPING: dict[str, dict[str, Any]] = {
    "POLICYHOLDER_INVITED": {
        "requires_verification": True,
        "fields": lambda: {"policyholder_invited_at": now_datetime()},
    },
    "POLICYHOLDER_JOINED": {
        "requires_verification": True,
        "fields": lambda: {"policyholder_joined_at": now_datetime()},
    },
    "PROJECT_AUTO_XA_LINKING_UNSUCCESSFUL": {
        "requires_verification": False,
        "fields": lambda: {
            "xa_autolink_fail": True,
            "xa_autolink_fail_at": now_datetime(),
        },
    },
}


@register_handler
class ProjectUpdateHandler(EventHandler):
    """
    Unified handler for simple project field updates.

    Uses event-to-field mapping to update project rows based on event type.
    Some events require in-flight project verification (policyholder events),
    while others can create minimal project rows directly (XA linking failure).
    """

    event_types = [
        "POLICYHOLDER_INVITED",
        "POLICYHOLDER_JOINED",
        "PROJECT_AUTO_XA_LINKING_UNSUCCESSFUL",
    ]
    supports_batching = False
    HANDLER_NAME = "project_update"

    @with_api_error_handling(
        api_calls=API_CALLS_SINGLE,  # Project verification (when required)
        log_context=lambda e: {"event_id": e.event_id, "project_id": e.project_id},
    )
    async def handle_event(
        self, event: ClaimXEventMessage, start_time: datetime
    ) -> EnrichmentResult:
        """Process project update event with optional in-flight verification."""
        event_config = EVENT_FIELD_MAPPING.get(event.event_type)
        if not event_config:
            return EnrichmentResult(
                event=event,
                success=False,
                error=f"Unknown event type: {event.event_type}",
                api_calls=0,
                duration_ms=0,
            )

        rows = EntityRowsMessage()
        api_calls = 0

        # Get fields to update (lambda allows dynamic timestamp generation)
        fields_to_update = event_config["fields"]()

        if event_config["requires_verification"]:
            # Fetch full project data for verification
            rows = await self.ensure_project_exists(
                int(event.project_id),
                source_event_id=event.event_id,
            )
            api_calls = 1

            # Apply field updates to the fetched project row
            if rows.projects:
                rows.projects[0].update(fields_to_update)
            else:
                # Fallback: create minimal project row if API didn't return data
                project_row = {
                    "project_id": event.project_id,
                    "event_id": event.event_id,
                }
                project_row.update(fields_to_update)
                rows.projects.append(project_row)

        else:
            # No verification needed - create minimal project row directly
            project_row = {
                "project_id": event.project_id,
                "updated_at": now_datetime(),
                "event_id": event.event_id,
            }
            project_row.update(fields_to_update)
            rows.projects.append(project_row)

        log_with_context(
            logger,
            logging.DEBUG,
            "Project update event processed",
            handler_name=ProjectUpdateHandler.HANDLER_NAME,
            fields_updated=list(fields_to_update.keys()),
            project_verification=event_config["requires_verification"],
            **extract_log_context(event),
        )

        return EnrichmentResult(
            event=event,
            success=True,
            rows=rows,
            api_calls=api_calls,
            duration_ms=elapsed_ms(start_time),
        )
