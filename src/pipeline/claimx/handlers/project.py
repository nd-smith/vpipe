"""
Project event handler.

Handles: PROJECT_CREATED, PROJECT_MFN_ADDED
"""

import logging
from datetime import UTC, datetime

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
    now_datetime,
)
from pipeline.claimx.schemas.entities import EntityRowsMessage
from pipeline.claimx.schemas.events import ClaimXEventMessage
from pipeline.common.logging import extract_log_context

logger = logging.getLogger(__name__)


@register_handler
class ProjectHandler(EventHandler):
    """
    Handler for project events.

    Fetches full project details and extracts:
    - Project row → claimx_projects
    - Contact rows → claimx_contacts
    """

    event_types = ["PROJECT_CREATED", "PROJECT_MFN_ADDED"]
    HANDLER_NAME = "project"

    async def handle_event(self, event: ClaimXEventMessage) -> EnrichmentResult:
        """Fetch project details and transform to entity rows."""
        start_time = datetime.now(UTC)

        logger.debug(
            "Processing project event",
            extra={
                "handler_name": ProjectHandler.HANDLER_NAME,
                **extract_log_context(event),
            },
        )

        try:
            rows = EntityRowsMessage()

            if event.event_type == "PROJECT_MFN_ADDED":
                # In-flight project verification: ensure project exists in warehouse
                # Fetch full project data first, then overlay MFN from event
                rows = await self.fetch_project_data(
                    int(event.project_id), source_event_id=event.event_id
                )

                # Overlay the MFN from event payload (this is the authoritative value)
                if rows.projects and event.master_file_name:
                    rows.projects[0]["master_file_name"] = event.master_file_name
                elif not rows.projects:
                    # Fallback if API call failed to return project data
                    rows.projects.append(
                        {
                            "project_id": event.project_id,
                            "master_file_name": event.master_file_name,
                            "updated_at": now_datetime(),
                            "event_id": event.event_id,
                        }
                    )

                duration_ms = elapsed_ms(start_time)
                logger.debug(
                    "Handler complete",
                    extra={
                        "handler_name": ProjectHandler.HANDLER_NAME,
                        "api_calls": 1,
                        "projects_count": len(rows.projects),
                        "contacts_count": len(rows.contacts),
                        "duration_ms": duration_ms,
                        **extract_log_context(event),
                    },
                )

                return EnrichmentResult(
                    event=event,
                    success=True,
                    rows=rows,
                    api_calls=1,
                    duration_ms=duration_ms,
                )

            # PROJECT_CREATED - fetch full project details
            rows = await self.fetch_project_data(
                int(event.project_id), source_event_id=event.event_id
            )

            duration_ms = elapsed_ms(start_time)
            logger.debug(
                "Handler complete",
                extra={
                    "handler_name": ProjectHandler.HANDLER_NAME,
                    "api_calls": 1,
                    "projects_count": len(rows.projects),
                    "contacts_count": len(rows.contacts),
                    "duration_ms": duration_ms,
                    **extract_log_context(event),
                },
            )

            return EnrichmentResult(
                event=event,
                success=True,
                rows=rows,
                api_calls=1,
                duration_ms=duration_ms,
            )

        except ClaimXApiError as e:
            duration_ms = elapsed_ms(start_time)
            logger.warning(
                "API error for project",
                extra={
                    "handler_name": ProjectHandler.HANDLER_NAME,
                    "error_message": str(e)[:LOG_ERROR_TRUNCATE_SHORT],
                    "error_category": e.category.value if e.category else None,
                    "http_status": e.status_code,
                    "is_retryable": e.is_retryable,
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
                api_calls=1,
                duration_ms=duration_ms,
            )

        except Exception as e:
            duration_ms = elapsed_ms(start_time)
            logger.error(
                "Unexpected error for project",
                extra={
                    "handler_name": ProjectHandler.HANDLER_NAME,
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
                api_calls=1,
                duration_ms=duration_ms,
            )

    async def fetch_project_data(
        self,
        project_id: int,
        source_event_id: str | None = None,
    ) -> EntityRowsMessage:
        """
        Fetch project details and transform to entity rows.

        Reusable by other handlers for in-flight verification.

        Uses project_cache if available to avoid redundant API calls
        for projects that have already been processed in this session.
        """
        project_id_str = str(project_id)

        # Check cache first - skip API call if project already processed
        if self.project_cache and self.project_cache.has(project_id_str):
            logger.debug(
                "Project in cache - skipping API call",
                extra={
                    "project_id": project_id,
                    "source_event_id": source_event_id,
                    "cache_size": self.project_cache.size(),
                },
            )
            return EntityRowsMessage()  # Return empty - project already in Delta

        # Project not in cache - fetch from API
        response = await self.client.get_project(project_id)
        rows = EntityRowsMessage()

        # Transform response to entity rows
        project_row = transformers.project_to_row(
            response,
            event_id=source_event_id,
        )
        # Use input project_id as fallback when API response structure varies
        if project_row.get("project_id") is None:
            project_row["project_id"] = project_id_str
        rows.projects.append(project_row)

        contact_rows = transformers.project_to_contacts(
            response,
            project_id=str(project_id),
            event_id=source_event_id or "",
        )
        rows.contacts.extend(contact_rows)

        # Add to cache only after successfully producing a project row
        if self.project_cache and rows.projects:
            self.project_cache.add(project_id_str)

        return rows
