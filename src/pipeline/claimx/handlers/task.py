"""
Task event handler.

Handles: CUSTOM_TASK_ASSIGNED, CUSTOM_TASK_COMPLETED
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
    safe_int,
)
from pipeline.claimx.schemas.entities import EntityRowsMessage
from pipeline.claimx.schemas.events import ClaimXEventMessage
from pipeline.common.logging import extract_log_context

logger = logging.getLogger(__name__)


@register_handler
class TaskHandler(EventHandler):
    """
    Handler for custom task events.

    Fetches task assignment details and extracts:
    - Task row → claimx_tasks
    - Template row → claimx_task_templates
    - External link row → claimx_external_links
    - Contact row → claimx_contacts (from link recipient)
    """

    event_types = ["CUSTOM_TASK_ASSIGNED", "CUSTOM_TASK_COMPLETED"]
    HANDLER_NAME = "task"

    def _build_task_rows(
        self, response: dict, event: ClaimXEventMessage,
        assignment_id: int, project_id: int, project_rows: EntityRowsMessage,
    ) -> EntityRowsMessage:
        """Build entity rows from task API response."""
        rows = EntityRowsMessage()

        task_row = transformers.task_to_row(response, trace_id=event.trace_id)
        if task_row.get("assignment_id") is not None:
            rows.tasks.append(task_row)

        rows.merge(project_rows)

        custom_task = response.get("customTask")
        if custom_task:
            template_row = transformers.template_to_row(custom_task, trace_id=event.trace_id)
            if template_row.get("task_id") is not None:
                rows.task_templates.append(template_row)

        link_data = response.get("externalLinkData")
        if link_data:
            link_row = transformers.link_to_row(
                link_data, assignment_id=assignment_id,
                project_id=project_id, trace_id=event.trace_id,
            )
            if link_row.get("link_id") is not None:
                rows.external_links.append(link_row)

            contact_row = transformers.link_to_contact(
                link_data, project_id=project_id,
                assignment_id=assignment_id, trace_id=event.trace_id,
            )
            if contact_row:
                rows.contacts.append(contact_row)

        return rows

    async def handle_event(self, event: ClaimXEventMessage) -> EnrichmentResult:
        """Fetch task details and transform to entity rows."""
        start_time = datetime.now(UTC)
        api_calls = 2  # Task + Project verification

        if not event.task_assignment_id:
            return EnrichmentResult(
                event=event, success=False,
                error="Missing task_assignment_id",
                error_category=ErrorCategory.PERMANENT, is_retryable=False,
                api_calls=0, duration_ms=0,
            )

        assignment_id = safe_int(event.task_assignment_id)
        if assignment_id is None:
            return EnrichmentResult(
                event=event, success=False,
                error=f"Invalid task_assignment_id: {event.task_assignment_id} (must be numeric)",
                error_category=ErrorCategory.PERMANENT, is_retryable=False,
                api_calls=0, duration_ms=0,
            )

        try:
            response = await self.client.get_custom_task(assignment_id)

            project_id = safe_int(response.get("projectId")) or int(event.project_id)
            assignment_id = safe_int(response.get("assignmentId")) or assignment_id

            project_rows = await self.ensure_project_exists(
                project_id, trace_id=event.trace_id,
            )

            rows = self._build_task_rows(response, event, assignment_id, project_id, project_rows)

            logger.debug(
                "Handler complete",
                extra={
                    "handler_name": TaskHandler.HANDLER_NAME,
                    "task_assignment_id": event.task_assignment_id,
                    "tasks_count": len(rows.tasks),
                    "templates_count": len(rows.task_templates),
                    "links_count": len(rows.external_links),
                    "contacts_count": len(rows.contacts),
                    "project_verification": bool(project_rows.projects),
                    **extract_log_context(event),
                },
            )

            return EnrichmentResult(
                event=event, success=True, rows=rows,
                api_calls=api_calls, duration_ms=elapsed_ms(start_time),
            )

        except ClaimXApiError as e:
            duration_ms = elapsed_ms(start_time)
            logger.warning(
                "API error",
                extra={
                    "handler_name": TaskHandler.HANDLER_NAME,
                    "error_message": str(e)[:LOG_ERROR_TRUNCATE_SHORT],
                    "error_category": e.category.value if e.category else None,
                    "http_status": e.status_code,
                    "duration_ms": duration_ms,
                    **extract_log_context(event),
                },
            )
            return EnrichmentResult(
                event=event, success=False, error=str(e),
                error_category=e.category, is_retryable=e.is_retryable,
                api_calls=api_calls, duration_ms=duration_ms,
            )

        except Exception as e:
            duration_ms = elapsed_ms(start_time)
            logger.error(
                "Unexpected error",
                extra={
                    "handler_name": TaskHandler.HANDLER_NAME,
                    "duration_ms": duration_ms,
                    **extract_log_context(event),
                },
                exc_info=True,
            )
            return EnrichmentResult(
                event=event, success=False, error=str(e),
                error_category=ErrorCategory.TRANSIENT, is_retryable=True,
                api_calls=api_calls, duration_ms=duration_ms,
            )
