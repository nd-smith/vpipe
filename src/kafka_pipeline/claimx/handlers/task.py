"""
Task event handler.

Handles: CUSTOM_TASK_ASSIGNED, CUSTOM_TASK_COMPLETED
"""

import logging
from datetime import datetime

from core.logging import get_logger, log_with_context
from core.types import ErrorCategory
from kafka_pipeline.claimx.handlers import transformers
from kafka_pipeline.claimx.handlers.base import (
    EnrichmentResult,
    EventHandler,
    register_handler,
    with_api_error_handling,
)
from kafka_pipeline.claimx.handlers.utils import (
    elapsed_ms,
    safe_int,
)
from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage
from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
from kafka_pipeline.common.logging import extract_log_context

logger = get_logger(__name__)


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
    supports_batching = False
    HANDLER_NAME = "task"

    @with_api_error_handling(
        api_calls=2,  # Task + Project verification
        log_context=lambda e: {
            "event_id": e.event_id,
            "task_assignment_id": e.task_assignment_id,
        },
    )
    async def handle_event(
        self, event: ClaimXEventMessage, start_time: datetime
    ) -> EnrichmentResult:
        """Fetch task details and transform to entity rows."""
        if not event.task_assignment_id:
            return EnrichmentResult(
                event=event,
                success=False,
                error="Missing task_assignment_id",
                error_category=ErrorCategory.PERMANENT,
                is_retryable=False,
                api_calls=0,
                duration_ms=0,
            )

        # Validate task_assignment_id is numeric
        assignment_id = safe_int(event.task_assignment_id)
        if assignment_id is None:
            return EnrichmentResult(
                event=event,
                success=False,
                error=f"Invalid task_assignment_id: {event.task_assignment_id} (must be numeric)",
                error_category=ErrorCategory.PERMANENT,
                is_retryable=False,
                api_calls=0,
                duration_ms=0,
            )

        # 1. Fetch task details
        response = await self.client.get_custom_task(assignment_id)

        rows = EntityRowsMessage()

        task_row = transformers.task_to_row(
            response,
            event_id=event.event_id,
        )
        if task_row.get("assignment_id") is not None:
            rows.tasks.append(task_row)

        project_id = safe_int(response.get("projectId")) or int(event.project_id)
        # assignment_id already validated above, use API response if available
        assignment_id = safe_int(response.get("assignmentId")) or assignment_id

        # 2. In-flight Project Verification
        project_rows = await self.ensure_project_exists(
            project_id,
            source_event_id=event.event_id,
        )
        rows.merge(project_rows)

        custom_task = response.get("customTask")
        if custom_task:
            template_row = transformers.template_to_row(
                custom_task,
                event_id=event.event_id,
            )
            if template_row.get("task_id") is not None:
                rows.task_templates.append(template_row)

        link_data = response.get("externalLinkData")
        if link_data:
            link_row = transformers.link_to_row(
                link_data,
                assignment_id=assignment_id,
                project_id=project_id,
                event_id=event.event_id,
            )
            if link_row.get("link_id") is not None:
                rows.external_links.append(link_row)

            contact_row = transformers.link_to_contact(
                link_data,
                project_id=project_id,
                assignment_id=assignment_id,
                event_id=event.event_id,
            )
            if contact_row:
                rows.contacts.append(contact_row)

        log_with_context(
            logger,
            logging.DEBUG,
            "Handler complete",
            handler_name=TaskHandler.HANDLER_NAME,
            task_assignment_id=event.task_assignment_id,
            tasks_count=len(rows.tasks),
            templates_count=len(rows.task_templates),
            links_count=len(rows.external_links),
            contacts_count=len(rows.contacts),
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
