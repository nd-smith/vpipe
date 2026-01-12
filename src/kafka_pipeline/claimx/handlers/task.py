"""
Task event handler.

Handles: CUSTOM_TASK_ASSIGNED, CUSTOM_TASK_COMPLETED
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from kafka_pipeline.claimx.api_client import ClaimXApiError
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
    safe_int32,
    safe_str,
    safe_str_id,
    safe_bool,
    parse_timestamp,
    now_iso,
    now_datetime,
    today_date,
    elapsed_ms,
)

from core.types import ErrorCategory
from core.logging import get_logger, log_with_context
from kafka_pipeline.common.logging import extract_log_context

logger = get_logger(__name__)


class TaskTransformer:
    """
    Transforms ClaimX API task response to entity rows.

    API response structure (from /customTasks/assignment/{assignmentId}):
    {
        "assignmentId": 123,
        "taskId": 456,
        "projectId": 789,
        "customTask": {...},
        "externalLinkData": {...}
    }
    """

    @staticmethod
    def to_task_row(
        data: Dict[str, Any],
        event_id: str,
    ) -> Dict[str, Any]:
        """Transform task assignment to row."""
        now = now_datetime()

        return {
            "assignment_id": safe_int(data.get("assignmentId")),
            "task_id": safe_int(data.get("taskId")),
            "task_name": safe_str(data.get("taskName")),
            "form_id": safe_str(data.get("formId")),
            "project_id": safe_str_id(data.get("projectId")),
            "assignee_id": safe_int(data.get("assigneeId")),
            "assignor_id": safe_int(data.get("assignorId")),
            "assignor_email": safe_str(data.get("assignor")),
            "date_assigned": parse_timestamp(data.get("dateAssigned")),
            "date_completed": parse_timestamp(data.get("dateCompleted")),
            "cancelled_date": parse_timestamp(data.get("cancelledDate")),
            "cancelled_by_resource_id": safe_int(data.get("cancelledByResourceId")),
            "status": safe_str(data.get("status")),
            "pdf_project_media_id": safe_int(data.get("pdfProjectMediaId")),
            "date_exported": parse_timestamp(data.get("dateExported")),
            "form_response_id": safe_str(data.get("formResponseId")),
            "stp_enabled": safe_bool(data.get("stpEnabled")),
            "stp_started_date": parse_timestamp(data.get("stpStartedDate")),
            "mfn": safe_str(data.get("mfn")),
            "xactimate_exportable": safe_bool(data.get("xactimateExportable")),
            "fraud_language_accepted_date": parse_timestamp(
                data.get("fraudLanguageAcceptedDate")
            ),
            "resubmit_task_assignment_id": safe_int(
                data.get("resubmitTaskAssignmentId")
            ),
            "task_url": safe_str(data.get("url")),
            "event_id": event_id,
            "created_at": now,
            "updated_at": now,
            "last_enriched_at": now,
        }

    @staticmethod
    def to_template_row(
        template: Dict[str, Any],
        event_id: str,
    ) -> Dict[str, Any]:
        """Transform custom task template to row."""
        now = now_datetime()

        return {
            "task_id": safe_int(template.get("taskId")),
            "comp_id": safe_int(template.get("compId")),
            "name": safe_str(template.get("name")),
            "description": safe_str(template.get("description")),
            "form_id": safe_str(template.get("formId")),
            "form_name": safe_str(template.get("formName")),
            "enabled": safe_bool(template.get("enabled")),
            "is_default": safe_bool(template.get("default")),
            "is_manual_delivery": safe_bool(template.get("isManualDelivery")),
            "is_external_link_delivery": safe_bool(
                template.get("isExternalLinkDelivery")
            ),
            "provide_portal_access": safe_bool(template.get("providePortalAccess")),
            "notify_assigned_send_recipient": safe_bool(
                template.get("notifyAssignedSendRecipient")
            ),
            "notify_assigned_send_recipient_sms": safe_bool(
                template.get("notifyAssignedSendRecipientSms")
            ),
            "notify_assigned_subject": safe_str(template.get("notifyAssignedSubject")),
            "notify_task_completed": safe_bool(template.get("notifyTaskCompleted")),
            "notify_completed_subject": safe_str(
                template.get("notifyCompletedSubject")
            ),
            "allow_resubmit": safe_bool(template.get("allowReSubmit")),
            "auto_generate_pdf": safe_bool(template.get("autoGeneratePdf")),
            "modified_by": safe_str(template.get("modifiedBy")),
            "modified_by_id": safe_int(template.get("modifiedById")),
            "modified_date": parse_timestamp(template.get("modifiedDate")),
            "event_id": event_id,
            "created_at": now,
            "updated_at": now,
            "last_enriched_at": now,
        }

    @staticmethod
    def to_link_row(
        link: Dict[str, Any],
        assignment_id: int,
        project_id: Any,
        event_id: str,
    ) -> Dict[str, Any]:
        """Transform external link data to row."""
        now = now_iso()

        return {
            "link_id": safe_int(link.get("linkId")),
            "assignment_id": assignment_id,
            "project_id": safe_str_id(project_id),
            "link_code": safe_str(link.get("linkCode")),
            "url": safe_str(link.get("url")),
            "notification_access_method": safe_str(
                link.get("notificationAccessMethod")
            ),
            "country_id": safe_int(link.get("countryId")),
            "state_id": safe_int(link.get("stateId")),
            "created_date": None,
            "accessed_count": 0,
            "last_accessed": None,
            "event_id": event_id,
            "created_at": now,
            "updated_at": now,
        }

    @staticmethod
    def to_contact_from_link(
        link: Dict[str, Any],
        project_id: Any,
        assignment_id: int,
        event_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Extract contact from external link data."""
        email = safe_str(link.get("email"))
        if not email:
            return None

        now = now_datetime()
        today = today_date()
        return {
            "project_id": safe_str_id(project_id),
            "contact_email": email,
            "contact_type": "POLICYHOLDER",
            "first_name": safe_str(link.get("firstName")),
            "last_name": safe_str(link.get("lastName")),
            "phone_number": safe_str(link.get("phone")),
            "phone_country_code": safe_int(link.get("phoneCountryCode")),
            "is_primary_contact": False,
            "master_file_name": None,
            "task_assignment_id": safe_int32(assignment_id),
            "video_collaboration_id": None,
            "event_id": event_id,
            "created_at": now,
            "updated_at": now_iso(),
            "created_date": today,
            "last_enriched_at": now,
        }


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

        # 1. Fetch task details
        response = await self.client.get_custom_task(int(event.task_assignment_id))

        rows = EntityRowsMessage()

        task_row = TaskTransformer.to_task_row(
            response,
            event_id=event.event_id,
        )
        if task_row.get("assignment_id") is not None:
            rows.tasks.append(task_row)

        project_id = safe_int(response.get("projectId")) or int(event.project_id)
        assignment_id = (
            safe_int(response.get("assignmentId")) or int(event.task_assignment_id)
        )
        
        # 2. In-flight Project Verification
        from kafka_pipeline.claimx.handlers.project import ProjectHandler
        
        project_handler = ProjectHandler(self.client, project_cache=self.project_cache)
        project_rows = await project_handler.fetch_project_data(
            project_id,
            source_event_id=event.event_id
        )
        rows.merge(project_rows)

        custom_task = response.get("customTask")
        if custom_task:
            template_row = TaskTransformer.to_template_row(
                custom_task,
                event_id=event.event_id,
            )
            if template_row.get("task_id") is not None:
                rows.task_templates.append(template_row)

        link_data = response.get("externalLinkData")
        if link_data:
            link_row = TaskTransformer.to_link_row(
                link_data,
                assignment_id=assignment_id,
                project_id=project_id,
                event_id=event.event_id,
            )
            if link_row.get("link_id") is not None:
                rows.external_links.append(link_row)

            contact_row = TaskTransformer.to_contact_from_link(
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
            handler_name="task",
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
