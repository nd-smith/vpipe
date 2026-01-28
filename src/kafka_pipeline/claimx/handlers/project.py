"""
Project event handler.

Handles: PROJECT_CREATED, PROJECT_MFN_ADDED
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

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
    safe_bool,
    parse_timestamp,
    now_datetime,
    now_iso,
    today_date,
    elapsed_ms,
    BaseTransformer,
)

from core.types import ErrorCategory
from core.logging import get_logger, log_exception, log_with_context
from kafka_pipeline.common.logging import extract_log_context

logger = get_logger(__name__)


@register_handler
class ProjectHandler(EventHandler):
    """
    Handler for project events.

    Fetches full project details and extracts:
    - Project row → claimx_projects
    - Contact rows → claimx_contacts
    """

    event_types = ["PROJECT_CREATED", "PROJECT_MFN_ADDED"]
    supports_batching = False

    async def handle_event(self, event: ClaimXEventMessage) -> EnrichmentResult:
        """Fetch project details and transform to entity rows."""
        start_time = datetime.now(timezone.utc)

        log_with_context(
            logger,
            logging.DEBUG,
            "Processing project event",
            handler_name="project",
            **extract_log_context(event),
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
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "Handler complete",
                    handler_name="project",
                    api_calls=1,
                    projects_count=len(rows.projects),
                    contacts_count=len(rows.contacts),
                    duration_ms=duration_ms,
                    **extract_log_context(event),
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
            log_with_context(
                logger,
                logging.DEBUG,
                "Handler complete",
                handler_name="project",
                api_calls=1,
                projects_count=len(rows.projects),
                contacts_count=len(rows.contacts),
                duration_ms=duration_ms,
                **extract_log_context(event),
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
            log_with_context(
                logger,
                logging.WARNING,
                "API error for project",
                handler_name="project",
                error_message=str(e)[:200],
                error_category=e.category.value if e.category else None,
                http_status=e.status_code,
                is_retryable=e.is_retryable,
                duration_ms=duration_ms,
                **extract_log_context(event),
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
            log_exception(
                logger,
                e,
                "Unexpected error for project",
                handler_name="project",
                duration_ms=duration_ms,
                **extract_log_context(event),
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
        source_event_id: Optional[str] = None,
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
        project_row = ProjectTransformer.to_project_row(
            response,
            event_id=source_event_id,
        )
        if project_row.get("project_id") is not None:
            rows.projects.append(project_row)

        contact_rows = ProjectTransformer.to_contact_rows(
            response,
            project_id=str(project_id),
            event_id=source_event_id or "",
        )
        rows.contacts.extend(contact_rows)

        # Add to cache after successful fetch
        if self.project_cache:
            self.project_cache.add(project_id_str)

        return rows


class ProjectTransformer:
    """
    Transforms ClaimX API project response to entity rows.

    API response structure (from /export/project/{projectId}):
    {
        "data": {
            "project": {
                "projectId": 123,
                "projectNumber": "...",
                "mfn": "...",
                "customerInformation": {...},
                "address": {...}
            },
            "teamMembers": [...]
        }
    }
    """

    @staticmethod
    def to_project_row(
        data: Dict[str, Any],
        event_id: Optional[str],
    ) -> Dict[str, Any]:
        """
        Transform API response to project row.

        Args:
            data: Full API response
            event_id: Event ID for traceability

        Returns:
            Project row dict
        """
        # Navigate to project object
        inner = data.get("data", data)
        project = inner.get("project", {})
        customer = project.get("customerInformation", {})
        address = project.get("address", {})

        # Extract primary email
        emails = customer.get("emails", [])
        primary_email = None
        for email in emails:
            if email.get("primary"):
                primary_email = safe_str(email.get("emailAddress"))
                break
        if not primary_email and emails:
            primary_email = safe_str(emails[0].get("emailAddress"))

        # Extract primary phone
        phones = customer.get("phones", [])
        primary_phone = None
        primary_phone_country_code = None
        if phones:
            primary_phone = safe_str(phones[0].get("phoneNumber"))
            primary_phone_country_code = safe_int(phones[0].get("phoneCountryCode"))

        project_id = safe_str(project.get("projectId"))

        log_with_context(
            logger,
            logging.DEBUG,
            "Transformed project row",
            project_id=project_id,
            has_customer_info=bool(customer),
            has_address=bool(address),
            email_count=len(emails),
            phone_count=len(phones),
            team_member_count=len(inner.get("teamMembers", [])),
        )

        row = {
            "project_id": project_id,
            "project_number": safe_str(project.get("projectNumber")),
            "master_file_name": safe_str(project.get("mfn")),
            "secondary_number": safe_str(project.get("secondaryNumber")),
            "created_date": parse_timestamp(project.get("createdDate")),
            "status": safe_str(project.get("status")),
            "date_of_loss": parse_timestamp(project.get("dateOfLoss")),
            "type_of_loss": safe_str(project.get("typeOfLoss")),
            "cause_of_loss": safe_str(project.get("causeOfLoss")),
            "loss_description": safe_str(project.get("lossDescription")),
            "customer_first_name": safe_str(customer.get("firstName")),
            "customer_last_name": safe_str(customer.get("lastName")),
            "custom_business_name": safe_str(customer.get("customBusinessName")),
            "business_line_type": safe_str(customer.get("businessLineType")),
            "year_built": safe_int(project.get("yearBuilt")),
            "square_footage": safe_int(project.get("squareFootage")),
            "street1": safe_str(address.get("street1")),
            "street2": safe_str(address.get("street2")),
            "city": safe_str(address.get("city")),
            "state_province": safe_str(address.get("stateProvince")),
            "zip_postcode": safe_str(address.get("zipPostcode")),
            "county": safe_str(address.get("county")),
            "country": safe_str(address.get("country")),
            "primary_email": primary_email,
            "primary_phone": primary_phone,
            "primary_phone_country_code": primary_phone_country_code,
            "date_received": parse_timestamp(project.get("dateReceived")),
            "date_contacted": parse_timestamp(project.get("dateContacted")),
            "planned_inspection_date": parse_timestamp(project.get("plannedInspectionDate")),
            "date_inspected": parse_timestamp(project.get("dateInspected")),
            "appointment_date": parse_timestamp(project.get("appointmentDate")),
            "custom_attribute1": safe_str(project.get("customAttribute1")),
            "custom_attribute2": safe_str(project.get("customAttribute2")),
            "custom_attribute3": safe_str(project.get("customAttribute3")),
            "custom_external_unique_id": safe_str(project.get("customExternalUniqueId")),
            "company_name": safe_str(inner.get("companyName")),
        }
        return BaseTransformer.inject_metadata(row, event_id, include_last_enriched=False)

    @staticmethod
    def to_contact_rows(
        data: Dict[str, Any],
        project_id: str,
        event_id: str,
    ) -> List[Dict[str, Any]]:
        """
        Extract contacts from project API response.

        Extracts:
        - POLICYHOLDER from customerInformation
        - CLAIM_REP from teamMembers

        Args:
            data: Full API response
            project_id: Project ID
            event_id: Event ID for traceability

        Returns:
            List of contact row dicts
        """
        contacts = []
        today = today_date()

        inner = data.get("data", data)
        project = inner.get("project", {})
        customer = project.get("customerInformation", {})
        team_members = inner.get("teamMembers", [])

        emails = customer.get("emails", [])
        primary_email = None
        for email in emails:
            if email.get("primary"):
                primary_email = safe_str(email.get("emailAddress"))
                break
        if not primary_email and emails:
            primary_email = safe_str(emails[0].get("emailAddress"))

        if primary_email:
            phones = customer.get("phones", [])
            phone = None
            phone_country_code = None
            if phones:
                phone = safe_str(phones[0].get("phoneNumber"))
                phone_country_code = safe_int(phones[0].get("phoneCountryCode"))

            row = {
                "project_id": project_id,
                "contact_email": primary_email,
                "contact_type": "POLICYHOLDER",
                "first_name": safe_str(customer.get("firstName")),
                "last_name": safe_str(customer.get("lastName")),
                "phone_number": phone,
                "phone_country_code": phone_country_code,
                "is_primary_contact": True,
                "master_file_name": None,
                "updated_at": now_iso(),
                "created_date": today,
                "task_assignment_id": None,
                "video_collaboration_id": None,
            }
            contacts.append(BaseTransformer.inject_metadata(row, event_id))

        for member in team_members:
            username = safe_str(member.get("userName"))
            if username:
                row = {
                    "project_id": project_id,
                    "contact_email": username,
                    "contact_type": "CLAIM_REP",
                    "first_name": None,
                    "last_name": None,
                    "phone_number": None,
                    "phone_country_code": None,
                    "is_primary_contact": safe_bool(member.get("primaryContact", False)),
                    "master_file_name": safe_str(member.get("mfn")),
                    "updated_at": now_iso(),
                    "created_date": today,
                    "task_assignment_id": None,
                    "video_collaboration_id": None,
                }
                contacts.append(BaseTransformer.inject_metadata(row, event_id))

        # Log contact extraction summary
        policyholder_count = sum(1 for c in contacts if c["contact_type"] == "POLICYHOLDER")
        claim_rep_count = sum(1 for c in contacts if c["contact_type"] == "CLAIM_REP")

        log_with_context(
            logger,
            logging.DEBUG,
            "Extracted contacts",
            project_id=project_id,
            total_contacts=len(contacts),
            policyholder_count=policyholder_count,
            claim_rep_count=claim_rep_count,
        )

        return contacts
