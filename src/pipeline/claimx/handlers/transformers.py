"""
ClaimX API response transformers.

Converts API responses to entity row dicts for Delta Lake writes.
"""

import logging
from typing import Any

from core.logging import get_logger, log_with_context
from pipeline.claimx.handlers.utils import (
    BaseTransformer,
    now_iso,
    parse_timestamp,
    safe_bool,
    safe_decimal_str,
    safe_int,
    safe_str,
    safe_str_id,
    today_date,
)

logger = get_logger(__name__)


def project_to_row(
    data: dict[str, Any],
    event_id: str | None,
) -> dict[str, Any]:
    """Transform API response to project row."""
    inner = data.get("data", data)
    project = inner.get("project", {})
    customer = project.get("customerInformation", {})
    address = project.get("address", {})

    emails = customer.get("emails", [])
    primary_email = None
    for email in emails:
        if email.get("primary"):
            primary_email = safe_str(email.get("emailAddress"))
            break
    if not primary_email and emails:
        primary_email = safe_str(emails[0].get("emailAddress"))

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
        "planned_inspection_date": parse_timestamp(
            project.get("plannedInspectionDate")
        ),
        "date_inspected": parse_timestamp(project.get("dateInspected")),
        "appointment_date": parse_timestamp(project.get("appointmentDate")),
        "custom_attribute1": safe_str(project.get("customAttribute1")),
        "custom_attribute2": safe_str(project.get("customAttribute2")),
        "custom_attribute3": safe_str(project.get("customAttribute3")),
        "custom_external_unique_id": safe_str(project.get("customExternalUniqueId")),
        "company_name": safe_str(inner.get("companyName")),
    }
    return BaseTransformer.inject_metadata(row, event_id, include_last_enriched=False)


def project_to_contacts(
    data: dict[str, Any],
    project_id: str,
    event_id: str,
) -> list[dict[str, Any]]:
    """Extract contacts from project API response."""
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

    policyholder_count = sum(
        1 for c in contacts if c["contact_type"] == "POLICYHOLDER"
    )
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


def task_to_row(
    data: dict[str, Any],
    event_id: str,
) -> dict[str, Any]:
    """Transform task assignment to row."""
    row = {
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
        "resubmit_task_assignment_id": safe_int(data.get("resubmitTaskAssignmentId")),
        "task_url": safe_str(data.get("url")),
    }
    return BaseTransformer.inject_metadata(row, event_id)


def template_to_row(
    template: dict[str, Any],
    event_id: str,
) -> dict[str, Any]:
    """Transform custom task template to row."""
    row = {
        "task_id": safe_int(template.get("taskId")),
        "comp_id": safe_int(template.get("compId")),
        "name": safe_str(template.get("name")),
        "description": safe_str(template.get("description")),
        "form_id": safe_str(template.get("formId")),
        "form_name": safe_str(template.get("formName")),
        "enabled": safe_bool(template.get("enabled")),
        "is_default": safe_bool(template.get("default")),
        "is_manual_delivery": safe_bool(template.get("isManualDelivery")),
        "is_external_link_delivery": safe_bool(template.get("isExternalLinkDelivery")),
        "provide_portal_access": safe_bool(template.get("providePortalAccess")),
        "notify_assigned_send_recipient": safe_bool(
            template.get("notifyAssignedSendRecipient")
        ),
        "notify_assigned_send_recipient_sms": safe_bool(
            template.get("notifyAssignedSendRecipientSms")
        ),
        "notify_assigned_subject": safe_str(template.get("notifyAssignedSubject")),
        "notify_task_completed": safe_bool(template.get("notifyTaskCompleted")),
        "notify_completed_subject": safe_str(template.get("notifyCompletedSubject")),
        "allow_resubmit": safe_bool(template.get("allowReSubmit")),
        "auto_generate_pdf": safe_bool(template.get("autoGeneratePdf")),
        "modified_by": safe_str(template.get("modifiedBy")),
        "modified_by_id": safe_int(template.get("modifiedById")),
        "modified_date": parse_timestamp(template.get("modifiedDate")),
    }
    return BaseTransformer.inject_metadata(row, event_id)


def link_to_row(
    link: dict[str, Any],
    assignment_id: int,
    project_id: Any,
    event_id: str,
) -> dict[str, Any]:
    """Transform external link data to row."""
    row = {
        "link_id": safe_int(link.get("linkId")),
        "assignment_id": assignment_id,
        "project_id": safe_str_id(project_id),
        "link_code": safe_str(link.get("linkCode")),
        "url": safe_str(link.get("url")),
        "notification_access_method": safe_str(link.get("notificationAccessMethod")),
        "country_id": safe_int(link.get("countryId")),
        "state_id": safe_int(link.get("stateId")),
        "created_date": None,
        "accessed_count": 0,
        "last_accessed": None,
    }
    return BaseTransformer.inject_metadata(row, event_id, include_last_enriched=False)


def link_to_contact(
    link: dict[str, Any],
    project_id: Any,
    assignment_id: int,
    event_id: str,
) -> dict[str, Any] | None:
    """Extract contact from external link data."""
    email = safe_str(link.get("email"))
    if not email:
        return None

    today = today_date()
    row = {
        "project_id": safe_str_id(project_id),
        "contact_email": email,
        "contact_type": "POLICYHOLDER",
        "first_name": safe_str(link.get("firstName")),
        "last_name": safe_str(link.get("lastName")),
        "phone_number": safe_str(link.get("phone")),
        "phone_country_code": safe_int(link.get("phoneCountryCode")),
        "is_primary_contact": False,
        "master_file_name": None,
        "task_assignment_id": safe_int(assignment_id),
        "video_collaboration_id": None,
        "updated_at": now_iso(),
        "created_date": today,
    }
    return BaseTransformer.inject_metadata(row, event_id)


def media_to_row(
    media: dict[str, Any],
    project_id: Any,
    event_id: str,
) -> dict[str, Any]:
    """Transform media item to row."""
    download_link = safe_str(media.get("fullDownloadLink"))

    row = {
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
    }
    return BaseTransformer.inject_metadata(row, event_id)


def video_collab_to_row(
    data: dict[str, Any],
    event_id: str,
) -> dict[str, Any]:
    """Transform API response to video collaboration row."""
    first_name = safe_str(data.get("claimRepFirstName"))
    last_name = safe_str(data.get("claimRepLastName"))
    full_name = safe_str(data.get("claimRepFullName"))

    if not full_name and (first_name or last_name):
        parts = [p for p in [first_name, last_name] if p]
        full_name = " ".join(parts) if parts else None

    row = {
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
        "live_call_first_session": parse_timestamp(data.get("liveCallFirstSession")),
        "live_call_last_session": parse_timestamp(data.get("liveCallLastSession")),
        "company_id": safe_int(data.get("companyId")),
        "company_name": safe_str(data.get("companyName")),
        "guid": safe_str(data.get("guid")),
    }
    return BaseTransformer.inject_metadata(row, event_id)
