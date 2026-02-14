"""Tests for ClaimX API response transformers."""

from pipeline.claimx.handlers.transformers import (
    link_to_contact,
    link_to_row,
    media_to_row,
    project_to_contacts,
    project_to_row,
    task_to_row,
    template_to_row,
    video_collab_to_row,
)

from .conftest import make_project_api_response

# ============================================================================
# project_to_row
# ============================================================================


class TestProjectToRow:
    def test_project_to_row_extracts_basic_fields(self):
        data = make_project_api_response()
        row = project_to_row(data, trace_id="evt_001")

        assert row["project_id"] == "123"
        assert row["project_number"] == "PN-001"
        assert row["master_file_name"] == "MFN-001"
        assert row["status"] == "Active"
        assert row["type_of_loss"] == "Water Damage"
        assert row["cause_of_loss"] == "Pipe Burst"
        assert row["company_name"] == "Test Insurance Co"

    def test_project_to_row_extracts_customer_info(self):
        data = make_project_api_response()
        row = project_to_row(data, trace_id="evt_001")

        assert row["customer_first_name"] == "John"
        assert row["customer_last_name"] == "Doe"
        assert row["primary_email"] == "john@example.com"
        assert row["primary_phone"] == "555-0100"
        assert row["primary_phone_country_code"] == 1

    def test_project_to_row_extracts_address(self):
        data = make_project_api_response()
        row = project_to_row(data, trace_id="evt_001")

        assert row["street1"] == "123 Main St"
        assert row["city"] == "Springfield"
        assert row["state_province"] == "IL"
        assert row["zip_postcode"] == "62701"
        assert row["country"] == "US"

    def test_project_to_row_selects_primary_email(self):
        data = {
            "data": {
                "project": {
                    "projectId": 1,
                    "customerInformation": {
                        "emails": [
                            {"emailAddress": "secondary@test.com", "primary": False},
                            {"emailAddress": "primary@test.com", "primary": True},
                        ],
                    },
                },
            },
        }
        row = project_to_row(data, trace_id="evt_001")
        assert row["primary_email"] == "primary@test.com"

    def test_project_to_row_falls_back_to_first_email_when_no_primary(self):
        data = {
            "data": {
                "project": {
                    "projectId": 1,
                    "customerInformation": {
                        "emails": [
                            {"emailAddress": "first@test.com", "primary": False},
                            {"emailAddress": "second@test.com", "primary": False},
                        ],
                    },
                },
            },
        }
        row = project_to_row(data, trace_id="evt_001")
        assert row["primary_email"] == "first@test.com"

    def test_project_to_row_handles_no_emails(self):
        data = {
            "data": {
                "project": {
                    "projectId": 1,
                    "customerInformation": {"emails": []},
                },
            },
        }
        row = project_to_row(data, trace_id="evt_001")
        assert row["primary_email"] is None

    def test_project_to_row_handles_no_phones(self):
        data = {
            "data": {
                "project": {
                    "projectId": 1,
                    "customerInformation": {"phones": []},
                },
            },
        }
        row = project_to_row(data, trace_id="evt_001")
        assert row["primary_phone"] is None
        assert row["primary_phone_country_code"] is None

    def test_project_to_row_handles_empty_data(self):
        row = project_to_row({}, trace_id="evt_001")
        assert row["project_id"] is None
        assert row["trace_id"] == "evt_001"

    def test_project_to_row_handles_flat_response_without_data_key(self):
        data = {
            "project": {
                "projectId": 99,
                "status": "Closed",
            },
        }
        row = project_to_row(data, trace_id="evt_001")
        assert row["project_id"] == "99"
        assert row["status"] == "Closed"

    def test_project_to_row_includes_metadata_without_last_enriched(self):
        data = make_project_api_response()
        row = project_to_row(data, trace_id="evt_001")
        assert row["trace_id"] == "evt_001"
        assert "created_at" in row
        assert "updated_at" in row
        assert "last_enriched_at" not in row

    def test_project_to_row_parses_timestamps(self):
        data = make_project_api_response()
        row = project_to_row(data, trace_id="evt_001")
        assert row["created_date"] == "2024-01-15T10:00:00+00:00"
        assert row["date_of_loss"] == "2024-01-10T00:00:00+00:00"


# ============================================================================
# project_to_contacts
# ============================================================================


class TestProjectToContacts:
    def test_project_to_contacts_extracts_policyholder(self):
        data = make_project_api_response()
        contacts = project_to_contacts(data, project_id="123", trace_id="evt_001")

        policyholder = [c for c in contacts if c["contact_type"] == "POLICYHOLDER"]
        assert len(policyholder) == 1
        assert policyholder[0]["contact_email"] == "john@example.com"
        assert policyholder[0]["first_name"] == "John"
        assert policyholder[0]["last_name"] == "Doe"
        assert policyholder[0]["is_primary_contact"] is True
        assert policyholder[0]["phone_number"] == "555-0100"

    def test_project_to_contacts_extracts_team_members(self):
        data = make_project_api_response()
        contacts = project_to_contacts(data, project_id="123", trace_id="evt_001")

        claim_reps = [c for c in contacts if c["contact_type"] == "CLAIM_REP"]
        assert len(claim_reps) == 1
        assert claim_reps[0]["contact_email"] == "adjuster@example.com"
        assert claim_reps[0]["is_primary_contact"] is True
        assert claim_reps[0]["master_file_name"] == "MFN-001"

    def test_project_to_contacts_skips_policyholder_without_email(self):
        data = {
            "data": {
                "project": {
                    "customerInformation": {"emails": []},
                },
                "teamMembers": [],
            },
        }
        contacts = project_to_contacts(data, project_id="123", trace_id="evt_001")
        assert len(contacts) == 0

    def test_project_to_contacts_skips_team_member_without_username(self):
        data = {
            "data": {
                "project": {
                    "customerInformation": {"emails": []},
                },
                "teamMembers": [
                    {"userName": None, "primaryContact": False},
                    {"primaryContact": False},
                ],
            },
        }
        contacts = project_to_contacts(data, project_id="123", trace_id="evt_001")
        assert len(contacts) == 0

    def test_project_to_contacts_handles_empty_response(self):
        contacts = project_to_contacts({}, project_id="123", trace_id="evt_001")
        assert contacts == []

    def test_project_to_contacts_includes_metadata(self):
        data = make_project_api_response()
        contacts = project_to_contacts(data, project_id="123", trace_id="evt_001")
        for contact in contacts:
            assert contact["trace_id"] == "evt_001"
            assert "created_at" in contact
            assert "last_enriched_at" in contact


# ============================================================================
# task_to_row
# ============================================================================


class TestTaskToRow:
    def test_task_to_row_extracts_fields(self):
        data = {
            "assignmentId": 100,
            "taskId": 200,
            "taskName": "Inspection Report",
            "formId": "form_001",
            "projectId": 123,
            "assigneeId": 10,
            "assignorId": 20,
            "assignor": "manager@example.com",
            "status": "COMPLETED",
            "stpEnabled": True,
            "url": "https://example.com/task/100",
        }
        row = task_to_row(data, trace_id="evt_001")

        assert row["assignment_id"] == 100
        assert row["task_id"] == 200
        assert row["task_name"] == "Inspection Report"
        assert row["project_id"] == "123"
        assert row["assignee_id"] == 10
        assert row["status"] == "COMPLETED"
        assert row["stp_enabled"] is True
        assert row["task_url"] == "https://example.com/task/100"

    def test_task_to_row_handles_empty_data(self):
        row = task_to_row({}, trace_id="evt_001")
        assert row["assignment_id"] is None
        assert row["task_id"] is None
        assert row["trace_id"] == "evt_001"

    def test_task_to_row_includes_metadata(self):
        row = task_to_row({"assignmentId": 1}, trace_id="evt_001")
        assert row["trace_id"] == "evt_001"
        assert "created_at" in row
        assert "last_enriched_at" in row


# ============================================================================
# template_to_row
# ============================================================================


class TestTemplateToRow:
    def test_template_to_row_extracts_fields(self):
        data = {
            "taskId": 50,
            "compId": 10,
            "name": "Photo Review",
            "description": "Review uploaded photos",
            "formId": "form_001",
            "formName": "Photo Review Form",
            "enabled": True,
            "default": False,
            "isManualDelivery": True,
            "allowReSubmit": True,
            "autoGeneratePdf": False,
            "modifiedBy": "admin@example.com",
            "modifiedById": 1,
        }
        row = template_to_row(data, trace_id="evt_001")

        assert row["task_id"] == 50
        assert row["comp_id"] == 10
        assert row["name"] == "Photo Review"
        assert row["enabled"] is True
        assert row["is_default"] is False
        assert row["is_manual_delivery"] is True
        assert row["allow_resubmit"] is True

    def test_template_to_row_handles_empty_data(self):
        row = template_to_row({}, trace_id="evt_001")
        assert row["task_id"] is None
        assert row["name"] is None


# ============================================================================
# link_to_row
# ============================================================================


class TestLinkToRow:
    def test_link_to_row_extracts_fields(self):
        data = {
            "linkId": 300,
            "linkCode": "ABC123",
            "url": "https://example.com/link/300",
            "notificationAccessMethod": "EMAIL",
            "countryId": 1,
            "stateId": 14,
        }
        row = link_to_row(data, assignment_id=100, project_id=123, trace_id="evt_001")

        assert row["link_id"] == 300
        assert row["assignment_id"] == 100
        assert row["project_id"] == "123"
        assert row["link_code"] == "ABC123"
        assert row["url"] == "https://example.com/link/300"

    def test_link_to_row_does_not_include_last_enriched(self):
        row = link_to_row({}, assignment_id=1, project_id=1, trace_id="evt_001")
        assert "last_enriched_at" not in row
        assert "created_at" in row

    def test_link_to_row_handles_empty_data(self):
        row = link_to_row({}, assignment_id=1, project_id=1, trace_id="evt_001")
        assert row["link_id"] is None
        assert row["link_code"] is None


# ============================================================================
# link_to_contact
# ============================================================================


class TestLinkToContact:
    def test_link_to_contact_extracts_fields(self):
        data = {
            "email": "policyholder@example.com",
            "firstName": "Jane",
            "lastName": "Doe",
            "phone": "555-0200",
            "phoneCountryCode": 1,
        }
        row = link_to_contact(data, project_id=123, assignment_id=100, trace_id="evt_001")

        assert row is not None
        assert row["contact_email"] == "policyholder@example.com"
        assert row["first_name"] == "Jane"
        assert row["last_name"] == "Doe"
        assert row["phone_number"] == "555-0200"
        assert row["contact_type"] == "POLICYHOLDER"
        assert row["is_primary_contact"] is False
        assert row["task_assignment_id"] == 100
        assert row["project_id"] == "123"

    def test_link_to_contact_returns_none_when_no_email(self):
        data = {"firstName": "Jane", "lastName": "Doe"}
        result = link_to_contact(data, project_id=123, assignment_id=100, trace_id="evt_001")
        assert result is None

    def test_link_to_contact_returns_none_for_empty_email(self):
        data = {"email": "  "}
        result = link_to_contact(data, project_id=123, assignment_id=100, trace_id="evt_001")
        assert result is None

    def test_link_to_contact_includes_metadata(self):
        data = {"email": "test@example.com"}
        row = link_to_contact(data, project_id=123, assignment_id=100, trace_id="evt_001")
        assert row["trace_id"] == "evt_001"
        assert "last_enriched_at" in row


# ============================================================================
# media_to_row
# ============================================================================


class TestMediaToRow:
    def test_media_to_row_extracts_fields(self):
        data = {
            "mediaID": 500,
            "mediaType": "IMAGE",
            "mediaName": "damage_photo.jpg",
            "mediaDescription": "Kitchen water damage",
            "mediaComment": "Taken during inspection",
            "latitude": "41.8781",
            "longitude": "-87.6298",
            "gpsSource": "DEVICE",
            "takenDate": "2024-01-15",
            "fullDownloadLink": "https://cdn.example.com/photo.jpg",
            "expiresAt": "2024-02-15T00:00:00Z",
            "taskAssignmentId": "100",
        }
        row = media_to_row(data, project_id=123, trace_id="evt_001")

        assert row["media_id"] == "500"
        assert row["project_id"] == "123"
        assert row["file_type"] == "IMAGE"
        assert row["file_name"] == "damage_photo.jpg"
        assert row["full_download_link"] == "https://cdn.example.com/photo.jpg"
        assert row["latitude"] == "41.8781"
        assert row["task_assignment_id"] == "100"

    def test_media_to_row_handles_empty_data(self):
        row = media_to_row({}, project_id=123, trace_id="evt_001")
        assert row["media_id"] is None
        assert row["file_type"] is None

    def test_media_to_row_includes_metadata(self):
        row = media_to_row({"mediaID": 1}, project_id=1, trace_id="evt_001")
        assert row["trace_id"] == "evt_001"
        assert "last_enriched_at" in row


# ============================================================================
# video_collab_to_row
# ============================================================================


class TestVideoCollabToRow:
    def test_video_collab_to_row_extracts_fields(self):
        data = {
            "videoCollaborationId": 700,
            "claimId": 123,
            "mfn": "MFN-001",
            "claimNumber": "CLM-001",
            "policyNumber": "POL-001",
            "emailUserName": "user@example.com",
            "claimRepFirstName": "John",
            "claimRepLastName": "Doe",
            "claimRepFullName": "John Doe",
            "numberOfVideos": 3,
            "numberOfPhotos": 10,
            "numberOfViewers": 2,
            "sessionCount": 1,
            "totalTimeSeconds": "120.5",
            "totalTime": "2m 0s",
            "companyId": 1,
            "companyName": "Test Co",
            "guid": "abc-123",
        }
        row = video_collab_to_row(data, trace_id="evt_001")

        assert row["video_collaboration_id"] == 700
        assert row["claim_id"] == 123
        assert row["claim_rep_full_name"] == "John Doe"
        assert row["number_of_videos"] == 3
        assert row["total_time_seconds"] == "120.5"

    def test_video_collab_to_row_builds_full_name_from_parts(self):
        data = {
            "claimRepFirstName": "Jane",
            "claimRepLastName": "Smith",
        }
        row = video_collab_to_row(data, trace_id="evt_001")
        assert row["claim_rep_full_name"] == "Jane Smith"

    def test_video_collab_to_row_builds_full_name_from_first_only(self):
        data = {
            "claimRepFirstName": "Jane",
        }
        row = video_collab_to_row(data, trace_id="evt_001")
        assert row["claim_rep_full_name"] == "Jane"

    def test_video_collab_to_row_builds_full_name_from_last_only(self):
        data = {
            "claimRepLastName": "Smith",
        }
        row = video_collab_to_row(data, trace_id="evt_001")
        assert row["claim_rep_full_name"] == "Smith"

    def test_video_collab_to_row_preserves_explicit_full_name(self):
        data = {
            "claimRepFirstName": "Jane",
            "claimRepLastName": "Smith",
            "claimRepFullName": "Dr. Jane Smith",
        }
        row = video_collab_to_row(data, trace_id="evt_001")
        assert row["claim_rep_full_name"] == "Dr. Jane Smith"

    def test_video_collab_to_row_uses_id_fallback(self):
        data = {"id": 999}
        row = video_collab_to_row(data, trace_id="evt_001")
        assert row["video_collaboration_id"] == 999

    def test_video_collab_to_row_handles_empty_data(self):
        row = video_collab_to_row({}, trace_id="evt_001")
        assert row["video_collaboration_id"] is None
        assert row["claim_rep_full_name"] is None
