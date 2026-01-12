"""
Tests for ClaimXEventMessage schema.

Validates Pydantic model behavior, JSON serialization, and field validation.
"""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage


class TestClaimXEventMessageCreation:
    """Test ClaimXEventMessage instantiation with valid data."""

    def test_create_with_all_fields(self):
        """ClaimXEventMessage can be created with all fields populated."""
        now = datetime.now(timezone.utc)
        event = ClaimXEventMessage(
            event_id="evt_12345",
            event_type="PROJECT_FILE_ADDED",
            project_id="proj_67890",
            ingested_at=now,
            media_id="media_111",
            task_assignment_id="task_222",
            video_collaboration_id="video_333",
            master_file_name="claim_2024.pdf",
            raw_data={"fileName": "photo.jpg", "fileSize": 1024}
        )

        assert event.event_id == "evt_12345"
        assert event.event_type == "PROJECT_FILE_ADDED"
        assert event.project_id == "proj_67890"
        assert event.ingested_at == now
        assert event.media_id == "media_111"
        assert event.task_assignment_id == "task_222"
        assert event.video_collaboration_id == "video_333"
        assert event.master_file_name == "claim_2024.pdf"
        assert event.raw_data == {"fileName": "photo.jpg", "fileSize": 1024}

    def test_create_with_minimal_fields(self):
        """ClaimXEventMessage can be created with only required fields."""
        now = datetime.now(timezone.utc)
        event = ClaimXEventMessage(
            event_id="evt_minimal",
            event_type="PROJECT_CREATED",
            project_id="proj_minimal",
            ingested_at=now
        )

        assert event.event_id == "evt_minimal"
        assert event.event_type == "PROJECT_CREATED"
        assert event.project_id == "proj_minimal"
        assert event.ingested_at == now
        assert event.media_id is None
        assert event.task_assignment_id is None
        assert event.video_collaboration_id is None
        assert event.master_file_name is None
        assert event.raw_data is None

    def test_create_project_created_event(self):
        """PROJECT_CREATED event with typical fields."""
        now = datetime.now(timezone.utc)
        event = ClaimXEventMessage(
            event_id="evt_proj_001",
            event_type="PROJECT_CREATED",
            project_id="proj_001",
            ingested_at=now,
            raw_data={"projectName": "Insurance Claim 2024", "claimNumber": "CLM-001"}
        )

        assert event.event_type == "PROJECT_CREATED"
        assert event.raw_data["projectName"] == "Insurance Claim 2024"

    def test_create_project_file_added_event(self):
        """PROJECT_FILE_ADDED event with media_id."""
        now = datetime.now(timezone.utc)
        event = ClaimXEventMessage(
            event_id="evt_file_001",
            event_type="PROJECT_FILE_ADDED",
            project_id="proj_001",
            ingested_at=now,
            media_id="media_12345",
            raw_data={"fileName": "damage_photo.jpg", "uploadedBy": "user@example.com"}
        )

        assert event.event_type == "PROJECT_FILE_ADDED"
        assert event.media_id == "media_12345"

    def test_create_custom_task_assigned_event(self):
        """CUSTOM_TASK_ASSIGNED event with task_assignment_id."""
        now = datetime.now(timezone.utc)
        event = ClaimXEventMessage(
            event_id="evt_task_001",
            event_type="CUSTOM_TASK_ASSIGNED",
            project_id="proj_001",
            ingested_at=now,
            task_assignment_id="task_67890",
            raw_data={"taskName": "Review photos", "assignee": "adjuster@insurance.com"}
        )

        assert event.event_type == "CUSTOM_TASK_ASSIGNED"
        assert event.task_assignment_id == "task_67890"

    def test_create_video_collaboration_event(self):
        """VIDEO_COLLABORATION_INVITE_SENT event with video_collaboration_id."""
        now = datetime.now(timezone.utc)
        event = ClaimXEventMessage(
            event_id="evt_video_001",
            event_type="VIDEO_COLLABORATION_INVITE_SENT",
            project_id="proj_001",
            ingested_at=now,
            video_collaboration_id="video_11111",
            raw_data={"invitee": "claimant@example.com"}
        )

        assert event.event_type == "VIDEO_COLLABORATION_INVITE_SENT"
        assert event.video_collaboration_id == "video_11111"

    def test_create_project_mfn_added_event(self):
        """PROJECT_MFN_ADDED event with master_file_name."""
        now = datetime.now(timezone.utc)
        event = ClaimXEventMessage(
            event_id="evt_mfn_001",
            event_type="PROJECT_MFN_ADDED",
            project_id="proj_001",
            ingested_at=now,
            master_file_name="CLM-2024-12345",
            raw_data={"addedBy": "system"}
        )

        assert event.event_type == "PROJECT_MFN_ADDED"
        assert event.master_file_name == "CLM-2024-12345"


class TestClaimXEventMessageValidation:
    """Test field validation rules."""

    def test_missing_required_field_raises_error(self):
        """Missing required fields raise ValidationError."""
        now = datetime.now(timezone.utc)

        with pytest.raises(ValidationError) as exc_info:
            ClaimXEventMessage(
                event_type="PROJECT_CREATED",
                project_id="proj_001",
                ingested_at=now
                # Missing event_id
            )

        errors = exc_info.value.errors()
        assert any(err['loc'] == ('event_id',) for err in errors)

    def test_empty_event_id_raises_error(self):
        """Empty event_id raises validation error."""
        now = datetime.now(timezone.utc)

        with pytest.raises(ValidationError) as exc_info:
            ClaimXEventMessage(
                event_id="",
                event_type="PROJECT_CREATED",
                project_id="proj_001",
                ingested_at=now
            )

        errors = exc_info.value.errors()
        assert any('event_id' in str(err) for err in errors)

    def test_whitespace_event_id_raises_error(self):
        """Whitespace-only event_id raises validation error."""
        now = datetime.now(timezone.utc)

        with pytest.raises(ValidationError) as exc_info:
            ClaimXEventMessage(
                event_id="   ",
                event_type="PROJECT_CREATED",
                project_id="proj_001",
                ingested_at=now
            )

        errors = exc_info.value.errors()
        assert any('event_id' in str(err) for err in errors)

    def test_empty_event_type_raises_error(self):
        """Empty event_type raises validation error."""
        now = datetime.now(timezone.utc)

        with pytest.raises(ValidationError) as exc_info:
            ClaimXEventMessage(
                event_id="evt_001",
                event_type="",
                project_id="proj_001",
                ingested_at=now
            )

        errors = exc_info.value.errors()
        assert any('event_type' in str(err) for err in errors)

    def test_empty_project_id_raises_error(self):
        """Empty project_id raises validation error."""
        now = datetime.now(timezone.utc)

        with pytest.raises(ValidationError) as exc_info:
            ClaimXEventMessage(
                event_id="evt_001",
                event_type="PROJECT_CREATED",
                project_id="",
                ingested_at=now
            )

        errors = exc_info.value.errors()
        assert any('project_id' in str(err) for err in errors)

    def test_string_fields_trimmed(self):
        """String fields with leading/trailing whitespace are trimmed."""
        now = datetime.now(timezone.utc)
        event = ClaimXEventMessage(
            event_id="  evt_001  ",
            event_type="  PROJECT_CREATED  ",
            project_id="  proj_001  ",
            ingested_at=now
        )

        assert event.event_id == "evt_001"
        assert event.event_type == "PROJECT_CREATED"
        assert event.project_id == "proj_001"


class TestClaimXEventMessageFromEventhouseRow:
    """Test creating ClaimXEventMessage from Eventhouse row data."""

    def test_from_eventhouse_row_snake_case(self):
        """Can create from Eventhouse row with snake_case fields."""
        now = datetime.now(timezone.utc)
        row = {
            "event_id": "evt_12345",
            "event_type": "PROJECT_FILE_ADDED",
            "project_id": "proj_67890",
            "ingested_at": now,
            "media_id": "media_111",
            "task_assignment_id": None,
            "video_collaboration_id": None,
            "master_file_name": None,
            "fileName": "photo.jpg"
        }

        event = ClaimXEventMessage.from_eventhouse_row(row)

        assert event.event_id == "evt_12345"
        assert event.event_type == "PROJECT_FILE_ADDED"
        assert event.project_id == "proj_67890"
        assert event.ingested_at == now
        assert event.media_id == "media_111"
        assert event.raw_data == row

    def test_from_eventhouse_row_camel_case(self):
        """Can create from Eventhouse row with camelCase fields."""
        now = datetime.now(timezone.utc)
        row = {
            "eventId": "evt_12345",
            "eventType": "PROJECT_CREATED",
            "projectId": "proj_67890",
            "IngestionTime": now,
            "mediaId": "media_111",
            "projectName": "Claim 2024"
        }

        event = ClaimXEventMessage.from_eventhouse_row(row)

        assert event.event_id == "evt_12345"
        assert event.event_type == "PROJECT_CREATED"
        assert event.project_id == "proj_67890"
        assert event.ingested_at == now
        assert event.media_id == "media_111"
        assert event.raw_data == row

    def test_from_eventhouse_row_mixed_case(self):
        """Can create from Eventhouse row with mixed case fields."""
        now = datetime.now(timezone.utc)
        row = {
            "event_id": "evt_001",
            "eventType": "CUSTOM_TASK_ASSIGNED",
            "project_id": "proj_001",
            "IngestionTime": now,
            "taskAssignmentId": "task_222"
        }

        event = ClaimXEventMessage.from_eventhouse_row(row)

        assert event.event_id == "evt_001"
        assert event.event_type == "CUSTOM_TASK_ASSIGNED"
        assert event.project_id == "proj_001"
        assert event.task_assignment_id == "task_222"

    def test_from_eventhouse_row_with_all_optional_fields(self):
        """Can create from Eventhouse row with all optional fields."""
        now = datetime.now(timezone.utc)
        row = {
            "event_id": "evt_full",
            "event_type": "PROJECT_FILE_ADDED",
            "project_id": "proj_full",
            "ingested_at": now,
            "media_id": "media_111",
            "task_assignment_id": "task_222",
            "video_collaboration_id": "video_333",
            "master_file_name": "claim.pdf",
            "extra_field": "extra_value"
        }

        event = ClaimXEventMessage.from_eventhouse_row(row)

        assert event.media_id == "media_111"
        assert event.task_assignment_id == "task_222"
        assert event.video_collaboration_id == "video_333"
        assert event.master_file_name == "claim.pdf"
        assert event.raw_data["extra_field"] == "extra_value"


class TestClaimXEventMessageSerialization:
    """Test JSON serialization and deserialization."""

    def test_model_dump_includes_all_fields(self):
        """model_dump includes all fields."""
        now = datetime.now(timezone.utc)
        event = ClaimXEventMessage(
            event_id="evt_001",
            event_type="PROJECT_CREATED",
            project_id="proj_001",
            ingested_at=now,
            raw_data={"key": "value"}
        )

        dumped = event.model_dump()

        assert dumped["event_id"] == "evt_001"
        assert dumped["event_type"] == "PROJECT_CREATED"
        assert dumped["project_id"] == "proj_001"
        assert dumped["ingested_at"] == now
        assert dumped["raw_data"] == {"key": "value"}

    def test_model_dump_json_serializable(self):
        """model_dump_json produces valid JSON."""
        now = datetime.now(timezone.utc)
        event = ClaimXEventMessage(
            event_id="evt_001",
            event_type="PROJECT_CREATED",
            project_id="proj_001",
            ingested_at=now,
            media_id="media_111"
        )

        json_str = event.model_dump_json()

        assert isinstance(json_str, str)
        assert "evt_001" in json_str
        assert "PROJECT_CREATED" in json_str
        assert "proj_001" in json_str

    def test_model_validate_from_dict(self):
        """Can validate and create from dict."""
        now = datetime.now(timezone.utc)
        data = {
            "event_id": "evt_001",
            "event_type": "PROJECT_CREATED",
            "project_id": "proj_001",
            "ingested_at": now,
            "media_id": "media_111"
        }

        event = ClaimXEventMessage.model_validate(data)

        assert event.event_id == "evt_001"
        assert event.media_id == "media_111"

    def test_model_validate_json_from_json_string(self):
        """Can validate and create from JSON string."""
        now = datetime.now(timezone.utc)
        json_str = f'{{"event_id":"evt_001","event_type":"PROJECT_CREATED","project_id":"proj_001","ingested_at":"{now.isoformat()}"}}'

        event = ClaimXEventMessage.model_validate_json(json_str)

        assert event.event_id == "evt_001"
        assert event.event_type == "PROJECT_CREATED"


class TestClaimXEventMessageEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_raw_data_can_be_deeply_nested(self):
        """raw_data can contain deeply nested structures."""
        now = datetime.now(timezone.utc)
        complex_data = {
            "level1": {
                "level2": {
                    "level3": {
                        "value": "deeply_nested"
                    }
                }
            }
        }

        event = ClaimXEventMessage(
            event_id="evt_nested",
            event_type="PROJECT_CREATED",
            project_id="proj_nested",
            ingested_at=now,
            raw_data=complex_data
        )

        assert event.raw_data["level1"]["level2"]["level3"]["value"] == "deeply_nested"

    def test_raw_data_can_contain_lists(self):
        """raw_data can contain lists."""
        now = datetime.now(timezone.utc)
        event = ClaimXEventMessage(
            event_id="evt_list",
            event_type="PROJECT_CREATED",
            project_id="proj_list",
            ingested_at=now,
            raw_data={
                "attachments": ["file1.pdf", "file2.jpg"],
                "tags": ["urgent", "water_damage"]
            }
        )

        assert len(event.raw_data["attachments"]) == 2
        assert "urgent" in event.raw_data["tags"]

    def test_all_event_types_supported(self):
        """All documented event types can be created."""
        now = datetime.now(timezone.utc)
        event_types = [
            "PROJECT_CREATED",
            "PROJECT_FILE_ADDED",
            "PROJECT_MFN_ADDED",
            "CUSTOM_TASK_ASSIGNED",
            "CUSTOM_TASK_COMPLETED",
            "POLICYHOLDER_INVITED",
            "POLICYHOLDER_JOINED",
            "VIDEO_COLLABORATION_INVITE_SENT",
            "VIDEO_COLLABORATION_COMPLETED"
        ]

        for event_type in event_types:
            event = ClaimXEventMessage(
                event_id=f"evt_{event_type.lower()}",
                event_type=event_type,
                project_id="proj_001",
                ingested_at=now
            )
            assert event.event_type == event_type
