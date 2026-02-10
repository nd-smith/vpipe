"""Tests for iTel Cabinet API models."""

from datetime import datetime

from pipeline.plugins.itel_cabinet_api.models import (
    CabinetAttachment,
    CabinetSubmission,
    ProcessedTask,
    TaskEvent,
)

# =====================
# TaskEvent tests
# =====================


class TestTaskEvent:
    def test_from_kafka_message_minimal(self):
        raw = {
            "event_id": "evt-1",
            "event_type": "CUSTOM_TASK_COMPLETED",
            "timestamp": "2024-06-15T10:00:00Z",
            "task_id": 456,
            "assignment_id": 1001,
            "project_id": "5395115",
            "task_name": "Cabinet Repair",
            "task_status": "COMPLETED",
        }
        event = TaskEvent.from_kafka_message(raw)

        assert event.event_id == "evt-1"
        assert event.event_type == "CUSTOM_TASK_COMPLETED"
        assert event.event_timestamp == "2024-06-15T10:00:00Z"
        assert event.task_id == 456
        assert event.assignment_id == 1001
        assert event.project_id == "5395115"
        assert event.task_name == "Cabinet Repair"
        assert event.task_status == "COMPLETED"
        assert event.assigned_to_user_id is None
        assert event.assigned_by_user_id is None
        assert event.task_created_at is None
        assert event.task_completed_at is None

    def test_from_kafka_message_with_task_details(self):
        raw = {
            "event_id": "evt-2",
            "event_type": "CUSTOM_TASK_ASSIGNED",
            "timestamp": "2024-06-15T10:00:00Z",
            "task_id": 789,
            "assignment_id": 1002,
            "project_id": "123456",
            "task_name": "Assessment",
            "task_status": "ASSIGNED",
            "task": {
                "assigned_to_user_id": 100,
                "assigned_by_user_id": 200,
                "created_at": "2024-06-14T08:00:00Z",
                "completed_at": None,
            },
        }
        event = TaskEvent.from_kafka_message(raw)

        assert event.assigned_to_user_id == 100
        assert event.assigned_by_user_id == 200
        assert event.task_created_at == "2024-06-14T08:00:00Z"
        assert event.task_completed_at is None

    def test_from_kafka_message_missing_required_field(self):
        raw = {
            "event_id": "evt-3",
            # "event_type" is missing
            "timestamp": "2024-06-15T10:00:00Z",
            "task_id": 456,
            "assignment_id": 1001,
            "project_id": "5395115",
            "task_name": "Task",
            "task_status": "COMPLETED",
        }
        try:
            TaskEvent.from_kafka_message(raw)
            raise AssertionError("Should have raised ValueError")
        except ValueError as e:
            assert "event_type" in str(e)

    def test_from_kafka_message_no_task_key(self):
        raw = {
            "event_id": "evt-4",
            "event_type": "CUSTOM_TASK_COMPLETED",
            "timestamp": "2024-06-15T10:00:00Z",
            "task_id": 456,
            "assignment_id": 1001,
            "project_id": "5395115",
            "task_name": "Task",
            "task_status": "COMPLETED",
        }
        event = TaskEvent.from_kafka_message(raw)
        assert event.assigned_to_user_id is None


# =====================
# CabinetSubmission tests
# =====================


class TestCabinetSubmission:
    def test_to_dict_basic(self):
        sub = CabinetSubmission(
            assignment_id=1001,
            project_id="5395115",
            form_id="abc123",
            form_response_id="resp-1",
            status="COMPLETED",
            event_id="evt-1",
        )
        d = sub.to_dict()

        assert d["assignment_id"] == 1001
        assert d["project_id"] == "5395115"
        assert d["form_id"] == "abc123"
        assert d["status"] == "COMPLETED"

    def test_to_dict_datetime_serialization(self):
        dt = datetime(2024, 6, 15, 10, 30, 0)
        sub = CabinetSubmission(
            assignment_id=1001,
            project_id="5395115",
            form_id="abc123",
            form_response_id="resp-1",
            status="COMPLETED",
            event_id="evt-1",
            created_at=dt,
        )
        d = sub.to_dict()

        assert d["created_at"] == "2024-06-15T10:30:00"

    def test_to_dict_none_values(self):
        sub = CabinetSubmission(
            assignment_id=1001,
            project_id="5395115",
            form_id="abc123",
            form_response_id="resp-1",
            status="COMPLETED",
            event_id="evt-1",
        )
        d = sub.to_dict()

        assert d["task_id"] is None
        assert d["customer_first_name"] is None
        assert d["lower_cabinets_damaged"] is None

    def test_all_cabinet_fields_present(self):
        sub = CabinetSubmission(
            assignment_id=1,
            project_id="1",
            form_id="f",
            form_response_id="r",
            status="s",
            event_id="e",
            lower_cabinets_damaged=True,
            upper_cabinets_damaged=False,
            full_height_cabinets_damaged=True,
            island_cabinets_damaged=False,
            countertops_lf=15.5,
        )
        d = sub.to_dict()

        assert d["lower_cabinets_damaged"] is True
        assert d["upper_cabinets_damaged"] is False
        assert d["full_height_cabinets_damaged"] is True
        assert d["island_cabinets_damaged"] is False
        assert d["countertops_lf"] == 15.5


# =====================
# CabinetAttachment tests
# =====================


class TestCabinetAttachment:
    def test_defaults(self):
        att = CabinetAttachment(
            assignment_id=1001,
            project_id=5395115,
            event_id="evt-1",
            control_id="ctrl-1",
            question_key="overview_photos",
            question_text="Upload Overview Photo(s)",
            topic_category="General",
            media_id=999,
        )
        assert att.url is None
        assert att.display_order == 0
        assert att.is_active is True
        assert att.media_type == "image/jpeg"

    def test_to_dict(self):
        dt = datetime(2024, 6, 15, 10, 0, 0)
        att = CabinetAttachment(
            assignment_id=1001,
            project_id=5395115,
            event_id="evt-1",
            control_id="ctrl-1",
            question_key="overview_photos",
            question_text="Upload Photo(s)",
            topic_category="General",
            media_id=999,
            url="https://cdn.example.com/img.jpg",
            display_order=1,
            created_at=dt,
        )
        d = att.to_dict()

        assert d["media_id"] == 999
        assert d["url"] == "https://cdn.example.com/img.jpg"
        assert d["created_at"] == "2024-06-15T10:00:00"

    def test_to_dict_none_url(self):
        att = CabinetAttachment(
            assignment_id=1,
            project_id=1,
            event_id="e",
            control_id="c",
            question_key="q",
            question_text="q",
            topic_category="t",
            media_id=1,
        )
        d = att.to_dict()
        assert d["url"] is None


# =====================
# ProcessedTask tests
# =====================


class TestProcessedTask:
    def test_was_enriched_true(self):
        event = TaskEvent(
            event_id="e",
            event_type="CUSTOM_TASK_COMPLETED",
            event_timestamp="ts",
            task_id=1,
            assignment_id=1,
            project_id="1",
            task_name="T",
            task_status="COMPLETED",
        )
        sub = CabinetSubmission(
            assignment_id=1,
            project_id="1",
            form_id="f",
            form_response_id="r",
            status="s",
            event_id="e",
        )
        task = ProcessedTask(event=event, submission=sub, attachments=[])
        assert task.was_enriched() is True

    def test_was_enriched_false(self):
        event = TaskEvent(
            event_id="e",
            event_type="CUSTOM_TASK_COMPLETED",
            event_timestamp="ts",
            task_id=1,
            assignment_id=1,
            project_id="1",
            task_name="T",
            task_status="COMPLETED",
        )
        task = ProcessedTask(event=event, submission=None, attachments=[])
        assert task.was_enriched() is False

    def test_readable_report_default(self):
        event = TaskEvent(
            event_id="e",
            event_type="CUSTOM_TASK_COMPLETED",
            event_timestamp="ts",
            task_id=1,
            assignment_id=1,
            project_id="1",
            task_name="T",
            task_status="COMPLETED",
        )
        task = ProcessedTask(event=event, submission=None, attachments=[])
        assert task.readable_report is None
