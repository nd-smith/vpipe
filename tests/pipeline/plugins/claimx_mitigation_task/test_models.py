"""
Tests for ClaimX Mitigation Task data models.

Covers MitigationTaskEvent, MitigationSubmission, and ProcessedMitigationTask.
These are pure dataclasses - no infrastructure mocking needed.
"""

import pytest

from pipeline.plugins.claimx_mitigation_task.models import (
    MitigationSubmission,
    MitigationTaskEvent,
    ProcessedMitigationTask,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

MINIMAL_KAFKA_MESSAGE = {
    "event_id": "evt-001",
    "event_type": "TASK_UPDATED",
    "timestamp": "2024-01-15T10:00:00Z",
    "task_id": 42,
    "assignment_id": 7,
    "project_id": "5395115",
    "task_name": "Water Extraction",
    "task_status": "COMPLETED",
}

FULL_KAFKA_MESSAGE = {
    **MINIMAL_KAFKA_MESSAGE,
    "task": {
        "assigned_to_user_id": 101,
        "assigned_by_user_id": 202,
        "created_at": "2024-01-10T08:00:00Z",
        "completed_at": "2024-01-15T10:00:00Z",
    },
}


def make_event(**overrides) -> MitigationTaskEvent:
    """Build a MitigationTaskEvent with sensible defaults."""
    return MitigationTaskEvent(
        event_id=overrides.get("event_id", "evt-001"),
        event_type=overrides.get("event_type", "TASK_UPDATED"),
        event_timestamp=overrides.get("event_timestamp", "2024-01-15T10:00:00Z"),
        task_id=overrides.get("task_id", 42),
        assignment_id=overrides.get("assignment_id", 7),
        project_id=overrides.get("project_id", "5395115"),
        task_name=overrides.get("task_name", "Water Extraction"),
        task_status=overrides.get("task_status", "COMPLETED"),
    )


def make_submission(**overrides) -> MitigationSubmission:
    """Build a MitigationSubmission with sensible defaults."""
    return MitigationSubmission(
        event_id=overrides.get("event_id", "evt-001"),
        assignment_id=overrides.get("assignment_id", 7),
        project_id=overrides.get("project_id", "5395115"),
        task_id=overrides.get("task_id", 42),
        task_name=overrides.get("task_name", "Water Extraction"),
        status=overrides.get("status", "COMPLETED"),
    )


# ---------------------------------------------------------------------------
# MitigationTaskEvent - basic construction
# ---------------------------------------------------------------------------


class TestMitigationTaskEventConstruction:
    def test_required_fields_only(self):
        event = make_event()

        assert event.event_id == "evt-001"
        assert event.event_type == "TASK_UPDATED"
        assert event.event_timestamp == "2024-01-15T10:00:00Z"
        assert event.task_id == 42
        assert event.assignment_id == 7
        assert event.project_id == "5395115"
        assert event.task_name == "Water Extraction"
        assert event.task_status == "COMPLETED"

    def test_optional_fields_default_to_none(self):
        event = make_event()

        assert event.assigned_to_user_id is None
        assert event.assigned_by_user_id is None
        assert event.task_created_at is None
        assert event.task_completed_at is None

    def test_optional_fields_can_be_set(self):
        event = MitigationTaskEvent(
            event_id="evt-002",
            event_type="TASK_CREATED",
            event_timestamp="2024-01-10T08:00:00Z",
            task_id=99,
            assignment_id=5,
            project_id="1234567",
            task_name="Drying",
            task_status="ASSIGNED",
            assigned_to_user_id=101,
            assigned_by_user_id=202,
            task_created_at="2024-01-10T08:00:00Z",
            task_completed_at="2024-01-12T16:00:00Z",
        )

        assert event.assigned_to_user_id == 101
        assert event.assigned_by_user_id == 202
        assert event.task_created_at == "2024-01-10T08:00:00Z"
        assert event.task_completed_at == "2024-01-12T16:00:00Z"


# ---------------------------------------------------------------------------
# MitigationTaskEvent.from_kafka_message - valid data
# ---------------------------------------------------------------------------


class TestMitigationTaskEventFromKafkaMessage:
    def test_minimal_message_parses_required_fields(self):
        event = MitigationTaskEvent.from_kafka_message(MINIMAL_KAFKA_MESSAGE)

        assert event.event_id == "evt-001"
        assert event.event_type == "TASK_UPDATED"
        assert event.event_timestamp == "2024-01-15T10:00:00Z"
        assert event.task_id == 42
        assert event.assignment_id == 7
        assert event.project_id == "5395115"
        assert event.task_name == "Water Extraction"
        assert event.task_status == "COMPLETED"

    def test_minimal_message_optional_fields_are_none(self):
        event = MitigationTaskEvent.from_kafka_message(MINIMAL_KAFKA_MESSAGE)

        assert event.assigned_to_user_id is None
        assert event.assigned_by_user_id is None
        assert event.task_created_at is None
        assert event.task_completed_at is None

    def test_full_message_populates_optional_fields(self):
        event = MitigationTaskEvent.from_kafka_message(FULL_KAFKA_MESSAGE)

        assert event.assigned_to_user_id == 101
        assert event.assigned_by_user_id == 202
        assert event.task_created_at == "2024-01-10T08:00:00Z"
        assert event.task_completed_at == "2024-01-15T10:00:00Z"

    def test_message_with_empty_task_dict_leaves_optionals_as_none(self):
        msg = {**MINIMAL_KAFKA_MESSAGE, "task": {}}
        event = MitigationTaskEvent.from_kafka_message(msg)

        assert event.assigned_to_user_id is None
        assert event.assigned_by_user_id is None
        assert event.task_created_at is None
        assert event.task_completed_at is None

    def test_message_with_partial_task_dict(self):
        msg = {
            **MINIMAL_KAFKA_MESSAGE,
            "task": {"assigned_to_user_id": 55},
        }
        event = MitigationTaskEvent.from_kafka_message(msg)

        assert event.assigned_to_user_id == 55
        assert event.assigned_by_user_id is None
        assert event.task_created_at is None
        assert event.task_completed_at is None

    def test_timestamp_key_maps_to_event_timestamp(self):
        # The Kafka message uses "timestamp"; the field is "event_timestamp"
        msg = {**MINIMAL_KAFKA_MESSAGE, "timestamp": "2099-12-31T23:59:59Z"}
        event = MitigationTaskEvent.from_kafka_message(msg)

        assert event.event_timestamp == "2099-12-31T23:59:59Z"


# ---------------------------------------------------------------------------
# MitigationTaskEvent.from_kafka_message - missing required fields
# ---------------------------------------------------------------------------


class TestMitigationTaskEventFromKafkaMessageMissingFields:
    REQUIRED_KEYS = [
        "event_id",
        "event_type",
        "timestamp",
        "task_id",
        "assignment_id",
        "project_id",
        "task_name",
        "task_status",
    ]

    @pytest.mark.parametrize("missing_key", REQUIRED_KEYS)
    def test_missing_required_field_raises_value_error(self, missing_key):
        msg = {k: v for k, v in MINIMAL_KAFKA_MESSAGE.items() if k != missing_key}

        with pytest.raises(ValueError, match="Missing required field"):
            MitigationTaskEvent.from_kafka_message(msg)

    def test_completely_empty_message_raises_value_error(self):
        with pytest.raises(ValueError):
            MitigationTaskEvent.from_kafka_message({})


# ---------------------------------------------------------------------------
# MitigationSubmission - basic construction
# ---------------------------------------------------------------------------


class TestMitigationSubmissionConstruction:
    def test_required_fields_only(self):
        sub = make_submission()

        assert sub.event_id == "evt-001"
        assert sub.assignment_id == 7
        assert sub.project_id == "5395115"
        assert sub.task_id == 42
        assert sub.task_name == "Water Extraction"
        assert sub.status == "COMPLETED"

    def test_optional_fields_default_to_none(self):
        sub = make_submission()

        assert sub.master_filename is None
        assert sub.type_of_loss is None
        assert sub.claim_number is None
        assert sub.policy_number is None
        assert sub.date_assigned is None
        assert sub.date_completed is None
        assert sub.media is None
        assert sub.ingested_at is None

    def test_all_fields_set(self):
        media = [{"media_id": 1, "url": "https://example.com/photo.jpg"}]
        sub = MitigationSubmission(
            event_id="evt-999",
            assignment_id=10,
            project_id="9999999",
            task_id=88,
            task_name="Mold Remediation",
            status="IN_PROGRESS",
            master_filename="claim_report.pdf",
            type_of_loss="Water",
            claim_number="CLM-0042",
            policy_number="POL-7777",
            date_assigned="2024-01-10T08:00:00Z",
            date_completed="2024-01-20T17:00:00Z",
            media=media,
            ingested_at="2024-01-20T17:05:00Z",
        )

        assert sub.master_filename == "claim_report.pdf"
        assert sub.type_of_loss == "Water"
        assert sub.claim_number == "CLM-0042"
        assert sub.policy_number == "POL-7777"
        assert sub.date_assigned == "2024-01-10T08:00:00Z"
        assert sub.date_completed == "2024-01-20T17:00:00Z"
        assert sub.media == media
        assert sub.ingested_at == "2024-01-20T17:05:00Z"


# ---------------------------------------------------------------------------
# MitigationSubmission.to_flat_dict
# ---------------------------------------------------------------------------


class TestMitigationSubmissionToFlatDict:
    def test_returns_dict(self):
        result = make_submission().to_flat_dict()
        assert isinstance(result, dict)

    def test_required_fields_present(self):
        result = make_submission().to_flat_dict()

        assert result["event_id"] == "evt-001"
        assert result["assignment_id"] == 7
        assert result["project_id"] == "5395115"
        assert result["task_id"] == 42
        assert result["task_name"] == "Water Extraction"
        assert result["status"] == "COMPLETED"

    def test_optional_fields_present_as_none_when_unset(self):
        result = make_submission().to_flat_dict()

        assert "master_filename" in result
        assert result["master_filename"] is None
        assert "type_of_loss" in result
        assert result["type_of_loss"] is None
        assert "claim_number" in result
        assert result["claim_number"] is None
        assert "policy_number" in result
        assert result["policy_number"] is None
        assert "date_assigned" in result
        assert result["date_assigned"] is None
        assert "date_completed" in result
        assert result["date_completed"] is None
        assert "media" in result
        assert result["media"] is None
        assert "ingested_at" in result
        assert result["ingested_at"] is None

    def test_optional_fields_reflect_set_values(self):
        media = [{"media_id": 1}]
        sub = MitigationSubmission(
            event_id="evt-002",
            assignment_id=3,
            project_id="1111111",
            task_id=5,
            task_name="Drying",
            status="ASSIGNED",
            master_filename="file.pdf",
            type_of_loss="Fire",
            claim_number="CLM-001",
            policy_number="POL-002",
            date_assigned="2024-03-01T09:00:00Z",
            date_completed="2024-03-05T17:00:00Z",
            media=media,
            ingested_at="2024-03-05T17:01:00Z",
        )
        result = sub.to_flat_dict()

        assert result["master_filename"] == "file.pdf"
        assert result["type_of_loss"] == "Fire"
        assert result["claim_number"] == "CLM-001"
        assert result["policy_number"] == "POL-002"
        assert result["date_assigned"] == "2024-03-01T09:00:00Z"
        assert result["date_completed"] == "2024-03-05T17:00:00Z"
        assert result["media"] == media
        assert result["ingested_at"] == "2024-03-05T17:01:00Z"

    def test_exact_key_set(self):
        result = make_submission().to_flat_dict()
        expected_keys = {
            "event_id",
            "assignment_id",
            "project_id",
            "task_id",
            "task_name",
            "status",
            "master_filename",
            "type_of_loss",
            "claim_number",
            "policy_number",
            "date_assigned",
            "date_completed",
            "media",
            "ingested_at",
        }
        assert set(result.keys()) == expected_keys

    def test_dict_is_not_cached_between_calls(self):
        # Each call should return a fresh dict
        sub = make_submission()
        d1 = sub.to_flat_dict()
        d2 = sub.to_flat_dict()
        assert d1 is not d2


# ---------------------------------------------------------------------------
# ProcessedMitigationTask - basic construction
# ---------------------------------------------------------------------------


class TestProcessedMitigationTaskConstruction:
    def test_with_submission(self):
        event = make_event()
        sub = make_submission()
        processed = ProcessedMitigationTask(event=event, submission=sub)

        assert processed.event is event
        assert processed.submission is sub

    def test_without_submission(self):
        event = make_event()
        processed = ProcessedMitigationTask(event=event, submission=None)

        assert processed.event is event
        assert processed.submission is None


# ---------------------------------------------------------------------------
# ProcessedMitigationTask.was_enriched
# ---------------------------------------------------------------------------


class TestProcessedMitigationTaskWasEnriched:
    def test_returns_true_when_submission_present(self):
        processed = ProcessedMitigationTask(
            event=make_event(),
            submission=make_submission(),
        )
        assert processed.was_enriched() is True

    def test_returns_false_when_submission_is_none(self):
        processed = ProcessedMitigationTask(
            event=make_event(),
            submission=None,
        )
        assert processed.was_enriched() is False

    def test_return_type_is_bool(self):
        result = ProcessedMitigationTask(
            event=make_event(),
            submission=make_submission(),
        ).was_enriched()
        assert isinstance(result, bool)
