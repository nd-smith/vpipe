"""Tests for pipeline.common.schemas.delta_batch module."""

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from pipeline.common.schemas.delta_batch import FailedDeltaBatch


def _make_batch(**overrides):
    defaults = {
        "events": [{"traceId": "t1", "data": "x"}],
        "first_failure_at": datetime(2024, 1, 1, tzinfo=UTC),
        "last_error": "Connection timeout",
        "table_path": "abfss://ws@onelake/lakehouse/Tables/events",
    }
    defaults.update(overrides)
    return FailedDeltaBatch(**defaults)


class TestFailedDeltaBatch:
    def test_defaults(self):
        batch = _make_batch()
        assert batch.retry_count == 0
        assert batch.error_category == "transient"
        assert batch.retry_at is None
        assert len(batch.batch_id) > 0  # UUID

    def test_event_count_auto_computed(self):
        batch = _make_batch(events=[{"a": 1}, {"b": 2}, {"c": 3}])
        assert batch.event_count == 3

    def test_event_count_explicit(self):
        batch = _make_batch(events=[{"a": 1}], event_count=99)
        assert batch.event_count == 99

    def test_error_truncation(self):
        long_error = "x" * 600
        batch = _make_batch(last_error=long_error)
        assert len(batch.last_error) == 500
        assert batch.last_error.endswith("...")

    def test_empty_error_becomes_unknown(self):
        batch = _make_batch(last_error="")
        assert batch.last_error == "Unknown error"

    def test_error_category_validation(self):
        batch = _make_batch(error_category="TRANSIENT")
        assert batch.error_category == "transient"

    def test_invalid_error_category_becomes_unknown(self):
        batch = _make_batch(error_category="bogus")
        assert batch.error_category == "unknown"

    def test_valid_categories(self):
        for cat in ("transient", "permanent", "auth", "circuit_open", "unknown"):
            batch = _make_batch(error_category=cat)
            assert batch.error_category == cat

    def test_empty_events_rejected(self):
        with pytest.raises(ValidationError):
            _make_batch(events=[])

    def test_increment_retry(self):
        batch = _make_batch()
        updated = batch.increment_retry(delay_seconds=300, error="Retry failed")
        assert updated.retry_count == 1
        assert updated.last_error == "Retry failed"
        assert updated.retry_at is not None
        assert updated.batch_id == batch.batch_id
        assert updated.events == batch.events

    def test_to_dlq_format(self):
        events = [{"traceId": f"t{i}"} for i in range(5)]
        batch = _make_batch(events=events)
        dlq = batch.to_dlq_format()
        assert dlq["event_count"] == 5
        assert len(dlq["sample_trace_ids"]) == 3  # capped at 3
        assert dlq["events"] == events
        assert dlq["batch_id"] == batch.batch_id

    def test_to_dlq_format_few_events(self):
        batch = _make_batch(events=[{"traceId": "t1"}])
        dlq = batch.to_dlq_format()
        assert len(dlq["sample_trace_ids"]) == 1

    def test_serialization_roundtrip(self):
        batch = _make_batch(retry_at=datetime(2024, 1, 2, tzinfo=UTC))
        data = batch.model_dump()
        restored = FailedDeltaBatch(**data)
        assert restored.batch_id == batch.batch_id
        assert restored.events == batch.events

    def test_timestamp_serialization(self):
        batch = _make_batch(retry_at=datetime(2024, 6, 15, 12, 0, tzinfo=UTC))
        data = batch.model_dump()
        assert isinstance(data["first_failure_at"], str)
        assert isinstance(data["retry_at"], str)

    def test_timestamp_serialization_none(self):
        batch = _make_batch()
        data = batch.model_dump()
        assert data["retry_at"] is None
