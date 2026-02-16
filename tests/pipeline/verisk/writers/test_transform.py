"""
Tests for verisk event transformation (flatten_events).

Covers:
- Scalar field extraction from JSON data column
- Nested field extraction (claim_number, contact_* fields)
- Attachments list → comma-joined string
- Null/invalid/empty JSON handling
- Dict-valued data column (struct) normalization
- Column ordering matches FLATTENED_SCHEMA
"""

import polars as pl
import pytest

from pipeline.verisk.writers.transform import FLATTENED_SCHEMA, flatten_events


def _make_event(
    data: str = "{}",
    event_type: str = "verisk.claims.property.xn.documentsReceived",
    trace_id: str = "trace-001",
    utc_datetime: str = "2024-01-15T10:30:00Z",
    version: str = "1",
    event_id: str | None = None,
) -> dict:
    """Build a raw event dict matching Kafka message format."""
    d = {
        "type": event_type,
        "version": version,
        "utcDateTime": utc_datetime,
        "traceId": trace_id,
        "data": data,
    }
    if event_id is not None:
        d["eventId"] = event_id
    return d


class TestFlattenEventsColumns:
    """Verify output schema matches FLATTENED_SCHEMA."""

    def test_column_order_matches_schema(self):
        df = pl.DataFrame([_make_event()])
        result = flatten_events(df)
        assert result.columns == list(FLATTENED_SCHEMA.keys())

    def test_all_extracted_columns_are_utf8_or_expected_types(self):
        df = pl.DataFrame([_make_event()])
        result = flatten_events(df)
        for col_name, expected_type in FLATTENED_SCHEMA.items():
            assert result[col_name].dtype == expected_type, (
                f"Column {col_name}: expected {expected_type}, got {result[col_name].dtype}"
            )


class TestFlattenEventsBaseFields:
    """Test base column extraction (non-data fields)."""

    def test_type_passthrough(self):
        df = pl.DataFrame([_make_event()])
        result = flatten_events(df)
        assert result["type"][0] == "verisk.claims.property.xn.documentsReceived"

    def test_status_subtype_extracted(self):
        df = pl.DataFrame([_make_event()])
        result = flatten_events(df)
        assert result["status_subtype"][0] == "documentsReceived"

    def test_trace_id_renamed(self):
        df = pl.DataFrame([_make_event(trace_id="abc-123")])
        result = flatten_events(df)
        assert result["trace_id"][0] == "abc-123"

    def test_ingested_at_parsed_as_utc(self):
        df = pl.DataFrame([_make_event(utc_datetime="2024-06-15T08:30:00Z")])
        result = flatten_events(df)
        ts = result["ingested_at"][0]
        assert ts.year == 2024
        assert ts.month == 6
        assert ts.hour == 8

    def test_event_date_extracted(self):
        from datetime import date

        df = pl.DataFrame([_make_event(utc_datetime="2024-06-15T08:30:00Z")])
        result = flatten_events(df)
        assert result["event_date"][0] == date(2024, 6, 15)

    def test_event_id_from_eventId_column(self):
        df = pl.DataFrame([_make_event(event_id="evt-001")])
        result = flatten_events(df)
        assert result["event_id"][0] == "evt-001"

    def test_event_id_null_when_absent(self):
        df = pl.DataFrame([_make_event()])
        result = flatten_events(df)
        assert result["event_id"][0] is None


class TestFlattenEventsSimpleFields:
    """Test simple (top-level) field extraction from data JSON."""

    def test_all_simple_fields(self):
        data = (
            '{"description": "Docs received", "assignmentId": "A123", '
            '"originalAssignmentId": "OA456", "xnAddress": "addr1", '
            '"carrierId": "C100", "estimateVersion": "2.0", '
            '"note": "A note", "author": "John", '
            '"senderReviewerName": "SR Name", "senderReviewerEmail": "sr@test.com", '
            '"carrierReviewerName": "CR Name", "carrierReviewerEmail": "cr@test.com", '
            '"dateTime": "2024-01-15T10:00:00"}'
        )
        df = pl.DataFrame([_make_event(data=data)])
        result = flatten_events(df)
        row = result.row(0, named=True)

        assert row["description"] == "Docs received"
        assert row["assignment_id"] == "A123"
        assert row["original_assignment_id"] == "OA456"
        assert row["xn_address"] == "addr1"
        assert row["carrier_id"] == "C100"
        assert row["estimate_version"] == "2.0"
        assert row["note"] == "A note"
        assert row["author"] == "John"
        assert row["sender_reviewer_name"] == "SR Name"
        assert row["sender_reviewer_email"] == "sr@test.com"
        assert row["carrier_reviewer_name"] == "CR Name"
        assert row["carrier_reviewer_email"] == "cr@test.com"
        assert row["event_datetime_mdt"] == "2024-01-15T10:00:00"

    def test_missing_fields_are_null(self):
        df = pl.DataFrame([_make_event(data="{}")])
        result = flatten_events(df)
        row = result.row(0, named=True)

        assert row["description"] is None
        assert row["assignment_id"] is None
        assert row["carrier_id"] is None


class TestFlattenEventsNestedFields:
    """Test nested field extraction from data JSON."""

    def test_claim_number(self):
        data = '{"adm": {"coverageLoss": {"claimNumber": "CLM-001"}}}'
        df = pl.DataFrame([_make_event(data=data)])
        result = flatten_events(df)
        assert result["claim_number"][0] == "CLM-001"

    def test_contact_fields(self):
        data = (
            '{"contact": {"type": "adjuster", "name": "Jane", '
            '"contactMethods": {"phone": {"type": "mobile", "number": "555-1234", '
            '"extension": "42"}, "email": {"address": "jane@test.com"}}}}'
        )
        df = pl.DataFrame([_make_event(data=data)])
        result = flatten_events(df)
        row = result.row(0, named=True)

        assert row["contact_type"] == "adjuster"
        assert row["contact_name"] == "Jane"
        assert row["contact_phone_type"] == "mobile"
        assert row["contact_phone_number"] == "555-1234"
        assert row["contact_phone_extension"] == "42"
        assert row["contact_email_address"] == "jane@test.com"

    def test_missing_nested_fields_are_null(self):
        df = pl.DataFrame([_make_event(data="{}")])
        result = flatten_events(df)
        row = result.row(0, named=True)

        assert row["claim_number"] is None
        assert row["contact_type"] is None
        assert row["contact_phone_number"] is None
        assert row["contact_email_address"] is None


class TestFlattenEventsAttachments:
    """Test attachments list → comma-joined string."""

    def test_multiple_attachments(self):
        data = '{"attachments": ["file1.pdf", "file2.pdf", "file3.pdf"]}'
        df = pl.DataFrame([_make_event(data=data)])
        result = flatten_events(df)
        assert result["attachments"][0] == "file1.pdf,file2.pdf,file3.pdf"

    def test_single_attachment(self):
        data = '{"attachments": ["only.pdf"]}'
        df = pl.DataFrame([_make_event(data=data)])
        result = flatten_events(df)
        assert result["attachments"][0] == "only.pdf"

    def test_empty_attachments_list(self):
        data = '{"attachments": []}'
        df = pl.DataFrame([_make_event(data=data)])
        result = flatten_events(df)
        assert result["attachments"][0] is None

    def test_no_attachments_key(self):
        data = '{"assignmentId": "A123"}'
        df = pl.DataFrame([_make_event(data=data)])
        result = flatten_events(df)
        assert result["attachments"][0] is None

    def test_null_and_empty_entries_filtered(self):
        data = '{"attachments": [null, "valid.pdf", ""]}'
        df = pl.DataFrame([_make_event(data=data)])
        result = flatten_events(df)
        assert result["attachments"][0] == "valid.pdf"


class TestFlattenEventsEdgeCases:
    """Test null, invalid, and edge-case data."""

    def test_null_data_column(self):
        """When data is explicitly null JSON."""
        event = _make_event(data="null")
        df = pl.DataFrame([event])
        result = flatten_events(df)
        assert result["assignment_id"][0] is None
        assert result["attachments"][0] is None

    def test_empty_json_object(self):
        df = pl.DataFrame([_make_event(data="{}")])
        result = flatten_events(df)
        row = result.row(0, named=True)
        assert row["assignment_id"] is None

    def test_multiple_rows_mixed_data(self):
        events = [
            _make_event(
                data='{"assignmentId": "A1", "attachments": ["f1.pdf"]}',
                trace_id="t1",
            ),
            _make_event(data="{}", trace_id="t2"),
            _make_event(
                data='{"carrierId": "C99", "adm": {"coverageLoss": {"claimNumber": "CLM-99"}}}',
                trace_id="t3",
            ),
        ]
        df = pl.DataFrame(events)
        result = flatten_events(df)

        assert len(result) == 3
        assert result["assignment_id"][0] == "A1"
        assert result["assignment_id"][1] is None
        assert result["carrier_id"][2] == "C99"
        assert result["claim_number"][2] == "CLM-99"
        assert result["attachments"][0] == "f1.pdf"
        assert result["attachments"][1] is None

    def test_dict_data_column(self):
        """When data arrives as already-parsed dicts (struct column)."""
        events = [
            {
                "type": "verisk.claims.property.xn.test",
                "version": "1",
                "utcDateTime": "2024-01-15T10:00:00Z",
                "traceId": "t1",
                "data": {"assignmentId": "A123", "attachments": ["f1.pdf"]},
            },
        ]
        df = pl.DataFrame(events)
        assert df["data"].dtype != pl.Utf8  # Should be Struct
        result = flatten_events(df)
        assert result["assignment_id"][0] == "A123"
        assert result["attachments"][0] == "f1.pdf"

    def test_raw_json_column_contains_full_row(self):
        df = pl.DataFrame([_make_event(data='{"assignmentId": "A1"}')])
        result = flatten_events(df)
        raw = result["raw_json"][0]
        assert "traceId" in raw or "trace_id" in raw
        assert "A1" in raw
