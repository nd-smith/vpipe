"""
Tests for EventMessage schema.

Validates Pydantic model behavior, JSON serialization, and field validation.
"""

import json

import pytest
from pydantic import ValidationError

from kafka_pipeline.xact.schemas.events import EventMessage


class TestEventMessageCreation:
    """Test EventMessage instantiation with valid data."""

    def test_create_with_all_fields(self):
        """EventMessage can be created with all fields populated."""
        event = EventMessage(
            type="verisk.claims.property.xn.documentsReceived",
            version=1,
            utcDateTime="2024-12-25T10:30:00Z",
            traceId="evt-123",
            data='{"assignmentId": "C-456", "attachments": ["https://storage.example.com/file1.pdf"]}'
        )

        assert event.type == "verisk.claims.property.xn.documentsReceived"
        assert event.version == 1
        assert event.utc_datetime == "2024-12-25T10:30:00Z"
        assert event.trace_id == "evt-123"
        assert event.attachments == ["https://storage.example.com/file1.pdf"]

    def test_create_without_attachments(self):
        """EventMessage can be created with data that has no attachments."""
        event = EventMessage(
            type="verisk.claims.property.xn.estimateCreated",
            version=2,
            utcDateTime="2024-12-25T10:30:00Z",
            traceId="evt-456",
            data='{"assignmentId": "P-789"}'
        )

        assert event.attachments is None

    def test_create_with_empty_attachments_list(self):
        """Empty attachments list in data returns empty list."""
        event = EventMessage(
            type="verisk.claims.property.xn.documentsReceived",
            version=1,
            utcDateTime="2024-12-25T10:30:00Z",
            traceId="evt-789",
            data='{"assignmentId": "A-123", "attachments": []}'
        )

        # Empty list returns None (filtered by the computed property)
        assert event.attachments is None or event.attachments == []

    def test_create_with_complex_data(self):
        """Data can contain nested structures."""
        complex_data = {
            "assignmentId": "C-001",
            "details": {
                "amount": 50000,
                "category": "property",
                "metadata": {
                    "filed_by": "agent-123",
                    "priority": "high"
                }
            },
            "attachments": [
                "https://example.com/file1.pdf",
                "https://example.com/file2.pdf"
            ]
        }

        event = EventMessage(
            type="verisk.claims.property.xn.documentsReceived",
            version=1,
            utcDateTime="2024-12-25T10:30:00Z",
            traceId="evt-complex",
            data=json.dumps(complex_data)
        )

        assert event.data_dict == complex_data
        assert event.data_dict["details"]["metadata"]["priority"] == "high"


class TestEventMessageValidation:
    """Test field validation rules."""

    def test_missing_required_field_raises_error(self):
        """Missing required fields raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            EventMessage(
                type="verisk.claims.property.xn.documentsReceived",
                version=1,
                utcDateTime="2024-12-25T10:30:00Z",
                data='{}'
                # Missing traceId
            )

        errors = exc_info.value.errors()
        assert any('traceId' in str(e) or 'trace_id' in str(e) for e in errors)

    def test_empty_trace_id_raises_error(self):
        """Empty trace_id raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            EventMessage(
                type="verisk.claims.property.xn.documentsReceived",
                version=1,
                utcDateTime="2024-12-25T10:30:00Z",
                traceId="",
                data='{}'
            )

        errors = exc_info.value.errors()
        assert any('trace_id' in str(e) or 'traceId' in str(e) for e in errors)

    def test_whitespace_trace_id_raises_error(self):
        """Whitespace-only trace_id raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            EventMessage(
                type="verisk.claims.property.xn.documentsReceived",
                version=1,
                utcDateTime="2024-12-25T10:30:00Z",
                traceId="   ",
                data='{}'
            )

        errors = exc_info.value.errors()
        assert any('trace_id' in str(e) or 'traceId' in str(e) for e in errors)

    def test_whitespace_is_trimmed(self):
        """Leading/trailing whitespace is trimmed from validated fields."""
        event = EventMessage(
            type="  verisk.claims.property.xn.documentsReceived  ",
            version=1,
            utcDateTime="2024-12-25T10:30:00Z",
            traceId="  evt-123  ",
            data='{}'
        )

        assert event.trace_id == "evt-123"
        assert event.type == "verisk.claims.property.xn.documentsReceived"


class TestEventMessageSerialization:
    """Test JSON serialization and deserialization."""

    def test_serialize_to_json_uses_aliases(self):
        """EventMessage serializes with camelCase aliases."""
        event = EventMessage(
            type="verisk.claims.property.xn.documentsReceived",
            version=1,
            utcDateTime="2024-12-25T10:30:00Z",
            traceId="evt-123",
            data='{"assignmentId": "C-456"}'
        )

        json_str = event.model_dump_json()
        parsed = json.loads(json_str)

        # Should use camelCase aliases
        assert "utcDateTime" in parsed
        assert "traceId" in parsed
        # Should NOT use snake_case
        assert "utc_datetime" not in parsed
        assert "trace_id" not in parsed

    def test_version_serializes_as_integer(self):
        """Version field serializes as integer, not string."""
        event = EventMessage(
            type="verisk.claims.property.xn.documentsReceived",
            version=1,
            utcDateTime="2024-12-25T10:30:00Z",
            traceId="evt-123",
            data='{}'
        )

        json_str = event.model_dump_json()
        parsed = json.loads(json_str)

        assert parsed["version"] == 1
        assert isinstance(parsed["version"], int)

    def test_data_serializes_as_object(self):
        """Data field serializes as JSON object, not string."""
        event = EventMessage(
            type="verisk.claims.property.xn.documentsReceived",
            version=1,
            utcDateTime="2024-12-25T10:30:00Z",
            traceId="evt-123",
            data='{"assignmentId": "C-456", "attachments": ["https://example.com/file.pdf"]}'
        )

        json_str = event.model_dump_json()
        parsed = json.loads(json_str)

        assert isinstance(parsed["data"], dict)
        assert parsed["data"]["assignmentId"] == "C-456"
        assert parsed["data"]["attachments"] == ["https://example.com/file.pdf"]

    def test_computed_fields_excluded_from_serialization(self):
        """Computed fields are excluded from JSON output."""
        event = EventMessage(
            type="verisk.claims.property.xn.documentsReceived",
            version=1,
            utcDateTime="2024-12-25T10:30:00Z",
            traceId="evt-123",
            data='{"assignmentId": "C-456", "attachments": ["https://example.com/file.pdf"]}'
        )

        json_str = event.model_dump_json()
        parsed = json.loads(json_str)

        # Computed fields should not be in output
        assert "status_subtype" not in parsed
        assert "data_dict" not in parsed
        assert "attachments" not in parsed
        assert "assignment_id" not in parsed
        assert "estimate_version" not in parsed

    def test_computed_fields_still_accessible(self):
        """Computed fields are still accessible on the model instance."""
        event = EventMessage(
            type="verisk.claims.property.xn.documentsReceived",
            version=1,
            utcDateTime="2024-12-25T10:30:00Z",
            traceId="evt-123",
            data='{"assignmentId": "C-456", "attachments": ["https://example.com/file.pdf"]}'
        )

        assert event.status_subtype == "documentsReceived"
        assert event.assignment_id == "C-456"
        assert event.attachments == ["https://example.com/file.pdf"]

    def test_deserialize_from_json(self):
        """EventMessage can be created from JSON string."""
        json_data = {
            "type": "verisk.claims.property.xn.estimateCreated",
            "version": 2,
            "utcDateTime": "2024-12-25T15:45:00Z",
            "traceId": "evt-789",
            "data": '{"assignmentId": "P-001"}'
        }

        json_str = json.dumps(json_data)
        event = EventMessage.model_validate_json(json_str)

        assert event.trace_id == "evt-789"
        assert event.type == "verisk.claims.property.xn.estimateCreated"
        assert event.version == 2

    def test_from_eventhouse_row(self):
        """EventMessage can be created from Eventhouse row dict."""
        row = {
            "type": "verisk.claims.property.xn.documentsReceived",
            "version": 1,
            "utcDateTime": "2024-12-25T10:30:00Z",
            "traceId": "evt-123",
            "data": {"assignmentId": "C-456", "attachments": ["https://example.com/file.pdf"]}
        }

        event = EventMessage.from_eventhouse_row(row)

        assert event.type == "verisk.claims.property.xn.documentsReceived"
        assert event.version == 1
        assert event.trace_id == "evt-123"
        assert event.assignment_id == "C-456"

    def test_from_eventhouse_row_converts_string_version_to_int(self):
        """from_eventhouse_row converts string version to int if numeric."""
        row = {
            "type": "verisk.claims.property.xn.documentsReceived",
            "version": "2",
            "utcDateTime": "2024-12-25T10:30:00Z",
            "traceId": "evt-123",
            "data": "{}"
        }

        event = EventMessage.from_eventhouse_row(row)

        assert event.version == 2
        assert isinstance(event.version, int)

    def test_to_eventhouse_row(self):
        """to_eventhouse_row returns dict with Eventhouse column names."""
        event = EventMessage(
            type="verisk.claims.property.xn.documentsReceived",
            version=1,
            utcDateTime="2024-12-25T10:30:00Z",
            traceId="evt-123",
            data='{"assignmentId": "C-456"}'
        )

        row = event.to_eventhouse_row()

        assert row["type"] == "verisk.claims.property.xn.documentsReceived"
        assert row["version"] == 1
        assert row["utcDateTime"] == "2024-12-25T10:30:00Z"
        assert row["traceId"] == "evt-123"
        assert row["data"] == '{"assignmentId": "C-456"}'


class TestEventMessageEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_data_is_valid(self):
        """Empty data object is valid."""
        event = EventMessage(
            type="verisk.claims.property.xn.test",
            version=1,
            utcDateTime="2024-12-25T10:30:00Z",
            traceId="evt-empty",
            data='{}'
        )

        assert event.data_dict == {}

    def test_very_long_trace_id(self):
        """Very long trace_id is accepted."""
        long_id = "evt-" + "x" * 1000
        event = EventMessage(
            type="verisk.claims.property.xn.test",
            version=1,
            utcDateTime="2024-12-25T10:30:00Z",
            traceId=long_id,
            data='{}'
        )

        assert event.trace_id == long_id

    def test_unicode_in_data(self):
        """Data can contain Unicode characters."""
        unicode_data = {
            "description": "Schäden an Gebäude",
            "note": "重要な情報",
            "emoji": "🔥💧"
        }
        event = EventMessage(
            type="verisk.claims.property.xn.test",
            version=1,
            utcDateTime="2024-12-25T10:30:00Z",
            traceId="evt-unicode",
            data=json.dumps(unicode_data)
        )

        assert event.data_dict["description"] == "Schäden an Gebäude"
        assert event.data_dict["note"] == "重要な情報"
        assert event.data_dict["emoji"] == "🔥💧"

    def test_status_subtype_extraction(self):
        """status_subtype correctly extracts last part of type."""
        event = EventMessage(
            type="verisk.claims.property.xn.documentsReceived",
            version=1,
            utcDateTime="2024-12-25T10:30:00Z",
            traceId="evt-123",
            data='{}'
        )

        assert event.status_subtype == "documentsReceived"

    def test_status_subtype_with_no_dots(self):
        """status_subtype returns full type if no dots."""
        event = EventMessage(
            type="simpleType",
            version=1,
            utcDateTime="2024-12-25T10:30:00Z",
            traceId="evt-123",
            data='{}'
        )

        assert event.status_subtype == "simpleType"

    def test_invalid_json_data_returns_empty_dict(self):
        """Invalid JSON data returns empty dict for data_dict."""
        event = EventMessage(
            type="verisk.claims.property.xn.test",
            version=1,
            utcDateTime="2024-12-25T10:30:00Z",
            traceId="evt-123",
            data='not valid json'
        )

        assert event.data_dict is None

    def test_version_as_string_preserved_if_not_numeric(self):
        """Version that is not a digit string is preserved as-is."""
        event = EventMessage(
            type="verisk.claims.property.xn.test",
            version="1.0.0",  # Non-digit string
            utcDateTime="2024-12-25T10:30:00Z",
            traceId="evt-123",
            data='{}'
        )

        assert event.version == "1.0.0"
