"""
Event message schemas for Kafka pipeline.

Contains Pydantic models for raw event messages consumed from source topics.
Schema aligned with verisk_pipeline EventRecord for compatibility.
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, computed_field, field_serializer, field_validator


class EventMessage(BaseModel):
    """Schema for raw event messages from Eventhouse/EventHub.

    Schema matches verisk_pipeline.xact.xact_models.EventRecord for compatibility.
    Represents raw event data before transformation.

    Eventhouse source columns:
        - type: Full event type string (e.g., "verisk.claims.property.xn.documentsReceived")
        - version: Event version (integer)
        - utcDateTime: Event timestamp
        - traceId: Trace identifier (camelCase in source)
        - data: JSON object with nested event data

    Attributes:
        type: Full event type string (e.g., "verisk.claims.property.xn.documentsReceived")
        version: Event version (integer)
        utc_datetime: Event timestamp as string
        trace_id: Unique identifier for the event (from traceId)
        data: JSON string with nested event data (serializes to object)

    Computed properties (excluded from serialization):
        status_subtype: Last part of type string (e.g., "documentsReceived")
        data_dict: Parsed data as dict (for convenience)
        attachments: List of attachment URLs from data.attachments

    Matches verisk_pipeline EventRecord:
        @dataclass
        class EventRecord:
            type: str
            version: str
            utc_datetime: str
            trace_id: str
            data: str  # JSON string

    Example:
        >>> event = EventMessage(
        ...     type="verisk.claims.property.xn.documentsReceived",
        ...     version=1,
        ...     utc_datetime="2024-12-25T10:30:00Z",
        ...     trace_id="abc123-def456",
        ...     data='{"assignmentId": "A12345", "attachments": ["https://..."]}'
        ... )
        >>> event.status_subtype
        'documentsReceived'
    """

    type: str = Field(
        ...,
        description="Full event type string (e.g., 'verisk.claims.property.xn.documentsReceived')",
        min_length=1
    )
    version: Union[int, str] = Field(
        ...,
        description="Event version (integer preferred)"
    )
    utc_datetime: str = Field(
        ...,
        description="Event timestamp as ISO string",
        alias="utcDateTime"
    )
    trace_id: str = Field(
        ...,
        description="Unique event identifier (from traceId)",
        min_length=1,
        alias="traceId"
    )
    event_id: Optional[str] = Field(
        default=None,
        description="Unique event ID generated during ingestion",
        alias="eventId"
    )
    data: str = Field(
        ...,
        description="Raw JSON string with nested event data"
    )

    @field_serializer('data')
    def serialize_data_as_object(self, v: str) -> Dict[str, Any]:
        """Serialize data field as JSON object instead of string."""
        if not v:
            return {}
        try:
            return json.loads(v)
        except (json.JSONDecodeError, TypeError):
            return {}

    @computed_field
    @property
    def status_subtype(self) -> str:
        """Extract status subtype from event type (last part after last dot)."""
        if "." in self.type:
            return self.type.split(".")[-1]
        return self.type

    @computed_field
    @property
    def data_dict(self) -> Optional[Dict[str, Any]]:
        """Parse data JSON string to dict (cached)."""
        if not self.data:
            return None
        try:
            return json.loads(self.data)
        except (json.JSONDecodeError, TypeError):
            return None

    @computed_field
    @property
    def attachments(self) -> Optional[List[str]]:
        """Extract attachments list from parsed data."""
        data = self.data_dict
        if data and "attachments" in data:
            attachments = data["attachments"]
            if isinstance(attachments, list):
                return [url for url in attachments if url and isinstance(url, str)]
        return None

    @computed_field
    @property
    def assignment_id(self) -> Optional[str]:
        """Extract assignmentId from parsed data."""
        data = self.data_dict
        if data:
            return data.get("assignmentId")
        return None

    @computed_field
    @property
    def estimate_version(self) -> Optional[str]:
        """Extract estimateVersion from parsed data."""
        data = self.data_dict
        if data:
            return data.get("estimateVersion")
        return None

    @field_validator('type', 'trace_id')
    @classmethod
    def validate_non_empty_strings(cls, v: str, info) -> str:
        """Ensure string fields are not empty or whitespace-only."""
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} cannot be empty or whitespace")
        return v.strip()

    def to_eventhouse_row(self) -> Dict[str, Any]:
        """
        Convert back to Eventhouse row format for flatten_events().

        Returns:
            Dict with Eventhouse column names (type, version, utcDateTime, traceId, data)
        """
        return {
            "type": self.type,
            "version": self.version,
            "utcDateTime": self.utc_datetime,
            "traceId": self.trace_id,
            "data": self.data,
        }

    def to_verisk_event_record(self) -> "EventRecord":
        """
        Convert to verisk_pipeline EventRecord dataclass.

        Returns:
            kafka_pipeline.xact.schemas.models.EventRecord instance
        """
        from kafka_pipeline.xact.schemas.models import EventRecord

        return EventRecord(
            type=self.type,
            version=self.version,
            utc_datetime=self.utc_datetime,
            trace_id=self.trace_id,
            data=self.data,
        )

    @classmethod
    def from_eventhouse_row(cls, row: Dict[str, Any]) -> "EventMessage":
        """
        Create from raw Eventhouse row dict.

        Args:
            row: Dict with Eventhouse columns (type, version, utcDateTime, traceId, data)

        Returns:
            EventMessage instance
        """
        # Handle data field - may be dict or string
        data = row.get("data", "{}")
        if isinstance(data, dict):
            data = json.dumps(data)

        # Preserve version as its original type (int preferred)
        version = row.get("version", 1)
        if isinstance(version, str) and version.isdigit():
            version = int(version)

        return cls(
            type=row.get("type", ""),
            version=version,
            utcDateTime=str(row.get("utcDateTime", "")),
            traceId=row.get("traceId", row.get("trace_id", "")),
            eventId=row.get("eventId", row.get("event_id")),
            data=data,
        )

    def model_dump_json(self, **kwargs) -> str:
        """Serialize to JSON with aliases and without computed fields.

        Overrides default to:
        - Use camelCase field names (utcDateTime, traceId)
        - Exclude computed fields (status_subtype, data_dict, etc.)
        - Serialize data as object (via field_serializer)
        """
        # Set defaults for Kafka-compatible output
        kwargs.setdefault('by_alias', True)
        kwargs.setdefault('exclude', {
            'status_subtype',
            'data_dict',
            'attachments',
            'assignment_id',
            'estimate_version',
        })
        return super().model_dump_json(**kwargs)

    def model_dump(self, **kwargs) -> Dict[str, Any]:
        """Serialize to dict with aliases and without computed fields.

        Overrides default to:
        - Use camelCase field names (utcDateTime, traceId)
        - Exclude computed fields (status_subtype, data_dict, etc.)
        - Serialize data as object (via field_serializer)
        """
        # Set defaults for Kafka-compatible output
        kwargs.setdefault('by_alias', True)
        kwargs.setdefault('exclude', {
            'status_subtype',
            'data_dict',
            'attachments',
            'assignment_id',
            'estimate_version',
        })
        return super().model_dump(**kwargs)

    model_config = {
        'populate_by_name': True,  # Allow both alias and field name
        'json_schema_extra': {
            'examples': [
                {
                    'type': 'verisk.claims.property.xn.documentsReceived',
                    'version': 1,
                    'utcDateTime': '2024-12-25T10:30:00Z',
                    'traceId': 'abc123-def456-ghi789',
                    'data': {"assignmentId": "A12345", "description": "Documents received", "attachments": ["https://xactware.com/docs/estimate.pdf"]}
                },
                {
                    'type': 'verisk.claims.property.xn.estimateCreated',
                    'version': 2,
                    'utcDateTime': '2024-12-25T11:00:00Z',
                    'traceId': 'xyz789-abc123',
                    'data': {"assignmentId": "B67890", "estimateVersion": "1.0", "note": "Initial estimate"}
                }
            ]
        }
    }
