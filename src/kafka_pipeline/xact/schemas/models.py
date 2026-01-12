"""
Xact data models migrated from verisk_pipeline.

Contains core models used for event processing and download task management.
"""

from dataclasses import dataclass
from typing import Optional


# XACT tracking table configuration
XACT_PRIMARY_KEYS = ["trace_id", "attachment_url"]


@dataclass
class EventRecord:
    """
    Parsed event record from Kusto.

    Represents raw event before transformation.
    """

    type: str
    version: str
    utc_datetime: str
    trace_id: str
    data: str  # JSON string

    @property
    def status_subtype(self) -> str:
        """Extract status subtype from event type."""
        return self.type.split(".")[-1] if "." in self.type else self.type


@dataclass
class Task:
    """
    Attachment download task.

    Represents a single attachment to be downloaded and stored.
    """

    trace_id: str
    attachment_url: str
    blob_path: str
    status_subtype: str
    file_type: str
    assignment_id: str
    estimate_version: Optional[str] = None
    retry_count: int = 0
