"""ClaimX event and entity schemas.

This module contains Pydantic models for:
- ClaimX event messages
- ClaimX enrichment and download tasks
- ClaimX entity row schemas
- ClaimX cached download messages
- ClaimX upload result messages

Schemas:
    events.py    - ClaimXEventMessage (raw ClaimX events from Eventhouse)
    tasks.py     - ClaimXEnrichmentTask, ClaimXDownloadTask (work items for workers)
    entities.py  - EntityRowsMessage (entity data container for 7 entity types)
    cached.py    - ClaimXCachedDownloadMessage (cached downloads awaiting upload)
    results.py   - ClaimXUploadResultMessage (upload outcomes for results topic)

Design Decisions:
    - Pydantic for validation and JSON serialization
    - Explicit schemas (no dynamic/dict-based messages)
    - Backward-compatible evolution (additive changes only)
    - Datetime fields as ISO 8601 strings
    - Conversion methods to/from verisk_pipeline for compatibility
"""

from kafka_pipeline.claimx.schemas.cached import ClaimXCachedDownloadMessage
from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage
from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
from kafka_pipeline.claimx.schemas.results import ClaimXUploadResultMessage
from kafka_pipeline.claimx.schemas.tasks import (
    ClaimXDownloadTask,
    ClaimXEnrichmentTask,
)

__all__ = [
    "ClaimXEventMessage",
    "ClaimXEnrichmentTask",
    "ClaimXDownloadTask",
    "ClaimXCachedDownloadMessage",
    "ClaimXUploadResultMessage",
    "EntityRowsMessage",
]
