"""ClaimX domain for processing ClaimX entity events and data.

This module contains all ClaimX-specific logic including:
- Event schemas and message definitions
- API client for ClaimX service
- Event handlers for different entity types
- Workers for event ingestion, enrichment, download, and upload
- Delta table writers for ClaimX entities

Import classes directly from submodules to avoid loading heavy dependencies:
    from kafka_pipeline.claimx.api_client import ClaimXApiClient, ClaimXApiError
    from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
    from kafka_pipeline.claimx.handlers import get_handler_registry
"""

# Don't import concrete implementations here to avoid loading
# heavy dependencies (aiohttp, etc.) at package import time.
# Users should import directly from submodules.

__all__: list[str] = []
