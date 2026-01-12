"""
XA linking event handler.

Handles: PROJECT_AUTO_XA_LINKING_UNSUCCESSFUL
"""

import logging

from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage
from kafka_pipeline.claimx.handlers.base import (
    NoOpHandler,
    register_handler,
)
from kafka_pipeline.claimx.handlers.utils import now_datetime

from core.logging import get_logger, log_with_context
from kafka_pipeline.common.logging import extract_log_context

logger = get_logger(__name__)


@register_handler
class XALinkingHandler(NoOpHandler):
    """
    Handler for XA linking failure events.

    Sets xa_autolink_fail flag on project row when XactAnalysis
    auto-linking is unsuccessful.
    """

    event_types = ["PROJECT_AUTO_XA_LINKING_UNSUCCESSFUL"]
    supports_batching = False

    def extract_rows(self, event: ClaimXEventMessage) -> EntityRowsMessage:
        """Extract project row with XA autolink failure flags."""
        now = now_datetime()
        rows = EntityRowsMessage()

        rows.projects.append({
            "project_id": event.project_id,
            "xa_autolink_fail": True,
            "xa_autolink_fail_at": now,
            "updated_at": now,
            "event_id": event.event_id,
        })

        log_with_context(
            logger,
            logging.DEBUG,
            "Extracted XA linking failure",
            handler_name="xa_linking",
            **extract_log_context(event),
        )

        return rows
