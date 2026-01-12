"""Log filters for context-based routing."""

import logging

from .context import get_log_context


class StageContextFilter(logging.Filter):
    """
    Filter logs to only pass those matching a specific stage.

    Used to route logs to worker-specific file handlers based on
    the current logging context's stage value.

    Usage:
        handler = RotatingFileHandler("download.log")
        handler.addFilter(StageContextFilter("download"))
    """

    def __init__(self, stage: str):
        """
        Initialize filter for a specific stage.

        Args:
            stage: Stage name to match (e.g., "download", "upload")
        """
        super().__init__()
        self.stage = stage

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Check if log record should pass to this handler.

        Args:
            record: Log record to check

        Returns:
            True if record's context stage matches this filter's stage
        """
        ctx = get_log_context()
        return ctx.get("stage") == self.stage
