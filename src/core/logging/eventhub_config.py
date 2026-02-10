"""EventHub logging configuration helper."""

import logging
import os


def prepare_eventhub_logging_config(logging_config: dict) -> dict | None:
    """
    Prepare EventHub logging configuration from logging config dict.

    Args:
        logging_config: Logging configuration dictionary from MessageConfig

    Returns:
        dict with EventHub handler configuration, or None if disabled/not configured
    """
    eventhub_logging = logging_config.get("eventhub_logging", {})

    if not eventhub_logging.get("enabled", True):
        return None

    # Get namespace connection string from environment
    connection_string = os.getenv("EVENTHUB_NAMESPACE_CONNECTION_STRING")
    if not connection_string:
        return None

    # Parse log level
    level_str = eventhub_logging.get("level", "INFO")
    level = getattr(logging, level_str.upper(), logging.INFO)

    return {
        "connection_string": connection_string,
        "eventhub_name": eventhub_logging.get("eventhub_name", "application-logs"),
        "level": level,
        "batch_size": int(eventhub_logging.get("batch_size", 100)),
        "batch_timeout_seconds": float(eventhub_logging.get("batch_timeout_seconds", 1.0)),
        "max_queue_size": int(eventhub_logging.get("max_queue_size", 10000)),
        "circuit_breaker_threshold": int(eventhub_logging.get("circuit_breaker_threshold", 5)),
    }
