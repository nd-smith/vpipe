"""
Structured logging module.

Provides JSON logging with correlation IDs and context propagation.
"""

from core.logging.context import (
    clear_log_context,
    clear_message_context,
    get_log_context,
    get_message_context,
    set_log_context,
    set_message_context,
)
from core.logging.context_managers import (
    LogContext,
    MessageLogContext,
    OperationContext,
    StageLogContext,
    log_operation,
    log_phase,
)
from core.logging.formatters import ConsoleFormatter, JSONFormatter
from core.logging.setup import (
    generate_cycle_id,
    get_log_file_path,
    get_logger,
    log_worker_startup,
    setup_logging,
)
from core.logging.utilities import (
    format_cycle_output,
    log_exception,
    log_with_context,
    log_worker_error,
)

__all__ = [
    # Setup
    "setup_logging",
    "get_logger",
    "generate_cycle_id",
    "get_log_file_path",
    "log_worker_startup",
    # Formatters
    "JSONFormatter",
    "ConsoleFormatter",
    # Context
    "set_log_context",
    "get_log_context",
    "clear_log_context",
    # Message Context
    "set_message_context",
    "get_message_context",
    "clear_message_context",
    "MessageLogContext",
    # Context Managers
    "LogContext",
    "StageLogContext",
    "OperationContext",
    "log_phase",
    "log_operation",
    # Utilities
    "log_with_context",
    "log_exception",
    "format_cycle_output",
    "log_worker_error",
]
