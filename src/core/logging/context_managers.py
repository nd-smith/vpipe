"""Context managers for structured logging."""

import logging
import time
from contextlib import contextmanager
from typing import Any, Dict, Optional

from core.logging.context import get_log_context, set_log_context
from core.logging.utilities import log_exception, log_with_context


class LogContext:
    """
    Context manager for temporary log context.

    Usage:
        with LogContext(stage="download", cycle_id=cycle_id):
            # All logs in this block will have stage and cycle_id
            do_work()
    """

    def __init__(
        self,
        cycle_id: Optional[str] = None,
        stage: Optional[str] = None,
        worker_id: Optional[str] = None,
        domain: Optional[str] = None,
    ):
        self.new_context = {
            "cycle_id": cycle_id,
            "stage": stage,
            "worker_id": worker_id,
            "domain": domain,
        }
        self.old_context: Dict[str, str] = {}

    def __enter__(self) -> "LogContext":
        self.old_context = get_log_context()
        for key, value in self.new_context.items():
            if value is not None:
                set_log_context(**{key: value})
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore old context
        set_log_context(
            cycle_id=self.old_context.get("cycle_id", ""),
            stage=self.old_context.get("stage", ""),
            worker_id=self.old_context.get("worker_id", ""),
            domain=self.old_context.get("domain", ""),
        )
        return False


class StageLogContext(LogContext):
    """
    Context manager for stage execution with automatic timing.

    Usage:
        with StageLogContext("download", cycle_id=cycle_id) as ctx:
            result = do_stage_work()
            ctx.set_result(records_processed=100)
    """

    def __init__(
        self,
        stage: str,
        cycle_id: Optional[str] = None,
        domain: Optional[str] = None,
    ):
        super().__init__(stage=stage, cycle_id=cycle_id, domain=domain)
        self.stage = stage
        self.start_time: Optional[float] = None
        self.result_context: Dict[str, Any] = {}

    def __enter__(self) -> "StageLogContext":
        super().__enter__()
        self.start_time = time.perf_counter()
        return self

    def set_result(self, **kwargs: Any) -> None:
        """Set result context to be logged on exit."""
        self.result_context.update(kwargs)

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = (time.perf_counter() - self.start_time) * 1000
        self.result_context["duration_ms"] = round(duration_ms, 2)
        super().__exit__(exc_type, exc_val, exc_tb)
        return False


@contextmanager
def log_phase(
    logger: logging.Logger,
    phase: str,
    level: int = logging.DEBUG,
    **context: Any,
):
    """
    Context manager for timing a phase within a stage.

    Args:
        logger: Logger instance
        phase: Phase name
        level: Log level for completion message
        **context: Additional context fields

    Example:
        with log_phase(logger, "fetch_events"):
            events = fetch_from_kusto()
    """
    # Convert string level names to integers
    if isinstance(level, str):
        level = logging.getLevelName(level.upper())

    start = time.perf_counter()
    try:
        yield
    finally:
        duration_ms = (time.perf_counter() - start) * 1000
        log_with_context(
            logger,
            level,
            f"Phase complete: {phase}",
            duration_ms=round(duration_ms, 2),
            **context,
        )


class OperationContext:
    """Context manager for timed operations with automatic logging."""

    def __init__(
        self,
        logger: logging.Logger,
        operation: str,
        level: int = logging.DEBUG,
        slow_threshold_ms: Optional[float] = 1000.0,
        log_start: bool = False,
        **context: Any,
    ):
        self.logger = logger
        self.operation = operation
        # Convert string level names to integers
        if isinstance(level, str):
            self.level = getattr(logging, level.upper(), logging.DEBUG)
        else:
            self.level = level
        self.slow_threshold_ms = slow_threshold_ms
        self.log_start = log_start
        self.context = context
        self._start_time: Optional[float] = None

    def __enter__(self) -> "OperationContext":
        self._start_time = time.perf_counter()
        if self.log_start:
            log_with_context(
                self.logger,
                logging.DEBUG,
                f"Starting: {self.operation}",
                operation=self.operation,
                **self.context,
            )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = (time.perf_counter() - self._start_time) * 1000

        # Auto-promote to INFO if slow
        effective_level = self.level
        if self.slow_threshold_ms and duration_ms > self.slow_threshold_ms:
            effective_level = max(self.level, logging.INFO)

        if exc_val is not None:
            log_exception(
                self.logger,
                exc_val,
                f"Failed: {self.operation}",
                duration_ms=round(duration_ms, 2),
                operation=self.operation,
                **self.context,
            )
        else:
            log_with_context(
                self.logger,
                effective_level,
                f"Completed: {self.operation}",
                duration_ms=round(duration_ms, 2),
                operation=self.operation,
                **self.context,
            )
        return False

    def add_context(self, **kwargs: Any) -> None:
        """Add context mid-operation (row counts, etc)."""
        self.context.update(kwargs)


@contextmanager
def log_operation(
    logger: logging.Logger,
    operation: str,
    level: int = logging.DEBUG,
    slow_threshold_ms: Optional[float] = 1000.0,
    **context: Any,
):
    """Convenience context manager for ad-hoc operation logging."""
    with OperationContext(
        logger, operation, level=level, slow_threshold_ms=slow_threshold_ms, **context
    ) as ctx:
        yield ctx
