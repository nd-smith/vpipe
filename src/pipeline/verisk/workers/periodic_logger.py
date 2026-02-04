"""Periodic statistics logging utility for workers."""

import asyncio
import logging
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)


class PeriodicStatsLogger:
    """
    Manages periodic statistics logging for workers.

    Replaces the inefficient 1-second sleep loop pattern with direct
    interval-based sleeping. Workers provide a callback that returns
    the log message and extra fields.
    """

    def __init__(
        self,
        interval_seconds: int,
        get_stats: Callable[[int], tuple[str, dict[str, Any]]],
        stage: str,
        worker_id: str,
    ):
        """
        Args:
            interval_seconds: Logging interval in seconds
            get_stats: Callback that takes cycle_count and returns (message, extra_fields)
            stage: Stage name for logging context
            worker_id: Worker identifier
        """
        self.interval_seconds = interval_seconds
        self.get_stats = get_stats
        self.stage = stage
        self.worker_id = worker_id
        self._task: asyncio.Task | None = None
        self._cycle_count = 0

    def start(self) -> None:
        """Start the periodic logging task."""
        if self._task is not None:
            logger.warning("Periodic logger already running")
            return

        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Stop the periodic logging task."""
        if self._task is None:
            return

        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    async def _run(self) -> None:
        """Run the periodic logging loop."""
        # Initial log (cycle 0)
        initial_msg, initial_extra = self.get_stats(0)
        logger.info(
            f"{initial_msg} [cycle output every {self.interval_seconds}s]",
            extra={
                "worker_id": self.worker_id,
                "stage": self.stage,
                "cycle": 0,
                **initial_extra,
            },
        )

        try:
            while True:
                await asyncio.sleep(self.interval_seconds)

                self._cycle_count += 1
                msg, extra = self.get_stats(self._cycle_count)

                logger.info(
                    msg,
                    extra={
                        "worker_id": self.worker_id,
                        "stage": self.stage,
                        "cycle": self._cycle_count,
                        "cycle_id": f"cycle-{self._cycle_count}",
                        "cycle_interval_seconds": self.interval_seconds,
                        **extra,
                    },
                )

        except asyncio.CancelledError:
            logger.debug("Periodic stats logger task cancelled")
            raise
