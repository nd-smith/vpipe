"""Periodic statistics logging utility for workers."""

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from core.logging.utilities import format_cycle_output

logger = logging.getLogger(__name__)


class PeriodicStatsLogger:
    """
    Manages periodic statistics logging for workers with delta tracking.

    Tracks changes between cycles and provides delta metrics for rate calculation.
    Workers provide a callback that returns extra fields with cumulative counts.
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
        self._previous_stats: dict[str, int] = {}

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
        """Run the periodic logging loop with delta tracking."""
        # Initial log (cycle 0) - no deltas for first cycle
        _, initial_extra = self.get_stats(0)

        # Use format_cycle_output for consistent formatting
        initial_msg = format_cycle_output(
            cycle_count=0,
            succeeded=initial_extra.get("records_succeeded", 0),
            failed=initial_extra.get("records_failed", 0),
            skipped=initial_extra.get("records_skipped", 0),
            deduplicated=initial_extra.get("records_deduplicated", 0),
        )

        logger.info(
            f"{initial_msg} [cycle output every {self.interval_seconds}s]",
            extra={
                "worker_id": self.worker_id,
                "stage": self.stage,
                "cycle": 0,
                **initial_extra,
            },
        )

        # Store initial values for delta calculation
        self._previous_stats = {
            "succeeded": initial_extra.get("records_succeeded", 0),
            "failed": initial_extra.get("records_failed", 0),
            "skipped": initial_extra.get("records_skipped", 0),
            "deduplicated": initial_extra.get("records_deduplicated", 0),
        }

        try:
            while True:
                await asyncio.sleep(self.interval_seconds)
                self._cycle_count += 1

                # Get current stats from worker
                _, extra = self.get_stats(self._cycle_count)

                # Extract current cumulative counts
                current_stats = {
                    "succeeded": extra.get("records_succeeded", 0),
                    "failed": extra.get("records_failed", 0),
                    "skipped": extra.get("records_skipped", 0),
                    "deduplicated": extra.get("records_deduplicated", 0),
                }

                # Calculate deltas since last cycle
                deltas = {
                    key: current_stats[key] - self._previous_stats.get(key, 0)
                    for key in current_stats
                }

                # Format message with deltas using the enhanced format_cycle_output
                msg = format_cycle_output(
                    cycle_count=self._cycle_count,
                    succeeded=current_stats["succeeded"],
                    failed=current_stats["failed"],
                    skipped=current_stats["skipped"],
                    deduplicated=current_stats["deduplicated"],
                    since_last=deltas,
                    interval_seconds=self.interval_seconds,
                )

                # Update previous stats for next cycle
                self._previous_stats = current_stats

                # Add delta metrics to extra fields for structured logging (JSON)
                delta_total = deltas["succeeded"] + deltas["failed"] + deltas["skipped"]
                rate = delta_total / self.interval_seconds if self.interval_seconds > 0 else 0

                logger.info(
                    msg,
                    extra={
                        "worker_id": self.worker_id,
                        "stage": self.stage,
                        "cycle": self._cycle_count,
                        "cycle_id": f"cycle-{self._cycle_count}",
                        "cycle_interval_seconds": self.interval_seconds,
                        "delta_succeeded": deltas["succeeded"],
                        "delta_failed": deltas["failed"],
                        "delta_skipped": deltas["skipped"],
                        "delta_total": delta_total,
                        "rate_msg_per_sec": round(rate, 1),
                        **extra,
                    },
                )

        except asyncio.CancelledError:
            logger.debug("Periodic stats logger task cancelled")
            raise
