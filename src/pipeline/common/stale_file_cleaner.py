"""Periodic stale file cleanup for download/upload workers."""

import asyncio
import contextlib
import logging
import time
from pathlib import Path

logger = logging.getLogger(__name__)

STALE_FILE_MAX_AGE_HOURS = 24
CLEANUP_INTERVAL_SECONDS = 3600


def _scan_and_remove(scan_dir: Path, in_flight: set, max_age: float) -> int:
    """Scan directory and remove stale files not in the in-flight set."""
    if not scan_dir.exists():
        return 0
    count = 0
    now = time.time()
    for path in scan_dir.rglob("*"):
        if not path.is_file():
            continue
        if path.parent.name in in_flight:
            continue
        try:
            age = now - path.stat().st_mtime
            if age > max_age:
                path.unlink()
                count += 1
                logger.warning(
                    "Removed stale file",
                    extra={
                        "file_path": str(path),
                        "age_hours": round(age / 3600, 1),
                    },
                )
        except OSError:
            pass
    return count


class StaleFileCleaner:
    """Periodically removes stale files from a directory, skipping in-flight tasks."""

    def __init__(
        self,
        scan_dir: Path,
        in_flight_lock: asyncio.Lock,
        in_flight_tasks: set,
        max_age_hours: int = STALE_FILE_MAX_AGE_HOURS,
    ):
        self._scan_dir = scan_dir
        self._in_flight_lock = in_flight_lock
        self._in_flight_tasks = in_flight_tasks
        self._max_age_seconds = max_age_hours * 3600
        self._task: asyncio.Task | None = None
        self.total_removed = 0

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _run(self) -> None:
        while True:
            try:
                await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)
                await self._cleanup()
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning(
                    "Error in periodic stale file cleanup",
                    extra={"error": str(e)},
                )

    async def _cleanup(self) -> None:
        async with self._in_flight_lock:
            in_flight = set(self._in_flight_tasks)

        removed = await asyncio.to_thread(
            _scan_and_remove, self._scan_dir, in_flight, self._max_age_seconds
        )
        self.total_removed += removed
