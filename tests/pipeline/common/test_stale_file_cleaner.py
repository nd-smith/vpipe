"""Tests for pipeline.common.stale_file_cleaner module."""

import asyncio
import time
from pathlib import Path

import pytest

from pipeline.common.stale_file_cleaner import StaleFileCleaner


@pytest.fixture
def scan_dir(tmp_path):
    return tmp_path / "downloads"


@pytest.fixture
def cleaner(scan_dir):
    lock = asyncio.Lock()
    in_flight = set()
    return StaleFileCleaner(scan_dir, lock, in_flight, max_age_hours=1)


class TestStaleFileCleaner:
    @pytest.mark.asyncio
    async def test_cleanup_removes_old_files(self, scan_dir, cleaner):
        scan_dir.mkdir()
        old_file = scan_dir / "old.txt"
        old_file.write_text("old")
        # Set mtime to 2 hours ago
        old_mtime = time.time() - 7200
        import os

        os.utime(old_file, (old_mtime, old_mtime))

        await cleaner._cleanup()
        assert not old_file.exists()
        assert cleaner.total_removed == 1

    @pytest.mark.asyncio
    async def test_cleanup_keeps_recent_files(self, scan_dir, cleaner):
        scan_dir.mkdir()
        recent_file = scan_dir / "recent.txt"
        recent_file.write_text("recent")

        await cleaner._cleanup()
        assert recent_file.exists()
        assert cleaner.total_removed == 0

    @pytest.mark.asyncio
    async def test_cleanup_skips_in_flight(self, scan_dir):
        scan_dir.mkdir()
        task_dir = scan_dir / "task-123"
        task_dir.mkdir()
        old_file = task_dir / "file.txt"
        old_file.write_text("data")
        import os

        old_mtime = time.time() - 7200
        os.utime(old_file, (old_mtime, old_mtime))

        lock = asyncio.Lock()
        in_flight = {"task-123"}
        cleaner = StaleFileCleaner(scan_dir, lock, in_flight, max_age_hours=1)

        await cleaner._cleanup()
        assert old_file.exists()
        assert cleaner.total_removed == 0

    @pytest.mark.asyncio
    async def test_cleanup_nonexistent_dir(self, cleaner):
        # scan_dir doesn't exist yet
        await cleaner._cleanup()
        assert cleaner.total_removed == 0

    @pytest.mark.asyncio
    async def test_start_and_stop(self, cleaner):
        await cleaner.start()
        assert cleaner._task is not None
        await cleaner.stop()
        assert cleaner._task is None

    @pytest.mark.asyncio
    async def test_stop_when_not_started(self, cleaner):
        await cleaner.stop()  # should not raise
