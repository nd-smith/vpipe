"""Tests for logging filters."""

import logging

import pytest

from core.logging.context import clear_log_context, set_log_context
from core.logging.filters import StageContextFilter


class TestStageContextFilter:
    """Tests for StageContextFilter."""

    @pytest.fixture(autouse=True)
    def clear_context(self):
        """Clear logging context before and after each test."""
        clear_log_context()
        yield
        clear_log_context()

    def test_passes_matching_stage(self):
        """Filter passes logs when stage context matches."""
        filter = StageContextFilter("download")
        set_log_context(stage="download")

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )

        assert filter.filter(record) is True

    def test_blocks_non_matching_stage(self):
        """Filter blocks logs when stage context doesn't match."""
        filter = StageContextFilter("download")
        set_log_context(stage="upload")

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )

        assert filter.filter(record) is False

    def test_blocks_empty_stage(self):
        """Filter blocks logs when no stage context is set."""
        filter = StageContextFilter("download")
        # No stage set

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )

        assert filter.filter(record) is False

    def test_different_filters_route_correctly(self):
        """Different filters route logs to correct handlers."""
        download_filter = StageContextFilter("download")
        upload_filter = StageContextFilter("upload")

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )

        # Set download stage
        set_log_context(stage="download")
        assert download_filter.filter(record) is True
        assert upload_filter.filter(record) is False

        # Switch to upload stage
        set_log_context(stage="upload")
        assert download_filter.filter(record) is False
        assert upload_filter.filter(record) is True
