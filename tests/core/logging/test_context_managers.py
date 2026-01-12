"""Tests for logging context managers."""

import logging
import time
from unittest.mock import MagicMock, patch

import pytest

from core.logging.context import clear_log_context, get_log_context, set_log_context
from core.logging.context_managers import (
    LogContext,
    OperationContext,
    StageLogContext,
    log_operation,
    log_phase,
)


@pytest.fixture(autouse=True)
def reset_context():
    """Reset log context before each test."""
    clear_log_context()
    yield
    clear_log_context()


@pytest.fixture
def logger():
    """Create a mock logger for testing."""
    return MagicMock(spec=logging.Logger)


class TestLogContext:
    """Tests for LogContext manager."""

    def test_sets_context_on_enter(self):
        """Test context is set when entering the context manager."""
        with LogContext(cycle_id="test-123", stage="download"):
            ctx = get_log_context()
            assert ctx["cycle_id"] == "test-123"
            assert ctx["stage"] == "download"

    def test_restores_context_on_exit(self):
        """Test context is restored when exiting the context manager."""
        # Set initial context
        set_log_context(cycle_id="initial", stage="initial-stage")

        with LogContext(cycle_id="test-123", stage="download"):
            pass

        # Should restore to initial
        ctx = get_log_context()
        assert ctx["cycle_id"] == "initial"
        assert ctx["stage"] == "initial-stage"

    def test_handles_none_values(self):
        """Test None values don't override context."""
        set_log_context(cycle_id="existing")

        with LogContext(stage="download"):
            ctx = get_log_context()
            assert ctx["cycle_id"] == "existing"
            assert ctx["stage"] == "download"

    def test_nested_contexts(self):
        """Test nested context managers work correctly."""
        with LogContext(cycle_id="outer", stage="stage1"):
            assert get_log_context()["cycle_id"] == "outer"

            with LogContext(cycle_id="inner", stage="stage2"):
                assert get_log_context()["cycle_id"] == "inner"
                assert get_log_context()["stage"] == "stage2"

            # Should restore to outer context
            assert get_log_context()["cycle_id"] == "outer"
            assert get_log_context()["stage"] == "stage1"


class TestStageLogContext:
    """Tests for StageLogContext manager."""

    def test_sets_stage_context(self):
        """Test stage context is set correctly."""
        with StageLogContext("download", cycle_id="test-123") as ctx:
            log_ctx = get_log_context()
            assert log_ctx["stage"] == "download"
            assert log_ctx["cycle_id"] == "test-123"

    def test_tracks_timing(self):
        """Test timing is tracked correctly."""
        with StageLogContext("download") as ctx:
            time.sleep(0.01)  # 10ms
            ctx.set_result(records=100)

        # Duration should be set
        assert "duration_ms" in ctx.result_context
        assert ctx.result_context["duration_ms"] >= 10

    def test_set_result_adds_context(self):
        """Test set_result adds to result context."""
        with StageLogContext("download") as ctx:
            ctx.set_result(records=100, errors=5)

        assert ctx.result_context["records"] == 100
        assert ctx.result_context["errors"] == 5


class TestLogPhase:
    """Tests for log_phase context manager."""

    def test_logs_phase_completion(self, logger):
        """Test phase completion is logged."""
        with log_phase(logger, "fetch_data"):
            time.sleep(0.01)

        # Should have logged completion
        logger.log.assert_called_once()
        args, kwargs = logger.log.call_args
        assert args[0] == logging.DEBUG
        assert "Phase complete: fetch_data" in args[1]
        assert "duration_ms" in kwargs["extra"]

    def test_custom_log_level(self, logger):
        """Test custom log level is used."""
        with log_phase(logger, "fetch_data", level=logging.INFO):
            pass

        args, _ = logger.log.call_args
        assert args[0] == logging.INFO

    def test_string_log_level(self, logger):
        """Test string log level is converted."""
        with log_phase(logger, "fetch_data", level="INFO"):
            pass

        args, _ = logger.log.call_args
        assert args[0] == logging.INFO

    def test_additional_context(self, logger):
        """Test additional context is logged."""
        with log_phase(logger, "fetch_data", records=100):
            pass

        _, kwargs = logger.log.call_args
        assert kwargs["extra"]["records"] == 100

    def test_logs_even_on_exception(self, logger):
        """Test phase is logged even if exception occurs."""
        with pytest.raises(ValueError):
            with log_phase(logger, "fetch_data"):
                raise ValueError("test error")

        # Should still log
        logger.log.assert_called_once()


class TestOperationContext:
    """Tests for OperationContext manager."""

    def test_logs_completion(self, logger):
        """Test operation completion is logged."""
        with OperationContext(logger, "download_file"):
            pass

        logger.log.assert_called_once()
        args, kwargs = logger.log.call_args
        assert "Completed: download_file" in args[1]
        assert kwargs["extra"]["operation"] == "download_file"
        assert "duration_ms" in kwargs["extra"]

    def test_logs_start_when_enabled(self, logger):
        """Test operation start is logged when enabled."""
        with OperationContext(logger, "download_file", log_start=True):
            pass

        # Should have 2 calls: start and completion
        assert logger.log.call_count == 2
        first_call = logger.log.call_args_list[0]
        assert "Starting: download_file" in first_call[0][1]

    def test_promotes_slow_operations_to_info(self, logger):
        """Test slow operations are promoted to INFO level."""
        with OperationContext(logger, "slow_op", slow_threshold_ms=1):
            time.sleep(0.01)  # 10ms > 1ms threshold

        args, _ = logger.log.call_args
        # Should be promoted to INFO
        assert args[0] >= logging.INFO

    def test_respects_threshold_for_fast_ops(self, logger):
        """Test fast operations use original level."""
        with OperationContext(logger, "fast_op", level=logging.DEBUG, slow_threshold_ms=1000):
            pass  # Very fast

        args, _ = logger.log.call_args
        assert args[0] == logging.DEBUG

    def test_logs_exceptions(self, logger):
        """Test exceptions are logged with context."""
        with pytest.raises(ValueError):
            with OperationContext(logger, "failing_op"):
                raise ValueError("test error")

        args, kwargs = logger.log.call_args
        assert "Failed: failing_op" in args[1]
        assert kwargs["extra"]["operation"] == "failing_op"
        assert "duration_ms" in kwargs["extra"]

    def test_add_context_mid_operation(self, logger):
        """Test context can be added during operation."""
        with OperationContext(logger, "download") as ctx:
            ctx.add_context(bytes_downloaded=1024)

        _, kwargs = logger.log.call_args
        assert kwargs["extra"]["bytes_downloaded"] == 1024

    def test_string_level_conversion(self, logger):
        """Test string log level is converted to int."""
        with OperationContext(logger, "op", level="WARNING"):
            pass

        args, _ = logger.log.call_args
        assert args[0] == logging.WARNING


class TestLogOperation:
    """Tests for log_operation convenience function."""

    def test_wraps_operation_context(self, logger):
        """Test log_operation wraps OperationContext."""
        with log_operation(logger, "test_op") as ctx:
            assert isinstance(ctx, OperationContext)

        logger.log.assert_called_once()

    def test_passes_parameters(self, logger):
        """Test parameters are passed to OperationContext."""
        with log_operation(logger, "test_op", level=logging.INFO, slow_threshold_ms=500):
            pass

        args, _ = logger.log.call_args
        assert args[0] == logging.INFO

    def test_allows_context_addition(self, logger):
        """Test context can be added through yielded object."""
        with log_operation(logger, "test_op") as ctx:
            ctx.add_context(result="success")

        _, kwargs = logger.log.call_args
        assert kwargs["extra"]["result"] == "success"
