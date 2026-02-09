"""Tests for logging utility functions."""

import logging
from unittest.mock import MagicMock, call, patch

import pytest

from core.logging.utilities import (
    _RESERVED_LOG_KEYS,
    detect_log_output_mode,
    format_cycle_output,
    get_log_output_mode,
    log_exception,
    log_startup_banner,
    log_with_context,
    log_worker_error,
)


class TestLogWithContext:

    def test_logs_message_at_given_level(self):
        logger = MagicMock()
        log_with_context(logger, logging.INFO, "test message")

        logger.log.assert_called_once_with(
            logging.INFO, "test message", exc_info=None, extra={}
        )

    def test_passes_extra_fields(self):
        logger = MagicMock()
        log_with_context(
            logger, logging.WARNING, "slow request",
            trace_id="t-1", duration_ms=500,
        )

        logger.log.assert_called_once_with(
            logging.WARNING, "slow request",
            exc_info=None,
            extra={"trace_id": "t-1", "duration_ms": 500},
        )

    def test_handles_exc_info_separately(self):
        logger = MagicMock()
        log_with_context(
            logger, logging.ERROR, "failed",
            exc_info=True, trace_id="t-2",
        )

        logger.log.assert_called_once_with(
            logging.ERROR, "failed",
            exc_info=True,
            extra={"trace_id": "t-2"},
        )

    def test_filters_reserved_log_keys(self):
        logger = MagicMock()
        log_with_context(
            logger, logging.INFO, "msg",
            name="should_be_filtered",
            lineno=99,
            trace_id="t-keep",
        )

        _, kwargs = logger.log.call_args
        assert "name" not in kwargs["extra"]
        assert "lineno" not in kwargs["extra"]
        assert kwargs["extra"]["trace_id"] == "t-keep"

    def test_filters_all_reserved_keys(self):
        logger = MagicMock()
        # Exclude "msg" and "args" since those are also positional params of log_with_context
        safe_reserved = {k: "value" for k in _RESERVED_LOG_KEYS if k not in ("msg", "args")}
        safe_reserved["custom_field"] = "kept"

        log_with_context(logger, logging.INFO, "test", **safe_reserved)

        _, kwargs = logger.log.call_args
        assert kwargs["extra"] == {"custom_field": "kept"}


class TestLogException:

    def test_logs_exception_with_traceback(self):
        logger = MagicMock()
        exc = ValueError("bad value")

        log_exception(logger, exc, "Operation failed")

        logger.log.assert_called_once()
        args, kwargs = logger.log.call_args
        assert args[0] == logging.ERROR
        assert args[1] == "Operation failed"
        assert kwargs["exc_info"] is exc
        assert kwargs["extra"]["error_message"] == "bad value"

    def test_logs_exception_without_traceback(self):
        logger = MagicMock()
        exc = RuntimeError("oops")

        log_exception(logger, exc, "Failed", include_traceback=False)

        logger.log.assert_called_once()
        _, kwargs = logger.log.call_args
        assert "exc_info" not in kwargs

    def test_uses_custom_log_level(self):
        logger = MagicMock()
        exc = ValueError("warn")

        log_exception(logger, exc, "Warning", level=logging.WARNING)

        args, _ = logger.log.call_args
        assert args[0] == logging.WARNING

    def test_extracts_error_category_from_pipeline_error(self):
        logger = MagicMock()

        class FakeCategory:
            value = "TRANSIENT"

        exc = Exception("transient failure")
        exc.category = FakeCategory()

        log_exception(logger, exc, "Retryable error")

        _, kwargs = logger.log.call_args
        assert kwargs["extra"]["error_category"] == "TRANSIENT"

    def test_extracts_string_category_from_exception(self):
        logger = MagicMock()
        exc = Exception("permanent")
        exc.category = "PERMANENT"

        log_exception(logger, exc, "Permanent error")

        _, kwargs = logger.log.call_args
        assert kwargs["extra"]["error_category"] == "PERMANENT"

    def test_does_not_override_explicit_error_category(self):
        logger = MagicMock()
        exc = Exception("err")
        exc.category = "FROM_EXCEPTION"

        log_exception(
            logger, exc, "Error",
            error_category="EXPLICIT",
        )

        _, kwargs = logger.log.call_args
        assert kwargs["extra"]["error_category"] == "EXPLICIT"

    def test_truncates_long_error_messages(self):
        logger = MagicMock()
        exc = ValueError("x" * 1000)

        log_exception(logger, exc, "Long error")

        _, kwargs = logger.log.call_args
        assert len(kwargs["extra"]["error_message"]) == 503  # 500 + "..."
        assert kwargs["extra"]["error_message"].endswith("...")

    def test_passes_extra_kwargs(self):
        logger = MagicMock()
        exc = ValueError("err")

        log_exception(
            logger, exc, "Error",
            trace_id="t-1", http_status=500,
        )

        _, kwargs = logger.log.call_args
        assert kwargs["extra"]["trace_id"] == "t-1"
        assert kwargs["extra"]["http_status"] == 500


class TestFormatCycleOutput:

    def test_formats_basic_cycle_without_deltas(self):
        result = format_cycle_output(1, succeeded=100, failed=5)

        assert "Cycle 1:" in result
        assert "processed=105" in result
        assert "succeeded=100" in result
        assert "failed=5" in result

    def test_omits_skipped_when_zero(self):
        result = format_cycle_output(1, succeeded=100, failed=5, skipped=0)

        assert "skipped" not in result

    def test_includes_skipped_when_nonzero(self):
        result = format_cycle_output(1, succeeded=100, failed=5, skipped=10)

        assert "skipped=10" in result

    def test_omits_deduplicated_when_zero(self):
        result = format_cycle_output(1, succeeded=100, failed=0, deduplicated=0)

        assert "deduped" not in result

    def test_includes_deduplicated_when_nonzero(self):
        result = format_cycle_output(1, succeeded=100, failed=0, deduplicated=20)

        assert "deduped=20" in result

    def test_formats_with_deltas(self):
        deltas = {"succeeded": 50, "failed": 2, "skipped": 1, "deduplicated": 0}
        result = format_cycle_output(
            5, succeeded=500, failed=10, skipped=5,
            since_last=deltas, interval_seconds=30,
        )

        assert "Cycle 5:" in result
        assert "+53 this cycle" in result
        assert "500 succeeded" in result
        assert "10 failed" in result
        assert "5 skipped" in result
        assert "msg/s" in result

    def test_delta_rate_calculation(self):
        deltas = {"succeeded": 60, "failed": 0, "skipped": 0}
        result = format_cycle_output(
            1, succeeded=60, failed=0,
            since_last=deltas, interval_seconds=30,
        )

        assert "2.0 msg/s" in result

    def test_delta_rate_with_zero_interval(self):
        deltas = {"succeeded": 10, "failed": 0, "skipped": 0}
        result = format_cycle_output(
            1, succeeded=10, failed=0,
            since_last=deltas, interval_seconds=0,
        )

        assert "0.0 msg/s" in result

    def test_delta_format_omits_zero_failed(self):
        deltas = {"succeeded": 10, "failed": 0, "skipped": 0}
        result = format_cycle_output(
            1, succeeded=10, failed=0,
            since_last=deltas, interval_seconds=30,
        )

        assert "failed" not in result

    def test_delta_format_omits_zero_skipped(self):
        deltas = {"succeeded": 10, "failed": 0, "skipped": 0}
        result = format_cycle_output(
            1, succeeded=10, failed=0, skipped=0,
            since_last=deltas, interval_seconds=30,
        )

        assert "skipped" not in result

    def test_delta_format_includes_deduped_when_nonzero(self):
        deltas = {"succeeded": 10, "failed": 0, "skipped": 0, "deduplicated": 5}
        result = format_cycle_output(
            1, succeeded=10, failed=0, deduplicated=5,
            since_last=deltas, interval_seconds=30,
        )

        assert "5 deduped" in result

    def test_cycle_zero(self):
        result = format_cycle_output(0, succeeded=0, failed=0)

        assert "Cycle 0:" in result
        assert "processed=0" in result


class TestGetLogOutputMode:

    def test_returns_stdout_when_log_to_stdout(self):
        assert get_log_output_mode(True, False, False) == "stdout"

    def test_returns_stdout_even_with_other_flags(self):
        assert get_log_output_mode(True, True, True) == "stdout"

    def test_returns_file_when_file_enabled(self):
        assert get_log_output_mode(False, True, False) == "file"

    def test_returns_eventhub_when_eventhub_enabled(self):
        assert get_log_output_mode(False, False, True) == "eventhub"

    def test_returns_file_plus_eventhub(self):
        assert get_log_output_mode(False, True, True) == "file+eventhub"

    def test_returns_none_when_nothing_enabled(self):
        assert get_log_output_mode(False, False, False) == "none"


class TestDetectLogOutputMode:

    def test_detects_stdout_with_single_stream_handler(self):
        root = logging.getLogger("test_detect_stdout")
        root.handlers.clear()
        root.addHandler(logging.StreamHandler())

        with patch("core.logging.utilities.logging.getLogger", return_value=root):
            assert detect_log_output_mode() == "stdout"

    def test_detects_file_handler(self):
        root = logging.getLogger("test_detect_file")
        root.handlers.clear()
        file_handler = MagicMock(spec=logging.FileHandler)
        file_handler.__class__ = logging.FileHandler
        stream_handler = logging.StreamHandler()
        root.addHandler(file_handler)
        root.addHandler(stream_handler)

        with patch("core.logging.utilities.logging.getLogger", return_value=root):
            assert detect_log_output_mode() == "file"

    def test_detects_eventhub_handler_by_class_name(self):
        root = logging.getLogger("test_detect_eh_name")
        root.handlers.clear()

        class EventHubLogHandler(logging.Handler):
            pass

        eh_handler = EventHubLogHandler()
        stream_handler = logging.StreamHandler()
        root.addHandler(eh_handler)
        root.addHandler(stream_handler)

        with patch("core.logging.utilities.logging.getLogger", return_value=root):
            assert detect_log_output_mode() == "eventhub"

    def test_handler_without_eventhub_in_name_is_not_detected(self):
        root = logging.getLogger("test_detect_eh")
        root.handlers.clear()

        class FakeCustomHandler(logging.Handler):
            pass

        root.addHandler(FakeCustomHandler())
        root.addHandler(logging.StreamHandler())

        with patch("core.logging.utilities.logging.getLogger", return_value=root):
            assert detect_log_output_mode() == "console"

    def test_detects_file_plus_eventhub(self):
        root = logging.getLogger("test_detect_both")
        root.handlers.clear()

        class EventHubLogHandler(logging.Handler):
            pass

        file_handler = MagicMock(spec=logging.FileHandler)
        file_handler.__class__ = logging.FileHandler
        root.addHandler(file_handler)
        root.addHandler(EventHubLogHandler())
        root.addHandler(logging.StreamHandler())

        with patch("core.logging.utilities.logging.getLogger", return_value=root):
            assert detect_log_output_mode() == "file+eventhub"

    def test_returns_console_when_no_handlers(self):
        root = logging.getLogger("test_detect_empty")
        root.handlers.clear()

        with patch("core.logging.utilities.logging.getLogger", return_value=root):
            assert detect_log_output_mode() == "console"


class TestLogStartupBanner:

    def test_logs_banner_with_worker_name(self):
        logger = MagicMock()
        log_startup_banner(logger, "Test Worker")

        logger.info.assert_called_once()
        banner = logger.info.call_args[0][0]
        assert "Test Worker" in banner
        assert "=" * 50 in banner

    def test_includes_optional_fields(self):
        logger = MagicMock()
        log_startup_banner(
            logger,
            "Test Worker",
            instance_id="worker-0",
            domain="claimx",
            input_topic="input.topic",
            output_topic="output.topic",
            health_port=8081,
            version="1.2.3",
            log_output_mode="file+eventhub",
        )

        banner = logger.info.call_args[0][0]
        assert "worker-0" in banner
        assert "claimx" in banner
        assert "input.topic" in banner
        assert "output.topic" in banner
        assert "8081" in banner
        assert "1.2.3" in banner
        assert "file+eventhub" in banner

    def test_omits_none_fields(self):
        logger = MagicMock()
        log_startup_banner(logger, "Test Worker")

        banner = logger.info.call_args[0][0]
        assert "Instance:" not in banner
        assert "Domain:" not in banner
        assert "Input Topic:" not in banner
        assert "Output Topic:" not in banner
        assert "Health:" not in banner
        assert "Version:" not in banner
        assert "Log Output:" not in banner


class TestLogWorkerError:

    def test_logs_error_with_message(self):
        logger = MagicMock()
        log_worker_error(logger, "Download failed")

        logger.error.assert_called_once()
        args, kwargs = logger.error.call_args
        assert args[0] == "Download failed"

    def test_includes_event_id_and_correlation_id(self):
        logger = MagicMock()
        log_worker_error(logger, "Error", event_id="evt-123")

        _, kwargs = logger.error.call_args
        assert kwargs["extra"]["event_id"] == "evt-123"
        assert kwargs["extra"]["correlation_id"] == "evt-123"

    def test_includes_error_category(self):
        logger = MagicMock()
        log_worker_error(logger, "Error", error_category="TRANSIENT")

        _, kwargs = logger.error.call_args
        assert kwargs["extra"]["error_category"] == "TRANSIENT"

    def test_includes_exception_traceback(self):
        logger = MagicMock()
        exc = ValueError("bad")
        log_worker_error(logger, "Error", exc=exc)

        _, kwargs = logger.error.call_args
        assert kwargs["exc_info"] is exc

    def test_logs_without_traceback_when_no_exception(self):
        logger = MagicMock()
        log_worker_error(logger, "Error")

        _, kwargs = logger.error.call_args
        assert "exc_info" not in kwargs

    def test_passes_extra_context(self):
        logger = MagicMock()
        log_worker_error(
            logger, "Error",
            media_id="m-1", status_code=500, retry_count=2,
        )

        _, kwargs = logger.error.call_args
        assert kwargs["extra"]["media_id"] == "m-1"
        assert kwargs["extra"]["status_code"] == 500
        assert kwargs["extra"]["retry_count"] == 2

    def test_omits_none_event_id(self):
        logger = MagicMock()
        log_worker_error(logger, "Error")

        _, kwargs = logger.error.call_args
        assert "event_id" not in kwargs["extra"]
        assert "correlation_id" not in kwargs["extra"]

    def test_omits_none_error_category(self):
        logger = MagicMock()
        log_worker_error(logger, "Error")

        _, kwargs = logger.error.call_args
        assert "error_category" not in kwargs["extra"]
