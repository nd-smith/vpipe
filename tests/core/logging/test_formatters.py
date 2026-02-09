"""Tests for JSON and console log formatters."""

import json
import logging
import sys
from unittest.mock import patch

import pytest

from core.logging.context import clear_log_context, set_log_context
from core.logging.formatters import ConsoleFormatter, JSONFormatter


def _make_record(
    msg="test message",
    level=logging.INFO,
    name="test.logger",
    exc_info=None,
    **extras,
):
    record = logging.LogRecord(
        name=name,
        level=level,
        pathname="test.py",
        lineno=42,
        msg=msg,
        args=(),
        exc_info=exc_info,
    )
    for key, value in extras.items():
        setattr(record, key, value)
    return record


class TestJSONFormatter:

    @pytest.fixture(autouse=True)
    def clear_context(self):
        clear_log_context()
        yield
        clear_log_context()

    def test_formats_basic_json_with_required_fields(self):
        formatter = JSONFormatter()
        record = _make_record()
        output = json.loads(formatter.format(record))

        assert output["level"] == "INFO"
        assert output["logger"] == "test.logger"
        assert output["message"] == "test message"
        assert "ts" in output
        assert output["ts"].endswith("Z")

    def test_includes_domain_from_context(self):
        set_log_context(domain="claimx")
        formatter = JSONFormatter()
        output = json.loads(formatter.format(_make_record()))

        assert output["domain"] == "claimx"

    def test_includes_stage_from_context(self):
        set_log_context(stage="download")
        formatter = JSONFormatter()
        output = json.loads(formatter.format(_make_record()))

        assert output["stage"] == "download"

    def test_includes_cycle_id_from_context(self):
        set_log_context(cycle_id="c-20260101-120000-abcd")
        formatter = JSONFormatter()
        output = json.loads(formatter.format(_make_record()))

        assert output["cycle_id"] == "c-20260101-120000-abcd"

    def test_includes_worker_id_from_context(self):
        set_log_context(worker_id="worker-0")
        formatter = JSONFormatter()
        output = json.loads(formatter.format(_make_record()))

        assert output["worker_id"] == "worker-0"

    def test_includes_trace_id_from_context(self):
        set_log_context(trace_id="abc123")
        formatter = JSONFormatter()
        output = json.loads(formatter.format(_make_record()))

        assert output["trace_id"] == "abc123"

    def test_extra_field_overrides_context_trace_id(self):
        set_log_context(trace_id="from-context")
        formatter = JSONFormatter()
        record = _make_record(trace_id="from-extra")
        output = json.loads(formatter.format(record))

        assert output["trace_id"] == "from-extra"

    def test_omits_empty_context_fields(self):
        formatter = JSONFormatter()
        output = json.loads(formatter.format(_make_record()))

        assert "domain" not in output
        assert "stage" not in output
        assert "cycle_id" not in output
        assert "worker_id" not in output

    def test_includes_file_location_for_debug(self):
        formatter = JSONFormatter()
        record = _make_record(level=logging.DEBUG)
        output = json.loads(formatter.format(record))

        assert "file" in output
        assert "test.py:42" == output["file"]

    def test_includes_file_location_for_error(self):
        formatter = JSONFormatter()
        record = _make_record(level=logging.ERROR)
        output = json.loads(formatter.format(record))

        assert "file" in output

    def test_includes_file_location_for_critical(self):
        formatter = JSONFormatter()
        record = _make_record(level=logging.CRITICAL)
        output = json.loads(formatter.format(record))

        assert "file" in output

    def test_omits_file_location_for_info(self):
        formatter = JSONFormatter()
        record = _make_record(level=logging.INFO)
        output = json.loads(formatter.format(record))

        assert "file" not in output

    def test_omits_file_location_for_warning(self):
        formatter = JSONFormatter()
        record = _make_record(level=logging.WARNING)
        output = json.loads(formatter.format(record))

        assert "file" not in output

    def test_extracts_extra_fields_from_record(self):
        formatter = JSONFormatter()
        record = _make_record(
            trace_id="t-123",
            duration_ms=150.5,
            http_status=200,
            error_message="something broke",
        )
        output = json.loads(formatter.format(record))

        assert output["trace_id"] == "t-123"
        assert output["duration_ms"] == 150.5
        assert output["http_status"] == 200
        assert output["error_message"] == "something broke"

    def test_omits_none_extra_fields(self):
        formatter = JSONFormatter()
        record = _make_record()
        output = json.loads(formatter.format(record))

        assert "trace_id" not in output
        assert "duration_ms" not in output

    def test_ensures_int_type_for_int_fields(self):
        formatter = JSONFormatter()
        record = _make_record(http_status="200", records_processed="50")
        output = json.loads(formatter.format(record))

        assert output["http_status"] == 200
        assert isinstance(output["http_status"], int)
        assert output["records_processed"] == 50
        assert isinstance(output["records_processed"], int)

    def test_ensures_float_type_for_float_fields(self):
        formatter = JSONFormatter()
        record = _make_record(duration_ms="123.45", processing_time_ms="99")
        output = json.loads(formatter.format(record))

        assert output["duration_ms"] == 123.45
        assert isinstance(output["duration_ms"], float)
        assert output["processing_time_ms"] == 99.0
        assert isinstance(output["processing_time_ms"], float)

    def test_returns_none_for_unconvertible_numeric_fields(self):
        formatter = JSONFormatter()
        record = _make_record(http_status="not-a-number")
        output = json.loads(formatter.format(record))

        assert output["http_status"] is None

    def test_passes_through_none_for_numeric_fields(self):
        formatter = JSONFormatter()
        result = formatter._ensure_type("http_status", None)
        assert result is None

    def test_passes_through_non_numeric_fields(self):
        formatter = JSONFormatter()
        result = formatter._ensure_type("error_message", "some error")
        assert result == "some error"

    def test_sanitizes_url_with_sig_param(self):
        formatter = JSONFormatter()
        url = "https://example.com/file?sig=secrettoken123&other=value"
        sanitized = formatter._sanitize_url(url)

        assert "secrettoken123" not in sanitized
        assert "sig=[REDACTED]" in sanitized
        assert "other=value" in sanitized

    def test_sanitizes_url_with_token_param(self):
        formatter = JSONFormatter()
        url = "https://example.com/api?token=abc123"
        sanitized = formatter._sanitize_url(url)

        assert "abc123" not in sanitized
        assert "token=[REDACTED]" in sanitized

    def test_sanitizes_url_with_key_param(self):
        formatter = JSONFormatter()
        url = "https://example.com/api?key=mykey&data=ok"
        sanitized = formatter._sanitize_url(url)

        assert "mykey" not in sanitized
        assert "key=[REDACTED]" in sanitized

    def test_sanitizes_url_with_password_param(self):
        formatter = JSONFormatter()
        url = "https://example.com/api?password=hunter2"
        sanitized = formatter._sanitize_url(url)

        assert "hunter2" not in sanitized
        assert "password=[REDACTED]" in sanitized

    def test_sanitizes_url_case_insensitive(self):
        formatter = JSONFormatter()
        url = "https://example.com/api?SIG=secretvalue"
        sanitized = formatter._sanitize_url(url)

        assert "secretvalue" not in sanitized
        assert "SIG=[REDACTED]" in sanitized

    def test_sanitizes_url_with_multiple_sensitive_params(self):
        formatter = JSONFormatter()
        url = "https://example.com/api?sig=s1&token=t1&key=k1"
        sanitized = formatter._sanitize_url(url)

        assert "s1" not in sanitized
        assert "t1" not in sanitized
        assert "k1" not in sanitized

    def test_sanitizes_url_fields_in_extras(self):
        formatter = JSONFormatter()
        record = _make_record(
            download_url="https://blob.core/file?sig=secretblob"
        )
        output = json.loads(formatter.format(record))

        assert "secretblob" not in output["download_url"]
        assert "sig=[REDACTED]" in output["download_url"]

    def test_does_not_sanitize_non_url_fields(self):
        formatter = JSONFormatter()
        record = _make_record(error_message="sig=not_a_url")
        output = json.loads(formatter.format(record))

        assert output["error_message"] == "sig=not_a_url"

    def test_sanitize_value_passes_through_non_string_url_field(self):
        formatter = JSONFormatter()
        result = formatter._sanitize_value("download_url", 12345)
        assert result == 12345

    def test_includes_exception_info(self):
        formatter = JSONFormatter()
        try:
            raise ValueError("test error")
        except ValueError:
            exc_info = sys.exc_info()

        record = _make_record(exc_info=exc_info)
        output = json.loads(formatter.format(record))

        assert "exception" in output
        assert output["exception"]["type"] == "ValueError"
        assert output["exception"]["message"] == "test error"
        assert "stacktrace" in output["exception"]

    def test_omits_exception_when_not_present(self):
        formatter = JSONFormatter()
        record = _make_record()
        output = json.loads(formatter.format(record))

        assert "exception" not in output

    def test_handles_exception_with_none_parts(self):
        formatter = JSONFormatter()
        record = _make_record(exc_info=(None, None, None))
        output = json.loads(formatter.format(record))

        assert output["exception"]["type"] is None
        assert output["exception"]["message"] is None

    def test_output_is_valid_json_single_line(self):
        formatter = JSONFormatter()
        result = formatter.format(_make_record())

        assert "\n" not in result
        json.loads(result)  # Should not raise

    def test_handles_unicode_messages(self):
        formatter = JSONFormatter()
        record = _make_record(msg="Nachricht mit Umlauten: aou")
        result = formatter.format(record)
        output = json.loads(result)

        assert "aou" in output["message"]

    def test_timestamp_format_is_iso_with_milliseconds(self):
        formatter = JSONFormatter()
        output = json.loads(formatter.format(_make_record()))

        ts = output["ts"]
        # Format: YYYY-MM-DDTHH:MM:SS.mmmZ
        assert len(ts) == 24
        assert ts[10] == "T"
        assert ts[-1] == "Z"


class TestConsoleFormatter:

    @pytest.fixture(autouse=True)
    def clear_context(self):
        clear_log_context()
        yield
        clear_log_context()

    def test_formats_basic_message(self):
        formatter = ConsoleFormatter()
        formatter._use_colors = False
        record = _make_record()
        result = formatter.format(record)

        assert "INFO" in result
        assert "test message" in result

    def test_includes_domain_in_output(self):
        set_log_context(domain="claimx")
        formatter = ConsoleFormatter()
        formatter._use_colors = False
        result = formatter.format(_make_record())

        assert "[claimx]" in result

    def test_includes_stage_in_output(self):
        set_log_context(stage="download")
        formatter = ConsoleFormatter()
        formatter._use_colors = False
        result = formatter.format(_make_record())

        assert "[download]" in result

    def test_includes_batch_id(self):
        formatter = ConsoleFormatter()
        formatter._use_colors = False
        record = _make_record(batch_id="batch-42")
        result = formatter.format(record)

        assert "[batch:batch-42]" in result

    def test_includes_trace_id_truncated_to_8_chars(self):
        formatter = ConsoleFormatter()
        formatter._use_colors = False
        record = _make_record(trace_id="abcdefgh12345678")
        result = formatter.format(record)

        assert "[abcdefgh]" in result
        assert "12345678" not in result

    def test_includes_both_batch_id_and_trace_id(self):
        formatter = ConsoleFormatter()
        formatter._use_colors = False
        record = _make_record(batch_id="b-1", trace_id="trace12345678")
        result = formatter.format(record)

        assert "[batch:b-1]" in result
        assert "[trace123]" in result

    def test_uses_trace_id_from_context_when_not_on_record(self):
        set_log_context(trace_id="ctx-trace-1234")
        formatter = ConsoleFormatter()
        formatter._use_colors = False
        record = _make_record()
        result = formatter.format(record)

        assert "[ctx-trac]" in result

    def test_applies_color_codes_when_tty(self):
        formatter = ConsoleFormatter()
        formatter._use_colors = True
        record = _make_record(level=logging.ERROR)
        result = formatter.format(record)

        assert "\033[31m" in result  # Red
        assert "\033[0m" in result   # Reset

    def test_no_color_codes_when_not_tty(self):
        formatter = ConsoleFormatter()
        formatter._use_colors = False
        record = _make_record(level=logging.ERROR)
        result = formatter.format(record)

        assert "\033[" not in result

    def test_color_codes_for_all_levels(self):
        formatter = ConsoleFormatter()
        formatter._use_colors = True
        levels = {
            logging.DEBUG: "\033[36m",     # Cyan
            logging.INFO: "\033[32m",      # Green
            logging.WARNING: "\033[33m",   # Yellow
            logging.ERROR: "\033[31m",     # Red
            logging.CRITICAL: "\033[35m",  # Magenta
        }
        for level, expected_color in levels.items():
            record = _make_record(level=level)
            result = formatter.format(record)
            assert expected_color in result

    def test_plain_message_without_extras(self):
        formatter = ConsoleFormatter()
        formatter._use_colors = False
        record = _make_record(msg="simple log")
        result = formatter.format(record)

        assert result.endswith("simple log")

    def test_includes_datetime_prefix(self):
        formatter = ConsoleFormatter()
        formatter._use_colors = False
        result = formatter.format(_make_record())

        # Should start with date-time pattern
        parts = result.split(" - ")
        assert len(parts[0]) == 19  # "YYYY-MM-DD HH:MM:SS"
