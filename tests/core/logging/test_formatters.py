"""Tests for logging formatters."""

import json
import logging
from datetime import datetime, timezone

import pytest

from core.logging.context import clear_log_context, set_log_context
from core.logging.formatters import ConsoleFormatter, JSONFormatter


class TestJSONFormatter:
    """Tests for JSONFormatter."""

    @pytest.fixture(autouse=True)
    def clear_context(self):
        """Clear log context before each test."""
        clear_log_context()
        yield
        clear_log_context()

    def test_basic_json_format(self):
        """Test basic JSON log formatting."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)
        log_entry = json.loads(output)

        # Verify basic fields
        assert log_entry["level"] == "INFO"
        assert log_entry["logger"] == "test.logger"
        assert log_entry["msg"] == "Test message"
        assert "ts" in log_entry
        # Verify timestamp format
        datetime.fromisoformat(log_entry["ts"].replace("Z", "+00:00"))

    def test_context_injection(self):
        """Test that context variables are injected into JSON logs."""
        formatter = JSONFormatter()
        set_log_context(
            domain="kafka",
            stage="consumer",
            cycle_id="c-20250115-120000-abcd",
            worker_id="worker-1",
        )

        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)
        log_entry = json.loads(output)

        # Verify context fields are present
        assert log_entry["domain"] == "kafka"
        assert log_entry["stage"] == "consumer"
        assert log_entry["cycle_id"] == "c-20250115-120000-abcd"
        assert log_entry["worker_id"] == "worker-1"

    def test_extra_fields_extraction(self):
        """Test that extra fields are extracted from LogRecord."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="Processing batch",
            args=(),
            exc_info=None,
        )
        # Add extra fields
        record.batch_size = 100
        record.records_processed = 95
        record.records_failed = 5
        record.duration_ms = 1234

        output = formatter.format(record)
        log_entry = json.loads(output)

        # Verify extra fields
        assert log_entry["batch_size"] == 100
        assert log_entry["records_processed"] == 95
        assert log_entry["records_failed"] == 5
        assert log_entry["duration_ms"] == 1234

    def test_url_sanitization(self):
        """Test that sensitive URL parameters are sanitized."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="Downloading file",
            args=(),
            exc_info=None,
        )
        # Add URL with sensitive params
        record.download_url = "https://example.com/file?sig=secret123&token=abc&file=data.csv"

        output = formatter.format(record)
        log_entry = json.loads(output)

        # Verify sensitive params are redacted
        assert "secret123" not in log_entry["download_url"]
        assert "abc" not in log_entry["download_url"]
        assert "[REDACTED]" in log_entry["download_url"]
        assert "data.csv" in log_entry["download_url"]

    def test_exception_info(self):
        """Test that exception info is included in JSON logs."""
        formatter = JSONFormatter()
        try:
            raise ValueError("Test error")
        except ValueError:
            import sys

            exc_info = sys.exc_info()

        record = logging.LogRecord(
            name="test.logger",
            level=logging.ERROR,
            pathname="test.py",
            lineno=42,
            msg="Error occurred",
            args=(),
            exc_info=exc_info,
        )

        output = formatter.format(record)
        log_entry = json.loads(output)

        # Verify exception is present
        assert "exception" in log_entry
        assert "ValueError" in log_entry["exception"]
        assert "Test error" in log_entry["exception"]

    def test_source_location_for_errors(self):
        """Test that file location is included for ERROR level logs."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test.logger",
            level=logging.ERROR,
            pathname="test.py",
            lineno=42,
            msg="Error message",
            args=(),
            exc_info=None,
        )
        record.filename = "test.py"

        output = formatter.format(record)
        log_entry = json.loads(output)

        # Verify file location is present
        assert "file" in log_entry
        assert log_entry["file"] == "test.py:42"

    def test_no_source_location_for_info(self):
        """Test that file location is NOT included for INFO level logs."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="Info message",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)
        log_entry = json.loads(output)

        # Verify file location is NOT present
        assert "file" not in log_entry

    def test_kafka_fields(self):
        """Test that Kafka-specific fields are extracted."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="Message consumed",
            args=(),
            exc_info=None,
        )
        # Add Kafka fields
        record.kafka_topic = "events.attachment"
        record.kafka_partition = 2
        record.kafka_offset = 12345

        output = formatter.format(record)
        log_entry = json.loads(output)

        # Verify Kafka fields
        assert log_entry["kafka_topic"] == "events.attachment"
        assert log_entry["kafka_partition"] == 2
        assert log_entry["kafka_offset"] == 12345


class TestConsoleFormatter:
    """Tests for ConsoleFormatter."""

    @pytest.fixture(autouse=True)
    def clear_context(self):
        """Clear log context before each test."""
        clear_log_context()
        yield
        clear_log_context()

    def test_basic_console_format(self):
        """Test basic console formatting."""
        formatter = ConsoleFormatter()
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)

        # Verify basic format (timestamp - level - message)
        assert "INFO" in output
        assert "Test message" in output
        assert "-" in output

    def test_context_in_console_format(self):
        """Test that context is included in console output."""
        formatter = ConsoleFormatter()
        set_log_context(domain="kafka", stage="consumer")

        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)

        # Verify context is present
        assert "[kafka]" in output
        assert "[consumer]" in output

    def test_trace_id_in_console_format(self):
        """Test that trace_id is included when present."""
        formatter = ConsoleFormatter()
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        record.trace_id = "trace-1234567890abcdef"

        output = formatter.format(record)

        # Verify trace_id is present (truncated to 8 chars)
        assert "[trace-12]" in output
        assert "Test message" in output


class TestURLSanitization:
    """Tests for URL sanitization in JSONFormatter."""

    @pytest.fixture
    def formatter(self):
        """Create formatter instance."""
        return JSONFormatter()

    def test_sanitize_signature_param(self, formatter):
        """Test sanitization of 'sig' parameter."""
        url = "https://storage.blob.core.windows.net/container/file?sig=secret123"
        sanitized = formatter._sanitize_url(url)
        assert "secret123" not in sanitized
        assert "sig=[REDACTED]" in sanitized

    def test_sanitize_token_param(self, formatter):
        """Test sanitization of 'token' parameter."""
        url = "https://api.example.com/data?token=abc123&format=json"
        sanitized = formatter._sanitize_url(url)
        assert "abc123" not in sanitized
        assert "token=[REDACTED]" in sanitized
        assert "format=json" in sanitized

    def test_sanitize_multiple_params(self, formatter):
        """Test sanitization of multiple sensitive parameters."""
        url = "https://example.com/file?key=k1&sig=s1&token=t1&file=data.csv"
        sanitized = formatter._sanitize_url(url)
        assert "k1" not in sanitized
        assert "s1" not in sanitized
        assert "t1" not in sanitized
        assert "key=[REDACTED]" in sanitized
        assert "sig=[REDACTED]" in sanitized
        assert "token=[REDACTED]" in sanitized
        assert "file=data.csv" in sanitized

    def test_case_insensitive_sanitization(self, formatter):
        """Test that sanitization is case-insensitive."""
        url = "https://example.com/file?SIG=s1&Token=t1"
        sanitized = formatter._sanitize_url(url)
        assert "s1" not in sanitized
        assert "t1" not in sanitized
        assert "SIG=[REDACTED]" in sanitized or "sig=[REDACTED]" in sanitized

    def test_no_sanitization_needed(self, formatter):
        """Test that safe URLs are not modified."""
        url = "https://example.com/file?format=json&limit=100"
        sanitized = formatter._sanitize_url(url)
        assert sanitized == url
