"""
Unit tests for retry_utils.

Test Coverage:
    - should_send_to_dlq: permanent errors, exhausted retries, retriable cases
    - calculate_retry_timestamp: produces correct future time
    - create_retry_headers: all fields populated correctly
    - create_dlq_headers: all fields populated correctly
    - truncate_error_message: short messages, long messages, boundary
    - add_error_metadata_to_dict: modifies dict in place
    - log_retry_decision: all action types logged correctly
    - record_retry_metrics: no-op (simplified)
    - record_dlq_metrics: delegates to record_dlq_message
"""

import pytest
from datetime import UTC, datetime, timedelta
from unittest.mock import patch, Mock

from core.types import ErrorCategory
from pipeline.common.retry.retry_utils import (
    should_send_to_dlq,
    calculate_retry_timestamp,
    create_retry_headers,
    create_dlq_headers,
    truncate_error_message,
    add_error_metadata_to_dict,
    log_retry_decision,
    record_retry_metrics,
    record_dlq_metrics,
)


class TestShouldSendToDlq:
    """Test should_send_to_dlq decision logic."""

    def test_permanent_error_always_goes_to_dlq(self):
        """Permanent errors go to DLQ regardless of retry count."""
        should_dlq, reason = should_send_to_dlq(ErrorCategory.PERMANENT, retry_count=0, max_retries=5)
        assert should_dlq is True
        assert reason == "permanent"

    def test_permanent_error_with_high_retry_count(self):
        """Permanent errors go to DLQ even at high retry count."""
        should_dlq, reason = should_send_to_dlq(ErrorCategory.PERMANENT, retry_count=10, max_retries=5)
        assert should_dlq is True
        assert reason == "permanent"

    def test_exhausted_retries_go_to_dlq(self):
        """Transient errors go to DLQ when retries are exhausted."""
        should_dlq, reason = should_send_to_dlq(ErrorCategory.TRANSIENT, retry_count=5, max_retries=5)
        assert should_dlq is True
        assert reason == "exhausted"

    def test_over_max_retries_go_to_dlq(self):
        """Retries exceeding max also go to DLQ."""
        should_dlq, reason = should_send_to_dlq(ErrorCategory.TRANSIENT, retry_count=6, max_retries=5)
        assert should_dlq is True
        assert reason == "exhausted"

    def test_transient_with_retries_remaining(self):
        """Transient errors with retries remaining do not go to DLQ."""
        should_dlq, reason = should_send_to_dlq(ErrorCategory.TRANSIENT, retry_count=2, max_retries=5)
        assert should_dlq is False
        assert reason == ""

    def test_unknown_error_with_retries_remaining(self):
        """Unknown errors with retries remaining do not go to DLQ."""
        should_dlq, reason = should_send_to_dlq(ErrorCategory.UNKNOWN, retry_count=0, max_retries=3)
        assert should_dlq is False
        assert reason == ""

    def test_unknown_error_exhausted(self):
        """Unknown errors with exhausted retries go to DLQ."""
        should_dlq, reason = should_send_to_dlq(ErrorCategory.UNKNOWN, retry_count=3, max_retries=3)
        assert should_dlq is True
        assert reason == "exhausted"

    def test_auth_error_with_retries_remaining(self):
        """Auth errors with retries remaining do not go to DLQ."""
        should_dlq, reason = should_send_to_dlq(ErrorCategory.AUTH, retry_count=0, max_retries=3)
        assert should_dlq is False
        assert reason == ""

    def test_zero_max_retries_always_exhausted(self):
        """Zero max retries means first attempt is exhausted."""
        should_dlq, reason = should_send_to_dlq(ErrorCategory.TRANSIENT, retry_count=0, max_retries=0)
        assert should_dlq is True
        assert reason == "exhausted"


class TestCalculateRetryTimestamp:
    """Test calculate_retry_timestamp."""

    def test_returns_future_time(self):
        """Returned time is in the future."""
        before = datetime.now(UTC)
        result = calculate_retry_timestamp(delay_seconds=60)
        after = datetime.now(UTC)

        assert result >= before + timedelta(seconds=59)
        assert result <= after + timedelta(seconds=61)

    def test_zero_delay(self):
        """Zero delay returns approximately now."""
        before = datetime.now(UTC)
        result = calculate_retry_timestamp(delay_seconds=0)
        after = datetime.now(UTC)

        assert before <= result <= after + timedelta(seconds=1)

    def test_large_delay(self):
        """Large delay works correctly."""
        before = datetime.now(UTC)
        result = calculate_retry_timestamp(delay_seconds=3600)

        # Should be about 1 hour from now
        delta = (result - before).total_seconds()
        assert 3599 <= delta <= 3601


class TestCreateRetryHeaders:
    """Test create_retry_headers."""

    def test_returns_all_expected_fields(self):
        """Headers contain all required fields."""
        retry_at = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)

        headers = create_retry_headers(
            retry_count=2,
            retry_at=retry_at,
            delay_seconds=300,
            target_topic="verisk.downloads",
            worker_type="download",
            original_key="msg-123",
            error_category=ErrorCategory.TRANSIENT,
            domain="verisk",
        )

        assert headers["retry_count"] == "2"
        assert headers["scheduled_retry_time"] == retry_at.isoformat()
        assert headers["retry_delay_seconds"] == "300"
        assert headers["target_topic"] == "verisk.downloads"
        assert headers["worker_type"] == "download"
        assert headers["original_key"] == "msg-123"
        assert headers["error_category"] == "transient"
        assert headers["domain"] == "verisk"

    def test_all_values_are_strings(self):
        """All header values must be strings."""
        headers = create_retry_headers(
            retry_count=0,
            retry_at=datetime.now(UTC),
            delay_seconds=60,
            target_topic="topic",
            worker_type="worker",
            original_key="key",
            error_category=ErrorCategory.UNKNOWN,
            domain="claimx",
        )

        for key, value in headers.items():
            assert isinstance(value, str), f"Header '{key}' should be string, got {type(value)}"


class TestCreateDlqHeaders:
    """Test create_dlq_headers."""

    def test_returns_expected_fields(self):
        """DLQ headers contain retry_count, error_category, and failed flag."""
        headers = create_dlq_headers(
            retry_count=3,
            error_category=ErrorCategory.PERMANENT,
        )

        assert headers["retry_count"] == "3"
        assert headers["error_category"] == "permanent"
        assert headers["failed"] == "true"

    def test_all_values_are_strings(self):
        """All header values must be strings."""
        headers = create_dlq_headers(retry_count=0, error_category=ErrorCategory.TRANSIENT)

        for key, value in headers.items():
            assert isinstance(value, str), f"Header '{key}' should be string, got {type(value)}"


class TestTruncateErrorMessage:
    """Test truncate_error_message."""

    def test_short_message_unchanged(self):
        """Messages shorter than max_length are returned as-is."""
        error = Exception("Short error")
        result = truncate_error_message(error)
        assert result == "Short error"

    def test_exact_length_message_unchanged(self):
        """Message at exactly max_length is returned as-is."""
        msg = "x" * 500
        error = Exception(msg)
        result = truncate_error_message(error, max_length=500)
        assert result == msg
        assert len(result) == 500

    def test_long_message_truncated_with_ellipsis(self):
        """Messages exceeding max_length are truncated with ellipsis."""
        msg = "x" * 600
        error = Exception(msg)
        result = truncate_error_message(error, max_length=500)
        assert len(result) == 500
        assert result.endswith("...")

    def test_custom_max_length(self):
        """Custom max_length is respected."""
        msg = "a" * 100
        error = Exception(msg)
        result = truncate_error_message(error, max_length=50)
        assert len(result) == 50
        assert result.endswith("...")

    def test_empty_error_message(self):
        """Empty error message returns empty string."""
        error = Exception("")
        result = truncate_error_message(error)
        assert result == ""


class TestAddErrorMetadataToDict:
    """Test add_error_metadata_to_dict."""

    def test_adds_error_fields_to_dict(self):
        """Metadata dict is modified in place with error context."""
        metadata = {"existing_key": "existing_value"}
        error = Exception("Test error")
        retry_at = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)

        add_error_metadata_to_dict(metadata, error, ErrorCategory.TRANSIENT, retry_at)

        assert metadata["existing_key"] == "existing_value"
        assert metadata["last_error"] == "Test error"
        assert metadata["error_category"] == "transient"
        assert metadata["retry_at"] == retry_at.isoformat()

    def test_truncates_long_error_in_metadata(self):
        """Long error messages are truncated in metadata."""
        metadata = {}
        error = Exception("x" * 600)
        retry_at = datetime.now(UTC)

        add_error_metadata_to_dict(metadata, error, ErrorCategory.PERMANENT, retry_at)

        assert len(metadata["last_error"]) == 500
        assert metadata["last_error"].endswith("...")


class TestLogRetryDecision:
    """Test log_retry_decision for different actions."""

    def test_logs_dlq_permanent_as_warning(self):
        """dlq_permanent action logs a warning."""
        with patch("pipeline.common.retry.retry_utils.logger") as mock_logger:
            log_retry_decision(
                action="dlq_permanent",
                task_id="task-1",
                retry_count=0,
                error_category=ErrorCategory.PERMANENT,
                error=Exception("Bad data"),
            )
            mock_logger.warning.assert_called_once()
            call_extra = mock_logger.warning.call_args[1]["extra"]
            assert call_extra["task_id"] == "task-1"

    def test_logs_dlq_exhausted_as_warning(self):
        """dlq_exhausted action logs a warning."""
        with patch("pipeline.common.retry.retry_utils.logger") as mock_logger:
            log_retry_decision(
                action="dlq_exhausted",
                task_id="task-2",
                retry_count=5,
                error_category=ErrorCategory.TRANSIENT,
                error=Exception("Timeout"),
            )
            mock_logger.warning.assert_called_once()

    def test_logs_retry_as_info(self):
        """retry action logs at info level."""
        with patch("pipeline.common.retry.retry_utils.logger") as mock_logger:
            log_retry_decision(
                action="retry",
                task_id="task-3",
                retry_count=1,
                error_category=ErrorCategory.TRANSIENT,
                error=Exception("Timeout"),
            )
            mock_logger.info.assert_called_once()

    def test_includes_extra_context(self):
        """Extra context is included in log output."""
        with patch("pipeline.common.retry.retry_utils.logger") as mock_logger:
            log_retry_decision(
                action="retry",
                task_id="task-4",
                retry_count=1,
                error_category=ErrorCategory.TRANSIENT,
                error=Exception("Timeout"),
                extra_context={"worker_type": "download"},
            )
            call_extra = mock_logger.info.call_args[1]["extra"]
            assert call_extra["worker_type"] == "download"

    def test_unknown_action_does_not_log(self):
        """Unknown action does not produce a log message."""
        with patch("pipeline.common.retry.retry_utils.logger") as mock_logger:
            log_retry_decision(
                action="unknown_action",
                task_id="task-5",
                retry_count=0,
                error_category=ErrorCategory.UNKNOWN,
                error=Exception("test"),
            )
            mock_logger.warning.assert_not_called()
            mock_logger.info.assert_not_called()


class TestRecordRetryMetrics:
    """Test record_retry_metrics."""

    def test_is_a_noop(self):
        """record_retry_metrics is a no-op after metrics simplification."""
        # Should not raise
        record_retry_metrics(
            domain="verisk",
            error_category=ErrorCategory.TRANSIENT,
            delay_seconds=300,
            worker_type="download",
        )


class TestRecordDlqMetrics:
    """Test record_dlq_metrics."""

    def test_delegates_to_record_dlq_message(self):
        """record_dlq_metrics calls record_dlq_message with correct args."""
        with patch("pipeline.common.retry.retry_utils.record_dlq_message") as mock_record:
            record_dlq_metrics(domain="verisk", reason="permanent")
            mock_record.assert_called_once_with(domain="verisk", reason="permanent")

    def test_accepts_optional_error_category(self):
        """record_dlq_metrics accepts optional error_category."""
        with patch("pipeline.common.retry.retry_utils.record_dlq_message") as mock_record:
            record_dlq_metrics(
                domain="claimx",
                reason="exhausted",
                error_category=ErrorCategory.TRANSIENT,
            )
            mock_record.assert_called_once_with(domain="claimx", reason="exhausted")
