"""
Tests for retry logic with exception-aware handling.

Test Coverage:
    - RetryConfig initialization and validation
    - Exponential backoff with jitter
    - Server-provided retry_after handling
    - Exception classification and retry decisions
    - Auth error detection and callbacks
    - Decorator behavior with various error types
    - Statistics tracking
"""

import time
from unittest.mock import Mock, patch, call

import pytest

from core.errors.exceptions import (
    ErrorCategory,
    PipelineError,
    AuthError,
    TransientError,
    ThrottlingError,
    PermanentError,
)
from core.resilience.retry import (
    RetryConfig,
    RetryStats,
    with_retry,
    DEFAULT_RETRY,
    AUTH_RETRY,
)


class TestRetryConfig:
    """Tests for RetryConfig dataclass."""

    def test_default_values(self):
        """RetryConfig uses sensible defaults."""
        config = RetryConfig()
        assert config.max_attempts == 3
        assert config.base_delay == 1.0
        assert config.max_delay == 30.0
        assert config.exponential_base == 2.0
        assert config.respect_permanent is True
        assert config.respect_retry_after is True
        assert config.always_retry == set()
        assert config.never_retry == set()

    def test_custom_values(self):
        """Can override all config values."""
        config = RetryConfig(
            max_attempts=5,
            base_delay=2.0,
            max_delay=60.0,
            exponential_base=3.0,
            respect_permanent=False,
            respect_retry_after=False,
        )
        assert config.max_attempts == 5
        assert config.base_delay == 2.0
        assert config.max_delay == 60.0
        assert config.exponential_base == 3.0
        assert config.respect_permanent is False
        assert config.respect_retry_after is False

    def test_post_init_type_conversion(self):
        """Post init converts types from env vars/YAML."""
        config = RetryConfig(
            max_attempts="5",
            base_delay="2.5",
            max_delay="100",
            exponential_base="3",
        )
        assert config.max_attempts == 5
        assert config.base_delay == 2.5
        assert config.max_delay == 100.0
        assert config.exponential_base == 3.0

    def test_get_delay_exponential_backoff(self):
        """Delay grows exponentially with jitter."""
        config = RetryConfig(base_delay=1.0, exponential_base=2.0, max_delay=30.0)

        # Attempt 0: base_delay=1.0, jitter between [0.5, 1.0]
        delay_0 = config.get_delay(0)
        assert 0.5 <= delay_0 <= 1.0

        # Attempt 1: base_delay=2.0, jitter between [1.0, 2.0]
        delay_1 = config.get_delay(1)
        assert 1.0 <= delay_1 <= 2.0

        # Attempt 2: base_delay=4.0, jitter between [2.0, 4.0]
        delay_2 = config.get_delay(2)
        assert 2.0 <= delay_2 <= 4.0

    def test_get_delay_respects_max(self):
        """Delay never exceeds max_delay."""
        config = RetryConfig(base_delay=10.0, exponential_base=10.0, max_delay=5.0)

        # Even with large base and exponential, cap at max
        delay = config.get_delay(10)
        assert delay <= 5.0

    def test_get_delay_with_retry_after(self):
        """Uses retry_after from ThrottlingError when available."""
        config = RetryConfig(respect_retry_after=True, max_delay=30.0)
        error = ThrottlingError("Rate limited", retry_after=5.0)

        delay = config.get_delay(0, error)
        assert delay == 5.0

    def test_get_delay_retry_after_capped_by_max(self):
        """retry_after is capped by max_delay."""
        config = RetryConfig(respect_retry_after=True, max_delay=10.0)
        error = ThrottlingError("Rate limited", retry_after=20.0)

        delay = config.get_delay(0, error)
        assert delay == 10.0

    def test_get_delay_ignores_retry_after_when_disabled(self):
        """Falls back to exponential when respect_retry_after=False."""
        config = RetryConfig(respect_retry_after=False, base_delay=1.0)
        error = ThrottlingError("Rate limited", retry_after=100.0)

        delay = config.get_delay(0, error)
        assert 0.5 <= delay <= 1.0  # Uses exponential jitter, not retry_after

    def test_should_retry_max_attempts_reached(self):
        """Does not retry when max attempts reached."""
        config = RetryConfig(max_attempts=3)
        error = TransientError("Temporary")

        # Attempt 2 (0-indexed) is the last attempt for max_attempts=3
        assert config.should_retry(error, 2) is False

    def test_should_retry_never_retry_list(self):
        """Never retries exceptions in never_retry set."""
        config = RetryConfig(never_retry={ValueError})
        error = ValueError("Bad value")

        assert config.should_retry(error, 0) is False

    def test_should_retry_always_retry_list(self):
        """Always retries exceptions in always_retry set."""
        config = RetryConfig(always_retry={ValueError}, max_attempts=3)
        error = ValueError("Bad value")

        assert config.should_retry(error, 0) is True
        assert config.should_retry(error, 1) is True

    def test_should_retry_permanent_error_respected(self):
        """Does not retry permanent errors when respect_permanent=True."""
        config = RetryConfig(respect_permanent=True)
        error = PermanentError("Not found")

        assert config.should_retry(error, 0) is False

    @patch("core.errors.exceptions.classify_exception")
    def test_should_retry_respects_classification(self, mock_classify):
        """Respects exception classification for non-PipelineError exceptions."""
        # Even with respect_permanent=False, PERMANENT isn't in the retryable set
        # so it still returns False. respect_permanent only affects PipelineError.
        mock_classify.return_value = ErrorCategory.PERMANENT
        config = RetryConfig(respect_permanent=False, max_attempts=3)
        error = RuntimeError("Some error")

        # PERMANENT is not in (TRANSIENT, AUTH, UNKNOWN) so not retried
        assert config.should_retry(error, 0) is False

        # But UNKNOWN would be retried
        mock_classify.return_value = ErrorCategory.UNKNOWN
        assert config.should_retry(error, 0) is True

    def test_should_retry_transient_error(self):
        """Retries transient errors."""
        config = RetryConfig(max_attempts=3)
        error = TransientError("Temporary")

        assert config.should_retry(error, 0) is True
        assert config.should_retry(error, 1) is True

    def test_should_retry_auth_error(self):
        """Retries auth errors."""
        config = RetryConfig(max_attempts=3)
        error = AuthError("Token expired")

        assert config.should_retry(error, 0) is True

    @patch("core.errors.exceptions.classify_exception")
    def test_should_retry_unknown_exception(self, mock_classify):
        """Retries unknown exceptions by default."""
        mock_classify.return_value = ErrorCategory.UNKNOWN
        config = RetryConfig(max_attempts=3)
        error = RuntimeError("Unknown error")

        assert config.should_retry(error, 0) is True


class TestRetryStats:
    """Tests for RetryStats dataclass."""

    def test_default_values(self):
        """RetryStats initializes with defaults."""
        stats = RetryStats()
        assert stats.attempts == 0
        assert stats.total_delay == 0.0
        assert stats.final_error is None
        assert stats.success is False

    def test_retried_property(self):
        """retried is True when attempts > 1."""
        stats = RetryStats(attempts=1)
        assert stats.retried is False

        stats = RetryStats(attempts=2)
        assert stats.retried is True


class TestWithRetryDecorator:
    """Tests for with_retry decorator."""

    @patch("core.resilience.retry.time.sleep")
    def test_success_on_first_attempt(self, mock_sleep):
        """Succeeds immediately without retries."""

        @with_retry()
        def func():
            return "success"

        result = func()
        assert result == "success"
        mock_sleep.assert_not_called()

    @patch("core.resilience.retry.time.sleep")
    def test_success_after_transient_error(self, mock_sleep):
        """Retries transient error and succeeds."""
        call_count = 0

        @with_retry(config=RetryConfig(max_attempts=3, base_delay=0.1))
        def func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise TransientError("Temporary")
            return "success"

        result = func()
        assert result == "success"
        assert call_count == 2
        assert mock_sleep.call_count == 1

    @patch("core.resilience.retry.time.sleep")
    def test_permanent_error_no_retry(self, mock_sleep):
        """Does not retry permanent errors."""

        @with_retry(config=RetryConfig(max_attempts=3))
        def func():
            raise PermanentError("Not found")

        with pytest.raises(PermanentError):
            func()

        mock_sleep.assert_not_called()

    @patch("core.resilience.retry.time.sleep")
    def test_max_retries_exhausted(self, mock_sleep):
        """Raises after max retries exhausted."""

        @with_retry(config=RetryConfig(max_attempts=3, base_delay=0.1))
        def func():
            raise TransientError("Always fails")

        with pytest.raises(TransientError):
            func()

        assert mock_sleep.call_count == 2  # 3 attempts = 2 sleeps

    @patch("core.resilience.retry.time.sleep")
    def test_auth_error_callback(self, mock_sleep):
        """Calls on_auth_error callback for auth errors."""
        auth_callback = Mock()
        call_count = 0

        @with_retry(
            config=RetryConfig(max_attempts=3, base_delay=0.1),
            on_auth_error=auth_callback,
        )
        def func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise AuthError("Token expired")
            return "success"

        result = func()
        assert result == "success"
        auth_callback.assert_called_once()

    @patch("core.resilience.retry.time.sleep")
    def test_on_retry_callback(self, mock_sleep):
        """Calls on_retry callback before each retry."""
        retry_callback = Mock()
        call_count = 0

        @with_retry(
            config=RetryConfig(max_attempts=3, base_delay=0.1),
            on_retry=retry_callback,
        )
        def func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise TransientError("Temporary")
            return "success"

        result = func()
        assert result == "success"
        assert retry_callback.call_count == 1
        # Verify callback receives (error, attempt, delay)
        args = retry_callback.call_args[0]
        assert isinstance(args[0], TransientError)
        assert args[1] == 0  # First attempt (0-indexed)
        assert isinstance(args[2], float)  # Delay

    @patch("core.resilience.retry.time.sleep")
    def test_on_retry_callback_error_does_not_stop_retry(self, mock_sleep):
        """Retry continues even if callback raises."""

        def bad_callback(error, attempt, delay):
            raise RuntimeError("Callback failed")

        call_count = 0

        @with_retry(
            config=RetryConfig(max_attempts=3, base_delay=0.1),
            on_retry=bad_callback,
        )
        def func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise TransientError("Temporary")
            return "success"

        result = func()
        assert result == "success"

    @patch("core.errors.exceptions.wrap_exception")
    @patch("core.resilience.retry.time.sleep")
    def test_wrap_errors_enabled(self, mock_sleep, mock_wrap):
        """Wraps unknown exceptions when wrap_errors=True."""
        mock_wrap.return_value = PermanentError("Wrapped")

        @with_retry(wrap_errors=True)
        def func():
            raise ValueError("Unknown error")

        with pytest.raises(PermanentError):
            func()

        mock_wrap.assert_called_once()

    @patch("core.resilience.retry.time.sleep")
    def test_wrap_errors_disabled(self, mock_sleep):
        """Does not wrap exceptions when wrap_errors=False."""

        @with_retry(wrap_errors=False)
        def func():
            raise ValueError("Unknown error")

        with pytest.raises(ValueError):
            func()

    @patch("core.resilience.retry.time.sleep")
    def test_throttling_error_uses_retry_after(self, mock_sleep):
        """Uses server-provided retry_after for ThrottlingError."""
        call_count = 0

        @with_retry(
            config=RetryConfig(max_attempts=3, respect_retry_after=True, max_delay=10.0)
        )
        def func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ThrottlingError("Rate limited", retry_after=2.5)
            return "success"

        result = func()
        assert result == "success"
        # Should have slept for retry_after=2.5
        mock_sleep.assert_called_once_with(2.5)

    def test_default_config_used_when_none(self):
        """Uses DEFAULT_RETRY when config not provided."""

        @with_retry()
        def func():
            return "success"

        # Should not raise
        result = func()
        assert result == "success"


class TestStandardConfigs:
    """Tests for standard retry configurations."""

    def test_default_retry_config(self):
        """DEFAULT_RETRY has sensible values."""
        assert DEFAULT_RETRY.max_attempts == 3
        assert DEFAULT_RETRY.base_delay == 1.0

    def test_auth_retry_config(self):
        """AUTH_RETRY is more aggressive."""
        assert AUTH_RETRY.max_attempts == 2
        assert AUTH_RETRY.base_delay == 0.5
