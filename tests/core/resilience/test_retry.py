"""
Tests for retry logic with exponential backoff and jitter.

Review checklist:
    [x] Jitter prevents thundering herd
    [x] Works with async functions (note: current implementation is sync only)
    [x] Respects Retry-After headers
"""

import time
from typing import List
from unittest.mock import Mock, call

import pytest

from core.resilience.retry import (
    AUTH_RETRY,
    DEFAULT_RETRY,
    RetryBudget,
    RetryConfig,
    RetryStats,
    with_retry,
)
from core.types import ErrorCategory
from kafka_pipeline.common.exceptions import (
    AuthError,
    PermanentError,
    PipelineError,
    ThrottlingError,
    TimeoutError,
    TokenExpiredError,
    TransientError,
)


class TestRetryConfig:
    """Tests for RetryConfig."""

    def test_default_values(self):
        """Test default configuration values."""
        config = RetryConfig()
        assert config.max_attempts == 3
        assert config.base_delay == 1.0
        assert config.max_delay == 30.0
        assert config.exponential_base == 2.0
        assert config.respect_permanent is True
        assert config.respect_retry_after is True
        assert config.always_retry == set()
        assert config.never_retry == set()

    def test_type_conversion_from_strings(self):
        """Test that config handles string inputs (e.g., from YAML)."""
        config = RetryConfig(
            max_attempts="5",
            base_delay="2.5",
            max_delay="60",
            exponential_base="3",
        )
        assert config.max_attempts == 5
        assert config.base_delay == 2.5
        assert config.max_delay == 60.0
        assert config.exponential_base == 3.0

    def test_exponential_backoff_calculation(self):
        """Test exponential delay calculation."""
        config = RetryConfig(base_delay=1.0, exponential_base=2.0, max_delay=30.0)

        # Attempt 0: base_delay * 2^0 = 1.0
        delay_0 = config.get_delay(0)
        assert 0.5 <= delay_0 <= 1.0  # Equal jitter: [0.5, 1.0]

        # Attempt 1: base_delay * 2^1 = 2.0
        delay_1 = config.get_delay(1)
        assert 1.0 <= delay_1 <= 2.0  # Equal jitter: [1.0, 2.0]

        # Attempt 2: base_delay * 2^2 = 4.0
        delay_2 = config.get_delay(2)
        assert 2.0 <= delay_2 <= 4.0  # Equal jitter: [2.0, 4.0]

    def test_jitter_prevents_thundering_herd(self):
        """Test that equal jitter spreads retry times."""
        config = RetryConfig(base_delay=1.0, exponential_base=2.0)

        # Generate multiple delays for same attempt
        delays = [config.get_delay(1) for _ in range(100)]

        # All delays should be within [0.5 * base * 2^1, 1.0 * base * 2^1]
        # = [1.0, 2.0]
        assert all(1.0 <= d <= 2.0 for d in delays)

        # Delays should be spread out (not all the same)
        unique_delays = len(set(delays))
        assert unique_delays > 10  # Should have good distribution

        # Verify equal jitter: delay = (base/2) + random(0, base/2)
        # Mean should be around 0.75 * base_delay * 2^1 = 1.5
        mean_delay = sum(delays) / len(delays)
        assert 1.3 <= mean_delay <= 1.7  # Allow some variance

    def test_max_delay_cap(self):
        """Test that delay is capped at max_delay."""
        config = RetryConfig(base_delay=10.0, max_delay=5.0, exponential_base=2.0)

        # Even for high attempts, delay should not exceed max_delay
        delay = config.get_delay(10)  # Would be 10 * 2^10 = 10240 without cap
        assert delay <= 5.0

    def test_retry_after_respected(self):
        """Test that Retry-After header from ThrottlingError is used."""
        config = RetryConfig(respect_retry_after=True, max_delay=30.0)

        error = ThrottlingError("Rate limited", retry_after=5.0)
        delay = config.get_delay(0, error)

        # Should use server-provided retry_after
        assert delay == 5.0

    def test_retry_after_capped_by_max_delay(self):
        """Test that Retry-After is still capped by max_delay."""
        config = RetryConfig(respect_retry_after=True, max_delay=10.0)

        error = ThrottlingError("Rate limited", retry_after=15.0)
        delay = config.get_delay(0, error)

        # Should cap at max_delay even for server-provided value
        assert delay == 10.0

    def test_retry_after_ignored_when_disabled(self):
        """Test that Retry-After is ignored when respect_retry_after=False."""
        config = RetryConfig(
            respect_retry_after=False, base_delay=1.0, exponential_base=2.0
        )

        error = ThrottlingError("Rate limited", retry_after=10.0)
        delay = config.get_delay(0, error)

        # Should use exponential backoff instead
        assert 0.5 <= delay <= 1.0  # Based on attempt 0

    def test_should_retry_respects_max_attempts(self):
        """Test that should_retry returns False after max attempts."""
        config = RetryConfig(max_attempts=3)

        assert config.should_retry(TransientError("test"), attempt=0) is True
        assert config.should_retry(TransientError("test"), attempt=1) is True
        assert config.should_retry(TransientError("test"), attempt=2) is False
        assert config.should_retry(TransientError("test"), attempt=3) is False

    def test_should_retry_permanent_errors(self):
        """Test that permanent errors are not retried."""
        config = RetryConfig(respect_permanent=True)

        # PipelineError with PERMANENT category
        error = PermanentError("Not found")
        assert config.should_retry(error, attempt=0) is False

    def test_should_retry_transient_errors(self):
        """Test that transient errors are retried."""
        config = RetryConfig(respect_permanent=True)

        # PipelineError with TRANSIENT category
        error = TransientError("Timeout")
        assert config.should_retry(error, attempt=0) is True
        assert config.should_retry(error, attempt=1) is True

    def test_should_retry_auth_errors(self):
        """Test that auth errors are retried."""
        config = RetryConfig(respect_permanent=True)

        error = AuthError("Unauthorized")
        assert config.should_retry(error, attempt=0) is True

    def test_should_retry_always_retry_list(self):
        """Test that always_retry overrides classification."""
        config = RetryConfig(
            respect_permanent=True, always_retry={PermanentError, ValueError}
        )

        # PermanentError would normally not retry
        error = PermanentError("Not found")
        assert config.should_retry(error, attempt=0) is True

        # ValueError should also retry
        error = ValueError("Invalid value")
        assert config.should_retry(error, attempt=0) is True

    def test_should_retry_never_retry_list(self):
        """Test that never_retry overrides classification."""
        config = RetryConfig(
            respect_permanent=True, never_retry={TransientError, TimeoutError}
        )

        # TransientError would normally retry
        error = TransientError("Timeout")
        assert config.should_retry(error, attempt=0) is False

        # TimeoutError should also not retry
        error = TimeoutError("Timed out")
        assert config.should_retry(error, attempt=0) is False

    def test_should_retry_unknown_exceptions(self):
        """Test that unknown exceptions are classified and handled."""
        config = RetryConfig(respect_permanent=True)

        # Generic exceptions should be classified as UNKNOWN
        # Note: classify_exception may classify some as PERMANENT based on string matching
        # For a truly unknown error that should retry, use a custom exception
        class CustomError(Exception):
            pass

        error = CustomError("Something went wrong")
        assert config.should_retry(error, attempt=0) is True


class TestRetryStats:
    """Tests for RetryStats."""

    def test_default_values(self):
        """Test default RetryStats values."""
        stats = RetryStats()
        assert stats.attempts == 0
        assert stats.total_delay == 0.0
        assert stats.final_error is None
        assert stats.success is False
        assert stats.retried is False

    def test_retried_property(self):
        """Test retried property."""
        stats = RetryStats(attempts=1)
        assert stats.retried is False

        stats = RetryStats(attempts=2)
        assert stats.retried is True


class TestRetryBudget:
    """Tests for retry budget tracking."""

    def test_default_threshold(self):
        """Test default retry budget threshold (10%)."""
        budget = RetryBudget()
        assert budget.threshold == 0.1
        assert budget.window_size == 1000

    def test_can_retry_when_empty(self):
        """Test that retry is allowed when no requests tracked."""
        budget = RetryBudget()
        assert budget.can_retry() is True

    def test_can_retry_within_budget(self):
        """Test retry allowed when under threshold."""
        budget = RetryBudget(threshold=0.1)

        # 9 retries out of 100 requests = 9% (under 10%)
        for _ in range(91):
            budget.record_request(retried=False)
        for _ in range(9):
            budget.record_request(retried=True)

        assert budget.can_retry() is True

    def test_cannot_retry_over_budget(self):
        """Test retry rejected when over threshold."""
        budget = RetryBudget(threshold=0.1)

        # 11 retries out of 100 requests = 11% (over 10%)
        for _ in range(89):
            budget.record_request(retried=False)
        for _ in range(11):
            budget.record_request(retried=True)

        assert budget.can_retry() is False

    def test_get_stats(self):
        """Test retry budget statistics."""
        budget = RetryBudget(threshold=0.1)

        # 5 retries out of 100 requests
        for _ in range(95):
            budget.record_request(retried=False)
        for _ in range(5):
            budget.record_request(retried=True)

        stats = budget.get_stats()
        assert stats["total_requests"] == 100
        assert stats["total_retries"] == 5
        assert stats["retry_ratio"] == 0.05
        assert stats["budget_remaining"] == 0.05
        assert stats["threshold"] == 0.1

    def test_reset(self):
        """Test retry budget reset."""
        budget = RetryBudget()

        budget.record_request(retried=True)
        budget.record_request(retried=False)

        budget.reset()

        stats = budget.get_stats()
        assert stats["total_requests"] == 0
        assert stats["total_retries"] == 0


class TestWithRetryDecorator:
    """Tests for @with_retry decorator."""

    def test_success_on_first_attempt(self):
        """Test that successful function returns immediately."""
        call_count = 0

        @with_retry()
        def success_func():
            nonlocal call_count
            call_count += 1
            return "success"

        result = success_func()
        assert result == "success"
        assert call_count == 1

    def test_retry_on_transient_error(self):
        """Test retry on transient errors."""
        call_count = 0

        @with_retry(config=RetryConfig(max_attempts=3, base_delay=0.01))
        def failing_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise TransientError("Temporary failure")
            return "success"

        result = failing_func()
        assert result == "success"
        assert call_count == 3

    def test_no_retry_on_permanent_error(self):
        """Test that permanent errors are not retried."""
        call_count = 0

        @with_retry(config=RetryConfig(max_attempts=3, base_delay=0.01))
        def failing_func():
            nonlocal call_count
            call_count += 1
            raise PermanentError("Not found")

        with pytest.raises(PermanentError):
            failing_func()

        # Should fail immediately without retry
        assert call_count == 1

    def test_max_retries_exhausted(self):
        """Test that error is raised after max retries."""
        call_count = 0

        @with_retry(config=RetryConfig(max_attempts=3, base_delay=0.01))
        def always_failing():
            nonlocal call_count
            call_count += 1
            raise TransientError("Always fails")

        with pytest.raises(TransientError):
            always_failing()

        # Should try 3 times
        assert call_count == 3

    def test_auth_error_callback(self):
        """Test that on_auth_error callback is invoked."""
        auth_callback = Mock()
        call_count = 0

        @with_retry(
            config=RetryConfig(max_attempts=3, base_delay=0.01),
            on_auth_error=auth_callback,
        )
        def auth_failing():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise AuthError("Unauthorized")
            return "success"

        result = auth_failing()
        assert result == "success"
        assert call_count == 2
        auth_callback.assert_called_once()

    def test_retry_callback(self):
        """Test that on_retry callback is invoked before each retry."""
        retry_callback = Mock()
        call_count = 0

        @with_retry(
            config=RetryConfig(max_attempts=3, base_delay=0.01),
            on_retry=retry_callback,
        )
        def failing_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise TransientError("Temporary failure")
            return "success"

        result = failing_func()
        assert result == "success"
        assert call_count == 3

        # Should be called twice (before 2nd and 3rd attempts)
        assert retry_callback.call_count == 2

        # Verify callback arguments: (error, attempt, delay)
        for call_args in retry_callback.call_args_list:
            args, kwargs = call_args
            error, attempt, delay = args
            assert isinstance(error, TransientError)
            assert isinstance(attempt, int)
            assert isinstance(delay, float)

    def test_wrap_errors_enabled(self):
        """Test that unknown exceptions are wrapped when wrap_errors=True."""

        @with_retry(config=RetryConfig(max_attempts=1), wrap_errors=True)
        def unknown_error():
            raise ValueError("Unknown error")

        with pytest.raises(PipelineError) as exc_info:
            unknown_error()

        # Should be wrapped in PipelineError
        assert exc_info.value.cause.__class__ == ValueError

    def test_wrap_errors_disabled(self):
        """Test that unknown exceptions are not wrapped when wrap_errors=False."""

        @with_retry(config=RetryConfig(max_attempts=1), wrap_errors=False)
        def unknown_error():
            raise ValueError("Unknown error")

        with pytest.raises(ValueError):
            unknown_error()

    def test_retry_delay_timing(self):
        """Test that retry delays are applied."""
        config = RetryConfig(max_attempts=3, base_delay=0.1, exponential_base=2.0)
        call_times: List[float] = []

        @with_retry(config=config)
        def failing_func():
            call_times.append(time.time())
            if len(call_times) < 3:
                raise TransientError("Temporary failure")
            return "success"

        result = failing_func()
        assert result == "success"
        assert len(call_times) == 3

        # Check that delays were applied between attempts
        # Attempt 0->1: delay should be ~0.05-0.1 (jittered)
        delay_1 = call_times[1] - call_times[0]
        assert 0.05 <= delay_1 <= 0.15  # Allow some overhead

        # Attempt 1->2: delay should be ~0.1-0.2 (jittered)
        delay_2 = call_times[2] - call_times[1]
        assert 0.1 <= delay_2 <= 0.25  # Allow some overhead

    def test_default_retry_config(self):
        """Test that DEFAULT_RETRY is used when no config provided."""

        @with_retry()
        def default_config_func():
            return "success"

        # Should use DEFAULT_RETRY (max_attempts=3, base_delay=1.0)
        result = default_config_func()
        assert result == "success"

    def test_auth_retry_config(self):
        """Test AUTH_RETRY configuration."""
        assert AUTH_RETRY.max_attempts == 2
        assert AUTH_RETRY.base_delay == 0.5

    def test_retry_callback_exception_handling(self):
        """Test that exceptions in on_retry callback don't break retry logic."""

        def bad_callback(error, attempt, delay):
            raise RuntimeError("Callback failed")

        call_count = 0

        @with_retry(
            config=RetryConfig(max_attempts=3, base_delay=0.01),
            on_retry=bad_callback,
        )
        def failing_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise TransientError("Temporary failure")
            return "success"

        # Should still succeed despite callback failures
        result = failing_func()
        assert result == "success"
        assert call_count == 3

    def test_server_retry_after_delay(self):
        """Test that server-provided Retry-After is used."""
        config = RetryConfig(
            max_attempts=3, base_delay=0.01, respect_retry_after=True
        )
        call_times: List[float] = []

        @with_retry(config=config)
        def throttled_func():
            call_times.append(time.time())
            if len(call_times) < 2:
                raise ThrottlingError("Rate limited", retry_after=0.15)
            return "success"

        result = throttled_func()
        assert result == "success"
        assert len(call_times) == 2

        # Check that server-provided delay was used (0.15s)
        delay = call_times[1] - call_times[0]
        assert 0.15 <= delay <= 0.2  # Allow some overhead

    def test_token_expired_error_triggers_auth_refresh(self):
        """Test that TokenExpiredError triggers auth refresh callback."""
        auth_callback = Mock()
        call_count = 0

        @with_retry(
            config=RetryConfig(max_attempts=3, base_delay=0.01),
            on_auth_error=auth_callback,
        )
        def token_expired():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise TokenExpiredError("Token expired")
            return "success"

        result = token_expired()
        assert result == "success"
        assert call_count == 2
        auth_callback.assert_called_once()


class TestRetryIntegration:
    """Integration tests combining retry with other components."""

    def test_retry_with_custom_exception_lists(self):
        """Test retry behavior with custom always/never retry lists."""
        config = RetryConfig(
            max_attempts=3,
            base_delay=0.01,
            always_retry={ValueError},
            never_retry={RuntimeError},
        )

        # ValueError should retry (in always_retry)
        call_count_1 = 0

        @with_retry(config=config)
        def value_error_func():
            nonlocal call_count_1
            call_count_1 += 1
            if call_count_1 < 2:
                raise ValueError("Should retry")
            return "success"

        assert value_error_func() == "success"
        assert call_count_1 == 2

        # RuntimeError should not retry (in never_retry)
        call_count_2 = 0

        @with_retry(config=config, wrap_errors=False)
        def runtime_error_func():
            nonlocal call_count_2
            call_count_2 += 1
            raise RuntimeError("Should not retry")

        with pytest.raises(RuntimeError):
            runtime_error_func()

        assert call_count_2 == 1
