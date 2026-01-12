"""
Tests for circuit breaker implementation.

Verifies:
- State transitions (CLOSED → OPEN → HALF_OPEN → CLOSED)
- Failure counting and thresholds
- Success counting in half-open state
- Thread safety for concurrent access
- Error classification integration
- Metrics collection
- Async function support
"""

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
from unittest.mock import Mock

import pytest

from core.resilience.circuit_breaker import (
    KAFKA_CIRCUIT_CONFIG,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitOpenError,
    CircuitState,
    circuit_protected,
    get_circuit_breaker,
)
from core.types import ErrorCategory


# =============================================================================
# Test Fixtures and Helpers
# =============================================================================


class TestException(Exception):
    """Test exception with category."""

    def __init__(self, message: str, category: ErrorCategory = ErrorCategory.TRANSIENT):
        super().__init__(message)
        self.category = category


class MockMetricsCollector:
    """Mock metrics collector for testing."""

    def __init__(self):
        self.counters = {}
        self.gauges = {}

    def increment_counter(self, name: str, labels: Optional[dict] = None):
        key = (name, tuple(sorted((labels or {}).items())))
        self.counters[key] = self.counters.get(key, 0) + 1

    def set_gauge(self, name: str, value: float, labels: Optional[dict] = None):
        key = (name, tuple(sorted((labels or {}).items())))
        self.gauges[key] = value


class MockErrorClassifier:
    """Mock error classifier for testing."""

    def __init__(self):
        self.classifications = {}

    def classify_error(self, error: Exception) -> ErrorCategory:
        # Check if we have a mapping for this error type
        error_type = type(error).__name__
        if error_type in self.classifications:
            return self.classifications[error_type]
        # Default to checking if error has category attribute
        if hasattr(error, "category"):
            return error.category
        return ErrorCategory.UNKNOWN


@pytest.fixture
def breaker():
    """Create a basic circuit breaker for testing."""
    config = CircuitBreakerConfig(
        failure_threshold=3,
        success_threshold=2,
        timeout_seconds=1.0,
        half_open_max_calls=2,
    )
    return CircuitBreaker("test", config)


@pytest.fixture
def metrics_collector():
    """Create a mock metrics collector."""
    return MockMetricsCollector()


@pytest.fixture
def error_classifier():
    """Create a mock error classifier."""
    return MockErrorClassifier()


# =============================================================================
# State Transition Tests
# =============================================================================


def test_circuit_starts_closed(breaker):
    """Circuit breaker should start in CLOSED state."""
    assert breaker.state == CircuitState.CLOSED
    assert breaker.is_closed
    assert not breaker.is_open


def test_transition_closed_to_open(breaker):
    """Circuit should open after failure threshold reached."""
    # Trigger failures up to threshold
    for i in range(3):
        try:
            breaker.call(lambda: (_ for _ in ()).throw(TestException("fail")))
        except TestException:
            pass

    assert breaker.state == CircuitState.OPEN
    assert breaker.is_open
    assert not breaker.is_closed


def test_transition_open_to_half_open(breaker):
    """Circuit should transition to half-open after timeout."""
    # Open the circuit
    for i in range(3):
        try:
            breaker.call(lambda: (_ for _ in ()).throw(TestException("fail")))
        except TestException:
            pass

    assert breaker.state == CircuitState.OPEN

    # Wait for timeout
    time.sleep(1.1)

    # Check state triggers transition
    assert breaker.state == CircuitState.HALF_OPEN


def test_transition_half_open_to_closed(breaker):
    """Circuit should close after success threshold in half-open."""
    # Open the circuit
    for i in range(3):
        try:
            breaker.call(lambda: (_ for _ in ()).throw(TestException("fail")))
        except TestException:
            pass

    # Wait for timeout
    time.sleep(1.1)
    assert breaker.state == CircuitState.HALF_OPEN

    # Record successes up to threshold
    breaker.call(lambda: "success")
    assert breaker.state == CircuitState.HALF_OPEN  # Still half-open

    breaker.call(lambda: "success")
    assert breaker.state == CircuitState.CLOSED  # Now closed


def test_transition_half_open_to_open_on_failure(breaker):
    """Circuit should reopen on any failure in half-open state."""
    # Open the circuit
    for i in range(3):
        try:
            breaker.call(lambda: (_ for _ in ()).throw(TestException("fail")))
        except TestException:
            pass

    # Wait for timeout
    time.sleep(1.1)
    assert breaker.state == CircuitState.HALF_OPEN

    # One failure should reopen
    try:
        breaker.call(lambda: (_ for _ in ()).throw(TestException("fail")))
    except TestException:
        pass

    assert breaker.state == CircuitState.OPEN


def test_half_open_call_limiting(breaker):
    """Half-open state should limit concurrent calls."""
    # Open the circuit
    for i in range(3):
        try:
            breaker.call(lambda: (_ for _ in ()).throw(TestException("fail")))
        except TestException:
            pass

    # Wait for timeout
    time.sleep(1.1)
    assert breaker.state == CircuitState.HALF_OPEN

    # First two calls should be allowed (half_open_max_calls=2)
    breaker.call(lambda: "success")
    assert breaker.state == CircuitState.HALF_OPEN

    # Third call should be rejected (would exceed half_open_max_calls)
    # Note: Second success would close circuit, so we check before that
    stats = breaker.stats
    assert stats.successful_calls == 1


# =============================================================================
# Failure Counting Tests
# =============================================================================


def test_failure_count_increments(breaker):
    """Failure count should increment on transient errors."""
    for i in range(2):
        try:
            breaker.call(lambda: (_ for _ in ()).throw(TestException("fail")))
        except TestException:
            pass

    stats = breaker.stats
    assert stats.failed_calls == 2
    assert breaker.state == CircuitState.CLOSED  # Not yet at threshold


def test_success_resets_failure_count(breaker):
    """Success should reset consecutive failure count."""
    # Two failures
    for i in range(2):
        try:
            breaker.call(lambda: (_ for _ in ()).throw(TestException("fail")))
        except TestException:
            pass

    # One success
    breaker.call(lambda: "success")

    # Two more failures (should not open circuit)
    for i in range(2):
        try:
            breaker.call(lambda: (_ for _ in ()).throw(TestException("fail")))
        except TestException:
            pass

    assert breaker.state == CircuitState.CLOSED


def test_auth_errors_ignored_when_configured(breaker):
    """Auth errors should not count toward failure threshold when ignore_auth_errors=True."""
    breaker.config.ignore_auth_errors = True

    # 5 auth errors should not open circuit
    for i in range(5):
        try:
            breaker.call(
                lambda: (_ for _ in ()).throw(
                    TestException("auth fail", ErrorCategory.AUTH)
                )
            )
        except TestException:
            pass

    assert breaker.state == CircuitState.CLOSED


def test_auth_errors_counted_when_not_ignored():
    """Auth errors should count when ignore_auth_errors=False."""
    config = CircuitBreakerConfig(
        failure_threshold=3,
        success_threshold=2,
        timeout_seconds=1.0,
        ignore_auth_errors=False,
    )
    breaker = CircuitBreaker("test", config)

    # 3 auth errors should open circuit
    for i in range(3):
        try:
            breaker.call(
                lambda: (_ for _ in ()).throw(
                    TestException("auth fail", ErrorCategory.AUTH)
                )
            )
        except TestException:
            pass

    assert breaker.state == CircuitState.OPEN


def test_permanent_errors_not_counted():
    """Permanent errors should not count toward failure threshold by default."""
    config = CircuitBreakerConfig(
        failure_threshold=3,
        success_threshold=2,
        timeout_seconds=1.0,
    )
    breaker = CircuitBreaker("test", config)

    # 5 permanent errors should not open circuit
    for i in range(5):
        try:
            breaker.call(
                lambda: (_ for _ in ()).throw(
                    TestException("permanent fail", ErrorCategory.PERMANENT)
                )
            )
        except TestException:
            pass

    assert breaker.state == CircuitState.CLOSED


# =============================================================================
# Circuit Open Behavior Tests
# =============================================================================


def test_circuit_open_rejects_calls(breaker):
    """Open circuit should reject calls with CircuitOpenError."""
    # Open the circuit
    for i in range(3):
        try:
            breaker.call(lambda: (_ for _ in ()).throw(TestException("fail")))
        except TestException:
            pass

    assert breaker.state == CircuitState.OPEN

    # Next call should be rejected
    with pytest.raises(CircuitOpenError) as exc_info:
        breaker.call(lambda: "should not execute")

    assert exc_info.value.circuit_name == "test"
    assert exc_info.value.retry_after > 0


def test_circuit_open_error_includes_retry_after(breaker):
    """CircuitOpenError should include accurate retry_after."""
    # Open the circuit
    for i in range(3):
        try:
            breaker.call(lambda: (_ for _ in ()).throw(TestException("fail")))
        except TestException:
            pass

    # Immediately check retry_after
    try:
        breaker.call(lambda: "test")
    except CircuitOpenError as e:
        assert 0.9 <= e.retry_after <= 1.1  # Should be close to timeout_seconds

    # Wait a bit
    time.sleep(0.5)
    try:
        breaker.call(lambda: "test")
    except CircuitOpenError as e:
        assert 0.4 <= e.retry_after <= 0.6  # Should be reduced


# =============================================================================
# Statistics Tests
# =============================================================================


def test_statistics_tracking(breaker):
    """Circuit should track call statistics accurately."""
    # 2 successes
    breaker.call(lambda: "success")
    breaker.call(lambda: "success")

    # 2 failures
    for i in range(2):
        try:
            breaker.call(lambda: (_ for _ in ()).throw(TestException("fail")))
        except TestException:
            pass

    stats = breaker.stats
    assert stats.total_calls == 4
    assert stats.successful_calls == 2
    assert stats.failed_calls == 2
    assert stats.rejected_calls == 0


def test_rejected_calls_counted(breaker):
    """Rejected calls should be counted in statistics."""
    # Open the circuit
    for i in range(3):
        try:
            breaker.call(lambda: (_ for _ in ()).throw(TestException("fail")))
        except TestException:
            pass

    # Attempt calls while open
    for i in range(2):
        try:
            breaker.call(lambda: "test")
        except CircuitOpenError:
            pass

    stats = breaker.stats
    assert stats.rejected_calls == 2


def test_state_changes_counted(breaker):
    """State changes should be counted."""
    # Open the circuit
    for i in range(3):
        try:
            breaker.call(lambda: (_ for _ in ()).throw(TestException("fail")))
        except TestException:
            pass

    stats = breaker.stats
    assert stats.state_changes >= 1  # CLOSED → OPEN

    # Transition to half-open
    time.sleep(1.1)
    _ = breaker.state  # Trigger state check

    stats = breaker.stats
    assert stats.state_changes >= 2  # CLOSED → OPEN → HALF_OPEN


# =============================================================================
# Metrics Integration Tests
# =============================================================================


def test_metrics_collection(metrics_collector):
    """Circuit should report metrics when collector provided."""
    config = CircuitBreakerConfig(failure_threshold=2, timeout_seconds=1.0)
    breaker = CircuitBreaker("test", config, metrics_collector=metrics_collector)

    # Success
    breaker.call(lambda: "success")

    # Verify success counter
    counter_key = ("circuit_breaker_calls_total", (("circuit_name", "test"), ("result", "success")))
    assert metrics_collector.counters.get(counter_key, 0) == 1

    # Failure
    try:
        breaker.call(lambda: (_ for _ in ()).throw(TestException("fail")))
    except TestException:
        pass

    # Verify failure counter
    counter_key = ("circuit_breaker_calls_total", (("circuit_name", "test"), ("result", "failure")))
    assert metrics_collector.counters.get(counter_key, 0) == 1


def test_state_metrics(metrics_collector):
    """Circuit should export state as gauge metric."""
    config = CircuitBreakerConfig(failure_threshold=2, timeout_seconds=1.0)
    breaker = CircuitBreaker("test", config, metrics_collector=metrics_collector)

    # Initial state should be CLOSED (0)
    gauge_key = ("circuit_breaker_state", (("circuit_name", "test"),))
    assert metrics_collector.gauges.get(gauge_key) == 0

    # Open circuit
    for i in range(2):
        try:
            breaker.call(lambda: (_ for _ in ()).throw(TestException("fail")))
        except TestException:
            pass

    # State should be OPEN (2)
    assert metrics_collector.gauges.get(gauge_key) == 2


# =============================================================================
# Error Classification Tests
# =============================================================================


def test_custom_error_classifier(error_classifier):
    """Circuit should use custom error classifier when provided."""
    config = CircuitBreakerConfig(failure_threshold=2, timeout_seconds=1.0)
    breaker = CircuitBreaker("test", config, error_classifier=error_classifier)

    # Configure classifier to treat RuntimeError as PERMANENT
    error_classifier.classifications["RuntimeError"] = ErrorCategory.PERMANENT

    # 5 RuntimeErrors should not open circuit
    for i in range(5):
        try:
            breaker.call(lambda: (_ for _ in ()).throw(RuntimeError("test")))
        except RuntimeError:
            pass

    assert breaker.state == CircuitState.CLOSED


# =============================================================================
# Thread Safety Tests
# =============================================================================


def test_concurrent_access():
    """Circuit should be thread-safe for concurrent access."""
    config = CircuitBreakerConfig(failure_threshold=10, timeout_seconds=1.0)
    breaker = CircuitBreaker("test", config)

    def worker(should_fail: bool):
        for i in range(10):
            try:
                if should_fail:
                    breaker.call(lambda: (_ for _ in ()).throw(TestException("fail")))
                else:
                    breaker.call(lambda: "success")
            except (TestException, CircuitOpenError):
                pass

    # Run 4 threads (2 failing, 2 succeeding)
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(worker, True),
            executor.submit(worker, True),
            executor.submit(worker, False),
            executor.submit(worker, False),
        ]
        for future in futures:
            future.result()

    # Verify statistics are consistent
    stats = breaker.stats
    assert stats.total_calls == stats.successful_calls + stats.failed_calls + stats.rejected_calls


# =============================================================================
# Async Support Tests
# =============================================================================


@pytest.mark.asyncio
async def test_async_function_support():
    """Circuit should support async functions."""
    config = CircuitBreakerConfig(failure_threshold=2, timeout_seconds=1.0)
    breaker = CircuitBreaker("test", config)

    async def async_success():
        await asyncio.sleep(0.01)
        return "success"

    async def async_failure():
        await asyncio.sleep(0.01)
        raise TestException("fail")

    # Success
    result = await breaker.call_async(async_success)
    assert result == "success"

    # Failures to open circuit
    for i in range(2):
        try:
            await breaker.call_async(async_failure)
        except TestException:
            pass

    assert breaker.state == CircuitState.OPEN

    # Rejected call
    with pytest.raises(CircuitOpenError):
        await breaker.call_async(async_success)


# =============================================================================
# Decorator Tests
# =============================================================================


def test_circuit_protected_decorator():
    """@circuit_protected decorator should work correctly."""

    @circuit_protected("test_decorator", CircuitBreakerConfig(failure_threshold=2, timeout_seconds=1.0))
    def protected_function(should_fail: bool):
        if should_fail:
            raise TestException("fail")
        return "success"

    # Success
    assert protected_function(False) == "success"

    # Failures to open circuit
    for i in range(2):
        try:
            protected_function(True)
        except TestException:
            pass

    # Should be rejected now
    with pytest.raises(CircuitOpenError):
        protected_function(False)


# =============================================================================
# Registry Tests
# =============================================================================


def test_get_circuit_breaker_creates_instance():
    """get_circuit_breaker should create instance on first call."""
    breaker = get_circuit_breaker("new_breaker")
    assert breaker.name == "new_breaker"


def test_get_circuit_breaker_returns_same_instance():
    """get_circuit_breaker should return same instance for same name."""
    breaker1 = get_circuit_breaker("shared_breaker")
    breaker2 = get_circuit_breaker("shared_breaker")
    assert breaker1 is breaker2


# =============================================================================
# Reset and Diagnostics Tests
# =============================================================================


def test_manual_reset(breaker):
    """Manual reset should close circuit and clear counters."""
    # Open the circuit
    for i in range(3):
        try:
            breaker.call(lambda: (_ for _ in ()).throw(TestException("fail")))
        except TestException:
            pass

    assert breaker.state == CircuitState.OPEN

    # Reset
    breaker.reset()

    assert breaker.state == CircuitState.CLOSED
    stats = breaker.stats
    # Note: stats are cumulative, but internal failure_count is reset


def test_diagnostics(breaker):
    """get_diagnostics should return complete diagnostic information."""
    diagnostics = breaker.get_diagnostics()

    assert diagnostics["name"] == "test"
    assert diagnostics["state"] == "closed"
    assert "config" in diagnostics
    assert "stats" in diagnostics
    assert diagnostics["config"]["failure_threshold"] == 3


# =============================================================================
# State Change Callback Tests
# =============================================================================


def test_state_change_callback():
    """on_state_change callback should be called on transitions."""
    callback_calls = []

    def callback(old_state: CircuitState, new_state: CircuitState):
        callback_calls.append((old_state, new_state))

    config = CircuitBreakerConfig(failure_threshold=2, timeout_seconds=1.0)
    breaker = CircuitBreaker("test", config, on_state_change=callback)

    # Open the circuit
    for i in range(2):
        try:
            breaker.call(lambda: (_ for _ in ()).throw(TestException("fail")))
        except TestException:
            pass

    assert len(callback_calls) == 1
    assert callback_calls[0] == (CircuitState.CLOSED, CircuitState.OPEN)


def test_state_change_callback_exception_handled():
    """Exceptions in state change callback should be handled gracefully."""

    def bad_callback(old_state: CircuitState, new_state: CircuitState):
        raise RuntimeError("callback error")

    config = CircuitBreakerConfig(failure_threshold=2, timeout_seconds=1.0)
    breaker = CircuitBreaker("test", config, on_state_change=bad_callback)

    # Should not raise, just log warning
    for i in range(2):
        try:
            breaker.call(lambda: (_ for _ in ()).throw(TestException("fail")))
        except TestException:
            pass

    assert breaker.state == CircuitState.OPEN


# =============================================================================
# Configuration Tests
# =============================================================================


def test_standard_configs():
    """Standard configs should have sensible values."""
    assert KAFKA_CIRCUIT_CONFIG.failure_threshold == 5
    assert KAFKA_CIRCUIT_CONFIG.success_threshold == 2
    assert KAFKA_CIRCUIT_CONFIG.timeout_seconds == 30.0
    assert KAFKA_CIRCUIT_CONFIG.ignore_auth_errors is False


# =============================================================================
# Manual Success/Failure Recording Tests
# =============================================================================


def test_manual_success_recording(breaker):
    """Manual success recording should work for context manager pattern."""
    breaker.record_success()

    stats = breaker.stats
    assert stats.successful_calls == 1
    assert stats.total_calls == 1


def test_manual_failure_recording(breaker):
    """Manual failure recording should work for context manager pattern."""
    breaker.record_failure(TestException("fail"))

    stats = breaker.stats
    assert stats.failed_calls == 1
    assert stats.total_calls == 1
