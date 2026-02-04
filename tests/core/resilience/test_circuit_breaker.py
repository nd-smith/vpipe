"""
Tests for circuit breaker implementation.

Test Coverage:
    - CircuitBreakerConfig initialization
    - State transitions (CLOSED -> OPEN -> HALF_OPEN -> CLOSED)
    - Failure and success counting
    - Timeout-based state transition
    - Thread safety with concurrent access
    - Error classification and filtering
    - Metrics collection
    - Registry pattern (get_circuit_breaker)
    - Decorator usage (circuit_protected)
"""

import asyncio
import threading
import time
from unittest.mock import Mock, AsyncMock

import pytest

from core.errors.exceptions import (
    ErrorCategory,
    TransientError,
    AuthError,
    PermanentError,
    CircuitOpenError,
)
from core.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
    CircuitStats,
    get_circuit_breaker,
    circuit_protected,
    KUSTO_CIRCUIT_CONFIG,
)


class TestCircuitBreakerConfig:
    """Tests for CircuitBreakerConfig dataclass."""

    def test_default_values(self):
        """CircuitBreakerConfig uses sensible defaults."""
        config = CircuitBreakerConfig()
        assert config.failure_threshold == 5
        assert config.success_threshold == 2
        assert config.timeout_seconds == 30.0
        assert config.half_open_max_calls == 3
        assert config.failure_categories is None
        assert config.ignore_auth_errors is True

    def test_custom_values(self):
        """Can override all config values."""
        config = CircuitBreakerConfig(
            failure_threshold=10,
            success_threshold=3,
            timeout_seconds=60.0,
            half_open_max_calls=5,
            ignore_auth_errors=False,
        )
        assert config.failure_threshold == 10
        assert config.success_threshold == 3
        assert config.timeout_seconds == 60.0
        assert config.half_open_max_calls == 5
        assert config.ignore_auth_errors is False


class TestCircuitBreakerStates:
    """Tests for circuit breaker state machine."""

    def test_initial_state_is_closed(self):
        """New circuit starts in CLOSED state."""
        breaker = CircuitBreaker("test")
        assert breaker.state == CircuitState.CLOSED
        assert breaker.is_closed is True
        assert breaker.is_open is False

    def test_transitions_to_open_after_threshold_failures(self):
        """Circuit opens after reaching failure threshold."""
        breaker = CircuitBreaker(
            "test", CircuitBreakerConfig(failure_threshold=3, ignore_auth_errors=False)
        )

        # Record failures up to threshold
        for _ in range(3):
            breaker.record_failure(TransientError("Fail"))

        assert breaker.state == CircuitState.OPEN
        assert breaker.is_open is True

    def test_auth_errors_ignored_when_configured(self):
        """Auth errors don't count toward failure threshold."""
        breaker = CircuitBreaker(
            "test", CircuitBreakerConfig(failure_threshold=3, ignore_auth_errors=True)
        )

        # Record auth errors
        for _ in range(5):
            breaker.record_failure(AuthError("Token expired"))

        # Circuit should still be closed
        assert breaker.state == CircuitState.CLOSED

    def test_auth_errors_counted_when_configured(self):
        """Auth errors count toward threshold when ignore_auth_errors=False."""
        breaker = CircuitBreaker(
            "test", CircuitBreakerConfig(failure_threshold=3, ignore_auth_errors=False)
        )

        for _ in range(3):
            breaker.record_failure(AuthError("Token expired"))

        assert breaker.state == CircuitState.OPEN

    def test_permanent_errors_not_counted(self):
        """Permanent errors don't count toward failure threshold."""
        breaker = CircuitBreaker(
            "test", CircuitBreakerConfig(failure_threshold=3, ignore_auth_errors=False)
        )

        # Permanent errors are app bugs, not service issues
        for _ in range(5):
            breaker.record_failure(PermanentError("Not found"))

        assert breaker.state == CircuitState.CLOSED

    def test_transitions_to_half_open_after_timeout(self):
        """Circuit transitions to HALF_OPEN after timeout."""
        breaker = CircuitBreaker(
            "test",
            CircuitBreakerConfig(failure_threshold=2, timeout_seconds=0.1),
        )

        # Open the circuit
        breaker.record_failure(TransientError("Fail 1"))
        breaker.record_failure(TransientError("Fail 2"))
        assert breaker.state == CircuitState.OPEN

        # Wait for timeout
        time.sleep(0.15)

        # Check state triggers transition
        assert breaker.is_open is False
        assert breaker.state == CircuitState.HALF_OPEN

    def test_half_open_success_closes_circuit(self):
        """Successful calls in HALF_OPEN close the circuit."""
        breaker = CircuitBreaker(
            "test",
            CircuitBreakerConfig(
                failure_threshold=2, success_threshold=2, timeout_seconds=0.1
            ),
        )

        # Open and transition to half-open
        breaker.record_failure(TransientError("Fail 1"))
        breaker.record_failure(TransientError("Fail 2"))
        time.sleep(0.15)
        assert breaker.is_open is False  # Triggers transition

        # Record successes up to threshold
        breaker.record_success()
        assert breaker.state == CircuitState.HALF_OPEN
        breaker.record_success()
        assert breaker.state == CircuitState.CLOSED

    def test_half_open_failure_reopens_circuit(self):
        """Any failure in HALF_OPEN reopens the circuit."""
        breaker = CircuitBreaker(
            "test",
            CircuitBreakerConfig(failure_threshold=2, timeout_seconds=0.1),
        )

        # Open and transition to half-open
        breaker.record_failure(TransientError("Fail 1"))
        breaker.record_failure(TransientError("Fail 2"))
        time.sleep(0.15)
        assert breaker.is_open is False

        # Failure in half-open goes back to open
        breaker.record_failure(TransientError("Fail again"))
        assert breaker.state == CircuitState.OPEN

    def test_closed_success_resets_failure_count(self):
        """Success in CLOSED resets consecutive failure count."""
        breaker = CircuitBreaker(
            "test", CircuitBreakerConfig(failure_threshold=3, ignore_auth_errors=False)
        )

        # Partial failures
        breaker.record_failure(TransientError("Fail 1"))
        breaker.record_failure(TransientError("Fail 2"))
        assert breaker.state == CircuitState.CLOSED

        # Success resets count
        breaker.record_success()

        # Need 3 more failures to open
        breaker.record_failure(TransientError("Fail 3"))
        breaker.record_failure(TransientError("Fail 4"))
        assert breaker.state == CircuitState.CLOSED  # Only 2 consecutive
        breaker.record_failure(TransientError("Fail 5"))
        assert breaker.state == CircuitState.OPEN


class TestCircuitBreakerCallAsync:
    """Tests for call_async method."""

    @pytest.mark.asyncio
    async def test_call_async_success_in_closed_state(self):
        """call_async succeeds when circuit is closed."""
        breaker = CircuitBreaker("test")

        async def func():
            return "success"

        result = await breaker.call_async(func)
        assert result == "success"
        assert breaker.stats.successful_calls == 1

    @pytest.mark.asyncio
    async def test_call_async_raises_when_open(self):
        """call_async raises CircuitOpenError when circuit is open."""
        breaker = CircuitBreaker(
            "test", CircuitBreakerConfig(failure_threshold=2, ignore_auth_errors=False)
        )

        # Open the circuit
        breaker.record_failure(TransientError("Fail 1"))
        breaker.record_failure(TransientError("Fail 2"))

        async def func():
            return "success"

        with pytest.raises(CircuitOpenError) as exc_info:
            await breaker.call_async(func)

        assert exc_info.value.circuit_name == "test"
        assert breaker.stats.rejected_calls == 1

    @pytest.mark.asyncio
    async def test_call_async_records_failure_on_exception(self):
        """call_async records failure when function raises."""
        breaker = CircuitBreaker("test")

        async def func():
            raise TransientError("Failed")

        with pytest.raises(TransientError):
            await breaker.call_async(func)

        assert breaker.stats.failed_calls == 1

    @pytest.mark.asyncio
    async def test_call_async_half_open_limits_concurrent_calls(self):
        """HALF_OPEN allows limited concurrent calls."""
        breaker = CircuitBreaker(
            "test",
            CircuitBreakerConfig(
                failure_threshold=1,
                timeout_seconds=0.1,
                half_open_max_calls=2,
            ),
        )

        # Open the circuit
        breaker.record_failure(TransientError("Fail"))
        time.sleep(0.15)
        assert breaker.is_open is False  # Now half-open

        # First 2 calls should be allowed
        call_count = 0

        async def slow_func():
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.1)
            return "success"

        # Start 3 calls concurrently
        tasks = [breaker.call_async(slow_func) for _ in range(3)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Only 2 should succeed, 1 should be rejected
        successful = sum(1 for r in results if r == "success")
        rejected = sum(1 for r in results if isinstance(r, CircuitOpenError))
        assert successful == 2
        assert rejected == 1


class TestCircuitBreakerStats:
    """Tests for statistics tracking."""

    def test_stats_tracks_calls(self):
        """Stats track all call types."""
        breaker = CircuitBreaker(
            "test", CircuitBreakerConfig(failure_threshold=2, ignore_auth_errors=False)
        )

        breaker.record_success()
        breaker.record_failure(TransientError("Fail 1"))
        breaker.record_failure(TransientError("Fail 2"))  # Opens circuit

        stats = breaker.stats
        assert stats.total_calls == 3
        assert stats.successful_calls == 1
        assert stats.failed_calls == 2
        assert stats.current_state == "open"

    def test_stats_tracks_state_changes(self):
        """Stats track state transitions."""
        breaker = CircuitBreaker(
            "test",
            CircuitBreakerConfig(
                failure_threshold=1, success_threshold=1, timeout_seconds=0.1
            ),
        )

        initial_stats = breaker.stats
        assert initial_stats.state_changes == 0

        # Transition to OPEN
        breaker.record_failure(TransientError("Fail"))
        assert breaker.stats.state_changes == 1

        # Transition to HALF_OPEN
        time.sleep(0.15)
        _ = breaker.is_open
        assert breaker.stats.state_changes == 2

        # Transition to CLOSED
        breaker.record_success()
        assert breaker.stats.state_changes == 3

    def test_get_diagnostics(self):
        """get_diagnostics returns comprehensive info."""
        breaker = CircuitBreaker(
            "test", CircuitBreakerConfig(failure_threshold=3, success_threshold=2)
        )

        breaker.record_success()
        breaker.record_failure(TransientError("Fail"))

        diag = breaker.get_diagnostics()
        assert diag["name"] == "test"
        assert diag["state"] == "closed"
        assert diag["config"]["failure_threshold"] == 3
        assert diag["config"]["success_threshold"] == 2
        assert diag["stats"]["total_calls"] == 2


class TestCircuitBreakerThreadSafety:
    """Tests for thread safety."""

    def test_concurrent_record_success(self):
        """Concurrent success recording is thread-safe."""
        breaker = CircuitBreaker("test")
        num_threads = 10
        calls_per_thread = 100

        def record_many():
            for _ in range(calls_per_thread):
                breaker.record_success()

        threads = [threading.Thread(target=record_many) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        expected_calls = num_threads * calls_per_thread
        assert breaker.stats.total_calls == expected_calls
        assert breaker.stats.successful_calls == expected_calls

    def test_concurrent_record_failure(self):
        """Concurrent failure recording is thread-safe."""
        breaker = CircuitBreaker(
            "test", CircuitBreakerConfig(failure_threshold=1000, ignore_auth_errors=False)
        )
        num_threads = 10
        calls_per_thread = 50

        def record_many():
            for _ in range(calls_per_thread):
                breaker.record_failure(TransientError("Fail"))

        threads = [threading.Thread(target=record_many) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        expected_calls = num_threads * calls_per_thread
        assert breaker.stats.total_calls == expected_calls
        assert breaker.stats.failed_calls == expected_calls


class TestCircuitBreakerMetrics:
    """Tests for metrics collection."""

    def test_metrics_collector_called_on_success(self):
        """Metrics collector receives success events."""
        metrics = Mock()
        breaker = CircuitBreaker("test", metrics_collector=metrics)

        breaker.record_success()

        metrics.increment_counter.assert_called_with(
            "circuit_breaker_calls_total",
            labels={"circuit_name": "test", "result": "success"},
        )

    def test_metrics_collector_called_on_failure(self):
        """Metrics collector receives failure events."""
        metrics = Mock()
        breaker = CircuitBreaker(
            "test",
            CircuitBreakerConfig(ignore_auth_errors=False),
            metrics_collector=metrics,
        )

        breaker.record_failure(TransientError("Fail"))

        metrics.increment_counter.assert_called_with(
            "circuit_breaker_calls_total",
            labels={"circuit_name": "test", "result": "failure"},
        )

    def test_metrics_collector_called_on_state_change(self):
        """Metrics collector receives state transition events."""
        metrics = Mock()
        breaker = CircuitBreaker(
            "test",
            CircuitBreakerConfig(failure_threshold=1, ignore_auth_errors=False),
            metrics_collector=metrics,
        )

        breaker.record_failure(TransientError("Fail"))

        # Check for state transition metric
        calls = [
            c
            for c in metrics.increment_counter.call_args_list
            if c[0][0] == "circuit_breaker_state_transitions"
        ]
        assert len(calls) == 1
        assert calls[0][1]["labels"]["from_state"] == "closed"
        assert calls[0][1]["labels"]["to_state"] == "open"


class TestCircuitBreakerCallbacks:
    """Tests for state change callbacks."""

    def test_on_state_change_callback(self):
        """on_state_change callback is invoked on transitions."""
        callback = Mock()
        breaker = CircuitBreaker(
            "test",
            CircuitBreakerConfig(failure_threshold=1, ignore_auth_errors=False),
            on_state_change=callback,
        )

        breaker.record_failure(TransientError("Fail"))

        callback.assert_called_once_with(CircuitState.CLOSED, CircuitState.OPEN)

    def test_callback_error_does_not_break_circuit(self):
        """Circuit continues working even if callback raises."""

        def bad_callback(old_state, new_state):
            raise RuntimeError("Callback failed")

        breaker = CircuitBreaker(
            "test",
            CircuitBreakerConfig(failure_threshold=1, ignore_auth_errors=False),
            on_state_change=bad_callback,
        )

        # Should not raise despite callback error
        breaker.record_failure(TransientError("Fail"))
        assert breaker.state == CircuitState.OPEN


class TestCircuitBreakerRegistry:
    """Tests for circuit breaker registry."""

    def test_get_circuit_breaker_creates_new(self):
        """get_circuit_breaker creates new breaker on first call."""
        from core.resilience.circuit_breaker import _breakers

        _breakers.clear()  # Reset registry

        breaker = get_circuit_breaker("test_registry")
        assert breaker.name == "test_registry"
        assert "test_registry" in _breakers

    def test_get_circuit_breaker_returns_existing(self):
        """get_circuit_breaker returns existing breaker."""
        from core.resilience.circuit_breaker import _breakers

        _breakers.clear()

        breaker1 = get_circuit_breaker("test_registry")
        breaker2 = get_circuit_breaker("test_registry")
        assert breaker1 is breaker2

    def test_get_circuit_breaker_with_config(self):
        """get_circuit_breaker uses provided config."""
        from core.resilience.circuit_breaker import _breakers

        _breakers.clear()

        config = CircuitBreakerConfig(failure_threshold=10)
        breaker = get_circuit_breaker("test_config", config)
        assert breaker.config.failure_threshold == 10


class TestCircuitProtectedDecorator:
    """Tests for circuit_protected decorator."""

    def test_decorator_success(self):
        """Decorator allows successful calls."""
        from core.resilience.circuit_breaker import _breakers

        _breakers.clear()

        @circuit_protected("test_decorator")
        def func():
            return "success"

        result = func()
        assert result == "success"

    def test_decorator_records_failure(self):
        """Decorator records failures."""
        from core.resilience.circuit_breaker import _breakers

        _breakers.clear()

        @circuit_protected(
            "test_decorator", CircuitBreakerConfig(failure_threshold=1, ignore_auth_errors=False)
        )
        def func():
            raise TransientError("Fail")

        with pytest.raises(TransientError):
            func()

        breaker = get_circuit_breaker("test_decorator")
        assert breaker.stats.failed_calls == 1

    def test_decorator_rejects_when_open(self):
        """Decorator raises CircuitOpenError when circuit is open."""
        from core.resilience.circuit_breaker import _breakers

        _breakers.clear()

        @circuit_protected(
            "test_decorator", CircuitBreakerConfig(failure_threshold=1, ignore_auth_errors=False)
        )
        def func():
            raise TransientError("Fail")

        # Open the circuit
        with pytest.raises(TransientError):
            func()

        # Next call should be rejected
        with pytest.raises(CircuitOpenError):
            func()


class TestCircuitBreakerReset:
    """Tests for manual circuit reset."""

    def test_reset_closes_circuit(self):
        """reset() closes an open circuit."""
        breaker = CircuitBreaker(
            "test", CircuitBreakerConfig(failure_threshold=1, ignore_auth_errors=False)
        )

        breaker.record_failure(TransientError("Fail"))
        assert breaker.state == CircuitState.OPEN

        breaker.reset()
        assert breaker.state == CircuitState.CLOSED


class TestStandardConfigs:
    """Tests for standard circuit breaker configurations."""

    def test_kusto_circuit_config(self):
        """KUSTO_CIRCUIT_CONFIG has expected values."""
        assert KUSTO_CIRCUIT_CONFIG.failure_threshold == 5
        assert KUSTO_CIRCUIT_CONFIG.timeout_seconds == 60.0
        assert KUSTO_CIRCUIT_CONFIG.ignore_auth_errors is True
