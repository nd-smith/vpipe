"""
Circuit breaker pattern for resilience against cascading failures.

Protects against scenarios like:
- VPN disconnects causing thousands of failures
- Upstream service outages
- Network partitions

States:
- CLOSED: Normal operation, requests pass through
- OPEN: Failing, requests rejected immediately (fast-fail)
- HALF_OPEN: Testing recovery, limited requests allowed

Usage:
    # Manual style (record success/failure explicitly)
    breaker = get_circuit_breaker("downloads")
    try:
        result = await download_file()
        breaker.record_success()
    except Exception as e:
        breaker.record_failure(e)
        raise

    # Using call_async helper
    breaker = get_circuit_breaker("downloads")
    result = await breaker.call_async(lambda: download_file())
"""

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from functools import wraps
from typing import Protocol, TypeVar

from core.errors.exceptions import CircuitOpenError
from core.types import ErrorCategory

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class MetricsCollector(Protocol):
    """Protocol for metrics collection (optional dependency)."""

    def increment_counter(self, name: str, labels: dict | None = None) -> None: ...
    def set_gauge(
        self, name: str, value: float, labels: dict | None = None
    ) -> None: ...


class ErrorClassifier(Protocol):
    def classify_error(self, error: Exception) -> ErrorCategory: ...


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior."""

    # Number of failures before opening circuit
    failure_threshold: int = 5

    # Number of successes in half-open before closing
    success_threshold: int = 2

    # Seconds to wait in open state before testing
    timeout_seconds: float = 30.0

    # Max concurrent calls allowed in half-open
    half_open_max_calls: int = 3

    # Error categories that count as failures (None = all errors)
    failure_categories: tuple | None = None

    # If True, auth errors don't count toward failure threshold
    # (they should trigger token refresh, not circuit open)
    ignore_auth_errors: bool = True


# Standard configs - all domains import from here


# Kusto circuit breaker tuned for managed service behavior:
# - 5 failures: Kusto outages are usually systematic, not flaky
# - 60s timeout: Kusto outages tend to last minutes, not seconds
# - Ignore auth: Token expiry shouldn't open circuit (handled separately)
KUSTO_CIRCUIT_CONFIG = CircuitBreakerConfig(
    failure_threshold=5,
    success_threshold=2,
    timeout_seconds=60.0,
    ignore_auth_errors=True,
)

ONELAKE_CIRCUIT_CONFIG = CircuitBreakerConfig(
    failure_threshold=5,
    success_threshold=2,
    timeout_seconds=30.0,
)

EXTERNAL_DOWNLOAD_CIRCUIT_CONFIG = CircuitBreakerConfig(
    failure_threshold=10,
    success_threshold=3,  # Confirm recovery before full load
    timeout_seconds=30.0,
    half_open_max_calls=5,
    ignore_auth_errors=False,  # External URLs don't use our auth
)

# Circuit breaker config for ClaimX API
# - Single service, all endpoints share fate
# - 5 failures: API issues are usually systematic
# - 60s timeout: Give time for service recovery
CLAIMX_API_CIRCUIT_CONFIG = CircuitBreakerConfig(
    failure_threshold=5,
    success_threshold=2,
    timeout_seconds=60.0,
    half_open_max_calls=3,
)

# Kafka circuit breaker config
# - 5 failures: Connection issues are usually systematic
# - 30s timeout: Give broker time to recover
KAFKA_CIRCUIT_CONFIG = CircuitBreakerConfig(
    failure_threshold=5,
    success_threshold=2,
    timeout_seconds=30.0,
    ignore_auth_errors=False,  # OAuth handled separately
)


@dataclass
class CircuitStats:
    """Statistics for circuit breaker monitoring."""

    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    state_changes: int = 0
    last_failure_time: float | None = None
    last_success_time: float | None = None
    last_state_change_time: float | None = None
    current_state: str = "closed"


def _default_classifier(exc: Exception) -> ErrorCategory:
    # Check if exception has a category attribute (from PipelineError)
    if hasattr(exc, "category") and isinstance(exc.category, ErrorCategory):
        return exc.category

    # Default classification logic
    # By default, only count transient and unknown errors
    # Permanent errors are application bugs, not service issues
    return ErrorCategory.UNKNOWN


class CircuitBreaker:
    """Circuit breaker implementation with exception-aware failure tracking. Thread-safe."""

    def __init__(
        self,
        name: str,
        config: CircuitBreakerConfig | None = None,
        on_state_change: Callable[[CircuitState, CircuitState], None] | None = None,
        metrics_collector: MetricsCollector | None = None,
        error_classifier: ErrorClassifier | None = None,
    ):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.on_state_change = on_state_change
        self._metrics = metrics_collector
        self._classifier = error_classifier

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: float | None = None
        self._half_open_calls = 0

        self._stats = CircuitStats()
        self._lock = threading.RLock()

        # Export initial state metric
        self._export_state_metric()

    @property
    def state(self) -> CircuitState:
        """Current circuit state (may transition on access)."""
        with self._lock:
            self._check_state_transition()
            return self._state

    @property
    def is_closed(self) -> bool:
        return self.state == CircuitState.CLOSED

    @property
    def is_open(self) -> bool:
        return self.state == CircuitState.OPEN

    @property
    def stats(self) -> CircuitStats:
        """Get copy of current statistics."""
        with self._lock:
            self._check_state_transition()
            return CircuitStats(
                total_calls=self._stats.total_calls,
                successful_calls=self._stats.successful_calls,
                failed_calls=self._stats.failed_calls,
                rejected_calls=self._stats.rejected_calls,
                state_changes=self._stats.state_changes,
                last_failure_time=self._stats.last_failure_time,
                last_success_time=self._stats.last_success_time,
                last_state_change_time=self._stats.last_state_change_time,
                current_state=self._state.value,
            )

    def _should_count_failure(self, exc: Exception) -> bool:
        # Classify the error
        if self._classifier:
            category = self._classifier.classify_error(exc)
        else:
            category = _default_classifier(exc)

        # Ignore auth errors if configured
        if self.config.ignore_auth_errors and category == ErrorCategory.AUTH:
            logger.debug(
                "Ignoring auth error for failure count: circuit_name=%s, error_category=%s",
                self.name,
                category.value,
            )
            return False

        # Check against allowed failure categories
        if self.config.failure_categories:
            return category in self.config.failure_categories

        # By default, count transient, auth (if not ignored), and unknown errors
        # Permanent errors are application bugs, not service issues
        return category in (
            ErrorCategory.TRANSIENT,
            ErrorCategory.AUTH,  # Count auth errors if not ignored
            ErrorCategory.UNKNOWN,
            ErrorCategory.CIRCUIT_OPEN,  # Downstream circuit issues
        )

    def _check_state_transition(self) -> None:
        if self._state == CircuitState.OPEN and self._last_failure_time is not None:
            elapsed = time.time() - self._last_failure_time
            if elapsed >= self.config.timeout_seconds:
                logger.debug(
                    "Circuit breaker timeout elapsed, transitioning to half-open: "
                    "circuit_name=%s, elapsed_seconds=%.2f, timeout_seconds=%.2f, failure_count=%d",
                    self.name,
                    elapsed,
                    self.config.timeout_seconds,
                    self._failure_count,
                )
                self._transition_to(CircuitState.HALF_OPEN)

    def _transition_to(self, new_state: CircuitState) -> None:
        old_state = self._state
        if old_state == new_state:
            return

        self._state = new_state
        self._stats.state_changes += 1
        self._stats.last_state_change_time = time.time()
        self._stats.current_state = new_state.value

        # Export state transition counter metric
        if self._metrics:
            self._metrics.increment_counter(
                "circuit_breaker_state_transitions",
                labels={
                    "circuit_name": self.name,
                    "from_state": old_state.value,
                    "to_state": new_state.value,
                },
            )

        # Reset counters on state change
        if new_state == CircuitState.CLOSED:
            self._failure_count = 0
            self._success_count = 0
            logger.info(
                "Circuit closed: circuit_name=%s, circuit_state=closed",
                self.name,
            )
        elif new_state == CircuitState.HALF_OPEN:
            self._success_count = 0
            self._half_open_calls = 0
            logger.info(
                "Circuit half-open: circuit_name=%s, circuit_state=half_open",
                self.name,
            )
        elif new_state == CircuitState.OPEN:
            self._success_count = 0
            logger.warning(
                "Circuit open: circuit_name=%s, circuit_state=open, timeout_seconds=%.2f",
                self.name,
                self.config.timeout_seconds,
            )

        if self.on_state_change:
            try:
                self.on_state_change(old_state, new_state)
            except Exception as e:
                logger.warning(
                    "Error in circuit state change callback: circuit_name=%s, error=%s",
                    self.name,
                    str(e),
                    exc_info=False,
                )

        # Update state metrics after transition
        self._export_state_metric()

    def _record_success(self) -> None:
        self._stats.successful_calls += 1
        self._stats.last_success_time = time.time()

        # Export success counter metric
        if self._metrics:
            self._metrics.increment_counter(
                "circuit_breaker_calls_total",
                labels={"circuit_name": self.name, "result": "success"},
            )

        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self.config.success_threshold:
                self._transition_to(CircuitState.CLOSED)
        elif self._state == CircuitState.CLOSED:
            # Reset failure count on success (consecutive failure tracking)
            self._failure_count = 0

        # Update state metrics
        self._export_state_metric()

    def _record_failure(self, exc: Exception) -> None:
        self._stats.failed_calls += 1
        self._stats.last_failure_time = time.time()

        # Export failure counter metric
        if self._metrics:
            self._metrics.increment_counter(
                "circuit_breaker_calls_total",
                labels={"circuit_name": self.name, "result": "failure"},
            )

        # Check if this error should count
        if not self._should_count_failure(exc):
            logger.debug(
                "Circuit breaker failure not counted: circuit_name=%s, circuit_state=%s, "
                "error_type=%s, error_message=%s",
                self.name,
                self._state.value,
                type(exc).__name__,
                str(exc)[:200],
            )
            return

        self._last_failure_time = time.time()

        if self._state == CircuitState.HALF_OPEN:
            # Any counted failure in half-open goes back to open
            logger.debug(
                "Circuit breaker failure recorded (half-open): circuit_name=%s, "
                "error_type=%s, error_message=%s, action=transitioning to open",
                self.name,
                type(exc).__name__,
                str(exc)[:200],
            )
            self._transition_to(CircuitState.OPEN)
        elif self._state == CircuitState.CLOSED:
            self._failure_count += 1
            logger.debug(
                "Circuit breaker failure recorded: circuit_name=%s, circuit_state=%s, "
                "error_type=%s, error_message=%s, failure_count=%d, failure_threshold=%d",
                self.name,
                self._state.value,
                type(exc).__name__,
                str(exc)[:200],
                self._failure_count,
                self.config.failure_threshold,
            )
            if self._failure_count >= self.config.failure_threshold:
                self._transition_to(CircuitState.OPEN)

        # Update state metrics
        self._export_state_metric()

    def _export_state_metric(self) -> None:
        if not self._metrics:
            return

        # Export state as numeric gauge (0=closed, 1=half_open, 2=open)
        state_value = {
            CircuitState.CLOSED: 0,
            CircuitState.HALF_OPEN: 1,
            CircuitState.OPEN: 2,
        }[self._state]

        self._metrics.set_gauge(
            "circuit_breaker_state", state_value, labels={"circuit_name": self.name}
        )

        # Export failure count
        self._metrics.set_gauge(
            "circuit_breaker_failures",
            self._failure_count,
            labels={"circuit_name": self.name},
        )

        # Export success count (relevant in half-open)
        self._metrics.set_gauge(
            "circuit_breaker_successes",
            self._success_count,
            labels={"circuit_name": self.name},
        )

    def _can_execute(self) -> bool:
        self._check_state_transition()

        if self._state == CircuitState.CLOSED:
            return True

        if self._state == CircuitState.OPEN:
            return False

        # HALF_OPEN: allow limited calls for testing
        if self._half_open_calls < self.config.half_open_max_calls:
            self._half_open_calls += 1
            return True

        return False

    def _get_retry_after(self) -> float:
        if self._last_failure_time is None:
            return 0.0
        elapsed = time.time() - self._last_failure_time
        return max(0, self.config.timeout_seconds - elapsed)

    async def call_async(self, func: Callable[[], T]) -> T:
        with self._lock:
            self._stats.total_calls += 1

            if not self._can_execute():
                self._stats.rejected_calls += 1
                retry_after = self._get_retry_after()
                raise CircuitOpenError(self.name, retry_after)

        # Execute outside lock
        try:
            result = await func()
            with self._lock:
                self._record_success()
            return result
        except Exception as e:
            with self._lock:
                self._record_failure(e)
            raise

    def record_success(self) -> None:
        with self._lock:
            self._stats.total_calls += 1
            self._record_success()

    def record_failure(self, exc: Exception) -> None:
        with self._lock:
            self._stats.total_calls += 1
            self._record_failure(exc)

    def reset(self) -> None:
        with self._lock:
            self._transition_to(CircuitState.CLOSED)
            self._failure_count = 0
            self._last_failure_time = None
            logger.info(
                "Circuit manually reset: circuit_name=%s",
                self.name,
            )

    def get_diagnostics(self) -> dict:
        with self._lock:
            self._check_state_transition()
            return {
                "name": self.name,
                "state": self._state.value,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
                "config": {
                    "failure_threshold": self.config.failure_threshold,
                    "success_threshold": self.config.success_threshold,
                    "timeout_seconds": self.config.timeout_seconds,
                },
                "stats": {
                    "total_calls": self._stats.total_calls,
                    "successful_calls": self._stats.successful_calls,
                    "failed_calls": self._stats.failed_calls,
                    "rejected_calls": self._stats.rejected_calls,
                    "state_changes": self._stats.state_changes,
                },
            }


# =============================================================================
# Circuit Breaker Registry
# =============================================================================

_breakers: dict[str, CircuitBreaker] = {}
_registry_lock = threading.Lock()


def get_circuit_breaker(
    name: str,
    config: CircuitBreakerConfig | None = None,
    metrics_collector: MetricsCollector | None = None,
    error_classifier: ErrorClassifier | None = None,
) -> CircuitBreaker:
    with _registry_lock:
        if name not in _breakers:
            _breakers[name] = CircuitBreaker(
                name,
                config,
                metrics_collector=metrics_collector,
                error_classifier=error_classifier,
            )
            logger.debug(
                "Created circuit breaker: circuit_name=%s",
                name,
            )
        return _breakers[name]


__all__ = [
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitOpenError",
    "CircuitState",
    "CircuitStats",
    "ErrorClassifier",
    "MetricsCollector",
    "get_circuit_breaker",
    # Standard configs
    "CLAIMX_API_CIRCUIT_CONFIG",
    "EXTERNAL_DOWNLOAD_CIRCUIT_CONFIG",
    "KAFKA_CIRCUIT_CONFIG",
    "KUSTO_CIRCUIT_CONFIG",
    "ONELAKE_CIRCUIT_CONFIG",
]
