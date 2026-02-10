"""
Tests for exception hierarchy and error classification.
"""

from core.errors.exceptions import (
    AuthError,
    CircuitOpenError,
    ErrorCategory,
    PermanentError,
    PipelineError,
    ThrottlingError,
    TransientError,
    classify_exception,
    classify_http_status,
    is_auth_error,
    is_retryable_error,
    is_transient_error,
    wrap_exception,
)

# NOTE: Many specialized exception classes removed in refactor per OVER_ENGINEERING_REVIEW.md
# Consolidated into base classes (AuthError, TransientError, PermanentError)
# Removed classes:
# - TokenExpiredError, ConnectionError, TimeoutError, ServiceUnavailableError -> TransientError
# - NotFoundError, ForbiddenError, ValidationError, ConfigurationError -> PermanentError
# - KustoError, KustoQueryError, DeltaTableError, OneLakeError -> Use base classes
# - DownloadError, Attachment* errors -> Use base classes with context dict


class TestErrorCategory:
    """Test ErrorCategory enum."""

    def test_all_categories_exist(self):
        """All expected categories are defined."""
        assert ErrorCategory.TRANSIENT.value == "transient"
        assert ErrorCategory.AUTH.value == "auth"
        assert ErrorCategory.PERMANENT.value == "permanent"
        assert ErrorCategory.CIRCUIT_OPEN.value == "circuit_open"
        assert ErrorCategory.UNKNOWN.value == "unknown"


class TestPipelineError:
    """Test base PipelineError class."""

    def test_basic_error(self):
        """Can create basic error with message."""
        err = PipelineError("Something went wrong")
        assert err.message == "Something went wrong"
        assert err.cause is None
        assert err.context == {}
        assert err.category == ErrorCategory.UNKNOWN

    def test_error_with_cause(self):
        """Can wrap another exception."""
        cause = ValueError("Invalid value")
        err = PipelineError("Wrapper message", cause=cause)
        assert err.cause == cause
        assert "Caused by" in str(err)

    def test_error_with_context(self):
        """Can add context dict."""
        err = PipelineError("Error", context={"file": "test.txt", "line": 42})
        assert err.context["file"] == "test.txt"
        assert err.context["line"] == 42

    def test_is_retryable_default(self):
        """Unknown category is retryable."""
        err = PipelineError("Error")
        assert err.is_retryable is True

    def test_should_refresh_auth_default(self):
        """Unknown category doesn't trigger auth refresh."""
        err = PipelineError("Error")
        assert err.should_refresh_auth is False


class TestAuthErrors:
    """Test authentication error hierarchy."""

    def test_auth_error_category(self):
        """AuthError has AUTH category."""
        err = AuthError("Auth failed")
        assert err.category == ErrorCategory.AUTH
        assert err.is_retryable is True
        assert err.should_refresh_auth is True


class TestTransientErrors:
    """Test transient error hierarchy."""

    def test_transient_error_category(self):
        """TransientError has TRANSIENT category."""
        err = TransientError("Temporary issue")
        assert err.category == ErrorCategory.TRANSIENT
        assert err.is_retryable is True

    def test_throttling_error(self):
        """ThrottlingError is transient."""
        err = ThrottlingError("Rate limited")
        assert err.category == ErrorCategory.TRANSIENT
        assert err.retry_after is None

    def test_throttling_error_with_retry_after(self):
        """ThrottlingError can include retry-after."""
        err = ThrottlingError("Rate limited", retry_after=60.0)
        assert err.retry_after == 60.0


class TestPermanentErrors:
    """Test permanent error hierarchy."""

    def test_permanent_error_category(self):
        """PermanentError has PERMANENT category."""
        err = PermanentError("Fatal error")
        assert err.category == ErrorCategory.PERMANENT
        assert err.is_retryable is False


class TestCircuitOpenError:
    """Test circuit breaker error."""

    def test_circuit_open_error(self):
        """CircuitOpenError has CIRCUIT_OPEN category."""
        err = CircuitOpenError("my_circuit", retry_after=30.0)
        assert err.category == ErrorCategory.CIRCUIT_OPEN
        assert err.circuit_name == "my_circuit"
        assert err.retry_after == 30.0
        assert "my_circuit" in str(err)
        assert err.is_retryable is False

    def test_circuit_open_with_cause(self):
        """CircuitOpenError can wrap cause."""
        cause = TimeoutError("Timeout")
        err = CircuitOpenError("my_circuit", retry_after=30.0, cause=cause)
        assert err.cause == cause


class TestIsAuthError:
    """Test is_auth_error() utility."""

    def test_typed_auth_error(self):
        """Detects typed AuthError."""
        assert is_auth_error(AuthError("Auth failed")) is True

    def test_typed_non_auth_error(self):
        """Rejects typed non-auth error."""
        assert is_auth_error(TimeoutError("Timeout")) is False

    def test_string_matching_401(self):
        """Detects 401 in generic exception."""
        assert is_auth_error(Exception("Got 401 unauthorized")) is True

    def test_string_matching_token_expired(self):
        """Detects token expiry in generic exception."""
        assert is_auth_error(Exception("token expired")) is True

    def test_string_matching_azure_code(self):
        """Detects Azure AD error codes."""
        assert is_auth_error(Exception("AADSTS700082")) is True

    def test_no_match(self):
        """Returns false for unrelated errors."""
        assert is_auth_error(Exception("Something else")) is False


class TestIsTransientError:
    """Test is_transient_error() utility."""

    def test_typed_transient_error(self):
        """Detects typed TransientError."""
        assert is_transient_error(TimeoutError("Timeout")) is True

    def test_string_matching_429(self):
        """Detects 429 in generic exception."""
        assert is_transient_error(Exception("Got 429 rate limit")) is True

    def test_string_matching_503(self):
        """Detects 503 in generic exception."""
        assert is_transient_error(Exception("503 service unavailable")) is True

    def test_string_matching_timeout(self):
        """Detects timeout in generic exception."""
        assert is_transient_error(Exception("connection timeout")) is True

    def test_no_match(self):
        """Returns false for unrelated errors."""
        assert is_transient_error(Exception("Something else")) is False


class TestIsRetryableError:
    """Test is_retryable_error() utility."""

    def test_transient_is_retryable(self):
        """Transient errors are retryable."""
        assert is_retryable_error(TimeoutError("Timeout")) is True

    def test_auth_is_retryable(self):
        """Auth errors are retryable."""
        assert is_retryable_error(AuthError("Auth failed")) is True

    def test_unknown_is_retryable(self):
        """Unknown errors are retryable."""
        assert is_retryable_error(PipelineError("Unknown")) is True

    def test_circuit_open_not_retryable(self):
        """Circuit open errors are not retryable."""
        err = CircuitOpenError("test", retry_after=30.0)
        assert is_retryable_error(err) is False

    def test_generic_exception_classification(self):
        """Generic exceptions are classified and checked."""
        assert is_retryable_error(Exception("timeout")) is True
        assert is_retryable_error(Exception("404 not found")) is False


class TestClassifyHttpStatus:
    """Test classify_http_status() utility."""

    def test_success_codes(self):
        """Success codes return UNKNOWN."""
        assert classify_http_status(200) == ErrorCategory.UNKNOWN
        assert classify_http_status(201) == ErrorCategory.UNKNOWN
        assert classify_http_status(204) == ErrorCategory.UNKNOWN

    def test_auth_codes(self):
        """401 and 302 are AUTH."""
        assert classify_http_status(401) == ErrorCategory.AUTH
        assert classify_http_status(302) == ErrorCategory.AUTH

    def test_rate_limit(self):
        """429 is TRANSIENT."""
        assert classify_http_status(429) == ErrorCategory.TRANSIENT

    def test_permanent_4xx(self):
        """Client errors are PERMANENT."""
        assert classify_http_status(400) == ErrorCategory.PERMANENT
        assert classify_http_status(403) == ErrorCategory.PERMANENT
        assert classify_http_status(404) == ErrorCategory.PERMANENT
        assert classify_http_status(405) == ErrorCategory.PERMANENT
        assert classify_http_status(410) == ErrorCategory.PERMANENT
        assert classify_http_status(422) == ErrorCategory.PERMANENT

    def test_other_4xx(self):
        """Other 4xx are PERMANENT."""
        assert classify_http_status(418) == ErrorCategory.PERMANENT

    def test_transient_5xx(self):
        """Server errors are TRANSIENT."""
        assert classify_http_status(500) == ErrorCategory.TRANSIENT
        assert classify_http_status(502) == ErrorCategory.TRANSIENT
        assert classify_http_status(503) == ErrorCategory.TRANSIENT
        assert classify_http_status(504) == ErrorCategory.TRANSIENT

    def test_other_5xx(self):
        """Other 5xx are TRANSIENT."""
        assert classify_http_status(507) == ErrorCategory.TRANSIENT


class TestClassifyException:
    """Test classify_exception() utility."""

    def test_delta_commit_errors(self):
        """Detects Delta Lake commit conflicts as transient."""
        assert (
            classify_exception(Exception("Failed to commit transaction: 15"))
            == ErrorCategory.TRANSIENT
        )
        assert classify_exception(Exception("transaction conflict")) == ErrorCategory.TRANSIENT
        assert classify_exception(Exception("version conflict")) == ErrorCategory.TRANSIENT
        assert classify_exception(Exception("concurrent modification")) == ErrorCategory.TRANSIENT

    def test_connection_errors(self):
        """Detects connection errors."""
        assert classify_exception(Exception("connection refused")) == ErrorCategory.TRANSIENT
        assert classify_exception(Exception("no route to host")) == ErrorCategory.TRANSIENT
        assert classify_exception(Exception("dns lookup failed")) == ErrorCategory.TRANSIENT

    def test_timeout_errors(self):
        """Detects timeout errors."""
        assert classify_exception(Exception("timeout occurred")) == ErrorCategory.TRANSIENT

    def test_auth_errors(self):
        """Detects auth errors."""
        assert classify_exception(Exception("401 unauthorized")) == ErrorCategory.AUTH
        assert classify_exception(Exception("token expired")) == ErrorCategory.AUTH

    def test_throttling_errors(self):
        """Detects throttling errors."""
        assert classify_exception(Exception("429 rate limit")) == ErrorCategory.TRANSIENT
        assert classify_exception(Exception("throttled")) == ErrorCategory.TRANSIENT

    def test_server_errors(self):
        """Detects server errors."""
        assert classify_exception(Exception("503 unavailable")) == ErrorCategory.TRANSIENT
        assert classify_exception(Exception("502 bad gateway")) == ErrorCategory.TRANSIENT

    def test_permission_errors(self):
        """Detects permission errors."""
        assert classify_exception(Exception("403 forbidden")) == ErrorCategory.PERMANENT
        assert classify_exception(Exception("access denied")) == ErrorCategory.PERMANENT

    def test_not_found_errors(self):
        """Detects not found errors."""
        assert classify_exception(Exception("404 not found")) == ErrorCategory.PERMANENT

    def test_unknown_errors(self):
        """Returns UNKNOWN for unrecognized errors."""
        assert classify_exception(Exception("something weird")) == ErrorCategory.UNKNOWN


class TestWrapException:
    """Test wrap_exception() utility."""

    def test_wrap_auth_error(self):
        """Wraps 401 as AuthError."""
        exc = Exception("401 unauthorized")
        wrapped = wrap_exception(exc)
        assert isinstance(wrapped, AuthError)
        assert wrapped.cause == exc

    def test_wrap_throttling(self):
        """Wraps 429 as ThrottlingError."""
        exc = Exception("429 rate limit")
        wrapped = wrap_exception(exc)
        assert isinstance(wrapped, ThrottlingError)

    def test_wrap_permanent(self):
        """Wraps other permanent errors."""
        exc = Exception("access denied - insufficient permissions")
        wrapped = wrap_exception(exc)
        assert isinstance(wrapped, PermanentError)

    def test_wrap_unknown(self):
        """Wraps unknown errors as default class."""
        exc = Exception("something weird")
        wrapped = wrap_exception(exc)
        assert isinstance(wrapped, PipelineError)
        assert wrapped.category == ErrorCategory.UNKNOWN

    def test_wrap_with_context(self):
        """Can add context when wrapping."""
        exc = Exception("timeout")
        wrapped = wrap_exception(exc, context={"url": "http://example.com"})
        assert wrapped.context["url"] == "http://example.com"
