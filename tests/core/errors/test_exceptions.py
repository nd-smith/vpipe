"""
Tests for exception hierarchy and error classification.
"""

import pytest

from core.errors.exceptions import (
    ErrorCategory,
    PipelineError,
    AuthError,
    TokenExpiredError,
    TransientError,
    ConnectionError,
    TimeoutError,
    ThrottlingError,
    ServiceUnavailableError,
    PermanentError,
    NotFoundError,
    ForbiddenError,
    ValidationError,
    ConfigurationError,
    CircuitOpenError,
    KustoError,
    KustoQueryError,
    DeltaTableError,
    OneLakeError,
    DownloadError,
    AttachmentAuthError,
    AttachmentThrottlingError,
    AttachmentServiceError,
    AttachmentClientError,
    AttachmentNotFoundError,
    AttachmentForbiddenError,
    AttachmentTokenExpiredError,
    is_auth_error,
    is_transient_error,
    is_retryable_error,
    classify_http_status,
    classify_exception,
    wrap_exception,
)


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

    def test_token_expired_error(self):
        """TokenExpiredError is auth error."""
        err = TokenExpiredError("Token expired")
        assert err.category == ErrorCategory.AUTH
        assert isinstance(err, AuthError)


class TestTransientErrors:
    """Test transient error hierarchy."""

    def test_transient_error_category(self):
        """TransientError has TRANSIENT category."""
        err = TransientError("Temporary issue")
        assert err.category == ErrorCategory.TRANSIENT
        assert err.is_retryable is True

    def test_connection_error(self):
        """ConnectionError is transient."""
        err = ConnectionError("Connection failed")
        assert err.category == ErrorCategory.TRANSIENT
        assert isinstance(err, TransientError)

    def test_timeout_error(self):
        """TimeoutError is transient."""
        err = TimeoutError("Request timed out")
        assert err.category == ErrorCategory.TRANSIENT
        assert isinstance(err, TransientError)

    def test_throttling_error(self):
        """ThrottlingError is transient."""
        err = ThrottlingError("Rate limited")
        assert err.category == ErrorCategory.TRANSIENT
        assert err.retry_after is None

    def test_throttling_error_with_retry_after(self):
        """ThrottlingError can include retry-after."""
        err = ThrottlingError("Rate limited", retry_after=60.0)
        assert err.retry_after == 60.0

    def test_service_unavailable_error(self):
        """ServiceUnavailableError is transient."""
        err = ServiceUnavailableError("Service down")
        assert err.category == ErrorCategory.TRANSIENT
        assert isinstance(err, TransientError)


class TestPermanentErrors:
    """Test permanent error hierarchy."""

    def test_permanent_error_category(self):
        """PermanentError has PERMANENT category."""
        err = PermanentError("Fatal error")
        assert err.category == ErrorCategory.PERMANENT
        assert err.is_retryable is False

    def test_not_found_error(self):
        """NotFoundError is permanent."""
        err = NotFoundError("Resource not found")
        assert err.category == ErrorCategory.PERMANENT
        assert isinstance(err, PermanentError)

    def test_forbidden_error(self):
        """ForbiddenError is permanent."""
        err = ForbiddenError("Access denied")
        assert err.category == ErrorCategory.PERMANENT
        assert isinstance(err, PermanentError)

    def test_validation_error(self):
        """ValidationError is permanent."""
        err = ValidationError("Invalid data")
        assert err.category == ErrorCategory.PERMANENT
        assert isinstance(err, PermanentError)

    def test_configuration_error(self):
        """ConfigurationError is permanent."""
        err = ConfigurationError("Bad config")
        assert err.category == ErrorCategory.PERMANENT
        assert isinstance(err, PermanentError)


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


class TestDomainErrors:
    """Test domain-specific errors."""

    def test_kusto_error(self):
        """KustoError has unknown category."""
        err = KustoError("Query failed")
        assert err.category == ErrorCategory.UNKNOWN

    def test_kusto_query_error(self):
        """KustoQueryError inherits from KustoError."""
        err = KustoQueryError("Syntax error")
        assert isinstance(err, KustoError)

    def test_delta_table_error(self):
        """DeltaTableError exists."""
        err = DeltaTableError("Table not found")
        assert err.category == ErrorCategory.UNKNOWN

    def test_onelake_error(self):
        """OneLakeError exists."""
        err = OneLakeError("OneLake error")
        assert err.category == ErrorCategory.UNKNOWN


class TestAttachmentErrors:
    """Test attachment download errors."""

    def test_download_error(self):
        """Base DownloadError has unknown category."""
        err = DownloadError("Download failed")
        assert err.category == ErrorCategory.UNKNOWN

    def test_attachment_auth_error(self):
        """AttachmentAuthError is transient."""
        err = AttachmentAuthError("401 from URL")
        assert err.category == ErrorCategory.TRANSIENT
        assert isinstance(err, DownloadError)

    def test_attachment_throttling_error(self):
        """AttachmentThrottlingError is transient."""
        err = AttachmentThrottlingError("429 from URL")
        assert err.category == ErrorCategory.TRANSIENT
        assert isinstance(err, DownloadError)

    def test_attachment_service_error(self):
        """AttachmentServiceError is transient."""
        err = AttachmentServiceError("503 from URL")
        assert err.category == ErrorCategory.TRANSIENT
        assert isinstance(err, DownloadError)

    def test_attachment_client_error(self):
        """AttachmentClientError is permanent."""
        err = AttachmentClientError("400 from URL")
        assert err.category == ErrorCategory.PERMANENT
        assert isinstance(err, DownloadError)

    def test_attachment_not_found_error(self):
        """AttachmentNotFoundError is permanent."""
        err = AttachmentNotFoundError("404 from URL")
        assert err.category == ErrorCategory.PERMANENT
        assert isinstance(err, DownloadError)

    def test_attachment_forbidden_error(self):
        """AttachmentForbiddenError is permanent."""
        err = AttachmentForbiddenError("403 from URL")
        assert err.category == ErrorCategory.PERMANENT
        assert isinstance(err, DownloadError)

    def test_attachment_token_expired_error(self):
        """AttachmentTokenExpiredError is permanent (can't refresh Xact tokens)."""
        err = AttachmentTokenExpiredError("STS token expired")
        assert err.category == ErrorCategory.PERMANENT
        assert isinstance(err, DownloadError)


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

    def test_typed_non_transient_error(self):
        """Rejects typed permanent error."""
        assert is_transient_error(NotFoundError("Not found")) is False

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

    def test_permanent_not_retryable(self):
        """Permanent errors are not retryable."""
        assert is_retryable_error(NotFoundError("Not found")) is False

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

    def test_already_classified(self):
        """Returns category from PipelineError."""
        err = NotFoundError("Not found")
        assert classify_exception(err) == ErrorCategory.PERMANENT

    def test_delta_commit_errors(self):
        """Detects Delta Lake commit conflicts as transient."""
        assert classify_exception(Exception("Failed to commit transaction: 15")) == ErrorCategory.TRANSIENT
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

    def test_already_pipeline_error(self):
        """Returns same error if already PipelineError."""
        err = NotFoundError("Not found")
        wrapped = wrap_exception(err)
        assert wrapped is err

    def test_add_context_to_pipeline_error(self):
        """Can add context to existing PipelineError."""
        err = NotFoundError("Not found")
        wrapped = wrap_exception(err, context={"file": "test.txt"})
        assert wrapped is err
        assert wrapped.context["file"] == "test.txt"

    def test_wrap_auth_error(self):
        """Wraps 401 as AuthError."""
        exc = Exception("401 unauthorized")
        wrapped = wrap_exception(exc)
        assert isinstance(wrapped, AuthError)
        assert wrapped.cause == exc

    def test_wrap_token_expired(self):
        """Wraps token expired as TokenExpiredError."""
        exc = Exception("token expired")
        wrapped = wrap_exception(exc)
        assert isinstance(wrapped, TokenExpiredError)

    def test_wrap_timeout(self):
        """Wraps timeout as TimeoutError."""
        exc = Exception("connection timeout")
        wrapped = wrap_exception(exc)
        assert isinstance(wrapped, TimeoutError)

    def test_wrap_throttling(self):
        """Wraps 429 as ThrottlingError."""
        exc = Exception("429 rate limit")
        wrapped = wrap_exception(exc)
        assert isinstance(wrapped, ThrottlingError)

    def test_wrap_503(self):
        """Wraps 503 as ServiceUnavailableError."""
        exc = Exception("503 service unavailable")
        wrapped = wrap_exception(exc)
        assert isinstance(wrapped, ServiceUnavailableError)

    def test_wrap_connection(self):
        """Wraps connection errors as ConnectionError."""
        exc = Exception("connection refused")
        wrapped = wrap_exception(exc)
        assert isinstance(wrapped, ConnectionError)

    def test_wrap_404(self):
        """Wraps 404 as NotFoundError."""
        exc = Exception("404 not found")
        wrapped = wrap_exception(exc)
        assert isinstance(wrapped, NotFoundError)

    def test_wrap_403(self):
        """Wraps 403 as ForbiddenError."""
        exc = Exception("403 forbidden")
        wrapped = wrap_exception(exc)
        assert isinstance(wrapped, ForbiddenError)

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

    def test_wrap_with_custom_default(self):
        """Can specify custom default class."""
        exc = Exception("something weird")
        wrapped = wrap_exception(exc, default_class=ValidationError)
        assert isinstance(wrapped, ValidationError)
