"""
Unified exception hierarchy for verisk_pipeline.

Provides typed exceptions with retry classification to enable
itelligent error handling throughout the pipeline.
"""


# Import ErrorCategory from canonical source to avoid duplicate enum issues
# (comparing enums from different classes always returns False)
from core.types import ErrorCategory


class PipelineError(Exception):
    """
    Base exception for all pipeline errors.

    Attributes:
        message: Human-readable error description
        category: Error classification for retry decisions
        cause: Original exception if wrapping
        context: Additional context dict for debugging
    """

    category: ErrorCategory = ErrorCategory.UNKNOWN

    def __init__(
        self,
        message: str,
        cause: Exception | None = None,
        context: dict | None = None,
    ):
        self.message = message
        self.cause = cause
        self.context = context or {}
        super().__init__(message)

    @property
    def is_retryable(self) -> bool:
        return self.category in (
            ErrorCategory.TRANSIENT,
            ErrorCategory.AUTH,
            ErrorCategory.UNKNOWN,
        )

    @property
    def should_refresh_auth(self) -> bool:
        return self.category == ErrorCategory.AUTH

    def __str__(self) -> str:
        parts = [self.message]
        if self.cause:
            parts.append(f"Caused by: {self.cause}")
        return " | ".join(parts)


# =============================================================================
# Authentication Errors
# =============================================================================


class AuthError(PipelineError):
    """Base class for authentication errors."""

    category = ErrorCategory.AUTH


# =============================================================================
# Network/Connection Errors (Transient)
# =============================================================================


class TransientError(PipelineError):
    """Base class for transient/retriable errors."""

    category = ErrorCategory.TRANSIENT


class ThrottlingError(TransientError):
    """Rate limited (429) - should back off."""

    def __init__(
        self,
        message: str,
        retry_after: float | None = None,
        cause: Exception | None = None,
        context: dict | None = None,
    ):
        super().__init__(message, cause, context)
        self.retry_after = retry_after  # Seconds to wait if provided


# =============================================================================
# Permanent Errors (Don't Retry)
# =============================================================================


class PermanentError(PipelineError):
    """Base class for permanent/non-retriable errors."""

    category = ErrorCategory.PERMANENT


# =============================================================================
# Circuit Breaker Errors
# =============================================================================


class CircuitOpenError(PipelineError):
    """Circuit breaker is open, rejecting requests."""

    category = ErrorCategory.CIRCUIT_OPEN

    def __init__(
        self,
        circuit_name: str,
        retry_after: float,
        cause: Exception | None = None,
    ):
        message = f"Circuit '{circuit_name}' is open"
        super().__init__(message, cause, {"circuit_name": circuit_name})
        self.circuit_name = circuit_name
        self.retry_after = retry_after


# =============================================================================
# Domain-Specific Errors
# =============================================================================


class KafkaError(PipelineError):
    """Error from Kafka operations (producer/consumer)."""

    pass


class KustoError(TransientError):
    """Error from Kusto/Eventhouse operations."""

    pass


class KustoQueryError(PermanentError):
    """KQL query syntax or semantic error (non-retryable)."""

    pass


class DeltaTableError(TransientError):
    """Error from Delta table operations."""

    pass


class OneLakeError(TransientError):
    """Error from OneLake operations."""

    pass


class TimeoutError(TransientError):
    """Operation timeout error (transient, retryable)."""

    pass


class ConnectionError(TransientError):
    """Connection error (transient, retryable)."""

    pass


# =============================================================================
# Error Classification Utilities
# =============================================================================

# Markers for string-based detection (fallback for non-PipelineError exceptions)
AUTH_ERROR_MARKERS = frozenset(
    {
        "401",
        "unauthorized",
        "authentication",
        "token expired",
        "invalid token",
        "access token",
        "refresh token expired",
        "aadsts700082",
        "aadsts70043",  # Azure AD token expiry codes
    }
)

TRANSIENT_ERROR_MARKERS = frozenset(
    {
        "429",
        "503",
        "502",
        "504",
        "timeout",
        "connection",
        "throttl",
        "rate limit",
        "temporarily unavailable",
        "service unavailable",
        "gateway",
    }
)


def is_auth_error(exc: Exception) -> bool:
    """
    Check if exception is authentication-related.

    Returns True if this is an auth error that should trigger token refresh.
    """
    # Check typed exceptions first
    if isinstance(exc, PipelineError):
        return exc.category == ErrorCategory.AUTH

    # Fall back to string matching
    error_str = str(exc).lower()
    return any(marker in error_str for marker in AUTH_ERROR_MARKERS)


def is_transient_error(exc: Exception) -> bool:
    """
    Check if exception is transient (retriable).

    Returns True if this is a transient error that may succeed on retry.
    """
    # Check typed exceptions first
    if isinstance(exc, PipelineError):
        return exc.category == ErrorCategory.TRANSIENT

    # Fall back to string matching
    error_str = str(exc).lower()
    return any(marker in error_str for marker in TRANSIENT_ERROR_MARKERS)


def is_retryable_error(exc: Exception) -> bool:
    """
    Check if exception should be retried.

    Retryable errors include:
    - Transient errors (connection, timeout, 5xx)
    - Auth errors (after token refresh)
    - Unknown errors (conservative retry)

    Non-retryable:
    - Permanent errors (404, 403, validation)
    - Circuit open errors (should wait)
    """
    if isinstance(exc, PipelineError):
        return exc.is_retryable

    category = classify_exception(exc)
    return category in (
        ErrorCategory.TRANSIENT,
        ErrorCategory.AUTH,
        ErrorCategory.UNKNOWN,
    )


def classify_http_status(status_code: int) -> ErrorCategory:
    """Classify HTTP status code into error category."""
    if 200 <= status_code < 300:
        return ErrorCategory.UNKNOWN  # Not an error

    # Auth redirects (302 = redirect to login page)
    if status_code == 302:
        return ErrorCategory.AUTH

    if status_code == 401:
        return ErrorCategory.AUTH

    if status_code == 429:
        return ErrorCategory.TRANSIENT  # Rate limited

    if status_code in (403, 404, 400, 405, 410, 422):
        return ErrorCategory.PERMANENT  # Client errors, won't fix with retry

    if 400 <= status_code < 500:
        return ErrorCategory.PERMANENT  # Other 4xx

    if status_code in (500, 502, 503, 504):
        return ErrorCategory.TRANSIENT  # Server errors, may recover

    if status_code >= 500:
        return ErrorCategory.TRANSIENT  # Other 5xx

    return ErrorCategory.UNKNOWN


def classify_os_error(error: OSError) -> ErrorCategory:
    """
    Classify OSError by errno into error category.

    Conservative classification: only mark as PERMANENT if certain.
    Disk full (ENOSPC), read-only filesystem (EROFS), permission denied (EACCES/EPERM).
    """
    import errno

    permanent_errnos = (errno.ENOSPC, errno.EROFS, errno.EACCES, errno.EPERM)
    return ErrorCategory.PERMANENT if error.errno in permanent_errnos else ErrorCategory.TRANSIENT


def classify_exception(exc: Exception) -> ErrorCategory:
    """Classify an exception into error category."""
    # Already classified
    if isinstance(exc, PipelineError):
        return exc.category

    # Check exception type
    exc_type = type(exc).__name__.lower()
    exc_str = str(exc).lower()

    # Delta Lake commit conflicts (delta-rs error code 15 = version conflict)
    # These are transient and should be retried with backoff
    delta_conflict_markers = (
        "commitfailederror",
        "failed to commit transaction",
        "transaction conflict",
        "version conflict",
        "concurrent modification",
    )
    if any(m in exc_type or m in exc_str for m in delta_conflict_markers):
        return ErrorCategory.TRANSIENT

    # Connection errors
    connection_markers = (
        "connectionerror",
        "connection refused",
        "connection reset",
        "connection aborted",
        "no route to host",
        "network unreachable",
        "name resolution",
        "dns",
        "socket",
        "broken pipe",
    )
    if any(m in exc_type or m in exc_str for m in connection_markers):
        return ErrorCategory.TRANSIENT

    # Timeout errors
    if "timeout" in exc_type or "timeout" in exc_str:
        return ErrorCategory.TRANSIENT

    # Auth errors (302 = redirect to login, 401 = unauthorized)
    auth_markers = (
        "302",
        "401",
        "unauthorized",
        "authentication",
        "token expired",
        "invalid token",
    )
    if any(m in exc_str for m in auth_markers):
        return ErrorCategory.AUTH

    # Throttling
    if "429" in exc_str or "throttl" in exc_str or "rate limit" in exc_str:
        return ErrorCategory.TRANSIENT

    # Server errors
    if "503" in exc_str or "502" in exc_str or "504" in exc_str:
        return ErrorCategory.TRANSIENT

    # Permission errors (not auth - actual permissions)
    if "403" in exc_str or "forbidden" in exc_str or "access denied" in exc_str:
        return ErrorCategory.PERMANENT

    # Not found
    if "404" in exc_str or "not found" in exc_str:
        return ErrorCategory.PERMANENT

    return ErrorCategory.UNKNOWN


def wrap_exception(
    exc: Exception,
    default_class: type = PipelineError,
    context: dict | None = None,
) -> PipelineError:
    """Wrap a generic exception in appropriate PipelineError subclass."""
    if isinstance(exc, PipelineError):
        if context:
            exc.context.update(context)
        return exc

    category = classify_exception(exc)
    exc_str = str(exc).lower()
    context = context or {}

    # Add error details to context dict instead of using specialized classes
    if "timeout" in exc_str:
        context["error_type"] = "timeout"
    elif "429" in exc_str or "throttl" in exc_str:
        context["error_type"] = "throttling"
    elif "503" in exc_str:
        context["error_type"] = "service_unavailable"
    elif "404" in exc_str or "not found" in exc_str:
        context["error_type"] = "not_found"
    elif "403" in exc_str or "forbidden" in exc_str:
        context["error_type"] = "forbidden"
    elif "expired" in exc_str:
        context["error_type"] = "token_expired"

    # Map to base exception types by category
    if category == ErrorCategory.AUTH:
        return AuthError(str(exc), cause=exc, context=context)

    if category == ErrorCategory.TRANSIENT:
        # Use ThrottlingError if rate limited (has retry_after logic)
        if "429" in exc_str or "throttl" in exc_str:
            return ThrottlingError(str(exc), cause=exc, context=context)
        return TransientError(str(exc), cause=exc, context=context)

    if category == ErrorCategory.PERMANENT:
        return PermanentError(str(exc), cause=exc, context=context)

    # Default wrapper
    return default_class(str(exc), cause=exc, context=context)
