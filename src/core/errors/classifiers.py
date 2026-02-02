"""
Centralized error classification for storage operations.

Provides consistent error handling across Kusto, Delta, and OneLake clients.
Wraps service-specific exceptions into typed PipelineError hierarchy.
"""


import contextlib

from core.errors.exceptions import (
    AuthError,
    ConnectionError,
    DeltaTableError,
    KustoError,
    KustoQueryError,
    OneLakeError,
    PermanentError,
    PipelineError,
    ThrottlingError,
    TimeoutError,
    TransientError,
    wrap_exception,
)

# Azure error code classifications (P2.11)
AZURE_ERROR_CODES = {
    # Authentication errors (AADSTS codes)
    "auth_errors": [
        "AADSTS50058",  # Silent sign-in request failed
        "AADSTS50079",  # MFA required
        "AADSTS50076",  # MFA authentication required
        "AADSTS70008",  # Expired authorization code
        "AADSTS700016",  # Invalid application identifier
        "AADSTS700082",  # Invalid client secret
        "AADSTS7000215",  # Invalid client secret provided
        "AADSTS7000222",  # Expired client secret
        "AADSTS90002",  # Tenant not found
    ],
    # Throttling errors
    "throttling_errors": [
        "429",  # Too Many Requests
        "AADSTS50196",  # Token issuance rate exceeded
        "AADSTS90015",  # Too many requests (query string limit)
    ],
    # Transient errors (retry recommended)
    "transient_errors": [
        "503",  # Service Unavailable
        "502",  # Bad Gateway
        "504",  # Gateway Timeout
        "AADSTS50001",  # Resource not found (transient)
        "AADSTS90033",  # Request temporarily failed
        "AADSTS90019",  # Service temporarily unavailable
    ],
    # Azure Storage specific
    "storage_errors": {
        "InternalError": "transient",
        "OperationTimedOut": "timeout",
        "ServerBusy": "throttling",
        "ServiceUnavailable": "transient",
        "AuthenticationFailed": "auth",
        "AccountIsDisabled": "permanent",
        "InsufficientAccountPermissions": "auth",
        "ContainerNotFound": "permanent",
        "BlobNotFound": "permanent",
    },
    # Azure SQL / Kusto errors
    "kusto_sql_errors": {
        "40501": "throttling",  # Service busy
        "40613": "transient",  # Database unavailable
        "49918": "throttling",  # Request limit exceeded
        "10054": "connection",  # Connection forcibly closed
        "40197": "transient",  # Service error processing request
        "40540": "transient",  # Service encountered error
    },
}


def classify_azure_error_code(error_code: str) -> str | None:
    """
    Classify Azure error code into error category (P2.11).

    Returns error category: "auth", "throttling", "transient", "timeout", "connection", "permanent", or None.
    """
    error_code = str(error_code).strip()

    # Check AADSTS codes
    if error_code in AZURE_ERROR_CODES["auth_errors"]:
        return "auth"
    if error_code in AZURE_ERROR_CODES["throttling_errors"]:
        return "throttling"
    if error_code in AZURE_ERROR_CODES["transient_errors"]:
        return "transient"

    # Check Azure Storage error codes
    if error_code in AZURE_ERROR_CODES["storage_errors"]:
        return AZURE_ERROR_CODES["storage_errors"][error_code]

    # Check Kusto/SQL error codes
    if error_code in AZURE_ERROR_CODES["kusto_sql_errors"]:
        return AZURE_ERROR_CODES["kusto_sql_errors"][error_code]

    # HTTP status codes
    if error_code == "401":
        return "auth"
    if error_code == "429":
        return "throttling"
    if error_code in ("503", "502", "504"):
        return "transient"
    if error_code in ("403", "404"):
        return "permanent"

    return None


class StorageErrorClassifier:
    """
    Centralized error classification for storage services.

    Provides consistent error categorization across Kusto, Delta, and OneLake.
    """

    @staticmethod
    def _classify_common_patterns(
        error: Exception,
        service: str,
        error_str: str,
        error_context: dict,
    ) -> PipelineError | None:
        """
        Classify common error patterns shared across storage services.
        Returns PipelineError if pattern matches, None if service-specific handling needed.
        """
        # Auth errors
        if any(m in error_str for m in ("401", "unauthorized", "authentication", "token")):
            return AuthError(
                f"{service} authentication failed: {error}",
                cause=error,
                context=error_context,
            )

        # Throttling
        if "429" in error_str or "throttl" in error_str:
            return ThrottlingError(
                f"{service} throttled: {error}",
                cause=error,
                context=error_context,
            )

        # Timeout
        if "timeout" in error_str:
            return TimeoutError(
                f"{service} operation timeout: {error}",
                cause=error,
                context=error_context,
            )

        # Connection/network errors
        if any(m in error_str for m in ("connection", "network", "dns", "socket")):
            return ConnectionError(
                f"{service} connection error: {error}",
                cause=error,
                context=error_context,
            )

        # Service errors (transient)
        if any(m in error_str for m in ("503", "502", "504", "service unavailable")):
            return TransientError(
                f"{service} service error: {error}",
                cause=error,
                context=error_context,
            )

        # Not found (permanent)
        if "404" in error_str or "not found" in error_str:
            return PermanentError(
                f"{service} resource not found: {error}",
                cause=error,
                context=error_context,
            )

        # Forbidden (permanent)
        if "403" in error_str or "forbidden" in error_str:
            return PermanentError(
                f"{service} access denied: {error}",
                cause=error,
                context=error_context,
            )

        return None

    @staticmethod
    def classify_kusto_error(
        error: Exception, context: dict | None = None
    ) -> PipelineError:
        """Classify a Kusto error into appropriate exception type."""
        error_str = str(error).lower()
        error_context = {"service": "kusto"}
        if context:
            error_context.update(context)

        # Kusto-specific: Throttling with Retry-After header extraction
        if "429" in error_str or "throttl" in error_str:
            retry_after_ms = None
            if hasattr(error, "response") and error.response is not None:
                headers = getattr(error.response, "headers", {})
                if "x-ms-retry-after-ms" in headers:
                    with contextlib.suppress(ValueError, TypeError):
                        retry_after_ms = int(headers["x-ms-retry-after-ms"])
                elif "retry-after" in headers:
                    with contextlib.suppress(ValueError, TypeError):
                        retry_after_ms = int(headers["retry-after"]) * 1000

            retry_after_seconds = retry_after_ms / 1000.0 if retry_after_ms else None
            if retry_after_ms:
                error_context["retry_after_ms"] = retry_after_ms

            return ThrottlingError(
                f"Kusto throttled: {error}",
                cause=error,
                context=error_context,
                retry_after=retry_after_seconds,
            )

        # Kusto-specific: Query errors (syntax/semantic errors)
        error_type = type(error).__name__
        if "KustoServiceError" in error_type:
            if "semantic error" in error_str or "syntax error" in error_str:
                return KustoQueryError(
                    f"Kusto query error: {error}",
                    cause=error,
                    context=error_context,
                )

        # Check common patterns
        common_error = StorageErrorClassifier._classify_common_patterns(
            error, "Kusto", error_str, error_context
        )
        if common_error:
            return common_error

        # Default to generic Kusto error
        return KustoError(
            f"Kusto error: {error}",
            cause=error,
            context=error_context,
        )

    @staticmethod
    def classify_delta_error(
        error: Exception, context: dict | None = None
    ) -> PipelineError:
        """Classify a Delta table error into appropriate exception type."""
        error_str = str(error).lower()
        error_context = {"service": "delta"}
        if context:
            error_context.update(context)

        # Check common patterns
        common_error = StorageErrorClassifier._classify_common_patterns(
            error, "Delta", error_str, error_context
        )
        if common_error:
            return common_error

        # Default to generic Delta error
        return DeltaTableError(
            f"Delta table error: {error}",
            cause=error,
            context=error_context,
        )

    @staticmethod
    def classify_onelake_error(
        error: Exception, context: dict | None = None
    ) -> PipelineError:
        """Classify an OneLake error into appropriate exception type."""
        error_str = str(error).lower()
        error_context = {"service": "onelake"}
        if context:
            error_context.update(context)

        # Check common patterns
        common_error = StorageErrorClassifier._classify_common_patterns(
            error, "OneLake", error_str, error_context
        )
        if common_error:
            return common_error

        # Default to generic OneLake error
        return OneLakeError(
            f"OneLake error: {error}",
            cause=error,
            context=error_context,
        )

    @staticmethod
    def classify_storage_error(
        error: Exception,
        service: str,
        context: dict | None = None,
    ) -> PipelineError:
        """
        Generic storage error classifier with service routing.

        Routes to service-specific classifier based on service name.
        """
        service = service.lower()

        if service == "kusto":
            return StorageErrorClassifier.classify_kusto_error(error, context)
        elif service == "delta":
            return StorageErrorClassifier.classify_delta_error(error, context)
        elif service == "onelake":
            return StorageErrorClassifier.classify_onelake_error(error, context)
        else:
            # Fall back to generic classification
            return wrap_exception(error, context=context)
