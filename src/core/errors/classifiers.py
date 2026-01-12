"""
Centralized error classification for storage operations.

Provides consistent error handling across Kusto, Delta, and OneLake clients.
Wraps service-specific exceptions into typed PipelineError hierarchy.
"""

from typing import Optional

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


def classify_azure_error_code(error_code: str) -> Optional[str]:
    """
    Classify Azure error code into error category (P2.11).

    Args:
        error_code: Azure error code (AADSTS*, HTTP status, or service-specific)

    Returns:
        Error category: "auth", "throttling", "transient", "timeout", "connection", "permanent", or None
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
    def classify_kusto_error(
        error: Exception, context: Optional[dict] = None
    ) -> PipelineError:
        """
        Classify a Kusto error into appropriate exception type.

        Args:
            error: Original exception
            context: Additional context (merged with default {"service": "kusto"})

        Returns:
            Classified PipelineError subclass
        """
        error_str = str(error).lower()
        ctx = {"service": "kusto"}
        if context:
            ctx.update(context)

        # Auth errors
        if (
            "401" in error_str
            or "unauthorized" in error_str
            or "access rights" in error_str
        ):
            return AuthError(
                f"Kusto authentication failed: {error}",
                cause=error,
                context=ctx,
            )

        # Throttling with Retry-After header extraction (Task F.2)
        if (
            "429" in error_str
            or "throttl" in error_str
            or "too many requests" in error_str
        ):
            # Try to extract retry-after header from exception
            retry_after_ms = None

            # Check for response object in exception
            if hasattr(error, "response") and error.response is not None:
                headers = getattr(error.response, "headers", {})

                # Azure Kusto uses x-ms-retry-after-ms
                if "x-ms-retry-after-ms" in headers:
                    try:
                        retry_after_ms = int(headers["x-ms-retry-after-ms"])
                    except (ValueError, TypeError):
                        pass

                # Standard Retry-After header (in seconds)
                elif "retry-after" in headers:
                    try:
                        retry_after_seconds = int(headers["retry-after"])
                        retry_after_ms = retry_after_seconds * 1000
                    except (ValueError, TypeError):
                        pass

            # Add retry_after to context if found (P2.6 Critical Fix)
            retry_after_seconds = None
            if retry_after_ms is not None:
                ctx["retry_after_ms"] = retry_after_ms
                retry_after_seconds = retry_after_ms / 1000.0

            return ThrottlingError(
                f"Kusto throttled: {error}",
                cause=error,
                context=ctx,
                retry_after=retry_after_seconds,
            )

        # Timeout
        if "timeout" in error_str:
            return TimeoutError(
                f"Kusto query timeout: {error}",
                cause=error,
                context=ctx,
            )

        # Service errors (transient)
        if any(
            marker in error_str
            for marker in ("503", "502", "504", "service unavailable")
        ):
            return TransientError(
                f"Kusto service error: {error}",
                cause=error,
                context=ctx,
            )

        # Query errors (likely permanent - bad query)
        # Check for Kusto-specific error types
        error_type = type(error).__name__
        if "KustoServiceError" in error_type:
            if "semantic error" in error_str or "syntax error" in error_str:
                return KustoQueryError(
                    f"Kusto query error: {error}",
                    cause=error,
                    context=ctx,
                )

        # Default to generic Kusto error
        return KustoError(
            f"Kusto error: {error}",
            cause=error,
            context=ctx,
        )

    @staticmethod
    def classify_delta_error(
        error: Exception, context: Optional[dict] = None
    ) -> PipelineError:
        """
        Classify a Delta table error into appropriate exception type.

        Args:
            error: Original exception
            context: Additional context (merged with default {"service": "delta"})

        Returns:
            Classified PipelineError subclass
        """
        error_str = str(error).lower()
        ctx = {"service": "delta"}
        if context:
            ctx.update(context)

        # Check for auth errors (token expiry, etc.)
        if any(marker in error_str for marker in ("401", "unauthorized", "token")):
            return AuthError(
                f"Delta authentication failed: {error}",
                cause=error,
                context=ctx,
            )

        # Throttling
        if "429" in error_str or "throttl" in error_str:
            return ThrottlingError(
                f"Delta throttled: {error}",
                cause=error,
                context=ctx,
            )

        # Connection/network errors
        if any(
            marker in error_str
            for marker in ("connection", "network", "timeout", "dns")
        ):
            if "timeout" in error_str:
                return TimeoutError(
                    f"Delta operation timeout: {error}",
                    cause=error,
                    context=ctx,
                )
            return ConnectionError(
                f"Delta connection error: {error}",
                cause=error,
                context=ctx,
            )

        # Service errors (transient)
        if any(marker in error_str for marker in ("503", "502", "504")):
            return TransientError(
                f"Delta service error: {error}",
                cause=error,
                context=ctx,
            )

        # Default to generic Delta error
        return DeltaTableError(
            f"Delta table error: {error}",
            cause=error,
            context=ctx,
        )

    @staticmethod
    def classify_onelake_error(
        error: Exception, context: Optional[dict] = None
    ) -> PipelineError:
        """
        Classify an OneLake error into appropriate exception type.

        Args:
            error: Original exception
            context: Additional context (merged with default {"service": "onelake"})

        Returns:
            Classified PipelineError subclass
        """
        error_str = str(error).lower()
        ctx = {"service": "onelake"}
        if context:
            ctx.update(context)

        # Auth errors
        if any(
            marker in error_str
            for marker in ("401", "unauthorized", "authentication", "token expired")
        ):
            return AuthError(
                f"OneLake authentication failed: {error}",
                cause=error,
                context=ctx,
            )

        # Throttling
        if "429" in error_str or "throttl" in error_str:
            return ThrottlingError(
                f"OneLake throttled: {error}",
                cause=error,
                context=ctx,
            )

        # Timeout
        if "timeout" in error_str:
            return TimeoutError(
                f"OneLake operation timeout: {error}",
                cause=error,
                context=ctx,
            )

        # Connection errors
        if any(
            marker in error_str for marker in ("connection", "network", "dns", "socket")
        ):
            return ConnectionError(
                f"OneLake connection error: {error}",
                cause=error,
                context=ctx,
            )

        # Service errors (transient)
        if any(
            marker in error_str
            for marker in ("503", "502", "504", "service unavailable")
        ):
            return TransientError(
                f"OneLake service error: {error}",
                cause=error,
                context=ctx,
            )

        # Not found errors (permanent)
        if "404" in error_str or "not found" in error_str:
            return PermanentError(
                f"OneLake resource not found: {error}",
                cause=error,
                context=ctx,
            )

        # Forbidden (permanent)
        if "403" in error_str or "forbidden" in error_str:
            return PermanentError(
                f"OneLake access denied: {error}",
                cause=error,
                context=ctx,
            )

        # Default to generic OneLake error
        return OneLakeError(
            f"OneLake error: {error}",
            cause=error,
            context=ctx,
        )

    @staticmethod
    def classify_storage_error(
        error: Exception,
        service: str,
        context: Optional[dict] = None,
    ) -> PipelineError:
        """
        Generic storage error classifier with service routing.

        Routes to service-specific classifier based on service name.

        Args:
            error: Original exception
            service: Service name ("kusto", "delta", "onelake")
            context: Additional context

        Returns:
            Classified PipelineError subclass
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
