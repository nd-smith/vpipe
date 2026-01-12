"""
Core types and protocols used across modules.

This module provides base types, enums, and protocol definitions that are
shared across the core library to ensure consistency and type safety.
"""

from enum import Enum
from typing import Protocol, Optional


class ErrorCategory(Enum):
    """
    Classification of error types for handling decisions.

    This enum is used throughout the pipeline to classify errors and determine
    appropriate retry/recovery strategies.

    Categories:
        TRANSIENT: Temporary failures that should retry with backoff
                   (e.g., network timeouts, 429/503 errors)
        AUTH: Authentication failures requiring credential refresh
              (e.g., 401 errors, expired tokens)
        PERMANENT: Non-retriable failures that won't succeed on retry
                   (e.g., 404, validation errors, configuration issues)
        CIRCUIT_OPEN: Circuit breaker is open, rejecting fast without attempting
        UNKNOWN: Unclassified errors, may retry conservatively
    """

    TRANSIENT = "transient"
    AUTH = "auth"
    PERMANENT = "permanent"
    CIRCUIT_OPEN = "circuit_open"
    UNKNOWN = "unknown"


class ErrorClassifier(Protocol):
    """
    Protocol for error classification implementations.

    Different modules (Azure storage, Kafka, HTTP) implement this protocol
    to classify domain-specific errors into standard categories.
    """

    def classify_error(self, error: Exception) -> ErrorCategory:
        """
        Classify an exception into an error category.

        Args:
            error: Exception to classify

        Returns:
            ErrorCategory indicating how to handle this error
        """
        ...

    def is_transient(self, error: Exception) -> bool:
        """
        Check if error is transient (retriable).

        Args:
            error: Exception to check

        Returns:
            True if error may succeed on retry
        """
        ...


class TokenProvider(Protocol):
    """
    Protocol for authentication token providers.

    Implementations provide access tokens for Azure AD, Kafka OAUTH, etc.
    """

    async def get_token(self, scopes: list[str]) -> str:
        """
        Get an access token for the specified scopes.

        Args:
            scopes: List of OAuth scopes required

        Returns:
            Access token string

        Raises:
            AuthError: If token acquisition fails
        """
        ...

    async def refresh_token(self) -> None:
        """
        Force refresh of cached token.

        Raises:
            AuthError: If token refresh fails
        """
        ...


__all__ = [
    "ErrorCategory",
    "ErrorClassifier",
    "TokenProvider",
]
