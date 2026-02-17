"""
Error classification and exception hierarchy.

Provides:
- ErrorCategory enum for classifying errors
- PipelineError hierarchy for typed exceptions
- Classification utilities for error handling
"""

from core.errors.exceptions import (
    AuthError,
    CircuitOpenError,
    # Enums
    ErrorCategory,
    KafkaError,
    PermanentError,
    # Base classes
    PipelineError,
    # Transient errors
    ThrottlingError,
    TransientError,
    classify_exception,
    classify_http_status,
    # Classification utilities
    is_auth_error,
    is_retryable_error,
    is_transient_error,
    wrap_exception,
)

__all__ = [
    # Enums
    "ErrorCategory",
    # Base classes
    "PipelineError",
    "AuthError",
    "TransientError",
    "PermanentError",
    "CircuitOpenError",
    "KafkaError",
    # Transient errors
    "ThrottlingError",
    # Classification utilities
    "is_auth_error",
    "is_transient_error",
    "is_retryable_error",
    "classify_http_status",
    "classify_exception",
    "wrap_exception",
]
