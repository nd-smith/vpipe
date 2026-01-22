# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
# 
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""
Error classification and exception hierarchy.

Provides:
- ErrorCategory enum for classifying errors
- PipelineError hierarchy for typed exceptions
- Classification utilities for error handling
- Storage error classifiers for Azure services
"""

from core.errors.exceptions import (
    # Enums
    ErrorCategory,
    # Base classes
    PipelineError,
    AuthError,
    TransientError,
    PermanentError,
    CircuitOpenError,
    KafkaError,
    # Transient errors
    ThrottlingError,
    # Classification utilities
    is_auth_error,
    is_transient_error,
    is_retryable_error,
    classify_http_status,
    classify_exception,
    wrap_exception,
)
from core.errors.classifiers import (
    # Constants
    AZURE_ERROR_CODES,
    # Functions
    classify_azure_error_code,
    # Classes
    StorageErrorClassifier,
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
    # Storage classifiers
    "AZURE_ERROR_CODES",
    "classify_azure_error_code",
    "StorageErrorClassifier",
]
