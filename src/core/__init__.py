"""
Core library: Reusable, infrastructure-agnostic components.

This package contains battle-tested business logic extracted from the legacy
pipeline, reviewed and refactored for use in the new Kafka-based architecture.

Modules:
    auth        - Azure authentication (AD, SPN, managed identity, Kafka OAUTHBEARER)
    resilience  - Circuit breaker, retry with backoff, rate limiting
    logging     - Structured JSON logging with correlation IDs
    errors      - Error classification and exception hierarchy
    security    - URL validation, SSRF prevention, input sanitization
    download    - Async HTTP download logic (decoupled from storage layer)

Design Principles:
    - No dependencies on Delta Lake, Kusto, or specific storage backends
    - All modules are independently testable
    - Async-first where applicable
    - Type hints throughout
"""

from .types import ErrorCategory, ErrorClassifier, TokenProvider

__version__ = "0.1.0"

__all__ = [
    "ErrorCategory",
    "ErrorClassifier",
    "TokenProvider",
]
