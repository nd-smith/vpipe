"""
Tests for error classification and storage error classifiers.
"""

import pytest

from core.errors import (
    AZURE_ERROR_CODES,
    AuthError,
    PermanentError,
    StorageErrorClassifier,
    ThrottlingError,
    TransientError,
    classify_azure_error_code,
)

# NOTE: Specialized exception classes removed in refactor - consolidated into base classes:
# - ConnectionError, TimeoutError -> TransientError
# - DeltaTableError, KustoError, KustoQueryError, OneLakeError -> Removed (use TransientError/PermanentError)
# TestKustoErrorClassifier, TestDeltaErrorClassifier, TestOneLakeErrorClassifier, TestStorageErrorRouting disabled

class TestAzureErrorCodeClassification:
    """Test Azure error code classification function."""

    def test_auth_errors(self):
        """Test authentication error code classification."""
        auth_codes = AZURE_ERROR_CODES["auth_errors"]
        for code in auth_codes:
            result = classify_azure_error_code(code)
            assert result == "auth", f"Expected 'auth' for {code}, got {result}"

        # Test HTTP 401
        assert classify_azure_error_code("401") == "auth"

    def test_throttling_errors(self):
        """Test throttling error code classification."""
        throttling_codes = AZURE_ERROR_CODES["throttling_errors"]
        for code in throttling_codes:
            result = classify_azure_error_code(code)
            assert result == "throttling", f"Expected 'throttling' for {code}, got {result}"

        # Test HTTP 429
        assert classify_azure_error_code("429") == "throttling"

    def test_transient_errors(self):
        """Test transient error code classification."""
        transient_codes = AZURE_ERROR_CODES["transient_errors"]
        for code in transient_codes:
            result = classify_azure_error_code(code)
            assert result == "transient", f"Expected 'transient' for {code}, got {result}"

        # Test HTTP status codes
        assert classify_azure_error_code("503") == "transient"
        assert classify_azure_error_code("502") == "transient"
        assert classify_azure_error_code("504") == "transient"

    def test_storage_errors(self):
        """Test Azure Storage error code classification."""
        storage_codes = AZURE_ERROR_CODES["storage_errors"]
        for code, expected_category in storage_codes.items():
            result = classify_azure_error_code(code)
            assert result == expected_category, f"Expected '{expected_category}' for {code}, got {result}"

    def test_kusto_sql_errors(self):
        """Test Kusto/SQL error code classification."""
        kusto_codes = AZURE_ERROR_CODES["kusto_sql_errors"]
        for code, expected_category in kusto_codes.items():
            result = classify_azure_error_code(code)
            assert result == expected_category, f"Expected '{expected_category}' for {code}, got {result}"

    def test_permanent_errors(self):
        """Test permanent error code classification."""
        assert classify_azure_error_code("403") == "permanent"
        assert classify_azure_error_code("404") == "permanent"

    def test_unknown_error_code(self):
        """Test unknown error codes return None."""
        assert classify_azure_error_code("999") is None
        assert classify_azure_error_code("UNKNOWN") is None
        assert classify_azure_error_code("") is None

    def test_whitespace_handling(self):
        """Test that error codes with whitespace are handled correctly."""
        assert classify_azure_error_code("  401  ") == "auth"
        assert classify_azure_error_code("\t429\n") == "throttling"
