"""
Tests for error classification and storage error classifiers.
"""

import pytest

from core.errors import (
    AZURE_ERROR_CODES,
    AuthError,
    ConnectionError,
    DeltaTableError,
    KustoError,
    KustoQueryError,
    OneLakeError,
    PermanentError,
    StorageErrorClassifier,
    ThrottlingError,
    TimeoutError,
    TransientError,
    classify_azure_error_code,
)


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


class TestKustoErrorClassifier:
    """Test Kusto error classification."""

    def test_auth_error(self):
        """Test authentication error detection."""
        error = Exception("401 Unauthorized access")
        result = StorageErrorClassifier.classify_kusto_error(error)
        assert isinstance(result, AuthError)
        assert "Kusto authentication failed" in str(result)
        assert result.context["service"] == "kusto"

    def test_throttling_error(self):
        """Test throttling error detection."""
        error = Exception("429 Too Many Requests")
        result = StorageErrorClassifier.classify_kusto_error(error)
        assert isinstance(result, ThrottlingError)
        assert "Kusto throttled" in str(result)
        assert result.context["service"] == "kusto"

    def test_throttling_with_retry_after_header(self):
        """Test throttling error with Retry-After header extraction."""
        # Create a mock exception with response object
        class MockResponse:
            headers = {"x-ms-retry-after-ms": "5000"}

        class MockError(Exception):
            response = MockResponse()

        error = MockError("429 throttled")
        result = StorageErrorClassifier.classify_kusto_error(error)
        assert isinstance(result, ThrottlingError)
        assert result.context["retry_after_ms"] == 5000
        assert result.retry_after == 5.0  # seconds

    def test_throttling_with_standard_retry_after(self):
        """Test throttling error with standard Retry-After header."""
        class MockResponse:
            headers = {"retry-after": "10"}

        class MockError(Exception):
            response = MockResponse()

        error = MockError("throttled")
        result = StorageErrorClassifier.classify_kusto_error(error)
        assert isinstance(result, ThrottlingError)
        assert result.context["retry_after_ms"] == 10000
        assert result.retry_after == 10.0

    def test_timeout_error(self):
        """Test timeout error detection."""
        error = Exception("Query timeout exceeded")
        result = StorageErrorClassifier.classify_kusto_error(error)
        assert isinstance(result, TimeoutError)
        assert "Kusto query timeout" in str(result)

    def test_service_error(self):
        """Test service error detection."""
        error = Exception("503 Service Unavailable")
        result = StorageErrorClassifier.classify_kusto_error(error)
        assert isinstance(result, TransientError)
        assert "Kusto service error" in str(result)

    def test_query_error(self):
        """Test Kusto query error detection."""
        class KustoServiceError(Exception):
            pass

        error = KustoServiceError("Semantic error in query")
        result = StorageErrorClassifier.classify_kusto_error(error)
        assert isinstance(result, KustoQueryError)
        assert "Kusto query error" in str(result)

    def test_generic_kusto_error(self):
        """Test generic Kusto error fallback."""
        error = Exception("Unknown Kusto error")
        result = StorageErrorClassifier.classify_kusto_error(error)
        assert isinstance(result, KustoError)
        assert "Kusto error" in str(result)

    def test_context_merging(self):
        """Test that additional context is merged correctly."""
        error = Exception("Some error")
        custom_context = {"query_id": "12345", "table": "events"}
        result = StorageErrorClassifier.classify_kusto_error(error, context=custom_context)
        assert result.context["service"] == "kusto"
        assert result.context["query_id"] == "12345"
        assert result.context["table"] == "events"


class TestDeltaErrorClassifier:
    """Test Delta table error classification."""

    def test_auth_error(self):
        """Test authentication error detection."""
        error = Exception("401 Unauthorized token")
        result = StorageErrorClassifier.classify_delta_error(error)
        assert isinstance(result, AuthError)
        assert "Delta authentication failed" in str(result)
        assert result.context["service"] == "delta"

    def test_throttling_error(self):
        """Test throttling error detection."""
        error = Exception("429 throttled request")
        result = StorageErrorClassifier.classify_delta_error(error)
        assert isinstance(result, ThrottlingError)
        assert "Delta throttled" in str(result)

    def test_timeout_error(self):
        """Test timeout error detection."""
        error = Exception("Connection timeout occurred")
        result = StorageErrorClassifier.classify_delta_error(error)
        assert isinstance(result, TimeoutError)
        assert "Delta operation timeout" in str(result)

    def test_connection_error(self):
        """Test connection error detection."""
        error = Exception("Network connection failed")
        result = StorageErrorClassifier.classify_delta_error(error)
        assert isinstance(result, ConnectionError)
        assert "Delta connection error" in str(result)

    def test_service_error(self):
        """Test service error detection."""
        error = Exception("503 Service error")
        result = StorageErrorClassifier.classify_delta_error(error)
        assert isinstance(result, TransientError)
        assert "Delta service error" in str(result)

    def test_generic_delta_error(self):
        """Test generic Delta error fallback."""
        error = Exception("Unknown Delta issue")
        result = StorageErrorClassifier.classify_delta_error(error)
        assert isinstance(result, DeltaTableError)
        assert "Delta table error" in str(result)


class TestOneLakeErrorClassifier:
    """Test OneLake error classification."""

    def test_auth_error(self):
        """Test authentication error detection."""
        error = Exception("401 authentication failed")
        result = StorageErrorClassifier.classify_onelake_error(error)
        assert isinstance(result, AuthError)
        assert "OneLake authentication failed" in str(result)
        assert result.context["service"] == "onelake"

    def test_throttling_error(self):
        """Test throttling error detection."""
        error = Exception("429 throttling active")
        result = StorageErrorClassifier.classify_onelake_error(error)
        assert isinstance(result, ThrottlingError)
        assert "OneLake throttled" in str(result)

    def test_timeout_error(self):
        """Test timeout error detection."""
        error = Exception("Request timeout")
        result = StorageErrorClassifier.classify_onelake_error(error)
        assert isinstance(result, TimeoutError)
        assert "OneLake operation timeout" in str(result)

    def test_connection_error(self):
        """Test connection error detection."""
        error = Exception("DNS resolution failed")
        result = StorageErrorClassifier.classify_onelake_error(error)
        assert isinstance(result, ConnectionError)
        assert "OneLake connection error" in str(result)

    def test_service_error(self):
        """Test service error detection."""
        error = Exception("503 service unavailable")
        result = StorageErrorClassifier.classify_onelake_error(error)
        assert isinstance(result, TransientError)
        assert "OneLake service error" in str(result)

    def test_not_found_error(self):
        """Test not found error detection."""
        error = Exception("404 resource not found")
        result = StorageErrorClassifier.classify_onelake_error(error)
        assert isinstance(result, PermanentError)
        assert "OneLake resource not found" in str(result)

    def test_forbidden_error(self):
        """Test forbidden error detection."""
        error = Exception("403 forbidden access")
        result = StorageErrorClassifier.classify_onelake_error(error)
        assert isinstance(result, PermanentError)
        assert "OneLake access denied" in str(result)

    def test_generic_onelake_error(self):
        """Test generic OneLake error fallback."""
        error = Exception("Unknown OneLake problem")
        result = StorageErrorClassifier.classify_onelake_error(error)
        assert isinstance(result, OneLakeError)
        assert "OneLake error" in str(result)


class TestStorageErrorRouting:
    """Test generic storage error routing."""

    def test_kusto_routing(self):
        """Test routing to Kusto classifier."""
        error = Exception("Kusto error")
        result = StorageErrorClassifier.classify_storage_error(error, "kusto")
        assert isinstance(result, KustoError)

    def test_delta_routing(self):
        """Test routing to Delta classifier."""
        error = Exception("Delta error")
        result = StorageErrorClassifier.classify_storage_error(error, "delta")
        assert isinstance(result, DeltaTableError)

    def test_onelake_routing(self):
        """Test routing to OneLake classifier."""
        error = Exception("OneLake error")
        result = StorageErrorClassifier.classify_storage_error(error, "onelake")
        assert isinstance(result, OneLakeError)

    def test_case_insensitive_routing(self):
        """Test that service names are case-insensitive."""
        error = Exception("Error")
        result1 = StorageErrorClassifier.classify_storage_error(error, "KUSTO")
        result2 = StorageErrorClassifier.classify_storage_error(error, "Kusto")
        result3 = StorageErrorClassifier.classify_storage_error(error, "kusto")

        assert isinstance(result1, KustoError)
        assert isinstance(result2, KustoError)
        assert isinstance(result3, KustoError)

    def test_unknown_service_fallback(self):
        """Test fallback for unknown services."""
        error = ValueError("Generic error")
        result = StorageErrorClassifier.classify_storage_error(error, "unknown_service")
        # Should fall back to wrap_exception behavior
        assert result.cause == error
