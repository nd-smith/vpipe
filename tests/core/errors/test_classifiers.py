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
    wrap_exception,
)
from core.errors.exceptions import (
    ConnectionError,
    DeltaTableError,
    KustoError,
    KustoQueryError,
    OneLakeError,
    PipelineError,
    TimeoutError,
)


class TestAzureErrorCodeClassification:

    def test_auth_errors(self):
        auth_codes = AZURE_ERROR_CODES["auth_errors"]
        for code in auth_codes:
            result = classify_azure_error_code(code)
            assert result == "auth", f"Expected 'auth' for {code}, got {result}"

        assert classify_azure_error_code("401") == "auth"

    def test_throttling_errors(self):
        throttling_codes = AZURE_ERROR_CODES["throttling_errors"]
        for code in throttling_codes:
            result = classify_azure_error_code(code)
            assert result == "throttling", f"Expected 'throttling' for {code}, got {result}"

        assert classify_azure_error_code("429") == "throttling"

    def test_transient_errors(self):
        transient_codes = AZURE_ERROR_CODES["transient_errors"]
        for code in transient_codes:
            result = classify_azure_error_code(code)
            assert result == "transient", f"Expected 'transient' for {code}, got {result}"

        assert classify_azure_error_code("503") == "transient"
        assert classify_azure_error_code("502") == "transient"
        assert classify_azure_error_code("504") == "transient"

    def test_storage_errors(self):
        storage_codes = AZURE_ERROR_CODES["storage_errors"]
        for code, expected_category in storage_codes.items():
            result = classify_azure_error_code(code)
            assert result == expected_category, f"Expected '{expected_category}' for {code}, got {result}"

    def test_kusto_sql_errors(self):
        kusto_codes = AZURE_ERROR_CODES["kusto_sql_errors"]
        for code, expected_category in kusto_codes.items():
            result = classify_azure_error_code(code)
            assert result == expected_category, f"Expected '{expected_category}' for {code}, got {result}"

    def test_permanent_errors(self):
        assert classify_azure_error_code("403") == "permanent"
        assert classify_azure_error_code("404") == "permanent"

    def test_unknown_error_code(self):
        assert classify_azure_error_code("999") is None
        assert classify_azure_error_code("UNKNOWN") is None
        assert classify_azure_error_code("") is None

    def test_whitespace_handling(self):
        assert classify_azure_error_code("  401  ") == "auth"
        assert classify_azure_error_code("\t429\n") == "throttling"

    def test_integer_code_converted_to_string(self):
        assert classify_azure_error_code(401) == "auth"
        assert classify_azure_error_code(429) == "throttling"


class TestCommonPatterns:
    """Test StorageErrorClassifier._classify_common_patterns via public methods."""

    def test_auth_error_from_401(self):
        error = Exception("HTTP 401 Unauthorized")
        result = StorageErrorClassifier.classify_kusto_error(error)
        assert isinstance(result, AuthError)
        assert "authentication failed" in result.message

    def test_auth_error_from_unauthorized(self):
        error = Exception("Request was unauthorized")
        result = StorageErrorClassifier.classify_delta_error(error)
        assert isinstance(result, AuthError)

    def test_auth_error_from_authentication(self):
        error = Exception("Authentication required for resource")
        result = StorageErrorClassifier.classify_onelake_error(error)
        assert isinstance(result, AuthError)

    def test_auth_error_from_token(self):
        error = Exception("Token has expired")
        result = StorageErrorClassifier.classify_delta_error(error)
        assert isinstance(result, AuthError)

    def test_throttling_from_429(self):
        error = Exception("HTTP 429 Too Many Requests")
        result = StorageErrorClassifier.classify_delta_error(error)
        assert isinstance(result, ThrottlingError)

    def test_throttling_from_throttle_keyword(self):
        error = Exception("Request was throttled by the server")
        result = StorageErrorClassifier.classify_delta_error(error)
        assert isinstance(result, ThrottlingError)

    def test_timeout_error(self):
        error = Exception("Operation timed out after 30s timeout")
        result = StorageErrorClassifier.classify_delta_error(error)
        assert isinstance(result, TimeoutError)
        assert "timeout" in result.message.lower()

    def test_connection_error(self):
        error = Exception("Connection refused by server")
        result = StorageErrorClassifier.classify_delta_error(error)
        assert isinstance(result, ConnectionError)

    def test_network_error(self):
        error = Exception("Network unreachable")
        result = StorageErrorClassifier.classify_delta_error(error)
        assert isinstance(result, ConnectionError)

    def test_dns_error(self):
        error = Exception("DNS resolution failed")
        result = StorageErrorClassifier.classify_delta_error(error)
        assert isinstance(result, ConnectionError)

    def test_socket_error(self):
        error = Exception("Socket closed unexpectedly")
        result = StorageErrorClassifier.classify_delta_error(error)
        assert isinstance(result, ConnectionError)

    def test_503_transient(self):
        error = Exception("HTTP 503 Service Unavailable")
        result = StorageErrorClassifier.classify_delta_error(error)
        assert isinstance(result, TransientError)
        assert "service error" in result.message.lower()

    def test_502_transient(self):
        error = Exception("HTTP 502 Bad Gateway")
        result = StorageErrorClassifier.classify_delta_error(error)
        assert isinstance(result, TransientError)

    def test_504_transient(self):
        error = Exception("HTTP 504 Gateway Timeout")
        # 504 has "timeout" in the lowercased string, so it matches timeout first
        result = StorageErrorClassifier.classify_delta_error(error)
        # The "timeout" keyword is checked before 504, so this becomes a TimeoutError
        assert isinstance(result, (TimeoutError, TransientError))

    def test_service_unavailable_transient(self):
        error = Exception("The service unavailable right now")
        result = StorageErrorClassifier.classify_delta_error(error)
        assert isinstance(result, TransientError)

    def test_404_permanent(self):
        error = Exception("HTTP 404 Not Found")
        result = StorageErrorClassifier.classify_delta_error(error)
        assert isinstance(result, PermanentError)
        assert "not found" in result.message.lower()

    def test_not_found_permanent(self):
        error = Exception("Resource not found in storage")
        result = StorageErrorClassifier.classify_delta_error(error)
        assert isinstance(result, PermanentError)

    def test_403_permanent(self):
        error = Exception("HTTP 403 Forbidden")
        result = StorageErrorClassifier.classify_delta_error(error)
        assert isinstance(result, PermanentError)
        assert "access denied" in result.message.lower()

    def test_forbidden_permanent(self):
        error = Exception("Access is forbidden for this resource")
        result = StorageErrorClassifier.classify_delta_error(error)
        assert isinstance(result, PermanentError)

    def test_unrecognized_error_returns_none_from_common(self):
        error = Exception("Something completely unexpected happened")
        result = StorageErrorClassifier.classify_delta_error(error)
        # Falls through common patterns to service-specific default
        assert isinstance(result, DeltaTableError)


class TestKustoErrorClassifier:

    def test_throttling_with_retry_after_ms_header(self):
        error = Exception("429 Too Many Requests")
        response = type("Response", (), {
            "headers": {"x-ms-retry-after-ms": "5000"}
        })()
        error.response = response
        result = StorageErrorClassifier.classify_kusto_error(error)
        assert isinstance(result, ThrottlingError)
        assert result.retry_after == 5.0
        assert result.context["retry_after_ms"] == 5000

    def test_throttling_with_retry_after_seconds_header(self):
        error = Exception("429 Too Many Requests")
        response = type("Response", (), {
            "headers": {"retry-after": "10"}
        })()
        error.response = response
        result = StorageErrorClassifier.classify_kusto_error(error)
        assert isinstance(result, ThrottlingError)
        assert result.retry_after == 10.0
        assert result.context["retry_after_ms"] == 10000

    def test_throttling_with_invalid_retry_after_header(self):
        error = Exception("429 Too Many Requests")
        response = type("Response", (), {
            "headers": {"x-ms-retry-after-ms": "not-a-number"}
        })()
        error.response = response
        result = StorageErrorClassifier.classify_kusto_error(error)
        assert isinstance(result, ThrottlingError)
        assert result.retry_after is None

    def test_throttling_without_response(self):
        error = Exception("Request was throttled")
        result = StorageErrorClassifier.classify_kusto_error(error)
        assert isinstance(result, ThrottlingError)
        assert result.retry_after is None

    def test_throttling_with_none_response(self):
        error = Exception("429 rate limited")
        error.response = None
        result = StorageErrorClassifier.classify_kusto_error(error)
        assert isinstance(result, ThrottlingError)
        assert result.retry_after is None

    def test_throttling_with_no_headers(self):
        error = Exception("429 throttled")
        error.response = type("Response", (), {})()
        result = StorageErrorClassifier.classify_kusto_error(error)
        assert isinstance(result, ThrottlingError)
        assert result.retry_after is None

    def test_kusto_service_error_semantic(self):
        # Simulate KustoServiceError with semantic error message
        error_cls = type("KustoServiceError", (Exception,), {})
        error = error_cls("Query has a semantic error near column 'foo'")
        result = StorageErrorClassifier.classify_kusto_error(error)
        assert isinstance(result, KustoQueryError)

    def test_kusto_service_error_syntax(self):
        error_cls = type("KustoServiceError", (Exception,), {})
        error = error_cls("syntax error at line 3")
        result = StorageErrorClassifier.classify_kusto_error(error)
        assert isinstance(result, KustoQueryError)

    def test_kusto_service_error_non_query_falls_through(self):
        # KustoServiceError without syntax/semantic -> falls to common patterns or default
        error_cls = type("KustoServiceError", (Exception,), {})
        error = error_cls("Internal server error on cluster")
        result = StorageErrorClassifier.classify_kusto_error(error)
        # No common pattern match for "internal server error" alone -> KustoError default
        assert isinstance(result, KustoError)

    def test_non_kusto_service_error_with_semantic_keyword(self):
        # Regular Exception with "semantic error" should NOT match KustoServiceError check
        error = Exception("semantic error in query")
        result = StorageErrorClassifier.classify_kusto_error(error)
        # type(error).__name__ is "Exception", not "KustoServiceError"
        assert not isinstance(result, KustoQueryError)

    def test_common_pattern_auth_recognized(self):
        error = Exception("authentication required")
        result = StorageErrorClassifier.classify_kusto_error(error)
        assert isinstance(result, AuthError)
        assert "Kusto" in result.message

    def test_default_kusto_error(self):
        error = Exception("something weird happened")
        result = StorageErrorClassifier.classify_kusto_error(error)
        assert isinstance(result, KustoError)
        assert "Kusto error" in result.message

    def test_context_merged(self):
        error = Exception("something broke")
        result = StorageErrorClassifier.classify_kusto_error(error, context={"query": "test"})
        assert result.context["service"] == "kusto"
        assert result.context["query"] == "test"

    def test_no_context_default(self):
        error = Exception("error")
        result = StorageErrorClassifier.classify_kusto_error(error)
        assert result.context == {"service": "kusto"}

    def test_cause_is_preserved(self):
        error = ValueError("bad value")
        result = StorageErrorClassifier.classify_kusto_error(error)
        assert result.cause is error

    def test_throttling_preempts_common_pattern(self):
        # Throttling is checked before common patterns in kusto classifier
        error = Exception("429 Too Many Requests with connection reset")
        result = StorageErrorClassifier.classify_kusto_error(error)
        assert isinstance(result, ThrottlingError)


class TestDeltaErrorClassifier:

    def test_default_delta_error(self):
        error = Exception("something went wrong with delta")
        result = StorageErrorClassifier.classify_delta_error(error)
        assert isinstance(result, DeltaTableError)
        assert "Delta table error" in result.message

    def test_context_merged(self):
        error = Exception("delta oops")
        result = StorageErrorClassifier.classify_delta_error(error, context={"table": "events"})
        assert result.context["service"] == "delta"
        assert result.context["table"] == "events"

    def test_no_context_default(self):
        error = Exception("delta error")
        result = StorageErrorClassifier.classify_delta_error(error)
        assert result.context == {"service": "delta"}

    def test_auth_error_classified(self):
        error = Exception("401 Unauthorized access")
        result = StorageErrorClassifier.classify_delta_error(error)
        assert isinstance(result, AuthError)
        assert "Delta" in result.message

    def test_cause_preserved(self):
        original = RuntimeError("original")
        result = StorageErrorClassifier.classify_delta_error(original)
        assert result.cause is original


class TestOneLakeErrorClassifier:

    def test_default_onelake_error(self):
        error = Exception("onelake storage issue")
        result = StorageErrorClassifier.classify_onelake_error(error)
        assert isinstance(result, OneLakeError)
        assert "OneLake error" in result.message

    def test_context_merged(self):
        error = Exception("onelake oops")
        result = StorageErrorClassifier.classify_onelake_error(error, context={"path": "/data"})
        assert result.context["service"] == "onelake"
        assert result.context["path"] == "/data"

    def test_no_context_default(self):
        error = Exception("onelake error")
        result = StorageErrorClassifier.classify_onelake_error(error)
        assert result.context == {"service": "onelake"}

    def test_auth_error_classified(self):
        error = Exception("token expired for onelake access")
        result = StorageErrorClassifier.classify_onelake_error(error)
        assert isinstance(result, AuthError)
        assert "OneLake" in result.message

    def test_cause_preserved(self):
        original = RuntimeError("original")
        result = StorageErrorClassifier.classify_onelake_error(original)
        assert result.cause is original


class TestStorageErrorRouting:

    def test_routes_to_kusto(self):
        error = Exception("some kusto error")
        result = StorageErrorClassifier.classify_storage_error(error, "kusto")
        assert result.context["service"] == "kusto"

    def test_routes_to_delta(self):
        error = Exception("some delta error")
        result = StorageErrorClassifier.classify_storage_error(error, "delta")
        assert result.context["service"] == "delta"

    def test_routes_to_onelake(self):
        error = Exception("some onelake error")
        result = StorageErrorClassifier.classify_storage_error(error, "onelake")
        assert result.context["service"] == "onelake"

    def test_case_insensitive_service_name(self):
        error = Exception("error")
        result = StorageErrorClassifier.classify_storage_error(error, "KUSTO")
        assert result.context["service"] == "kusto"

    def test_unknown_service_falls_back_to_wrap_exception(self):
        error = Exception("error from unknown service")
        result = StorageErrorClassifier.classify_storage_error(error, "cosmosdb")
        assert isinstance(result, PipelineError)

    def test_unknown_service_with_context(self):
        error = Exception("error")
        result = StorageErrorClassifier.classify_storage_error(
            error, "cosmosdb", context={"db": "mydb"}
        )
        assert isinstance(result, PipelineError)

    def test_kusto_with_context_passthrough(self):
        error = Exception("auth token issue")
        result = StorageErrorClassifier.classify_storage_error(
            error, "kusto", context={"cluster": "mycluster"}
        )
        assert isinstance(result, AuthError)
        assert result.context["cluster"] == "mycluster"
        assert result.context["service"] == "kusto"
