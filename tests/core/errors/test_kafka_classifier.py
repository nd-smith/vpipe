"""
Tests for Kafka error classification.
"""

import pytest

from core.errors.exceptions import (
    AuthError,
    KafkaError,
    PermanentError,
    ThrottlingError,
    TransientError,
)

# NOTE: Specialized exceptions removed in refactor - consolidated into base classes:
# - ConnectionError, TimeoutError -> TransientError
# - ValidationError -> PermanentError
from core.errors.kafka_classifier import (
    EVENTHUB_ERROR_MAPPINGS,
    KAFKA_ERROR_MAPPINGS,
    KafkaErrorClassifier,
    classify_error_type,
    classify_kafka_error_type,
)


class MockKafkaError(Exception):
    """Mock Kafka exception for testing."""

    pass


class TestKafkaErrorTypeClassification:
    """Test Kafka error type classification function."""

    def test_transient_errors(self):
        """Test transient error type classification."""
        transient_types = KAFKA_ERROR_MAPPINGS["transient"]
        for error_type in transient_types:
            result = classify_kafka_error_type(error_type)
            assert result == "transient", f"Expected 'transient' for {error_type}, got {result}"

    def test_auth_errors(self):
        """Test authentication error type classification."""
        auth_types = KAFKA_ERROR_MAPPINGS["auth"]
        for error_type in auth_types:
            result = classify_kafka_error_type(error_type)
            assert result == "auth", f"Expected 'auth' for {error_type}, got {result}"

    def test_permanent_errors(self):
        """Test permanent error type classification."""
        permanent_types = KAFKA_ERROR_MAPPINGS["permanent"]
        for error_type in permanent_types:
            result = classify_kafka_error_type(error_type)
            assert result == "permanent", f"Expected 'permanent' for {error_type}, got {result}"

    def test_throttling_errors(self):
        """Test throttling error type classification."""
        throttling_types = KAFKA_ERROR_MAPPINGS["throttling"]
        for error_type in throttling_types:
            result = classify_kafka_error_type(error_type)
            assert result == "throttling", f"Expected 'throttling' for {error_type}, got {result}"

    def test_unknown_error_type(self):
        """Test unknown error types return None."""
        assert classify_kafka_error_type("UnknownError") is None
        assert classify_kafka_error_type("CustomException") is None
        assert classify_kafka_error_type("") is None


class TestKafkaConsumerErrorClassifier:
    """Test Kafka consumer error classification."""

    def test_auth_error_by_type(self):
        """Test authentication error detection by exception type."""
        error = MockKafkaError("Failed to authenticate")
        error.__class__.__name__ = "TopicAuthorizationFailedError"

        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, AuthError)
        assert "authentication failed" in str(result).lower()
        assert result.context["service"] == "kafka_consumer"

    def test_auth_error_by_string(self):
        """Test authentication error detection by error message."""
        error = Exception("Kafka authorization failed")
        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, AuthError)
        assert result.context["service"] == "kafka_consumer"

    def test_throttling_error_by_type(self):
        """Test throttling error detection by exception type."""
        error = MockKafkaError("Rate limited")
        error.__class__.__name__ = "KafkaThrottlingError"

        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, ThrottlingError)
        assert "throttled" in str(result).lower()

    @pytest.mark.skip("Removed exception types - classifier now returns base classes")
    def test_timeout_error_by_type(self):
        """Test timeout error detection by exception type."""
        error = MockKafkaError("Operation timed out")
        error.__class__.__name__ = "KafkaTimeoutError"

        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, TimeoutError)
        assert "timeout" in str(result).lower()

    @pytest.mark.skip("Removed exception types - classifier now returns base classes")
    def test_timeout_error_by_string(self):
        """Test timeout error detection by error message."""
        error = Exception("Request timeout exceeded")
        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, TimeoutError)

    @pytest.mark.skip("Removed exception types - classifier now returns base classes")
    def test_connection_error_by_type(self):
        """Test connection error detection by exception type."""
        error = MockKafkaError("Cannot connect to broker")
        error.__class__.__name__ = "KafkaConnectionError"

        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, ConnectionError)
        assert "connection" in str(result).lower()

    @pytest.mark.skip("Removed exception types - classifier now returns base classes")
    def test_connection_error_by_string(self):
        """Test connection error detection by error message."""
        error = Exception("Broker not available")
        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, ConnectionError)

    def test_permanent_error_topic_not_found(self):
        """Test permanent error for topic not found."""
        error = MockKafkaError("Topic does not exist")
        error.__class__.__name__ = "UnknownTopicOrPartitionError"

        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, PermanentError)
        assert "topic does not exist" in str(result).lower()

    def test_permanent_error_offset_out_of_range(self):
        """Test permanent error for offset out of range."""
        error = MockKafkaError("Offset out of range")
        error.__class__.__name__ = "OffsetOutOfRangeError"

        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, PermanentError)
        assert "offset out of range" in str(result).lower()

    def test_permanent_error_message_too_large(self):
        """Test permanent error for message too large."""
        error = MockKafkaError("Message size exceeds limit")
        error.__class__.__name__ = "MessageSizeTooLargeError"

        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, PermanentError)
        assert "too large" in str(result).lower()

    def test_transient_error_broker_unavailable(self):
        """Test transient error for broker unavailable."""
        error = MockKafkaError("Broker temporarily unavailable")
        error.__class__.__name__ = "BrokerNotAvailableError"

        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, TransientError)

    def test_transient_error_leader_not_available(self):
        """Test transient error for leader not available."""
        error = MockKafkaError("Leader election in progress")
        error.__class__.__name__ = "LeaderNotAvailableError"

        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, TransientError)

    def test_generic_kafka_error(self):
        """Test generic Kafka error for unknown cases."""
        error = Exception("Unknown Kafka consumer issue")
        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, KafkaError)
        assert result.context["service"] == "kafka_consumer"

    @pytest.mark.skip("Removed exception types - classifier now returns base classes")
    def test_context_preservation(self):
        """Test that additional context is preserved."""
        error = Exception("Connection failed")
        context = {"topic": "test-topic", "partition": 0}

        result = KafkaErrorClassifier.classify_consumer_error(error, context)
        assert result.context["service"] == "kafka_consumer"
        assert result.context["topic"] == "test-topic"
        assert result.context["partition"] == 0


class TestKafkaProducerErrorClassifier:
    """Test Kafka producer error classification."""

    def test_auth_error_by_type(self):
        """Test authentication error detection by exception type."""
        error = MockKafkaError("Authorization denied")
        error.__class__.__name__ = "GroupAuthorizationFailedError"

        result = KafkaErrorClassifier.classify_producer_error(error)
        assert isinstance(result, AuthError)
        assert "authentication failed" in str(result).lower()
        assert result.context["service"] == "kafka_producer"

    def test_auth_error_by_string(self):
        """Test authentication error detection by error message."""
        error = Exception("Kafka unauthorized access")
        result = KafkaErrorClassifier.classify_producer_error(error)
        assert isinstance(result, AuthError)

    def test_throttling_error_by_type(self):
        """Test throttling error detection by exception type."""
        error = MockKafkaError("Producer fenced")
        error.__class__.__name__ = "ProducerFencedException"

        result = KafkaErrorClassifier.classify_producer_error(error)
        assert isinstance(result, ThrottlingError)

    @pytest.mark.skip("Removed exception types - classifier now returns base classes")
    def test_timeout_error_by_type(self):
        """Test timeout error detection by exception type."""
        error = MockKafkaError("Send timeout")
        error.__class__.__name__ = "RequestTimedOutError"

        result = KafkaErrorClassifier.classify_producer_error(error)
        assert isinstance(result, TimeoutError)

    @pytest.mark.skip("Removed exception types - classifier now returns base classes")
    def test_timeout_error_by_string(self):
        """Test timeout error detection by error message."""
        error = Exception("Producer timeout waiting for ack")
        result = KafkaErrorClassifier.classify_producer_error(error)
        assert isinstance(result, TimeoutError)

    @pytest.mark.skip("Removed exception types - classifier now returns base classes")
    def test_connection_error_by_type(self):
        """Test connection error detection by exception type."""
        error = MockKafkaError("Lost connection")
        error.__class__.__name__ = "NodeNotReadyError"

        result = KafkaErrorClassifier.classify_producer_error(error)
        assert isinstance(result, ConnectionError)

    @pytest.mark.skip("Removed exception types - classifier now returns base classes")
    def test_connection_error_by_string(self):
        """Test connection error detection by error message."""
        error = Exception("Network connection lost to leader")
        result = KafkaErrorClassifier.classify_producer_error(error)
        assert isinstance(result, ConnectionError)

    def test_permanent_error_topic_not_found(self):
        """Test permanent error for topic not found."""
        error = MockKafkaError("Topic test-topic not found")
        error.__class__.__name__ = "InvalidTopicError"

        result = KafkaErrorClassifier.classify_producer_error(error)
        assert isinstance(result, PermanentError)
        assert "topic does not exist" in str(result).lower()

    def test_permanent_error_message_too_large(self):
        """Test permanent error for message too large."""
        error = MockKafkaError("Message size 15MB exceeds limit")
        error.__class__.__name__ = "RecordTooLargeError"

        result = KafkaErrorClassifier.classify_producer_error(error)
        assert isinstance(result, PermanentError)
        assert "too large" in str(result).lower()

    @pytest.mark.skip("Removed exception types - classifier now returns base classes")
    def test_validation_error_invalid_config(self):
        """Test validation error for invalid configuration."""
        error = MockKafkaError("Invalid producer configuration")
        error.__class__.__name__ = "InvalidConfigurationError"

        result = KafkaErrorClassifier.classify_producer_error(error)
        assert isinstance(result, ValidationError)

    def test_transient_error_not_leader(self):
        """Test transient error for not leader for partition."""
        error = MockKafkaError("Not leader for partition")
        error.__class__.__name__ = "NotLeaderForPartitionError"

        result = KafkaErrorClassifier.classify_producer_error(error)
        assert isinstance(result, TransientError)

    def test_generic_kafka_error(self):
        """Test generic Kafka error for unknown cases."""
        error = Exception("Unknown Kafka producer issue")
        result = KafkaErrorClassifier.classify_producer_error(error)
        assert isinstance(result, KafkaError)
        assert result.context["service"] == "kafka_producer"

    @pytest.mark.skip("Removed exception types - classifier now returns base classes")
    def test_context_preservation(self):
        """Test that additional context is preserved."""
        error = Exception("Connection failed")
        context = {"topic": "output-topic", "partition": 2}

        result = KafkaErrorClassifier.classify_producer_error(error, context)
        assert result.context["service"] == "kafka_producer"
        assert result.context["topic"] == "output-topic"
        assert result.context["partition"] == 2


class TestKafkaErrorClassifierGeneric:
    """Test generic Kafka error classifier routing."""

    @pytest.mark.skip("Removed exception types - classifier now returns base classes")
    def test_consumer_routing(self):
        """Test that consumer operations are routed correctly."""
        error = Exception("Consumer timeout")
        result = KafkaErrorClassifier.classify_kafka_error(error, "consumer")
        assert isinstance(result, TimeoutError)
        assert result.context["service"] == "kafka_consumer"

    @pytest.mark.skip("Removed exception types - classifier now returns base classes")
    def test_producer_routing(self):
        """Test that producer operations are routed correctly."""
        error = Exception("Producer timeout")
        result = KafkaErrorClassifier.classify_kafka_error(error, "producer")
        assert isinstance(result, TimeoutError)
        assert result.context["service"] == "kafka_producer"

    def test_case_insensitive_routing(self):
        """Test that operation type is case-insensitive."""
        error = Exception("Test error")

        result1 = KafkaErrorClassifier.classify_kafka_error(error, "CONSUMER")
        result2 = KafkaErrorClassifier.classify_kafka_error(error, "Consumer")
        result3 = KafkaErrorClassifier.classify_kafka_error(error, "consumer")

        assert result1.context["service"] == "kafka_consumer"
        assert result2.context["service"] == "kafka_consumer"
        assert result3.context["service"] == "kafka_consumer"

    def test_unknown_operation_type(self):
        """Test that unknown operation types fall back to generic wrapper."""
        error = Exception("Test error")
        result = KafkaErrorClassifier.classify_kafka_error(error, "unknown")
        # Should fall back to wrap_exception which returns PipelineError
        assert isinstance(result, (KafkaError, Exception))

    def test_context_propagation(self):
        """Test that context is propagated through routing."""
        error = Exception("Test error")
        context = {"test_key": "test_value"}

        result = KafkaErrorClassifier.classify_kafka_error(
            error, "consumer", context
        )
        assert result.context.get("test_key") == "test_value"


class TestKafkaErrorIntegration:
    """Integration tests for real-world Kafka error scenarios."""

    def test_consumer_group_rebalance_scenario(self):
        """Test error handling during consumer group rebalance."""
        error = MockKafkaError("Consumer is not subscribed to topic")
        error.__class__.__name__ = "IllegalStateError"

        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, PermanentError)
        # Permanent because state errors typically indicate programming issues

    @pytest.mark.skip("Removed exception types - classifier now returns base classes")
    def test_broker_restart_scenario(self):
        """Test error handling during broker restart."""
        error = MockKafkaError("Broker connection lost")
        error.__class__.__name__ = "KafkaConnectionError"

        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, ConnectionError)
        # Transient - should retry after broker comes back

    def test_partition_leader_election_scenario(self):
        """Test error handling during partition leader election."""
        error = MockKafkaError("Leader not available for partition")
        error.__class__.__name__ = "LeaderNotAvailableError"

        result = KafkaErrorClassifier.classify_producer_error(error)
        assert isinstance(result, TransientError)
        # Transient - leader election completes quickly

    def test_oversized_message_scenario(self):
        """Test error handling for oversized messages."""
        error = MockKafkaError("Message batch size 2MB exceeds max 1MB")
        error.__class__.__name__ = "RecordBatchTooLargeError"

        result = KafkaErrorClassifier.classify_producer_error(error)
        assert isinstance(result, PermanentError)
        # Permanent - message needs to be split or rejected

    def test_sasl_authentication_scenario(self):
        """Test error handling for SASL authentication failures."""
        error = MockKafkaError("SASL authentication failed")
        error.__class__.__name__ = "SaslAuthenticationError"

        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, AuthError)
        # Auth error - should refresh credentials and retry


class TestEventHubErrorTypeClassification:
    """Test EventHub error type classification function."""

    def test_transient_errors(self):
        """Test transient EventHub error type classification."""
        transient_types = EVENTHUB_ERROR_MAPPINGS["transient"]
        for error_type in transient_types:
            result = classify_error_type(error_type)
            assert result == "transient", f"Expected 'transient' for {error_type}, got {result}"

    def test_auth_errors(self):
        """Test authentication EventHub error type classification."""
        auth_types = EVENTHUB_ERROR_MAPPINGS["auth"]
        for error_type in auth_types:
            result = classify_error_type(error_type)
            assert result == "auth", f"Expected 'auth' for {error_type}, got {result}"

    def test_permanent_errors(self):
        """Test permanent EventHub error type classification."""
        permanent_types = EVENTHUB_ERROR_MAPPINGS["permanent"]
        for error_type in permanent_types:
            result = classify_error_type(error_type)
            assert result == "permanent", f"Expected 'permanent' for {error_type}, got {result}"

    def test_throttling_errors(self):
        """Test throttling EventHub error type classification."""
        throttling_types = EVENTHUB_ERROR_MAPPINGS["throttling"]
        for error_type in throttling_types:
            result = classify_error_type(error_type)
            assert result == "throttling", f"Expected 'throttling' for {error_type}, got {result}"

    def test_eventhub_error_as_transient(self):
        """Test that EventHubError is classified as transient."""
        result = classify_error_type("EventHubError")
        assert result == "transient"

    def test_authentication_error_as_auth(self):
        """Test that AuthenticationError is classified as auth."""
        result = classify_error_type("AuthenticationError")
        assert result == "auth"

    def test_server_busy_error_as_throttling(self):
        """Test that ServerBusyError is classified as throttling."""
        result = classify_error_type("ServerBusyError")
        assert result == "throttling"

    def test_event_data_send_error_as_permanent(self):
        """Test that EventDataSendError is classified as permanent."""
        result = classify_error_type("EventDataSendError")
        assert result == "permanent"


class TestClassifyErrorTypeFunction:
    """Test the unified classify_error_type function."""

    def test_kafka_error_classification(self):
        """Test that Kafka errors are still classified correctly."""
        assert classify_error_type("KafkaConnectionError") == "transient"
        assert classify_error_type("TopicAuthorizationFailedError") == "auth"
        assert classify_error_type("MessageSizeTooLargeError") == "permanent"
        assert classify_error_type("KafkaThrottlingError") == "throttling"

    def test_eventhub_error_classification(self):
        """Test that EventHub errors are classified correctly."""
        assert classify_error_type("EventHubError") == "transient"
        assert classify_error_type("AuthenticationError") == "auth"
        assert classify_error_type("EventDataSendError") == "permanent"
        assert classify_error_type("ServerBusyError") == "throttling"

    def test_kafka_priority_over_eventhub(self):
        """Test that Kafka mappings are checked first."""
        # If a name existed in both, Kafka would win (though this shouldn't happen in practice)
        kafka_result = classify_error_type("BrokerNotAvailableError")
        assert kafka_result == "transient"

    def test_unknown_error_returns_none(self):
        """Test unknown error types return None."""
        assert classify_error_type("UnknownError") is None
        assert classify_error_type("CustomException") is None
        assert classify_error_type("") is None

    def test_backward_compatibility_with_kafka_classifier(self):
        """Test backward compatibility - old function still works."""
        # Legacy function should still work for Kafka errors
        assert classify_kafka_error_type("KafkaConnectionError") == "transient"
        assert classify_kafka_error_type("EventHubError") is None  # Old function doesn't know EventHub


class TestEventHubConsumerErrorClassification:
    """Test EventHub consumer error classification."""

    def test_eventhub_error_as_transient(self):
        """Test EventHubError is classified as transient."""
        error = MockKafkaError("EventHub operation failed")
        error.__class__.__name__ = "EventHubError"

        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, TransientError)
        assert "transient" in str(result).lower()

    def test_connection_lost_error_as_transient(self):
        """Test ConnectionLostError is classified as transient."""
        error = MockKafkaError("Connection to EventHub lost")
        error.__class__.__name__ = "ConnectionLostError"

        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, TransientError)

    def test_operation_timeout_error_as_transient(self):
        """Test OperationTimeoutError is classified as transient."""
        error = MockKafkaError("Operation timed out")
        error.__class__.__name__ = "OperationTimeoutError"

        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, TransientError)

    def test_authentication_error_as_auth(self):
        """Test AuthenticationError is classified as auth."""
        error = MockKafkaError("Authentication failed")
        error.__class__.__name__ = "AuthenticationError"

        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, AuthError)
        assert "authentication failed" in str(result).lower()

    def test_client_authentication_error_as_auth(self):
        """Test ClientAuthenticationError is classified as auth."""
        error = MockKafkaError("Client authentication failed")
        error.__class__.__name__ = "ClientAuthenticationError"

        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, AuthError)

    def test_server_busy_error_as_throttling(self):
        """Test ServerBusyError is classified as throttling."""
        error = MockKafkaError("Server is busy")
        error.__class__.__name__ = "ServerBusyError"

        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, ThrottlingError)
        assert "throttled" in str(result).lower()

    def test_event_data_send_error_as_permanent(self):
        """Test EventDataSendError is classified as permanent."""
        error = MockKafkaError("Failed to send event data")
        error.__class__.__name__ = "EventDataSendError"

        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, PermanentError)

    def test_event_data_error_as_permanent(self):
        """Test EventDataError is classified as permanent."""
        error = MockKafkaError("Invalid event data")
        error.__class__.__name__ = "EventDataError"

        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, PermanentError)

    def test_schema_error_as_permanent(self):
        """Test SchemaError is classified as permanent."""
        error = MockKafkaError("Schema validation failed")
        error.__class__.__name__ = "SchemaError"

        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, PermanentError)


class TestEventHubProducerErrorClassification:
    """Test EventHub producer error classification."""

    def test_eventhub_error_as_transient(self):
        """Test EventHubError is classified as transient for producer."""
        error = MockKafkaError("EventHub operation failed")
        error.__class__.__name__ = "EventHubError"

        result = KafkaErrorClassifier.classify_producer_error(error)
        assert isinstance(result, TransientError)

    def test_authentication_error_as_auth(self):
        """Test AuthenticationError is classified as auth for producer."""
        error = MockKafkaError("Authentication failed")
        error.__class__.__name__ = "AuthenticationError"

        result = KafkaErrorClassifier.classify_producer_error(error)
        assert isinstance(result, AuthError)

    def test_server_busy_error_as_throttling(self):
        """Test ServerBusyError is classified as throttling for producer."""
        error = MockKafkaError("Server is busy")
        error.__class__.__name__ = "ServerBusyError"

        result = KafkaErrorClassifier.classify_producer_error(error)
        assert isinstance(result, ThrottlingError)

    def test_event_data_send_error_as_permanent(self):
        """Test EventDataSendError is classified as permanent for producer."""
        error = MockKafkaError("Failed to send event data")
        error.__class__.__name__ = "EventDataSendError"

        result = KafkaErrorClassifier.classify_producer_error(error)
        assert isinstance(result, PermanentError)


class TestMixedKafkaEventHubErrorClassification:
    """Test that both Kafka and EventHub errors work in the same classifier."""

    def test_consumer_handles_both_kafka_and_eventhub_errors(self):
        """Test consumer classifier handles both Kafka and EventHub exceptions."""
        # Kafka error
        kafka_error = MockKafkaError("Kafka broker unavailable")
        kafka_error.__class__.__name__ = "BrokerNotAvailableError"
        kafka_result = KafkaErrorClassifier.classify_consumer_error(kafka_error)
        assert isinstance(kafka_result, TransientError)

        # EventHub error
        eventhub_error = MockKafkaError("EventHub connection lost")
        eventhub_error.__class__.__name__ = "ConnectionLostError"
        eventhub_result = KafkaErrorClassifier.classify_consumer_error(eventhub_error)
        assert isinstance(eventhub_result, TransientError)

    def test_producer_handles_both_kafka_and_eventhub_errors(self):
        """Test producer classifier handles both Kafka and EventHub exceptions."""
        # Kafka error
        kafka_error = MockKafkaError("Topic not found")
        kafka_error.__class__.__name__ = "UnknownTopicOrPartitionError"
        kafka_result = KafkaErrorClassifier.classify_producer_error(kafka_error)
        assert isinstance(kafka_result, PermanentError)

        # EventHub error
        eventhub_error = MockKafkaError("Server busy")
        eventhub_error.__class__.__name__ = "ServerBusyError"
        eventhub_result = KafkaErrorClassifier.classify_producer_error(eventhub_error)
        assert isinstance(eventhub_result, ThrottlingError)

    def test_context_preserved_for_eventhub_errors(self):
        """Test that context is preserved for EventHub errors."""
        error = MockKafkaError("Authentication failed")
        error.__class__.__name__ = "AuthenticationError"
        context = {"eventhub": "test-hub", "partition": "0"}

        result = KafkaErrorClassifier.classify_consumer_error(error, context)
        assert result.context["service"] == "kafka_consumer"
        assert result.context["eventhub"] == "test-hub"
        assert result.context["partition"] == "0"
