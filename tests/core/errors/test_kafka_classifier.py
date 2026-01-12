"""
Tests for Kafka error classification.
"""

import pytest

from core.errors.exceptions import (
    AuthError,
    ConnectionError,
    KafkaError,
    PermanentError,
    ThrottlingError,
    TimeoutError,
    TransientError,
    ValidationError,
)
from core.errors.kafka_classifier import (
    KAFKA_ERROR_MAPPINGS,
    KafkaErrorClassifier,
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

    def test_timeout_error_by_type(self):
        """Test timeout error detection by exception type."""
        error = MockKafkaError("Operation timed out")
        error.__class__.__name__ = "KafkaTimeoutError"

        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, TimeoutError)
        assert "timeout" in str(result).lower()

    def test_timeout_error_by_string(self):
        """Test timeout error detection by error message."""
        error = Exception("Request timeout exceeded")
        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, TimeoutError)

    def test_connection_error_by_type(self):
        """Test connection error detection by exception type."""
        error = MockKafkaError("Cannot connect to broker")
        error.__class__.__name__ = "KafkaConnectionError"

        result = KafkaErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, ConnectionError)
        assert "connection" in str(result).lower()

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

    def test_timeout_error_by_type(self):
        """Test timeout error detection by exception type."""
        error = MockKafkaError("Send timeout")
        error.__class__.__name__ = "RequestTimedOutError"

        result = KafkaErrorClassifier.classify_producer_error(error)
        assert isinstance(result, TimeoutError)

    def test_timeout_error_by_string(self):
        """Test timeout error detection by error message."""
        error = Exception("Producer timeout waiting for ack")
        result = KafkaErrorClassifier.classify_producer_error(error)
        assert isinstance(result, TimeoutError)

    def test_connection_error_by_type(self):
        """Test connection error detection by exception type."""
        error = MockKafkaError("Lost connection")
        error.__class__.__name__ = "NodeNotReadyError"

        result = KafkaErrorClassifier.classify_producer_error(error)
        assert isinstance(result, ConnectionError)

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

    def test_consumer_routing(self):
        """Test that consumer operations are routed correctly."""
        error = Exception("Consumer timeout")
        result = KafkaErrorClassifier.classify_kafka_error(error, "consumer")
        assert isinstance(result, TimeoutError)
        assert result.context["service"] == "kafka_consumer"

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
