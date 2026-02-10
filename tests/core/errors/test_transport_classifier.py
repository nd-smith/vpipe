"""
Tests for transport error classification (Kafka and EventHub).
"""

import pytest

from core.errors.exceptions import (
    AuthError,
    ConnectionError,
    KafkaError,
    PermanentError,
    PipelineError,
    ThrottlingError,
    TimeoutError,
    TransientError,
)
from core.errors.transport_classifier import (
    EVENTHUB_ERROR_MAPPINGS,
    KAFKA_ERROR_MAPPINGS,
    TransportErrorClassifier,
    classify_error_type,
    classify_kafka_error_type,
)

# ---------------------------------------------------------------------------
# classify_kafka_error_type
# ---------------------------------------------------------------------------


class TestClassifyKafkaErrorType:
    @pytest.mark.parametrize("error_name", KAFKA_ERROR_MAPPINGS["transient"])
    def test_transient_errors(self, error_name):
        assert classify_kafka_error_type(error_name) == "transient"

    @pytest.mark.parametrize("error_name", KAFKA_ERROR_MAPPINGS["auth"])
    def test_auth_errors(self, error_name):
        assert classify_kafka_error_type(error_name) == "auth"

    @pytest.mark.parametrize("error_name", KAFKA_ERROR_MAPPINGS["permanent"])
    def test_permanent_errors(self, error_name):
        assert classify_kafka_error_type(error_name) == "permanent"

    @pytest.mark.parametrize("error_name", KAFKA_ERROR_MAPPINGS["throttling"])
    def test_throttling_errors(self, error_name):
        assert classify_kafka_error_type(error_name) == "throttling"

    def test_unknown_error_returns_none(self):
        assert classify_kafka_error_type("SomeRandomError") is None

    def test_empty_string_returns_none(self):
        assert classify_kafka_error_type("") is None

    def test_eventhub_error_not_found_in_kafka(self):
        assert classify_kafka_error_type("EventHubError") is None


# ---------------------------------------------------------------------------
# classify_error_type (combined Kafka + EventHub)
# ---------------------------------------------------------------------------


class TestClassifyErrorType:
    @pytest.mark.parametrize("error_name", KAFKA_ERROR_MAPPINGS["transient"])
    def test_kafka_transient(self, error_name):
        assert classify_error_type(error_name) == "transient"

    @pytest.mark.parametrize("error_name", KAFKA_ERROR_MAPPINGS["auth"])
    def test_kafka_auth(self, error_name):
        assert classify_error_type(error_name) == "auth"

    @pytest.mark.parametrize("error_name", KAFKA_ERROR_MAPPINGS["permanent"])
    def test_kafka_permanent(self, error_name):
        assert classify_error_type(error_name) == "permanent"

    @pytest.mark.parametrize("error_name", KAFKA_ERROR_MAPPINGS["throttling"])
    def test_kafka_throttling(self, error_name):
        assert classify_error_type(error_name) == "throttling"

    @pytest.mark.parametrize("error_name", EVENTHUB_ERROR_MAPPINGS["transient"])
    def test_eventhub_transient(self, error_name):
        assert classify_error_type(error_name) == "transient"

    @pytest.mark.parametrize("error_name", EVENTHUB_ERROR_MAPPINGS["auth"])
    def test_eventhub_auth(self, error_name):
        assert classify_error_type(error_name) == "auth"

    @pytest.mark.parametrize("error_name", EVENTHUB_ERROR_MAPPINGS["permanent"])
    def test_eventhub_permanent(self, error_name):
        assert classify_error_type(error_name) == "permanent"

    @pytest.mark.parametrize("error_name", EVENTHUB_ERROR_MAPPINGS["throttling"])
    def test_eventhub_throttling(self, error_name):
        assert classify_error_type(error_name) == "throttling"

    def test_unknown_returns_none(self):
        assert classify_error_type("CompletelyUnknownError") is None

    def test_kafka_checked_before_eventhub(self):
        # BrokerNotAvailableError is only in Kafka; verify it's found
        assert classify_error_type("BrokerNotAvailableError") == "transient"
        # ServerBusyError is only in EventHub; verify it's found
        assert classify_error_type("ServerBusyError") == "throttling"


# ---------------------------------------------------------------------------
# TransportErrorClassifier.classify_consumer_error
# ---------------------------------------------------------------------------


class TestConsumerErrorClassification:
    def test_pipeline_error_passed_through(self):
        original = AuthError("already classified")
        result = TransportErrorClassifier.classify_consumer_error(original)
        assert result is original

    def test_default_service_context(self):
        error = Exception("something broke")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert result.context["service"] == "message_consumer"

    def test_context_merged_with_default(self):
        error = Exception("something broke")
        result = TransportErrorClassifier.classify_consumer_error(
            error, context={"topic": "events"}
        )
        assert result.context["service"] == "message_consumer"
        assert result.context["topic"] == "events"

    def test_cause_preserved(self):
        error = RuntimeError("original")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert result.cause is error

    # -- Auth category --

    def test_auth_from_kafka_type(self):
        error_cls = type("TopicAuthorizationFailedError", (Exception,), {})
        error = error_cls("not authorized")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, AuthError)
        assert "authentication failed" in result.message.lower()

    def test_auth_from_sasl_type(self):
        error_cls = type("SaslAuthenticationError", (Exception,), {})
        error = error_cls("SASL handshake failed")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, AuthError)

    def test_auth_from_eventhub_type(self):
        error_cls = type("ClientAuthenticationError", (Exception,), {})
        error = error_cls("auth error")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, AuthError)

    # -- Throttling category --

    def test_throttling_from_kafka_type(self):
        error_cls = type("KafkaThrottlingError", (Exception,), {})
        error = error_cls("throttled")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, ThrottlingError)
        assert "throttled" in result.message.lower()

    def test_throttling_from_eventhub_type(self):
        error_cls = type("ServerBusyError", (Exception,), {})
        error = error_cls("server busy")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, ThrottlingError)

    # -- Permanent category --

    def test_permanent_from_type(self):
        error_cls = type("MessageSizeTooLargeError", (Exception,), {})
        error = error_cls("message size too large")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, PermanentError)
        assert "message too large" in result.message.lower()

    def test_permanent_topic_not_exist(self):
        error_cls = type("UnknownTopicOrPartitionError", (Exception,), {})
        error = error_cls("topic 'foo' not found")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, PermanentError)
        assert "topic does not exist" in result.message.lower()

    def test_permanent_offset_out_of_range(self):
        error_cls = type("OffsetOutOfRangeError", (Exception,), {})
        error = error_cls("Offset out of range for partition 0")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, PermanentError)
        assert "offset out of range" in result.message.lower()

    def test_permanent_generic(self):
        error_cls = type("InvalidTopicError", (Exception,), {})
        error = error_cls("bad config value")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, PermanentError)
        assert "permanent error" in result.message.lower()

    def test_permanent_from_eventhub_type(self):
        error_cls = type("EventDataSendError", (Exception,), {})
        error = error_cls("send failed permanently")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, PermanentError)

    # -- Transient category --

    def test_transient_timeout_from_type(self):
        error_cls = type("KafkaTimeoutError", (Exception,), {})
        error = error_cls("request timed out timeout exceeded")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, TransientError)
        assert "timeout" in result.message.lower()

    def test_transient_connection_from_type(self):
        error_cls = type("KafkaConnectionError", (Exception,), {})
        error = error_cls("connection lost to broker")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, TransientError)
        assert "connection error" in result.message.lower()

    def test_transient_generic_from_type(self):
        error_cls = type("LeaderNotAvailableError", (Exception,), {})
        error = error_cls("leader is unavailable")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, TransientError)
        assert "transient error" in result.message.lower()

    def test_transient_from_eventhub_type(self):
        error_cls = type("ConnectionLostError", (Exception,), {})
        error = error_cls("AMQP link detached")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, TransientError)

    def test_transient_timeout_from_type_name_match(self):
        # KafkaTimeoutError type name check (not just error_str)
        error_cls = type("KafkaTimeoutError", (Exception,), {})
        error = error_cls("request failed")  # No "timeout" in message
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, TransientError)
        assert "timeout" in result.message.lower()

    def test_transient_connection_from_type_name_match(self):
        error_cls = type("KafkaConnectionError", (Exception,), {})
        error = error_cls("failed")  # No "connection" in message
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, TransientError)
        assert "connection error" in result.message.lower()

    # -- String-based fallback --

    def test_fallback_auth_from_unauthorized_string(self):
        error = Exception("The request was unauthorized")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, AuthError)
        assert "auth error" in result.message.lower()

    def test_fallback_auth_from_authentication_string(self):
        error = Exception("authentication failed with broker")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, AuthError)

    def test_fallback_auth_from_authorization_string(self):
        error = Exception("authorization check failed")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, AuthError)

    def test_fallback_timeout_from_string(self):
        error = Exception("operation timed out after timeout period")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, TimeoutError)
        assert result.category.value == "transient"

    def test_fallback_connection_from_string(self):
        error = Exception("lost connection to remote host")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, ConnectionError)
        assert result.category.value == "transient"

    def test_fallback_broker_from_string(self):
        error = Exception("broker is not responding")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, ConnectionError)
        assert result.category.value == "transient"

    def test_fallback_network_from_string(self):
        error = Exception("network unreachable")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, ConnectionError)
        assert result.category.value == "transient"

    def test_fallback_node_not_ready_from_string(self):
        error = Exception("node not ready yet")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, ConnectionError)
        assert result.category.value == "transient"

    # -- Default --

    def test_unrecognized_error_becomes_kafka_error(self):
        error = Exception("something completely unknown happened")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, KafkaError)
        assert "Kafka consumer error" in result.message


# ---------------------------------------------------------------------------
# TransportErrorClassifier.classify_producer_error
# ---------------------------------------------------------------------------


class TestProducerErrorClassification:
    def test_pipeline_error_passed_through(self):
        original = ThrottlingError("already classified")
        result = TransportErrorClassifier.classify_producer_error(original)
        assert result is original

    def test_default_service_context(self):
        error = Exception("something broke")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert result.context["service"] == "message_producer"

    def test_context_merged_with_default(self):
        error = Exception("something broke")
        result = TransportErrorClassifier.classify_producer_error(
            error, context={"topic": "output"}
        )
        assert result.context["service"] == "message_producer"
        assert result.context["topic"] == "output"

    def test_cause_preserved(self):
        error = RuntimeError("original")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert result.cause is error

    # -- Auth category --

    def test_auth_from_type(self):
        error_cls = type("ClusterAuthorizationFailedError", (Exception,), {})
        error = error_cls("no cluster auth")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert isinstance(result, AuthError)
        assert "producer authentication failed" in result.message.lower()

    def test_auth_from_eventhub_type(self):
        error_cls = type("AuthenticationError", (Exception,), {})
        error = error_cls("bad creds")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert isinstance(result, AuthError)

    # -- Throttling category --

    def test_throttling_from_type(self):
        error_cls = type("ProducerFencedException", (Exception,), {})
        error = error_cls("fenced")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert isinstance(result, ThrottlingError)
        assert "producer throttled" in result.message.lower()

    # -- Permanent category --

    def test_permanent_topic_not_exist(self):
        error_cls = type("UnknownTopicOrPartitionError", (Exception,), {})
        error = error_cls("topic 'bar' not available")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert isinstance(result, PermanentError)
        assert "topic does not exist" in result.message.lower()

    def test_permanent_message_too_large_size(self):
        error_cls = type("MessageSizeTooLargeError", (Exception,), {})
        error = error_cls("message size exceeds max")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert isinstance(result, PermanentError)
        assert "message too large" in result.message.lower()

    def test_permanent_record_too_large_without_message_keyword(self):
        # "record is too large" does not contain "message", so it misses the
        # "message too large" branch and falls through to generic permanent error.
        error_cls = type("RecordTooLargeError", (Exception,), {})
        error = error_cls("record is too large for the broker")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert isinstance(result, PermanentError)
        assert "permanent error" in result.message.lower()

    def test_permanent_record_too_large_with_message_keyword(self):
        # When the error message contains both "message" and "large", it hits the right branch
        error_cls = type("RecordTooLargeError", (Exception,), {})
        error = error_cls("message record is too large")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert isinstance(result, PermanentError)
        assert "message too large" in result.message.lower()

    def test_permanent_invalid_error(self):
        error_cls = type("InvalidConfigurationError", (Exception,), {})
        error = error_cls("invalid config supplied")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert isinstance(result, PermanentError)
        assert "validation error" in result.message.lower()

    def test_permanent_generic(self):
        error_cls = type("UnsupportedVersionError", (Exception,), {})
        error = error_cls("protocol v99 not supported")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert isinstance(result, PermanentError)
        assert "permanent error" in result.message.lower()

    def test_permanent_from_eventhub_type(self):
        error_cls = type("EventDataError", (Exception,), {})
        error = error_cls("bad event data")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert isinstance(result, PermanentError)

    # -- Transient category --

    def test_transient_timeout_from_type(self):
        error_cls = type("RequestTimedOutError", (Exception,), {})
        error = error_cls("request timeout on send")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert isinstance(result, TransientError)
        assert "timeout" in result.message.lower()

    def test_transient_connection_from_type(self):
        error_cls = type("KafkaConnectionError", (Exception,), {})
        error = error_cls("connection dropped to broker")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert isinstance(result, TransientError)
        assert "connection error" in result.message.lower()

    def test_transient_generic_from_type(self):
        error_cls = type("BrokerNotAvailableError", (Exception,), {})
        error = error_cls("no brokers available")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert isinstance(result, TransientError)
        assert "transient error" in result.message.lower()

    def test_transient_timeout_from_type_name_match(self):
        error_cls = type("KafkaTimeoutError", (Exception,), {})
        error = error_cls("request failed")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert isinstance(result, TransientError)
        assert "timeout" in result.message.lower()

    def test_transient_connection_from_type_name_match(self):
        error_cls = type("KafkaConnectionError", (Exception,), {})
        error = error_cls("failed")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert isinstance(result, TransientError)
        assert "connection error" in result.message.lower()

    def test_transient_from_eventhub_type(self):
        error_cls = type("OperationTimeoutError", (Exception,), {})
        error = error_cls("operation timed out")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert isinstance(result, TransientError)

    # -- String-based fallback --

    def test_fallback_auth_from_unauthorized_string(self):
        error = Exception("The request was unauthorized by the cluster")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert isinstance(result, AuthError)
        assert "auth error" in result.message.lower()

    def test_fallback_auth_from_authentication_string(self):
        error = Exception("authentication with SASL failed")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert isinstance(result, AuthError)

    def test_fallback_auth_from_authorization_string(self):
        error = Exception("authorization denied for topic")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert isinstance(result, AuthError)

    def test_fallback_timeout_from_string(self):
        error = Exception("send timeout after 60 seconds")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert isinstance(result, TimeoutError)
        assert result.category.value == "transient"

    def test_fallback_connection_from_string(self):
        error = Exception("lost connection to kafka cluster")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert isinstance(result, ConnectionError)
        assert result.category.value == "transient"

    def test_fallback_broker_from_string(self):
        error = Exception("broker not responding to produce")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert isinstance(result, ConnectionError)
        assert result.category.value == "transient"

    def test_fallback_network_from_string(self):
        error = Exception("network error during send")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert isinstance(result, ConnectionError)
        assert result.category.value == "transient"

    def test_fallback_leader_from_string(self):
        error = Exception("leader not available for partition")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert isinstance(result, ConnectionError)
        assert result.category.value == "transient"

    # -- Default --

    def test_unrecognized_error_becomes_kafka_error(self):
        error = Exception("something completely unknown")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert isinstance(result, KafkaError)
        assert "Kafka producer error" in result.message


# ---------------------------------------------------------------------------
# TransportErrorClassifier.classify_transport_error (routing)
# ---------------------------------------------------------------------------


class TestTransportErrorRouting:
    def test_routes_to_consumer(self):
        error = Exception("something broke")
        result = TransportErrorClassifier.classify_transport_error(error, "consumer")
        assert result.context["service"] == "message_consumer"

    def test_routes_to_producer(self):
        error = Exception("something broke")
        result = TransportErrorClassifier.classify_transport_error(error, "producer")
        assert result.context["service"] == "message_producer"

    def test_case_insensitive_operation_type(self):
        error = Exception("error")
        result = TransportErrorClassifier.classify_transport_error(error, "CONSUMER")
        assert result.context["service"] == "message_consumer"

    def test_unknown_operation_falls_back_to_wrap_exception(self):
        error = Exception("error from unknown operation")
        result = TransportErrorClassifier.classify_transport_error(error, "admin")
        assert isinstance(result, PipelineError)

    def test_unknown_operation_with_context(self):
        error = Exception("error")
        result = TransportErrorClassifier.classify_transport_error(
            error, "admin", context={"op": "create_topic"}
        )
        assert isinstance(result, PipelineError)

    def test_consumer_with_context_passthrough(self):
        error_cls = type("SaslAuthenticationError", (Exception,), {})
        error = error_cls("SASL fail")
        result = TransportErrorClassifier.classify_transport_error(
            error, "consumer", context={"group": "my-group"}
        )
        assert isinstance(result, AuthError)
        assert result.context["group"] == "my-group"
        assert result.context["service"] == "message_consumer"

    def test_producer_with_context_passthrough(self):
        error_cls = type("KafkaThrottlingError", (Exception,), {})
        error = error_cls("slow down")
        result = TransportErrorClassifier.classify_transport_error(
            error, "producer", context={"batch_size": 100}
        )
        assert isinstance(result, ThrottlingError)
        assert result.context["batch_size"] == 100
        assert result.context["service"] == "message_producer"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestTransportEdgeCases:
    def test_pipeline_error_subclass_preserved_consumer(self):
        original = TransientError("transient thing")
        result = TransportErrorClassifier.classify_consumer_error(original)
        assert result is original

    def test_pipeline_error_subclass_preserved_producer(self):
        original = PermanentError("perm thing")
        result = TransportErrorClassifier.classify_producer_error(original)
        assert result is original

    def test_no_context_gives_default_consumer(self):
        error = Exception("oops")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert result.context == {"service": "message_consumer"}

    def test_no_context_gives_default_producer(self):
        error = Exception("oops")
        result = TransportErrorClassifier.classify_producer_error(error)
        assert result.context == {"service": "message_producer"}

    def test_error_message_includes_original(self):
        error = Exception("specific failure detail XYZ-123")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert "XYZ-123" in result.message

    def test_priority_type_based_over_string_based(self):
        # An error whose type says "permanent" but string says "timeout"
        # Type-based classification should win
        error_cls = type("InvalidTopicError", (Exception,), {})
        error = error_cls("timeout waiting for topic validation")
        result = TransportErrorClassifier.classify_consumer_error(error)
        assert isinstance(result, PermanentError)
