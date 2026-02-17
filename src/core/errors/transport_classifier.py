"""
Transport error classification for message consumer and producer operations.

Provides consistent error handling for transport exceptions (Kafka protocol, EventHub),
mapping them to typed PipelineError hierarchy with retry decisions.
"""

import json

from core.errors.exceptions import (
    AuthError,
    ConnectionError,
    KafkaError,
    PermanentError,
    PipelineError,
    ThrottlingError,
    TimeoutError,
    TransientError,
    wrap_exception,
)

# Kafka error classifications based on aiokafka exception types
KAFKA_ERROR_MAPPINGS = {
    # Transient errors (retry recommended)
    "transient": [
        "BrokerNotAvailableError",
        "KafkaConnectionError",
        "NodeNotReadyError",
        "LeaderNotAvailableError",
        "NotLeaderForPartitionError",
        "RequestTimedOutError",
        "KafkaTimeoutError",
        "NetworkException",
        "CorrelationIdError",
        "BrokerResponseError",
    ],
    # Auth errors (credential refresh needed)
    "auth": [
        "TopicAuthorizationFailedError",
        "GroupAuthorizationFailedError",
        "ClusterAuthorizationFailedError",
        "SaslAuthenticationError",
    ],
    # Permanent errors (don't retry)
    "permanent": [
        "UnknownTopicOrPartitionError",
        "MessageSizeTooLargeError",
        "RecordTooLargeError",
        "InvalidTopicError",
        "InvalidConfigurationError",
        "UnsupportedVersionError",
        "IllegalStateError",
        "OffsetOutOfRangeError",
        "InvalidReplicationFactorError",
        "InvalidPartitionsError",
        "RecordBatchTooLargeError",
    ],
    # Throttling (backoff needed)
    "throttling": [
        "KafkaThrottlingError",
        "ProducerFencedException",
    ],
}

# Azure EventHub error classifications based on azure-eventhub SDK exceptions
EVENTHUB_ERROR_MAPPINGS = {
    # Transient errors (retry recommended)
    "transient": [
        "EventHubError",
        "ConnectionLostError",
        "ConnectError",
        "OperationTimeoutError",
        "AMQPConnectionError",
    ],
    # Auth errors (credential refresh needed)
    "auth": [
        "AuthenticationError",
        "ClientAuthenticationError",
    ],
    # Permanent errors (don't retry)
    "permanent": [
        "EventDataSendError",
        "EventDataError",
        "SchemaError",
    ],
    # Throttling (backoff needed)
    "throttling": [
        "ServerBusyError",
    ],
}


def classify_error_type(error_type_name: str) -> str | None:
    """
    Classify error by exception type name, checking both Kafka and EventHub mappings.

    Args:
        error_type_name: Name of the exception class

    Returns:
        Error category: "transient", "auth", "permanent", "throttling", or None
    """
    # Check Kafka mappings first
    for category, error_types in KAFKA_ERROR_MAPPINGS.items():
        if error_type_name in error_types:
            return category

    # Check EventHub mappings if not found in Kafka
    for category, error_types in EVENTHUB_ERROR_MAPPINGS.items():
        if error_type_name in error_types:
            return category

    return None


class TransportErrorClassifier:
    """
    Centralized error classification for message transport operations.

    Provides consistent error categorization for both producer and consumer operations.
    Maps transport exceptions (Kafka protocol via aiokafka, EventHub) to PipelineError hierarchy.
    """

    @staticmethod
    def classify_consumer_error(error: Exception, context: dict | None = None) -> PipelineError:
        """
        Classify a message consumer error into appropriate exception type.

        Args:
            error: Original exception from message consumer
            context: Additional context (merged with default {"service": "message_consumer"})

        Returns:
            Classified PipelineError subclass
        """
        # If already a PipelineError (e.g., CircuitOpenError, PermanentError), preserve it
        if isinstance(error, PipelineError):
            return error

        error_str = str(error).lower()
        error_type = type(error).__name__
        error_context = {"service": "message_consumer"}
        if context:
            error_context.update(context)

        # Data deserialization errors are permanent - malformed data won't fix on retry
        if isinstance(error, (json.JSONDecodeError, UnicodeDecodeError)):
            return PermanentError(
                f"Message deserialization failed: {error}",
                cause=error,
                context=error_context,
            )

        # Classify by exception type first (checks both Kafka and EventHub mappings)
        category = classify_error_type(error_type)

        if category == "auth":
            return AuthError(
                f"Kafka consumer authentication failed: {error}",
                cause=error,
                context=error_context,
            )

        if category == "throttling":
            return ThrottlingError(
                f"Kafka consumer throttled: {error}",
                cause=error,
                context=error_context,
            )

        if category == "permanent":
            # Specific handling for common permanent errors
            if "topic" in error_str and "not" in error_str:
                return PermanentError(
                    f"Kafka topic does not exist: {error}",
                    cause=error,
                    context=error_context,
                )
            if "offset" in error_str and "out of range" in error_str:
                return PermanentError(
                    f"Kafka offset out of range: {error}",
                    cause=error,
                    context=error_context,
                )
            if "message" in error_str and "size" in error_str:
                return PermanentError(
                    f"Kafka message too large: {error}",
                    cause=error,
                    context=error_context,
                )
            return PermanentError(
                f"Kafka consumer permanent error: {error}",
                cause=error,
                context=error_context,
            )

        if category == "transient":
            # Differentiate between timeout and connection errors
            if "timeout" in error_str or "KafkaTimeoutError" in error_type:
                return TransientError(
                    f"Kafka consumer timeout: {error}",
                    cause=error,
                    context=error_context,
                )
            if "connection" in error_str or "KafkaConnectionError" in error_type:
                return TransientError(
                    f"Kafka consumer connection error: {error}",
                    cause=error,
                    context=error_context,
                )
            return TransientError(
                f"Kafka consumer transient error: {error}",
                cause=error,
                context=error_context,
            )

        # String-based fallback classification
        if any(
            marker in error_str for marker in ("unauthorized", "authentication", "authorization")
        ):
            return AuthError(
                f"Kafka consumer auth error: {error}",
                cause=error,
                context=error_context,
            )

        if "timeout" in error_str:
            return TimeoutError(
                f"Kafka consumer timeout: {error}",
                cause=error,
                context=error_context,
            )

        if any(
            marker in error_str for marker in ("connection", "broker", "network", "node not ready")
        ):
            return ConnectionError(
                f"Kafka consumer connection error: {error}",
                cause=error,
                context=error_context,
            )

        # Default to generic Kafka error
        return KafkaError(
            f"Kafka consumer error: {error}",
            cause=error,
            context=error_context,
        )

    @staticmethod
    def classify_producer_error(error: Exception, context: dict | None = None) -> PipelineError:
        """
        Classify a message producer error into appropriate exception type.

        Args:
            error: Original exception from message producer
            context: Additional context (merged with default {"service": "message_producer"})

        Returns:
            Classified PipelineError subclass
        """
        # If already a PipelineError (e.g., CircuitOpenError, PermanentError), preserve it
        if isinstance(error, PipelineError):
            return error

        error_str = str(error).lower()
        error_type = type(error).__name__
        error_context = {"service": "message_producer"}
        if context:
            error_context.update(context)

        # Data serialization errors are permanent - malformed data won't fix on retry
        if isinstance(error, (json.JSONDecodeError, UnicodeDecodeError)):
            return PermanentError(
                f"Message serialization failed: {error}",
                cause=error,
                context=error_context,
            )

        # Classify by exception type first (checks both Kafka and EventHub mappings)
        category = classify_error_type(error_type)

        if category == "auth":
            return AuthError(
                f"Kafka producer authentication failed: {error}",
                cause=error,
                context=error_context,
            )

        if category == "throttling":
            return ThrottlingError(
                f"Kafka producer throttled: {error}",
                cause=error,
                context=error_context,
            )

        if category == "permanent":
            # Specific handling for common permanent errors
            if "topic" in error_str and "not" in error_str:
                return PermanentError(
                    f"Kafka topic does not exist: {error}",
                    cause=error,
                    context=error_context,
                )
            if "message" in error_str and ("size" in error_str or "large" in error_str):
                return PermanentError(
                    f"Kafka message too large: {error}",
                    cause=error,
                    context=error_context,
                )
            if "invalid" in error_str:
                return PermanentError(
                    f"Kafka producer validation error: {error}",
                    cause=error,
                    context=error_context,
                )
            return PermanentError(
                f"Kafka producer permanent error: {error}",
                cause=error,
                context=error_context,
            )

        if category == "transient":
            # Differentiate between timeout and connection errors
            if "timeout" in error_str or "KafkaTimeoutError" in error_type:
                return TransientError(
                    f"Kafka producer timeout: {error}",
                    cause=error,
                    context=error_context,
                )
            if "connection" in error_str or "KafkaConnectionError" in error_type:
                return TransientError(
                    f"Kafka producer connection error: {error}",
                    cause=error,
                    context=error_context,
                )
            return TransientError(
                f"Kafka producer transient error: {error}",
                cause=error,
                context=error_context,
            )

        # String-based fallback classification
        if any(
            marker in error_str for marker in ("unauthorized", "authentication", "authorization")
        ):
            return AuthError(
                f"Kafka producer auth error: {error}",
                cause=error,
                context=error_context,
            )

        if "timeout" in error_str:
            return TimeoutError(
                f"Kafka producer timeout: {error}",
                cause=error,
                context=error_context,
            )

        if any(marker in error_str for marker in ("connection", "broker", "network", "leader")):
            return ConnectionError(
                f"Kafka producer connection error: {error}",
                cause=error,
                context=error_context,
            )

        # Default to generic Kafka error
        return KafkaError(
            f"Kafka producer error: {error}",
            cause=error,
            context=error_context,
        )

    @staticmethod
    def classify_transport_error(
        error: Exception,
        operation_type: str,
        context: dict | None = None,
    ) -> PipelineError:
        """
        Generic transport error classifier with operation routing.

        Routes to operation-specific classifier based on operation type.

        Args:
            error: Original exception
            operation_type: Operation type ("consumer", "producer")
            context: Additional context

        Returns:
            Classified PipelineError subclass
        """
        classifiers = {
            "consumer": TransportErrorClassifier.classify_consumer_error,
            "producer": TransportErrorClassifier.classify_producer_error,
        }
        classifier = classifiers.get(operation_type.lower(), wrap_exception)
        return classifier(error, context=context)
