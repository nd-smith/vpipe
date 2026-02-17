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


def _classify_kafka_error(
    error: Exception,
    service_name: str,
    context: dict | None = None,
) -> PipelineError:
    """
    Shared classification logic for Kafka consumer and producer errors.

    Args:
        error: Original exception
        service_name: "consumer" or "producer" (used in error messages and context)
        context: Additional context to merge
    """
    # If already a PipelineError (e.g., CircuitOpenError, PermanentError), preserve it
    if isinstance(error, PipelineError):
        return error

    error_str = str(error).lower()
    error_type = type(error).__name__
    error_context = {"service": f"message_{service_name}"}
    if context:
        error_context.update(context)

    label = f"Kafka {service_name}"

    # Data serialization errors are permanent - malformed data won't fix on retry
    if isinstance(error, (json.JSONDecodeError, UnicodeDecodeError)):
        return PermanentError(
            f"Message {'de' if service_name == 'consumer' else ''}serialization failed: {error}",
            cause=error,
            context=error_context,
        )

    # Classify by exception type first (checks both Kafka and EventHub mappings)
    category = classify_error_type(error_type)

    if category == "auth":
        return AuthError(
            f"{label} authentication failed: {error}",
            cause=error,
            context=error_context,
        )

    if category == "throttling":
        return ThrottlingError(
            f"{label} throttled: {error}",
            cause=error,
            context=error_context,
        )

    if category == "permanent":
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
        if service_name == "consumer" and "offset" in error_str and "out of range" in error_str:
            return PermanentError(
                f"Kafka offset out of range: {error}",
                cause=error,
                context=error_context,
            )
        if service_name == "producer" and "invalid" in error_str:
            return PermanentError(
                f"{label} validation error: {error}",
                cause=error,
                context=error_context,
            )
        return PermanentError(
            f"{label} permanent error: {error}",
            cause=error,
            context=error_context,
        )

    if category == "transient":
        if "timeout" in error_str or "KafkaTimeoutError" in error_type:
            return TransientError(
                f"{label} timeout: {error}",
                cause=error,
                context=error_context,
            )
        if "connection" in error_str or "KafkaConnectionError" in error_type:
            return TransientError(
                f"{label} connection error: {error}",
                cause=error,
                context=error_context,
            )
        return TransientError(
            f"{label} transient error: {error}",
            cause=error,
            context=error_context,
        )

    # String-based fallback classification
    if any(marker in error_str for marker in ("unauthorized", "authentication", "authorization")):
        return AuthError(
            f"{label} auth error: {error}",
            cause=error,
            context=error_context,
        )

    if "timeout" in error_str:
        return TimeoutError(
            f"{label} timeout: {error}",
            cause=error,
            context=error_context,
        )

    connection_markers = ["connection", "broker", "network"]
    if service_name == "consumer":
        connection_markers.append("node not ready")
    else:
        connection_markers.append("leader")

    if any(marker in error_str for marker in connection_markers):
        return ConnectionError(
            f"{label} connection error: {error}",
            cause=error,
            context=error_context,
        )

    # Default to generic Kafka error
    return KafkaError(
        f"{label} error: {error}",
        cause=error,
        context=error_context,
    )


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
        return _classify_kafka_error(error, "consumer", context)

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
        return _classify_kafka_error(error, "producer", context)

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
