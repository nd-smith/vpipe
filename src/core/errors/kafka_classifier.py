"""
Kafka error classification for consumer and producer operations.

Provides consistent error handling for aiokafka exceptions,
mapping them to typed PipelineError hierarchy with retry decisions.
"""

from typing import Optional

from core.errors.exceptions import (
    AuthError,
    CircuitOpenError,
    ConnectionError,
    KafkaError,
    PermanentError,
    PipelineError,
    ThrottlingError,
    TimeoutError,
    TransientError,
    ValidationError,
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


def classify_kafka_error_type(error_type_name: str) -> Optional[str]:
    """
    Classify Kafka error by exception type name.

    Args:
        error_type_name: Name of the exception class

    Returns:
        Error category: "transient", "auth", "permanent", "throttling", or None
    """
    for category, error_types in KAFKA_ERROR_MAPPINGS.items():
        if error_type_name in error_types:
            return category
    return None


class KafkaErrorClassifier:
    """
    Centralized error classification for Kafka operations.

    Provides consistent error categorization for both producer and consumer operations.
    Maps aiokafka exceptions to PipelineError hierarchy.
    """

    @staticmethod
    def classify_consumer_error(
        error: Exception, context: Optional[dict] = None
    ) -> PipelineError:
        """
        Classify a Kafka consumer error into appropriate exception type.

        Args:
            error: Original exception from aiokafka consumer
            context: Additional context (merged with default {"service": "kafka_consumer"})

        Returns:
            Classified PipelineError subclass
        """
        # If already a PipelineError (e.g., CircuitOpenError), preserve it
        if isinstance(error, PipelineError):
            return error

        error_str = str(error).lower()
        error_type = type(error).__name__
        ctx = {"service": "kafka_consumer"}
        if context:
            ctx.update(context)

        # Classify by exception type first
        category = classify_kafka_error_type(error_type)

        if category == "auth":
            return AuthError(
                f"Kafka consumer authentication failed: {error}",
                cause=error,
                context=ctx,
            )

        if category == "throttling":
            return ThrottlingError(
                f"Kafka consumer throttled: {error}",
                cause=error,
                context=ctx,
            )

        if category == "permanent":
            # Specific handling for common permanent errors
            if "topic" in error_str and "not" in error_str:
                return PermanentError(
                    f"Kafka topic does not exist: {error}",
                    cause=error,
                    context=ctx,
                )
            if "offset" in error_str and "out of range" in error_str:
                return PermanentError(
                    f"Kafka offset out of range: {error}",
                    cause=error,
                    context=ctx,
                )
            if "message" in error_str and "size" in error_str:
                return PermanentError(
                    f"Kafka message too large: {error}",
                    cause=error,
                    context=ctx,
                )
            return PermanentError(
                f"Kafka consumer permanent error: {error}",
                cause=error,
                context=ctx,
            )

        if category == "transient":
            # Differentiate between timeout and connection errors
            if "timeout" in error_str or "KafkaTimeoutError" in error_type:
                return TimeoutError(
                    f"Kafka consumer timeout: {error}",
                    cause=error,
                    context=ctx,
                )
            if "connection" in error_str or "KafkaConnectionError" in error_type:
                return ConnectionError(
                    f"Kafka consumer connection error: {error}",
                    cause=error,
                    context=ctx,
                )
            return TransientError(
                f"Kafka consumer transient error: {error}",
                cause=error,
                context=ctx,
            )

        # String-based fallback classification
        if any(
            marker in error_str
            for marker in ("unauthorized", "authentication", "authorization")
        ):
            return AuthError(
                f"Kafka consumer auth error: {error}",
                cause=error,
                context=ctx,
            )

        if "timeout" in error_str:
            return TimeoutError(
                f"Kafka consumer timeout: {error}",
                cause=error,
                context=ctx,
            )

        if any(
            marker in error_str
            for marker in ("connection", "broker", "network", "node not ready")
        ):
            return ConnectionError(
                f"Kafka consumer connection error: {error}",
                cause=error,
                context=ctx,
            )

        # Default to generic Kafka error
        return KafkaError(
            f"Kafka consumer error: {error}",
            cause=error,
            context=ctx,
        )

    @staticmethod
    def classify_producer_error(
        error: Exception, context: Optional[dict] = None
    ) -> PipelineError:
        """
        Classify a Kafka producer error into appropriate exception type.

        Args:
            error: Original exception from aiokafka producer
            context: Additional context (merged with default {"service": "kafka_producer"})

        Returns:
            Classified PipelineError subclass
        """
        # If already a PipelineError (e.g., CircuitOpenError), preserve it
        if isinstance(error, PipelineError):
            return error

        error_str = str(error).lower()
        error_type = type(error).__name__
        ctx = {"service": "kafka_producer"}
        if context:
            ctx.update(context)

        # Classify by exception type first
        category = classify_kafka_error_type(error_type)

        if category == "auth":
            return AuthError(
                f"Kafka producer authentication failed: {error}",
                cause=error,
                context=ctx,
            )

        if category == "throttling":
            return ThrottlingError(
                f"Kafka producer throttled: {error}",
                cause=error,
                context=ctx,
            )

        if category == "permanent":
            # Specific handling for common permanent errors
            if "topic" in error_str and "not" in error_str:
                return PermanentError(
                    f"Kafka topic does not exist: {error}",
                    cause=error,
                    context=ctx,
                )
            if "message" in error_str and ("size" in error_str or "large" in error_str):
                return PermanentError(
                    f"Kafka message too large: {error}",
                    cause=error,
                    context=ctx,
                )
            if "invalid" in error_str:
                return ValidationError(
                    f"Kafka producer validation error: {error}",
                    cause=error,
                    context=ctx,
                )
            return PermanentError(
                f"Kafka producer permanent error: {error}",
                cause=error,
                context=ctx,
            )

        if category == "transient":
            # Differentiate between timeout and connection errors
            if "timeout" in error_str or "KafkaTimeoutError" in error_type:
                return TimeoutError(
                    f"Kafka producer timeout: {error}",
                    cause=error,
                    context=ctx,
                )
            if "connection" in error_str or "KafkaConnectionError" in error_type:
                return ConnectionError(
                    f"Kafka producer connection error: {error}",
                    cause=error,
                    context=ctx,
                )
            return TransientError(
                f"Kafka producer transient error: {error}",
                cause=error,
                context=ctx,
            )

        # String-based fallback classification
        if any(
            marker in error_str
            for marker in ("unauthorized", "authentication", "authorization")
        ):
            return AuthError(
                f"Kafka producer auth error: {error}",
                cause=error,
                context=ctx,
            )

        if "timeout" in error_str:
            return TimeoutError(
                f"Kafka producer timeout: {error}",
                cause=error,
                context=ctx,
            )

        if any(
            marker in error_str
            for marker in ("connection", "broker", "network", "leader")
        ):
            return ConnectionError(
                f"Kafka producer connection error: {error}",
                cause=error,
                context=ctx,
            )

        # Default to generic Kafka error
        return KafkaError(
            f"Kafka producer error: {error}",
            cause=error,
            context=ctx,
        )

    @staticmethod
    def classify_kafka_error(
        error: Exception,
        operation_type: str,
        context: Optional[dict] = None,
    ) -> PipelineError:
        """
        Generic Kafka error classifier with operation routing.

        Routes to operation-specific classifier based on operation type.

        Args:
            error: Original exception
            operation_type: Operation type ("consumer", "producer")
            context: Additional context

        Returns:
            Classified PipelineError subclass
        """
        operation_type = operation_type.lower()

        if operation_type == "consumer":
            return KafkaErrorClassifier.classify_consumer_error(error, context)
        elif operation_type == "producer":
            return KafkaErrorClassifier.classify_producer_error(error, context)
        else:
            # Fall back to generic classification
            return wrap_exception(error, context=context)
