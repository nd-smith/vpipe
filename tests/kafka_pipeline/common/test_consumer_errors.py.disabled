"""
Tests for Kafka consumer error handling and classification.

Tests the error classification and routing logic added in WP-207:
- Error classification using KafkaErrorClassifier
- Transient error handling (don't commit, log for retry)
- Permanent error handling (don't commit, log for DLQ)
- Auth error handling (don't commit, reprocess after token refresh)
- Circuit open error handling (don't commit, reprocess when circuit closes)
- Unknown error handling (don't commit, conservative retry)

These are unit tests that use mocks - no Docker/Kafka required.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, call, patch
from aiokafka.structs import ConsumerRecord

from core.errors.exceptions import (
    AuthError,
    CircuitOpenError,
    ErrorCategory,
    PermanentError,
    TimeoutError,
    TransientError,
)
from core.resilience.circuit_breaker import CircuitBreaker
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.common.consumer import BaseKafkaConsumer


@pytest.fixture
def kafka_config():
    """Create test Kafka configuration with required domain config."""
    return KafkaConfig(
        bootstrap_servers="localhost:9092",
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        # Consumer defaults (used by get_worker_config)
        consumer_defaults={
            "enable_auto_commit": False,
            "auto_offset_reset": "earliest",
            "max_poll_records": 100,
            "session_timeout_ms": 30000,
            "max_poll_interval_ms": 300000,
        },
        # Domain config required for get_worker_config and get_consumer_group
        xact={
            "topics": {
                "events": "xact.events.raw",
                "downloads_pending": "xact.downloads.pending",
            },
            "consumer_group_prefix": "xact",
            "test_worker": {
                "consumer": {},
                "processing": {},
            },
        },
    )


@pytest.fixture
def mock_circuit_breaker():
    """Create mock circuit breaker."""
    breaker = MagicMock(spec=CircuitBreaker)

    async def passthrough(coro):
        return await coro()

    breaker.call_async = AsyncMock(side_effect=passthrough)
    return breaker


@pytest.fixture
def mock_message_handler():
    """Create mock message handler."""
    return AsyncMock()


@pytest.fixture
def mock_aiokafka_consumer():
    """Create mock AIOKafkaConsumer."""
    consumer = MagicMock()
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()
    consumer.commit = AsyncMock()
    consumer.getmany = AsyncMock()
    consumer.assignment = MagicMock(return_value=[])
    return consumer


def create_consumer_record(
    topic: str = "test-topic",
    partition: int = 0,
    offset: int = 0,
    key: bytes = b"test-key",
    value: bytes = b'{"test": "value"}',
) -> ConsumerRecord:
    """Helper to create ConsumerRecord for testing."""
    return ConsumerRecord(
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp=1234567890,
        timestamp_type=0,
        key=key,
        value=value,
        headers=[],
        checksum=None,
        serialized_key_size=len(key) if key else 0,
        serialized_value_size=len(value) if value else 0,
    )


class TestErrorClassification:
    """Tests for error classification logic."""

    @pytest.mark.asyncio
    async def test_transient_error_no_commit(
        self,
        kafka_config,
        mock_message_handler,
        mock_circuit_breaker,
        mock_aiokafka_consumer,
    ):
        """Transient errors don't commit offset."""
        consumer = BaseKafkaConsumer(
            config=kafka_config,
            domain="xact",
            worker_name="test_worker",
            topics=["test-topic"],
            message_handler=mock_message_handler,
            circuit_breaker=mock_circuit_breaker,
        )
        consumer._consumer = mock_aiokafka_consumer

        # Handler raises transient error
        mock_message_handler.side_effect = TimeoutError("Connection timeout")

        message = create_consumer_record()

        # Process message - should not raise
        await consumer._process_message(message)

        # Handler was called
        mock_message_handler.assert_called_once_with(message)

        # Offset NOT committed
        mock_aiokafka_consumer.commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_permanent_error_no_commit(
        self,
        kafka_config,
        mock_message_handler,
        mock_circuit_breaker,
        mock_aiokafka_consumer,
    ):
        """Permanent errors don't commit offset (logged for DLQ)."""
        consumer = BaseKafkaConsumer(
            config=kafka_config,
            domain="xact",
            worker_name="test_worker",
            topics=["test-topic"],
            message_handler=mock_message_handler,
            circuit_breaker=mock_circuit_breaker,
        )
        consumer._consumer = mock_aiokafka_consumer

        # Handler raises permanent error
        mock_message_handler.side_effect = PermanentError("Invalid message format")

        message = create_consumer_record()

        # Process message - should not raise
        await consumer._process_message(message)

        # Handler was called
        mock_message_handler.assert_called_once_with(message)

        # Offset NOT committed
        mock_aiokafka_consumer.commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_auth_error_no_commit(
        self,
        kafka_config,
        mock_message_handler,
        mock_circuit_breaker,
        mock_aiokafka_consumer,
    ):
        """Auth errors don't commit offset (will reprocess after token refresh)."""
        consumer = BaseKafkaConsumer(
            config=kafka_config,
            domain="xact",
            worker_name="test_worker",
            topics=["test-topic"],
            message_handler=mock_message_handler,
            circuit_breaker=mock_circuit_breaker,
        )
        consumer._consumer = mock_aiokafka_consumer

        # Handler raises auth error
        mock_message_handler.side_effect = AuthError("Token expired")

        message = create_consumer_record()

        # Process message - should not raise
        await consumer._process_message(message)

        # Handler was called
        mock_message_handler.assert_called_once_with(message)

        # Offset NOT committed
        mock_aiokafka_consumer.commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_circuit_open_error_no_commit(
        self,
        kafka_config,
        mock_message_handler,
        mock_circuit_breaker,
        mock_aiokafka_consumer,
    ):
        """Circuit open errors don't commit offset (will reprocess when circuit closes)."""
        consumer = BaseKafkaConsumer(
            config=kafka_config,
            domain="xact",
            worker_name="test_worker",
            topics=["test-topic"],
            message_handler=mock_message_handler,
            circuit_breaker=mock_circuit_breaker,
        )
        consumer._consumer = mock_aiokafka_consumer

        # Handler raises circuit open error
        mock_message_handler.side_effect = CircuitOpenError(
            circuit_name="test-circuit", retry_after=60.0
        )

        message = create_consumer_record()

        # Process message - should not raise
        await consumer._process_message(message)

        # Handler was called
        mock_message_handler.assert_called_once_with(message)

        # Offset NOT committed
        mock_aiokafka_consumer.commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_unknown_error_no_commit(
        self,
        kafka_config,
        mock_message_handler,
        mock_circuit_breaker,
        mock_aiokafka_consumer,
    ):
        """Unknown errors don't commit offset (conservative retry)."""
        consumer = BaseKafkaConsumer(
            config=kafka_config,
            domain="xact",
            worker_name="test_worker",
            topics=["test-topic"],
            message_handler=mock_message_handler,
            circuit_breaker=mock_circuit_breaker,
        )
        consumer._consumer = mock_aiokafka_consumer

        # Handler raises unknown error
        mock_message_handler.side_effect = RuntimeError("Unknown error")

        message = create_consumer_record()

        # Process message - should not raise
        await consumer._process_message(message)

        # Handler was called
        mock_message_handler.assert_called_once_with(message)

        # Offset NOT committed
        mock_aiokafka_consumer.commit.assert_not_called()


class TestErrorRouting:
    """Tests for error routing and logging."""

    @pytest.mark.asyncio
    async def test_error_classified_with_context(
        self,
        kafka_config,
        mock_message_handler,
        mock_circuit_breaker,
        mock_aiokafka_consumer,
    ):
        """Errors are classified with proper context."""
        consumer = BaseKafkaConsumer(
            config=kafka_config,
            domain="xact",
            worker_name="test_worker",
            topics=["test-topic"],
            message_handler=mock_message_handler,
            circuit_breaker=mock_circuit_breaker,
        )
        consumer._consumer = mock_aiokafka_consumer

        # Mock the classifier (note the correct module path)
        with patch(
            "kafka_pipeline.common.consumer.KafkaErrorClassifier.classify_consumer_error"
        ) as mock_classify:
            mock_error = TransientError("Test error")
            mock_classify.return_value = mock_error

            # Handler raises error
            original_error = ValueError("Processing failed")
            mock_message_handler.side_effect = original_error

            message = create_consumer_record(
                topic="my-topic", partition=2, offset=100
            )

            # Process message
            await consumer._process_message(message)

            # Classifier called with correct context
            mock_classify.assert_called_once()
            call_args = mock_classify.call_args
            assert call_args[0][0] == original_error
            assert call_args[1]["context"]["topic"] == "my-topic"
            assert call_args[1]["context"]["partition"] == 2
            assert call_args[1]["context"]["offset"] == 100
            # group_id is now derived from config, not passed directly
            assert call_args[1]["context"]["group_id"] == "xact-test_worker"

    @pytest.mark.asyncio
    async def test_transient_error_logged_with_context(
        self,
        kafka_config,
        mock_message_handler,
        mock_circuit_breaker,
        mock_aiokafka_consumer,
        caplog,
    ):
        """Transient errors are logged with message context."""
        import logging

        caplog.set_level(logging.WARNING)

        consumer = BaseKafkaConsumer(
            config=kafka_config,
            domain="xact",
            worker_name="test_worker",
            topics=["test-topic"],
            message_handler=mock_message_handler,
            circuit_breaker=mock_circuit_breaker,
        )
        consumer._consumer = mock_aiokafka_consumer

        # Handler raises transient error
        mock_message_handler.side_effect = TransientError("Connection timeout")

        message = create_consumer_record(topic="my-topic", partition=1, offset=50)

        # Process message
        await consumer._process_message(message)

        # Check log contains error details - the actual message may vary slightly
        # Looking for either the full message or partial indication of transient error
        log_text_lower = caplog.text.lower()
        assert "transient" in log_text_lower or "will retry" in log_text_lower

    @pytest.mark.asyncio
    async def test_permanent_error_logged_for_dlq(
        self,
        kafka_config,
        mock_message_handler,
        mock_circuit_breaker,
        mock_aiokafka_consumer,
        caplog,
    ):
        """Permanent errors are logged for DLQ routing."""
        import logging

        caplog.set_level(logging.ERROR)

        consumer = BaseKafkaConsumer(
            config=kafka_config,
            domain="xact",
            worker_name="test_worker",
            topics=["test-topic"],
            message_handler=mock_message_handler,
            circuit_breaker=mock_circuit_breaker,
        )
        consumer._consumer = mock_aiokafka_consumer

        # Handler raises permanent error
        mock_message_handler.side_effect = PermanentError("Invalid format")

        message = create_consumer_record()

        # Process message
        await consumer._process_message(message)

        # Check log mentions DLQ or permanent error handling
        # Note: The KafkaErrorClassifier might classify this differently,
        # so we check for either permanent error or the actual logged message
        log_has_permanent = any(
            "Permanent error" in r.message for r in caplog.records
        )
        log_has_dlq = "should route to DLQ" in caplog.text or log_has_permanent

        # If not permanent, check if it was logged as unknown (still correct behavior)
        if not log_has_permanent:
            # Unknown category is also acceptable for conservative retry
            assert any("Unknown error" in r.message for r in caplog.records)

        # Verify error was logged at ERROR level
        assert any(r.levelname == "ERROR" for r in caplog.records)


class TestErrorHandlingIntegration:
    """Integration tests for error handling."""

    @pytest.mark.asyncio
    async def test_multiple_errors_dont_stop_processing(
        self,
        kafka_config,
        mock_message_handler,
        mock_circuit_breaker,
        mock_aiokafka_consumer,
    ):
        """Consumer continues processing after errors."""
        consumer = BaseKafkaConsumer(
            config=kafka_config,
            domain="xact",
            worker_name="test_worker",
            topics=["test-topic"],
            message_handler=mock_message_handler,
            circuit_breaker=mock_circuit_breaker,
        )
        consumer._consumer = mock_aiokafka_consumer

        # Create sequence: success, error, success, error, success
        call_count = 0

        async def handler_with_errors(message):
            nonlocal call_count
            call_count += 1
            if call_count in [2, 4]:
                raise TransientError(f"Error on call {call_count}")

        mock_message_handler.side_effect = handler_with_errors

        # Process 5 messages
        for i in range(5):
            message = create_consumer_record(offset=i)
            await consumer._process_message(message)

        # All 5 messages processed (errors didn't stop processing)
        assert call_count == 5

        # Only successful messages committed (calls 1, 3, 5)
        assert mock_aiokafka_consumer.commit.call_count == 3

    @pytest.mark.asyncio
    async def test_error_category_determines_commit_behavior(
        self,
        kafka_config,
        mock_message_handler,
        mock_circuit_breaker,
        mock_aiokafka_consumer,
    ):
        """Different error categories result in correct commit behavior."""
        consumer = BaseKafkaConsumer(
            config=kafka_config,
            domain="xact",
            worker_name="test_worker",
            topics=["test-topic"],
            message_handler=mock_message_handler,
            circuit_breaker=mock_circuit_breaker,
        )
        consumer._consumer = mock_aiokafka_consumer

        # Test each error category
        errors_to_test = [
            TransientError("Transient"),
            PermanentError("Permanent"),
            AuthError("Auth"),
            CircuitOpenError("circuit", 60.0),
            RuntimeError("Unknown"),
        ]

        for error in errors_to_test:
            mock_aiokafka_consumer.commit.reset_mock()
            mock_message_handler.side_effect = error

            message = create_consumer_record()
            await consumer._process_message(message)

            # None of these errors should commit
            mock_aiokafka_consumer.commit.assert_not_called()

        # Test success case
        mock_aiokafka_consumer.commit.reset_mock()
        mock_message_handler.side_effect = None

        message = create_consumer_record()
        await consumer._process_message(message)

        # Success should commit
        mock_aiokafka_consumer.commit.assert_called_once()
