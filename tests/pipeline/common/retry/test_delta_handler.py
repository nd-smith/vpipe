"""
Tests for DeltaRetryHandler.

Tests retry logic for Delta Lake batch write failures with error classification
and DLQ routing.
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

from core.types import ErrorCategory
from pipeline.config import KafkaConfig
from pipeline.common.retry.delta_handler import DeltaRetryHandler
from pipeline.verisk.schemas.delta_batch import FailedDeltaBatch


@pytest.fixture
def kafka_config():
    """Create test Kafka configuration."""
    return KafkaConfig(
        bootstrap_servers="localhost:9092",
        verisk={
            "topics": {
                "events": "test.events.raw",
                "dlq": "test.downloads.dlq",
            },
            "retry_delays": [300, 600, 1200, 2400],  # 5m, 10m, 20m, 40m
            "consumer_group_prefix": "test",
        },
    )


@pytest.fixture
def mock_retry_producer():
    """Create mock producer for retry topics."""
    producer = AsyncMock()
    producer.send = AsyncMock()
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    return producer


@pytest.fixture
def mock_dlq_producer():
    """Create mock producer for DLQ topic."""
    producer = AsyncMock()
    producer.send = AsyncMock()
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    return producer


@pytest.fixture
async def retry_handler(kafka_config, mock_retry_producer, mock_dlq_producer):
    """Create DeltaRetryHandler with mocked dependencies."""
    handler = DeltaRetryHandler(
        config=kafka_config,
        table_path="abfss://workspace@onelake/lakehouse/Tables/xact_events",
        retry_delays=[300, 600, 1200, 2400],
        retry_topic_prefix="delta-events.retry",
        dlq_topic="delta-events.dlq",
        domain="verisk",
    )

    def _create_producer_side_effect(**kwargs):
        if kwargs.get("topic_key") == "retry":
            return mock_retry_producer
        elif kwargs.get("topic_key") == "dlq":
            return mock_dlq_producer
        return AsyncMock()

    with patch(
        "pipeline.common.retry.delta_handler.create_producer",
        side_effect=_create_producer_side_effect,
    ):
        await handler.start()

    return handler


@pytest.fixture
def sample_batch():
    """Create sample event batch."""
    return [
        {
            "traceId": "evt-001",
            "eventId": "e-001",
            "type": "claim.created",
            "data": {"claimNumber": "C-123"},
        },
        {
            "traceId": "evt-002",
            "eventId": "e-002",
            "type": "claim.updated",
            "data": {"claimNumber": "C-124"},
        },
        {
            "traceId": "evt-003",
            "eventId": "e-003",
            "type": "claim.submitted",
            "data": {"claimNumber": "C-125"},
        },
    ]


class TestDeltaRetryHandlerInit:
    """Test DeltaRetryHandler initialization."""

    def test_initialization(self, kafka_config):
        """Test handler initializes with correct configuration."""
        handler = DeltaRetryHandler(
            config=kafka_config,
            table_path="abfss://workspace@onelake/lakehouse/Tables/xact_events",
            retry_delays=[300, 600, 1200, 2400],
            retry_topic_prefix="delta-events.retry",
            dlq_topic="delta-events.dlq",
            domain="verisk",
        )

        assert handler.config == kafka_config
        assert handler.table_path == "abfss://workspace@onelake/lakehouse/Tables/xact_events"
        assert handler.domain == "verisk"
        assert handler._retry_delays == [300, 600, 1200, 2400]
        assert handler._max_retries == 4
        assert handler._retry_topic_prefix == "delta-events.retry"
        assert handler._dlq_topic == "delta-events.dlq"
        assert handler._retry_producer is None
        assert handler._dlq_producer is None

    def test_initialization_with_defaults(self, kafka_config):
        """Test handler uses defaults when parameters not provided."""
        handler = DeltaRetryHandler(
            config=kafka_config,
            table_path="abfss://workspace@onelake/lakehouse/Tables/xact_events",
            domain="verisk",
        )

        assert handler._retry_delays == [300, 600, 1200, 2400]
        assert handler._retry_topic_prefix == "com.allstate.pcesdopodappv1.delta-events.retry"
        assert handler._dlq_topic == "com.allstate.pcesdopodappv1.delta-events.dlq"


class TestDeltaRetryHandlerErrorClassification:
    """Test error classification for Delta errors."""

    def test_classify_transient_timeout_error(self, retry_handler):
        """Test timeout errors are classified as TRANSIENT."""
        error = TimeoutError("Delta write timeout after 30s")
        category = retry_handler.classify_delta_error(error)
        assert category == ErrorCategory.TRANSIENT

    def test_classify_transient_connection_error(self, retry_handler):
        """Test connection errors are classified as TRANSIENT."""
        error = ConnectionError("Network connection failed")
        category = retry_handler.classify_delta_error(error)
        assert category == ErrorCategory.TRANSIENT

    def test_classify_transient_throttling_error(self, retry_handler):
        """Test throttling errors are classified as TRANSIENT."""
        error = Exception("429 Too Many Requests - throttled")
        category = retry_handler.classify_delta_error(error)
        assert category == ErrorCategory.TRANSIENT

    def test_classify_transient_service_error(self, retry_handler):
        """Test service errors are classified as TRANSIENT."""
        error = Exception("503 Service Unavailable")
        category = retry_handler.classify_delta_error(error)
        assert category == ErrorCategory.TRANSIENT

    def test_classify_transient_deadlock_error(self, retry_handler):
        """Test deadlock errors are classified as TRANSIENT."""
        error = Exception("Deadlock detected in Delta Lake commit")
        category = retry_handler.classify_delta_error(error)
        assert category == ErrorCategory.TRANSIENT

    def test_classify_permanent_schema_error(self, retry_handler):
        """Test schema errors are classified as PERMANENT."""
        error = Exception("Schema mismatch: column 'foo' does not exist")
        category = retry_handler.classify_delta_error(error)
        assert category == ErrorCategory.PERMANENT

    def test_classify_permanent_type_error(self, retry_handler):
        """Test type errors are classified as PERMANENT."""
        error = TypeError("Cannot cast string to integer")
        category = retry_handler.classify_delta_error(error)
        assert category == ErrorCategory.PERMANENT

    def test_classify_permanent_constraint_error(self, retry_handler):
        """Test constraint violations are classified as PERMANENT."""
        error = Exception("Primary key constraint violated")
        category = retry_handler.classify_delta_error(error)
        assert category == ErrorCategory.PERMANENT

    def test_classify_permanent_permission_error(self, retry_handler):
        """Test permission errors are classified as PERMANENT."""
        error = Exception("403 Forbidden - permission denied")
        category = retry_handler.classify_delta_error(error)
        assert category == ErrorCategory.PERMANENT

    def test_classify_permanent_table_not_found(self, retry_handler):
        """Test table not found errors are classified as PERMANENT."""
        error = Exception("Table not found at path")
        category = retry_handler.classify_delta_error(error)
        assert category == ErrorCategory.PERMANENT

    def test_classify_unknown_error(self, retry_handler):
        """Test unrecognized errors are classified as UNKNOWN."""
        error = Exception("Something unexpected happened")
        category = retry_handler.classify_delta_error(error)
        assert category == ErrorCategory.UNKNOWN


class TestDeltaRetryHandlerRetry:
    """Test retry routing for Delta batch failures."""

    @pytest.mark.asyncio
    async def test_first_retry_transient_error(
        self, retry_handler, sample_batch, mock_retry_producer
    ):
        """Test first retry sends to retry topic with correct metadata."""
        error = TimeoutError("Delta write timeout")

        await retry_handler.handle_batch_failure(
            batch=sample_batch,
            error=error,
            retry_count=0,
            error_category="transient",
            batch_id="batch-001",
        )

        # Verify send was called once on retry producer
        assert mock_retry_producer.send.call_count == 1

        # Verify sent to correct retry topic
        call_args = mock_retry_producer.send.call_args
        assert "retry" in call_args.kwargs["topic"]

        # Verify FailedDeltaBatch structure
        failed_batch = call_args.kwargs["value"]
        assert isinstance(failed_batch, FailedDeltaBatch)
        assert failed_batch.batch_id == "batch-001"
        assert failed_batch.events == sample_batch
        assert failed_batch.retry_count == 1
        assert failed_batch.event_count == 3
        assert "Delta write timeout" in failed_batch.last_error
        assert failed_batch.error_category == "transient"
        assert failed_batch.retry_at is not None
        assert failed_batch.table_path == "abfss://workspace@onelake/lakehouse/Tables/xact_events"

        # Verify headers
        headers = call_args.kwargs["headers"]
        assert headers["retry_count"] == "1"
        assert headers["error_category"] == "transient"

    @pytest.mark.asyncio
    async def test_second_retry(
        self, retry_handler, sample_batch, mock_retry_producer
    ):
        """Test second retry sends to 600s retry topic."""
        error = ConnectionError("Network timeout")

        await retry_handler.handle_batch_failure(
            batch=sample_batch,
            error=error,
            retry_count=1,
            error_category="transient",
            batch_id="batch-002",
        )

        call_args = mock_retry_producer.send.call_args
        failed_batch = call_args.kwargs["value"]
        assert failed_batch.retry_count == 2

    @pytest.mark.asyncio
    async def test_third_retry(
        self, retry_handler, sample_batch, mock_retry_producer
    ):
        """Test third retry sends to 1200s retry topic."""
        error = Exception("503 Service Unavailable")

        await retry_handler.handle_batch_failure(
            batch=sample_batch,
            error=error,
            retry_count=2,
            error_category="transient",
            batch_id="batch-003",
        )

        call_args = mock_retry_producer.send.call_args
        failed_batch = call_args.kwargs["value"]
        assert failed_batch.retry_count == 3

    @pytest.mark.asyncio
    async def test_preserves_first_failure_timestamp(
        self, retry_handler, sample_batch, mock_retry_producer
    ):
        """Test first_failure_at is preserved across retries."""
        first_failure = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        error = TimeoutError("Timeout")

        await retry_handler.handle_batch_failure(
            batch=sample_batch,
            error=error,
            retry_count=1,
            error_category="transient",
            batch_id="batch-004",
            first_failure_at=first_failure,
        )

        failed_batch = mock_retry_producer.send.call_args.kwargs["value"]
        assert failed_batch.first_failure_at == first_failure

    @pytest.mark.asyncio
    async def test_error_category_enum_conversion(
        self, retry_handler, sample_batch, mock_retry_producer
    ):
        """Test error_category string is converted to ErrorCategory enum."""
        error = TimeoutError("Timeout")

        # Pass string instead of enum
        await retry_handler.handle_batch_failure(
            batch=sample_batch,
            error=error,
            retry_count=0,
            error_category="transient",
            batch_id="batch-005",
        )

        # Should still work correctly
        assert mock_retry_producer.send.call_count == 1


class TestDeltaRetryHandlerDLQ:
    """Test DLQ routing for Delta batch failures."""

    @pytest.mark.asyncio
    async def test_permanent_error_sends_to_dlq(
        self, retry_handler, sample_batch, mock_dlq_producer
    ):
        """Test permanent error skips retry and sends to DLQ."""
        error = Exception("Schema mismatch: column not found")

        await retry_handler.handle_batch_failure(
            batch=sample_batch,
            error=error,
            retry_count=0,
            error_category="permanent",
            batch_id="batch-dlq-001",
        )

        # Verify sent directly to DLQ
        assert mock_dlq_producer.send.call_count == 1
        call_args = mock_dlq_producer.send.call_args
        assert call_args.kwargs["topic"] == "delta-events.dlq"

        # Verify FailedDeltaBatch structure
        failed_batch = call_args.kwargs["value"]
        assert isinstance(failed_batch, FailedDeltaBatch)
        assert failed_batch.batch_id == "batch-dlq-001"
        assert failed_batch.events == sample_batch
        assert failed_batch.retry_count == 0
        assert "Schema mismatch" in failed_batch.last_error
        assert failed_batch.error_category == "permanent"
        assert failed_batch.retry_at is None  # No retry scheduled

        # Verify headers
        headers = call_args.kwargs["headers"]
        assert headers["retry_count"] == "0"
        assert headers["error_category"] == "permanent"
        assert headers["failed"] == "true"

    @pytest.mark.asyncio
    async def test_retries_exhausted_sends_to_dlq(
        self, retry_handler, sample_batch, mock_dlq_producer
    ):
        """Test exhausted retries send to DLQ."""
        error = TimeoutError("Network timeout")

        await retry_handler.handle_batch_failure(
            batch=sample_batch,
            error=error,
            retry_count=4,  # Max retries reached
            error_category="transient",
            batch_id="batch-dlq-002",
        )

        # Verify sent to DLQ
        call_args = mock_dlq_producer.send.call_args
        assert call_args.kwargs["topic"] == "delta-events.dlq"

        # Verify retry count preserved
        failed_batch = call_args.kwargs["value"]
        assert failed_batch.retry_count == 4

    @pytest.mark.asyncio
    async def test_dlq_error_message_truncation(
        self, retry_handler, sample_batch, mock_dlq_producer
    ):
        """Test long error messages are truncated for DLQ."""
        long_error = Exception("E" * 600)

        await retry_handler.handle_batch_failure(
            batch=sample_batch,
            error=long_error,
            retry_count=0,
            error_category="permanent",
            batch_id="batch-dlq-003",
        )

        failed_batch = mock_dlq_producer.send.call_args.kwargs["value"]
        assert len(failed_batch.last_error) == 500
        assert failed_batch.last_error.endswith("...")

    @pytest.mark.asyncio
    async def test_classifies_error_when_string_category_invalid(
        self, retry_handler, sample_batch, mock_retry_producer
    ):
        """Test handler classifies error when category string is invalid."""
        error = TimeoutError("Timeout error")

        # Pass invalid category string
        await retry_handler.handle_batch_failure(
            batch=sample_batch,
            error=error,
            retry_count=0,
            error_category="invalid_category",
            batch_id="batch-006",
        )

        # Should classify the error and route to retry
        assert mock_retry_producer.send.call_count == 1


class TestDeltaRetryHandlerEdgeCases:
    """Test edge cases in Delta retry handler."""

    @pytest.mark.asyncio
    async def test_empty_batch_handling(
        self, retry_handler, mock_retry_producer, mock_dlq_producer
    ):
        """Test handling empty batch."""
        error = TimeoutError("Timeout")

        await retry_handler.handle_batch_failure(
            batch=[],
            error=error,
            retry_count=0,
            error_category="transient",
            batch_id="batch-empty",
        )

        # Should not send anything
        assert mock_retry_producer.send.call_count == 0
        assert mock_dlq_producer.send.call_count == 0

    @pytest.mark.asyncio
    async def test_batch_without_trace_ids(
        self, retry_handler, mock_retry_producer
    ):
        """Test handling batch with events missing trace IDs."""
        batch = [
            {"eventId": "e-001", "type": "test"},
            {"eventId": "e-002", "type": "test"},
        ]
        error = TimeoutError("Timeout")

        await retry_handler.handle_batch_failure(
            batch=batch,
            error=error,
            retry_count=0,
            error_category="transient",
            batch_id="batch-no-traces",
        )

        # Should still send to retry topic
        assert mock_retry_producer.send.call_count == 1

    @pytest.mark.asyncio
    async def test_batch_id_optional(
        self, retry_handler, sample_batch, mock_retry_producer
    ):
        """Test batch_id is optional."""
        error = TimeoutError("Timeout")

        await retry_handler.handle_batch_failure(
            batch=sample_batch,
            error=error,
            retry_count=0,
            error_category="transient",
            batch_id=None,
        )

        # Should still send to retry topic
        assert mock_retry_producer.send.call_count == 1
        failed_batch = mock_retry_producer.send.call_args.kwargs["value"]
        assert failed_batch.batch_id is not None  # Auto-generated by FailedDeltaBatch

    @pytest.mark.asyncio
    async def test_unknown_error_category_retries(
        self, retry_handler, sample_batch, mock_retry_producer
    ):
        """Test UNKNOWN error category still retries."""
        error = Exception("Unknown error type")

        await retry_handler.handle_batch_failure(
            batch=sample_batch,
            error=error,
            retry_count=0,
            error_category="unknown",
            batch_id="batch-unknown",
        )

        # Should retry cautiously
        assert mock_retry_producer.send.call_count == 1


class TestDeltaRetryHandlerHelpers:
    """Test helper methods in Delta retry handler."""

    def test_get_all_retry_topics(self, retry_handler):
        """Test get_all_retry_topics returns correct topic names."""
        topics = retry_handler.get_all_retry_topics()

        assert topics == [
            "delta-events.retry.300s",
            "delta-events.retry.600s",
            "delta-events.retry.1200s",
            "delta-events.retry.2400s",
        ]

    def test_get_all_retry_topics_with_custom_delays(self, kafka_config):
        """Test get_all_retry_topics with custom delays."""
        handler = DeltaRetryHandler(
            config=kafka_config,
            table_path="abfss://test",
            retry_delays=[60, 120, 240],
            retry_topic_prefix="custom.retry",
            domain="verisk",
        )

        topics = handler.get_all_retry_topics()
        assert topics == [
            "custom.retry.60s",
            "custom.retry.120s",
            "custom.retry.240s",
        ]
