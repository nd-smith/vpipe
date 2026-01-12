"""
Tests for Kafka pipeline Prometheus metrics.

Validates:
- Metric registration and labels
- Helper function behavior
- Metric value updates
- Integration with producer and consumer
"""

import pytest
from prometheus_client import REGISTRY

from kafka_pipeline.common.metrics import (
    # Metrics
    messages_produced_total,
    messages_produced_bytes,
    messages_consumed_total,
    messages_consumed_bytes,
    consumer_lag,
    consumer_offset,
    processing_errors_total,
    producer_errors_total,
    message_processing_duration_seconds,
    batch_processing_duration_seconds,
    circuit_breaker_state,
    circuit_breaker_failures,
    kafka_connection_status,
    consumer_assigned_partitions,
    # Helper functions
    record_message_produced,
    record_message_consumed,
    record_processing_error,
    record_producer_error,
    update_consumer_lag,
    update_consumer_offset,
    update_circuit_breaker_state,
    record_circuit_breaker_failure,
    update_connection_status,
    update_assigned_partitions,
)


class TestMetricRegistration:
    """Test that all metrics are properly registered with Prometheus."""

    def test_all_metrics_registered(self):
        """Verify all metrics are registered in the Prometheus registry."""
        # Note: Counters don't have _total suffix in their internal name,
        # but they are exported with _total suffix
        metric_names = {
            "kafka_messages_produced",
            "kafka_messages_produced_bytes",
            "kafka_messages_consumed",
            "kafka_messages_consumed_bytes",
            "kafka_consumer_lag",
            "kafka_consumer_offset",
            "kafka_processing_errors",
            "kafka_producer_errors",
            "kafka_message_processing_duration_seconds",
            "kafka_batch_processing_duration_seconds",
            "kafka_circuit_breaker_state",
            "kafka_circuit_breaker_failures",
            "kafka_connection_status",
            "kafka_consumer_assigned_partitions",
        }

        registered_metrics = {
            metric.name
            for metric in REGISTRY.collect()
            if metric.name.startswith("kafka_")
        }

        assert metric_names.issubset(registered_metrics), (
            f"Missing metrics: {metric_names - registered_metrics}"
        )


class TestProducerMetrics:
    """Test producer-related metrics."""

    def test_record_message_produced_success(self):
        """Test recording successful message production."""
        topic = "test-topic"
        message_bytes = 1024

        # Get initial values
        initial_count = messages_produced_total.labels(
            topic=topic, status="success"
        )._value.get()
        initial_bytes = messages_produced_bytes.labels(topic=topic)._value.get()

        # Record successful production
        record_message_produced(topic, message_bytes, success=True)

        # Verify metrics incremented
        assert (
            messages_produced_total.labels(topic=topic, status="success")._value.get()
            == initial_count + 1
        )
        assert (
            messages_produced_bytes.labels(topic=topic)._value.get()
            == initial_bytes + message_bytes
        )

    def test_record_message_produced_failure(self):
        """Test recording failed message production."""
        topic = "test-topic"
        message_bytes = 1024

        # Get initial values
        initial_count = messages_produced_total.labels(
            topic=topic, status="error"
        )._value.get()
        initial_bytes = messages_produced_bytes.labels(topic=topic)._value.get()

        # Record failed production
        record_message_produced(topic, message_bytes, success=False)

        # Verify error counter incremented, bytes not incremented
        assert (
            messages_produced_total.labels(topic=topic, status="error")._value.get()
            == initial_count + 1
        )
        assert messages_produced_bytes.labels(topic=topic)._value.get() == initial_bytes

    def test_record_producer_error(self):
        """Test recording producer errors."""
        topic = "test-topic"
        error_type = "ConnectionError"

        initial_count = producer_errors_total.labels(
            topic=topic, error_type=error_type
        )._value.get()

        record_producer_error(topic, error_type)

        assert (
            producer_errors_total.labels(topic=topic, error_type=error_type)._value.get()
            == initial_count + 1
        )

    def test_batch_processing_duration(self):
        """Test batch processing duration histogram."""
        topic = "test-topic"

        # Record some durations - this test verifies the metric accepts observations
        # without errors
        batch_processing_duration_seconds.labels(topic=topic).observe(0.5)
        batch_processing_duration_seconds.labels(topic=topic).observe(1.5)
        batch_processing_duration_seconds.labels(topic=topic).observe(2.5)

        # If we got here without exceptions, the histogram is working correctly


class TestConsumerMetrics:
    """Test consumer-related metrics."""

    def test_record_message_consumed_success(self):
        """Test recording successful message consumption."""
        topic = "test-topic"
        consumer_group = "test-group"
        message_bytes = 2048

        # Get initial values
        initial_count = messages_consumed_total.labels(
            topic=topic, consumer_group=consumer_group, status="success"
        )._value.get()
        initial_bytes = messages_consumed_bytes.labels(
            topic=topic, consumer_group=consumer_group
        )._value.get()

        # Record successful consumption
        record_message_consumed(topic, consumer_group, message_bytes, success=True)

        # Verify metrics incremented
        assert (
            messages_consumed_total.labels(
                topic=topic, consumer_group=consumer_group, status="success"
            )._value.get()
            == initial_count + 1
        )
        assert (
            messages_consumed_bytes.labels(
                topic=topic, consumer_group=consumer_group
            )._value.get()
            == initial_bytes + message_bytes
        )

    def test_record_message_consumed_failure(self):
        """Test recording failed message consumption."""
        topic = "test-topic"
        consumer_group = "test-group"
        message_bytes = 2048

        # Get initial values
        initial_count = messages_consumed_total.labels(
            topic=topic, consumer_group=consumer_group, status="error"
        )._value.get()
        initial_bytes = messages_consumed_bytes.labels(
            topic=topic, consumer_group=consumer_group
        )._value.get()

        # Record failed consumption
        record_message_consumed(topic, consumer_group, message_bytes, success=False)

        # Verify error counter incremented, bytes not incremented
        assert (
            messages_consumed_total.labels(
                topic=topic, consumer_group=consumer_group, status="error"
            )._value.get()
            == initial_count + 1
        )
        assert (
            messages_consumed_bytes.labels(
                topic=topic, consumer_group=consumer_group
            )._value.get()
            == initial_bytes
        )

    def test_record_processing_error(self):
        """Test recording processing errors by category."""
        topic = "test-topic"
        consumer_group = "test-group"
        error_category = "transient"

        initial_count = processing_errors_total.labels(
            topic=topic, consumer_group=consumer_group, error_category=error_category
        )._value.get()

        record_processing_error(topic, consumer_group, error_category)

        assert (
            processing_errors_total.labels(
                topic=topic, consumer_group=consumer_group, error_category=error_category
            )._value.get()
            == initial_count + 1
        )

    def test_update_consumer_lag(self):
        """Test updating consumer lag gauge."""
        topic = "test-topic"
        partition = 0
        consumer_group = "test-group"
        lag = 100

        update_consumer_lag(topic, partition, consumer_group, lag)

        assert (
            consumer_lag.labels(
                topic=topic, partition=str(partition), consumer_group=consumer_group
            )._value.get()
            == lag
        )

    def test_update_consumer_offset(self):
        """Test updating consumer offset gauge."""
        topic = "test-topic"
        partition = 0
        consumer_group = "test-group"
        offset = 1000

        update_consumer_offset(topic, partition, consumer_group, offset)

        assert (
            consumer_offset.labels(
                topic=topic, partition=str(partition), consumer_group=consumer_group
            )._value.get()
            == offset
        )

    def test_update_assigned_partitions(self):
        """Test updating assigned partitions gauge."""
        consumer_group = "test-group"
        count = 5

        update_assigned_partitions(consumer_group, count)

        assert (
            consumer_assigned_partitions.labels(consumer_group=consumer_group)
            ._value.get()
            == count
        )

    def test_message_processing_duration(self):
        """Test message processing duration histogram."""
        topic = "test-topic"
        consumer_group = "test-group"

        # Record some durations - this test verifies the metric accepts observations
        # without errors
        message_processing_duration_seconds.labels(
            topic=topic, consumer_group=consumer_group
        ).observe(0.01)
        message_processing_duration_seconds.labels(
            topic=topic, consumer_group=consumer_group
        ).observe(0.05)
        message_processing_duration_seconds.labels(
            topic=topic, consumer_group=consumer_group
        ).observe(0.1)

        # If we got here without exceptions, the histogram is working correctly


class TestCircuitBreakerMetrics:
    """Test circuit breaker metrics."""

    def test_update_circuit_breaker_state(self):
        """Test updating circuit breaker state gauge."""
        component = "producer"
        state = 1  # OPEN

        update_circuit_breaker_state(component, state)

        assert (
            circuit_breaker_state.labels(component=component)._value.get() == state
        )

    def test_record_circuit_breaker_failure(self):
        """Test recording circuit breaker failures."""
        component = "consumer"

        initial_count = circuit_breaker_failures.labels(
            component=component
        )._value.get()

        record_circuit_breaker_failure(component)

        assert (
            circuit_breaker_failures.labels(component=component)._value.get()
            == initial_count + 1
        )


class TestConnectionMetrics:
    """Test connection status metrics."""

    def test_update_connection_status_connected(self):
        """Test updating connection status to connected."""
        component = "producer"

        update_connection_status(component, connected=True)

        assert kafka_connection_status.labels(component=component)._value.get() == 1

    def test_update_connection_status_disconnected(self):
        """Test updating connection status to disconnected."""
        component = "consumer"

        update_connection_status(component, connected=False)

        assert kafka_connection_status.labels(component=component)._value.get() == 0


class TestMetricLabels:
    """Test that metrics have correct labels."""

    def test_producer_metrics_have_topic_label(self):
        """Verify producer metrics include topic label."""
        # Access metrics with labels to ensure they work
        messages_produced_total.labels(topic="test", status="success")
        messages_produced_bytes.labels(topic="test")
        producer_errors_total.labels(topic="test", error_type="timeout")

    def test_consumer_metrics_have_required_labels(self):
        """Verify consumer metrics include required labels."""
        # Access metrics with labels to ensure they work
        messages_consumed_total.labels(
            topic="test", consumer_group="group", status="success"
        )
        messages_consumed_bytes.labels(topic="test", consumer_group="group")
        processing_errors_total.labels(
            topic="test", consumer_group="group", error_category="transient"
        )
        consumer_lag.labels(topic="test", partition="0", consumer_group="group")
        consumer_offset.labels(topic="test", partition="0", consumer_group="group")

    def test_histogram_metrics_have_buckets(self):
        """Verify histogram metrics have configured buckets."""
        # Check processing duration buckets
        processing_buckets = message_processing_duration_seconds.labels(
            topic="test", consumer_group="group"
        )._buckets
        assert len(processing_buckets) > 0

        # Check batch duration buckets
        batch_buckets = batch_processing_duration_seconds.labels(topic="test")._buckets
        assert len(batch_buckets) > 0


class TestMetricIntegration:
    """Test integration scenarios with multiple metrics."""

    def test_successful_message_flow(self):
        """Test metrics for a successful message flow."""
        topic = "integration-test"
        consumer_group = "integration-group"
        partition = 0

        # Producer sends message
        record_message_produced(topic, 512, success=True)

        # Consumer receives and processes message
        record_message_consumed(topic, consumer_group, 512, success=True)
        update_consumer_offset(topic, partition, consumer_group, 100)
        update_consumer_lag(topic, partition, consumer_group, 0)
        message_processing_duration_seconds.labels(
            topic=topic, consumer_group=consumer_group
        ).observe(0.05)

        # Verify all metrics updated
        assert (
            messages_produced_total.labels(topic=topic, status="success")._value.get()
            > 0
        )
        assert (
            messages_consumed_total.labels(
                topic=topic, consumer_group=consumer_group, status="success"
            )._value.get()
            > 0
        )
        assert (
            consumer_offset.labels(
                topic=topic, partition=str(partition), consumer_group=consumer_group
            )._value.get()
            == 100
        )
        assert (
            consumer_lag.labels(
                topic=topic, partition=str(partition), consumer_group=consumer_group
            )._value.get()
            == 0
        )

    def test_failed_message_flow(self):
        """Test metrics for a failed message flow."""
        topic = "integration-test-fail"
        consumer_group = "integration-group-fail"

        # Producer sends message
        record_message_produced(topic, 512, success=True)

        # Consumer receives but fails to process
        record_message_consumed(topic, consumer_group, 512, success=False)
        record_processing_error(topic, consumer_group, "permanent")
        message_processing_duration_seconds.labels(
            topic=topic, consumer_group=consumer_group
        ).observe(0.01)

        # Verify error metrics updated
        assert (
            messages_consumed_total.labels(
                topic=topic, consumer_group=consumer_group, status="error"
            )._value.get()
            > 0
        )
        assert (
            processing_errors_total.labels(
                topic=topic, consumer_group=consumer_group, error_category="permanent"
            )._value.get()
            > 0
        )
