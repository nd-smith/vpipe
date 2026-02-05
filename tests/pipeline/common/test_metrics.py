"""
Unit tests for pipeline metrics module.

Test Coverage:
    - NoOpMetric class (method chaining, arbitrary calls)
    - Lazy initialization of prometheus_client
    - Registry integration via telemetry module
    - Metric creation (Counter, Gauge, Histogram)
    - Duplicate metric handling (ValueError recovery)
    - Convenience functions (all helpers)
    - Module-level metrics initialization

No prometheus infrastructure required - all tests use mocks.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import sys


class TestNoOpMetric:
    """Test NoOpMetric graceful degradation."""

    def test_accepts_any_method_call(self):
        """NoOpMetric accepts arbitrary method calls."""
        from pipeline.common.metrics import NoOpMetric

        metric = NoOpMetric()

        # Should accept any method call
        result = metric.inc()
        assert isinstance(result, NoOpMetric)

    def test_method_chaining_works(self):
        """NoOpMetric supports method chaining."""
        from pipeline.common.metrics import NoOpMetric

        metric = NoOpMetric()

        # Should chain indefinitely
        result = metric.labels(topic="test").inc().labels(foo="bar").set(42)
        assert isinstance(result, NoOpMetric)

    def test_accepts_arbitrary_arguments(self):
        """NoOpMetric accepts any arguments."""
        from pipeline.common.metrics import NoOpMetric

        metric = NoOpMetric()

        # Should accept positional and keyword args
        result = metric.some_method(1, 2, 3, foo="bar", baz=42)
        assert isinstance(result, NoOpMetric)

    def test_multiple_label_calls(self):
        """NoOpMetric handles multiple label calls."""
        from pipeline.common.metrics import NoOpMetric

        metric = NoOpMetric()

        # Simulate real prometheus usage pattern
        result = metric.labels(topic="test").labels(consumer_group="group1").inc()
        assert isinstance(result, NoOpMetric)


class TestLazyInitialization:
    """Test lazy loading of prometheus_client."""

    def setup_method(self):
        """Reset module state before each test."""
        import pipeline.common.metrics as metrics_module

        metrics_module._prometheus_client = None
        metrics_module._metrics_available = False

    def test_ensure_prometheus_loads_successfully(self):
        """_ensure_prometheus loads prometheus_client when available."""
        mock_prometheus = Mock()

        with patch.dict(sys.modules, {"prometheus_client": mock_prometheus}):
            from pipeline.common.metrics import _ensure_prometheus
            import pipeline.common.metrics as metrics_module

            result = _ensure_prometheus()

            assert result is True
            assert metrics_module._prometheus_client is mock_prometheus
            assert metrics_module._metrics_available is True

    def test_ensure_prometheus_handles_import_error(self):
        """_ensure_prometheus handles missing prometheus_client."""
        import pipeline.common.metrics as metrics_module
        import builtins

        # Reset state
        metrics_module._prometheus_client = None
        metrics_module._metrics_available = False

        # Mock __import__ to raise ImportError for prometheus_client
        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "prometheus_client":
                raise ImportError("No module named 'prometheus_client'")
            return real_import(name, *args, **kwargs)

        with patch.object(builtins, "__import__", side_effect=mock_import):
            from pipeline.common.metrics import _ensure_prometheus

            result = _ensure_prometheus()

            assert result is False
            assert metrics_module._metrics_available is False

    def test_ensure_prometheus_only_loads_once(self):
        """_ensure_prometheus uses cached import."""
        mock_prometheus = Mock()

        with patch.dict(sys.modules, {"prometheus_client": mock_prometheus}):
            from pipeline.common.metrics import _ensure_prometheus

            # First call loads
            result1 = _ensure_prometheus()
            # Second call uses cache
            result2 = _ensure_prometheus()

            assert result1 is True
            assert result2 is True


class TestRegistryIntegration:
    """Test Prometheus registry integration via telemetry module."""

    def test_get_registry_returns_telemetry_registry(self):
        """_get_registry returns registry from telemetry module."""
        mock_registry = Mock()

        with patch(
            "pipeline.common.telemetry.get_prometheus_registry",
            return_value=mock_registry,
        ):
            from pipeline.common.metrics import _get_registry

            result = _get_registry()

            assert result is mock_registry

    def test_get_registry_handles_import_error(self):
        """_get_registry returns None when telemetry unavailable."""
        # Patch the import to raise ImportError
        with patch.dict("sys.modules", {"pipeline.common.telemetry": None}):
            # Force re-import of _get_registry to trigger ImportError path
            import importlib
            import pipeline.common.metrics

            importlib.reload(pipeline.common.metrics)
            from pipeline.common.metrics import _get_registry

            result = _get_registry()

            assert result is None


class TestMetricCreation:
    """Test Counter, Gauge, and Histogram creation."""

    def setup_method(self):
        """Reset module state before each test."""
        import pipeline.common.metrics as metrics_module

        metrics_module._prometheus_client = None
        metrics_module._metrics_available = False

    def test_create_counter_returns_real_counter(self):
        """_create_counter returns real Counter when prometheus available."""
        mock_registry = Mock()
        mock_counter_class = Mock()
        mock_counter_instance = Mock()
        mock_counter_class.return_value = mock_counter_instance

        mock_prometheus = Mock()
        mock_prometheus.Counter = mock_counter_class

        with patch.dict(sys.modules, {"prometheus_client": mock_prometheus}):
            with patch(
                "pipeline.common.telemetry.get_prometheus_registry",
                return_value=mock_registry,
            ):
                from pipeline.common.metrics import _create_counter

                result = _create_counter(
                    "test_counter", "Test counter", labelnames=["label1"]
                )

                assert result is mock_counter_instance
                mock_counter_class.assert_called_once_with(
                    "test_counter",
                    "Test counter",
                    labelnames=["label1"],
                    registry=mock_registry,
                )

    def test_create_counter_returns_noop_when_prometheus_unavailable(self):
        """_create_counter returns NoOpMetric when prometheus unavailable."""
        with patch("pipeline.common.metrics._ensure_prometheus", return_value=False):
            from pipeline.common.metrics import _create_counter, NoOpMetric

            result = _create_counter("test_counter", "Test counter")

            assert isinstance(result, NoOpMetric)

    def test_create_counter_returns_noop_when_no_registry(self):
        """_create_counter returns NoOpMetric when registry unavailable."""
        mock_prometheus = Mock()

        with patch.dict(sys.modules, {"prometheus_client": mock_prometheus}):
            with patch(
                "pipeline.common.telemetry.get_prometheus_registry", return_value=None
            ):
                from pipeline.common.metrics import _create_counter, NoOpMetric

                result = _create_counter("test_counter", "Test counter")

                assert isinstance(result, NoOpMetric)

    def test_create_counter_handles_duplicate_registration(self):
        """_create_counter returns existing counter on duplicate registration."""
        mock_registry = Mock()
        mock_existing_counter = Mock()
        mock_registry._collector_to_names = {("test_counter",): mock_existing_counter}

        mock_counter_class = Mock()
        mock_counter_class.side_effect = ValueError("Duplicate metric")

        mock_prometheus = Mock()
        mock_prometheus.Counter = mock_counter_class

        with patch.dict(sys.modules, {"prometheus_client": mock_prometheus}):
            with patch(
                "pipeline.common.telemetry.get_prometheus_registry",
                return_value=mock_registry,
            ):
                from pipeline.common.metrics import _create_counter

                result = _create_counter("test_counter", "Test counter")

                assert result is mock_existing_counter

    def test_create_gauge_returns_real_gauge(self):
        """_create_gauge returns real Gauge when prometheus available."""
        mock_registry = Mock()
        mock_gauge_class = Mock()
        mock_gauge_instance = Mock()
        mock_gauge_class.return_value = mock_gauge_instance

        mock_prometheus = Mock()
        mock_prometheus.Gauge = mock_gauge_class

        with patch.dict(sys.modules, {"prometheus_client": mock_prometheus}):
            with patch(
                "pipeline.common.telemetry.get_prometheus_registry",
                return_value=mock_registry,
            ):
                from pipeline.common.metrics import _create_gauge

                result = _create_gauge(
                    "test_gauge", "Test gauge", labelnames=["label1"]
                )

                assert result is mock_gauge_instance
                mock_gauge_class.assert_called_once_with(
                    "test_gauge",
                    "Test gauge",
                    labelnames=["label1"],
                    registry=mock_registry,
                )

    def test_create_histogram_returns_real_histogram(self):
        """_create_histogram returns real Histogram when prometheus available."""
        mock_registry = Mock()
        mock_histogram_class = Mock()
        mock_histogram_instance = Mock()
        mock_histogram_class.return_value = mock_histogram_instance

        mock_prometheus = Mock()
        mock_prometheus.Histogram = mock_histogram_class

        with patch.dict(sys.modules, {"prometheus_client": mock_prometheus}):
            with patch(
                "pipeline.common.telemetry.get_prometheus_registry",
                return_value=mock_registry,
            ):
                from pipeline.common.metrics import _create_histogram

                result = _create_histogram(
                    "test_histogram",
                    "Test histogram",
                    labelnames=["label1"],
                    buckets=[0.1, 0.5, 1.0],
                )

                assert result is mock_histogram_instance
                mock_histogram_class.assert_called_once_with(
                    name="test_histogram",
                    documentation="Test histogram",
                    labelnames=["label1"],
                    registry=mock_registry,
                    buckets=[0.1, 0.5, 1.0],
                )

    def test_create_histogram_without_buckets(self):
        """_create_histogram works without custom buckets."""
        mock_registry = Mock()
        mock_histogram_class = Mock()
        mock_histogram_instance = Mock()
        mock_histogram_class.return_value = mock_histogram_instance

        mock_prometheus = Mock()
        mock_prometheus.Histogram = mock_histogram_class

        with patch.dict(sys.modules, {"prometheus_client": mock_prometheus}):
            with patch(
                "pipeline.common.telemetry.get_prometheus_registry",
                return_value=mock_registry,
            ):
                from pipeline.common.metrics import _create_histogram

                result = _create_histogram("test_histogram", "Test histogram")

                assert result is mock_histogram_instance
                # Verify buckets not in call
                call_kwargs = mock_histogram_class.call_args[1]
                assert "buckets" not in call_kwargs


class TestConvenienceFunctions:
    """Test convenience wrapper functions."""

    def test_record_message_produced_increments_counter(self):
        """record_message_produced increments counter with correct labels."""
        mock_counter = Mock()
        mock_labels = Mock()
        mock_counter.labels.return_value = mock_labels

        with patch(
            "pipeline.common.metrics.messages_produced_counter", mock_counter
        ):
            from pipeline.common.metrics import record_message_produced

            record_message_produced("test-topic", 100)

            mock_counter.labels.assert_called_once_with(topic="test-topic")
            mock_labels.inc.assert_called_once()

    def test_record_message_produced_records_failure(self):
        """record_message_produced records producer error on failure."""
        mock_produced_counter = Mock()
        mock_produced_labels = Mock()
        mock_produced_counter.labels.return_value = mock_produced_labels

        mock_error_counter = Mock()
        mock_error_labels = Mock()
        mock_error_counter.labels.return_value = mock_error_labels

        with patch(
            "pipeline.common.metrics.messages_produced_counter", mock_produced_counter
        ):
            with patch(
                "pipeline.common.metrics.producer_errors_counter", mock_error_counter
            ):
                from pipeline.common.metrics import record_message_produced

                record_message_produced("test-topic", 100, success=False)

                mock_produced_labels.inc.assert_called_once()
                mock_error_counter.labels.assert_called_once_with(
                    topic="test-topic", error_type="send_failed"
                )
                mock_error_labels.inc.assert_called_once()

    def test_record_message_consumed_increments_counter(self):
        """record_message_consumed increments counter with correct labels."""
        mock_counter = Mock()
        mock_labels = Mock()
        mock_counter.labels.return_value = mock_labels

        with patch(
            "pipeline.common.metrics.messages_consumed_counter", mock_counter
        ):
            from pipeline.common.metrics import record_message_consumed

            record_message_consumed("test-topic", "test-group", 100)

            mock_counter.labels.assert_called_once_with(
                topic="test-topic", consumer_group="test-group"
            )
            mock_labels.inc.assert_called_once()

    def test_record_message_consumed_records_failure(self):
        """record_message_consumed records processing error on failure."""
        mock_consumed_counter = Mock()
        mock_consumed_labels = Mock()
        mock_consumed_counter.labels.return_value = mock_consumed_labels

        mock_error_counter = Mock()
        mock_error_labels = Mock()
        mock_error_counter.labels.return_value = mock_error_labels

        with patch(
            "pipeline.common.metrics.messages_consumed_counter", mock_consumed_counter
        ):
            with patch(
                "pipeline.common.metrics.processing_errors_counter", mock_error_counter
            ):
                from pipeline.common.metrics import record_message_consumed

                record_message_consumed("test-topic", "test-group", 100, success=False)

                mock_consumed_labels.inc.assert_called_once()
                mock_error_counter.labels.assert_called_once_with(
                    topic="test-topic",
                    consumer_group="test-group",
                    error_category="processing_failed",
                )
                mock_error_labels.inc.assert_called_once()

    def test_record_processing_error(self):
        """record_processing_error increments error counter."""
        mock_counter = Mock()
        mock_labels = Mock()
        mock_counter.labels.return_value = mock_labels

        with patch(
            "pipeline.common.metrics.processing_errors_counter", mock_counter
        ):
            from pipeline.common.metrics import record_processing_error

            record_processing_error("test-topic", "test-group", "schema_error")

            mock_counter.labels.assert_called_once_with(
                topic="test-topic",
                consumer_group="test-group",
                error_category="schema_error",
            )
            mock_labels.inc.assert_called_once()

    def test_record_producer_error(self):
        """record_producer_error increments producer error counter."""
        mock_counter = Mock()
        mock_labels = Mock()
        mock_counter.labels.return_value = mock_labels

        with patch("pipeline.common.metrics.producer_errors_counter", mock_counter):
            from pipeline.common.metrics import record_producer_error

            record_producer_error("test-topic", "timeout")

            mock_counter.labels.assert_called_once_with(
                topic="test-topic", error_type="timeout"
            )
            mock_labels.inc.assert_called_once()

    def test_update_consumer_lag(self):
        """update_consumer_lag sets gauge value."""
        mock_gauge = Mock()
        mock_labels = Mock()
        mock_gauge.labels.return_value = mock_labels

        with patch("pipeline.common.metrics.consumer_lag_gauge", mock_gauge):
            from pipeline.common.metrics import update_consumer_lag

            update_consumer_lag("test-topic", 0, "test-group", 42)

            mock_gauge.labels.assert_called_once_with(
                topic="test-topic", partition="0", consumer_group="test-group"
            )
            mock_labels.set.assert_called_once_with(42)

    def test_update_consumer_offset(self):
        """update_consumer_offset sets gauge value."""
        mock_gauge = Mock()
        mock_labels = Mock()
        mock_gauge.labels.return_value = mock_labels

        with patch("pipeline.common.metrics.consumer_offset_gauge", mock_gauge):
            from pipeline.common.metrics import update_consumer_offset

            update_consumer_offset("test-topic", 5, "test-group", 1000)

            mock_gauge.labels.assert_called_once_with(
                topic="test-topic", partition="5", consumer_group="test-group"
            )
            mock_labels.set.assert_called_once_with(1000)

    def test_update_connection_status_connected(self):
        """update_connection_status sets gauge to 1 when connected."""
        mock_gauge = Mock()
        mock_labels = Mock()
        mock_gauge.labels.return_value = mock_labels

        with patch(
            "pipeline.common.metrics.kafka_connection_status_gauge", mock_gauge
        ):
            from pipeline.common.metrics import update_connection_status

            update_connection_status("producer", True)

            mock_gauge.labels.assert_called_once_with(component="producer")
            mock_labels.set.assert_called_once_with(1)

    def test_update_connection_status_disconnected(self):
        """update_connection_status sets gauge to 0 when disconnected."""
        mock_gauge = Mock()
        mock_labels = Mock()
        mock_gauge.labels.return_value = mock_labels

        with patch(
            "pipeline.common.metrics.kafka_connection_status_gauge", mock_gauge
        ):
            from pipeline.common.metrics import update_connection_status

            update_connection_status("consumer", False)

            mock_gauge.labels.assert_called_once_with(component="consumer")
            mock_labels.set.assert_called_once_with(0)

    def test_update_assigned_partitions(self):
        """update_assigned_partitions sets gauge value."""
        mock_gauge = Mock()
        mock_labels = Mock()
        mock_gauge.labels.return_value = mock_labels

        with patch(
            "pipeline.common.metrics.consumer_assigned_partitions_gauge", mock_gauge
        ):
            from pipeline.common.metrics import update_assigned_partitions

            update_assigned_partitions("test-group", 3)

            mock_gauge.labels.assert_called_once_with(consumer_group="test-group")
            mock_labels.set.assert_called_once_with(3)

    def test_record_delta_write_success(self):
        """record_delta_write increments counter with success=true."""
        mock_counter = Mock()
        mock_labels = Mock()
        mock_counter.labels.return_value = mock_labels

        with patch("pipeline.common.metrics.delta_writes_counter", mock_counter):
            from pipeline.common.metrics import record_delta_write

            record_delta_write("events_table", 10, success=True)

            mock_counter.labels.assert_called_once_with(
                table="events_table", success="true"
            )
            mock_labels.inc.assert_called_once()

    def test_record_delta_write_failure(self):
        """record_delta_write increments counter with success=false."""
        mock_counter = Mock()
        mock_labels = Mock()
        mock_counter.labels.return_value = mock_labels

        with patch("pipeline.common.metrics.delta_writes_counter", mock_counter):
            from pipeline.common.metrics import record_delta_write

            record_delta_write("events_table", 10, success=False)

            mock_counter.labels.assert_called_once_with(
                table="events_table", success="false"
            )
            mock_labels.inc.assert_called_once()

    def test_record_dlq_message(self):
        """record_dlq_message increments counter."""
        mock_counter = Mock()
        mock_labels = Mock()
        mock_counter.labels.return_value = mock_labels

        with patch("pipeline.common.metrics.dlq_messages_counter", mock_counter):
            from pipeline.common.metrics import record_dlq_message

            record_dlq_message("verisk", "max_retries_exceeded")

            mock_counter.labels.assert_called_once_with(
                domain="verisk", reason="max_retries_exceeded"
            )
            mock_labels.inc.assert_called_once()


class TestModuleLevelMetrics:
    """Test module-level metric initialization."""

    def test_module_exports_all_metrics(self):
        """Module exports all expected metrics in __all__."""
        from pipeline.common import metrics

        # Verify key metrics are defined
        assert hasattr(metrics, "messages_produced_counter")
        assert hasattr(metrics, "messages_consumed_counter")
        assert hasattr(metrics, "consumer_lag_gauge")
        assert hasattr(metrics, "consumer_offset_gauge")
        assert hasattr(metrics, "processing_errors_counter")
        assert hasattr(metrics, "producer_errors_counter")
        assert hasattr(metrics, "delta_writes_counter")
        assert hasattr(metrics, "dlq_messages_counter")
        assert hasattr(metrics, "kafka_connection_status_gauge")
        assert hasattr(metrics, "consumer_assigned_partitions_gauge")
        assert hasattr(metrics, "message_processing_duration_seconds")

    def test_module_exports_all_functions(self):
        """Module exports all expected convenience functions."""
        from pipeline.common import metrics

        # Verify helper functions are defined
        assert hasattr(metrics, "record_message_produced")
        assert hasattr(metrics, "record_message_consumed")
        assert hasattr(metrics, "record_processing_error")
        assert hasattr(metrics, "record_producer_error")
        assert hasattr(metrics, "update_consumer_lag")
        assert hasattr(metrics, "update_consumer_offset")
        assert hasattr(metrics, "update_connection_status")
        assert hasattr(metrics, "update_assigned_partitions")
        assert hasattr(metrics, "record_delta_write")
        assert hasattr(metrics, "record_dlq_message")

    def test_metrics_are_callable_when_prometheus_unavailable(self):
        """Module-level metrics work as NoOpMetric when prometheus unavailable."""
        from pipeline.common import metrics

        # Should not raise even if prometheus unavailable
        metrics.messages_produced_counter.labels(topic="test").inc()
        metrics.consumer_lag_gauge.labels(
            topic="test", partition="0", consumer_group="group"
        ).set(42)
        metrics.message_processing_duration_seconds.labels(
            topic="test", consumer_group="group"
        ).observe(0.5)
