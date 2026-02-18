"""Tests for pipeline.common.telemetry module."""

import pipeline.common.telemetry as telemetry_mod
from pipeline.common.telemetry import (
    _NoOpScope,
    _NoOpSpan,
    _NoOpTracer,
    get_prometheus_registry,
    get_tracer,
    initialize_telemetry,
    initialize_worker_telemetry,
    is_available,
    is_initialized,
    shutdown_telemetry,
)


def _reset_telemetry():
    """Reset module-level state for test isolation."""
    telemetry_mod._initialized = False
    telemetry_mod._telemetry_available = False
    telemetry_mod._prometheus_registry = None


class TestInitializeTelemetry:
    def setup_method(self):
        _reset_telemetry()

    def teardown_method(self):
        _reset_telemetry()

    def test_disabled_via_env(self, monkeypatch):
        monkeypatch.setenv("ENABLE_TELEMETRY", "false")
        initialize_telemetry("test-service")
        assert is_initialized() is True
        assert is_available() is False

    def test_disabled_via_env_zero(self, monkeypatch):
        monkeypatch.setenv("ENABLE_TELEMETRY", "0")
        initialize_telemetry("test-service")
        assert is_initialized() is True
        assert is_available() is False

    def test_initialize_with_metrics(self, monkeypatch):
        monkeypatch.delenv("ENABLE_TELEMETRY", raising=False)
        initialize_telemetry("test-service", enable_metrics=True)
        assert is_initialized() is True
        # prometheus-client should be available in test env
        assert get_prometheus_registry() is not None

    def test_double_init_skipped(self, monkeypatch):
        monkeypatch.delenv("ENABLE_TELEMETRY", raising=False)
        initialize_telemetry("test-service")
        registry = get_prometheus_registry()
        initialize_telemetry("test-service-2")  # should be no-op
        assert get_prometheus_registry() is registry

    def test_metrics_disabled(self, monkeypatch):
        monkeypatch.delenv("ENABLE_TELEMETRY", raising=False)
        initialize_telemetry("test-service", enable_metrics=False)
        assert is_initialized() is True
        assert get_prometheus_registry() is None


class TestInitializeWorkerTelemetry:
    def setup_method(self):
        _reset_telemetry()

    def teardown_method(self):
        _reset_telemetry()

    def test_initializes_with_domain_worker(self, monkeypatch):
        monkeypatch.delenv("ENABLE_TELEMETRY", raising=False)
        monkeypatch.setenv("ENVIRONMENT", "staging")
        initialize_worker_telemetry("verisk", "download-worker")
        assert is_initialized() is True


class TestShutdownTelemetry:
    def setup_method(self):
        _reset_telemetry()

    def teardown_method(self):
        _reset_telemetry()

    def test_shutdown_resets_initialized(self, monkeypatch):
        monkeypatch.delenv("ENABLE_TELEMETRY", raising=False)
        initialize_telemetry("test-service")
        assert is_initialized() is True
        shutdown_telemetry()
        assert is_initialized() is False

    def test_shutdown_when_not_initialized(self):
        shutdown_telemetry()  # should not raise
        assert is_initialized() is False


class TestNoOpTracer:
    def test_start_active_span(self):
        tracer = _NoOpTracer()
        with tracer.start_active_span("test-op") as scope:
            assert isinstance(scope, _NoOpScope)
            assert isinstance(scope.span, _NoOpSpan)
            scope.span.set_tag("key", "value")  # should not raise

    def test_get_tracer(self):
        tracer = get_tracer("test")
        assert isinstance(tracer, _NoOpTracer)
