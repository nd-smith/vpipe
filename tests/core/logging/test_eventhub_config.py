"""Tests for core.logging.eventhub_config module."""

import logging

from core.logging.eventhub_config import prepare_eventhub_logging_config


class TestPrepareEventhubLoggingConfig:
    def test_returns_none_when_disabled(self):
        config = {"eventhub_logging": {"enabled": False}}
        assert prepare_eventhub_logging_config(config) is None

    def test_returns_none_when_no_connection_string(self, monkeypatch):
        monkeypatch.delenv("EVENTHUB_NAMESPACE_CONNECTION_STRING", raising=False)
        config = {"eventhub_logging": {"enabled": True}}
        assert prepare_eventhub_logging_config(config) is None

    def test_returns_config_with_defaults(self, monkeypatch):
        monkeypatch.setenv("EVENTHUB_NAMESPACE_CONNECTION_STRING", "Endpoint=sb://test")
        config = {"eventhub_logging": {"enabled": True}}
        result = prepare_eventhub_logging_config(config)
        assert result is not None
        assert result["connection_string"] == "Endpoint=sb://test"
        assert result["eventhub_name"] == "application-logs"
        assert result["level"] == logging.INFO
        assert result["batch_size"] == 100
        assert result["batch_timeout_seconds"] == 1.0
        assert result["max_queue_size"] == 10000
        assert result["circuit_breaker_threshold"] == 5

    def test_custom_values(self, monkeypatch):
        monkeypatch.setenv("EVENTHUB_NAMESPACE_CONNECTION_STRING", "Endpoint=sb://test")
        config = {
            "eventhub_logging": {
                "enabled": True,
                "level": "WARNING",
                "eventhub_name": "custom-logs",
                "batch_size": 50,
                "batch_timeout_seconds": 2.5,
                "max_queue_size": 5000,
                "circuit_breaker_threshold": 10,
            }
        }
        result = prepare_eventhub_logging_config(config)
        assert result["level"] == logging.WARNING
        assert result["eventhub_name"] == "custom-logs"
        assert result["batch_size"] == 50
        assert result["batch_timeout_seconds"] == 2.5
        assert result["max_queue_size"] == 5000
        assert result["circuit_breaker_threshold"] == 10

    def test_empty_eventhub_logging_section(self, monkeypatch):
        monkeypatch.setenv("EVENTHUB_NAMESPACE_CONNECTION_STRING", "Endpoint=sb://test")
        config = {}
        # eventhub_logging defaults to {} -> enabled defaults to True
        result = prepare_eventhub_logging_config(config)
        assert result is not None

    def test_invalid_log_level_defaults_to_info(self, monkeypatch):
        monkeypatch.setenv("EVENTHUB_NAMESPACE_CONNECTION_STRING", "Endpoint=sb://test")
        config = {"eventhub_logging": {"level": "NONEXISTENT"}}
        result = prepare_eventhub_logging_config(config)
        assert result["level"] == logging.INFO
