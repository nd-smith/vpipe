"""Tests for Kafka security configuration builder."""

import ssl
from unittest.mock import Mock, patch

from pipeline.common.kafka_config import build_kafka_security_config


def _make_config(**overrides):
    config = Mock()
    config.security_protocol = overrides.get("security_protocol", "PLAINTEXT")
    config.sasl_mechanism = overrides.get("sasl_mechanism", "PLAIN")
    config.sasl_plain_username = overrides.get("sasl_plain_username", "user")
    config.sasl_plain_password = overrides.get("sasl_plain_password", "pass")
    config.sasl_kerberos_service_name = overrides.get(
        "sasl_kerberos_service_name", "kafka"
    )
    return config


class TestBuildKafkaSecurityConfigPlaintext:

    def test_returns_empty_dict_for_plaintext(self):
        config = _make_config(security_protocol="PLAINTEXT")
        result = build_kafka_security_config(config)
        assert result == {}


class TestBuildKafkaSecurityConfigSASLPlain:

    def test_sasl_plain_includes_username_and_password(self):
        config = _make_config(
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_plain_username="myuser",
            sasl_plain_password="mypass",
        )
        result = build_kafka_security_config(config)

        assert result["security_protocol"] == "SASL_PLAINTEXT"
        assert result["sasl_mechanism"] == "PLAIN"
        assert result["sasl_plain_username"] == "myuser"
        assert result["sasl_plain_password"] == "mypass"
        assert "ssl_context" not in result

    def test_sasl_ssl_plain_includes_ssl_context(self):
        config = _make_config(
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_plain_username="u",
            sasl_plain_password="p",
        )
        result = build_kafka_security_config(config)

        assert result["security_protocol"] == "SASL_SSL"
        assert isinstance(result["ssl_context"], ssl.SSLContext)
        assert result["sasl_plain_username"] == "u"
        assert result["sasl_plain_password"] == "p"


class TestBuildKafkaSecurityConfigOAuthBearer:

    @patch("pipeline.common.kafka_config.create_eventhub_oauth_callback")
    def test_oauthbearer_sets_token_provider(self, mock_create_callback):
        mock_provider = Mock()
        mock_create_callback.return_value = mock_provider

        config = _make_config(
            security_protocol="SASL_SSL",
            sasl_mechanism="OAUTHBEARER",
        )
        result = build_kafka_security_config(config)

        assert result["sasl_oauth_token_provider"] is mock_provider
        mock_create_callback.assert_called_once()
        assert "sasl_plain_username" not in result
        assert "sasl_plain_password" not in result

    @patch("pipeline.common.kafka_config.create_eventhub_oauth_callback")
    def test_oauthbearer_includes_ssl_context(self, mock_create_callback):
        mock_create_callback.return_value = Mock()
        config = _make_config(
            security_protocol="SASL_SSL",
            sasl_mechanism="OAUTHBEARER",
        )
        result = build_kafka_security_config(config)

        assert isinstance(result["ssl_context"], ssl.SSLContext)


class TestBuildKafkaSecurityConfigGSSAPI:

    def test_gssapi_sets_kerberos_service_name(self):
        config = _make_config(
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="GSSAPI",
            sasl_kerberos_service_name="my-kafka",
        )
        result = build_kafka_security_config(config)

        assert result["sasl_mechanism"] == "GSSAPI"
        assert result["sasl_kerberos_service_name"] == "my-kafka"
        assert "sasl_plain_username" not in result


class TestBuildKafkaSecurityConfigSSL:

    def test_ssl_protocol_creates_ssl_context(self):
        config = _make_config(
            security_protocol="SSL",
            sasl_mechanism="PLAIN",
        )
        result = build_kafka_security_config(config)

        assert isinstance(result["ssl_context"], ssl.SSLContext)
        assert result["security_protocol"] == "SSL"
