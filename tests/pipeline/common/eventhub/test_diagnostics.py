"""Tests for pipeline.common.eventhub.diagnostics module.

Covers mask_connection_string, parse_connection_string, extract_namespace_host,
test_dns_resolution, log_connection_diagnostics, and log_connection_attempt_details.
"""

import logging
import os
import socket
from unittest.mock import MagicMock, patch

import pytest

from pipeline.common.eventhub.diagnostics import (
    extract_namespace_host,
    log_connection_attempt_details,
    log_connection_diagnostics,
    mask_connection_string,
    parse_connection_string,
    test_dns_resolution,
)


# =============================================================================
# mask_connection_string
# =============================================================================


class TestMaskConnectionString:
    def test_masks_shared_access_key(self):
        conn = (
            "Endpoint=sb://myhub.servicebus.windows.net/;"
            "SharedAccessKeyName=RootManageSharedAccessKey;"
            "SharedAccessKey=abc123xyz"
        )
        result = mask_connection_string(conn)
        assert "abc123xyz" not in result
        assert "***MASKED***" in result
        assert "RootManageSharedAccessKey" in result
        assert "myhub.servicebus.windows.net" in result

    def test_empty_string_returns_empty(self):
        assert mask_connection_string("") == ""

    def test_none_returns_empty(self):
        assert mask_connection_string(None) == ""

    def test_no_key_returns_string_unchanged(self):
        conn = "Endpoint=sb://myhub.servicebus.windows.net/"
        assert mask_connection_string(conn) == conn

    def test_case_insensitive_masking(self):
        conn = "sharedaccesskey=secretvalue;Endpoint=sb://x.net/"
        result = mask_connection_string(conn)
        assert "secretvalue" not in result
        assert "***MASKED***" in result

    def test_key_with_special_characters(self):
        conn = (
            "Endpoint=sb://x.net/;"
            "SharedAccessKey=abc+def/ghi=123"
        )
        result = mask_connection_string(conn)
        assert "abc+def/ghi=123" not in result
        assert "***MASKED***" in result


# =============================================================================
# parse_connection_string
# =============================================================================


class TestParseConnectionString:
    def test_parses_standard_connection_string(self):
        conn = (
            "Endpoint=sb://myhub.servicebus.windows.net/;"
            "SharedAccessKeyName=RootPolicy;"
            "SharedAccessKey=secret123"
        )
        parts = parse_connection_string(conn)
        assert parts["Endpoint"] == "sb://myhub.servicebus.windows.net/"
        assert parts["SharedAccessKeyName"] == "RootPolicy"
        assert parts["SharedAccessKey"] == "secret123"

    def test_empty_string_returns_empty_dict(self):
        assert parse_connection_string("") == {}

    def test_none_returns_empty_dict(self):
        assert parse_connection_string(None) == {}

    def test_handles_entity_path(self):
        conn = (
            "Endpoint=sb://myhub.servicebus.windows.net/;"
            "SharedAccessKeyName=Policy;"
            "SharedAccessKey=key;"
            "EntityPath=myentity"
        )
        parts = parse_connection_string(conn)
        assert parts["EntityPath"] == "myentity"

    def test_handles_value_with_equals_sign(self):
        conn = "SharedAccessKey=abc=def=ghi"
        parts = parse_connection_string(conn)
        assert parts["SharedAccessKey"] == "abc=def=ghi"


# =============================================================================
# extract_namespace_host
# =============================================================================


class TestExtractNamespaceHost:
    def test_extracts_hostname_from_standard_connection_string(self):
        conn = "Endpoint=sb://myhub.servicebus.windows.net/;SharedAccessKey=x"
        assert extract_namespace_host(conn) == "myhub.servicebus.windows.net"

    def test_returns_none_for_empty_string(self):
        assert extract_namespace_host("") is None

    def test_returns_none_when_no_endpoint(self):
        conn = "SharedAccessKey=abc"
        assert extract_namespace_host(conn) is None

    def test_returns_none_when_no_sb_scheme(self):
        conn = "Endpoint=https://example.com/"
        assert extract_namespace_host(conn) is None


# =============================================================================
# test_dns_resolution
# =============================================================================


class TestDnsResolution:
    @patch("pipeline.common.eventhub.diagnostics.socket.getaddrinfo")
    def test_successful_dns_resolution(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.1", 443)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.2", 443)),
        ]

        result = test_dns_resolution("myhub.servicebus.windows.net")

        assert result["resolved"] is True
        assert "10.0.0.1" in result["ip_addresses"]
        assert result["error"] is None

    @patch("pipeline.common.eventhub.diagnostics.socket.getaddrinfo")
    def test_dns_resolution_failure(self, mock_getaddrinfo):
        mock_getaddrinfo.side_effect = socket.gaierror("Name or service not known")

        result = test_dns_resolution("nonexistent.host.example.com")

        assert result["resolved"] is False
        assert result["ip_addresses"] == []
        assert "DNS resolution failed" in result["error"]

    @patch("pipeline.common.eventhub.diagnostics.socket.getaddrinfo")
    def test_dns_resolution_generic_exception(self, mock_getaddrinfo):
        mock_getaddrinfo.side_effect = RuntimeError("unexpected")

        result = test_dns_resolution("host.example.com")

        assert result["resolved"] is False
        assert "DNS resolution error" in result["error"]

    @patch("pipeline.common.eventhub.diagnostics.socket.getaddrinfo")
    def test_deduplicates_ip_addresses(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.1", 443)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.1", 443)),
            (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("10.0.0.1", 443, 0, 0)),
        ]

        result = test_dns_resolution("myhub.servicebus.windows.net")

        assert result["resolved"] is True
        assert len(result["ip_addresses"]) == 1


# =============================================================================
# log_connection_diagnostics
# =============================================================================


class TestLogConnectionDiagnostics:
    @patch("pipeline.common.eventhub.diagnostics.test_dns_resolution")
    @patch.dict(os.environ, {}, clear=False)
    def test_logs_connection_info(self, mock_dns, caplog):
        mock_dns.return_value = {
            "resolved": True,
            "ip_addresses": ["10.0.0.1"],
            "error": None,
        }

        # Remove SSL env vars so we hit the "system default" branch
        env_vars = ["SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE"]
        for v in env_vars:
            os.environ.pop(v, None)

        conn = "Endpoint=sb://myhub.servicebus.windows.net/;SharedAccessKeyName=P;SharedAccessKey=s"
        with caplog.at_level(logging.DEBUG):
            log_connection_diagnostics(conn, "my-entity")

        log_text = caplog.text
        assert "myhub.servicebus.windows.net" in log_text
        assert "my-entity" in log_text

    @patch("pipeline.common.eventhub.diagnostics.test_dns_resolution")
    @patch.dict(os.environ, {"SSL_CERT_FILE": "/etc/ssl/custom.pem"})
    def test_logs_custom_ca_bundle(self, mock_dns, caplog):
        mock_dns.return_value = {"resolved": True, "ip_addresses": ["10.0.0.1"], "error": None}

        conn = "Endpoint=sb://myhub.servicebus.windows.net/;SharedAccessKey=s"
        with caplog.at_level(logging.DEBUG):
            log_connection_diagnostics(conn, "entity")

        assert "/etc/ssl/custom.pem" in caplog.text

    @patch("pipeline.common.eventhub.diagnostics.test_dns_resolution")
    def test_handles_missing_hostname(self, mock_dns, caplog):
        with caplog.at_level(logging.DEBUG):
            log_connection_diagnostics("SharedAccessKey=abc", "entity")

        assert "Could not extract hostname" in caplog.text
        mock_dns.assert_not_called()


# =============================================================================
# log_connection_attempt_details
# =============================================================================


class TestLogConnectionAttemptDetails:
    def test_logs_basic_details(self, caplog):
        with caplog.at_level(logging.DEBUG):
            log_connection_attempt_details(
                eventhub_name="my-entity",
                transport_type="AmqpOverWebsocket",
                ssl_kwargs={},
            )

        assert "my-entity" in caplog.text
        assert "AmqpOverWebsocket" in caplog.text

    @patch("pipeline.common.eventhub.diagnostics.os.path.exists", return_value=True)
    @patch("pipeline.common.eventhub.diagnostics.os.path.getsize", return_value=1234)
    def test_logs_custom_ca_bundle_details(self, mock_size, mock_exists, caplog):
        with caplog.at_level(logging.DEBUG):
            log_connection_attempt_details(
                eventhub_name="entity",
                transport_type="AmqpOverWebsocket",
                ssl_kwargs={"connection_verify": "/etc/ssl/bundle.pem"},
            )

        assert "/etc/ssl/bundle.pem" in caplog.text
        assert "1234" in caplog.text

    @patch("pipeline.common.eventhub.diagnostics.os.path.exists", return_value=False)
    def test_logs_missing_ca_bundle(self, mock_exists, caplog):
        with caplog.at_level(logging.DEBUG):
            log_connection_attempt_details(
                eventhub_name="entity",
                transport_type="AmqpOverWebsocket",
                ssl_kwargs={"connection_verify": "/nonexistent/cert.pem"},
            )

        assert "NOT FOUND" in caplog.text

    def test_logs_system_default_when_no_ssl_kwargs(self, caplog):
        with caplog.at_level(logging.DEBUG):
            log_connection_attempt_details(
                eventhub_name="entity",
                transport_type="AmqpOverWebsocket",
                ssl_kwargs={},
            )

        assert "system default" in caplog.text
