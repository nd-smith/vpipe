"""Tests for core.security.ssl_utils module."""

from core.security.ssl_utils import get_ca_bundle_kwargs


class TestGetCaBundleKwargs:
    def test_returns_empty_when_no_env_vars(self, monkeypatch):
        monkeypatch.delenv("SSL_CERT_FILE", raising=False)
        monkeypatch.delenv("REQUESTS_CA_BUNDLE", raising=False)
        monkeypatch.delenv("CURL_CA_BUNDLE", raising=False)
        assert get_ca_bundle_kwargs() == {}

    def test_ssl_cert_file(self, monkeypatch):
        monkeypatch.setenv("SSL_CERT_FILE", "/path/to/cert.pem")
        monkeypatch.delenv("REQUESTS_CA_BUNDLE", raising=False)
        monkeypatch.delenv("CURL_CA_BUNDLE", raising=False)
        assert get_ca_bundle_kwargs() == {"connection_verify": "/path/to/cert.pem"}

    def test_requests_ca_bundle_fallback(self, monkeypatch):
        monkeypatch.delenv("SSL_CERT_FILE", raising=False)
        monkeypatch.setenv("REQUESTS_CA_BUNDLE", "/path/to/bundle.pem")
        monkeypatch.delenv("CURL_CA_BUNDLE", raising=False)
        assert get_ca_bundle_kwargs() == {"connection_verify": "/path/to/bundle.pem"}

    def test_curl_ca_bundle_fallback(self, monkeypatch):
        monkeypatch.delenv("SSL_CERT_FILE", raising=False)
        monkeypatch.delenv("REQUESTS_CA_BUNDLE", raising=False)
        monkeypatch.setenv("CURL_CA_BUNDLE", "/path/to/curl.pem")
        assert get_ca_bundle_kwargs() == {"connection_verify": "/path/to/curl.pem"}

    def test_ssl_cert_file_takes_precedence(self, monkeypatch):
        monkeypatch.setenv("SSL_CERT_FILE", "/first.pem")
        monkeypatch.setenv("REQUESTS_CA_BUNDLE", "/second.pem")
        assert get_ca_bundle_kwargs() == {"connection_verify": "/first.pem"}
