"""
Tests for URL validation and SSRF prevention.
"""

import os
from unittest.mock import patch

import pytest

from core.security.url_validation import (
    BLOCKED_HOSTS,
    DEFAULT_ALLOWED_DOMAINS,
    get_allowed_domains,
    is_private_ip,
    validate_download_url,
)


class TestGetAllowedDomains:
    """Tests for get_allowed_domains() function."""

    def test_returns_default_when_no_env_var(self):
        with patch.dict(os.environ, {}, clear=True):
            domains = get_allowed_domains()
            assert domains == DEFAULT_ALLOWED_DOMAINS

    def test_parses_env_var_comma_separated(self):
        with patch.dict(
            os.environ, {"ALLOWED_ATTACHMENT_DOMAINS": "example.com,test.org"}
        ):
            domains = get_allowed_domains()
            assert domains == {"example.com", "test.org"}

    def test_strips_whitespace_from_domains(self):
        with patch.dict(
            os.environ, {"ALLOWED_ATTACHMENT_DOMAINS": " example.com , test.org "}
        ):
            domains = get_allowed_domains()
            assert domains == {"example.com", "test.org"}

    def test_converts_domains_to_lowercase(self):
        with patch.dict(
            os.environ, {"ALLOWED_ATTACHMENT_DOMAINS": "Example.COM,TEST.ORG"}
        ):
            domains = get_allowed_domains()
            assert domains == {"example.com", "test.org"}

    def test_ignores_empty_entries(self):
        with patch.dict(
            os.environ, {"ALLOWED_ATTACHMENT_DOMAINS": "example.com,,test.org,"}
        ):
            domains = get_allowed_domains()
            assert domains == {"example.com", "test.org"}


class TestValidateDownloadUrl:
    """Tests for validate_download_url() function."""

    # Valid URL tests
    def test_valid_https_url_with_default_domains(self):
        url = "https://claimxperience.com/path/to/file.pdf"
        is_valid, error = validate_download_url(url)
        assert is_valid is True
        assert error == ""

    def test_valid_https_url_with_custom_domains(self):
        url = "https://example.com/file.pdf"
        is_valid, error = validate_download_url(url, allowed_domains={"example.com"})
        assert is_valid is True
        assert error == ""

    def test_valid_url_with_query_params(self):
        url = "https://claimxperience.com/file.pdf?key=value&foo=bar"
        is_valid, error = validate_download_url(url)
        assert is_valid is True
        assert error == ""

    def test_valid_url_with_fragment(self):
        url = "https://claimxperience.com/file.pdf#section"
        is_valid, error = validate_download_url(url)
        assert is_valid is True
        assert error == ""

    # Empty/None URL tests
    def test_empty_url(self):
        is_valid, error = validate_download_url("")
        assert is_valid is False
        assert error == "Empty URL"

    # HTTP (non-HTTPS) tests
    def test_http_url_rejected(self):
        url = "http://claimxperience.com/file.pdf"
        is_valid, error = validate_download_url(url)
        assert is_valid is False
        assert "HTTPS" in error
        assert "http" in error

    def test_ftp_scheme_rejected(self):
        url = "ftp://claimxperience.com/file.pdf"
        is_valid, error = validate_download_url(url)
        assert is_valid is False
        assert "Invalid scheme" in error or "scheme" in error.lower()

    # Malformed URL tests
    def test_malformed_url(self):
        url = "not a valid url at all"
        is_valid, error = validate_download_url(url)
        assert is_valid is False
        # Should handle gracefully - either "Must be HTTPS" or "Invalid URL format"
        assert error != ""

    def test_url_without_hostname(self):
        url = "https:///path/to/file"
        is_valid, error = validate_download_url(url)
        assert is_valid is False
        assert "No hostname" in error

    # Domain allowlist tests
    def test_domain_not_in_allowlist(self):
        url = "https://malicious.example.com/file.pdf"
        is_valid, error = validate_download_url(url)
        assert is_valid is False
        assert "not in allowlist" in error
        assert "malicious.example.com" in error

    def test_subdomain_not_matching_parent(self):
        """Subdomain rejection: subdomains must be explicitly in allowlist."""
        url = "https://sub.claimxperience.com/file.pdf"
        # This should fail unless sub.claimxperience.com is in allowlist
        is_valid, error = validate_download_url(url)
        assert is_valid is False
        assert "not in allowlist" in error

    def test_case_insensitive_domain_matching(self):
        url = "https://ClaimXperience.COM/file.pdf"
        is_valid, error = validate_download_url(url)
        assert is_valid is True
        assert error == ""

    def test_custom_allowlist_overrides_default(self):
        url = "https://claimxperience.com/file.pdf"
        # This is in default list but not in custom list
        is_valid, error = validate_download_url(
            url, allowed_domains={"example.com"}
        )
        assert is_valid is False
        assert "not in allowlist" in error

    # Security edge cases
    def test_url_with_username_password(self):
        """Authentication in URL: still valid if domain matches allowlist."""
        url = "https://user:pass@claimxperience.com/file.pdf"
        is_valid, error = validate_download_url(url)
        assert is_valid is True  # Still valid if domain matches

    def test_url_with_port(self):
        url = "https://claimxperience.com:443/file.pdf"
        is_valid, error = validate_download_url(url)
        assert is_valid is True

    def test_url_with_non_standard_port(self):
        url = "https://claimxperience.com:8443/file.pdf"
        is_valid, error = validate_download_url(url)
        assert is_valid is True

    def test_unicode_in_domain(self):
        """IDN handling: ensures Unicode domains don't crash validation."""
        # This tests that we don't crash on Unicode domains
        url = "https://例え.jp/file.pdf"
        is_valid, error = validate_download_url(url)
        assert is_valid is False  # Not in allowlist
        assert error != ""  # Should provide error message

    def test_url_encoded_characters(self):
        url = "https://claimxperience.com/file%20name.pdf"
        is_valid, error = validate_download_url(url)
        assert is_valid is True

    def test_ip_address_not_in_allowlist(self):
        url = "https://192.168.1.1/file.pdf"
        is_valid, error = validate_download_url(url)
        assert is_valid is False
        assert "not in allowlist" in error

    def test_ip_address_in_custom_allowlist(self):
        """IP addresses can be explicitly allowed in custom allowlist."""
        url = "https://203.0.113.1/file.pdf"
        is_valid, error = validate_download_url(
            url, allowed_domains={"203.0.113.1"}
        )
        assert is_valid is True


class TestIsPrivateIp:
    """Tests for is_private_ip() function."""

    # Blocked hosts
    def test_localhost_string(self):
        """Localhost is intentionally allowed for local testing."""
        # Note: localhost is not in BLOCKED_HOSTS to allow local dummy source testing
        assert is_private_ip("localhost") is False

    def test_localhost_case_insensitive(self):
        """Localhost is intentionally allowed for local testing (case insensitive)."""
        # Note: localhost is not in BLOCKED_HOSTS to allow local dummy source testing
        assert is_private_ip("LOCALHOST") is False

    def test_127_0_0_1(self):
        assert is_private_ip("127.0.0.1") is True

    def test_0_0_0_0(self):
        assert is_private_ip("0.0.0.0") is True

    def test_metadata_google_internal(self):
        assert is_private_ip("metadata.google.internal") is True

    def test_metadata_aws_internal(self):
        assert is_private_ip("metadata.aws.internal") is True

    def test_metadata_ip_169_254_169_254(self):
        """Cloud metadata IP: 169.254.169.254 must be blocked for SSRF protection."""
        assert is_private_ip("169.254.169.254") is True

    # Private IP ranges
    def test_10_network(self):
        assert is_private_ip("10.0.0.1") is True
        assert is_private_ip("10.255.255.254") is True

    def test_172_16_network(self):
        assert is_private_ip("172.16.0.1") is True
        assert is_private_ip("172.31.255.254") is True

    def test_192_168_network(self):
        assert is_private_ip("192.168.0.1") is True
        assert is_private_ip("192.168.255.254") is True

    def test_169_254_link_local(self):
        assert is_private_ip("169.254.1.1") is True
        assert is_private_ip("169.254.254.254") is True

    def test_127_network_loopback(self):
        assert is_private_ip("127.0.0.1") is True
        assert is_private_ip("127.255.255.254") is True

    # Public IPs
    def test_public_ip(self):
        assert is_private_ip("8.8.8.8") is False
        assert is_private_ip("1.1.1.1") is False
        assert is_private_ip("203.0.113.1") is False

    # Hostnames (non-IP)
    def test_public_hostname(self):
        """No DNS resolution: public hostnames are not detected as private."""
        assert is_private_ip("example.com") is False
        assert is_private_ip("google.com") is False

    def test_private_hostname_not_in_blocklist(self):
        """Hostnames not in BLOCKED_HOSTS are not detected (no DNS resolution)."""
        # This hostname is not resolved to IP, so not detected
        assert is_private_ip("internal.example.com") is False

    # Edge cases
    def test_invalid_ip_format(self):
        assert is_private_ip("999.999.999.999") is False
        assert is_private_ip("not.an.ip.address") is False

    def test_ipv6_loopback(self):
        assert is_private_ip("::1") is True

    def test_ipv6_link_local(self):
        assert is_private_ip("fe80::1") is True
