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
        """Should return DEFAULT_ALLOWED_DOMAINS when env var not set."""
        with patch.dict(os.environ, {}, clear=True):
            domains = get_allowed_domains()
            assert domains == DEFAULT_ALLOWED_DOMAINS

    def test_parses_env_var_comma_separated(self):
        """Should parse comma-separated domains from environment."""
        with patch.dict(
            os.environ, {"ALLOWED_ATTACHMENT_DOMAINS": "example.com,test.org"}
        ):
            domains = get_allowed_domains()
            assert domains == {"example.com", "test.org"}

    def test_strips_whitespace_from_domains(self):
        """Should strip whitespace from domain names."""
        with patch.dict(
            os.environ, {"ALLOWED_ATTACHMENT_DOMAINS": " example.com , test.org "}
        ):
            domains = get_allowed_domains()
            assert domains == {"example.com", "test.org"}

    def test_converts_domains_to_lowercase(self):
        """Should convert domains to lowercase."""
        with patch.dict(
            os.environ, {"ALLOWED_ATTACHMENT_DOMAINS": "Example.COM,TEST.ORG"}
        ):
            domains = get_allowed_domains()
            assert domains == {"example.com", "test.org"}

    def test_ignores_empty_entries(self):
        """Should ignore empty entries in comma-separated list."""
        with patch.dict(
            os.environ, {"ALLOWED_ATTACHMENT_DOMAINS": "example.com,,test.org,"}
        ):
            domains = get_allowed_domains()
            assert domains == {"example.com", "test.org"}


class TestValidateDownloadUrl:
    """Tests for validate_download_url() function."""

    # Valid URL tests
    def test_valid_https_url_with_default_domains(self):
        """Should accept valid HTTPS URL with domain in default allowlist."""
        url = "https://claimxperience.com/path/to/file.pdf"
        is_valid, error = validate_download_url(url)
        assert is_valid is True
        assert error == ""

    def test_valid_https_url_with_custom_domains(self):
        """Should accept valid HTTPS URL with domain in custom allowlist."""
        url = "https://example.com/file.pdf"
        is_valid, error = validate_download_url(url, allowed_domains={"example.com"})
        assert is_valid is True
        assert error == ""

    def test_valid_url_with_query_params(self):
        """Should accept valid URL with query parameters."""
        url = "https://claimxperience.com/file.pdf?key=value&foo=bar"
        is_valid, error = validate_download_url(url)
        assert is_valid is True
        assert error == ""

    def test_valid_url_with_fragment(self):
        """Should accept valid URL with fragment."""
        url = "https://claimxperience.com/file.pdf#section"
        is_valid, error = validate_download_url(url)
        assert is_valid is True
        assert error == ""

    # Empty/None URL tests
    def test_empty_url(self):
        """Should reject empty URL."""
        is_valid, error = validate_download_url("")
        assert is_valid is False
        assert error == "Empty URL"

    # HTTP (non-HTTPS) tests
    def test_http_url_rejected(self):
        """Should reject HTTP URLs (require HTTPS)."""
        url = "http://claimxperience.com/file.pdf"
        is_valid, error = validate_download_url(url)
        assert is_valid is False
        assert "Must be HTTPS" in error
        assert "http" in error

    def test_ftp_scheme_rejected(self):
        """Should reject non-HTTPS schemes."""
        url = "ftp://claimxperience.com/file.pdf"
        is_valid, error = validate_download_url(url)
        assert is_valid is False
        assert "Must be HTTPS" in error

    # Malformed URL tests
    def test_malformed_url(self):
        """Should reject malformed URLs."""
        url = "not a valid url at all"
        is_valid, error = validate_download_url(url)
        assert is_valid is False
        # Should handle gracefully - either "Must be HTTPS" or "Invalid URL format"
        assert error != ""

    def test_url_without_hostname(self):
        """Should reject URL without hostname."""
        url = "https:///path/to/file"
        is_valid, error = validate_download_url(url)
        assert is_valid is False
        assert "No hostname" in error

    # Domain allowlist tests
    def test_domain_not_in_allowlist(self):
        """Should reject domain not in allowlist."""
        url = "https://malicious.example.com/file.pdf"
        is_valid, error = validate_download_url(url)
        assert is_valid is False
        assert "not in allowlist" in error
        assert "malicious.example.com" in error

    def test_subdomain_not_matching_parent(self):
        """Should reject subdomain if not explicitly in allowlist."""
        url = "https://sub.claimxperience.com/file.pdf"
        # This should fail unless sub.claimxperience.com is in allowlist
        is_valid, error = validate_download_url(url)
        assert is_valid is False
        assert "not in allowlist" in error

    def test_case_insensitive_domain_matching(self):
        """Should match domains case-insensitively."""
        url = "https://ClaimXperience.COM/file.pdf"
        is_valid, error = validate_download_url(url)
        assert is_valid is True
        assert error == ""

    def test_custom_allowlist_overrides_default(self):
        """Should use custom allowlist instead of default."""
        url = "https://claimxperience.com/file.pdf"
        # This is in default list but not in custom list
        is_valid, error = validate_download_url(
            url, allowed_domains={"example.com"}
        )
        assert is_valid is False
        assert "not in allowlist" in error

    # Security edge cases
    def test_url_with_username_password(self):
        """Should handle URL with username/password."""
        url = "https://user:pass@claimxperience.com/file.pdf"
        is_valid, error = validate_download_url(url)
        assert is_valid is True  # Still valid if domain matches

    def test_url_with_port(self):
        """Should handle URL with explicit port."""
        url = "https://claimxperience.com:443/file.pdf"
        is_valid, error = validate_download_url(url)
        assert is_valid is True

    def test_url_with_non_standard_port(self):
        """Should accept URL with non-standard port if domain matches."""
        url = "https://claimxperience.com:8443/file.pdf"
        is_valid, error = validate_download_url(url)
        assert is_valid is True

    def test_unicode_in_domain(self):
        """Should handle Unicode in domain (IDN)."""
        # This tests that we don't crash on Unicode domains
        url = "https://例え.jp/file.pdf"
        is_valid, error = validate_download_url(url)
        assert is_valid is False  # Not in allowlist
        assert error != ""  # Should provide error message

    def test_url_encoded_characters(self):
        """Should handle URL-encoded characters in path."""
        url = "https://claimxperience.com/file%20name.pdf"
        is_valid, error = validate_download_url(url)
        assert is_valid is True

    def test_ip_address_not_in_allowlist(self):
        """Should reject IP addresses not in domain allowlist."""
        url = "https://192.168.1.1/file.pdf"
        is_valid, error = validate_download_url(url)
        assert is_valid is False
        assert "not in allowlist" in error

    def test_ip_address_in_custom_allowlist(self):
        """Should accept IP address if explicitly in allowlist."""
        url = "https://203.0.113.1/file.pdf"
        is_valid, error = validate_download_url(
            url, allowed_domains={"203.0.113.1"}
        )
        assert is_valid is True


class TestIsPrivateIp:
    """Tests for is_private_ip() function."""

    # Blocked hosts
    def test_localhost_string(self):
        """Should detect 'localhost' as private."""
        assert is_private_ip("localhost") is True

    def test_localhost_case_insensitive(self):
        """Should detect 'LOCALHOST' as private."""
        assert is_private_ip("LOCALHOST") is True

    def test_127_0_0_1(self):
        """Should detect 127.0.0.1 as private."""
        assert is_private_ip("127.0.0.1") is True

    def test_0_0_0_0(self):
        """Should detect 0.0.0.0 as private."""
        assert is_private_ip("0.0.0.0") is True

    def test_metadata_google_internal(self):
        """Should detect metadata.google.internal as blocked."""
        assert is_private_ip("metadata.google.internal") is True

    def test_metadata_aws_internal(self):
        """Should detect metadata.aws.internal as blocked."""
        assert is_private_ip("metadata.aws.internal") is True

    def test_metadata_ip_169_254_169_254(self):
        """Should detect 169.254.169.254 as blocked."""
        assert is_private_ip("169.254.169.254") is True

    # Private IP ranges
    def test_10_network(self):
        """Should detect 10.x.x.x as private."""
        assert is_private_ip("10.0.0.1") is True
        assert is_private_ip("10.255.255.254") is True

    def test_172_16_network(self):
        """Should detect 172.16-31.x.x as private."""
        assert is_private_ip("172.16.0.1") is True
        assert is_private_ip("172.31.255.254") is True

    def test_192_168_network(self):
        """Should detect 192.168.x.x as private."""
        assert is_private_ip("192.168.0.1") is True
        assert is_private_ip("192.168.255.254") is True

    def test_169_254_link_local(self):
        """Should detect 169.254.x.x as private (link-local)."""
        assert is_private_ip("169.254.1.1") is True
        assert is_private_ip("169.254.254.254") is True

    def test_127_network_loopback(self):
        """Should detect 127.x.x.x as private (loopback)."""
        assert is_private_ip("127.0.0.1") is True
        assert is_private_ip("127.255.255.254") is True

    # Public IPs
    def test_public_ip(self):
        """Should detect public IPs as not private."""
        assert is_private_ip("8.8.8.8") is False
        assert is_private_ip("1.1.1.1") is False
        assert is_private_ip("203.0.113.1") is False

    # Hostnames (non-IP)
    def test_public_hostname(self):
        """Should not detect public hostname as private (no DNS resolution)."""
        assert is_private_ip("example.com") is False
        assert is_private_ip("google.com") is False

    def test_private_hostname_not_in_blocklist(self):
        """Should not detect private hostname if not in BLOCKED_HOSTS."""
        # This hostname is not resolved to IP, so not detected
        assert is_private_ip("internal.example.com") is False

    # Edge cases
    def test_invalid_ip_format(self):
        """Should handle invalid IP format gracefully."""
        assert is_private_ip("999.999.999.999") is False
        assert is_private_ip("not.an.ip.address") is False

    def test_ipv6_loopback(self):
        """Should detect IPv6 loopback as private."""
        assert is_private_ip("::1") is True

    def test_ipv6_link_local(self):
        """Should detect IPv6 link-local as private."""
        assert is_private_ip("fe80::1") is True
