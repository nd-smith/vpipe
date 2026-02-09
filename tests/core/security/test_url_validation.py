import os
from unittest.mock import patch

import pytest

from core.security.exceptions import URLValidationError
from core.security.url_validation import (
    BLOCKED_HOSTS,
    DEFAULT_ALLOWED_DOMAINS,
    extract_filename_from_url,
    get_allowed_domains,
    is_private_ip,
    sanitize_error_message,
    sanitize_url,
    validate_download_url,
)


# =========================================================================
# get_allowed_domains
# =========================================================================


class TestGetAllowedDomains:

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

    def test_returns_default_when_env_var_is_empty_string(self):
        with patch.dict(os.environ, {"ALLOWED_ATTACHMENT_DOMAINS": ""}):
            domains = get_allowed_domains()
            assert domains == DEFAULT_ALLOWED_DOMAINS

    def test_returns_default_when_env_var_is_only_whitespace(self):
        with patch.dict(os.environ, {"ALLOWED_ATTACHMENT_DOMAINS": "   "}):
            # All entries are whitespace-only and get stripped to empty, filtered out
            # Result is empty set from env, but the code checks truthiness of env_domains
            # "   " is truthy, so it goes through the split path
            domains = get_allowed_domains()
            assert domains == set()


# =========================================================================
# validate_download_url - production (non-localhost) paths
# =========================================================================


class TestValidateDownloadUrl:

    def test_accepts_valid_https_url_with_default_domains(self):
        url = "https://claimxperience.com/path/to/file.pdf"
        validate_download_url(url)

    def test_accepts_valid_https_url_with_custom_domains(self):
        url = "https://example.com/file.pdf"
        validate_download_url(url, allowed_domains={"example.com"})

    def test_accepts_url_with_query_params(self):
        url = "https://claimxperience.com/file.pdf?key=value&foo=bar"
        validate_download_url(url)

    def test_accepts_url_with_fragment(self):
        url = "https://claimxperience.com/file.pdf#section"
        validate_download_url(url)

    def test_rejects_empty_url(self):
        with pytest.raises(URLValidationError, match="Empty URL"):
            validate_download_url("")

    def test_rejects_http_url(self):
        url = "http://claimxperience.com/file.pdf"
        with pytest.raises(URLValidationError, match="HTTPS"):
            validate_download_url(url)

    def test_rejects_ftp_scheme(self):
        url = "ftp://claimxperience.com/file.pdf"
        with pytest.raises(URLValidationError, match="scheme"):
            validate_download_url(url)

    def test_rejects_url_without_hostname(self):
        url = "https:///path/to/file"
        with pytest.raises(URLValidationError, match="No hostname"):
            validate_download_url(url)

    def test_rejects_domain_not_in_allowlist(self):
        url = "https://malicious.example.com/file.pdf"
        with pytest.raises(URLValidationError, match="not in allowlist"):
            validate_download_url(url)

    def test_rejects_subdomain_not_explicitly_allowed(self):
        url = "https://sub.claimxperience.com/file.pdf"
        with pytest.raises(URLValidationError, match="not in allowlist"):
            validate_download_url(url)

    def test_case_insensitive_domain_matching(self):
        url = "https://ClaimXperience.COM/file.pdf"
        validate_download_url(url)

    def test_custom_allowlist_overrides_default(self):
        url = "https://claimxperience.com/file.pdf"
        with pytest.raises(URLValidationError, match="not in allowlist"):
            validate_download_url(url, allowed_domains={"example.com"})

    def test_accepts_url_with_credentials_if_domain_matches(self):
        url = "https://user:pass@claimxperience.com/file.pdf"
        validate_download_url(url)

    def test_accepts_url_with_standard_port(self):
        url = "https://claimxperience.com:443/file.pdf"
        validate_download_url(url)

    def test_accepts_url_with_nonstandard_port(self):
        url = "https://claimxperience.com:8443/file.pdf"
        validate_download_url(url)

    def test_rejects_unicode_domain_not_in_allowlist(self):
        url = "https://\u4f8b\u3048.jp/file.pdf"
        with pytest.raises(URLValidationError):
            validate_download_url(url)

    def test_accepts_url_encoded_characters(self):
        url = "https://claimxperience.com/file%20name.pdf"
        validate_download_url(url)

    def test_rejects_private_ip(self):
        url = "https://192.168.1.1/file.pdf"
        with pytest.raises(URLValidationError, match="Private IP"):
            validate_download_url(url)

    def test_accepts_public_ip_in_custom_allowlist(self):
        url = "https://203.0.113.1/file.pdf"
        validate_download_url(url, allowed_domains={"203.0.113.1"})

    def test_rejects_localhost(self):
        url = "https://localhost/file.pdf"
        with pytest.raises(URLValidationError, match="Blocked host"):
            validate_download_url(url)

    def test_rejects_127_0_0_1(self):
        url = "https://127.0.0.1/file.pdf"
        with pytest.raises(URLValidationError, match="Blocked host"):
            validate_download_url(url)

    def test_rejects_metadata_endpoint(self):
        url = "https://169.254.169.254/latest/meta-data/"
        with pytest.raises(URLValidationError, match="Blocked host"):
            validate_download_url(url)

    def test_rejects_10_network_ip(self):
        url = "https://10.0.0.1/file.pdf"
        with pytest.raises(URLValidationError, match="Private IP"):
            validate_download_url(url)

    def test_custom_allowlist_lowercased(self):
        url = "https://example.com/file.pdf"
        validate_download_url(url, allowed_domains={"EXAMPLE.COM"})

    def test_all_default_domains_accepted(self):
        for domain in DEFAULT_ALLOWED_DOMAINS:
            url = f"https://{domain}/file.pdf"
            validate_download_url(url)


# =========================================================================
# validate_download_url - localhost / simulation mode
# =========================================================================


class TestValidateDownloadUrlLocalhost:

    @patch.dict(os.environ, {}, clear=True)
    def test_accepts_localhost_when_allow_localhost_true(self):
        url = "http://localhost:8080/api/file"
        validate_download_url(url, allow_localhost=True)

    @patch.dict(os.environ, {}, clear=True)
    def test_accepts_127_0_0_1_when_allow_localhost_true(self):
        url = "http://127.0.0.1:5000/data"
        validate_download_url(url, allow_localhost=True)

    @patch.dict(os.environ, {}, clear=True)
    def test_accepts_simulation_internal_hostname(self):
        url = "http://myhost-simulation:8080/file"
        validate_download_url(url, allow_localhost=True)

    @patch.dict(os.environ, {}, clear=True)
    def test_accepts_pcesdopodappv1_hostname(self):
        url = "http://pcesdopodappv1_worker:9000/file"
        validate_download_url(url, allow_localhost=True)

    @patch.dict(os.environ, {}, clear=True)
    def test_rejects_path_traversal_on_localhost(self):
        url = "http://localhost:8080/../../etc/passwd"
        with pytest.raises(URLValidationError, match="Path traversal"):
            validate_download_url(url, allow_localhost=True)

    @patch.dict(os.environ, {}, clear=True)
    def test_rejects_credential_injection_on_localhost(self):
        url = "http://user:PLACEHOLDER@localhost:8080/file"
        with pytest.raises(URLValidationError, match="Credential injection"):
            validate_download_url(url, allow_localhost=True)

    @patch.dict(os.environ, {}, clear=True)
    def test_rejects_suspicious_query_params_on_localhost(self):
        url = "http://localhost:8080/file?redirect=file://etc/passwd"
        with pytest.raises(URLValidationError, match="Suspicious query parameter"):
            validate_download_url(url, allow_localhost=True)

    @patch.dict(os.environ, {}, clear=True)
    def test_rejects_gopher_in_query_on_localhost(self):
        url = "http://localhost:8080/file?url=gopher://evil"
        with pytest.raises(URLValidationError, match="Suspicious query parameter"):
            validate_download_url(url, allow_localhost=True)

    @patch.dict(os.environ, {}, clear=True)
    def test_rejects_invalid_scheme_on_localhost(self):
        url = "ftp://localhost:8080/file"
        with pytest.raises(URLValidationError, match="http or https"):
            validate_download_url(url, allow_localhost=True)

    @patch.dict(os.environ, {"ENVIRONMENT": "production"}, clear=True)
    def test_blocks_localhost_in_production_environment(self):
        url = "http://localhost:8080/file"
        with pytest.raises(URLValidationError, match="not allowed in production"):
            validate_download_url(url, allow_localhost=True)

    @patch.dict(os.environ, {"DEPLOYMENT_ENV": "prod-west"}, clear=True)
    def test_blocks_localhost_in_prod_deployment_env(self):
        url = "http://localhost:8080/file"
        with pytest.raises(URLValidationError, match="not allowed in production"):
            validate_download_url(url, allow_localhost=True)

    @patch.dict(os.environ, {"APP_ENV": "live-us"}, clear=True)
    def test_blocks_localhost_in_live_app_env(self):
        url = "http://localhost:8080/file"
        with pytest.raises(URLValidationError, match="not allowed in production"):
            validate_download_url(url, allow_localhost=True)

    @patch.dict(os.environ, {"ENVIRONMENT": "staging"}, clear=True)
    def test_allows_localhost_in_non_production_environment(self):
        url = "http://localhost:8080/file"
        validate_download_url(url, allow_localhost=True)

    @patch.dict(os.environ, {}, clear=True)
    def test_accepts_https_on_localhost(self):
        url = "https://localhost:8443/file"
        validate_download_url(url, allow_localhost=True)


# =========================================================================
# _is_production_environment
# =========================================================================


class TestIsProductionEnvironment:

    @patch.dict(os.environ, {}, clear=True)
    def test_returns_false_with_no_env_vars(self):
        from core.security.url_validation import _is_production_environment

        assert _is_production_environment() is False

    @patch.dict(os.environ, {"ENVIRONMENT": "production"}, clear=True)
    def test_detects_environment_production(self):
        from core.security.url_validation import _is_production_environment

        assert _is_production_environment() is True

    @patch.dict(os.environ, {"ENVIRONMENT": "prod"}, clear=True)
    def test_detects_environment_prod(self):
        from core.security.url_validation import _is_production_environment

        assert _is_production_environment() is True

    @patch.dict(os.environ, {"DEPLOYMENT_ENV": "live"}, clear=True)
    def test_detects_deployment_env_live(self):
        from core.security.url_validation import _is_production_environment

        assert _is_production_environment() is True

    @patch.dict(os.environ, {"APP_ENV": "Production"}, clear=True)
    def test_detects_app_env_production_case_insensitive(self):
        from core.security.url_validation import _is_production_environment

        assert _is_production_environment() is True

    @patch.dict(os.environ, {"ENVIRONMENT": "staging"}, clear=True)
    def test_returns_false_for_staging(self):
        from core.security.url_validation import _is_production_environment

        assert _is_production_environment() is False

    @patch.dict(os.environ, {"ENVIRONMENT": "development"}, clear=True)
    def test_returns_false_for_development(self):
        from core.security.url_validation import _is_production_environment

        assert _is_production_environment() is False


# =========================================================================
# is_private_ip
# =========================================================================


class TestIsPrivateIp:

    def test_localhost_string(self):
        assert is_private_ip("localhost") is True

    def test_localhost_case_insensitive(self):
        assert is_private_ip("LOCALHOST") is True

    def test_127_0_0_1(self):
        assert is_private_ip("127.0.0.1") is True

    def test_0_0_0_0(self):
        assert is_private_ip("0.0.0.0") is True

    def test_metadata_google_internal(self):
        assert is_private_ip("metadata.google.internal") is True

    def test_metadata_aws_internal(self):
        assert is_private_ip("metadata.aws.internal") is True

    def test_metadata_ip_169_254_169_254(self):
        assert is_private_ip("169.254.169.254") is True

    def test_10_network(self):
        assert is_private_ip("10.0.0.1") is True
        assert is_private_ip("10.255.255.254") is True

    def test_172_16_network(self):
        assert is_private_ip("172.16.0.1") is True
        assert is_private_ip("172.31.255.254") is True

    def test_172_15_is_public(self):
        assert is_private_ip("172.15.0.1") is False

    def test_172_32_is_public(self):
        assert is_private_ip("172.32.0.1") is False

    def test_192_168_network(self):
        assert is_private_ip("192.168.0.1") is True
        assert is_private_ip("192.168.255.254") is True

    def test_169_254_link_local(self):
        assert is_private_ip("169.254.1.1") is True
        assert is_private_ip("169.254.254.254") is True

    def test_127_network_loopback(self):
        assert is_private_ip("127.0.0.1") is True
        assert is_private_ip("127.255.255.254") is True

    def test_public_ip(self):
        assert is_private_ip("8.8.8.8") is False
        assert is_private_ip("1.1.1.1") is False
        assert is_private_ip("203.0.113.1") is False

    def test_public_hostname(self):
        assert is_private_ip("example.com") is False
        assert is_private_ip("google.com") is False

    def test_private_hostname_not_in_blocklist(self):
        assert is_private_ip("internal.example.com") is False

    def test_invalid_ip_format(self):
        assert is_private_ip("999.999.999.999") is False
        assert is_private_ip("not.an.ip.address") is False

    def test_ipv6_loopback(self):
        assert is_private_ip("::1") is True

    def test_ipv6_link_local(self):
        assert is_private_ip("fe80::1") is True

    def test_ipv6_unique_local(self):
        assert is_private_ip("fc00::1") is True
        assert is_private_ip("fd00::1") is True

    def test_ipv6_public(self):
        assert is_private_ip("2001:db8::1") is False


# =========================================================================
# extract_filename_from_url
# =========================================================================


class TestExtractFilenameFromUrl:

    def test_extracts_filename_and_extension(self):
        filename, file_type = extract_filename_from_url(
            "https://example.com/path/to/document.pdf"
        )
        assert filename == "document"
        assert file_type == "PDF"

    def test_extracts_jpeg_extension(self):
        filename, file_type = extract_filename_from_url(
            "https://example.com/photo.jpeg"
        )
        assert filename == "photo"
        assert file_type == "JPEG"

    def test_handles_multiple_dots_in_filename(self):
        filename, file_type = extract_filename_from_url(
            "https://example.com/my.file.name.txt"
        )
        assert filename == "my.file.name"
        assert file_type == "TXT"

    def test_handles_no_extension(self):
        filename, file_type = extract_filename_from_url(
            "https://example.com/noextension"
        )
        assert filename == "noextension"
        assert file_type == "UNKNOWN"

    def test_handles_empty_path(self):
        filename, file_type = extract_filename_from_url("https://example.com/")
        assert filename == "unknown"
        assert file_type == "UNKNOWN"

    def test_handles_query_params_in_url(self):
        filename, file_type = extract_filename_from_url(
            "https://example.com/file.pdf?sig=abc&token=123"
        )
        assert filename == "file"
        assert file_type == "PDF"

    def test_returns_unknown_for_empty_string(self):
        filename, file_type = extract_filename_from_url("")
        assert filename == "unknown"
        assert file_type == "UNKNOWN"

    def test_returns_unknown_for_unparseable_url(self):
        # urlparse handles most strings without raising, so this tests the fallback
        filename, file_type = extract_filename_from_url("://")
        assert isinstance(filename, str)
        assert isinstance(file_type, str)


# =========================================================================
# sanitize_url
# =========================================================================


class TestSanitizeUrl:

    def test_returns_empty_string_for_empty_input(self):
        assert sanitize_url("") == ""

    def test_returns_url_unchanged_when_no_query(self):
        url = "https://example.com/path"
        assert sanitize_url(url) == url

    def test_redacts_sig_parameter(self):
        url = "https://example.com/file?sig=secret123&name=test"
        result = sanitize_url(url)
        assert "sig=[REDACTED]" in result
        assert "secret123" not in result
        assert "name=test" in result

    def test_redacts_token_parameter(self):
        url = "https://example.com/file?token=abc123"
        result = sanitize_url(url)
        assert "token=[REDACTED]" in result
        assert "abc123" not in result

    def test_redacts_access_token_parameter(self):
        url = "https://example.com/file?access_token=xyz"
        result = sanitize_url(url)
        assert "access_token=[REDACTED]" in result

    def test_redacts_api_key_parameter(self):
        url = "https://example.com/file?api_key=mykey"
        result = sanitize_url(url)
        assert "api_key=[REDACTED]" in result

    def test_redacts_password_parameter(self):
        url = "https://example.com/file?password=secret"
        result = sanitize_url(url)
        assert "password=[REDACTED]" in result

    def test_redacts_aws_signature(self):
        url = "https://bucket.s3.amazonaws.com/file?x-amz-signature=sig123&x-amz-credential=cred456"
        result = sanitize_url(url)
        assert "x-amz-signature=[REDACTED]" in result
        assert "x-amz-credential=[REDACTED]" in result
        assert "sig123" not in result
        assert "cred456" not in result

    def test_preserves_non_sensitive_params(self):
        url = "https://example.com/file?page=1&sort=name"
        assert sanitize_url(url) == url

    def test_handles_param_without_value(self):
        url = "https://example.com/file?flagonly&name=test"
        result = sanitize_url(url)
        assert "flagonly" in result
        assert "name=test" in result

    def test_redacts_azure_sas_params(self):
        url = "https://storage.blob.core.windows.net/file?sv=2020&se=2025&sp=r&sig=abc"
        result = sanitize_url(url)
        assert "sv=[REDACTED]" in result
        assert "se=[REDACTED]" in result
        assert "sig=[REDACTED]" in result

    def test_case_insensitive_redaction(self):
        url = "https://example.com/file?Token=abc&SIG=def"
        result = sanitize_url(url)
        assert "abc" not in result
        assert "def" not in result


# =========================================================================
# sanitize_error_message
# =========================================================================


class TestSanitizeErrorMessage:

    def test_returns_empty_for_empty_input(self):
        assert sanitize_error_message("") == ""

    def test_redacts_sig_in_message(self):
        msg = "Failed to download: sig=abc123def"
        result = sanitize_error_message(msg)
        assert "sig=[REDACTED]" in result
        assert "abc123def" not in result

    def test_redacts_token_in_message(self):
        msg = "Error: token=mytoken123"
        result = sanitize_error_message(msg)
        assert "token=[REDACTED]" in result

    def test_redacts_bearer_token(self):
        msg = "Auth failed: bearer eyJhbGciOiJSUzI1NiJ9.payload.sig"
        result = sanitize_error_message(msg)
        assert "bearer [REDACTED]" in result

    def test_redacts_api_key_in_message(self):
        msg = "Request error: api_key=12345"
        result = sanitize_error_message(msg)
        assert "api_key=[REDACTED]" in result

    def test_redacts_aws_signature_in_message(self):
        msg = "Fetch failed: x-amz-signature=abcdef"
        result = sanitize_error_message(msg)
        assert "x-amz-signature=[REDACTED]" in result

    def test_truncates_long_message(self):
        msg = "x" * 600
        result = sanitize_error_message(msg, max_length=500)
        assert len(result) == 500
        assert result.endswith("...")

    def test_does_not_truncate_short_message(self):
        msg = "Short error message"
        result = sanitize_error_message(msg, max_length=500)
        assert result == msg

    def test_sanitizes_embedded_url(self):
        msg = "Download failed: https://example.com/file?sig=secret123&name=test"
        result = sanitize_error_message(msg)
        assert "secret123" not in result

    def test_custom_max_length(self):
        msg = "a" * 100
        result = sanitize_error_message(msg, max_length=50)
        assert len(result) == 50
        assert result.endswith("...")

    def test_redacts_password_in_message(self):
        msg = "Connection string: password=DUMMY_VALUE"
        result = sanitize_error_message(msg)
        assert "password=[REDACTED]" in result
        assert "DUMMY_VALUE" not in result

    def test_redacts_secret_in_message(self):
        msg = "Config error: secret=myvalue"
        result = sanitize_error_message(msg)
        assert "secret=[REDACTED]" in result
