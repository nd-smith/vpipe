"""
Tests for URL validation with simulation mode support.

Verifies that:
1. Localhost URLs are blocked in production (security requirement)
2. Localhost URLs are allowed in simulation mode when enabled
3. Path traversal and injection attacks are blocked for localhost
4. Production URL validation still works correctly
"""

import os
import pytest
from unittest.mock import patch

from core.security.url_validation import (
    validate_download_url,
    _is_production_environment,
    _validate_localhost_url,
    _validate_production_url,
)


class TestProductionEnvironmentDetection:
    """Test production environment detection."""

    def test_production_detected_via_environment(self):
        """Production should be detected via ENVIRONMENT variable."""
        with patch.dict(os.environ, {"ENVIRONMENT": "production"}):
            assert _is_production_environment() is True

        with patch.dict(os.environ, {"ENVIRONMENT": "prod"}):
            assert _is_production_environment() is True

        with patch.dict(os.environ, {"ENVIRONMENT": "live"}):
            assert _is_production_environment() is True

    def test_production_detected_via_deployment_env(self):
        """Production should be detected via DEPLOYMENT_ENV variable."""
        with patch.dict(os.environ, {"DEPLOYMENT_ENV": "production"}):
            assert _is_production_environment() is True

    def test_production_detected_via_app_env(self):
        """Production should be detected via APP_ENV variable."""
        with patch.dict(os.environ, {"APP_ENV": "production"}):
            assert _is_production_environment() is True

    def test_development_not_production(self):
        """Development/staging should not be detected as production."""
        with patch.dict(os.environ, {"ENVIRONMENT": "development"}, clear=True):
            assert _is_production_environment() is False

        with patch.dict(os.environ, {"ENVIRONMENT": "staging"}, clear=True):
            assert _is_production_environment() is False

        with patch.dict(os.environ, {"ENVIRONMENT": "test"}, clear=True):
            assert _is_production_environment() is False

    def test_no_environment_not_production(self):
        """Empty/missing environment should not be detected as production."""
        with patch.dict(os.environ, {}, clear=True):
            assert _is_production_environment() is False


class TestLocalhostValidation:
    """Test localhost URL validation logic."""

    def test_valid_localhost_http(self):
        """Valid localhost HTTP URL should pass."""
        from urllib.parse import urlparse
        url = "http://localhost:8765/files/test.jpg"
        parsed = urlparse(url)
        is_valid, error = _validate_localhost_url(url, parsed)
        assert is_valid is True
        assert error == ""

    def test_valid_localhost_ip(self):
        """Valid 127.0.0.1 URL should pass."""
        from urllib.parse import urlparse
        url = "http://127.0.0.1:8765/files/test.jpg"
        parsed = urlparse(url)
        is_valid, error = _validate_localhost_url(url, parsed)
        assert is_valid is True
        assert error == ""

    def test_valid_localhost_https(self):
        """HTTPS localhost URL should pass (flexible for testing)."""
        from urllib.parse import urlparse
        url = "https://localhost:8765/files/test.jpg"
        parsed = urlparse(url)
        is_valid, error = _validate_localhost_url(url, parsed)
        assert is_valid is True
        assert error == ""

    def test_path_traversal_blocked(self):
        """Path traversal attempts should be blocked."""
        from urllib.parse import urlparse
        url = "http://localhost:8765/files/../../../etc/passwd"
        parsed = urlparse(url)
        is_valid, error = _validate_localhost_url(url, parsed)
        assert is_valid is False
        assert "path traversal" in error.lower()

    def test_credential_injection_blocked(self):
        """Credential injection should be blocked."""
        from urllib.parse import urlparse
        url = "http://user:pass@localhost:8765/files/test.jpg"
        parsed = urlparse(url)
        is_valid, error = _validate_localhost_url(url, parsed)
        assert is_valid is False
        assert "credential injection" in error.lower()

    def test_suspicious_query_params_blocked(self):
        """Suspicious query parameters should be blocked."""
        from urllib.parse import urlparse
        test_cases = [
            "http://localhost:8765/test?redirect=file:///etc/passwd",
            "http://localhost:8765/test?url=gopher://evil.com",
            "http://localhost:8765/test?path=dict://attack",
        ]
        for url in test_cases:
            parsed = urlparse(url)
            is_valid, error = _validate_localhost_url(url, parsed)
            assert is_valid is False, f"Should block: {url}"
            assert "suspicious" in error.lower()

    def test_invalid_scheme_blocked(self):
        """Non-HTTP/HTTPS schemes should be blocked."""
        from urllib.parse import urlparse
        url = "ftp://localhost:8765/files/test.jpg"
        parsed = urlparse(url)
        is_valid, error = _validate_localhost_url(url, parsed)
        assert is_valid is False
        assert "scheme" in error.lower()


class TestProductionValidation:
    """Test production URL validation logic."""

    def test_valid_https_url(self):
        """Valid HTTPS production URL should pass."""
        from urllib.parse import urlparse
        url = "https://claimxperience.com/file.pdf"
        parsed = urlparse(url)
        is_valid, error = _validate_production_url(
            url, parsed, "claimxperience.com", "claimxperience.com", None
        )
        assert is_valid is True
        assert error == ""

    def test_http_blocked_in_production(self):
        """HTTP should be blocked in production validation."""
        from urllib.parse import urlparse
        url = "http://claimxperience.com/file.pdf"
        parsed = urlparse(url)
        is_valid, error = _validate_production_url(
            url, parsed, "claimxperience.com", "claimxperience.com", None
        )
        assert is_valid is False
        assert "https" in error.lower()

    def test_localhost_blocked_in_production(self):
        """Localhost should be blocked in production validation."""
        from urllib.parse import urlparse
        url = "https://localhost:8765/file.pdf"
        parsed = urlparse(url)
        is_valid, error = _validate_production_url(
            url, parsed, "localhost", "localhost", None
        )
        assert is_valid is False
        assert "blocked host" in error.lower()

    def test_private_ip_blocked(self):
        """Private IPs should be blocked in production validation."""
        from urllib.parse import urlparse
        url = "https://10.0.0.1/file.pdf"
        parsed = urlparse(url)
        is_valid, error = _validate_production_url(
            url, parsed, "10.0.0.1", "10.0.0.1", None
        )
        assert is_valid is False
        assert "private ip" in error.lower()

    def test_domain_not_in_allowlist(self):
        """Domains not in allowlist should be blocked."""
        from urllib.parse import urlparse
        url = "https://evil.com/file.pdf"
        parsed = urlparse(url)
        is_valid, error = _validate_production_url(
            url, parsed, "evil.com", "evil.com", None
        )
        assert is_valid is False
        assert "not in allowlist" in error.lower()


class TestValidateDownloadUrlIntegration:
    """Integration tests for validate_download_url function."""

    def test_production_url_allowed(self):
        """Production URLs should work in any environment."""
        with patch.dict(os.environ, {"ENVIRONMENT": "development"}, clear=True):
            is_valid, error = validate_download_url(
                "https://claimxperience.com/file.pdf",
                allow_localhost=False,
            )
            assert is_valid is True
            assert error == ""

    def test_localhost_blocked_by_default(self):
        """Localhost should be blocked when allow_localhost=False."""
        with patch.dict(os.environ, {"ENVIRONMENT": "development"}, clear=True):
            is_valid, error = validate_download_url(
                "http://localhost:8765/file.jpg",
                allow_localhost=False,
            )
            assert is_valid is False
            # Localhost blocked either by HTTPS requirement or allowlist
            assert "https" in error.lower() or "not in allowlist" in error.lower()

    def test_localhost_allowed_in_development(self):
        """Localhost should be allowed in development when allow_localhost=True."""
        with patch.dict(os.environ, {"ENVIRONMENT": "development"}, clear=True):
            is_valid, error = validate_download_url(
                "http://localhost:8765/files/test.jpg",
                allow_localhost=True,
            )
            assert is_valid is True, f"Should allow localhost in dev: {error}"
            assert error == ""

    def test_localhost_blocked_in_production_regardless_of_flag(self):
        """CRITICAL: Localhost must be blocked in production even if allow_localhost=True."""
        with patch.dict(os.environ, {"ENVIRONMENT": "production"}, clear=True):
            is_valid, error = validate_download_url(
                "http://localhost:8765/file.jpg",
                allow_localhost=True,  # Should be ignored in production
            )
            assert is_valid is False, "Localhost MUST be blocked in production"
            assert "production" in error.lower()
            assert "not allowed" in error.lower()

    def test_path_traversal_blocked_even_with_localhost(self):
        """Path traversal should be blocked even when localhost is allowed."""
        with patch.dict(os.environ, {"ENVIRONMENT": "development"}, clear=True):
            is_valid, error = validate_download_url(
                "http://localhost:8765/../../../etc/passwd",
                allow_localhost=True,
            )
            assert is_valid is False
            assert "traversal" in error.lower()

    def test_credential_injection_blocked_even_with_localhost(self):
        """Credential injection should be blocked even when localhost is allowed."""
        with patch.dict(os.environ, {"ENVIRONMENT": "development"}, clear=True):
            is_valid, error = validate_download_url(
                "http://user:pass@localhost:8765/file.jpg",
                allow_localhost=True,
            )
            assert is_valid is False
            assert "credential" in error.lower()

    def test_empty_url_rejected(self):
        """Empty URL should be rejected."""
        is_valid, error = validate_download_url("", allow_localhost=False)
        assert is_valid is False
        assert "empty" in error.lower()

    def test_malformed_url_rejected(self):
        """Malformed URL should be rejected."""
        is_valid, error = validate_download_url(
            "not-a-valid-url",
            allow_localhost=False,
        )
        assert is_valid is False


class TestSimulationModeScenarios:
    """Test realistic simulation mode usage scenarios."""

    def test_dummy_server_url_in_simulation_mode(self):
        """Dummy file server URLs should work in simulation mode."""
        with patch.dict(os.environ, {"ENVIRONMENT": "development"}, clear=True):
            # Typical dummy server URL from WP1
            is_valid, error = validate_download_url(
                "http://localhost:8765/files/proj_123/media_456/photo.jpg",
                allow_localhost=True,
            )
            assert is_valid is True, f"Simulation URL should be valid: {error}"
            assert error == ""

    def test_production_urls_still_work_in_simulation_mode(self):
        """Production URLs should still work when allow_localhost=True."""
        with patch.dict(os.environ, {"ENVIRONMENT": "development"}, clear=True):
            # S3 URL should still work
            is_valid, error = validate_download_url(
                "https://claimxperience.s3.amazonaws.com/file.pdf",
                allow_localhost=True,
            )
            assert is_valid is True
            assert error == ""

    def test_simulation_mode_disabled_blocks_localhost(self):
        """When simulation mode is disabled, localhost should be blocked."""
        with patch.dict(os.environ, {"ENVIRONMENT": "development"}, clear=True):
            is_valid, error = validate_download_url(
                "http://localhost:8765/file.jpg",
                allow_localhost=False,  # Simulation mode disabled
            )
            assert is_valid is False
            # Localhost blocked either by HTTPS requirement or allowlist
            assert "https" in error.lower() or "not in allowlist" in error.lower()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
