"""Tests for OAuth2 data models."""

from datetime import UTC, datetime, timedelta

import pytest

from core.oauth2.models import OAuth2Config, OAuth2Token


class TestOAuth2Token:
    """Tests for OAuth2Token model."""

    @pytest.fixture
    def sample_response(self):
        """Sample OAuth2 token response."""
        return {
            "access_token": "test-access-token",
            "token_type": "Bearer",
            "expires_in": 3600,
            "scope": "read write",
        }

    def test_from_response_basic(self, sample_response):
        """Should create token from response dict."""
        token = OAuth2Token.from_response(sample_response)

        assert token.access_token == sample_response["access_token"]
        assert token.token_type == "Bearer"
        assert token.scope == "read write"
        assert isinstance(token.expires_at, datetime)

    def test_from_response_expires_in(self, sample_response):
        """Should calculate expires_at from expires_in."""
        before = datetime.now(UTC)
        token = OAuth2Token.from_response(sample_response)
        after = datetime.now(UTC)

        expected_expiry = before + timedelta(seconds=3600)
        assert token.expires_at >= expected_expiry
        assert token.expires_at <= after + timedelta(seconds=3600)

    def test_from_response_default_expires_in(self):
        """Should use default expires_in if not provided."""
        response = {"access_token": "token123"}
        token = OAuth2Token.from_response(response)

        # Default is 3600 seconds
        expected_expiry = datetime.now(UTC) + timedelta(seconds=3600)
        assert abs((token.expires_at - expected_expiry).total_seconds()) < 2

    def test_from_response_with_refresh_token(self, sample_response):
        """Should include refresh token if provided."""
        sample_response["refresh_token"] = "test-refresh-tok"
        token = OAuth2Token.from_response(sample_response)

        assert token.refresh_token == "test-refresh-tok"

    def test_is_expired_fresh_token(self):
        """Fresh token should not be expired."""
        expires_at = datetime.now(UTC) + timedelta(hours=1)
        token = OAuth2Token(access_token="token", token_type="Bearer", expires_at=expires_at)

        assert not token.is_expired()

    def test_is_expired_with_buffer(self):
        """Token within buffer period should be considered expired."""
        # Expires in 4 minutes, buffer is 5 minutes
        expires_at = datetime.now(UTC) + timedelta(minutes=4)
        token = OAuth2Token(access_token="token", token_type="Bearer", expires_at=expires_at)

        assert token.is_expired(buffer_seconds=300)

    def test_is_expired_past_expiry(self):
        """Expired token should be expired."""
        expires_at = datetime.now(UTC) - timedelta(minutes=1)
        token = OAuth2Token(access_token="token", token_type="Bearer", expires_at=expires_at)

        assert token.is_expired()

    def test_remaining_lifetime_fresh_token(self):
        """Should calculate remaining lifetime correctly."""
        expires_at = datetime.now(UTC) + timedelta(hours=1)
        token = OAuth2Token(access_token="token", token_type="Bearer", expires_at=expires_at)

        remaining = token.remaining_lifetime
        assert timedelta(minutes=59) < remaining < timedelta(minutes=61)

    def test_remaining_lifetime_expired_token(self):
        """Expired token should have negative remaining lifetime."""
        expires_at = datetime.now(UTC) - timedelta(minutes=5)
        token = OAuth2Token(access_token="token", token_type="Bearer", expires_at=expires_at)

        remaining = token.remaining_lifetime
        assert remaining < timedelta(0)


class TestOAuth2Config:
    """Tests for OAuth2Config model."""

    def test_basic_config(self):
        """Should create basic config."""
        config = OAuth2Config(
            provider_name="test_provider",
            client_id="client_123",
            client_secret="test-cs",
            token_url="https://auth.example.com/token",
        )

        assert config.provider_name == "test_provider"
        assert config.client_id == "client_123"
        assert config.client_secret == "test-cs"
        assert config.token_url == "https://auth.example.com/token"

    def test_scope_as_string(self):
        """Should handle scope as string."""
        config = OAuth2Config(
            provider_name="test",
            client_id="id",
            client_secret="cs",
            token_url="https://auth.example.com/token",
            scope="read write admin",
        )

        assert config.get_scope_string() == "read write admin"

    def test_scope_as_list(self):
        """Should convert scope list to space-separated string."""
        config = OAuth2Config(
            provider_name="test",
            client_id="id",
            client_secret="cs",
            token_url="https://auth.example.com/token",
            scope=["read", "write", "admin"],
        )

        assert config.get_scope_string() == "read write admin"

    def test_scope_none(self):
        """Should handle None scope."""
        config = OAuth2Config(
            provider_name="test",
            client_id="id",
            client_secret="cs",
            token_url="https://auth.example.com/token",
        )

        assert config.get_scope_string() == ""

    def test_additional_params(self):
        """Should include additional parameters."""
        config = OAuth2Config(
            provider_name="test",
            client_id="id",
            client_secret="cs",
            token_url="https://auth.example.com/token",
            additional_params={"audience": "https://api.example.com"},
        )

        assert config.additional_params == {"audience": "https://api.example.com"}
