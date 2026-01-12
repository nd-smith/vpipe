"""
Tests for Kafka OAUTHBEARER authentication.

Tests callback creation, token acquisition, and error handling
for Kafka/EventHub OAuth authentication.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone, timedelta

from core.auth.kafka_oauth import (
    create_kafka_oauth_callback,
    get_kafka_oauth_token,
    KafkaOAuthError,
    EVENTHUB_RESOURCE,
    EVENTHUB_SCOPE,
)
from core.auth.credentials import AzureAuthError, AzureCredentialProvider
from core.auth.token_cache import TokenCache


class TestKafkaOAuthCallback:
    """Test Kafka OAuth callback creation and execution."""

    def test_create_callback_with_explicit_provider(self):
        """Test creating callback with explicit credential provider."""
        provider = AzureCredentialProvider(
            client_id="test-client",
            client_secret="test-secret",
            tenant_id="test-tenant",
        )

        callback = create_kafka_oauth_callback(provider)

        assert callable(callback)
        assert callback.__name__ == "oauth_callback"

    def test_create_callback_without_provider(self, monkeypatch):
        """Test creating callback without provider (loads from env)."""
        monkeypatch.setenv("AZURE_CLIENT_ID", "env-client")
        monkeypatch.setenv("AZURE_CLIENT_SECRET", "env-secret")
        monkeypatch.setenv("AZURE_TENANT_ID", "env-tenant")

        callback = create_kafka_oauth_callback()

        assert callable(callback)

    @patch('core.auth.kafka_oauth.AzureCredentialProvider')
    def test_callback_acquires_token_successfully(self, mock_provider_class):
        """Test callback successfully acquires EventHub token."""
        # Setup mock provider
        mock_provider = Mock()
        mock_provider.get_token_for_resource.return_value = "test-eventhub-token"
        mock_provider.auth_mode = "spn_secret"

        # Create callback
        callback = create_kafka_oauth_callback(mock_provider)

        # Execute callback
        token = callback()

        # Verify
        assert token == "test-eventhub-token"
        mock_provider.get_token_for_resource.assert_called_once_with(EVENTHUB_RESOURCE)

    @patch('core.auth.kafka_oauth.AzureCredentialProvider')
    def test_callback_handles_auth_error(self, mock_provider_class):
        """Test callback converts AzureAuthError to KafkaOAuthError."""
        # Setup mock provider to raise error
        mock_provider = Mock()
        mock_provider.get_token_for_resource.side_effect = AzureAuthError("Auth failed")
        mock_provider.auth_mode = "spn_secret"

        # Create callback
        callback = create_kafka_oauth_callback(mock_provider)

        # Execute callback - should raise KafkaOAuthError
        with pytest.raises(KafkaOAuthError) as exc_info:
            callback()

        # Verify error message contains useful info
        assert "Failed to acquire OAuth token for Kafka/EventHub" in str(exc_info.value)
        assert "spn_secret" in str(exc_info.value)
        assert "Auth failed" in str(exc_info.value)

        # Verify original exception is chained
        assert isinstance(exc_info.value.__cause__, AzureAuthError)

    @patch('core.auth.kafka_oauth.AzureCredentialProvider')
    def test_callback_handles_unexpected_error(self, mock_provider_class):
        """Test callback handles unexpected exceptions."""
        # Setup mock provider to raise unexpected error
        mock_provider = Mock()
        mock_provider.get_token_for_resource.side_effect = RuntimeError("Unexpected error")
        mock_provider.auth_mode = "spn_secret"

        # Create callback
        callback = create_kafka_oauth_callback(mock_provider)

        # Execute callback - should raise KafkaOAuthError
        with pytest.raises(KafkaOAuthError) as exc_info:
            callback()

        # Verify error message
        assert "Unexpected error acquiring Kafka OAuth token" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, RuntimeError)

    def test_callback_uses_cached_token(self):
        """Test callback reuses cached token from provider."""
        cache = TokenCache()
        cache.set(EVENTHUB_RESOURCE, "cached-token")

        provider = AzureCredentialProvider(
            cache=cache,
            use_cli=True,
        )

        callback = create_kafka_oauth_callback(provider)

        # Execute callback - should use cached token
        with patch.object(provider, '_fetch_cli_token') as mock_fetch:
            token = callback()

            # Verify token came from cache, not CLI
            assert token == "cached-token"
            mock_fetch.assert_not_called()

    def test_callback_reuses_provider_instance(self):
        """Test callback captures and reuses provider instance."""
        provider = Mock()
        provider.get_token_for_resource.return_value = "token-1"
        provider.auth_mode = "spn_secret"

        callback = create_kafka_oauth_callback(provider)

        # Call multiple times
        token1 = callback()
        token2 = callback()

        # Verify same provider used
        assert provider.get_token_for_resource.call_count == 2
        assert token1 == "token-1"
        assert token2 == "token-1"


class TestGetKafkaOAuthToken:
    """Test direct token acquisition function."""

    @patch('core.auth.kafka_oauth.AzureCredentialProvider')
    def test_get_token_with_explicit_provider(self, mock_provider_class):
        """Test getting token with explicit provider."""
        mock_provider = Mock()
        mock_provider.get_token_for_resource.return_value = "test-token"

        token = get_kafka_oauth_token(mock_provider)

        assert token == "test-token"
        mock_provider.get_token_for_resource.assert_called_once_with(
            EVENTHUB_RESOURCE,
            False
        )

    @patch('core.auth.kafka_oauth.AzureCredentialProvider')
    def test_get_token_without_provider(self, mock_provider_class):
        """Test getting token without provider (creates new one)."""
        # Setup mock
        mock_instance = Mock()
        mock_instance.get_token_for_resource.return_value = "env-token"
        mock_provider_class.return_value = mock_instance

        token = get_kafka_oauth_token()

        assert token == "env-token"
        mock_provider_class.assert_called_once_with()
        mock_instance.get_token_for_resource.assert_called_once()

    @patch('core.auth.kafka_oauth.AzureCredentialProvider')
    def test_get_token_with_force_refresh(self, mock_provider_class):
        """Test getting token with force_refresh=True."""
        mock_provider = Mock()
        mock_provider.get_token_for_resource.return_value = "fresh-token"

        token = get_kafka_oauth_token(mock_provider, force_refresh=True)

        assert token == "fresh-token"
        mock_provider.get_token_for_resource.assert_called_once_with(
            EVENTHUB_RESOURCE,
            True
        )

    @patch('core.auth.kafka_oauth.AzureCredentialProvider')
    def test_get_token_converts_auth_error(self, mock_provider_class):
        """Test get_token converts AzureAuthError to KafkaOAuthError."""
        mock_provider = Mock()
        mock_provider.get_token_for_resource.side_effect = AzureAuthError("Auth failed")

        with pytest.raises(KafkaOAuthError) as exc_info:
            get_kafka_oauth_token(mock_provider)

        assert "Failed to get Kafka OAuth token" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, AzureAuthError)


class TestEventHubConstants:
    """Test EventHub resource constants."""

    def test_eventhub_resource_url(self):
        """Test EventHub resource URL is correctly defined."""
        assert EVENTHUB_RESOURCE == "https://eventhubs.azure.net/"
        assert isinstance(EVENTHUB_RESOURCE, str)

    def test_eventhub_scope(self):
        """Test EventHub scope is correctly defined."""
        assert EVENTHUB_SCOPE == "https://eventhubs.azure.net/.default"
        assert isinstance(EVENTHUB_SCOPE, str)

    def test_resource_and_scope_relationship(self):
        """Test EventHub scope follows Azure naming convention."""
        assert EVENTHUB_SCOPE == EVENTHUB_RESOURCE.rstrip("/") + "/.default"


class TestIntegrationWithCredentialProvider:
    """Integration tests with real AzureCredentialProvider."""

    def test_callback_with_token_file(self, tmp_path):
        """Test callback works with token file authentication."""
        # Create token file with EventHub token
        token_file = tmp_path / "tokens.json"
        token_file.write_text(json.dumps({
            EVENTHUB_RESOURCE: "eventhub-token-from-file"
        }))

        provider = AzureCredentialProvider(token_file=str(token_file))
        callback = create_kafka_oauth_callback(provider)

        # Execute callback
        token = callback()

        assert token == "eventhub-token-from-file"

    def test_callback_with_token_file_normalized_key(self, tmp_path):
        """Test callback handles resource URL normalization."""
        # Create token file with trailing slash difference
        token_file = tmp_path / "tokens.json"
        token_file.write_text(json.dumps({
            "https://eventhubs.azure.net": "eventhub-token-normalized"  # no trailing slash
        }))

        provider = AzureCredentialProvider(token_file=str(token_file))
        callback = create_kafka_oauth_callback(provider)

        # Execute callback - should normalize and match
        token = callback()

        assert token == "eventhub-token-normalized"

    def test_callback_with_missing_resource_in_token_file(self, tmp_path):
        """Test callback fails when EventHub resource not in token file."""
        # Create token file without EventHub resource
        token_file = tmp_path / "tokens.json"
        token_file.write_text(json.dumps({
            "https://storage.azure.com/": "storage-token-only"
        }))

        provider = AzureCredentialProvider(token_file=str(token_file))
        callback = create_kafka_oauth_callback(provider)

        # Execute callback - should fail with actionable error
        # Note: Provider tries fallback to other auth methods when token file fails
        with pytest.raises(KafkaOAuthError) as exc_info:
            callback()

        error_msg = str(exc_info.value)
        # Should reference EventHub in the error
        assert "eventhubs.azure.net" in error_msg or "Kafka/EventHub" in error_msg


class TestKafkaOAuthError:
    """Test KafkaOAuthError exception."""

    def test_kafka_oauth_error_is_exception(self):
        """Test KafkaOAuthError inherits from Exception."""
        error = KafkaOAuthError("test error")
        assert isinstance(error, Exception)

    def test_kafka_oauth_error_message(self):
        """Test KafkaOAuthError preserves message."""
        error = KafkaOAuthError("OAuth acquisition failed")
        assert str(error) == "OAuth acquisition failed"

    def test_kafka_oauth_error_with_cause(self):
        """Test KafkaOAuthError can chain exceptions."""
        original = AzureAuthError("Auth failed")
        kafka_error = KafkaOAuthError("Kafka OAuth failed")
        kafka_error.__cause__ = original

        assert kafka_error.__cause__ is original
        assert isinstance(kafka_error.__cause__, AzureAuthError)


class TestAiokafkaCompatibility:
    """Test compatibility with aiokafka usage patterns."""

    def test_callback_signature_matches_aiokafka(self):
        """Test callback signature is compatible with aiokafka."""
        # aiokafka expects: Callable[[], str]
        provider = Mock()
        provider.get_token_for_resource.return_value = "test-token"
        provider.auth_mode = "spn_secret"

        callback = create_kafka_oauth_callback(provider)

        # Verify signature
        import inspect
        sig = inspect.signature(callback)

        # Should take no parameters and return value
        assert len(sig.parameters) == 0
        assert callable(callback)

    def test_callback_returns_string(self):
        """Test callback returns string token (not AccessToken object)."""
        provider = Mock()
        provider.get_token_for_resource.return_value = "string-token"
        provider.auth_mode = "spn_secret"

        callback = create_kafka_oauth_callback(provider)
        token = callback()

        # Should return string, not AccessToken object
        assert isinstance(token, str)
        assert token == "string-token"

    def test_callback_can_be_called_multiple_times(self):
        """Test callback can be invoked repeatedly (as aiokafka does)."""
        provider = Mock()
        provider.get_token_for_resource.return_value = "token"
        provider.auth_mode = "spn_secret"

        callback = create_kafka_oauth_callback(provider)

        # Call multiple times (simulating aiokafka token refresh)
        tokens = [callback() for _ in range(5)]

        # All should succeed
        assert all(token == "token" for token in tokens)
        assert provider.get_token_for_resource.call_count == 5


# Import json for token file tests
import json
