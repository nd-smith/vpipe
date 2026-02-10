"""Tests for EventHub SASL/OAUTHBEARER authentication callbacks."""

from unittest.mock import MagicMock, patch

import pytest

from core.auth.credentials import AzureAuthError, AzureCredentialProvider
from core.auth.eventhub_oauth import (
    EVENTHUB_RESOURCE,
    EventHubAuthError,
    create_eventhub_oauth_callback,
    get_eventhub_oauth_token,
)

# ---------------------------------------------------------------------------
# create_eventhub_oauth_callback
# ---------------------------------------------------------------------------


class TestCreateEventhubOauthCallback:
    def test_returns_callable(self):
        provider = MagicMock(spec=AzureCredentialProvider)
        provider.auth_mode = "spn_secret"
        callback = create_eventhub_oauth_callback(provider)
        assert callable(callback)

    @patch.dict("os.environ", {"AZURE_CLIENT_ID": "cid"}, clear=False)
    def test_creates_provider_from_env_when_none_given(self):
        with patch("core.auth.eventhub_oauth.AzureCredentialProvider") as mock_provider_cls:
            mock_provider_cls.return_value.auth_mode = "none"
            callback = create_eventhub_oauth_callback()
            mock_provider_cls.assert_called_once()
            assert callable(callback)

    def test_callback_returns_token(self):
        provider = MagicMock(spec=AzureCredentialProvider)
        provider.auth_mode = "spn_secret"
        provider.get_token_for_resource.return_value = "test-eh-tok"

        callback = create_eventhub_oauth_callback(provider)
        result = callback()

        assert result == "test-eh-tok"
        provider.get_token_for_resource.assert_called_once_with(EVENTHUB_RESOURCE)

    def test_callback_raises_eventhub_auth_error_on_azure_auth_error(self):
        provider = MagicMock(spec=AzureCredentialProvider)
        provider.auth_mode = "spn_secret"
        provider.get_token_for_resource.side_effect = AzureAuthError("cred fail")

        callback = create_eventhub_oauth_callback(provider)

        with pytest.raises(EventHubAuthError, match="Failed to acquire OAuth token"):
            callback()

    def test_callback_wraps_azure_auth_error_with_cause(self):
        provider = MagicMock(spec=AzureCredentialProvider)
        provider.auth_mode = "spn_secret"
        original = AzureAuthError("original error")
        provider.get_token_for_resource.side_effect = original

        callback = create_eventhub_oauth_callback(provider)

        with pytest.raises(EventHubAuthError) as exc_info:
            callback()
        assert exc_info.value.__cause__ is original

    def test_callback_raises_eventhub_auth_error_on_unexpected_error(self):
        provider = MagicMock(spec=AzureCredentialProvider)
        provider.auth_mode = "spn_secret"
        provider.get_token_for_resource.side_effect = RuntimeError("boom")

        callback = create_eventhub_oauth_callback(provider)

        with pytest.raises(EventHubAuthError, match="Unexpected error"):
            callback()

    def test_callback_wraps_unexpected_error_with_cause(self):
        provider = MagicMock(spec=AzureCredentialProvider)
        provider.auth_mode = "spn_secret"
        original = RuntimeError("boom")
        provider.get_token_for_resource.side_effect = original

        callback = create_eventhub_oauth_callback(provider)

        with pytest.raises(EventHubAuthError) as exc_info:
            callback()
        assert exc_info.value.__cause__ is original

    def test_callback_includes_auth_mode_in_error_message(self):
        provider = MagicMock(spec=AzureCredentialProvider)
        provider.auth_mode = "file"
        provider.get_token_for_resource.side_effect = AzureAuthError("fail")

        callback = create_eventhub_oauth_callback(provider)

        with pytest.raises(EventHubAuthError, match="Auth mode: file"):
            callback()


# ---------------------------------------------------------------------------
# get_eventhub_oauth_token
# ---------------------------------------------------------------------------


class TestGetEventhubOauthToken:
    def test_returns_token_with_provided_provider(self):
        provider = MagicMock(spec=AzureCredentialProvider)
        provider.get_token_for_resource.return_value = "test-tok"

        result = get_eventhub_oauth_token(provider)
        assert result == "test-tok"
        provider.get_token_for_resource.assert_called_once_with(EVENTHUB_RESOURCE, False)

    def test_passes_force_refresh(self):
        provider = MagicMock(spec=AzureCredentialProvider)
        provider.get_token_for_resource.return_value = "test-fresh-tok"

        result = get_eventhub_oauth_token(provider, force_refresh=True)
        assert result == "test-fresh-tok"
        provider.get_token_for_resource.assert_called_once_with(EVENTHUB_RESOURCE, True)

    @patch.dict("os.environ", {"AZURE_CLIENT_ID": "cid"}, clear=False)
    def test_creates_provider_when_none_given(self):
        with patch("core.auth.eventhub_oauth.AzureCredentialProvider") as mock_provider_cls:
            mock_provider_cls.return_value.get_token_for_resource.return_value = "tok"
            result = get_eventhub_oauth_token()
            assert result == "tok"
            mock_provider_cls.assert_called_once()

    def test_raises_eventhub_auth_error_on_azure_auth_error(self):
        provider = MagicMock(spec=AzureCredentialProvider)
        provider.get_token_for_resource.side_effect = AzureAuthError("auth fail")

        with pytest.raises(EventHubAuthError, match="Failed to get EventHub OAuth token"):
            get_eventhub_oauth_token(provider)

    def test_wraps_azure_auth_error_cause(self):
        provider = MagicMock(spec=AzureCredentialProvider)
        original = AzureAuthError("auth fail")
        provider.get_token_for_resource.side_effect = original

        with pytest.raises(EventHubAuthError) as exc_info:
            get_eventhub_oauth_token(provider)
        assert exc_info.value.__cause__ is original


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


class TestConstants:
    def test_eventhub_resource_url(self):
        assert EVENTHUB_RESOURCE == "https://eventhubs.azure.net/"
