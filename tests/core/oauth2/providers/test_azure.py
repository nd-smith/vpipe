"""Tests for AzureADProvider - Azure AD OAuth2 client credentials flow."""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from core.oauth2.exceptions import InvalidConfigurationError, TokenAcquisitionError
from core.oauth2.models import OAuth2Token
from core.oauth2.providers.azure import AzureADProvider

# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------


class TestAzureADProviderInit:
    @patch("core.oauth2.providers.azure.AZURE_IDENTITY_AVAILABLE", True)
    @patch("core.oauth2.providers.azure.ClientSecretCredential")
    def test_creates_provider_with_valid_params(self, mock_csc):
        provider = AzureADProvider(
            provider_name="test_azure",
            client_id="cid",
            client_secret="cs",
            tenant_id="tid",
        )
        assert provider.provider_name == "test_azure"
        assert provider.client_id == "cid"
        assert provider.tenant_id == "tid"
        mock_csc.assert_called_once_with(tenant_id="tid", client_id="cid", client_secret="cs")

    @patch("core.oauth2.providers.azure.AZURE_IDENTITY_AVAILABLE", True)
    @patch("core.oauth2.providers.azure.ClientSecretCredential")
    def test_uses_default_scopes(self, mock_csc):
        provider = AzureADProvider(
            provider_name="test",
            client_id="cid",
            client_secret="cs",
            tenant_id="tid",
        )
        assert provider.scopes == ["https://management.azure.com/.default"]

    @patch("core.oauth2.providers.azure.AZURE_IDENTITY_AVAILABLE", True)
    @patch("core.oauth2.providers.azure.ClientSecretCredential")
    def test_uses_custom_scopes(self, mock_csc):
        provider = AzureADProvider(
            provider_name="test",
            client_id="cid",
            client_secret="cs",
            tenant_id="tid",
            scopes=["https://storage.azure.com/.default"],
        )
        assert provider.scopes == ["https://storage.azure.com/.default"]

    @patch("core.oauth2.providers.azure.AZURE_IDENTITY_AVAILABLE", False)
    def test_raises_when_azure_identity_not_installed(self):
        with pytest.raises(InvalidConfigurationError, match="azure-identity"):
            AzureADProvider(
                provider_name="test",
                client_id="cid",
                client_secret="cs",
                tenant_id="tid",
            )

    @patch("core.oauth2.providers.azure.AZURE_IDENTITY_AVAILABLE", True)
    @patch("core.oauth2.providers.azure.ClientSecretCredential")
    def test_raises_when_client_id_missing(self, mock_csc):
        with pytest.raises(InvalidConfigurationError, match="client_id"):
            AzureADProvider(
                provider_name="test",
                client_id="",
                client_secret="cs",
                tenant_id="tid",
            )

    @patch("core.oauth2.providers.azure.AZURE_IDENTITY_AVAILABLE", True)
    @patch("core.oauth2.providers.azure.ClientSecretCredential")
    def test_raises_when_client_secret_missing(self, mock_csc):
        with pytest.raises(InvalidConfigurationError, match="client_secret"):
            AzureADProvider(
                provider_name="test",
                client_id="cid",
                client_secret="",
                tenant_id="tid",
            )

    @patch("core.oauth2.providers.azure.AZURE_IDENTITY_AVAILABLE", True)
    @patch("core.oauth2.providers.azure.ClientSecretCredential")
    def test_raises_when_tenant_id_missing(self, mock_csc):
        with pytest.raises(InvalidConfigurationError, match="tenant_id"):
            AzureADProvider(
                provider_name="test",
                client_id="cid",
                client_secret="cs",
                tenant_id="",
            )


# ---------------------------------------------------------------------------
# acquire_token
# ---------------------------------------------------------------------------


class TestAcquireToken:
    @patch("core.oauth2.providers.azure.AZURE_IDENTITY_AVAILABLE", True)
    @patch("core.oauth2.providers.azure.ClientSecretCredential")
    async def test_returns_token_with_int_expires_on(self, mock_csc):
        expires_timestamp = int((datetime.now(UTC) + timedelta(hours=1)).timestamp())
        mock_access_token = MagicMock()
        mock_access_token.token = "test-tok"
        mock_access_token.expires_on = expires_timestamp
        mock_csc.return_value.get_token.return_value = mock_access_token

        provider = AzureADProvider(
            provider_name="test",
            client_id="cid",
            client_secret="cs",
            tenant_id="tid",
            scopes=["https://storage.azure.com/.default"],
        )

        token = await provider.acquire_token()

        assert isinstance(token, OAuth2Token)
        assert token.access_token == "test-tok"
        assert token.token_type == "Bearer"
        assert token.scope == "https://storage.azure.com/.default"
        mock_csc.return_value.get_token.assert_called_once_with(
            "https://storage.azure.com/.default"
        )

    @patch("core.oauth2.providers.azure.AZURE_IDENTITY_AVAILABLE", True)
    @patch("core.oauth2.providers.azure.ClientSecretCredential")
    async def test_handles_datetime_expires_on(self, mock_csc):
        expires_dt = datetime.now(UTC) + timedelta(hours=1)
        mock_access_token = MagicMock()
        mock_access_token.token = "test-tok"
        mock_access_token.expires_on = expires_dt
        mock_csc.return_value.get_token.return_value = mock_access_token

        provider = AzureADProvider(
            provider_name="test",
            client_id="cid",
            client_secret="cs",
            tenant_id="tid",
        )

        token = await provider.acquire_token()
        assert token.expires_at is expires_dt

    @patch("core.oauth2.providers.azure.AZURE_IDENTITY_AVAILABLE", True)
    @patch("core.oauth2.providers.azure.ClientSecretCredential")
    async def test_converts_int_expires_on_to_datetime(self, mock_csc):
        ts = int((datetime.now(UTC) + timedelta(hours=1)).timestamp())
        mock_access_token = MagicMock()
        mock_access_token.token = "tok"
        mock_access_token.expires_on = ts
        mock_csc.return_value.get_token.return_value = mock_access_token

        provider = AzureADProvider(
            provider_name="test",
            client_id="cid",
            client_secret="cs",
            tenant_id="tid",
        )

        token = await provider.acquire_token()
        assert isinstance(token.expires_at, datetime)
        # Should be close to the expected time
        expected = datetime.fromtimestamp(ts, UTC)
        assert abs((token.expires_at - expected).total_seconds()) < 2

    @patch("core.oauth2.providers.azure.AZURE_IDENTITY_AVAILABLE", True)
    @patch("core.oauth2.providers.azure.ClientSecretCredential")
    async def test_passes_multiple_scopes(self, mock_csc):
        mock_access_token = MagicMock()
        mock_access_token.token = "tok"
        mock_access_token.expires_on = int(datetime.now(UTC).timestamp()) + 3600
        mock_csc.return_value.get_token.return_value = mock_access_token

        provider = AzureADProvider(
            provider_name="test",
            client_id="cid",
            client_secret="cs",
            tenant_id="tid",
            scopes=["scope1", "scope2"],
        )

        await provider.acquire_token()
        mock_csc.return_value.get_token.assert_called_once_with("scope1", "scope2")

    @patch("core.oauth2.providers.azure.AZURE_IDENTITY_AVAILABLE", True)
    @patch("core.oauth2.providers.azure.ClientSecretCredential")
    async def test_raises_token_acquisition_error_on_sdk_failure(self, mock_csc):
        mock_csc.return_value.get_token.side_effect = Exception("Azure SDK error")

        provider = AzureADProvider(
            provider_name="test",
            client_id="cid",
            client_secret="cs",
            tenant_id="tid",
        )

        with pytest.raises(TokenAcquisitionError, match="Azure AD token acquisition failed"):
            await provider.acquire_token()

    @patch("core.oauth2.providers.azure.AZURE_IDENTITY_AVAILABLE", True)
    @patch("core.oauth2.providers.azure.ClientSecretCredential")
    async def test_token_scope_is_space_joined(self, mock_csc):
        mock_access_token = MagicMock()
        mock_access_token.token = "tok"
        mock_access_token.expires_on = int(datetime.now(UTC).timestamp()) + 3600
        mock_csc.return_value.get_token.return_value = mock_access_token

        provider = AzureADProvider(
            provider_name="test",
            client_id="cid",
            client_secret="cs",
            tenant_id="tid",
            scopes=["scope1", "scope2"],
        )

        token = await provider.acquire_token()
        assert token.scope == "scope1 scope2"


# ---------------------------------------------------------------------------
# refresh_token
# ---------------------------------------------------------------------------


class TestRefreshToken:
    @patch("core.oauth2.providers.azure.AZURE_IDENTITY_AVAILABLE", True)
    @patch("core.oauth2.providers.azure.ClientSecretCredential")
    async def test_refresh_acquires_new_token(self, mock_csc):
        """Azure client credentials flow has no refresh tokens; refresh == acquire."""
        mock_access_token = MagicMock()
        mock_access_token.token = "test-new-tok"
        mock_access_token.expires_on = int(datetime.now(UTC).timestamp()) + 3600
        mock_csc.return_value.get_token.return_value = mock_access_token

        provider = AzureADProvider(
            provider_name="test",
            client_id="cid",
            client_secret="cs",
            tenant_id="tid",
        )

        old_token = OAuth2Token(
            access_token="test-old-tok",
            token_type="Bearer",
            expires_at=datetime.now(UTC) - timedelta(minutes=5),
        )

        result = await provider.refresh_token(old_token)
        assert result.access_token == "test-new-tok"
        mock_csc.return_value.get_token.assert_called_once()

    @patch("core.oauth2.providers.azure.AZURE_IDENTITY_AVAILABLE", True)
    @patch("core.oauth2.providers.azure.ClientSecretCredential")
    async def test_refresh_ignores_old_token(self, mock_csc):
        mock_access_token = MagicMock()
        mock_access_token.token = "fresh"
        mock_access_token.expires_on = int(datetime.now(UTC).timestamp()) + 3600
        mock_csc.return_value.get_token.return_value = mock_access_token

        provider = AzureADProvider(
            provider_name="test",
            client_id="cid",
            client_secret="cs",
            tenant_id="tid",
        )

        old_token = OAuth2Token(
            access_token="stale",
            token_type="Bearer",
            expires_at=datetime.now(UTC),
        )

        result = await provider.refresh_token(old_token)
        assert result.access_token == "fresh"
