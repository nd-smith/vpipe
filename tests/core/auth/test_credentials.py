"""Tests for AzureCredentialProvider - Azure auth with token file and SPN support."""

import json
from unittest.mock import MagicMock, patch

import pytest

from core.auth.credentials import (
    STORAGE_RESOURCE,
    AzureAuthError,
    AzureCredentialProvider,
    clear_token_cache,
    get_default_provider,
    get_storage_options,
)
from core.auth.token_cache import TokenCache


# ---------------------------------------------------------------------------
# AzureCredentialProvider.__init__
# ---------------------------------------------------------------------------


class TestCredentialProviderInit:

    def test_creates_default_cache_when_none_provided(self):
        provider = AzureCredentialProvider(token_file="/tmp/fake")
        assert isinstance(provider._cache, TokenCache)

    def test_uses_provided_cache(self):
        cache = TokenCache()
        provider = AzureCredentialProvider(cache=cache, token_file="/tmp/fake")
        assert provider._cache is cache

    def test_stores_explicit_params(self):
        provider = AzureCredentialProvider(
            token_file="/tmp/token",
            client_id="cid",
            client_secret="csecret",
            tenant_id="tid",
        )
        assert provider.token_file == "/tmp/token"
        assert provider.client_id == "cid"
        assert provider.client_secret == "csecret"
        assert provider.tenant_id == "tid"

    @patch.dict(
        "os.environ",
        {
            "AZURE_TOKEN_FILE": "/env/token",
            "AZURE_CLIENT_ID": "env_cid",
            "AZURE_CLIENT_SECRET": "env_secret",
            "AZURE_TENANT_ID": "env_tid",
        },
    )
    def test_loads_from_env_when_no_token_file_or_client_id(self):
        provider = AzureCredentialProvider()
        assert provider.token_file == "/env/token"
        assert provider.client_id == "env_cid"
        assert provider.client_secret == "env_secret"
        assert provider.tenant_id == "env_tid"

    @patch.dict("os.environ", {}, clear=True)
    def test_loads_from_env_when_nothing_provided(self):
        provider = AzureCredentialProvider()
        assert provider.token_file is None
        assert provider.client_id is None

    def test_skips_env_load_when_token_file_given(self):
        provider = AzureCredentialProvider(token_file="/tmp/tf")
        assert provider.token_file == "/tmp/tf"

    def test_skips_env_load_when_client_id_given(self):
        provider = AzureCredentialProvider(client_id="cid")
        assert provider.client_id == "cid"
        assert provider.token_file is None


# ---------------------------------------------------------------------------
# Properties: has_spn_credentials, auth_mode
# ---------------------------------------------------------------------------


class TestCredentialProviderProperties:

    def test_has_spn_credentials_true(self):
        provider = AzureCredentialProvider(
            client_id="cid", client_secret="cs", tenant_id="tid"
        )
        assert provider.has_spn_credentials is True

    def test_has_spn_credentials_false_missing_secret(self):
        provider = AzureCredentialProvider(client_id="cid", tenant_id="tid")
        assert provider.has_spn_credentials is False

    def test_has_spn_credentials_false_missing_tenant(self):
        provider = AzureCredentialProvider(client_id="cid", client_secret="cs")
        assert provider.has_spn_credentials is False

    def test_auth_mode_file(self):
        provider = AzureCredentialProvider(token_file="/tmp/tf")
        assert provider.auth_mode == "file"

    def test_auth_mode_spn_secret(self):
        provider = AzureCredentialProvider(
            client_id="cid", client_secret="cs", tenant_id="tid"
        )
        assert provider.auth_mode == "spn_secret"

    def test_auth_mode_none(self):
        provider = AzureCredentialProvider(client_id="only_id")
        assert provider.auth_mode == "none"

    def test_auth_mode_file_takes_priority_over_spn(self):
        provider = AzureCredentialProvider(
            token_file="/tmp/tf",
            client_id="cid",
            client_secret="cs",
            tenant_id="tid",
        )
        assert provider.auth_mode == "file"


# ---------------------------------------------------------------------------
# _get_azure_credential
# ---------------------------------------------------------------------------


class TestGetAzureCredential:

    @patch("core.auth.credentials.AZURE_IDENTITY_AVAILABLE", False)
    def test_raises_when_azure_identity_not_installed(self):
        provider = AzureCredentialProvider(
            client_id="cid", client_secret="cs", tenant_id="tid"
        )
        with pytest.raises(AzureAuthError, match="azure-identity library not installed"):
            provider._get_azure_credential()

    @patch("core.auth.credentials.AZURE_IDENTITY_AVAILABLE", True)
    @patch("core.auth.credentials.ClientSecretCredential")
    def test_creates_credential_with_spn(self, mock_csc):
        provider = AzureCredentialProvider(
            client_id="cid", client_secret="cs", tenant_id="tid"
        )
        result = provider._get_azure_credential()
        mock_csc.assert_called_once_with(
            tenant_id="tid", client_id="cid", client_secret="cs"
        )
        assert result is mock_csc.return_value

    @patch("core.auth.credentials.AZURE_IDENTITY_AVAILABLE", True)
    @patch("core.auth.credentials.ClientSecretCredential")
    def test_reuses_cached_credential(self, mock_csc):
        provider = AzureCredentialProvider(
            client_id="cid", client_secret="cs", tenant_id="tid"
        )
        first = provider._get_azure_credential()
        second = provider._get_azure_credential()
        assert first is second
        assert mock_csc.call_count == 1

    @patch("core.auth.credentials.AZURE_IDENTITY_AVAILABLE", True)
    @patch("core.auth.credentials.ClientSecretCredential")
    @patch.dict("os.environ", {"SSL_CERT_FILE": "/path/to/ca.pem"})
    def test_passes_ca_bundle_from_ssl_cert_file(self, mock_csc):
        provider = AzureCredentialProvider(
            client_id="cid", client_secret="cs", tenant_id="tid"
        )
        provider._get_azure_credential()
        mock_csc.assert_called_once_with(
            tenant_id="tid",
            client_id="cid",
            client_secret="cs",
            connection_verify="/path/to/ca.pem",
        )

    @patch("core.auth.credentials.AZURE_IDENTITY_AVAILABLE", True)
    @patch("core.auth.credentials.ClientSecretCredential")
    @patch.dict(
        "os.environ",
        {"REQUESTS_CA_BUNDLE": "/path/to/requests_ca.pem"},
        clear=False,
    )
    def test_passes_ca_bundle_from_requests_ca_bundle(self, mock_csc):
        # Clear SSL_CERT_FILE to test fallback
        import os
        os.environ.pop("SSL_CERT_FILE", None)
        os.environ.pop("CURL_CA_BUNDLE", None)

        provider = AzureCredentialProvider(
            client_id="cid", client_secret="cs", tenant_id="tid"
        )
        provider._get_azure_credential()
        mock_csc.assert_called_once_with(
            tenant_id="tid",
            client_id="cid",
            client_secret="cs",
            connection_verify="/path/to/requests_ca.pem",
        )

    @patch("core.auth.credentials.AZURE_IDENTITY_AVAILABLE", True)
    def test_raises_when_no_credentials_configured(self):
        provider = AzureCredentialProvider(client_id="only_id")
        with pytest.raises(AzureAuthError, match="No valid Azure credential"):
            provider._get_azure_credential()


# ---------------------------------------------------------------------------
# _read_token_file
# ---------------------------------------------------------------------------


class TestReadTokenFile:

    def test_raises_when_token_file_not_set(self):
        provider = AzureCredentialProvider(client_id="cid")
        with pytest.raises(AzureAuthError, match="AZURE_TOKEN_FILE not set"):
            provider._read_token_file("https://storage.azure.com/")

    def test_raises_when_file_not_found(self, tmp_path):
        provider = AzureCredentialProvider(
            token_file=str(tmp_path / "nonexistent.json")
        )
        with pytest.raises(AzureAuthError, match="Token file not found"):
            provider._read_token_file("https://storage.azure.com/")

    def test_raises_when_file_empty(self, tmp_path):
        token_file = tmp_path / "empty.json"
        token_file.write_text("")
        provider = AzureCredentialProvider(token_file=str(token_file))
        with pytest.raises(AzureAuthError, match="Token file is empty"):
            provider._read_token_file("https://storage.azure.com/")

    def test_reads_plain_text_token(self, tmp_path):
        token_file = tmp_path / "token.txt"
        token_file.write_text("eyJplaintoken...")
        provider = AzureCredentialProvider(token_file=str(token_file))
        result = provider._read_token_file("https://storage.azure.com/")
        assert result == "eyJplaintoken..."

    def test_reads_json_with_exact_resource_match(self, tmp_path):
        tokens = {
            "https://storage.azure.com/": "storage_token",
            "https://eventhubs.azure.net/": "eh_token",
        }
        token_file = tmp_path / "tokens.json"
        token_file.write_text(json.dumps(tokens))
        provider = AzureCredentialProvider(token_file=str(token_file))

        result = provider._read_token_file("https://storage.azure.com/")
        assert result == "storage_token"

    def test_reads_json_with_normalized_slash_match(self, tmp_path):
        tokens = {"https://storage.azure.com": "storage_token_no_slash"}
        token_file = tmp_path / "tokens.json"
        token_file.write_text(json.dumps(tokens))
        provider = AzureCredentialProvider(token_file=str(token_file))

        result = provider._read_token_file("https://storage.azure.com/")
        assert result == "storage_token_no_slash"

    def test_raises_when_resource_not_in_json(self, tmp_path):
        tokens = {"https://other.azure.com/": "other_token"}
        token_file = tmp_path / "tokens.json"
        token_file.write_text(json.dumps(tokens))
        provider = AzureCredentialProvider(token_file=str(token_file))

        with pytest.raises(AzureAuthError, match="not found in token file"):
            provider._read_token_file("https://storage.azure.com/")

    def test_handles_utf8_bom(self, tmp_path):
        token_file = tmp_path / "token_bom.txt"
        token_file.write_bytes(b"\xef\xbb\xbfmy_token_value")
        provider = AzureCredentialProvider(token_file=str(token_file))

        result = provider._read_token_file("https://storage.azure.com/")
        assert result == "my_token_value"

    def test_raises_on_os_error(self, tmp_path):
        token_file = tmp_path / "token.txt"
        token_file.write_text("content")
        provider = AzureCredentialProvider(token_file=str(token_file))

        with patch("pathlib.Path.read_text", side_effect=OSError("Permission denied")):
            with pytest.raises(AzureAuthError, match="Failed to read token file"):
                provider._read_token_file("https://storage.azure.com/")

    def test_reads_json_with_whitespace_only_content_as_empty(self, tmp_path):
        token_file = tmp_path / "token.txt"
        token_file.write_text("   \n  ")
        provider = AzureCredentialProvider(token_file=str(token_file))
        with pytest.raises(AzureAuthError, match="Token file is empty"):
            provider._read_token_file("https://storage.azure.com/")


# ---------------------------------------------------------------------------
# get_token_for_resource
# ---------------------------------------------------------------------------


class TestGetTokenForResource:

    def test_returns_token_from_file(self, tmp_path):
        token_file = tmp_path / "token.txt"
        token_file.write_text("file_token_123")
        provider = AzureCredentialProvider(token_file=str(token_file))

        result = provider.get_token_for_resource("https://storage.azure.com/")
        assert result == "file_token_123"

    def test_caches_token_from_file(self, tmp_path):
        token_file = tmp_path / "token.txt"
        token_file.write_text("file_token_123")
        cache = TokenCache()
        provider = AzureCredentialProvider(token_file=str(token_file), cache=cache)

        provider.get_token_for_resource("https://storage.azure.com/")
        assert cache.get("https://storage.azure.com/") == "file_token_123"

    @patch("core.auth.credentials.AZURE_IDENTITY_AVAILABLE", True)
    @patch("core.auth.credentials.ClientSecretCredential")
    def test_falls_back_to_spn_when_token_file_fails(self, mock_csc, tmp_path):
        mock_access_token = MagicMock()
        mock_access_token.token = "spn_token_456"
        mock_csc.return_value.get_token.return_value = mock_access_token

        # Token file doesn't exist, but is configured
        provider = AzureCredentialProvider(
            token_file=str(tmp_path / "missing.json"),
            client_id="cid",
            client_secret="cs",
            tenant_id="tid",
        )

        result = provider.get_token_for_resource("https://storage.azure.com/")
        assert result == "spn_token_456"

    def test_returns_cached_token(self):
        cache = TokenCache()
        cache.set("https://storage.azure.com/", "cached_token")
        provider = AzureCredentialProvider(client_id="cid", cache=cache)

        result = provider.get_token_for_resource("https://storage.azure.com/")
        assert result == "cached_token"

    @patch("core.auth.credentials.AZURE_IDENTITY_AVAILABLE", True)
    @patch("core.auth.credentials.ClientSecretCredential")
    def test_skips_cache_when_force_refresh(self, mock_csc):
        mock_access_token = MagicMock()
        mock_access_token.token = "fresh_token"
        mock_csc.return_value.get_token.return_value = mock_access_token

        cache = TokenCache()
        cache.set("https://storage.azure.com/", "old_cached_token")
        provider = AzureCredentialProvider(
            client_id="cid", client_secret="cs", tenant_id="tid", cache=cache
        )

        result = provider.get_token_for_resource(
            "https://storage.azure.com/", force_refresh=True
        )
        assert result == "fresh_token"

    @patch("core.auth.credentials.AZURE_IDENTITY_AVAILABLE", True)
    @patch("core.auth.credentials.ClientSecretCredential")
    def test_acquires_token_via_spn(self, mock_csc):
        mock_access_token = MagicMock()
        mock_access_token.token = "spn_token"
        mock_csc.return_value.get_token.return_value = mock_access_token

        provider = AzureCredentialProvider(
            client_id="cid", client_secret="cs", tenant_id="tid"
        )
        result = provider.get_token_for_resource("https://storage.azure.com/")
        assert result == "spn_token"
        mock_csc.return_value.get_token.assert_called_once_with(
            "https://storage.azure.com/.default"
        )

    @patch("core.auth.credentials.AZURE_IDENTITY_AVAILABLE", True)
    @patch("core.auth.credentials.ClientSecretCredential")
    def test_caches_spn_token(self, mock_csc):
        mock_access_token = MagicMock()
        mock_access_token.token = "spn_token"
        mock_csc.return_value.get_token.return_value = mock_access_token

        cache = TokenCache()
        provider = AzureCredentialProvider(
            client_id="cid", client_secret="cs", tenant_id="tid", cache=cache
        )
        provider.get_token_for_resource("https://storage.azure.com/")
        assert cache.get("https://storage.azure.com/") == "spn_token"

    def test_raises_azure_auth_error_on_failure(self):
        provider = AzureCredentialProvider(client_id="only_id")
        with pytest.raises(AzureAuthError, match="Failed to acquire token"):
            provider.get_token_for_resource("https://storage.azure.com/")

    @patch("core.auth.credentials.AZURE_IDENTITY_AVAILABLE", True)
    @patch("core.auth.credentials.ClientSecretCredential")
    def test_normalizes_scope_trailing_slash(self, mock_csc):
        mock_access_token = MagicMock()
        mock_access_token.token = "token"
        mock_csc.return_value.get_token.return_value = mock_access_token

        provider = AzureCredentialProvider(
            client_id="cid", client_secret="cs", tenant_id="tid"
        )
        provider.get_token_for_resource("https://storage.azure.com/")
        mock_csc.return_value.get_token.assert_called_with(
            "https://storage.azure.com/.default"
        )


# ---------------------------------------------------------------------------
# get_storage_token, get_storage_options, get_kusto_token
# ---------------------------------------------------------------------------


class TestConvenienceMethods:

    def test_get_storage_token_delegates(self):
        cache = TokenCache()
        cache.set(STORAGE_RESOURCE, "storage_tok")
        provider = AzureCredentialProvider(client_id="cid", cache=cache)

        assert provider.get_storage_token() == "storage_tok"

    def test_get_storage_options_returns_spn_creds_when_available(self):
        provider = AzureCredentialProvider(
            client_id="cid", client_secret="cs", tenant_id="tid"
        )
        opts = provider.get_storage_options()
        assert opts == {
            "azure_client_id": "cid",
            "azure_client_secret": "cs",
            "azure_tenant_id": "tid",
        }

    def test_get_storage_options_returns_token_when_file_mode(self, tmp_path):
        token_file = tmp_path / "token.txt"
        token_file.write_text("my_storage_token")
        provider = AzureCredentialProvider(
            token_file=str(token_file),
            client_id="cid",
            client_secret="cs",
            tenant_id="tid",
        )
        opts = provider.get_storage_options()
        assert opts == {"azure_storage_token": "my_storage_token"}

    def test_get_storage_options_returns_empty_on_auth_failure(self):
        provider = AzureCredentialProvider(client_id="only_id")
        opts = provider.get_storage_options()
        assert opts == {}

    def test_get_kusto_token_delegates(self):
        cache = TokenCache()
        cache.set("https://mycluster.kusto.windows.net", "kusto_tok")
        provider = AzureCredentialProvider(client_id="cid", cache=cache)

        result = provider.get_kusto_token("https://mycluster.kusto.windows.net")
        assert result == "kusto_tok"


# ---------------------------------------------------------------------------
# clear_cache, get_diagnostics
# ---------------------------------------------------------------------------


class TestCacheAndDiagnostics:

    def test_clear_cache_specific_resource(self):
        cache = TokenCache()
        cache.set("res1", "tok1")
        cache.set("res2", "tok2")
        provider = AzureCredentialProvider(client_id="cid", cache=cache)

        provider.clear_cache("res1")
        assert cache.get("res1") is None
        assert cache.get("res2") == "tok2"

    def test_clear_cache_all(self):
        cache = TokenCache()
        cache.set("res1", "tok1")
        cache.set("res2", "tok2")
        provider = AzureCredentialProvider(client_id="cid", cache=cache)

        provider.clear_cache()
        assert cache.get("res1") is None
        assert cache.get("res2") is None

    def test_get_diagnostics_file_mode(self):
        provider = AzureCredentialProvider(token_file="/tmp/tf")
        diag = provider.get_diagnostics()
        assert diag["auth_mode"] == "file"
        assert diag["token_file"] == "/tmp/tf"
        assert diag["spn_configured"] is False

    def test_get_diagnostics_spn_mode(self):
        provider = AzureCredentialProvider(
            client_id="cid", client_secret="cs", tenant_id="tid"
        )
        diag = provider.get_diagnostics()
        assert diag["auth_mode"] == "spn_secret"
        assert diag["spn_configured"] is True

    def test_get_diagnostics_includes_token_age(self):
        cache = TokenCache()
        cache.set(STORAGE_RESOURCE, "tok")
        provider = AzureCredentialProvider(client_id="cid", cache=cache)
        diag = provider.get_diagnostics()
        assert "storage_token_age_seconds" in diag
        assert diag["storage_token_age_seconds"] < 1

    def test_get_diagnostics_no_token_age_when_uncached(self):
        provider = AzureCredentialProvider(client_id="cid")
        diag = provider.get_diagnostics()
        assert "storage_token_age_seconds" not in diag


# ---------------------------------------------------------------------------
# Module-level functions
# ---------------------------------------------------------------------------


class TestModuleLevelFunctions:

    def setup_method(self):
        # Reset the module-level singleton
        import core.auth.credentials as mod
        mod._default_provider = None

    @patch.dict("os.environ", {"AZURE_CLIENT_ID": "env_cid"}, clear=False)
    def test_get_default_provider_creates_singleton(self):
        p1 = get_default_provider()
        p2 = get_default_provider()
        assert p1 is p2
        assert isinstance(p1, AzureCredentialProvider)

    @patch.dict("os.environ", {"AZURE_CLIENT_ID": "env_cid"}, clear=False)
    def test_get_storage_options_uses_default_provider(self):
        with patch.object(
            AzureCredentialProvider, "get_storage_options", return_value={"key": "val"}
        ):
            result = get_storage_options()
            assert result == {"key": "val"}

    @patch.dict("os.environ", {"AZURE_CLIENT_ID": "env_cid"}, clear=False)
    def test_clear_token_cache_uses_default_provider(self):
        with patch.object(AzureCredentialProvider, "clear_cache") as mock_clear:
            clear_token_cache("some_resource")
            mock_clear.assert_called_once_with("some_resource")
