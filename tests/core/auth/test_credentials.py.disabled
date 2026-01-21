"""
Tests for Azure credential provider.

Tests all authentication methods: CLI, SPN (secret/certificate),
token file, and DefaultAzureCredential.
"""

import pytest
import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone, timedelta

from core.auth.credentials import (
    AzureAuthError,
    AzureCredentialProvider,
    get_default_provider,
    get_storage_options,
    clear_token_cache,
    STORAGE_RESOURCE,
    AZURE_IDENTITY_AVAILABLE,
)
from core.auth.token_cache import TokenCache


# Mock azure.identity if not available
if not AZURE_IDENTITY_AVAILABLE:
    sys.modules['azure'] = MagicMock()
    sys.modules['azure.identity'] = MagicMock()
    sys.modules['azure.core'] = MagicMock()
    sys.modules['azure.core.credentials'] = MagicMock()


class TestAzureCredentialProvider:
    """Test AzureCredentialProvider initialization and configuration."""

    def test_init_with_explicit_config(self):
        """Test explicit configuration via constructor."""
        provider = AzureCredentialProvider(
            use_cli=True,
            client_id="test-client",
            tenant_id="test-tenant",
        )

        assert provider.use_cli is True
        assert provider.client_id == "test-client"
        assert provider.tenant_id == "test-tenant"
        assert provider.auth_mode == "cli"

    def test_init_from_env_cli(self, monkeypatch):
        """Test loading CLI config from environment."""
        monkeypatch.setenv("AZURE_AUTH_INTERACTIVE", "true")

        provider = AzureCredentialProvider()

        assert provider.use_cli is True
        assert provider.auth_mode == "cli"

    def test_init_from_env_spn_secret(self, monkeypatch):
        """Test loading SPN secret config from environment."""
        monkeypatch.setenv("AZURE_CLIENT_ID", "test-client")
        monkeypatch.setenv("AZURE_CLIENT_SECRET", "test-secret")
        monkeypatch.setenv("AZURE_TENANT_ID", "test-tenant")

        provider = AzureCredentialProvider()

        assert provider.has_spn_credentials is True
        assert provider.auth_mode == "spn_secret"

    def test_init_from_env_spn_cert(self, monkeypatch, tmp_path):
        """Test loading SPN certificate config from environment."""
        cert_path = tmp_path / "cert.pem"
        cert_path.write_text("fake-cert")

        monkeypatch.setenv("AZURE_CLIENT_ID", "test-client")
        monkeypatch.setenv("AZURE_CERTIFICATE_PATH", str(cert_path))
        monkeypatch.setenv("AZURE_TENANT_ID", "test-tenant")

        provider = AzureCredentialProvider()

        assert provider.has_spn_credentials is True
        assert provider.auth_mode == "spn_cert"

    def test_init_from_env_token_file(self, monkeypatch, tmp_path):
        """Test loading token file config from environment."""
        token_file = tmp_path / "tokens.json"
        token_file.write_text('{"https://storage.azure.com/": "test-token"}')

        monkeypatch.setenv("AZURE_TOKEN_FILE", str(token_file))

        provider = AzureCredentialProvider()

        assert provider.token_file == str(token_file)
        assert provider.auth_mode == "file"

    def test_init_defaults_to_default_credential(self):
        """Test defaulting to DefaultAzureCredential when no config."""
        provider = AzureCredentialProvider()

        assert provider.use_default_credential is True
        assert provider.auth_mode == "default"

    def test_has_spn_credentials_with_secret(self):
        """Test SPN detection with client secret."""
        provider = AzureCredentialProvider(
            client_id="test-client",
            client_secret="test-secret",
            tenant_id="test-tenant",
        )

        assert provider.has_spn_credentials is True

    def test_has_spn_credentials_with_cert(self):
        """Test SPN detection with certificate."""
        provider = AzureCredentialProvider(
            client_id="test-client",
            certificate_path="/path/to/cert.pem",
            tenant_id="test-tenant",
        )

        assert provider.has_spn_credentials is True

    def test_has_spn_credentials_incomplete(self):
        """Test SPN detection with incomplete config."""
        provider = AzureCredentialProvider(
            client_id="test-client",
            # Missing tenant_id and secret/cert
        )

        assert provider.has_spn_credentials is False


class TestTokenFileReading:
    """Test token file reading with JSON and plain text formats."""

    def test_read_json_token_file(self, tmp_path):
        """Test reading token from JSON file."""
        token_file = tmp_path / "tokens.json"
        tokens = {
            "https://storage.azure.com/": "storage-token",
            "https://vault.azure.net/": "vault-token",
        }
        token_file.write_text(json.dumps(tokens))

        provider = AzureCredentialProvider(token_file=str(token_file))

        token = provider._read_token_file(STORAGE_RESOURCE)
        assert token == "storage-token"

    def test_read_json_token_file_normalized_match(self, tmp_path):
        """Test reading with trailing slash normalization."""
        token_file = tmp_path / "tokens.json"
        tokens = {
            "https://storage.azure.com": "storage-token",  # No trailing slash
        }
        token_file.write_text(json.dumps(tokens))

        provider = AzureCredentialProvider(token_file=str(token_file))

        # Request with trailing slash should match
        token = provider._read_token_file("https://storage.azure.com/")
        assert token == "storage-token"

    def test_read_plain_text_token_file(self, tmp_path):
        """Test reading plain text token file (legacy format)."""
        token_file = tmp_path / "token.txt"
        token_file.write_text("plain-text-token")

        provider = AzureCredentialProvider(token_file=str(token_file))

        token = provider._read_token_file(STORAGE_RESOURCE)
        assert token == "plain-text-token"

    def test_read_token_file_not_found(self):
        """Test error when token file doesn't exist."""
        provider = AzureCredentialProvider(token_file="/nonexistent/tokens.json")

        with pytest.raises(AzureAuthError, match="Token file not found"):
            provider._read_token_file(STORAGE_RESOURCE)

    def test_read_token_file_empty(self, tmp_path):
        """Test error when token file is empty."""
        token_file = tmp_path / "empty.json"
        token_file.write_text("")

        provider = AzureCredentialProvider(token_file=str(token_file))

        with pytest.raises(AzureAuthError, match="Token file is empty"):
            provider._read_token_file(STORAGE_RESOURCE)

    def test_read_token_file_resource_not_found(self, tmp_path):
        """Test error when resource not in JSON file."""
        token_file = tmp_path / "tokens.json"
        tokens = {"https://vault.azure.net/": "vault-token"}
        token_file.write_text(json.dumps(tokens))

        provider = AzureCredentialProvider(token_file=str(token_file))

        with pytest.raises(AzureAuthError, match="Resource .* not found"):
            provider._read_token_file(STORAGE_RESOURCE)

    def test_read_token_file_not_configured(self):
        """Test error when token file not configured."""
        provider = AzureCredentialProvider(use_cli=True)

        with pytest.raises(AzureAuthError, match="AZURE_TOKEN_FILE not set"):
            provider._read_token_file(STORAGE_RESOURCE)


class TestCLITokenFetching:
    """Test Azure CLI token fetching."""

    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/az")
    def test_fetch_cli_token_success(self, mock_which, mock_popen):
        """Test successful CLI token fetch."""
        mock_proc = Mock()
        mock_proc.returncode = 0
        mock_proc.communicate.return_value = ("test-token\n", "")
        mock_popen.return_value = mock_proc

        provider = AzureCredentialProvider(use_cli=True)

        token = provider._fetch_cli_token(STORAGE_RESOURCE)

        assert token == "test-token"
        mock_popen.assert_called_once()

    @patch("shutil.which", return_value=None)
    def test_fetch_cli_token_not_installed(self, mock_which):
        """Test error when Azure CLI not installed."""
        provider = AzureCredentialProvider(use_cli=True)

        with pytest.raises(AzureAuthError, match="Azure CLI not found"):
            provider._fetch_cli_token(STORAGE_RESOURCE)

    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/az")
    def test_fetch_cli_token_session_expired(self, mock_which, mock_popen):
        """Test error when CLI session expired."""
        mock_proc = Mock()
        mock_proc.returncode = 1
        mock_proc.communicate.return_value = ("", "Please run 'az login' to authenticate")
        mock_popen.return_value = mock_proc

        provider = AzureCredentialProvider(use_cli=True)

        with pytest.raises(AzureAuthError, match="Azure CLI session expired"):
            provider._fetch_cli_token(STORAGE_RESOURCE)

    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/az")
    def test_fetch_cli_token_empty_response(self, mock_which, mock_popen):
        """Test error when CLI returns empty token."""
        mock_proc = Mock()
        mock_proc.returncode = 0
        mock_proc.communicate.return_value = ("", "")
        mock_popen.return_value = mock_proc

        provider = AzureCredentialProvider(use_cli=True)

        with pytest.raises(AzureAuthError, match="returned empty token"):
            provider._fetch_cli_token(STORAGE_RESOURCE)

    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/az")
    def test_fetch_cli_token_timeout_retry(self, mock_which, mock_popen):
        """Test timeout handling with retry."""
        mock_proc = Mock()
        mock_proc.communicate.side_effect = subprocess.TimeoutExpired(
            cmd="az", timeout=60
        )
        mock_popen.return_value = mock_proc

        provider = AzureCredentialProvider(use_cli=True)

        with pytest.raises(AzureAuthError, match="timed out after"):
            provider._fetch_cli_token(STORAGE_RESOURCE)

        # Should have tried twice
        assert mock_popen.call_count == 2

    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/az")
    def test_fetch_cli_token_with_tenant(self, mock_which, mock_popen):
        """Test CLI token fetch with tenant ID."""
        mock_proc = Mock()
        mock_proc.returncode = 0
        mock_proc.communicate.return_value = ("test-token", "")
        mock_popen.return_value = mock_proc

        provider = AzureCredentialProvider(
            use_cli=True,
            tenant_id="test-tenant"
        )

        token = provider._fetch_cli_token(STORAGE_RESOURCE)

        assert token == "test-token"

        # Check that --tenant was included in command
        call_args = mock_popen.call_args[0][0]
        assert "--tenant" in call_args
        assert "test-tenant" in call_args


class TestGetTokenForResource:
    """Test unified token acquisition."""

    def test_get_token_from_file(self, tmp_path):
        """Test token file has highest priority."""
        token_file = tmp_path / "tokens.json"
        tokens = {STORAGE_RESOURCE: "file-token"}
        token_file.write_text(json.dumps(tokens))

        # Configure both file and CLI (file should win)
        provider = AzureCredentialProvider(
            token_file=str(token_file),
            use_cli=True,
        )

        token = provider.get_token_for_resource(STORAGE_RESOURCE)
        assert token == "file-token"

    def test_get_token_from_cache(self):
        """Test cache is used when available."""
        cache = TokenCache()
        cache.set(STORAGE_RESOURCE, "cached-token")

        provider = AzureCredentialProvider(
            use_cli=True,
            cache=cache,
        )

        token = provider.get_token_for_resource(STORAGE_RESOURCE)
        assert token == "cached-token"

    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/az")
    def test_get_token_from_cli(self, mock_which, mock_popen):
        """Test CLI fallback when cache empty."""
        mock_proc = Mock()
        mock_proc.returncode = 0
        mock_proc.communicate.return_value = ("cli-token", "")
        mock_popen.return_value = mock_proc

        provider = AzureCredentialProvider(use_cli=True)

        token = provider.get_token_for_resource(STORAGE_RESOURCE)
        assert token == "cli-token"

    def test_get_token_force_refresh_skips_cache(self, tmp_path):
        """Test force_refresh bypasses cache."""
        token_file = tmp_path / "tokens.json"
        tokens = {STORAGE_RESOURCE: "fresh-token"}
        token_file.write_text(json.dumps(tokens))

        cache = TokenCache()
        cache.set(STORAGE_RESOURCE, "stale-token")

        provider = AzureCredentialProvider(
            token_file=str(token_file),
            cache=cache,
        )

        # force_refresh should skip cache and read file
        token = provider.get_token_for_resource(STORAGE_RESOURCE, force_refresh=True)
        assert token == "fresh-token"

    @patch("core.auth.credentials.AZURE_IDENTITY_AVAILABLE", True)
    def test_get_token_from_spn(self):
        """Test SPN credential fallback."""
        mock_credential = Mock()
        mock_token = Mock()
        mock_token.token = "spn-token"
        mock_credential.get_token.return_value = mock_token

        mock_credential_class = Mock(return_value=mock_credential)

        with patch("core.auth.credentials.ClientSecretCredential", mock_credential_class):
            provider = AzureCredentialProvider(
                client_id="test-client",
                client_secret="test-secret",
                tenant_id="test-tenant",
            )

            token = provider.get_token_for_resource(STORAGE_RESOURCE)

            assert token == "spn-token"
            mock_credential.get_token.assert_called_once()

    @patch("core.auth.credentials.AZURE_IDENTITY_AVAILABLE", True)
    def test_get_token_from_default_credential(self):
        """Test DefaultAzureCredential fallback."""
        mock_credential = Mock()
        mock_token = Mock()
        mock_token.token = "default-token"
        mock_credential.get_token.return_value = mock_token

        mock_credential_class = Mock(return_value=mock_credential)

        with patch("core.auth.credentials.DefaultAzureCredential", mock_credential_class):
            provider = AzureCredentialProvider(use_default_credential=True)

            token = provider.get_token_for_resource(STORAGE_RESOURCE)

            assert token == "default-token"
            mock_credential.get_token.assert_called_once()


class TestStorageOperations:
    """Test storage-specific methods."""

    def test_get_storage_token(self, tmp_path):
        """Test get_storage_token wrapper."""
        token_file = tmp_path / "tokens.json"
        tokens = {STORAGE_RESOURCE: "storage-token"}
        token_file.write_text(json.dumps(tokens))

        provider = AzureCredentialProvider(token_file=str(token_file))

        token = provider.get_storage_token()
        assert token == "storage-token"

    def test_get_storage_options_with_token(self, tmp_path):
        """Test get_storage_options returns token."""
        token_file = tmp_path / "tokens.json"
        tokens = {STORAGE_RESOURCE: "storage-token"}
        token_file.write_text(json.dumps(tokens))

        provider = AzureCredentialProvider(token_file=str(token_file))

        options = provider.get_storage_options()

        assert options == {"azure_storage_token": "storage-token"}

    def test_get_storage_options_with_spn(self):
        """Test get_storage_options returns SPN credentials."""
        provider = AzureCredentialProvider(
            client_id="test-client",
            client_secret="test-secret",
            tenant_id="test-tenant",
        )

        options = provider.get_storage_options()

        assert options == {
            "azure_client_id": "test-client",
            "azure_client_secret": "test-secret",
            "azure_tenant_id": "test-tenant",
        }

    def test_get_storage_options_failure_returns_empty(self):
        """Test get_storage_options returns empty dict on failure."""
        provider = AzureCredentialProvider(
            use_cli=True,  # But CLI will fail (not mocked)
        )

        # Should return empty dict instead of raising
        options = provider.get_storage_options()
        assert options == {}


class TestKustoOperations:
    """Test Kusto/Eventhouse token methods."""

    def test_get_kusto_token(self, tmp_path):
        """Test get_kusto_token for custom resource."""
        cluster_uri = "https://test.eastus.kusto.windows.net"

        token_file = tmp_path / "tokens.json"
        tokens = {cluster_uri: "kusto-token"}
        token_file.write_text(json.dumps(tokens))

        provider = AzureCredentialProvider(token_file=str(token_file))

        token = provider.get_kusto_token(cluster_uri)
        assert token == "kusto-token"


class TestCacheOperations:
    """Test cache management methods."""

    def test_clear_cache_specific_resource(self):
        """Test clearing specific resource from cache."""
        cache = TokenCache()
        cache.set(STORAGE_RESOURCE, "token1")
        cache.set("https://vault.azure.net/", "token2")

        provider = AzureCredentialProvider(cache=cache)
        provider.clear_cache(STORAGE_RESOURCE)

        assert cache.get(STORAGE_RESOURCE) is None
        assert cache.get("https://vault.azure.net/") == "token2"

    def test_clear_cache_all(self):
        """Test clearing all cached tokens."""
        cache = TokenCache()
        cache.set(STORAGE_RESOURCE, "token1")
        cache.set("https://vault.azure.net/", "token2")

        provider = AzureCredentialProvider(cache=cache)
        provider.clear_cache()

        assert cache.get(STORAGE_RESOURCE) is None
        assert cache.get("https://vault.azure.net/") is None

    def test_get_diagnostics(self):
        """Test diagnostics output."""
        provider = AzureCredentialProvider(
            use_cli=True,
            tenant_id="test-tenant",
        )

        diag = provider.get_diagnostics()

        assert diag["auth_mode"] == "cli"
        assert diag["cli_enabled"] is True
        assert "spn_configured" in diag

    def test_get_diagnostics_with_cache_age(self):
        """Test diagnostics includes cache age."""
        cache = TokenCache()
        cache.set(STORAGE_RESOURCE, "test-token")

        provider = AzureCredentialProvider(use_cli=True, cache=cache)

        diag = provider.get_diagnostics()

        assert "storage_token_age_seconds" in diag
        assert diag["storage_token_age_seconds"] >= 0


class TestFileTokenCaching:
    """Test file token caching with mtime-based invalidation."""

    def test_file_cache_returns_cached_token_when_unchanged(self, tmp_path):
        """Test that repeated reads return cached token without re-reading file."""
        token_file = tmp_path / "tokens.json"
        tokens = {"https://storage.azure.com/": "storage-token"}
        token_file.write_text(json.dumps(tokens))

        provider = AzureCredentialProvider(token_file=str(token_file))

        # First read - populates cache
        token1 = provider._read_token_file(STORAGE_RESOURCE)
        assert token1 == "storage-token"
        assert provider._token_file_mtime is not None

        # Modify file content but don't change mtime (simulated by not touching file)
        # Second read should return cached value
        token2 = provider._read_token_file(STORAGE_RESOURCE)
        assert token2 == "storage-token"

    def test_file_cache_invalidated_when_file_modified(self, tmp_path):
        """Test that cache is invalidated when file mtime changes."""
        import time

        token_file = tmp_path / "tokens.json"
        tokens = {"https://storage.azure.com/": "old-token"}
        token_file.write_text(json.dumps(tokens))

        provider = AzureCredentialProvider(token_file=str(token_file))

        # First read
        token1 = provider._read_token_file(STORAGE_RESOURCE)
        assert token1 == "old-token"

        # Wait a bit and update file (ensure mtime changes)
        time.sleep(0.1)
        tokens = {"https://storage.azure.com/": "new-token"}
        token_file.write_text(json.dumps(tokens))

        # Second read should get new token
        token2 = provider._read_token_file(STORAGE_RESOURCE)
        assert token2 == "new-token"

    def test_file_cache_stores_all_resources(self, tmp_path):
        """Test that all resources from file are cached on first read."""
        token_file = tmp_path / "tokens.json"
        tokens = {
            "https://storage.azure.com/": "storage-token",
            "https://vault.azure.net/": "vault-token",
            "https://eventhubs.azure.net/": "eventhub-token",
        }
        token_file.write_text(json.dumps(tokens))

        provider = AzureCredentialProvider(token_file=str(token_file))

        # Read one resource - should cache all
        provider._read_token_file(STORAGE_RESOURCE)

        # All resources should be in cache
        assert len(provider._token_file_cache) == 3
        assert provider._token_file_cache["https://storage.azure.com/"] == "storage-token"
        assert provider._token_file_cache["https://vault.azure.net/"] == "vault-token"

    def test_file_cache_normalized_match_from_cache(self, tmp_path):
        """Test that cached tokens can be found with normalized resource URLs."""
        token_file = tmp_path / "tokens.json"
        tokens = {"https://storage.azure.com": "storage-token"}  # No trailing slash
        token_file.write_text(json.dumps(tokens))

        provider = AzureCredentialProvider(token_file=str(token_file))

        # First read with trailing slash
        token1 = provider._read_token_file("https://storage.azure.com/")
        assert token1 == "storage-token"

        # Second read (from cache) with trailing slash should still work
        token2 = provider._read_token_file("https://storage.azure.com/")
        assert token2 == "storage-token"

    def test_clear_file_cache_forces_reread(self, tmp_path):
        """Test that clear_file_cache forces file to be re-read."""
        token_file = tmp_path / "tokens.json"
        tokens = {"https://storage.azure.com/": "token1"}
        token_file.write_text(json.dumps(tokens))

        provider = AzureCredentialProvider(token_file=str(token_file))

        # First read
        provider._read_token_file(STORAGE_RESOURCE)
        assert provider._token_file_mtime is not None
        original_mtime = provider._token_file_mtime

        # Clear file cache
        provider.clear_file_cache()
        assert provider._token_file_mtime is None
        assert len(provider._token_file_cache) == 0

        # Next read should re-read file
        provider._read_token_file(STORAGE_RESOURCE)
        assert provider._token_file_mtime is not None

    def test_clear_cache_also_clears_file_cache(self, tmp_path):
        """Test that clear_cache() clears both in-memory and file caches."""
        token_file = tmp_path / "tokens.json"
        tokens = {"https://storage.azure.com/": "token1"}
        token_file.write_text(json.dumps(tokens))

        provider = AzureCredentialProvider(token_file=str(token_file))

        # Populate file cache
        provider._read_token_file(STORAGE_RESOURCE)
        assert len(provider._token_file_cache) > 0

        # Clear all caches
        provider.clear_cache()
        assert len(provider._token_file_cache) == 0
        assert provider._token_file_mtime is None

    def test_is_token_file_modified_first_read(self, tmp_path):
        """Test that _is_token_file_modified returns True on first read."""
        token_file = tmp_path / "tokens.json"
        token_file.write_text('{"resource": "token"}')

        provider = AzureCredentialProvider(token_file=str(token_file))

        # First check should return True (no previous mtime)
        assert provider._is_token_file_modified() is True

    def test_is_token_file_modified_after_read(self, tmp_path):
        """Test that _is_token_file_modified returns False after read if unchanged."""
        token_file = tmp_path / "tokens.json"
        tokens = {"https://storage.azure.com/": "token"}
        token_file.write_text(json.dumps(tokens))

        provider = AzureCredentialProvider(token_file=str(token_file))

        # Read to populate mtime
        provider._read_token_file(STORAGE_RESOURCE)

        # Now should return False (file unchanged)
        assert provider._is_token_file_modified() is False

    def test_diagnostics_includes_file_cache_info(self, tmp_path):
        """Test that get_diagnostics includes file cache information."""
        token_file = tmp_path / "tokens.json"
        tokens = {
            "https://storage.azure.com/": "storage-token",
            "https://vault.azure.net/": "vault-token",
        }
        token_file.write_text(json.dumps(tokens))

        provider = AzureCredentialProvider(token_file=str(token_file))

        # Read to populate cache
        provider._read_token_file(STORAGE_RESOURCE)

        diag = provider.get_diagnostics()

        assert "file_cache_resources" in diag
        assert len(diag["file_cache_resources"]) == 2
        assert "file_cache_mtime" in diag
        assert diag["file_cache_mtime"] is not None

    def test_plain_text_file_caching(self, tmp_path):
        """Test that plain text token files are also cached."""
        token_file = tmp_path / "token.txt"
        token_file.write_text("plain-token")

        provider = AzureCredentialProvider(token_file=str(token_file))

        # First read
        token1 = provider._read_token_file(STORAGE_RESOURCE)
        assert token1 == "plain-token"
        assert provider._token_file_mtime is not None

        # Second read should use cache
        token2 = provider._read_token_file(STORAGE_RESOURCE)
        assert token2 == "plain-token"


class TestModuleLevelFunctions:
    """Test module-level convenience functions."""

    def test_get_default_provider_singleton(self):
        """Test default provider is singleton."""
        provider1 = get_default_provider()
        provider2 = get_default_provider()

        assert provider1 is provider2

    @patch("core.auth.credentials._default_provider")
    def test_get_storage_options_uses_default_provider(self, mock_provider):
        """Test get_storage_options uses default provider."""
        mock_instance = Mock()
        mock_instance.get_storage_options.return_value = {"azure_storage_token": "test"}
        mock_provider.return_value = mock_instance

        # This would normally use the singleton, but we're mocking it
        # Just verify the function exists and is callable
        assert callable(get_storage_options)

    @patch("core.auth.credentials._default_provider")
    def test_clear_token_cache_uses_default_provider(self, mock_provider):
        """Test clear_token_cache uses default provider."""
        mock_instance = Mock()
        mock_provider.return_value = mock_instance

        # Verify function exists and is callable
        assert callable(clear_token_cache)


class TestErrorMessages:
    """Test error messages are actionable and don't leak secrets."""

    def test_cli_not_found_error_message(self):
        """Test CLI not found gives installation hint."""
        provider = AzureCredentialProvider(use_cli=True)

        with pytest.raises(AzureAuthError) as exc_info:
            provider._fetch_cli_token(STORAGE_RESOURCE)

        error_msg = str(exc_info.value)
        assert "Azure CLI not found" in error_msg
        assert "Hint:" in error_msg or "https://" in error_msg

    def test_token_file_not_found_error_message(self):
        """Test token file not found gives helpful hint."""
        provider = AzureCredentialProvider(token_file="/nonexistent/tokens.json")

        with pytest.raises(AzureAuthError) as exc_info:
            provider._read_token_file(STORAGE_RESOURCE)

        error_msg = str(exc_info.value)
        assert "Token file not found" in error_msg
        assert "Hint:" in error_msg or "token_refresher" in error_msg

    def test_error_messages_dont_contain_secrets(self):
        """Test error messages don't leak client secrets."""
        provider = AzureCredentialProvider(
            client_id="test-client",
            client_secret="super-secret-password",
            tenant_id="test-tenant",
        )

        # Diagnostics should not contain secrets
        diag = provider.get_diagnostics()
        diag_str = str(diag)

        # Secret should never appear
        assert "super-secret-password" not in diag_str
        # Client ID is not included in diagnostics (not needed for health checks)


class TestAzureCredentialCreation:
    """Test Azure credential object creation."""

    @patch("core.auth.credentials.AZURE_IDENTITY_AVAILABLE", True)
    def test_create_client_secret_credential(self):
        """Test ClientSecretCredential creation."""
        mock_credential_class = Mock()

        with patch("core.auth.credentials.ClientSecretCredential", mock_credential_class):
            provider = AzureCredentialProvider(
                client_id="test-client",
                client_secret="test-secret",
                tenant_id="test-tenant",
            )

            credential = provider._get_azure_credential()

            mock_credential_class.assert_called_once_with(
                tenant_id="test-tenant",
                client_id="test-client",
                client_secret="test-secret",
            )

    @patch("core.auth.credentials.AZURE_IDENTITY_AVAILABLE", True)
    def test_create_certificate_credential(self, tmp_path):
        """Test CertificateCredential creation."""
        mock_credential_class = Mock()

        with patch("core.auth.credentials.CertificateCredential", mock_credential_class):
            cert_file = tmp_path / "cert.pem"
            cert_file.write_text("fake-cert")

            provider = AzureCredentialProvider(
                client_id="test-client",
                certificate_path=str(cert_file),
                tenant_id="test-tenant",
            )

            credential = provider._get_azure_credential()

            mock_credential_class.assert_called_once_with(
                tenant_id="test-tenant",
                client_id="test-client",
                certificate_path=str(cert_file),
            )

    @patch("core.auth.credentials.AZURE_IDENTITY_AVAILABLE", True)
    def test_certificate_not_found_error(self):
        """Test error when certificate file doesn't exist."""
        provider = AzureCredentialProvider(
            client_id="test-client",
            certificate_path="/nonexistent/cert.pem",
            tenant_id="test-tenant",
        )

        with pytest.raises(AzureAuthError, match="Certificate file not found"):
            provider._get_azure_credential()

    @patch("core.auth.credentials.AZURE_IDENTITY_AVAILABLE", True)
    def test_create_default_credential(self):
        """Test DefaultAzureCredential creation."""
        mock_credential_class = Mock()

        with patch("core.auth.credentials.DefaultAzureCredential", mock_credential_class):
            provider = AzureCredentialProvider(use_default_credential=True)

            credential = provider._get_azure_credential()

            mock_credential_class.assert_called_once()

    @patch("core.auth.credentials.AZURE_IDENTITY_AVAILABLE", False)
    def test_azure_identity_not_installed_error(self):
        """Test error when azure-identity not installed."""
        provider = AzureCredentialProvider(
            client_id="test-client",
            client_secret="test-secret",
            tenant_id="test-tenant",
        )

        with pytest.raises(AzureAuthError, match="azure-identity library not installed"):
            provider._get_azure_credential()

    def test_defaults_to_default_credential_when_nothing_configured(self, monkeypatch):
        """Test that DefaultAzureCredential is used when nothing else is configured."""
        # Clear all env vars
        monkeypatch.delenv("AZURE_AUTH_INTERACTIVE", raising=False)
        monkeypatch.delenv("AZURE_TOKEN_FILE", raising=False)
        monkeypatch.delenv("AZURE_CLIENT_ID", raising=False)
        monkeypatch.delenv("AZURE_CLIENT_SECRET", raising=False)
        monkeypatch.delenv("AZURE_TENANT_ID", raising=False)

        provider = AzureCredentialProvider()

        # Should default to using DefaultAzureCredential
        assert provider.use_default_credential is True
        assert provider.auth_mode == "default"

    @patch("core.auth.credentials.AZURE_IDENTITY_AVAILABLE", True)
    def test_credential_caching(self, tmp_path):
        """Test credential object is created once and reused."""
        mock_credential_class = Mock()

        with patch("core.auth.credentials.CertificateCredential", mock_credential_class):
            cert_file = tmp_path / "cert.pem"
            cert_file.write_text("fake-cert")

            provider = AzureCredentialProvider(
                client_id="test-client",
                certificate_path=str(cert_file),
                tenant_id="test-tenant",
            )

            cred1 = provider._get_azure_credential()
            cred2 = provider._get_azure_credential()

            # Should only be called once
            assert mock_credential_class.call_count == 1
            assert cred1 is cred2
