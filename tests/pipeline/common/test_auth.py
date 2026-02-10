"""Tests for Azure authentication module."""

import json
import os
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock, patch

import pytest

from pipeline.common.auth import (
    AzureAuth,
    AzureAuthError,
    CachedToken,
    TokenCache,
    _mask_credential,
    clear_token_cache,
    get_auth,
    get_storage_options,
)


class TestCachedToken:
    def test_fresh_token_is_valid(self):
        token = CachedToken(value="abc", acquired_at=datetime.now(UTC))
        assert token.is_valid() is True

    def test_expired_token_is_invalid(self):
        old_time = datetime.now(UTC) - timedelta(minutes=55)
        token = CachedToken(value="abc", acquired_at=old_time)
        assert token.is_valid() is False

    def test_custom_buffer(self):
        recent = datetime.now(UTC) - timedelta(minutes=5)
        token = CachedToken(value="abc", acquired_at=recent)
        assert token.is_valid(buffer_mins=10) is True
        assert token.is_valid(buffer_mins=3) is False


class TestTokenCache:
    def test_get_returns_none_for_missing(self):
        cache = TokenCache()
        assert cache.get("https://storage.azure.com/") is None

    def test_set_and_get_valid_token(self):
        cache = TokenCache()
        cache.set("https://storage.azure.com/", "my-token")
        assert cache.get("https://storage.azure.com/") == "my-token"

    def test_get_returns_none_for_expired_token(self):
        cache = TokenCache()
        cache.set("https://storage.azure.com/", "old-token")
        # Manipulate the acquired_at to be old
        cache._tokens["https://storage.azure.com/"].acquired_at = datetime.now(UTC) - timedelta(
            minutes=55
        )
        assert cache.get("https://storage.azure.com/") is None

    def test_clear_specific_resource(self):
        cache = TokenCache()
        cache.set("res1", "token1")
        cache.set("res2", "token2")
        cache.clear("res1")
        assert cache.get("res1") is None
        assert cache.get("res2") == "token2"

    def test_clear_all(self):
        cache = TokenCache()
        cache.set("res1", "token1")
        cache.set("res2", "token2")
        cache.clear()
        assert cache.get("res1") is None
        assert cache.get("res2") is None

    def test_clear_nonexistent_resource_does_not_raise(self):
        cache = TokenCache()
        cache.clear("nonexistent")  # Should not raise


class TestAzureAuthMode:
    def test_mode_file_when_token_file_set(self, monkeypatch):
        monkeypatch.setenv("AZURE_TOKEN_FILE", "/tmp/token.txt")
        monkeypatch.delenv("AZURE_AUTH_INTERACTIVE", raising=False)
        monkeypatch.delenv("AZURE_CLIENT_ID", raising=False)
        auth = AzureAuth()
        assert auth.auth_mode == "file"

    def test_mode_cli_when_interactive(self, monkeypatch):
        monkeypatch.delenv("AZURE_TOKEN_FILE", raising=False)
        monkeypatch.setenv("AZURE_AUTH_INTERACTIVE", "true")
        monkeypatch.delenv("AZURE_CLIENT_ID", raising=False)
        auth = AzureAuth()
        assert auth.auth_mode == "cli"

    def test_mode_spn_when_credentials_set(self, monkeypatch):
        monkeypatch.delenv("AZURE_TOKEN_FILE", raising=False)
        monkeypatch.delenv("AZURE_AUTH_INTERACTIVE", raising=False)
        monkeypatch.setenv("AZURE_CLIENT_ID", "cid")
        monkeypatch.setenv("AZURE_CLIENT_SECRET", "cs")
        monkeypatch.setenv("AZURE_TENANT_ID", "tid")
        auth = AzureAuth()
        assert auth.auth_mode == "spn"
        assert auth.has_spn_credentials is True

    def test_mode_none_when_nothing_set(self, monkeypatch):
        monkeypatch.delenv("AZURE_TOKEN_FILE", raising=False)
        monkeypatch.delenv("AZURE_AUTH_INTERACTIVE", raising=False)
        monkeypatch.delenv("AZURE_CLIENT_ID", raising=False)
        monkeypatch.delenv("AZURE_CLIENT_SECRET", raising=False)
        monkeypatch.delenv("AZURE_TENANT_ID", raising=False)
        auth = AzureAuth()
        assert auth.auth_mode == "none"
        assert auth.has_spn_credentials is False

    def test_file_mode_takes_priority_over_cli(self, monkeypatch):
        monkeypatch.setenv("AZURE_TOKEN_FILE", "/tmp/token.txt")
        monkeypatch.setenv("AZURE_AUTH_INTERACTIVE", "true")
        auth = AzureAuth()
        assert auth.auth_mode == "file"


class TestAzureAuthReadTokenFile:
    def test_reads_plain_text_token(self, tmp_path):
        token_file = tmp_path / "token.txt"
        token_file.write_text("my-plain-token")

        auth = AzureAuth()
        auth.token_file = str(token_file)
        result = auth._read_token_file()
        assert result == "my-plain-token"

    def test_reads_json_token_exact_match(self, tmp_path):
        token_file = tmp_path / "token.json"
        tokens = {"https://storage.azure.com/": "storage-token"}
        token_file.write_text(json.dumps(tokens))

        auth = AzureAuth()
        auth.token_file = str(token_file)
        result = auth._read_token_file(resource="https://storage.azure.com/")
        assert result == "storage-token"

    def test_reads_json_token_normalized_match(self, tmp_path):
        token_file = tmp_path / "token.json"
        tokens = {"https://storage.azure.com": "storage-token"}
        token_file.write_text(json.dumps(tokens))

        auth = AzureAuth()
        auth.token_file = str(token_file)
        result = auth._read_token_file(resource="https://storage.azure.com/")
        assert result == "storage-token"

    def test_raises_when_resource_not_in_json(self, tmp_path):
        token_file = tmp_path / "token.json"
        tokens = {"https://other.com/": "other-token"}
        token_file.write_text(json.dumps(tokens))

        auth = AzureAuth()
        auth.token_file = str(token_file)
        with pytest.raises(AzureAuthError, match="not found in token file"):
            auth._read_token_file(resource="https://storage.azure.com/")

    def test_raises_when_file_empty(self, tmp_path):
        token_file = tmp_path / "token.txt"
        token_file.write_text("")

        auth = AzureAuth()
        auth.token_file = str(token_file)
        with pytest.raises(AzureAuthError, match="empty"):
            auth._read_token_file()

    def test_raises_when_file_not_found(self):
        auth = AzureAuth()
        auth.token_file = "/nonexistent/token.txt"
        with pytest.raises(AzureAuthError, match="not found"):
            auth._read_token_file()

    def test_raises_when_token_file_not_set(self):
        auth = AzureAuth()
        auth.token_file = None
        with pytest.raises(AzureAuthError, match="AZURE_TOKEN_FILE not set"):
            auth._read_token_file()

    def test_reads_utf8_sig_encoded_file(self, tmp_path):
        token_file = tmp_path / "token.txt"
        token_file.write_bytes(b"\xef\xbb\xbfmy-token")  # BOM + content

        auth = AzureAuth()
        auth.token_file = str(token_file)
        result = auth._read_token_file()
        assert result == "my-token"

    def test_json_without_explicit_resource_uses_storage_default(self, tmp_path):
        token_file = tmp_path / "token.json"
        tokens = {"https://storage.azure.com/": "default-storage-token"}
        token_file.write_text(json.dumps(tokens))

        auth = AzureAuth()
        auth.token_file = str(token_file)
        result = auth._read_token_file()  # No resource arg
        assert result == "default-storage-token"


class TestAzureAuthTokenFileModified:
    def test_first_check_always_returns_true(self, tmp_path):
        token_file = tmp_path / "token.txt"
        token_file.write_text("token")

        auth = AzureAuth()
        auth.token_file = str(token_file)
        auth._token_file_mtime = None
        assert auth._is_token_file_modified() is True

    def test_returns_false_when_unmodified(self, tmp_path):
        token_file = tmp_path / "token.txt"
        token_file.write_text("token")

        auth = AzureAuth()
        auth.token_file = str(token_file)
        auth._update_token_file_mtime()
        assert auth._is_token_file_modified() is False

    def test_returns_true_when_no_token_file(self):
        auth = AzureAuth()
        auth.token_file = None
        assert auth._is_token_file_modified() is False

    def test_returns_true_when_file_gone(self):
        auth = AzureAuth()
        auth.token_file = "/nonexistent/path"
        auth._token_file_mtime = 100.0
        assert auth._is_token_file_modified() is True


class TestAzureAuthFetchCliToken:
    def test_fetches_token_from_cli(self):
        auth = AzureAuth()
        auth.tenant_id = None

        with (
            patch("pipeline.common.auth.shutil.which", return_value="/usr/bin/az"),
            patch("pipeline.common.auth.subprocess.run") as mock_run,
        ):
            mock_run.return_value = Mock(returncode=0, stdout="cli-token\n", stderr="")
            result = auth._fetch_cli_token("https://storage.azure.com/")

        assert result == "cli-token"

    def test_includes_tenant_when_set(self):
        auth = AzureAuth()
        auth.tenant_id = "my-tenant"

        with (
            patch("pipeline.common.auth.shutil.which", return_value="/usr/bin/az"),
            patch("pipeline.common.auth.subprocess.run") as mock_run,
        ):
            mock_run.return_value = Mock(returncode=0, stdout="token\n", stderr="")
            auth._fetch_cli_token("https://storage.azure.com/")

        cmd = mock_run.call_args[0][0]
        assert "--tenant" in cmd
        assert "my-tenant" in cmd

    def test_raises_when_az_not_found(self):
        auth = AzureAuth()
        with (
            patch("pipeline.common.auth.shutil.which", return_value=None),
            pytest.raises(AzureAuthError, match="Azure CLI not found"),
        ):
            auth._fetch_cli_token("https://storage.azure.com/")

    def test_raises_on_cli_failure(self):
        auth = AzureAuth()
        with (
            patch("pipeline.common.auth.shutil.which", return_value="/usr/bin/az"),
            patch("pipeline.common.auth.subprocess.run") as mock_run,
        ):
            mock_run.return_value = Mock(returncode=1, stdout="", stderr="AADSTS something failed")
            with pytest.raises(AzureAuthError, match="Azure CLI failed"):
                auth._fetch_cli_token("https://storage.azure.com/")

    def test_raises_on_expired_session(self):
        auth = AzureAuth()
        with (
            patch("pipeline.common.auth.shutil.which", return_value="/usr/bin/az"),
            patch("pipeline.common.auth.subprocess.run") as mock_run,
        ):
            mock_run.return_value = Mock(returncode=1, stdout="", stderr="Please run 'az login'")
            with pytest.raises(AzureAuthError, match="az login"):
                auth._fetch_cli_token("https://storage.azure.com/")

    def test_raises_on_empty_token(self):
        auth = AzureAuth()
        with (
            patch("pipeline.common.auth.shutil.which", return_value="/usr/bin/az"),
            patch("pipeline.common.auth.subprocess.run") as mock_run,
        ):
            mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
            with pytest.raises(AzureAuthError, match="empty token"):
                auth._fetch_cli_token("https://storage.azure.com/")

    def test_raises_on_timeout(self):
        auth = AzureAuth()
        import subprocess

        with (
            patch("pipeline.common.auth.shutil.which", return_value="/usr/bin/az"),
            patch("pipeline.common.auth.subprocess.run") as mock_run,
        ):
            mock_run.side_effect = subprocess.TimeoutExpired(cmd="az", timeout=60)
            with pytest.raises(AzureAuthError, match="timed out"):
                auth._fetch_cli_token("https://storage.azure.com/")


class TestAzureAuthGetStorageToken:
    def test_returns_cached_token_in_file_mode(self, tmp_path, monkeypatch):
        token_file = tmp_path / "token.txt"
        token_file.write_text("file-token")

        monkeypatch.setenv("AZURE_TOKEN_FILE", str(token_file))
        monkeypatch.delenv("AZURE_AUTH_INTERACTIVE", raising=False)
        monkeypatch.delenv("AZURE_CLIENT_ID", raising=False)

        cache = TokenCache()
        auth = AzureAuth(cache=cache)

        # First call reads from file
        token1 = auth.get_storage_token()
        assert token1 == "file-token"

        # Second call uses cache
        token_file.write_text("new-token")
        token2 = auth.get_storage_token()
        assert token2 == "file-token"  # Still cached

    def test_force_refresh_rereads_file(self, tmp_path, monkeypatch):
        token_file = tmp_path / "token.txt"
        token_file.write_text("token-v1")

        monkeypatch.setenv("AZURE_TOKEN_FILE", str(token_file))
        monkeypatch.delenv("AZURE_AUTH_INTERACTIVE", raising=False)
        monkeypatch.delenv("AZURE_CLIENT_ID", raising=False)

        cache = TokenCache()
        auth = AzureAuth(cache=cache)

        token1 = auth.get_storage_token()
        assert token1 == "token-v1"

        token_file.write_text("token-v2")
        token2 = auth.get_storage_token(force_refresh=True)
        assert token2 == "token-v2"

    def test_returns_none_in_spn_mode(self, monkeypatch):
        monkeypatch.delenv("AZURE_TOKEN_FILE", raising=False)
        monkeypatch.delenv("AZURE_AUTH_INTERACTIVE", raising=False)
        monkeypatch.setenv("AZURE_CLIENT_ID", "cid")
        monkeypatch.setenv("AZURE_CLIENT_SECRET", "cs")
        monkeypatch.setenv("AZURE_TENANT_ID", "tid")

        auth = AzureAuth()
        assert auth.get_storage_token() is None

    def test_returns_none_when_no_auth(self, monkeypatch):
        monkeypatch.delenv("AZURE_TOKEN_FILE", raising=False)
        monkeypatch.delenv("AZURE_AUTH_INTERACTIVE", raising=False)
        monkeypatch.delenv("AZURE_CLIENT_ID", raising=False)
        monkeypatch.delenv("AZURE_CLIENT_SECRET", raising=False)
        monkeypatch.delenv("AZURE_TENANT_ID", raising=False)

        auth = AzureAuth()
        assert auth.get_storage_token() is None

    def test_cli_mode_returns_token(self, monkeypatch):
        monkeypatch.delenv("AZURE_TOKEN_FILE", raising=False)
        monkeypatch.setenv("AZURE_AUTH_INTERACTIVE", "true")
        monkeypatch.delenv("AZURE_CLIENT_ID", raising=False)

        auth = AzureAuth()
        with patch.object(auth, "_fetch_cli_token", return_value="cli-token"):
            token = auth.get_storage_token()
        assert token == "cli-token"

    def test_cli_mode_returns_none_on_failure(self, monkeypatch):
        monkeypatch.delenv("AZURE_TOKEN_FILE", raising=False)
        monkeypatch.setenv("AZURE_AUTH_INTERACTIVE", "true")
        monkeypatch.delenv("AZURE_CLIENT_ID", raising=False)

        auth = AzureAuth()
        with patch.object(auth, "_fetch_cli_token", side_effect=AzureAuthError("CLI failed")):
            token = auth.get_storage_token()
        assert token is None

    def test_file_mode_returns_none_on_read_error(self, monkeypatch):
        monkeypatch.setenv("AZURE_TOKEN_FILE", "/nonexistent/token.txt")
        monkeypatch.delenv("AZURE_AUTH_INTERACTIVE", raising=False)
        monkeypatch.delenv("AZURE_CLIENT_ID", raising=False)

        auth = AzureAuth()
        token = auth.get_storage_token()
        assert token is None

    def test_file_modified_clears_cache(self, tmp_path, monkeypatch):
        token_file = tmp_path / "token.txt"
        token_file.write_text("token-v1")

        monkeypatch.setenv("AZURE_TOKEN_FILE", str(token_file))
        monkeypatch.delenv("AZURE_AUTH_INTERACTIVE", raising=False)
        monkeypatch.delenv("AZURE_CLIENT_ID", raising=False)

        cache = TokenCache()
        auth = AzureAuth(cache=cache)

        auth.get_storage_token()
        auth._update_token_file_mtime()

        # Write new content and push mtime forward so the change is detected
        token_file.write_text("token-v2")
        future_mtime = auth._token_file_mtime + 10
        os.utime(str(token_file), (future_mtime, future_mtime))

        token = auth.get_storage_token()
        assert token == "token-v2"


class TestAzureAuthGetStorageOptions:
    def test_returns_token_in_file_mode(self, tmp_path, monkeypatch):
        token_file = tmp_path / "token.txt"
        token_file.write_text("my-token")

        monkeypatch.setenv("AZURE_TOKEN_FILE", str(token_file))
        monkeypatch.delenv("AZURE_AUTH_INTERACTIVE", raising=False)
        monkeypatch.delenv("AZURE_CLIENT_ID", raising=False)

        auth = AzureAuth()
        opts = auth.get_storage_options()
        assert opts == {"azure_storage_token": "my-token"}

    def test_returns_spn_credentials(self, monkeypatch):
        monkeypatch.delenv("AZURE_TOKEN_FILE", raising=False)
        monkeypatch.delenv("AZURE_AUTH_INTERACTIVE", raising=False)
        monkeypatch.setenv("AZURE_CLIENT_ID", "cid")
        monkeypatch.setenv("AZURE_CLIENT_SECRET", "cs")
        monkeypatch.setenv("AZURE_TENANT_ID", "tid")

        auth = AzureAuth()
        opts = auth.get_storage_options()
        assert opts["azure_client_id"] == "cid"
        assert opts["azure_client_secret"] == "cs"
        assert opts["azure_tenant_id"] == "tid"

    def test_returns_empty_when_no_auth(self, monkeypatch):
        monkeypatch.delenv("AZURE_TOKEN_FILE", raising=False)
        monkeypatch.delenv("AZURE_AUTH_INTERACTIVE", raising=False)
        monkeypatch.delenv("AZURE_CLIENT_ID", raising=False)
        monkeypatch.delenv("AZURE_CLIENT_SECRET", raising=False)
        monkeypatch.delenv("AZURE_TENANT_ID", raising=False)

        auth = AzureAuth()
        opts = auth.get_storage_options()
        assert opts == {}

    def test_returns_empty_when_file_read_fails(self, monkeypatch):
        monkeypatch.setenv("AZURE_TOKEN_FILE", "/nonexistent/token.txt")
        monkeypatch.delenv("AZURE_AUTH_INTERACTIVE", raising=False)
        monkeypatch.delenv("AZURE_CLIENT_ID", raising=False)

        auth = AzureAuth()
        opts = auth.get_storage_options()
        assert opts == {}

    def test_cli_mode_returns_token_option(self, monkeypatch):
        monkeypatch.delenv("AZURE_TOKEN_FILE", raising=False)
        monkeypatch.setenv("AZURE_AUTH_INTERACTIVE", "true")
        monkeypatch.delenv("AZURE_CLIENT_ID", raising=False)

        auth = AzureAuth()
        with patch.object(auth, "_fetch_cli_token", return_value="cli-tok"):
            opts = auth.get_storage_options()
        assert opts == {"azure_storage_token": "cli-tok"}


class TestAzureAuthClearCache:
    def test_clear_cache_delegates_to_token_cache(self):
        cache = TokenCache()
        cache.set("https://storage.azure.com/", "token")
        auth = AzureAuth(cache=cache)

        auth.clear_cache()
        assert cache.get("https://storage.azure.com/") is None

    def test_clear_cache_specific_resource(self):
        cache = TokenCache()
        cache.set("res1", "token1")
        cache.set("res2", "token2")
        auth = AzureAuth(cache=cache)

        auth.clear_cache("res1")
        assert cache.get("res1") is None
        assert cache.get("res2") == "token2"


class TestMaskCredential:
    def test_masks_long_value(self):
        result = _mask_credential("abcdefghij")
        assert result == "abcd...(10 chars)"

    def test_masks_short_value(self):
        result = _mask_credential("abc")
        assert result == "***"

    def test_returns_not_set_for_none(self):
        result = _mask_credential(None)
        assert result == "<not_set>"

    def test_returns_not_set_for_empty(self):
        result = _mask_credential("")
        assert result == "<not_set>"

    def test_custom_visible_chars(self):
        result = _mask_credential("abcdefgh", visible_chars=6)
        assert result == "abcdef...(8 chars)"


class TestGetAuthSingleton:
    def test_returns_same_instance(self, monkeypatch):
        import pipeline.common.auth as auth_module

        monkeypatch.delenv("AZURE_TOKEN_FILE", raising=False)
        monkeypatch.delenv("AZURE_AUTH_INTERACTIVE", raising=False)
        monkeypatch.delenv("AZURE_CLIENT_ID", raising=False)
        monkeypatch.delenv("AZURE_CLIENT_SECRET", raising=False)
        monkeypatch.delenv("AZURE_TENANT_ID", raising=False)

        # Reset singleton
        auth_module._auth_instance = None
        try:
            a1 = get_auth()
            a2 = get_auth()
            assert a1 is a2
        finally:
            auth_module._auth_instance = None


class TestGetStorageOptionsFunction:
    def test_delegates_to_auth_instance(self, monkeypatch):
        import pipeline.common.auth as auth_module

        monkeypatch.delenv("AZURE_TOKEN_FILE", raising=False)
        monkeypatch.delenv("AZURE_AUTH_INTERACTIVE", raising=False)
        monkeypatch.setenv("AZURE_CLIENT_ID", "cid")
        monkeypatch.setenv("AZURE_CLIENT_SECRET", "cs")
        monkeypatch.setenv("AZURE_TENANT_ID", "tid")

        auth_module._auth_instance = None
        try:
            opts = get_storage_options()
            assert opts["azure_client_id"] == "cid"
        finally:
            auth_module._auth_instance = None


class TestClearTokenCacheFunction:
    def test_clears_via_singleton(self, monkeypatch):
        import pipeline.common.auth as auth_module

        monkeypatch.delenv("AZURE_TOKEN_FILE", raising=False)
        monkeypatch.delenv("AZURE_AUTH_INTERACTIVE", raising=False)
        monkeypatch.delenv("AZURE_CLIENT_ID", raising=False)
        monkeypatch.delenv("AZURE_CLIENT_SECRET", raising=False)
        monkeypatch.delenv("AZURE_TENANT_ID", raising=False)

        auth_module._auth_instance = None
        try:
            clear_token_cache()  # Should not raise
        finally:
            auth_module._auth_instance = None
