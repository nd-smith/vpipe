"""
Tests for OneLake storage client.

Covers:
- parse_abfss_path parsing logic
- Error classification helpers
- Upload recursion guard
- TokenCredential and FileBackedTokenCredential
- TCPKeepAliveAdapter socket options
- WriteOperation idempotency
- OneLakeClient path helpers, pool stats, lifecycle
- OneLakeClient upload/download/exists/delete/properties operations
- Async wrapper methods
- Credential refresh on auth errors
"""

import socket
import threading
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from pipeline.common.storage.onelake import (
    OneLakeClient,
    WriteOperation,
    TokenCredential,
    FileBackedTokenCredential,
    TCPKeepAliveAdapter,
    parse_abfss_path,
    _is_auth_error,
    _classify_error,
    _is_in_upload,
    _set_in_upload,
    _refresh_all_credentials,
    _file_credential_registry,
    _register_file_credential,
    _clear_all_file_credentials,
    ONELAKE_RETRY_CONFIG,
    AUTH_ERROR_MARKERS,
)


# ---------------------------------------------------------------------------
# parse_abfss_path
# ---------------------------------------------------------------------------

class TestParseAbfssPath:

    def test_parses_valid_abfss_path(self):
        host, container, directory = parse_abfss_path(
            "abfss://mycontainer@myaccount.dfs.fabric.microsoft.com/path/to/files"
        )
        assert host == "myaccount.dfs.fabric.microsoft.com"
        assert container == "mycontainer"
        assert directory == "path/to/files"

    def test_parses_path_with_single_segment(self):
        host, container, directory = parse_abfss_path(
            "abfss://ws@acct.dfs.fabric.microsoft.com/Tables"
        )
        assert host == "acct.dfs.fabric.microsoft.com"
        assert container == "ws"
        assert directory == "Tables"

    def test_parses_path_with_trailing_slash(self):
        _, _, directory = parse_abfss_path(
            "abfss://ws@acct.dfs.fabric.microsoft.com/a/b/"
        )
        assert directory == "a/b/"

    def test_raises_on_non_abfss_scheme(self):
        with pytest.raises(ValueError, match="Expected abfss://"):
            parse_abfss_path("https://storage.blob.core.windows.net/container/path")

    def test_raises_on_missing_at_sign(self):
        with pytest.raises(ValueError, match="Invalid OneLake path format"):
            parse_abfss_path("abfss://noaccount.dfs.fabric.microsoft.com/path")

    def test_parses_empty_directory_path(self):
        host, container, directory = parse_abfss_path(
            "abfss://ws@acct.dfs.fabric.microsoft.com/"
        )
        assert directory == ""


# ---------------------------------------------------------------------------
# Error classification helpers
# ---------------------------------------------------------------------------

class TestIsAuthError:

    def test_detects_401_error(self):
        assert _is_auth_error(Exception("Server returned 401 Unauthorized"))

    def test_detects_unauthorized_keyword(self):
        assert _is_auth_error(Exception("unauthorized access"))

    def test_detects_authentication_keyword(self):
        assert _is_auth_error(Exception("authentication failed"))

    def test_detects_token_expired(self):
        assert _is_auth_error(Exception("token expired"))

    def test_returns_false_for_non_auth_error(self):
        assert not _is_auth_error(Exception("connection timeout"))


class TestClassifyError:

    def test_classifies_timeout_by_message(self):
        assert _classify_error(Exception("connection timed out")) == "timeout"

    def test_classifies_timeout_by_type_name(self):
        class TimeoutError(Exception):
            pass
        assert _classify_error(TimeoutError("oops")) == "timeout"

    def test_classifies_auth_error(self):
        assert _classify_error(Exception("401 unauthorized")) == "auth"

    def test_classifies_not_found_error(self):
        assert _classify_error(Exception("404 not found")) == "not_found"

    def test_classifies_unknown_error(self):
        assert _classify_error(Exception("something weird")) == "unknown"

    def test_timeout_takes_precedence_over_other_markers(self):
        # "timed out" matches timeout markers
        assert _classify_error(Exception("timed out")) == "timeout"

    def test_classifies_connection_aborted(self):
        assert _classify_error(Exception("connection aborted")) == "timeout"

    def test_classifies_does_not_exist(self):
        assert _classify_error(Exception("resource does not exist")) == "not_found"


# ---------------------------------------------------------------------------
# Upload recursion guard
# ---------------------------------------------------------------------------

class TestUploadRecursionGuard:

    def test_default_not_in_upload(self):
        # Reset thread-local state
        _set_in_upload(False)
        assert not _is_in_upload()

    def test_set_in_upload_true(self):
        _set_in_upload(True)
        assert _is_in_upload()
        _set_in_upload(False)

    def test_set_in_upload_false(self):
        _set_in_upload(True)
        _set_in_upload(False)
        assert not _is_in_upload()


# ---------------------------------------------------------------------------
# TokenCredential
# ---------------------------------------------------------------------------

class TestTokenCredential:

    def test_get_token_returns_access_token(self):
        cred = TokenCredential("my-token", expires_in_hours=2)
        result = cred.get_token("https://storage.azure.com/.default")
        assert result.token == "my-token"
        assert result.expires_on > 0

    def test_expiry_is_in_the_future(self):
        cred = TokenCredential("tok", expires_in_hours=1)
        result = cred.get_token()
        now_ts = int(datetime.now(UTC).timestamp())
        # Should expire roughly 1 hour from now (within 60 seconds tolerance)
        assert result.expires_on > now_ts
        assert result.expires_on < now_ts + 3700


# ---------------------------------------------------------------------------
# FileBackedTokenCredential
# ---------------------------------------------------------------------------

class TestFileBackedTokenCredential:

    def setup_method(self):
        # Clear the registry before each test
        _file_credential_registry.clear()

    @patch("pipeline.common.storage.onelake.get_auth")
    @patch("pipeline.common.storage.onelake.clear_token_cache")
    def test_registers_itself_on_init(self, mock_clear, mock_auth):
        cred = FileBackedTokenCredential()
        assert cred in _file_credential_registry

    @patch("pipeline.common.storage.onelake.get_auth")
    @patch("pipeline.common.storage.onelake.clear_token_cache")
    def test_should_refresh_when_no_cached_token(self, mock_clear, mock_auth):
        cred = FileBackedTokenCredential()
        assert cred._should_refresh()

    @patch("pipeline.common.storage.onelake.get_auth")
    @patch("pipeline.common.storage.onelake.clear_token_cache")
    def test_should_not_refresh_when_recently_acquired(self, mock_clear, mock_auth):
        cred = FileBackedTokenCredential()
        cred._cached_token = "tok"
        cred._token_acquired_at = datetime.now(UTC)
        assert not cred._should_refresh()

    @patch("pipeline.common.storage.onelake.get_auth")
    @patch("pipeline.common.storage.onelake.clear_token_cache")
    def test_should_refresh_when_token_is_old(self, mock_clear, mock_auth):
        cred = FileBackedTokenCredential()
        cred._cached_token = "tok"
        cred._token_acquired_at = datetime.now(UTC) - timedelta(minutes=6)
        assert cred._should_refresh()

    @patch("pipeline.common.storage.onelake.get_auth")
    @patch("pipeline.common.storage.onelake.clear_token_cache")
    def test_fetch_token_for_storage_resource(self, mock_clear, mock_auth):
        mock_auth_instance = MagicMock()
        mock_auth_instance.get_storage_token.return_value = "storage-token"
        mock_auth.return_value = mock_auth_instance

        cred = FileBackedTokenCredential(resource="https://storage.azure.com/")
        token = cred._fetch_token()

        assert token == "storage-token"
        assert cred._cached_token == "storage-token"
        assert cred._token_acquired_at is not None
        mock_auth_instance.get_storage_token.assert_called_once_with(force_refresh=True)

    @patch("pipeline.common.storage.onelake.get_auth")
    @patch("pipeline.common.storage.onelake.clear_token_cache")
    def test_fetch_token_for_kusto_resource(self, mock_clear, mock_auth):
        mock_auth_instance = MagicMock()
        mock_auth_instance.get_kusto_token.return_value = "kusto-token"
        mock_auth.return_value = mock_auth_instance

        cred = FileBackedTokenCredential(resource="https://kusto.fabric.microsoft.com/")
        token = cred._fetch_token()

        assert token == "kusto-token"
        mock_auth_instance.get_kusto_token.assert_called_once_with(
            "https://kusto.fabric.microsoft.com/", force_refresh=True
        )

    @patch("pipeline.common.storage.onelake.get_auth")
    @patch("pipeline.common.storage.onelake.clear_token_cache")
    def test_fetch_token_raises_on_empty_token(self, mock_clear, mock_auth):
        mock_auth_instance = MagicMock()
        mock_auth_instance.get_storage_token.return_value = None
        mock_auth.return_value = mock_auth_instance

        cred = FileBackedTokenCredential()
        with pytest.raises(RuntimeError, match="Failed to get token"):
            cred._fetch_token()

    @patch("pipeline.common.storage.onelake.get_auth")
    @patch("pipeline.common.storage.onelake.clear_token_cache")
    def test_get_token_triggers_refresh_when_needed(self, mock_clear, mock_auth):
        mock_auth_instance = MagicMock()
        mock_auth_instance.get_storage_token.return_value = "new-tok"
        mock_auth.return_value = mock_auth_instance

        cred = FileBackedTokenCredential()
        result = cred.get_token()

        assert result.token == "new-tok"
        assert result.expires_on > 0

    @patch("pipeline.common.storage.onelake.get_auth")
    @patch("pipeline.common.storage.onelake.clear_token_cache")
    def test_get_token_uses_cached_when_fresh(self, mock_clear, mock_auth):
        cred = FileBackedTokenCredential()
        cred._cached_token = "cached-tok"
        cred._token_acquired_at = datetime.now(UTC)

        result = cred.get_token()
        assert result.token == "cached-tok"
        # get_auth should not have been called for token fetch
        mock_auth.return_value.get_storage_token.assert_not_called()

    @patch("pipeline.common.storage.onelake.get_auth")
    @patch("pipeline.common.storage.onelake.clear_token_cache")
    def test_force_refresh_clears_and_fetches(self, mock_clear, mock_auth):
        mock_auth_instance = MagicMock()
        mock_auth_instance.get_storage_token.return_value = "refreshed"
        mock_auth.return_value = mock_auth_instance

        cred = FileBackedTokenCredential()
        cred._cached_token = "old"
        cred._token_acquired_at = datetime.now(UTC)

        cred.force_refresh()
        assert cred._cached_token == "refreshed"

    def test_close_is_noop(self):
        cred = FileBackedTokenCredential.__new__(FileBackedTokenCredential)
        # Should not raise
        cred.close()

    @patch("pipeline.common.storage.onelake.get_auth")
    @patch("pipeline.common.storage.onelake.clear_token_cache")
    def test_get_token_expires_on_without_acquired_at(self, mock_clear, mock_auth):
        """When _token_acquired_at is None after get_token, expires_on uses now()."""
        mock_auth_instance = MagicMock()
        mock_auth_instance.get_storage_token.return_value = "tok"
        mock_auth.return_value = mock_auth_instance

        cred = FileBackedTokenCredential()
        # Simulate _fetch_token setting cached_token but not _token_acquired_at
        cred._cached_token = "tok"
        cred._token_acquired_at = None

        result = cred.get_token()
        now_ts = int(datetime.now(UTC).timestamp())
        # Should be roughly 1 hour from now
        assert result.expires_on > now_ts


# ---------------------------------------------------------------------------
# Credential registry helpers
# ---------------------------------------------------------------------------

class TestCredentialRegistry:

    def setup_method(self):
        _file_credential_registry.clear()

    def test_register_file_credential(self):
        cred = MagicMock()
        _register_file_credential(cred)
        assert cred in _file_credential_registry

    def test_clear_all_file_credentials(self):
        cred1 = MagicMock()
        cred1._cached_token = "tok1"
        cred1._token_acquired_at = datetime.now(UTC)
        cred2 = MagicMock()
        cred2._cached_token = "tok2"
        cred2._token_acquired_at = datetime.now(UTC)

        _register_file_credential(cred1)
        _register_file_credential(cred2)

        _clear_all_file_credentials()

        assert cred1._cached_token is None
        assert cred1._token_acquired_at is None
        assert cred2._cached_token is None
        assert cred2._token_acquired_at is None

    @patch("pipeline.common.storage.onelake.clear_token_cache")
    def test_refresh_all_credentials(self, mock_clear):
        cred = MagicMock()
        cred._cached_token = "tok"
        _register_file_credential(cred)

        _refresh_all_credentials()

        mock_clear.assert_called_once()
        assert cred._cached_token is None


# ---------------------------------------------------------------------------
# TCPKeepAliveAdapter
# ---------------------------------------------------------------------------

class TestTCPKeepAliveAdapter:

    def test_init_poolmanager_adds_keepalive_options(self):
        adapter = TCPKeepAliveAdapter()
        # Call init_poolmanager with mocked super
        with patch.object(
            type(adapter).__mro__[1], "init_poolmanager", return_value=None
        ) as mock_super:
            adapter.init_poolmanager(1, 1)
            call_kwargs = mock_super.call_args[1]
            opts = call_kwargs["socket_options"]
            # Verify keepalive options are present
            assert (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1) in opts
            assert (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 120) in opts
            assert (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 30) in opts
            assert (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 8) in opts

    def test_init_poolmanager_preserves_existing_socket_options(self):
        adapter = TCPKeepAliveAdapter()
        existing_opts = [(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)]
        with patch.object(
            type(adapter).__mro__[1], "init_poolmanager", return_value=None
        ) as mock_super:
            adapter.init_poolmanager(1, 1, socket_options=existing_opts)
            call_kwargs = mock_super.call_args[1]
            opts = call_kwargs["socket_options"]
            # Existing option should still be there
            assert (socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) in opts
            # Plus the keepalive ones
            assert len(opts) == 5


# ---------------------------------------------------------------------------
# WriteOperation
# ---------------------------------------------------------------------------

class TestWriteOperation:

    def test_creates_write_operation(self):
        now = datetime.now(UTC)
        op = WriteOperation(
            token="abc-123",
            relative_path="claims/file.pdf",
            timestamp=now,
            bytes_written=1024,
        )
        assert op.token == "abc-123"
        assert op.relative_path == "claims/file.pdf"
        assert op.timestamp == now
        assert op.bytes_written == 1024


# ---------------------------------------------------------------------------
# OneLakeClient - construction and path helpers
# ---------------------------------------------------------------------------

VALID_BASE_PATH = "abfss://myws@myaccount.dfs.fabric.microsoft.com/lakehouse/Files"


class TestOneLakeClientInit:

    def test_parses_base_path_components(self):
        client = OneLakeClient(VALID_BASE_PATH)
        assert client.account_host == "myaccount.dfs.fabric.microsoft.com"
        assert client.container == "myws"
        assert client.base_directory == "lakehouse/Files"

    def test_strips_trailing_slash(self):
        client = OneLakeClient(VALID_BASE_PATH + "/")
        assert client.base_path == VALID_BASE_PATH

    def test_default_pool_size(self):
        client = OneLakeClient(VALID_BASE_PATH)
        assert client._max_pool_size == 16

    def test_custom_pool_size(self):
        client = OneLakeClient(VALID_BASE_PATH, max_pool_size=50)
        assert client._max_pool_size == 50

    def test_pool_stats_initialized(self):
        client = OneLakeClient(VALID_BASE_PATH)
        assert client._pool_stats["connections_created"] == 0
        assert client._pool_stats["requests_processed"] == 0
        assert client._pool_stats["errors_encountered"] == 0


class TestOneLakeClientPathHelpers:

    def test_full_path(self):
        client = OneLakeClient(VALID_BASE_PATH)
        result = client._full_path("claims/C-123/file.pdf")
        assert result == "lakehouse/Files/claims/C-123/file.pdf"

    def test_split_path_with_directory(self):
        client = OneLakeClient(VALID_BASE_PATH)
        directory, filename = client._split_path("a/b/c/file.pdf")
        assert directory == "a/b/c"
        assert filename == "file.pdf"

    def test_split_path_without_directory(self):
        client = OneLakeClient(VALID_BASE_PATH)
        directory, filename = client._split_path("file.pdf")
        assert directory == ""
        assert filename == "file.pdf"


# ---------------------------------------------------------------------------
# OneLakeClient - idempotency
# ---------------------------------------------------------------------------

class TestOneLakeClientIdempotency:

    def test_is_duplicate_returns_false_initially(self):
        client = OneLakeClient(VALID_BASE_PATH)
        assert not client._is_duplicate("token-1")

    def test_is_duplicate_returns_true_after_record(self):
        client = OneLakeClient(VALID_BASE_PATH)
        op = WriteOperation("token-1", "path", datetime.now(UTC), 100)
        client._record_token(op)
        assert client._is_duplicate("token-1")

    def test_record_token_stores_operation(self):
        client = OneLakeClient(VALID_BASE_PATH)
        op = WriteOperation("token-2", "some/path", datetime.now(UTC), 200)
        client._record_token(op)
        assert client._write_tokens["token-2"] is op


# ---------------------------------------------------------------------------
# OneLakeClient - pool stats
# ---------------------------------------------------------------------------

class TestOneLakeClientPoolStats:

    def test_track_request_success(self):
        client = OneLakeClient(VALID_BASE_PATH)
        client.track_request(success=True)
        assert client._pool_stats["requests_processed"] == 1
        assert client._pool_stats["errors_encountered"] == 0

    def test_track_request_failure(self):
        client = OneLakeClient(VALID_BASE_PATH)
        client.track_request(success=False)
        assert client._pool_stats["requests_processed"] == 1
        assert client._pool_stats["errors_encountered"] == 1

    def test_get_pool_stats_includes_uptime(self):
        client = OneLakeClient(VALID_BASE_PATH)
        stats = client.get_pool_stats()
        assert "uptime_seconds" in stats
        assert stats["uptime_seconds"] >= 0

    def test_reset_pool_stats(self):
        client = OneLakeClient(VALID_BASE_PATH)
        client.track_request(success=True)
        client.track_request(success=False)
        client.reset_pool_stats()

        assert client._pool_stats["requests_processed"] == 0
        assert client._pool_stats["errors_encountered"] == 0
        assert client._pool_stats["connections_created"] == 0

    def test_log_pool_health_no_requests(self):
        client = OneLakeClient(VALID_BASE_PATH)
        # Should not raise even with zero requests
        client.log_pool_health()

    def test_log_pool_health_with_errors(self):
        client = OneLakeClient(VALID_BASE_PATH)
        client.track_request(success=True)
        client.track_request(success=False)
        # Should not raise
        client.log_pool_health()


# ---------------------------------------------------------------------------
# OneLakeClient - close / lifecycle
# ---------------------------------------------------------------------------

class TestOneLakeClientLifecycle:

    def test_close_when_no_clients_created(self):
        client = OneLakeClient(VALID_BASE_PATH)
        # Should not raise
        client.close()

    def test_close_closes_service_client(self):
        client = OneLakeClient(VALID_BASE_PATH)
        mock_svc = MagicMock()
        client._service_client = mock_svc
        client.close()
        mock_svc.close.assert_called_once()
        assert client._service_client is None

    def test_close_closes_session(self):
        client = OneLakeClient(VALID_BASE_PATH)
        mock_session = MagicMock()
        client._session = mock_session
        client.close()
        mock_session.close.assert_called_once()
        assert client._session is None

    def test_close_closes_credential_with_close_method(self):
        client = OneLakeClient(VALID_BASE_PATH)
        mock_cred = MagicMock()
        client._credential = mock_cred
        client.close()
        mock_cred.close.assert_called_once()
        assert client._credential is None

    def test_close_handles_service_client_error(self):
        client = OneLakeClient(VALID_BASE_PATH)
        mock_svc = MagicMock()
        mock_svc.close.side_effect = Exception("close failed")
        client._service_client = mock_svc
        # Should not raise
        client.close()
        assert client._service_client is None

    def test_context_manager_enter_creates_clients(self):
        client = OneLakeClient(VALID_BASE_PATH)
        with patch.object(client, "_create_clients") as mock_create:
            result = client.__enter__()
            mock_create.assert_called_once_with(max_pool_size=client._max_pool_size)
            assert result is client

    def test_context_manager_exit_closes(self):
        client = OneLakeClient(VALID_BASE_PATH)
        with patch.object(client, "close") as mock_close:
            result = client.__exit__(None, None, None)
            mock_close.assert_called_once()
            assert result is False

    async def test_async_context_manager_enter(self):
        client = OneLakeClient(VALID_BASE_PATH)
        with patch.object(client, "_create_clients"):
            result = await client.__aenter__()
            assert result is client

    async def test_async_context_manager_exit(self):
        client = OneLakeClient(VALID_BASE_PATH)
        with patch.object(client, "close"):
            result = await client.__aexit__(None, None, None)
            assert result is False


# ---------------------------------------------------------------------------
# OneLakeClient - _ensure_client
# ---------------------------------------------------------------------------

class TestOneLakeClientEnsureClient:

    def test_ensure_client_creates_when_none(self):
        client = OneLakeClient(VALID_BASE_PATH)
        assert client._file_system_client is None
        with patch.object(client, "_create_clients") as mock_create:
            client._ensure_client()
            mock_create.assert_called_once()

    def test_ensure_client_skips_when_exists(self):
        client = OneLakeClient(VALID_BASE_PATH)
        client._file_system_client = MagicMock()
        with patch.object(client, "_create_clients") as mock_create:
            client._ensure_client()
            mock_create.assert_not_called()


# ---------------------------------------------------------------------------
# OneLakeClient - credential refresh
# ---------------------------------------------------------------------------

class TestOneLakeClientCredentialRefresh:

    def test_refresh_credential_with_file_backed(self):
        client = OneLakeClient(VALID_BASE_PATH)
        mock_file_cred = MagicMock()
        client._file_credential = mock_file_cred
        client._refresh_credential()
        mock_file_cred.force_refresh.assert_called_once()

    @patch("pipeline.common.storage.onelake.clear_token_cache")
    def test_refresh_credential_without_file_backed(self, mock_clear):
        client = OneLakeClient(VALID_BASE_PATH)
        client._file_credential = None
        client._refresh_credential()
        mock_clear.assert_called_once()

    def test_refresh_credential_handles_force_refresh_error(self):
        client = OneLakeClient(VALID_BASE_PATH)
        mock_file_cred = MagicMock()
        mock_file_cred.force_refresh.side_effect = Exception("refresh failed")
        client._file_credential = mock_file_cred
        # Should not raise
        client._refresh_credential()

    def test_refresh_client_recreates_clients(self):
        client = OneLakeClient(VALID_BASE_PATH)
        client._service_client = MagicMock()
        client._session = MagicMock()
        client._credential = MagicMock()
        client._file_credential = None

        with patch.object(client, "_create_clients") as mock_create:
            with patch("pipeline.common.storage.onelake.clear_token_cache"):
                client._refresh_client()
                mock_create.assert_called_once()

    def test_handle_auth_error_refreshes_on_auth(self):
        client = OneLakeClient(VALID_BASE_PATH)
        with patch.object(client, "_refresh_client") as mock_refresh:
            client._handle_auth_error(Exception("401 unauthorized"))
            mock_refresh.assert_called_once()

    def test_handle_auth_error_ignores_non_auth(self):
        client = OneLakeClient(VALID_BASE_PATH)
        with patch.object(client, "_refresh_client") as mock_refresh:
            client._handle_auth_error(Exception("connection timeout"))
            mock_refresh.assert_not_called()


# ---------------------------------------------------------------------------
# OneLakeClient - upload_bytes
# ---------------------------------------------------------------------------

class TestOneLakeClientUploadBytes:

    def _make_client_with_mock_fs(self):
        """Create a client with mocked file system client."""
        client = OneLakeClient(VALID_BASE_PATH)
        mock_fs = MagicMock()
        client._file_system_client = mock_fs
        return client, mock_fs

    @patch("pipeline.common.storage.onelake.with_retry", lambda **kw: lambda fn: fn)
    def test_upload_bytes_returns_full_path(self):
        client, mock_fs = self._make_client_with_mock_fs()
        _set_in_upload(False)

        result = client.upload_bytes("claims/file.txt", b"hello")

        assert result == f"{VALID_BASE_PATH}/claims/file.txt"

    @patch("pipeline.common.storage.onelake.with_retry", lambda **kw: lambda fn: fn)
    def test_upload_bytes_calls_upload_data(self):
        client, mock_fs = self._make_client_with_mock_fs()
        _set_in_upload(False)

        client.upload_bytes("claims/file.txt", b"hello", overwrite=True)

        mock_dir = mock_fs.get_directory_client.return_value
        mock_file = mock_dir.get_file_client.return_value
        mock_file.upload_data.assert_called_once_with(b"hello", overwrite=True)

    def test_upload_bytes_skips_during_recursive_upload(self):
        client, mock_fs = self._make_client_with_mock_fs()
        _set_in_upload(True)
        try:
            result = client.upload_bytes("claims/file.txt", b"hello")
            assert "claims/file.txt" in result
            # Should not have called upload_data
            mock_fs.get_directory_client.assert_not_called()
        finally:
            _set_in_upload(False)

    @patch("pipeline.common.storage.onelake.with_retry", lambda **kw: lambda fn: fn)
    def test_upload_bytes_resets_in_upload_flag_on_success(self):
        client, mock_fs = self._make_client_with_mock_fs()
        _set_in_upload(False)

        client.upload_bytes("file.txt", b"data")
        assert not _is_in_upload()

    @patch("pipeline.common.storage.onelake.with_retry", lambda **kw: lambda fn: fn)
    def test_upload_bytes_resets_in_upload_flag_on_error(self):
        client, mock_fs = self._make_client_with_mock_fs()
        _set_in_upload(False)
        mock_dir = mock_fs.get_directory_client.return_value
        mock_file = mock_dir.get_file_client.return_value
        mock_file.upload_data.side_effect = Exception("upload failed")

        with pytest.raises(Exception, match="upload failed"):
            client.upload_bytes("file.txt", b"data")

        assert not _is_in_upload()


# ---------------------------------------------------------------------------
# OneLakeClient - upload_bytes_with_idempotency
# ---------------------------------------------------------------------------

class TestOneLakeClientUploadBytesWithIdempotency:

    def test_skips_duplicate_upload(self):
        client = OneLakeClient(VALID_BASE_PATH)
        existing_op = WriteOperation("tok-1", "path", datetime.now(UTC), 100)
        client._write_tokens["tok-1"] = existing_op

        result = client.upload_bytes_with_idempotency(
            "path", b"data", operation_token="tok-1"
        )

        assert result.token == "tok-1"
        assert result.bytes_written == 0

    def test_generates_token_when_none(self):
        client = OneLakeClient(VALID_BASE_PATH)
        mock_fs = MagicMock()
        client._file_system_client = mock_fs
        _set_in_upload(False)

        result = client.upload_bytes_with_idempotency("path", b"data")

        assert result.token is not None
        assert len(result.token) > 0
        assert result.bytes_written == len(b"data")
        _set_in_upload(False)

    def test_records_token_after_upload(self):
        client = OneLakeClient(VALID_BASE_PATH)
        mock_fs = MagicMock()
        client._file_system_client = mock_fs
        _set_in_upload(False)

        result = client.upload_bytes_with_idempotency(
            "path", b"data", operation_token="new-tok"
        )

        assert "new-tok" in client._write_tokens
        assert result.bytes_written == 4
        _set_in_upload(False)


# ---------------------------------------------------------------------------
# OneLakeClient - download_bytes
# ---------------------------------------------------------------------------

class TestOneLakeClientDownloadBytes:

    def test_download_bytes_returns_content(self):
        client = OneLakeClient(VALID_BASE_PATH)
        mock_fs = MagicMock()
        client._file_system_client = mock_fs

        mock_download = MagicMock()
        mock_download.readall.return_value = b"file-content"
        mock_dir = mock_fs.get_directory_client.return_value
        mock_file = mock_dir.get_file_client.return_value
        mock_file.download_file.return_value = mock_download

        result = client.download_bytes("claims/file.txt")

        assert result == b"file-content"


# ---------------------------------------------------------------------------
# OneLakeClient - exists
# ---------------------------------------------------------------------------

class TestOneLakeClientExists:

    def test_exists_returns_true_when_file_exists(self):
        client = OneLakeClient(VALID_BASE_PATH)
        mock_fs = MagicMock()
        client._file_system_client = mock_fs

        result = client.exists("claims/file.txt")
        assert result is True

    def test_exists_returns_false_on_404(self):
        client = OneLakeClient(VALID_BASE_PATH)
        mock_fs = MagicMock()
        client._file_system_client = mock_fs

        mock_dir = mock_fs.get_directory_client.return_value
        mock_file = mock_dir.get_file_client.return_value
        mock_file.get_file_properties.side_effect = Exception("404 not found")

        result = client.exists("claims/missing.txt")
        assert result is False

    def test_exists_raises_on_non_404_error(self):
        client = OneLakeClient(VALID_BASE_PATH)
        mock_fs = MagicMock()
        client._file_system_client = mock_fs

        mock_dir = mock_fs.get_directory_client.return_value
        mock_file = mock_dir.get_file_client.return_value
        mock_file.get_file_properties.side_effect = Exception("500 server error")

        with pytest.raises(Exception, match="500 server error"):
            client.exists("claims/file.txt")


# ---------------------------------------------------------------------------
# OneLakeClient - delete
# ---------------------------------------------------------------------------

class TestOneLakeClientDelete:

    def test_delete_returns_true_on_success(self):
        client = OneLakeClient(VALID_BASE_PATH)
        mock_fs = MagicMock()
        client._file_system_client = mock_fs

        result = client.delete("claims/file.txt")
        assert result is True

    def test_delete_returns_false_on_404(self):
        client = OneLakeClient(VALID_BASE_PATH)
        mock_fs = MagicMock()
        client._file_system_client = mock_fs

        mock_dir = mock_fs.get_directory_client.return_value
        mock_file = mock_dir.get_file_client.return_value
        mock_file.delete_file.side_effect = Exception("404 not found")

        result = client.delete("claims/missing.txt")
        assert result is False

    def test_delete_raises_on_non_404_error(self):
        client = OneLakeClient(VALID_BASE_PATH)
        mock_fs = MagicMock()
        client._file_system_client = mock_fs

        mock_dir = mock_fs.get_directory_client.return_value
        mock_file = mock_dir.get_file_client.return_value
        mock_file.delete_file.side_effect = Exception("permission denied")

        with pytest.raises(Exception, match="permission denied"):
            client.delete("claims/file.txt")


# ---------------------------------------------------------------------------
# OneLakeClient - get_file_properties
# ---------------------------------------------------------------------------

class TestOneLakeClientGetFileProperties:

    def test_returns_properties_dict(self):
        client = OneLakeClient(VALID_BASE_PATH)
        mock_fs = MagicMock()
        client._file_system_client = mock_fs

        mock_props = MagicMock()
        mock_props.name = "file.txt"
        mock_props.size = 1024
        mock_props.creation_time = datetime(2024, 1, 1, tzinfo=UTC)
        mock_props.last_modified = datetime(2024, 6, 1, tzinfo=UTC)
        mock_props.content_settings.content_type = "text/plain"

        mock_dir = mock_fs.get_directory_client.return_value
        mock_file = mock_dir.get_file_client.return_value
        mock_file.get_file_properties.return_value = mock_props

        result = client.get_file_properties("file.txt")

        assert result["name"] == "file.txt"
        assert result["size"] == 1024
        assert result["content_type"] == "text/plain"

    def test_returns_none_on_404(self):
        client = OneLakeClient(VALID_BASE_PATH)
        mock_fs = MagicMock()
        client._file_system_client = mock_fs

        mock_dir = mock_fs.get_directory_client.return_value
        mock_file = mock_dir.get_file_client.return_value
        mock_file.get_file_properties.side_effect = Exception("404 not found")

        result = client.get_file_properties("missing.txt")
        assert result is None

    def test_raises_on_non_404_error(self):
        client = OneLakeClient(VALID_BASE_PATH)
        mock_fs = MagicMock()
        client._file_system_client = mock_fs

        mock_dir = mock_fs.get_directory_client.return_value
        mock_file = mock_dir.get_file_client.return_value
        mock_file.get_file_properties.side_effect = Exception("auth error 401")

        with pytest.raises(Exception, match="auth error 401"):
            client.get_file_properties("file.txt")


# ---------------------------------------------------------------------------
# OneLakeClient - upload_file
# ---------------------------------------------------------------------------

class TestOneLakeClientUploadFile:

    @patch("builtins.open", create=True)
    @patch("os.path.getsize", return_value=2048)
    def test_upload_file_returns_full_path(self, mock_size, mock_open):
        client = OneLakeClient(VALID_BASE_PATH)
        mock_fs = MagicMock()
        client._file_system_client = mock_fs
        _set_in_upload(False)

        result = client.upload_file("claims/doc.pdf", "/tmp/doc.pdf")

        assert result == f"{VALID_BASE_PATH}/claims/doc.pdf"
        _set_in_upload(False)

    def test_upload_file_skips_during_recursive_upload(self):
        client = OneLakeClient(VALID_BASE_PATH)
        mock_fs = MagicMock()
        client._file_system_client = mock_fs
        _set_in_upload(True)
        try:
            result = client.upload_file("claims/doc.pdf", "/tmp/doc.pdf")
            assert "claims/doc.pdf" in result
            mock_fs.get_directory_client.assert_not_called()
        finally:
            _set_in_upload(False)

    @patch("os.path.getsize", return_value=0)
    def test_upload_file_resets_flag_on_error(self, mock_size):
        client = OneLakeClient(VALID_BASE_PATH)
        mock_fs = MagicMock()
        client._file_system_client = mock_fs
        _set_in_upload(False)

        # Make the file client's upload_data raise to trigger the finally block
        mock_dir = mock_fs.get_directory_client.return_value
        mock_file = mock_dir.get_file_client.return_value
        mock_file.upload_data.side_effect = RuntimeError("upload failed")

        with pytest.raises(Exception):
            client.upload_file("file.txt", "/tmp/nonexistent.txt")

        assert not _is_in_upload()


# ---------------------------------------------------------------------------
# OneLakeClient - async wrappers
# ---------------------------------------------------------------------------

class TestOneLakeClientAsyncWrappers:

    async def test_async_upload_file(self):
        client = OneLakeClient(VALID_BASE_PATH)
        with patch.object(client, "upload_file", return_value="abfss://path") as mock_uf:
            result = await client.async_upload_file("rel", "/tmp/f.txt")
            assert result == "abfss://path"

    async def test_async_upload_file_with_pathlib(self):
        from pathlib import Path
        client = OneLakeClient(VALID_BASE_PATH)
        with patch.object(client, "upload_file", return_value="abfss://path") as mock_uf:
            result = await client.async_upload_file("rel", Path("/tmp/f.txt"))
            mock_uf.assert_called_once_with("rel", "/tmp/f.txt", True)

    async def test_async_upload_bytes(self):
        client = OneLakeClient(VALID_BASE_PATH)
        with patch.object(client, "upload_bytes", return_value="abfss://path") as mock_ub:
            result = await client.async_upload_bytes("rel", b"data")
            assert result == "abfss://path"

    async def test_async_exists(self):
        client = OneLakeClient(VALID_BASE_PATH)
        with patch.object(client, "exists", return_value=True) as mock_exists:
            result = await client.async_exists("rel/path")
            assert result is True

    async def test_async_delete(self):
        client = OneLakeClient(VALID_BASE_PATH)
        with patch.object(client, "delete", return_value=True) as mock_del:
            result = await client.async_delete("rel/path")
            assert result is True


# ---------------------------------------------------------------------------
# OneLakeClient - _create_clients
# ---------------------------------------------------------------------------

class TestOneLakeClientCreateClients:

    @patch("pipeline.common.storage.onelake.get_auth")
    @patch("pipeline.common.storage.onelake.DataLakeServiceClient")
    @patch("pipeline.common.storage.onelake.RequestsTransport")
    @patch("pipeline.common.storage.onelake.requests.Session")
    @patch("pipeline.common.storage.onelake.TCPKeepAliveAdapter")
    def test_creates_client_with_token_file_auth(
        self, mock_adapter, mock_session, mock_transport, mock_dls, mock_auth
    ):
        auth_instance = MagicMock()
        auth_instance.token_file = "/tmp/token.txt"
        auth_instance.STORAGE_RESOURCE = "https://storage.azure.com/"
        mock_auth.return_value = auth_instance

        with patch(
            "pipeline.common.storage.onelake.FileBackedTokenCredential"
        ) as mock_fbt:
            mock_fbt.return_value = MagicMock()
            client = OneLakeClient(VALID_BASE_PATH)
            client._create_clients()

            assert client._service_client is not None
            mock_fbt.assert_called_once_with(resource="https://storage.azure.com/")

    @patch("pipeline.common.storage.onelake.get_auth")
    @patch("pipeline.common.storage.onelake.DataLakeServiceClient")
    @patch("pipeline.common.storage.onelake.RequestsTransport")
    @patch("pipeline.common.storage.onelake.requests.Session")
    @patch("pipeline.common.storage.onelake.TCPKeepAliveAdapter")
    def test_creates_client_with_cli_auth(
        self, mock_adapter, mock_session, mock_transport, mock_dls, mock_auth
    ):
        auth_instance = MagicMock()
        auth_instance.token_file = None
        auth_instance.use_cli = True
        auth_instance.get_storage_token.return_value = "cli-token"
        mock_auth.return_value = auth_instance

        client = OneLakeClient(VALID_BASE_PATH)
        client._create_clients()

        assert client._service_client is not None

    @patch("pipeline.common.storage.onelake.get_auth")
    def test_raises_when_no_credentials(self, mock_auth):
        auth_instance = MagicMock()
        auth_instance.token_file = None
        auth_instance.use_cli = False
        auth_instance.has_spn_credentials = False
        mock_auth.return_value = auth_instance

        client = OneLakeClient(VALID_BASE_PATH)
        with pytest.raises(RuntimeError, match="No Azure credentials configured"):
            client._create_clients()

    @patch("pipeline.common.storage.onelake.get_auth")
    @patch("pipeline.common.storage.onelake.DataLakeServiceClient")
    @patch("pipeline.common.storage.onelake.RequestsTransport")
    @patch("pipeline.common.storage.onelake.requests.Session")
    @patch("pipeline.common.storage.onelake.TCPKeepAliveAdapter")
    def test_cli_auth_raises_on_no_token(
        self, mock_adapter, mock_session, mock_transport, mock_dls, mock_auth
    ):
        auth_instance = MagicMock()
        auth_instance.token_file = None
        auth_instance.use_cli = True
        auth_instance.has_spn_credentials = False
        auth_instance.get_storage_token.return_value = None
        mock_auth.return_value = auth_instance

        client = OneLakeClient(VALID_BASE_PATH)
        with pytest.raises(RuntimeError, match="Failed to get CLI storage token"):
            client._create_clients()

    @patch("pipeline.common.storage.onelake.get_auth")
    @patch("pipeline.common.storage.onelake.DataLakeServiceClient")
    @patch("pipeline.common.storage.onelake.RequestsTransport")
    @patch("pipeline.common.storage.onelake.requests.Session")
    @patch("pipeline.common.storage.onelake.TCPKeepAliveAdapter")
    def test_falls_back_to_cli_when_file_auth_fails(
        self, mock_adapter, mock_session, mock_transport, mock_dls, mock_auth
    ):
        auth_instance = MagicMock()
        auth_instance.token_file = "/tmp/token.txt"
        auth_instance.STORAGE_RESOURCE = "https://storage.azure.com/"
        auth_instance.use_cli = True
        auth_instance.get_storage_token.return_value = "cli-token"
        mock_auth.return_value = auth_instance

        with patch(
            "pipeline.common.storage.onelake.FileBackedTokenCredential",
            side_effect=Exception("file auth failed"),
        ):
            client = OneLakeClient(VALID_BASE_PATH)
            client._create_clients()
            # Should have fallen through to CLI auth
            assert client._service_client is not None


# ---------------------------------------------------------------------------
# ONELAKE_RETRY_CONFIG values
# ---------------------------------------------------------------------------

class TestOnelakeRetryConfig:

    def test_retry_config_values(self):
        assert ONELAKE_RETRY_CONFIG.max_attempts == 3
        assert ONELAKE_RETRY_CONFIG.base_delay == 1.0
        assert ONELAKE_RETRY_CONFIG.max_delay == 10.0
