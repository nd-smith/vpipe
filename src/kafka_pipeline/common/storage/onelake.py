"""
OneLake file storage operations using Azure Data Lake Storage Gen2 API.

OneLake requires the DFS (Data Lake Storage) API, not the Blob API.

Migrated from verisk_pipeline.storage.onelake for kafka_pipeline reorganization (REORG-502).
"""

import logging
import os
import socket
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

import requests
from azure.core.credentials import AccessToken
from azure.core.pipeline.transport import RequestsTransport
from azure.storage.filedatalake import DataLakeServiceClient  # type: ignore
from requests.adapters import HTTPAdapter

from kafka_pipeline.common.auth import get_auth, clear_token_cache
from kafka_pipeline.common.logging import LoggedClass, logged_operation
from kafka_pipeline.common.retry import RetryConfig, with_retry

# Retry config for OneLake operations
ONELAKE_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=1.0,
    max_delay=10.0,
)

# Connection timeout (5 minutes for slow networks and large uploads)
CONNECTION_TIMEOUT = 300

# Auth error markers for detection
AUTH_ERROR_MARKERS = ("401", "unauthorized", "authentication", "token expired")

# Error classification markers
TIMEOUT_ERROR_MARKERS = ("timeout", "timed out", "timedout", "connection aborted")
NOT_FOUND_ERROR_MARKERS = ("404", "not found", "notfound", "does not exist")


def _is_auth_error(error: Exception) -> bool:
    error_str = str(error).lower()
    return any(marker in error_str for marker in AUTH_ERROR_MARKERS)


def _classify_error(error: Exception) -> str:
    error_str = str(error).lower()
    error_type = type(error).__name__.lower()

    if any(marker in error_str for marker in TIMEOUT_ERROR_MARKERS):
        return "timeout"
    if "timeout" in error_type:
        return "timeout"

    if any(marker in error_str for marker in AUTH_ERROR_MARKERS):
        return "auth"

    if any(marker in error_str for marker in NOT_FOUND_ERROR_MARKERS):
        return "not_found"

    return "unknown"


@dataclass
class WriteOperation:
    """
    Idempotency token for OneLake write operations (Task G.3b).

    Tracks upload operations to prevent duplicate uploads during retries.
    Similar to delta.py WriteOperation but for OneLake file uploads.
    """

    token: str
    relative_path: str
    timestamp: datetime
    bytes_written: int


class TCPKeepAliveAdapter(HTTPAdapter):
    """HTTPAdapter with TCP keepalive to prevent Azure Load Balancer 4-minute idle timeout.

    Azure Load Balancer drops idle TCP connections after 4 minutes.
    This adapter configures TCP keepalive socket options to send periodic probes,
    preventing connection drops during long-running uploads.

    Configuration:
    - Start keepalive after 120s idle (before Azure 4-min timeout)
    - Send probe every 30s
    - Close connection after 8 failed probes (4 minutes total)
    """

    def init_poolmanager(self, *args, **kwargs):
        if "socket_options" not in kwargs:
            kwargs["socket_options"] = []

        kwargs["socket_options"].append((socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1))
        kwargs["socket_options"].append((socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 120))
        kwargs["socket_options"].append((socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 30))
        kwargs["socket_options"].append((socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 8))

        return super().init_poolmanager(*args, **kwargs)


class TokenCredential:
    """Simple credential wrapper for raw access token. Implements Azure SDK protocol."""

    def __init__(self, token: str, expires_in_hours: int = 1):
        self._token = token
        self._expires_on = int(
            (datetime.now(timezone.utc) + timedelta(hours=expires_in_hours)).timestamp()
        )

    def get_token(self, *scopes, **kwargs) -> AccessToken:
        return AccessToken(self._token, self._expires_on)


# Registry of FileBackedTokenCredential instances for coordinated refresh
_file_credential_registry: list = []


def _register_file_credential(credential: "FileBackedTokenCredential") -> None:
    _file_credential_registry.append(credential)


def _clear_all_file_credentials() -> None:
    for cred in _file_credential_registry:
        try:
            cred._cached_token = None
            cred._token_acquired_at = None
        except Exception:
            pass


def _refresh_all_credentials() -> None:
    """Clear all credential caches on auth error.

    Callback used by @with_retry(on_auth_error=...) decorators.
    Clears both AzureAuth's in-memory token cache (CLI auth) and
    all FileBackedTokenCredential instances (file-based auth).
    """
    clear_token_cache()
    _clear_all_file_credentials()


class FileBackedTokenCredential:
    """Credential that re-reads from token file when token is near expiry.

    Solves the problem where a long-running stage creates a TokenCredential
    once at startup, but the token expires after 60 minutes. This class
    automatically re-reads the token from the file (which token_refresher.py
    keeps updated) before the current token expires.

    Token refresh timeline:
    - Token refresher writes new token every 45 minutes
    - Azure tokens expire at 60 minutes
    - This class re-reads token every 5 minutes to stay fresh

    Supports both storage and Kusto resources through unified auth system.
    """

    TOKEN_REFRESH_MINUTES = 5

    def __init__(self, resource: str = "https://storage.azure.com/"):
        self._resource = resource
        self._cached_token: Optional[str] = None
        self._token_acquired_at: Optional[datetime] = None
        self._logger = logging.getLogger(__name__)
        _register_file_credential(self)

    def _should_refresh(self) -> bool:
        if self._cached_token is None or self._token_acquired_at is None:
            return True

        age = datetime.now(timezone.utc) - self._token_acquired_at
        return age > timedelta(minutes=self.TOKEN_REFRESH_MINUTES)

    def _fetch_token(self) -> str:
        auth = get_auth()
        clear_token_cache()

        # Use appropriate token method based on resource
        if "kusto" in self._resource.lower() or "fabric" in self._resource.lower():
            token = auth.get_kusto_token(self._resource, force_refresh=True)
        else:
            token = auth.get_storage_token(force_refresh=True)

        if not token:
            raise RuntimeError(f"Failed to get token for resource: {self._resource}")

        self._cached_token = token
        self._token_acquired_at = datetime.now(timezone.utc)

        self._logger.debug("FileBackedTokenCredential refreshed token for %s", self._resource)
        return token

    def get_token(self, *scopes, **kwargs) -> AccessToken:
        if self._should_refresh():
            self._fetch_token()

        if self._token_acquired_at:
            expires_on = int((self._token_acquired_at + timedelta(hours=1)).timestamp())
        else:
            expires_on = int((datetime.now(timezone.utc) + timedelta(hours=1)).timestamp())

        return AccessToken(self._cached_token, expires_on)

    def force_refresh(self) -> None:
        self._cached_token = None
        self._token_acquired_at = None
        self._fetch_token()

    def close(self) -> None:
        """Close the credential (no-op for file-backed credential)."""
        pass


def parse_abfss_path(path: str) -> Tuple[str, str, str]:
    """Parse abfss://container@account.dfs.fabric.microsoft.com/path/to/files into components."""
    parsed = urlparse(path)

    if parsed.scheme != "abfss":
        raise ValueError(f"Expected abfss:// scheme, got: {parsed.scheme}")

    if "@" not in parsed.netloc:
        raise ValueError(f"Invalid OneLake path format: {path}")

    container, account_host = parsed.netloc.split("@", 1)
    directory_path = parsed.path.lstrip("/")

    return account_host, container, directory_path


class OneLakeClient(LoggedClass):
    """Client for OneLake file operations with automatic operation logging.

    Uses Azure Data Lake Storage Gen2 API (DFS endpoint).
    Supports both Azure CLI and Service Principal authentication.
    """

    log_component = "onelake"

    def __init__(
        self,
        base_path: str,
        max_pool_size: Optional[int] = None,
        connection_timeout: int = CONNECTION_TIMEOUT,
        request_timeout: int = 300,
    ):
        self.base_path = base_path.rstrip("/")

        default_upload_max_concurrency = 16
        default_upload_block_size_mb = 4
        default_upload_max_single_put_mb = 64

        self._max_pool_size = (
            max_pool_size if max_pool_size is not None else default_upload_max_concurrency
        )
        self._upload_block_size_mb = default_upload_block_size_mb
        self._upload_max_single_put_mb = default_upload_max_single_put_mb

        self._connection_timeout = connection_timeout
        self._request_timeout = request_timeout

        self.account_host, self.container, self.base_directory = parse_abfss_path(base_path)

        self._service_client: Optional[DataLakeServiceClient] = None
        self._file_system_client = None
        self._session: Optional[requests.Session] = None

        self._write_tokens: Dict[str, WriteOperation] = {}

        from datetime import datetime, timezone
        import threading

        self._pool_stats = {
            "connections_created": 0,
            "requests_processed": 0,
            "errors_encountered": 0,
            "last_reset": datetime.now(timezone.utc),
        }
        self._pool_stats_lock = threading.Lock()

        super().__init__()

    def _create_clients(self, max_pool_size: int = 25) -> None:
        """Create or recreate clients with dynamic connection pool sizing (P2.4)."""
        cpu_cores = os.cpu_count() or 4
        max_concurrency = 16

        # Azure Storage limits: 500 concurrent connections per storage account
        calculated_size = min(cpu_cores * 10, max_concurrency, 250)
        actual_pool_size = max_pool_size if max_pool_size != 25 else calculated_size

        self._log(
            logging.INFO,
            "Connection pool sizing",
            cpu_cores=cpu_cores,
            max_concurrency=max_concurrency,
            calculated_size=calculated_size,
            actual_size=actual_pool_size,
        )

        auth = get_auth()

        if auth.token_file:
            try:
                credential = FileBackedTokenCredential(resource=auth.STORAGE_RESOURCE)
                self._file_credential = credential
                auth_mode = "file"
                self._log(
                    logging.INFO,
                    "Using FileBackedTokenCredential for auto-refresh",
                    token_file=auth.token_file,
                )
            except Exception as e:
                self._log(
                    logging.WARNING,
                    "Token file auth failed, trying other methods",
                    error=str(e)[:200],
                )
                credential = None
                auth_mode = None
                self._file_credential = None
        else:
            credential = None
            auth_mode = None
            self._file_credential = None

        if credential is None and auth.use_cli:
            token = auth.get_storage_token()
            if not token:
                raise RuntimeError("Failed to get CLI storage token")
            credential = TokenCredential(token)
            auth_mode = "cli"

        if credential is None and auth.has_spn_credentials:
            from azure.identity import ClientSecretCredential

            assert auth.tenant_id is not None
            assert auth.client_id is not None
            assert auth.client_secret is not None

            credential = ClientSecretCredential(
                tenant_id=auth.tenant_id,
                client_id=auth.client_id,
                client_secret=auth.client_secret,
            )
            auth_mode = "spn"

        if credential is None:
            raise RuntimeError(
                "No Azure credentials configured. "
                "Set AZURE_TOKEN_FILE for token file auth, "
                "Set AZURE_AUTH_INTERACTIVE=true for CLI auth, or "
                "set AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID for SPN auth."
            )

        account_url = f"https://{self.account_host}"

        adapter = TCPKeepAliveAdapter(
            pool_connections=actual_pool_size,
            pool_maxsize=actual_pool_size,
        )

        session = requests.Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        transport = RequestsTransport(
            session=session,
            session_owner=False,
        )

        self._service_client = DataLakeServiceClient(
            account_url=account_url,
            credential=credential,
            transport=transport,
            connection_timeout=self._connection_timeout,
        )
        self._file_system_client = self._service_client.get_file_system_client(self.container)

        self._session = session

        with self._pool_stats_lock:
            self._pool_stats["connections_created"] += 1

        self._log(
            logging.DEBUG,
            "Created OneLake client",
            account_host=self.account_host,
            container=self.container,
            pool_size=actual_pool_size,
            auth_mode=auth_mode,
        )

    def __enter__(self):
        self._create_clients(max_pool_size=self._max_pool_size)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    async def __aenter__(self):
        """Async context manager entry - create and initialize client."""
        import asyncio

        await asyncio.to_thread(self._create_clients, max_pool_size=self._max_pool_size)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - close client."""
        import asyncio

        await asyncio.to_thread(self.close)
        return False

    def close(self) -> None:
        if self._service_client is not None:
            try:
                self._service_client.close()
                self._log(logging.DEBUG, "Closed OneLake client")
            except Exception as e:
                self._log_exception(
                    e,
                    "Error closing OneLake client",
                    level=logging.WARNING,
                )
        if self._session is not None:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None
        self._service_client = None
        self._file_system_client = None

    def _ensure_client(self) -> None:
        if self._file_system_client is None:
            self._create_clients(max_pool_size=self._max_pool_size)

    def _refresh_credential(self) -> None:
        """Force refresh the credential (called on auth errors).

        For FileBackedTokenCredential: forces immediate re-read from token file.
        For other auth modes: clears token cache.
        """
        if hasattr(self, "_file_credential") and self._file_credential is not None:
            try:
                self._file_credential.force_refresh()
                self._log(
                    logging.INFO,
                    "FileBackedTokenCredential force refreshed",
                )
            except Exception as e:
                self._log(
                    logging.WARNING,
                    "Failed to force refresh FileBackedTokenCredential",
                    error=str(e)[:200],
                )
        else:
            clear_token_cache()

    def _refresh_client(self) -> None:
        self._refresh_credential()

        if self._service_client is not None:
            try:
                self._service_client.close()
                self._log(logging.DEBUG, "Closed OneLake client for refresh")
            except Exception as e:
                self._log_exception(
                    e,
                    "Error closing OneLake client during refresh",
                    level=logging.WARNING,
                )
        if self._session is not None:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None
        self._service_client = None
        self._file_system_client = None
        self._create_clients(max_pool_size=self._max_pool_size)
        self._log(logging.INFO, "OneLake client refreshed with new credentials")

    def _handle_auth_error(self, e: Exception) -> None:
        if _is_auth_error(e):
            self._log(
                logging.WARNING,
                "Auth error detected, refreshing OneLake client",
                error_message=str(e)[:200],
            )
            self._refresh_client()

    def _full_path(self, relative_path: str) -> str:
        return f"{self.base_directory}/{relative_path}"

    def _split_path(self, full_path: str) -> Tuple[str, str]:
        if "/" in full_path:
            directory = "/".join(full_path.split("/")[:-1])
            filename = full_path.split("/")[-1]
        else:
            directory = ""
            filename = full_path
        return directory, filename

    def _is_duplicate(self, operation_token: str) -> bool:
        """Check if write operation with this token was already completed (Task G.3b)."""
        return operation_token in self._write_tokens

    def _record_token(self, write_op: WriteOperation) -> None:
        """Record write operation token for idempotency tracking (Task G.3b)."""
        self._write_tokens[write_op.token] = write_op
        self._log(
            logging.DEBUG,
            "Recorded write operation token",
            token=write_op.token[:8],
            path=write_op.relative_path,
        )

    @logged_operation(level=logging.DEBUG)
    @with_retry(config=ONELAKE_RETRY_CONFIG, on_auth_error=_refresh_all_credentials)
    def upload_bytes(
        self,
        relative_path: str,
        data: bytes,
        overwrite: bool = True,
    ) -> str:
        self._ensure_client()

        full_path = self._full_path(relative_path)
        directory, filename = self._split_path(full_path)
        bytes_count = len(data)

        try:
            dir_client = self._file_system_client.get_directory_client(directory)  # type: ignore
            file_client = dir_client.get_file_client(filename)
            file_client.upload_data(data, overwrite=overwrite)

            result_path = f"{self.base_path}/{relative_path}"
            self._log(
                logging.DEBUG,
                "Upload complete",
                blob_path=relative_path,
                bytes_written=bytes_count,
            )
            return result_path

        except Exception as e:
            self._handle_auth_error(e)
            raise

    @logged_operation(level=logging.INFO)
    @with_retry(config=ONELAKE_RETRY_CONFIG, on_auth_error=_refresh_all_credentials)
    def upload_bytes_with_idempotency(
        self,
        relative_path: str,
        data: bytes,
        operation_token: Optional[str] = None,
        overwrite: bool = True,
    ) -> WriteOperation:
        """Upload bytes with idempotency token to prevent duplicate uploads (Task G.3b)."""
        if operation_token is None:
            operation_token = str(uuid.uuid4())

        if self._is_duplicate(operation_token):
            self._log(
                logging.INFO,
                "Skipping duplicate upload operation",
                token=operation_token[:8],
                path=relative_path,
            )
            existing_op = self._write_tokens[operation_token]
            return WriteOperation(
                token=operation_token,
                relative_path=relative_path,
                timestamp=existing_op.timestamp,
                bytes_written=0,
            )

        self.upload_bytes(relative_path, data, overwrite=overwrite)

        write_op = WriteOperation(
            token=operation_token,
            relative_path=relative_path,
            timestamp=datetime.now(timezone.utc),
            bytes_written=len(data),
        )
        self._record_token(write_op)

        return write_op

    @logged_operation(level=logging.DEBUG)
    @with_retry(config=ONELAKE_RETRY_CONFIG, on_auth_error=_refresh_all_credentials)
    def download_bytes(self, relative_path: str) -> bytes:
        self._ensure_client()

        full_path = self._full_path(relative_path)
        directory, filename = self._split_path(full_path)

        try:
            dir_client = self._file_system_client.get_directory_client(directory)  # type: ignore
            file_client = dir_client.get_file_client(filename)

            download = file_client.download_file()
            content = download.readall()

            bytes_count = len(content)

            self._log(
                logging.DEBUG,
                "Download complete",
                blob_path=relative_path,
                bytes_downloaded=bytes_count,
            )
            return content

        except Exception as e:
            self._handle_auth_error(e)
            raise

    @logged_operation(level=logging.DEBUG)
    @with_retry(config=ONELAKE_RETRY_CONFIG, on_auth_error=_refresh_all_credentials)
    def upload_file(
        self,
        relative_path: str,
        local_path: str,
        overwrite: bool = True,
    ) -> str:
        self._ensure_client()

        full_path = self._full_path(relative_path)
        directory, filename = self._split_path(full_path)

        file_size = os.path.getsize(local_path)

        try:
            dir_client = self._file_system_client.get_directory_client(directory)  # type: ignore
            file_client = dir_client.get_file_client(filename)

            with open(local_path, "rb") as f:
                file_client.upload_data(f, overwrite=overwrite)

            result_path = f"{self.base_path}/{relative_path}"
            self._log(
                logging.DEBUG,
                "Upload complete",
                blob_path=relative_path,
                bytes_written=file_size,
            )
            return result_path

        except Exception as e:
            self._handle_auth_error(e)
            raise

    @with_retry(config=ONELAKE_RETRY_CONFIG, on_auth_error=_refresh_all_credentials)
    def exists(self, relative_path: str) -> bool:
        self._ensure_client()

        full_path = self._full_path(relative_path)
        directory, filename = self._split_path(full_path)

        try:
            dir_client = self._file_system_client.get_directory_client(directory)  # type: ignore
            file_client = dir_client.get_file_client(filename)
            file_client.get_file_properties()

            return True
        except Exception as e:
            error_str = str(e).lower()
            if "404" in error_str or "not found" in error_str:
                self._log(logging.DEBUG, "File does not exist", blob_path=relative_path)
                return False

            self._handle_auth_error(e)
            raise

    @with_retry(config=ONELAKE_RETRY_CONFIG, on_auth_error=_refresh_all_credentials)
    def delete(self, relative_path: str) -> bool:
        self._ensure_client()

        full_path = self._full_path(relative_path)
        directory, filename = self._split_path(full_path)

        try:
            dir_client = self._file_system_client.get_directory_client(directory)  # type: ignore
            file_client = dir_client.get_file_client(filename)
            file_client.delete_file()

            self._log(logging.DEBUG, "Deleted file", blob_path=relative_path)
            return True
        except Exception as e:
            error_str = str(e).lower()
            if "404" in error_str or "not found" in error_str:
                return False

            self._handle_auth_error(e)
            raise

    @with_retry(config=ONELAKE_RETRY_CONFIG, on_auth_error=_refresh_all_credentials)
    def get_file_properties(self, relative_path: str) -> Optional[dict]:
        self._ensure_client()

        full_path = self._full_path(relative_path)
        directory, filename = self._split_path(full_path)

        try:
            dir_client = self._file_system_client.get_directory_client(directory)  # type: ignore
            file_client = dir_client.get_file_client(filename)
            props = file_client.get_file_properties()

            result = {
                "name": props.name,
                "size": props.size,
                "created_on": props.creation_time,
                "modified_on": props.last_modified,
                "content_type": props.content_settings.content_type,
            }
            self._log(
                logging.DEBUG,
                "Got file properties",
                blob_path=relative_path,
                size=props.size,
            )
            return result
        except Exception as e:
            error_str = str(e).lower()
            if "404" in error_str or "not found" in error_str:
                return None
            self._handle_auth_error(e)
            raise

    def track_request(self, success: bool = True) -> None:
        with self._pool_stats_lock:
            self._pool_stats["requests_processed"] += 1
            if not success:
                self._pool_stats["errors_encountered"] += 1

    def get_pool_stats(self) -> Dict[str, Any]:
        from datetime import datetime, timezone

        with self._pool_stats_lock:
            stats = self._pool_stats.copy()
            stats["uptime_seconds"] = (
                datetime.now(timezone.utc) - stats["last_reset"]
            ).total_seconds()
            return stats

    def reset_pool_stats(self) -> None:
        from datetime import datetime, timezone

        with self._pool_stats_lock:
            self._pool_stats = {
                "connections_created": 0,
                "requests_processed": 0,
                "errors_encountered": 0,
                "last_reset": datetime.now(timezone.utc),
            }

    def log_pool_health(self) -> None:
        stats = self.get_pool_stats()
        error_rate = (
            stats["errors_encountered"] / stats["requests_processed"] * 100
            if stats["requests_processed"] > 0
            else 0
        )

        self._log(
            logging.INFO,
            "OneLake connection pool health",
            connections_created=stats["connections_created"],
            requests_processed=stats["requests_processed"],
            errors_encountered=stats["errors_encountered"],
            error_rate_pct=round(error_rate, 2),
            uptime_seconds=round(stats["uptime_seconds"], 1),
        )

    # Async methods for use in async contexts
    async def async_upload_file(
        self,
        relative_path: str,
        local_path,  # str or Path
        overwrite: bool = True,
    ) -> str:
        """
        Upload file from local path to OneLake (async, non-blocking).

        Args:
            relative_path: Path relative to base_path (e.g. "claims/C-123/file.pdf")
            local_path: Local file path to upload (str or pathlib.Path)
            overwrite: Whether to overwrite existing file (default: True)

        Returns:
            Full abfss:// path to uploaded file

        Raises:
            FileNotFoundError: If local_path doesn't exist
            Exception: On upload failures (auth, network, etc.)
        """
        import asyncio
        from pathlib import Path

        # Convert Path to str if needed
        local_path_str = str(local_path) if isinstance(local_path, Path) else local_path

        return await asyncio.to_thread(
            self.upload_file,
            relative_path,
            local_path_str,
            overwrite,
        )

    async def async_upload_bytes(
        self,
        relative_path: str,
        data: bytes,
        overwrite: bool = True,
    ) -> str:
        """
        Upload bytes to OneLake (async, non-blocking).

        Args:
            relative_path: Path relative to base_path
            data: File content as bytes
            overwrite: Whether to overwrite existing file (default: True)

        Returns:
            Full abfss:// path to uploaded file

        Raises:
            Exception: On upload failures (auth, network, etc.)
        """
        import asyncio

        return await asyncio.to_thread(
            self.upload_bytes,
            relative_path,
            data,
            overwrite,
        )

    async def async_exists(self, relative_path: str) -> bool:
        """
        Check if file exists in OneLake (async, non-blocking).

        Args:
            relative_path: Path relative to base_path

        Returns:
            True if file exists, False otherwise
        """
        import asyncio

        return await asyncio.to_thread(
            self.exists,
            relative_path,
        )

    async def async_delete(self, relative_path: str) -> bool:
        """
        Delete a file from OneLake (async, non-blocking).

        Args:
            relative_path: Path relative to base_path

        Returns:
            True if deleted, False if didn't exist
        """
        import asyncio

        return await asyncio.to_thread(
            self.delete,
            relative_path,
        )


__all__ = [
    "OneLakeClient",
    "WriteOperation",
    "ONELAKE_RETRY_CONFIG",
    "parse_abfss_path",
    "TokenCredential",
    "FileBackedTokenCredential",
    "TCPKeepAliveAdapter",
]
