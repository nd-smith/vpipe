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
from kafka_pipeline.common.metrics import record_onelake_error, record_onelake_operation
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
    """Check if exception is auth-related."""
    error_str = str(error).lower()
    return any(marker in error_str for marker in AUTH_ERROR_MARKERS)


def _classify_error(error: Exception) -> str:
    """
    Classify an exception into an error category for metrics.

    Args:
        error: The exception to classify

    Returns:
        Error category: timeout, auth, not_found, or unknown
    """
    error_str = str(error).lower()
    error_type = type(error).__name__.lower()

    # Check for timeout errors
    if any(marker in error_str for marker in TIMEOUT_ERROR_MARKERS):
        return "timeout"
    if "timeout" in error_type:
        return "timeout"

    # Check for auth errors
    if any(marker in error_str for marker in AUTH_ERROR_MARKERS):
        return "auth"

    # Check for not found errors
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
        """Initialize pool manager with TCP keepalive socket options."""
        if "socket_options" not in kwargs:
            kwargs["socket_options"] = []

        # Enable TCP keepalive
        kwargs["socket_options"].append((socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1))
        # Start keepalive after 120s idle (before Azure 4-min timeout)
        kwargs["socket_options"].append((socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 120))
        # Send keepalive probe every 30s
        kwargs["socket_options"].append((socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 30))
        # Close connection after 8 failed probes (4 minutes total)
        kwargs["socket_options"].append((socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 8))

        return super().init_poolmanager(*args, **kwargs)


class TokenCredential:
    """
    Simple credential wrapper for raw access token.

    Implements the protocol expected by Azure SDK clients.
    """

    def __init__(self, token: str, expires_in_hours: int = 1):
        self._token = token
        self._expires_on = int(
            (datetime.now(timezone.utc) + timedelta(hours=expires_in_hours)).timestamp()
        )

    def get_token(self, *scopes, **kwargs) -> AccessToken:
        """Return token in Azure SDK expected format."""
        return AccessToken(self._token, self._expires_on)


# Registry of FileBackedTokenCredential instances for coordinated refresh
_file_credential_registry: list = []


def _register_file_credential(credential: "FileBackedTokenCredential") -> None:
    """Register a FileBackedTokenCredential for coordinated refresh."""
    _file_credential_registry.append(credential)


def _clear_all_file_credentials() -> None:
    """Force refresh all registered FileBackedTokenCredentials."""
    for cred in _file_credential_registry:
        try:
            cred._cached_token = None
            cred._token_acquired_at = None
        except Exception:
            pass  # Best effort


def _refresh_all_credentials() -> None:
    """
    Clear all credential caches on auth error.

    This is the callback used by @with_retry(on_auth_error=...) decorators.
    It clears both:
    1. AzureAuth's in-memory token cache (for CLI auth)
    2. All FileBackedTokenCredential instances (for file-based auth)
    """
    clear_token_cache()
    _clear_all_file_credentials()


class FileBackedTokenCredential:
    """
    Credential that re-reads from token file when token is near expiry.

    This solves the problem where a long-running stage creates a TokenCredential
    once at startup, but the token expires after 60 minutes. With this class,
    the credential will automatically re-read the token from the file (which
    token_refresher.py keeps updated) before the current token expires.

    Token refresh timeline:
    - Token refresher writes new token every 45 minutes
    - Azure tokens expire at 60 minutes
    - This class re-reads token every 5 minutes to stay fresh

    We use a short refresh interval (5 min) because:
    1. The token file read is very fast (just reading a small JSON file)
    2. This ensures we always have a fresh token even if timing is off
    3. It handles cases where token_refresher restarts or tokens are manually updated
    """

    # Re-read token every 5 minutes to stay fresh
    # This is conservative but safe - file reads are fast
    TOKEN_REFRESH_MINUTES = 5

    def __init__(self, resource: str = "https://storage.azure.com/"):
        """
        Args:
            resource: Azure resource URL for token lookup in JSON file
        """
        self._resource = resource
        self._cached_token: Optional[str] = None
        self._token_acquired_at: Optional[datetime] = None
        self._logger = logging.getLogger(__name__)
        # Register for coordinated refresh
        _register_file_credential(self)

    def _should_refresh(self) -> bool:
        """Check if token should be re-read from file."""
        if self._cached_token is None or self._token_acquired_at is None:
            return True

        age = datetime.now(timezone.utc) - self._token_acquired_at
        return age > timedelta(minutes=self.TOKEN_REFRESH_MINUTES)

    def _fetch_token(self) -> str:
        """Fetch fresh token from auth system (re-reads from file)."""
        auth = get_auth()

        # Clear cache to force re-read from file
        clear_token_cache()

        token = auth.get_storage_token(force_refresh=True)
        if not token:
            raise RuntimeError("Failed to get storage token")

        self._cached_token = token
        self._token_acquired_at = datetime.now(timezone.utc)

        self._logger.debug(
            "FileBackedTokenCredential refreshed token for %s", self._resource
        )
        return token

    def get_token(self, *scopes, **kwargs) -> AccessToken:
        """
        Return token in Azure SDK expected format.

        Automatically refreshes from file if token is near expiry.
        """
        if self._should_refresh():
            self._fetch_token()

        # Calculate expires_on (assume 60 min from acquisition)
        if self._token_acquired_at:
            expires_on = int(
                (self._token_acquired_at + timedelta(hours=1)).timestamp()
            )
        else:
            expires_on = int(
                (datetime.now(timezone.utc) + timedelta(hours=1)).timestamp()
            )

        return AccessToken(self._cached_token, expires_on)

    def force_refresh(self) -> None:
        """Force an immediate token refresh (called on auth errors)."""
        self._cached_token = None
        self._token_acquired_at = None
        self._fetch_token()


def parse_abfss_path(path: str) -> Tuple[str, str, str]:
    """
    Parse an abfss:// path into components.

    Args:
        path: abfss://container@account.dfs.fabric.microsoft.com/path/to/files

    Returns:
        Tuple of (account_host, container, directory_path)

    Raises:
        ValueError: If path format is invalid
    """
    parsed = urlparse(path)

    if parsed.scheme != "abfss":
        raise ValueError(f"Expected abfss:// scheme, got: {parsed.scheme}")

    if "@" not in parsed.netloc:
        raise ValueError(f"Invalid OneLake path format: {path}")

    container, account_host = parsed.netloc.split("@", 1)
    directory_path = parsed.path.lstrip("/")

    return account_host, container, directory_path


class OneLakeClient(LoggedClass):
    """
    Client for OneLake file operations with automatic operation logging.

    Uses Azure Data Lake Storage Gen2 API (DFS endpoint).
    Supports both Azure CLI and Service Principal authentication.

    Usage:
        client = OneLakeClient("abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Files")

        with client:
            client.upload_bytes("folder/file.pdf", content)
            exists = client.exists("folder/file.pdf")
    """

    log_component = "onelake"

    def __init__(
        self,
        base_path: str,
        max_pool_size: Optional[int] = None,
        connection_timeout: int = CONNECTION_TIMEOUT,
        request_timeout: int = 300,
    ):
        """
        Args:
            base_path: abfss:// path to files directory
            max_pool_size: HTTP connection pool size (defaults to config.lakehouse.upload_max_concurrency)
            connection_timeout: Connection timeout in seconds (default: 300s/5min)
            request_timeout: Request timeout in seconds for upload/download operations (default: 300s/5min)
        """
        self.base_path = base_path.rstrip("/")

        # Get upload configuration from config (Task E.5)
        # Default values for upload configuration
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

        # Parse path components
        self.account_host, self.container, self.base_directory = parse_abfss_path(
            base_path
        )

        # Lazy-initialized clients
        self._service_client: Optional[DataLakeServiceClient] = None
        self._file_system_client = None
        self._session: Optional[requests.Session] = None

        # Idempotency tracking (Task G.3b)
        self._write_tokens: Dict[str, WriteOperation] = {}

        # Connection pool statistics (P2.5)
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

        # Dynamic connection pool sizing (P2.4)
        cpu_cores = os.cpu_count() or 4
        max_concurrency = 16  # Default upload max concurrency

        # Azure Storage limits: 500 concurrent connections per storage account
        # Formula: min(cpu_cores * 10, max_concurrency, 250)
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

        # Get credential based on auth mode (priority: token_file > cli > spn)
        if auth.token_file:
            # Token file mode - use FileBackedTokenCredential for auto-refresh
            # This credential will re-read from the token file when tokens near expiry
            try:
                credential = FileBackedTokenCredential(
                    resource=auth.STORAGE_RESOURCE
                )
                # Store reference so we can call force_refresh on auth errors
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
                # Fall through to try other auth methods
                credential = None
                auth_mode = None
                self._file_credential = None
        else:
            credential = None
            auth_mode = None
            self._file_credential = None

        # Try CLI auth if token file didn't work
        if credential is None and auth.use_cli:
            # For CLI, also use FileBackedTokenCredential if there's a fallback token file
            # Otherwise use regular TokenCredential (CLI can refresh itself)
            token = auth.get_storage_token()
            if not token:
                raise RuntimeError("Failed to get CLI storage token")
            credential = TokenCredential(token)
            auth_mode = "cli"

        # Try SPN auth if CLI didn't work
        if credential is None and auth.has_spn_credentials:
            from azure.identity import ClientSecretCredential

            # Assert credentials are not None (has_spn_credentials guarantees this)
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

        # Create TCP keepalive adapter to prevent Azure 4-minute idle timeout
        adapter = TCPKeepAliveAdapter(
            pool_connections=actual_pool_size,  # Number of pools
            pool_maxsize=actual_pool_size,  # Connections per pool
        )

        # Create a session and mount the keepalive adapter
        session = requests.Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        # Configure transport with the session (session_owner=False since we manage the session)
        transport = RequestsTransport(
            session=session,
            session_owner=False,  # Don't close session when transport is closed
        )

        self._service_client = DataLakeServiceClient(
            account_url=account_url,
            credential=credential,
            transport=transport,
            connection_timeout=self._connection_timeout,  # Connection establishment timeout
        )
        self._file_system_client = self._service_client.get_file_system_client(  # type: ignore
            self.container
        )

        # Store session reference for cleanup
        self._session = session

        # Track connection creation (P2.5)
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

    def close(self) -> None:
        """Close client and release resources."""
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
        # Close the session we created (since session_owner=False)
        if self._session is not None:
            try:
                self._session.close()
            except Exception:
                pass  # Best effort cleanup
            self._session = None
        self._service_client = None
        self._file_system_client = None

    def _ensure_client(self) -> None:
        """Ensure client is initialized."""
        if self._file_system_client is None:
            self._create_clients(max_pool_size=self._max_pool_size)

    def _refresh_credential(self) -> None:
        """
        Force refresh the credential (called on auth errors).

        For FileBackedTokenCredential: forces immediate re-read from token file.
        For other auth modes: clears token cache.
        """
        # If using FileBackedTokenCredential, force it to re-read from file
        if hasattr(self, '_file_credential') and self._file_credential is not None:
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
            # For CLI/SPN auth, just clear the cache
            clear_token_cache()

    def _refresh_client(self) -> None:
        """Refresh client with new credentials."""
        # First refresh the credential
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
        # Close the session we created (since session_owner=False)
        if self._session is not None:
            try:
                self._session.close()
            except Exception:
                pass  # Best effort cleanup
            self._session = None
        self._service_client = None
        self._file_system_client = None
        self._create_clients(max_pool_size=self._max_pool_size)
        self._log(logging.INFO, "OneLake client refreshed with new credentials")

    def _handle_auth_error(self, e: Exception) -> None:
        """Handle potential auth error by refreshing client."""
        if _is_auth_error(e):
            self._log(
                logging.WARNING,
                "Auth error detected, refreshing OneLake client",
                error_message=str(e)[:200],
            )
            self._refresh_client()

    def _full_path(self, relative_path: str) -> str:
        """Build full directory path from relative path."""
        return f"{self.base_directory}/{relative_path}"

    def _split_path(self, full_path: str) -> Tuple[str, str]:
        """Split full path into directory and filename."""
        if "/" in full_path:
            directory = "/".join(full_path.split("/")[:-1])
            filename = full_path.split("/")[-1]
        else:
            directory = ""
            filename = full_path
        return directory, filename

    def _is_duplicate(self, operation_token: str) -> bool:
        """
        Check if write operation with this token was already completed (Task G.3b).

        Args:
            operation_token: Idempotency token for the write operation

        Returns:
            True if operation was already completed
        """
        return operation_token in self._write_tokens

    def _record_token(self, write_op: WriteOperation) -> None:
        """
        Record write operation token for idempotency tracking (Task G.3b).

        Args:
            write_op: Write operation to record
        """
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
        """
        Upload bytes to OneLake.

        Args:
            relative_path: Path relative to base_path
            data: File content as bytes
            overwrite: Whether to overwrite existing file

        Returns:
            Full abfss:// path to uploaded file
        """
        self._ensure_client()

        full_path = self._full_path(relative_path)
        directory, filename = self._split_path(full_path)
        bytes_count = len(data)
        start_time = time.perf_counter()

        try:
            dir_client = self._file_system_client.get_directory_client(directory)  # type: ignore
            file_client = dir_client.get_file_client(filename)
            file_client.upload_data(data, overwrite=overwrite)

            duration = time.perf_counter() - start_time
            record_onelake_operation(
                operation="upload",
                status="success",
                duration=duration,
                bytes_transferred=bytes_count,
            )

            result_path = f"{self.base_path}/{relative_path}"
            self._log(
                logging.DEBUG,
                "Upload complete",
                blob_path=relative_path,
                bytes_written=bytes_count,
            )
            return result_path

        except Exception as e:
            duration = time.perf_counter() - start_time
            error_type = _classify_error(e)
            record_onelake_operation(
                operation="upload",
                status="error",
                duration=duration,
                bytes_transferred=0,
            )
            record_onelake_error(operation="upload", error_type=error_type)
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
        """
        Upload bytes with idempotency token to prevent duplicate uploads (Task G.3b).

        If operation_token is provided and already recorded, skips upload.
        Otherwise uploads and records token for future deduplication.

        Args:
            relative_path: Path relative to base_path
            data: File content as bytes
            operation_token: Optional idempotency token (generates UUID if None)
            overwrite: Whether to overwrite existing file

        Returns:
            WriteOperation with token and upload details
        """
        if operation_token is None:
            operation_token = str(uuid.uuid4())

        # Check for duplicate operation
        if self._is_duplicate(operation_token):
            self._log(
                logging.INFO,
                "Skipping duplicate upload operation",
                token=operation_token[:8],
                path=relative_path,
            )
            # Return existing operation record
            existing_op = self._write_tokens[operation_token]
            return WriteOperation(
                token=operation_token,
                relative_path=relative_path,
                timestamp=existing_op.timestamp,
                bytes_written=0,  # Not written this time
            )

        # Perform upload
        self.upload_bytes(relative_path, data, overwrite=overwrite)

        # Record operation
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
        """
        Download file content from OneLake.

        Args:
            relative_path: Path relative to base_path

        Returns:
            File content as bytes
        """
        self._ensure_client()

        full_path = self._full_path(relative_path)
        directory, filename = self._split_path(full_path)
        start_time = time.perf_counter()

        try:
            dir_client = self._file_system_client.get_directory_client(directory)  # type: ignore
            file_client = dir_client.get_file_client(filename)

            download = file_client.download_file()
            content = download.readall()

            duration = time.perf_counter() - start_time
            bytes_count = len(content)
            record_onelake_operation(
                operation="download",
                status="success",
                duration=duration,
                bytes_transferred=bytes_count,
            )

            self._log(
                logging.DEBUG,
                "Download complete",
                blob_path=relative_path,
                bytes_downloaded=bytes_count,
            )
            return content

        except Exception as e:
            duration = time.perf_counter() - start_time
            error_type = _classify_error(e)
            record_onelake_operation(
                operation="download",
                status="error",
                duration=duration,
                bytes_transferred=0,
            )
            record_onelake_error(operation="download", error_type=error_type)
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
        """
        Upload file from local path to OneLake (streaming, memory-efficient).

        Args:
            relative_path: Path relative to base_path
            local_path: Local file path to upload
            overwrite: Whether to overwrite existing file

        Returns:
            Full abfss:// path to uploaded file
        """
        self._ensure_client()

        full_path = self._full_path(relative_path)
        directory, filename = self._split_path(full_path)

        # Get file size before upload for metrics
        file_size = os.path.getsize(local_path)
        start_time = time.perf_counter()

        try:
            dir_client = self._file_system_client.get_directory_client(directory)  # type: ignore
            file_client = dir_client.get_file_client(filename)

            with open(local_path, "rb") as f:
                file_client.upload_data(f, overwrite=overwrite)

            duration = time.perf_counter() - start_time
            record_onelake_operation(
                operation="upload",
                status="success",
                duration=duration,
                bytes_transferred=file_size,
            )

            result_path = f"{self.base_path}/{relative_path}"
            self._log(
                logging.DEBUG,
                "Upload complete",
                blob_path=relative_path,
                bytes_written=file_size,
            )
            return result_path

        except Exception as e:
            duration = time.perf_counter() - start_time
            error_type = _classify_error(e)
            record_onelake_operation(
                operation="upload",
                status="error",
                duration=duration,
                bytes_transferred=0,
            )
            record_onelake_error(operation="upload", error_type=error_type)
            self._handle_auth_error(e)
            raise

    @with_retry(config=ONELAKE_RETRY_CONFIG, on_auth_error=_refresh_all_credentials)
    def exists(self, relative_path: str) -> bool:
        """
        Check if file exists.

        Args:
            relative_path: Path relative to base_path

        Returns:
            True if file exists
        """
        self._ensure_client()

        full_path = self._full_path(relative_path)
        directory, filename = self._split_path(full_path)
        start_time = time.perf_counter()

        try:
            dir_client = self._file_system_client.get_directory_client(directory)  # type: ignore
            file_client = dir_client.get_file_client(filename)
            file_client.get_file_properties()

            duration = time.perf_counter() - start_time
            record_onelake_operation(
                operation="exists",
                status="success",
                duration=duration,
            )
            return True
        except Exception as e:
            duration = time.perf_counter() - start_time
            # 404 is expected for non-existent files, not an error
            error_str = str(e).lower()
            if "404" in error_str or "not found" in error_str:
                # File not existing is a successful check, not an error
                record_onelake_operation(
                    operation="exists",
                    status="success",
                    duration=duration,
                )
                self._log(logging.DEBUG, "File does not exist", blob_path=relative_path)
                return False
            # Actual errors
            error_type = _classify_error(e)
            record_onelake_operation(
                operation="exists",
                status="error",
                duration=duration,
            )
            record_onelake_error(operation="exists", error_type=error_type)
            # Auth errors should trigger refresh and retry
            self._handle_auth_error(e)
            raise

    @with_retry(config=ONELAKE_RETRY_CONFIG, on_auth_error=_refresh_all_credentials)
    def delete(self, relative_path: str) -> bool:
        """
        Delete a file.

        Args:
            relative_path: Path relative to base_path

        Returns:
            True if deleted, False if didn't exist
        """
        self._ensure_client()

        full_path = self._full_path(relative_path)
        directory, filename = self._split_path(full_path)
        start_time = time.perf_counter()

        try:
            dir_client = self._file_system_client.get_directory_client(directory)  # type: ignore
            file_client = dir_client.get_file_client(filename)
            file_client.delete_file()

            duration = time.perf_counter() - start_time
            record_onelake_operation(
                operation="delete",
                status="success",
                duration=duration,
            )
            self._log(logging.DEBUG, "Deleted file", blob_path=relative_path)
            return True
        except Exception as e:
            duration = time.perf_counter() - start_time
            error_str = str(e).lower()
            if "404" in error_str or "not found" in error_str:
                # File not existing is a successful delete (idempotent)
                record_onelake_operation(
                    operation="delete",
                    status="success",
                    duration=duration,
                )
                return False
            # Actual errors
            error_type = _classify_error(e)
            record_onelake_operation(
                operation="delete",
                status="error",
                duration=duration,
            )
            record_onelake_error(operation="delete", error_type=error_type)
            self._handle_auth_error(e)
            raise

    @with_retry(config=ONELAKE_RETRY_CONFIG, on_auth_error=_refresh_all_credentials)
    def get_file_properties(self, relative_path: str) -> Optional[dict]:
        """
        Get file properties/metadata.

        Args:
            relative_path: Path relative to base_path

        Returns:
            Dict of properties or None if file doesn't exist
        """
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
        """Track request statistics (P2.5)."""
        with self._pool_stats_lock:
            self._pool_stats["requests_processed"] += 1
            if not success:
                self._pool_stats["errors_encountered"] += 1

    def get_pool_stats(self) -> Dict[str, Any]:
        """Get current connection pool statistics (P2.5)."""
        from datetime import datetime, timezone

        with self._pool_stats_lock:
            stats = self._pool_stats.copy()
            stats["uptime_seconds"] = (
                datetime.now(timezone.utc) - stats["last_reset"]
            ).total_seconds()
            return stats

    def reset_pool_stats(self) -> None:
        """Reset connection pool statistics (P2.5)."""
        from datetime import datetime, timezone

        with self._pool_stats_lock:
            self._pool_stats = {
                "connections_created": 0,
                "requests_processed": 0,
                "errors_encountered": 0,
                "last_reset": datetime.now(timezone.utc),
            }

    def log_pool_health(self) -> None:
        """Log current connection pool health metrics (P2.5)."""
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


__all__ = [
    "OneLakeClient",
    "WriteOperation",
    "ONELAKE_RETRY_CONFIG",
    "parse_abfss_path",
    "TokenCredential",
    "FileBackedTokenCredential",
    "TCPKeepAliveAdapter",
]
