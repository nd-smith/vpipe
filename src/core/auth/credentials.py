"""
Azure credential provider supporting multiple authentication methods.

This module provides a unified interface for Azure authentication across
CLI, Service Principal (secret/certificate), and Managed Identity modes.

Supported Authentication Methods:
    - Azure CLI: Uses `az account get-access-token` for interactive development
    - Service Principal (Secret): Uses client ID/secret for service accounts
    - Service Principal (Certificate): Uses client ID/certificate for long-lived auth
    - Token File: Reads tokens from JSON file (for local development)
    - Default Azure Credential: Uses azure-identity's credential chain
      (includes managed identity, environment variables, VS Code, etc.)

Thread Safety:
    All credential operations use the shared TokenCache which is thread-safe.
    Token acquisition is protected to prevent concurrent fetches.

Example:
    >>> # CLI authentication
    >>> provider = AzureCredentialProvider(use_cli=True)
    >>> token = provider.get_storage_token()

    >>> # Service Principal with secret
    >>> provider = AzureCredentialProvider(
    ...     client_id="...",
    ...     client_secret="...",
    ...     tenant_id="..."
    ... )
    >>> options = provider.get_storage_options()

    >>> # Default Azure Credential (managed identity, etc.)
    >>> provider = AzureCredentialProvider(use_default_credential=True)
    >>> token = provider.get_token_for_resource("https://storage.azure.com/")
"""

import logging
import os
import subprocess
import shutil
from typing import Dict, Optional, Union
from pathlib import Path

from core.auth.token_cache import TokenCache

# Conditional imports for Azure SDK
try:
    from azure.identity import (
        ClientSecretCredential,
        CertificateCredential,
        DefaultAzureCredential,
    )
    from azure.core.credentials import AccessToken
    AZURE_IDENTITY_AVAILABLE = True
except ImportError:
    # Define placeholder types for when library not installed
    ClientSecretCredential = None
    CertificateCredential = None
    DefaultAzureCredential = None
    AccessToken = None
    AZURE_IDENTITY_AVAILABLE = False


logger = logging.getLogger(__name__)


# Azure resource endpoints
STORAGE_RESOURCE = "https://storage.azure.com/"
STORAGE_SCOPE = "https://storage.azure.com/.default"


class AzureAuthError(Exception):
    """
    Raised when Azure authentication fails.

    This exception provides actionable error messages to help diagnose
    authentication issues across different authentication modes.
    """
    pass


class AzureCredentialProvider:
    """
    Unified Azure credential provider with multi-mode support.

    Supports multiple authentication methods with automatic fallback:
    1. Token File (highest priority if configured)
    2. Azure CLI (interactive development)
    3. Service Principal with Secret (service accounts)
    4. Service Principal with Certificate (long-lived auth)
    5. Default Azure Credential (managed identity, env vars, etc.)

    Thread Safety:
        All operations use TokenCache which is thread-safe. Credential
        objects are created once and reused.

    Attributes:
        use_cli: Whether to use Azure CLI for authentication
        use_default_credential: Whether to use DefaultAzureCredential
        token_file: Path to JSON token file (optional)
        client_id: Azure AD client ID (for SPN auth)
        client_secret: Client secret (for secret-based SPN auth)
        tenant_id: Azure AD tenant ID
        certificate_path: Path to certificate file (for cert-based SPN auth)
    """

    def __init__(
        self,
        cache: Optional[TokenCache] = None,
        use_cli: bool = False,
        use_default_credential: bool = False,
        token_file: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        tenant_id: Optional[str] = None,
        certificate_path: Optional[str] = None,
    ):
        """
        Initialize credential provider.

        Args:
            cache: Optional TokenCache instance (creates new if None)
            use_cli: Use Azure CLI for authentication
            use_default_credential: Use DefaultAzureCredential
            token_file: Path to JSON token file
            client_id: Azure AD client ID (for SPN)
            client_secret: Client secret (for SPN with secret)
            tenant_id: Azure AD tenant ID
            certificate_path: Path to certificate (for SPN with cert)

        Note:
            If no authentication method is explicitly configured, the provider
            will attempt to load configuration from environment variables.
        """
        self._cache = cache or TokenCache()
        self._credential: Optional[Union[ClientSecretCredential, CertificateCredential, DefaultAzureCredential]] = None

        # File token caching - tracks file mtime to avoid re-reading unchanged files
        self._token_file_mtime: Optional[float] = None
        self._token_file_cache: Dict[str, str] = {}  # resource -> token

        # Store configuration
        self.use_cli = use_cli
        self.use_default_credential = use_default_credential
        self.token_file = token_file
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.certificate_path = certificate_path

        # Load from environment if not explicitly configured
        if not any([use_cli, use_default_credential, token_file, client_id]):
            self._load_config_from_env()

    def _load_config_from_env(self) -> None:
        """
        Load authentication configuration from environment variables.

        Environment Variables:
            AZURE_AUTH_INTERACTIVE: Set to "true" to use Azure CLI
            AZURE_TOKEN_FILE: Path to token file
            AZURE_CLIENT_ID: Service principal client ID
            AZURE_CLIENT_SECRET: Service principal secret
            AZURE_TENANT_ID: Azure AD tenant ID
            AZURE_CERTIFICATE_PATH: Path to certificate for SPN auth
        """
        self.use_cli = os.getenv("AZURE_AUTH_INTERACTIVE", "").lower() == "true"
        self.token_file = os.getenv("AZURE_TOKEN_FILE")
        self.client_id = os.getenv("AZURE_CLIENT_ID")
        self.client_secret = os.getenv("AZURE_CLIENT_SECRET")
        self.tenant_id = os.getenv("AZURE_TENANT_ID")
        self.certificate_path = os.getenv("AZURE_CERTIFICATE_PATH")

        # If no specific auth mode configured, default to DefaultAzureCredential
        if not any([self.use_cli, self.token_file, self.client_id]):
            self.use_default_credential = True

    def _is_token_file_modified(self) -> bool:
        """
        Check if token file has been modified since last read.

        Uses file modification time (mtime) to detect changes. Returns True
        if the file should be re-read (modified, first read, or inaccessible).

        Returns:
            True if file has been modified or hasn't been read yet
        """
        if not self.token_file:
            return False

        try:
            current_mtime = os.path.getmtime(self.token_file)
            if self._token_file_mtime is None:
                # First read - treat as "modified"
                return True
            if current_mtime > self._token_file_mtime:
                logger.debug(
                    "Token file modified, will re-read",
                    extra={
                        "token_file": self.token_file,
                        "previous_mtime": self._token_file_mtime,
                        "current_mtime": current_mtime,
                    }
                )
                return True
            return False
        except OSError:
            # File doesn't exist or can't be accessed - let _read_token_file handle error
            return True

    @property
    def has_spn_credentials(self) -> bool:
        """
        Check if Service Principal credentials are fully configured.

        Returns:
            True if SPN with secret OR certificate is configured
        """
        has_secret = all([self.client_id, self.client_secret, self.tenant_id])
        has_cert = all([self.client_id, self.certificate_path, self.tenant_id])
        return has_secret or has_cert

    @property
    def auth_mode(self) -> str:
        """
        Get current authentication mode for diagnostics.

        Returns:
            String describing active auth mode: "file", "cli", "spn_secret",
            "spn_cert", "default", or "none"
        """
        if self.token_file:
            return "file"
        if self.use_cli:
            return "cli"
        if self.has_spn_credentials:
            if self.certificate_path:
                return "spn_cert"
            return "spn_secret"
        if self.use_default_credential:
            return "default"
        return "none"

    def _get_azure_credential(self):
        """
        Get or create Azure credential object.

        Returns:
            Azure credential object (ClientSecretCredential, CertificateCredential,
            or DefaultAzureCredential)

        Raises:
            AzureAuthError: If azure-identity is not installed or config invalid
        """
        if self._credential is not None:
            return self._credential

        if not AZURE_IDENTITY_AVAILABLE:
            raise AzureAuthError(
                "azure-identity library not installed. "
                "Install with: pip install azure-identity"
            )

        # Certificate-based SPN (highest priority if configured)
        if self.certificate_path and self.client_id and self.tenant_id:
            if not Path(self.certificate_path).exists():
                raise AzureAuthError(
                    f"Certificate file not found: {self.certificate_path}"
                )

            logger.info(
                "Using certificate-based Service Principal authentication",
                extra={"tenant_id": self.tenant_id, "client_id": self.client_id}
            )

            self._credential = CertificateCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                certificate_path=self.certificate_path,
            )
            return self._credential

        # Secret-based SPN
        if self.client_secret and self.client_id and self.tenant_id:
            logger.debug(
                "Using client secret Service Principal authentication",
                extra={"tenant_id": self.tenant_id, "client_id": self.client_id}
            )

            self._credential = ClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
            return self._credential

        # Default credential chain (includes managed identity)
        if self.use_default_credential:
            logger.info("Using DefaultAzureCredential (managed identity, env vars, etc.)")
            self._credential = DefaultAzureCredential()
            return self._credential

        raise AzureAuthError(
            "No valid Azure credential configuration found. "
            "Please configure one of: CLI, SPN (secret/cert), or DefaultAzureCredential"
        )

    def _read_token_file(self, resource: str) -> str:
        """
        Read token from JSON file with caching.

        Uses file modification time to avoid re-reading unchanged files.
        Only re-reads when the file has been modified since last read.

        Supports two formats:
        1. JSON: {"https://storage.azure.com/": "token", ...}
        2. Plain text: single token (legacy)

        Args:
            resource: Resource URL to look up in JSON format

        Returns:
            Access token string

        Raises:
            AzureAuthError: If file not found, empty, or resource not in JSON
        """
        if not self.token_file:
            raise AzureAuthError(
                "Token file authentication requested but AZURE_TOKEN_FILE not set"
            )

        # Check cache first - return cached token if file hasn't changed
        if not self._is_token_file_modified():
            if resource in self._token_file_cache:
                logger.debug(
                    "Using cached token from file",
                    extra={"token_file": self.token_file, "resource": resource}
                )
                return self._token_file_cache[resource]
            # Check normalized match in cache
            for cached_resource in self._token_file_cache:
                if resource.rstrip("/") == cached_resource.rstrip("/"):
                    logger.debug(
                        "Using cached token from file (normalized match)",
                        extra={"resource": resource, "cached_key": cached_resource}
                    )
                    return self._token_file_cache[cached_resource]

        # File modified or not in cache - read fresh
        token_path = Path(self.token_file)
        if not token_path.exists():
            raise AzureAuthError(
                f"Token file not found: {self.token_file}\n"
                f"Hint: Ensure token_refresher is running or file path is correct"
            )

        try:
            content = token_path.read_text(encoding="utf-8-sig").strip()
            # Update mtime after successful read
            self._token_file_mtime = os.path.getmtime(self.token_file)
        except IOError as e:
            raise AzureAuthError(
                f"Failed to read token file: {self.token_file}\n"
                f"Error: {str(e)}"
            ) from e

        if not content:
            raise AzureAuthError(
                f"Token file is empty: {self.token_file}\n"
                f"Hint: Check if token_refresher is running properly"
            )

        # Try parsing as JSON (multi-resource format)
        try:
            import json
            tokens = json.loads(content)

            if isinstance(tokens, dict):
                # Update entire cache with all tokens from file
                self._token_file_cache = tokens.copy()
                logger.debug(
                    "Refreshed token file cache",
                    extra={
                        "token_file": self.token_file,
                        "resources": list(tokens.keys()),
                    }
                )

                # Try exact match first
                if resource in tokens:
                    return tokens[resource]

                # Try normalized match (with/without trailing slash)
                for key in tokens:
                    if resource.rstrip("/") == key.rstrip("/"):
                        return tokens[key]

                # Resource not found
                available = list(tokens.keys())
                raise AzureAuthError(
                    f"Resource '{resource}' not found in token file\n"
                    f"Available resources: {available}\n"
                    f"Hint: Run token_refresher with correct resource scopes"
                )

        except json.JSONDecodeError:
            # Not JSON - treat as plain text token (legacy)
            # Cache with empty string key for legacy format
            self._token_file_cache = {"": content}
            logger.debug(
                "Read token from plain text file (legacy format)",
                extra={"token_file": self.token_file}
            )
            return content

    def _fetch_cli_token(self, resource: str) -> str:
        """
        Fetch token from Azure CLI with timeout and retry.

        Args:
            resource: Azure resource URL (e.g., "https://storage.azure.com/")

        Returns:
            Access token string

        Raises:
            AzureAuthError: If CLI not found, session expired, or fetch fails
        """
        az_path = shutil.which("az")
        if not az_path:
            raise AzureAuthError(
                "Azure CLI not found in PATH\n"
                "Hint: Install Azure CLI from https://aka.ms/azure-cli"
            )

        cmd = [az_path, "account", "get-access-token", "--resource", resource]
        if self.tenant_id:
            cmd.extend(["--tenant", self.tenant_id])
        cmd.extend(["--query", "accessToken", "-o", "tsv"])

        # Retry with timeout (CLI can hang sometimes)
        max_attempts = 2
        timeout_seconds = 60

        for attempt in range(max_attempts):
            proc = None
            try:
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                stdout, stderr = proc.communicate(timeout=timeout_seconds)

                if proc.returncode != 0:
                    stderr = stderr.strip()

                    # Check for expired session
                    if "az login" in stderr.lower() or "please run" in stderr.lower():
                        raise AzureAuthError(
                            "Azure CLI session expired\n"
                            f"Run: az login\n"
                            f"Details: {stderr}"
                        )

                    raise AzureAuthError(
                        f"Azure CLI token fetch failed\n"
                        f"Command: {' '.join(cmd)}\n"
                        f"Error: {stderr}"
                    )

                token = stdout.strip()
                if not token:
                    raise AzureAuthError(
                        "Azure CLI returned empty token\n"
                        "Hint: Try running 'az login' again"
                    )

                logger.debug(
                    "Fetched token from Azure CLI",
                    extra={"resource": resource, "attempt": attempt + 1}
                )
                return token

            except subprocess.TimeoutExpired:
                logger.warning(
                    "Azure CLI token request timed out",
                    extra={
                        "resource": resource,
                        "attempt": attempt + 1,
                        "max_attempts": max_attempts
                    }
                )

                # Kill hung process
                if proc:
                    proc.kill()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        pass

                # Retry on timeout (unless last attempt)
                if attempt < max_attempts - 1:
                    import time
                    time.sleep(5)
                    continue

                raise AzureAuthError(
                    f"Azure CLI token request timed out after {max_attempts} attempts\n"
                    f"Resource: {resource}\n"
                    f"Hint: Check network connectivity and Azure CLI installation"
                )

        # Should not reach here
        raise AzureAuthError("Unexpected error in CLI token fetch")

    def get_token_for_resource(
        self,
        resource: str,
        force_refresh: bool = False
    ) -> str:
        """
        Get access token for specified resource.

        Tries authentication methods in priority order:
        1. Token file (if configured)
        2. Cached token (if valid and not force_refresh)
        3. Azure CLI (if configured)
        4. Azure credential (SPN or DefaultAzureCredential)

        Args:
            resource: Azure resource URL (e.g., "https://storage.azure.com/")
            force_refresh: Skip cache and fetch fresh token

        Returns:
            Access token string

        Raises:
            AzureAuthError: If all authentication methods fail
        """
        # Try token file first (highest priority)
        if self.token_file:
            try:
                return self._read_token_file(resource)
            except AzureAuthError:
                logger.warning(
                    "Token file configured but failed to read, trying other methods",
                    extra={"token_file": self.token_file}
                )
                # Continue to other methods

        # Check cache (unless force refresh)
        if not force_refresh:
            cached = self._cache.get(resource)
            if cached:
                logger.debug("Using cached token", extra={"resource": resource})
                return cached

        # Try Azure CLI
        if self.use_cli:
            token = self._fetch_cli_token(resource)
            self._cache.set(resource, token)
            return token

        # Try Azure credential (SPN or DefaultAzureCredential)
        try:
            credential = self._get_azure_credential()

            # Convert resource URL to scope (append /.default)
            scope = resource.rstrip("/") + "/.default"

            # Get token (synchronous - azure-identity handles async internally)
            access_token: AccessToken = credential.get_token(scope)
            token = access_token.token

            self._cache.set(resource, token)
            logger.debug(
                "Acquired token from Azure credential",
                extra={"resource": resource, "auth_mode": self.auth_mode}
            )
            return token

        except Exception as e:
            raise AzureAuthError(
                f"Failed to acquire token for {resource}\n"
                f"Auth mode: {self.auth_mode}\n"
                f"Error: {str(e)}"
            ) from e

    def get_storage_token(self, force_refresh: bool = False) -> str:
        """
        Get token for Azure Storage operations.

        Args:
            force_refresh: Skip cache and fetch fresh token

        Returns:
            Access token for Azure Storage

        Raises:
            AzureAuthError: If token acquisition fails
        """
        return self.get_token_for_resource(STORAGE_RESOURCE, force_refresh)

    def get_storage_options(self, force_refresh: bool = False) -> Dict[str, str]:
        """
        Get storage authentication options for delta-rs/object_store.

        Returns dictionary compatible with delta-rs storage_options parameter.
        For token-based auth, returns {"azure_storage_token": "..."}.
        For SPN, returns {"azure_client_id": "...", "azure_client_secret": "...", ...}.

        Args:
            force_refresh: Skip cache and fetch fresh token

        Returns:
            Dictionary of storage options for delta-rs
        """
        # For SPN with secret, return credentials directly (more efficient)
        if self.client_secret and self.client_id and self.tenant_id and not self.token_file:
            return {
                "azure_client_id": self.client_id,
                "azure_client_secret": self.client_secret,
                "azure_tenant_id": self.tenant_id,
            }

        # For all other modes, get token
        try:
            token = self.get_storage_token(force_refresh)
            return {"azure_storage_token": token}
        except AzureAuthError:
            logger.error(
                "Failed to get storage token - authentication will fail at access time"
            )
            return {}

    def get_kusto_token(self, cluster_uri: str, force_refresh: bool = False) -> str:
        """
        Get token for Azure Data Explorer (Kusto/Eventhouse).

        Args:
            cluster_uri: Kusto cluster URI (e.g., "https://cluster.region.kusto.windows.net")
            force_refresh: Skip cache and fetch fresh token

        Returns:
            Access token for Kusto

        Raises:
            AzureAuthError: If token acquisition fails

        Note:
            For SPN auth with KustoConnectionStringBuilder, you should use
            the builder directly instead of this method.
        """
        return self.get_token_for_resource(cluster_uri, force_refresh)

    def clear_cache(self, resource: Optional[str] = None) -> None:
        """
        Clear cached tokens (both in-memory cache and file token cache).

        Args:
            resource: Specific resource to clear, or None to clear all
        """
        self._cache.clear(resource)
        # Also clear file cache
        if resource is None:
            self._token_file_cache.clear()
            self._token_file_mtime = None
        else:
            self._token_file_cache.pop(resource, None)
        logger.debug(
            "Cleared token cache",
            extra={"resource": resource if resource else "all"}
        )

    def clear_file_cache(self) -> None:
        """
        Clear only the file token cache, forcing re-read on next access.

        This is useful when you know the token file has been updated
        but the mtime-based detection might not work (e.g., sub-second updates).
        """
        self._token_file_cache.clear()
        self._token_file_mtime = None
        logger.debug("Cleared file token cache")

    def get_diagnostics(self) -> Dict:
        """
        Get authentication diagnostics for health checks.

        Returns:
            Dictionary with auth mode, configuration, and cache state
        """
        diag = {
            "auth_mode": self.auth_mode,
            "cli_enabled": self.use_cli,
            "default_credential_enabled": self.use_default_credential,
            "spn_configured": self.has_spn_credentials,
            "token_file": self.token_file,
        }

        # Add cache age for storage resource
        storage_age = self._cache.get_age(STORAGE_RESOURCE)
        if storage_age:
            diag["storage_token_age_seconds"] = storage_age.total_seconds()

        # Add file cache info
        if self.token_file:
            diag["file_cache_resources"] = list(self._token_file_cache.keys())
            diag["file_cache_mtime"] = self._token_file_mtime

        return diag


# Module-level singleton (optional - can create instances directly)
_default_provider: Optional[AzureCredentialProvider] = None


def get_default_provider() -> AzureCredentialProvider:
    """
    Get or create default credential provider singleton.

    The default provider loads configuration from environment variables.

    Returns:
        Singleton AzureCredentialProvider instance
    """
    global _default_provider
    if _default_provider is None:
        _default_provider = AzureCredentialProvider()
    return _default_provider


def get_storage_options(force_refresh: bool = False) -> Dict[str, str]:
    """
    Get storage options from default provider.

    Convenience function for common use case.

    Args:
        force_refresh: Skip cache and fetch fresh token

    Returns:
        Storage options dictionary for delta-rs
    """
    return get_default_provider().get_storage_options(force_refresh)


def clear_token_cache(resource: Optional[str] = None) -> None:
    """
    Clear token cache on default provider.

    Args:
        resource: Specific resource to clear, or None to clear all
    """
    get_default_provider().clear_cache(resource)


__all__ = [
    "AzureAuthError",
    "AzureCredentialProvider",
    "get_default_provider",
    "get_storage_options",
    "clear_token_cache",
    "STORAGE_RESOURCE",
    "STORAGE_SCOPE",
]
