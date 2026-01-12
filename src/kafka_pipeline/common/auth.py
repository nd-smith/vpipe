"""
Azure authentication for kafka_pipeline storage operations.

Simplified version supporting:
- Azure CLI token acquisition
- Service Principal (SPN) credentials
- Token file-based auth
- Token caching with automatic refresh
"""

import json
import logging
import os
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

from core.logging import get_logger, log_with_context

logger = get_logger(__name__)

# Token timing constants
TOKEN_REFRESH_MINS = 50  # Refresh before expiry
TOKEN_EXPIRY_MINS = 60  # Azure token lifetime


@dataclass
class CachedToken:
    """Token with acquisition timestamp."""

    value: str
    acquired_at: datetime

    def is_valid(self, buffer_mins: int = TOKEN_REFRESH_MINS) -> bool:
        """Check if token is still valid with buffer."""
        age = datetime.now(timezone.utc) - self.acquired_at
        return age < timedelta(minutes=buffer_mins)


class TokenCache:
    """Token cache for multiple resources."""

    def __init__(self):
        self._tokens: Dict[str, CachedToken] = {}

    def get(self, resource: str) -> Optional[str]:
        """Get cached token if still valid."""
        cached = self._tokens.get(resource)
        if cached and cached.is_valid():
            return cached.value
        return None

    def set(self, resource: str, token: str) -> None:
        """Cache a token."""
        self._tokens[resource] = CachedToken(
            value=token, acquired_at=datetime.now(timezone.utc)
        )

    def clear(self, resource: Optional[str] = None) -> None:
        """Clear one or all cached tokens."""
        if resource:
            self._tokens.pop(resource, None)
            log_with_context(
                logger, logging.DEBUG, "Cleared token cache", resource=resource
            )
        else:
            self._tokens.clear()
            log_with_context(logger, logging.DEBUG, "Cleared all token caches")


class AzureAuthError(Exception):
    """Raised when Azure authentication fails."""

    pass


class AzureAuth:
    """
    Azure authentication provider.

    Supports three modes:
    - CLI: Uses `az account get-access-token` (set AZURE_AUTH_INTERACTIVE=true)
    - SPN: Uses client credentials (set AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)
    - File: Reads token from file (set AZURE_TOKEN_FILE=/path/to/token.txt)
    """

    STORAGE_RESOURCE = "https://storage.azure.com/"

    def __init__(self, cache: Optional[TokenCache] = None):
        self._cache = cache or TokenCache()
        self._token_file_mtime: Optional[float] = None
        self._load_config()

    def _load_config(self) -> None:
        """Load auth configuration from environment."""
        self.use_cli = os.getenv("AZURE_AUTH_INTERACTIVE", "").lower() == "true"
        self.token_file = os.getenv("AZURE_TOKEN_FILE")
        self.client_id = os.getenv("AZURE_CLIENT_ID")
        self.client_secret = os.getenv("AZURE_CLIENT_SECRET")
        self.tenant_id = os.getenv("AZURE_TENANT_ID")

    @property
    def has_spn_credentials(self) -> bool:
        """Check if SPN credentials are fully configured."""
        return all([self.client_id, self.client_secret, self.tenant_id])

    @property
    def auth_mode(self) -> str:
        """Return current auth mode for diagnostics."""
        if self.token_file:
            return "file"
        if self.use_cli:
            return "cli"
        if self.has_spn_credentials:
            return "spn"
        return "none"

    def _is_token_file_modified(self) -> bool:
        """Check if token file has been modified since last read."""
        if not self.token_file:
            return False

        try:
            current_mtime = os.path.getmtime(self.token_file)
            if self._token_file_mtime is None:
                return True
            if current_mtime > self._token_file_mtime:
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "Token file modified, will re-read",
                    token_file=self.token_file,
                )
                return True
            return False
        except OSError:
            return True

    def _update_token_file_mtime(self) -> None:
        """Update the cached file modification time after successful read."""
        if self.token_file:
            try:
                self._token_file_mtime = os.path.getmtime(self.token_file)
            except OSError:
                pass

    def _read_token_file(self, resource: Optional[str] = None) -> str:
        """
        Read token from file.

        Supports two formats:
        1. JSON file with resource-keyed tokens: {"https://storage.azure.com/": "token", ...}
        2. Plain text file with single token (legacy)
        """
        if not self.token_file:
            raise AzureAuthError("AZURE_TOKEN_FILE not set")

        try:
            with open(self.token_file, "r", encoding="utf-8-sig") as f:
                content = f.read().strip()

            if not content:
                raise AzureAuthError(f"Token file is empty: {self.token_file}")

            # Try to parse as JSON (multi-resource token file)
            try:
                tokens = json.loads(content)
                if isinstance(tokens, dict):
                    lookup_resource = resource or self.STORAGE_RESOURCE

                    # Try exact match first
                    if lookup_resource in tokens:
                        log_with_context(
                            logger,
                            logging.DEBUG,
                            "Read token from JSON file",
                            token_file=self.token_file,
                            resource=lookup_resource,
                        )
                        return tokens[lookup_resource]

                    # Try normalized match (with/without trailing slash)
                    for key in tokens:
                        if lookup_resource.rstrip("/") == key.rstrip("/"):
                            log_with_context(
                                logger,
                                logging.DEBUG,
                                "Read token from JSON file (normalized match)",
                                token_file=self.token_file,
                                resource=lookup_resource,
                            )
                            return tokens[key]

                    # Resource not found
                    available_resources = list(tokens.keys())
                    raise AzureAuthError(
                        f"Resource '{lookup_resource}' not found in token file. "
                        f"Available: {available_resources}"
                    )
            except json.JSONDecodeError:
                # Not JSON - treat as plain text token (legacy format)
                pass

            # Plain text format: entire file content is the token
            log_with_context(
                logger,
                logging.DEBUG,
                "Read token from plain text file",
                token_file=self.token_file,
            )
            return content

        except FileNotFoundError as e:
            raise AzureAuthError(f"Token file not found: {self.token_file}") from e
        except IOError as e:
            raise AzureAuthError(f"Failed to read token file: {self.token_file}") from e

    def _fetch_cli_token(self, resource: str) -> str:
        """Fetch fresh token from Azure CLI."""
        az_path = shutil.which("az")
        if not az_path:
            raise AzureAuthError(
                "Azure CLI not found. Ensure 'az' is installed and in PATH."
            )

        cmd = [az_path, "account", "get-access-token", "--resource", resource]
        if self.tenant_id:
            cmd.extend(["--tenant", self.tenant_id])
        cmd.extend(["--query", "accessToken", "-o", "tsv"])

        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=60,
                check=False,
            )

            if result.returncode != 0:
                stderr = result.stderr.strip()
                if "az login" in stderr.lower() or "please run" in stderr.lower():
                    raise AzureAuthError(
                        f"Azure CLI session expired. Run 'az login' to re-authenticate.\n{stderr}"
                    )
                raise AzureAuthError(f"Azure CLI failed: {stderr}")

            token = result.stdout.strip()
            if not token:
                raise AzureAuthError("Azure CLI returned empty token")

            log_with_context(
                logger,
                logging.DEBUG,
                "Acquired token from Azure CLI",
                resource=resource,
            )
            return token

        except subprocess.TimeoutExpired:
            raise AzureAuthError(f"Azure CLI token request timed out for {resource}")

    def get_storage_token(self, force_refresh: bool = False) -> Optional[str]:
        """Get token for OneLake/ADLS storage operations."""
        if self.token_file:
            # Check if file was modified - if so, clear cache to force re-read
            if self._is_token_file_modified():
                self._cache.clear()

            # Check cache first
            if not force_refresh:
                cached = self._cache.get(self.STORAGE_RESOURCE)
                if cached:
                    log_with_context(
                        logger,
                        logging.DEBUG,
                        "Using cached storage token",
                        resource=self.STORAGE_RESOURCE,
                    )
                    return cached

            try:
                token = self._read_token_file(resource=self.STORAGE_RESOURCE)
                self._cache.set(self.STORAGE_RESOURCE, token)
                self._update_token_file_mtime()
                return token
            except AzureAuthError as e:
                log_with_context(
                    logger,
                    logging.WARNING,
                    "Token file configured but unavailable",
                    token_file=self.token_file,
                    error=str(e),
                )
                return None

        if self.use_cli:
            # Check cache first for CLI mode too
            if not force_refresh:
                cached = self._cache.get(self.STORAGE_RESOURCE)
                if cached:
                    log_with_context(
                        logger,
                        logging.DEBUG,
                        "Using cached CLI token",
                        resource=self.STORAGE_RESOURCE,
                    )
                    return cached

            try:
                token = self._fetch_cli_token(self.STORAGE_RESOURCE)
                self._cache.set(self.STORAGE_RESOURCE, token)
                return token
            except AzureAuthError as e:
                log_with_context(
                    logger,
                    logging.WARNING,
                    "Azure CLI token acquisition failed",
                    error=str(e),
                )
                return None

        # SPN mode doesn't use tokens - return None to use credentials
        return None

    def get_storage_options(self, force_refresh: bool = False) -> Dict[str, str]:
        """Get delta-rs / object_store compatible auth options."""
        if self.token_file or self.use_cli:
            token = self.get_storage_token(force_refresh)
            if token:
                return {"azure_storage_token": token}
            return {}

        if self.has_spn_credentials:
            return {
                "azure_client_id": self.client_id,
                "azure_client_secret": self.client_secret,
                "azure_tenant_id": self.tenant_id,
            }

        log_with_context(
            logger,
            logging.WARNING,
            "No Azure credentials configured",
            resource=self.STORAGE_RESOURCE,
        )
        return {}

    def clear_cache(self, resource: Optional[str] = None) -> None:
        """Clear cached tokens."""
        self._cache.clear(resource)


# =============================================================================
# Singleton instance and convenience functions
# =============================================================================

_auth_instance: Optional[AzureAuth] = None


def get_auth() -> AzureAuth:
    """Get or create the singleton AzureAuth instance."""
    global _auth_instance
    if _auth_instance is None:
        _auth_instance = AzureAuth()
    return _auth_instance


def get_storage_options(force_refresh: bool = False) -> Dict[str, str]:
    """Get storage auth options from singleton."""
    return get_auth().get_storage_options(force_refresh)


def clear_token_cache(resource: Optional[str] = None) -> None:
    """Clear token cache on singleton."""
    get_auth().clear_cache(resource)
