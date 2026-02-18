"""
Azure authentication for pipeline storage operations.

Simplified version supporting:
- Azure CLI token acquisition
- Service Principal (SPN) credentials
- Token file-based auth
- Token caching with automatic refresh
"""

import contextlib
import json
import logging
import os
import shutil
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

logger = logging.getLogger(__name__)

# Token timing constants
TOKEN_REFRESH_MINS = 50  # Refresh before expiry
TOKEN_EXPIRY_MINS = 60  # Azure token lifetime


@dataclass
class CachedToken:
    value: str
    acquired_at: datetime

    def is_valid(self, buffer_mins: int = TOKEN_REFRESH_MINS) -> bool:
        """Check if token is still valid with buffer."""
        age = datetime.now(UTC) - self.acquired_at
        return age < timedelta(minutes=buffer_mins)


class TokenCache:
    def __init__(self):
        self._tokens: dict[str, CachedToken] = {}

    def get(self, resource: str) -> str | None:
        """Get cached token if still valid."""
        cached = self._tokens.get(resource)
        if cached and cached.is_valid():
            return cached.value
        return None

    def set(self, resource: str, token: str) -> None:
        """Cache a token."""
        self._tokens[resource] = CachedToken(value=token, acquired_at=datetime.now(UTC))

    def clear(self, resource: str | None = None) -> None:
        """Clear one or all cached tokens."""
        if resource:
            self._tokens.pop(resource, None)
            logger.debug("Cleared token cache", extra={"resource": resource})
        else:
            self._tokens.clear()
            logger.debug("Cleared all token caches")


class AzureAuthError(Exception):
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

    def __init__(self, cache: TokenCache | None = None):
        self._cache = cache or TokenCache()
        self._token_file_mtime: float | None = None
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
                logger.debug(
                    "Token file modified, will re-read",
                    extra={"token_file": self.token_file},
                )
                return True
            return False
        except OSError:
            return True

    def _update_token_file_mtime(self) -> None:
        """Update the cached file modification time after successful read."""
        if self.token_file:
            with contextlib.suppress(OSError):
                self._token_file_mtime = os.path.getmtime(self.token_file)

    def _lookup_json_token(self, tokens: dict, resource: str) -> str:
        """Look up a resource token in a JSON token dict (exact then normalized match)."""
        if resource in tokens:
            logger.debug("Read token from JSON file", extra={"token_file": self.token_file, "resource": resource})
            return tokens[resource]

        # Try normalized match (with/without trailing slash)
        normalized = resource.rstrip("/")
        for key, value in tokens.items():
            if normalized == key.rstrip("/"):
                logger.debug("Read token from JSON file (normalized match)", extra={"token_file": self.token_file, "resource": resource})
                return value

        raise AzureAuthError(
            f"Resource '{resource}' not found in token file. Available: {list(tokens.keys())}"
        )

    def _read_token_file(self, resource: str | None = None) -> str:
        """
        Read token from file.

        Supports two formats:
        1. JSON file with resource-keyed tokens: {"https://storage.azure.com/": "token", ...}
        2. Plain text file with single token (legacy)
        """
        if not self.token_file:
            raise AzureAuthError("AZURE_TOKEN_FILE not set")

        try:
            with open(self.token_file, encoding="utf-8-sig") as f:
                content = f.read().strip()

            if not content:
                raise AzureAuthError(f"Token file is empty: {self.token_file}")

            # Try to parse as JSON (multi-resource token file)
            try:
                tokens = json.loads(content)
                if isinstance(tokens, dict):
                    return self._lookup_json_token(tokens, resource or self.STORAGE_RESOURCE)
            except json.JSONDecodeError:
                pass

            # Plain text format: entire file content is the token
            logger.debug("Read token from plain text file", extra={"token_file": self.token_file})
            return content

        except FileNotFoundError as e:
            raise AzureAuthError(f"Token file not found: {self.token_file}") from e
        except OSError as e:
            raise AzureAuthError(f"Failed to read token file: {self.token_file}") from e

    def _fetch_cli_token(self, resource: str) -> str:
        """Fetch fresh token from Azure CLI."""
        az_path = shutil.which("az")
        if not az_path:
            raise AzureAuthError("Azure CLI not found. Ensure 'az' is installed and in PATH.")

        cmd = [az_path, "account", "get-access-token", "--resource", resource]
        if self.tenant_id:
            cmd.extend(["--tenant", self.tenant_id])
        cmd.extend(["--query", "accessToken", "-o", "tsv"])

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
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

            logger.debug(
                "Acquired token from Azure CLI",
                extra={"resource": resource},
            )
            return token

        except subprocess.TimeoutExpired as e:
            raise AzureAuthError(f"Azure CLI token request timed out for {resource}") from e

    def _get_cached_or_fetch(
        self, fetch_fn, force_refresh: bool, error_label: str
    ) -> str | None:
        """Check cache, then fetch via fetch_fn, caching on success. Returns None on error."""
        if not force_refresh:
            cached = self._cache.get(self.STORAGE_RESOURCE)
            if cached:
                logger.debug("Using cached token", extra={"resource": self.STORAGE_RESOURCE})
                return cached
        try:
            token = fetch_fn()
            self._cache.set(self.STORAGE_RESOURCE, token)
            return token
        except AzureAuthError as e:
            logger.warning(error_label, extra={"error": str(e)})
            return None

    def get_storage_token(self, force_refresh: bool = False) -> str | None:
        """Get token for OneLake/ADLS storage operations."""
        if self.token_file:
            if self._is_token_file_modified():
                self._cache.clear()

            def _fetch_file():
                token = self._read_token_file(resource=self.STORAGE_RESOURCE)
                self._update_token_file_mtime()
                return token

            return self._get_cached_or_fetch(_fetch_file, force_refresh, "Token file configured but unavailable")

        if self.use_cli:
            return self._get_cached_or_fetch(
                lambda: self._fetch_cli_token(self.STORAGE_RESOURCE),
                force_refresh,
                "Azure CLI token acquisition failed",
            )

        # SPN mode doesn't use tokens - return None to use credentials
        return None

    def get_storage_options(self, force_refresh: bool = False) -> dict[str, str]:
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

        logger.warning(
            "No Azure credentials configured",
            extra={"resource": self.STORAGE_RESOURCE},
        )
        return {}

    def clear_cache(self, resource: str | None = None) -> None:
        """Clear cached tokens."""
        self._cache.clear(resource)


# =============================================================================
# Singleton instance and convenience functions
# =============================================================================

_auth_instance: AzureAuth | None = None


def get_auth() -> AzureAuth:
    """Get or create the singleton AzureAuth instance."""
    global _auth_instance
    if _auth_instance is None:
        _auth_instance = AzureAuth()
        # Log singleton creation to help diagnose auth issues
        logger.debug(
            "Created AzureAuth singleton",
            extra={
                "auth_mode": _auth_instance.auth_mode,
                "token_file": _auth_instance.token_file or "not_set",
            },
        )
    return _auth_instance


def _mask_credential(value: str | None, visible_chars: int = 4) -> str:
    """Mask a credential showing only first N chars for verification."""
    if not value:
        return "<not_set>"
    if len(value) <= visible_chars:
        return "***"
    return f"{value[:visible_chars]}...({len(value)} chars)"


def _ensure_ssl_cert_env() -> None:
    """Propagate CA bundle path to SSL_CERT_FILE for delta-rs.

    Corporate environments often set REQUESTS_CA_BUNDLE or CURL_CA_BUNDLE
    for Python HTTP libraries, but delta-rs uses Rust's native TLS which
    only reads SSL_CERT_FILE. If SSL_CERT_FILE is not set, propagate
    from the other env vars so delta-rs can verify certificates.
    """
    if os.getenv("SSL_CERT_FILE"):
        return

    ca_bundle = os.getenv("REQUESTS_CA_BUNDLE") or os.getenv("CURL_CA_BUNDLE")
    if ca_bundle:
        os.environ["SSL_CERT_FILE"] = ca_bundle
        logger.info(
            "Set SSL_CERT_FILE for delta-rs TLS verification",
            extra={"ca_bundle": ca_bundle},
        )


def get_storage_options(force_refresh: bool = False) -> dict[str, str]:
    """Get storage auth options from singleton."""
    auth = get_auth()
    opts = auth.get_storage_options(force_refresh)

    # Ensure delta-rs can find the CA bundle for SSL verification
    _ensure_ssl_cert_env()

    # Log detailed auth context for debugging 403 errors
    auth_mode = auth.auth_mode
    logger.info(
        "Storage auth context",
        extra={"auth_mode": auth_mode, "force_refresh": force_refresh},
    )

    if auth_mode == "spn":
        # Log partial SPN credentials for verification (first 4 chars only)
        logger.info(
            "SPN credentials in use",
            extra={
                "client_id_prefix": _mask_credential(auth.client_id),
                "tenant_id_prefix": _mask_credential(auth.tenant_id),
                "client_secret_set": bool(auth.client_secret),
                "client_secret_prefix": _mask_credential(auth.client_secret),
            },
        )
    elif auth_mode == "file":
        token = opts.get("azure_storage_token")
        logger.info(
            "Token file auth in use",
            extra={
                "token_file": auth.token_file,
                "token_prefix": _mask_credential(token),
            },
        )
    elif auth_mode == "cli":
        token = opts.get("azure_storage_token")
        logger.info(
            "CLI auth in use",
            extra={"token_prefix": _mask_credential(token)},
        )
    elif auth_mode == "none":
        logger.warning("No Azure credentials configured - operations will fail with 403")

    # Also log the option keys being returned
    if opts:
        logger.debug(
            "Storage options returned",
            extra={"option_keys": list(opts.keys())},
        )
    else:
        logger.warning("Storage options returned empty")
    return opts


def clear_token_cache(resource: str | None = None) -> None:
    """Clear token cache on singleton."""
    get_auth().clear_cache(resource)
