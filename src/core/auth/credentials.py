"""Azure credential provider with token file and SPN secret support.

Thread-safe via TokenCache. Auth priority: token file > cached token > SPN secret.
"""

import logging
import os
from typing import Dict, Optional
from pathlib import Path

from core.auth.token_cache import TokenCache

try:
    from azure.identity import ClientSecretCredential
    from azure.core.credentials import AccessToken

    AZURE_IDENTITY_AVAILABLE = True
except ImportError:
    ClientSecretCredential = None
    AccessToken = None
    AZURE_IDENTITY_AVAILABLE = False


logger = logging.getLogger(__name__)

STORAGE_RESOURCE = "https://storage.azure.com/"
STORAGE_SCOPE = "https://storage.azure.com/.default"


class AzureAuthError(Exception):
    pass


class AzureCredentialProvider:
    """Azure credential provider with token file and SPN secret support.

    Auth priority: token file > cached token > SPN secret.
    Thread-safe via TokenCache. Credentials created once and reused.
    """

    def __init__(
        self,
        cache: Optional[TokenCache] = None,
        token_file: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        tenant_id: Optional[str] = None,
    ):
        self._cache = cache or TokenCache()
        self._credential: Optional[ClientSecretCredential] = None

        self.token_file = token_file
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id

        if not any([token_file, client_id]):
            self._load_config_from_env()

    def _load_config_from_env(self) -> None:
        self.token_file = os.getenv("AZURE_TOKEN_FILE")
        self.client_id = os.getenv("AZURE_CLIENT_ID")
        self.client_secret = os.getenv("AZURE_CLIENT_SECRET")
        self.tenant_id = os.getenv("AZURE_TENANT_ID")

    @property
    def has_spn_credentials(self) -> bool:
        return all([self.client_id, self.client_secret, self.tenant_id])

    @property
    def auth_mode(self):
        if self.token_file:
            return "file"
        if self.has_spn_credentials:
            return "spn_secret"
        return "none"

    def _get_azure_credential(self):
        if self._credential is not None:
            return self._credential

        if not AZURE_IDENTITY_AVAILABLE:
            raise AzureAuthError(
                "azure-identity library not installed. " "Install with: pip install azure-identity"
            )

        if self.client_secret and self.client_id and self.tenant_id:
            logger.debug(
                "Using Service Principal authentication",
                extra={"tenant_id": self.tenant_id, "client_id": self.client_id},
            )

            self._credential = ClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
            return self._credential

        raise AzureAuthError(
            "No valid Azure credential configuration found. "
            "Please configure token file or SPN secret (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)"
        )

    def _read_token_file(self, resource: str) -> str:
        """Read token from file. Supports JSON dict or plain text."""
        if not self.token_file:
            raise AzureAuthError("Token file authentication requested but AZURE_TOKEN_FILE not set")

        token_path = Path(self.token_file)
        if not token_path.exists():
            raise AzureAuthError(
                f"Token file not found: {self.token_file}\n"
                f"Hint: Ensure token_refresher is running or file path is correct"
            )

        try:
            content = token_path.read_text(encoding="utf-8-sig").strip()
        except IOError as e:
            raise AzureAuthError(
                f"Failed to read token file: {self.token_file}\n" f"Error: {str(e)}"
            ) from e

        if not content:
            raise AzureAuthError(
                f"Token file is empty: {self.token_file}\n"
                f"Hint: Check if token_refresher is running properly"
            )

        try:
            import json

            tokens = json.loads(content)

            if isinstance(tokens, dict):
                logger.debug(
                    "Read token file with multiple resources",
                    extra={
                        "token_file": self.token_file,
                        "resources": list(tokens.keys()),
                    },
                )

                if resource in tokens:
                    return tokens[resource]

                # Try normalized match (trailing slash handling)
                for key in tokens:
                    if resource.rstrip("/") == key.rstrip("/"):
                        return tokens[key]

                available = list(tokens.keys())
                raise AzureAuthError(
                    f"Resource '{resource}' not found in token file\n"
                    f"Available resources: {available}\n"
                    f"Hint: Run token_refresher with correct resource scopes"
                )

        except json.JSONDecodeError:
            logger.debug(
                "Read token from plain text file (legacy format)",
                extra={"token_file": self.token_file},
            )
            return content

    def get_token_for_resource(self, resource: str, force_refresh: bool = False) -> str:
        """Get access token. Priority: token file > cache > SPN secret."""
        if self.token_file:
            try:
                token = self._read_token_file(resource)
                # Cache token from file for subsequent requests
                self._cache.set(resource, token)
                return token
            except AzureAuthError:
                logger.warning(
                    "Token file configured but failed to read, trying SPN authentication",
                    extra={"token_file": self.token_file},
                )

        if not force_refresh:
            cached = self._cache.get(resource)
            if cached:
                logger.debug("Using cached token", extra={"resource": resource})
                return cached

        try:
            credential = self._get_azure_credential()
            scope = resource.rstrip("/") + "/.default"
            access_token: AccessToken = credential.get_token(scope)
            token = access_token.token

            self._cache.set(resource, token)
            logger.debug("Acquired token from Service Principal", extra={"resource": resource})
            return token

        except Exception as e:
            raise AzureAuthError(
                f"Failed to acquire token for {resource}\n"
                f"Auth mode: {self.auth_mode}\n"
                f"Error: {str(e)}"
            ) from e

    def get_storage_token(self, force_refresh: bool = False) -> str:
        return self.get_token_for_resource(STORAGE_RESOURCE, force_refresh)

    def get_storage_options(self, force_refresh: bool = False) -> Dict[str, str]:
        """Get storage options for delta-rs. Returns SPN credentials directly if available, else token."""
        if self.client_secret and self.client_id and self.tenant_id and not self.token_file:
            return {
                "azure_client_id": self.client_id,
                "azure_client_secret": self.client_secret,
                "azure_tenant_id": self.tenant_id,
            }

        try:
            token = self.get_storage_token(force_refresh)
            return {"azure_storage_token": token}
        except AzureAuthError:
            logger.error("Failed to get storage token - authentication will fail at access time")
            return {}

    def get_kusto_token(self, cluster_uri: str, force_refresh: bool = False) -> str:
        return self.get_token_for_resource(cluster_uri, force_refresh)

    def clear_cache(self, resource: Optional[str] = None) -> None:
        self._cache.clear(resource)
        logger.debug("Cleared token cache", extra={"resource": resource if resource else "all"})

    def get_diagnostics(self) -> Dict:
        diag = {
            "auth_mode": self.auth_mode,
            "spn_configured": self.has_spn_credentials,
            "token_file": self.token_file,
        }

        storage_age = self._cache.get_age(STORAGE_RESOURCE)
        if storage_age:
            diag["storage_token_age_seconds"] = storage_age.total_seconds()

        return diag


_default_provider: Optional[AzureCredentialProvider] = None


def get_default_provider() -> AzureCredentialProvider:
    global _default_provider
    if _default_provider is None:
        _default_provider = AzureCredentialProvider()
    return _default_provider


def get_storage_options(force_refresh: bool = False) -> Dict[str, str]:
    return get_default_provider().get_storage_options(force_refresh)


def clear_token_cache(resource: Optional[str] = None) -> None:
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
