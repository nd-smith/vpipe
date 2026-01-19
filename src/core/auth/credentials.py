"""Azure credential provider with multi-mode support: CLI, SPN (secret/cert), token file, and managed identity.

Thread-safe via TokenCache. All methods attempt configured auth modes in priority order.
"""

import logging
import os
import subprocess
import shutil
from typing import Dict, Optional, Union
from pathlib import Path

from core.auth.token_cache import TokenCache

try:
    from azure.identity import (
        ClientSecretCredential,
        CertificateCredential,
        DefaultAzureCredential,
    )
    from azure.core.credentials import AccessToken
    AZURE_IDENTITY_AVAILABLE = True
except ImportError:
    ClientSecretCredential = None
    CertificateCredential = None
    DefaultAzureCredential = None
    AccessToken = None
    AZURE_IDENTITY_AVAILABLE = False


logger = logging.getLogger(__name__)

STORAGE_RESOURCE = "https://storage.azure.com/"
STORAGE_SCOPE = "https://storage.azure.com/.default"


class AzureAuthError(Exception):
    pass


class AzureCredentialProvider:
    """Unified Azure credential provider with multi-mode support.

    Auth priority: token file > cached token > CLI > SPN (cert > secret) > DefaultAzureCredential.
    Thread-safe via TokenCache. Credentials created once and reused.
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
        self._cache = cache or TokenCache()
        self._credential: Optional[Union[ClientSecretCredential, CertificateCredential, DefaultAzureCredential]] = None
        self._token_file_mtime: Optional[float] = None
        self._token_file_cache: Dict[str, str] = {}

        self.use_cli = use_cli
        self.use_default_credential = use_default_credential
        self.token_file = token_file
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.certificate_path = certificate_path

        if not any([use_cli, use_default_credential, token_file, client_id]):
            self._load_config_from_env()

    def _load_config_from_env(self) -> None:
        self.use_cli = os.getenv("AZURE_AUTH_INTERACTIVE", "").lower() == "true"
        self.token_file = os.getenv("AZURE_TOKEN_FILE")
        self.client_id = os.getenv("AZURE_CLIENT_ID")
        self.client_secret = os.getenv("AZURE_CLIENT_SECRET")
        self.tenant_id = os.getenv("AZURE_TENANT_ID")
        self.certificate_path = os.getenv("AZURE_CERTIFICATE_PATH")

        if not any([self.use_cli, self.token_file, self.client_id]):
            self.use_default_credential = True

    def _is_token_file_modified(self) -> bool:
        if not self.token_file:
            return False

        try:
            current_mtime = os.path.getmtime(self.token_file)
            if self._token_file_mtime is None:
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
            return True

    @property
    def has_spn_credentials(self) -> bool:
        has_secret = all([self.client_id, self.client_secret, self.tenant_id])
        has_cert = all([self.client_id, self.certificate_path, self.tenant_id])
        return has_secret or has_cert

    @property
    def auth_mode(self):
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
        if self._credential is not None:
            return self._credential

        if not AZURE_IDENTITY_AVAILABLE:
            raise AzureAuthError(
                "azure-identity library not installed. "
                "Install with: pip install azure-identity"
            )

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

        if self.use_default_credential:
            logger.info("Using DefaultAzureCredential (managed identity, env vars, etc.)")
            self._credential = DefaultAzureCredential()
            return self._credential

        raise AzureAuthError(
            "No valid Azure credential configuration found. "
            "Please configure one of: CLI, SPN (secret/cert), or DefaultAzureCredential"
        )

    def _read_token_file(self, resource: str) -> str:
        """Read token from file with mtime-based caching. Supports JSON dict or plain text."""
        if not self.token_file:
            raise AzureAuthError(
                "Token file authentication requested but AZURE_TOKEN_FILE not set"
            )

        if not self._is_token_file_modified():
            if resource in self._token_file_cache:
                logger.debug(
                    "Using cached token from file",
                    extra={"token_file": self.token_file, "resource": resource}
                )
                return self._token_file_cache[resource]
            for cached_resource in self._token_file_cache:
                if resource.rstrip("/") == cached_resource.rstrip("/"):
                    logger.debug(
                        "Using cached token from file (normalized match)",
                        extra={"resource": resource, "cached_key": cached_resource}
                    )
                    return self._token_file_cache[cached_resource]

        token_path = Path(self.token_file)
        if not token_path.exists():
            raise AzureAuthError(
                f"Token file not found: {self.token_file}\n"
                f"Hint: Ensure token_refresher is running or file path is correct"
            )

        try:
            content = token_path.read_text(encoding="utf-8-sig").strip()
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

        try:
            import json
            tokens = json.loads(content)

            if isinstance(tokens, dict):
                self._token_file_cache = tokens.copy()
                logger.debug(
                    "Refreshed token file cache",
                    extra={
                        "token_file": self.token_file,
                        "resources": list(tokens.keys()),
                    }
                )

                if resource in tokens:
                    return tokens[resource]

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
            self._token_file_cache = {"": content}
            logger.debug(
                "Read token from plain text file (legacy format)",
                extra={"token_file": self.token_file}
            )
            return content

    def _fetch_cli_token(self, resource: str) -> str:
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

                if proc:
                    proc.kill()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        pass

                if attempt < max_attempts - 1:
                    import time
                    time.sleep(5)
                    continue

                raise AzureAuthError(
                    f"Azure CLI token request timed out after {max_attempts} attempts\n"
                    f"Resource: {resource}\n"
                    f"Hint: Check network connectivity and Azure CLI installation"
                )

        raise AzureAuthError("Unexpected error in CLI token fetch")

    def get_token_for_resource(
        self,
        resource: str,
        force_refresh: bool = False
    ) -> str:
        """Get access token. Priority: token file > cache > CLI > SPN/DefaultAzureCredential."""
        if self.token_file:
            try:
                return self._read_token_file(resource)
            except AzureAuthError:
                logger.warning(
                    "Token file configured but failed to read, trying other methods",
                    extra={"token_file": self.token_file}
                )

        if not force_refresh:
            cached = self._cache.get(resource)
            if cached:
                logger.debug("Using cached token", extra={"resource": resource})
                return cached

        if self.use_cli:
            token = self._fetch_cli_token(resource)
            self._cache.set(resource, token)
            return token

        try:
            credential = self._get_azure_credential()
            scope = resource.rstrip("/") + "/.default"
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
            logger.error(
                "Failed to get storage token - authentication will fail at access time"
            )
            return {}

    def get_kusto_token(self, cluster_uri: str, force_refresh: bool = False) -> str:
        return self.get_token_for_resource(cluster_uri, force_refresh)

    def clear_cache(self, resource: Optional[str] = None) -> None:
        self._cache.clear(resource)
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
        self._token_file_cache.clear()
        self._token_file_mtime = None
        logger.debug("Cleared file token cache")

    def get_diagnostics(self) -> Dict:
        diag = {
            "auth_mode": self.auth_mode,
            "cli_enabled": self.use_cli,
            "default_credential_enabled": self.use_default_credential,
            "spn_configured": self.has_spn_credentials,
            "token_file": self.token_file,
        }

        storage_age = self._cache.get_age(STORAGE_RESOURCE)
        if storage_age:
            diag["storage_token_age_seconds"] = storage_age.total_seconds()

        if self.token_file:
            diag["file_cache_resources"] = list(self._token_file_cache.keys())
            diag["file_cache_mtime"] = self._token_file_mtime

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
