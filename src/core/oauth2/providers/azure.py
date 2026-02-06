"""Azure AD OAuth2 provider."""

import logging
from datetime import UTC, datetime

from core.oauth2.exceptions import InvalidConfigurationError, TokenAcquisitionError
from core.oauth2.models import OAuth2Token
from core.oauth2.providers.base import BaseOAuth2Provider

try:
    from azure.identity import ClientSecretCredential

    AZURE_IDENTITY_AVAILABLE = True
except ImportError:
    ClientSecretCredential = None
    AZURE_IDENTITY_AVAILABLE = False

logger = logging.getLogger(__name__)


class AzureADProvider(BaseOAuth2Provider):
    """
    Azure AD OAuth2 provider using client credentials flow.

    Uses azure-identity library's ClientSecretCredential for token acquisition.
    Tokens are scoped to specific resources (e.g., https://storage.azure.com/.default).
    """

    def __init__(
        self,
        provider_name: str,
        client_id: str,
        client_secret: str,
        tenant_id: str,
        scopes: list[str] | None = None,
    ):
        """
        Initialize Azure AD provider.

        Args:
            provider_name: Unique identifier for this provider
            client_id: Azure AD application (client) ID
            client_secret: Azure AD client secret
            tenant_id: Azure AD tenant ID
            scopes: OAuth scopes to request (e.g., ["https://storage.azure.com/.default"])

        Raises:
            InvalidConfigurationError: If azure-identity is not installed or params invalid
        """
        super().__init__(provider_name)

        if not AZURE_IDENTITY_AVAILABLE:
            raise InvalidConfigurationError(
                "azure-identity library not installed. "
                "Install with: pip install azure-identity"
            )

        if not all([client_id, client_secret, tenant_id]):
            raise InvalidConfigurationError(
                "client_id, client_secret, and tenant_id are required"
            )

        self.client_id = client_id
        self.tenant_id = tenant_id
        self.scopes = scopes or ["https://management.azure.com/.default"]

        # Don't log the secret
        logger.debug(
            f"Initialized Azure AD provider '{provider_name}'",
            extra={"tenant_id": tenant_id, "client_id": client_id},
        )

        self._credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
        )

    async def acquire_token(self) -> OAuth2Token:
        """
        Acquire token from Azure AD.

        Returns:
            OAuth2Token with access token

        Raises:
            TokenAcquisitionError: If token acquisition fails
        """
        try:
            # Azure SDK's get_token is synchronous, but we're in async context
            # Run in executor would be ideal, but for now we'll call it directly
            access_token = self._credential.get_token(*self.scopes)

            # Azure SDK returns expires_on as Unix timestamp (int)
            # Convert to datetime for OAuth2Token
            if isinstance(access_token.expires_on, int):
                expires_at = datetime.fromtimestamp(access_token.expires_on, UTC)
            else:
                # Already a datetime object
                expires_at = access_token.expires_on

            logger.debug(
                f"Acquired Azure AD token for '{self.provider_name}'",
                extra={"expires_at": expires_at.isoformat()},
            )

            return OAuth2Token(
                access_token=access_token.token,
                token_type="Bearer",
                expires_at=expires_at,
                scope=" ".join(self.scopes),
            )
        except Exception as e:
            logger.error(
                f"Failed to acquire Azure AD token for '{self.provider_name}': {e}"
            )
            raise TokenAcquisitionError(f"Azure AD token acquisition failed: {e}")

    async def refresh_token(self, token: OAuth2Token) -> OAuth2Token:
        """
        Refresh token by acquiring a new one.

        Azure AD doesn't use refresh tokens in client credentials flow,
        so we just acquire a new token.

        Args:
            token: Current token (ignored)

        Returns:
            New OAuth2Token

        Raises:
            TokenAcquisitionError: If token acquisition fails
        """
        return await self.acquire_token()


__all__ = ["AzureADProvider"]
