"""Generic OAuth2 provider for standard OAuth2 servers."""

import logging

import aiohttp

from core.oauth2.exceptions import (
    InvalidConfigurationError,
    TokenAcquisitionError,
)
from core.oauth2.models import OAuth2Config, OAuth2Token
from core.oauth2.providers.base import BaseOAuth2Provider

logger = logging.getLogger(__name__)


class GenericOAuth2Provider(BaseOAuth2Provider):
    """
    Generic OAuth2 provider supporting client credentials flow.

    Works with any OAuth2-compliant server. Uses client_credentials grant type
    to acquire tokens for machine-to-machine authentication.
    """

    def __init__(self, config: OAuth2Config):
        """
        Initialize generic OAuth2 provider.

        Args:
            config: OAuth2 configuration

        Raises:
            InvalidConfigurationError: If required parameters are missing
        """
        super().__init__(config.provider_name)

        if not all([config.client_id, config.client_secret, config.token_url]):
            raise InvalidConfigurationError("client_id, client_secret, and token_url are required")

        self.config = config
        self._session: aiohttp.ClientSession | None = None

        logger.debug(
            f"Initialized generic OAuth2 provider '{config.provider_name}'",
            extra={"token_url": config.token_url},
        )

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP client session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def acquire_token(self) -> OAuth2Token:
        """
        Acquire token using client credentials flow.

        Returns:
            OAuth2Token with access token

        Raises:
            TokenAcquisitionError: If token acquisition fails
        """
        session = await self._ensure_session()

        request_data = {
            "grant_type": "client_credentials",
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
        }

        scope = self.config.get_scope_string()
        if scope:
            request_data["scope"] = scope

        if self.config.additional_params:
            request_data.update(self.config.additional_params)

        try:
            async with session.post(
                self.config.token_url,
                data=request_data,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(
                        f"Token acquisition failed for '{self.provider_name}': "
                        f"HTTP {response.status}",
                        extra={"error": error_text[:200]},
                    )
                    raise TokenAcquisitionError(f"HTTP {response.status}: {error_text[:200]}")

                response_data = await response.json()

                logger.debug(
                    f"Acquired token for '{self.provider_name}'",
                    extra={"expires_in": response_data.get("expires_in")},
                )

                return OAuth2Token.from_response(response_data)

        except aiohttp.ClientError as e:
            logger.error(f"HTTP error during token acquisition for '{self.provider_name}': {e}")
            raise TokenAcquisitionError(f"HTTP error: {e}") from e
        except Exception as e:
            logger.error(
                f"Unexpected error during token acquisition for '{self.provider_name}': {e}"
            )
            raise TokenAcquisitionError(f"Token acquisition failed: {e}") from e

    async def refresh_token(self, token: OAuth2Token) -> OAuth2Token:
        """
        Refresh token using refresh_token grant.

        If current token has a refresh_token, uses that. Otherwise, acquires
        a new token using client credentials.

        Args:
            token: Current token to refresh

        Returns:
            New OAuth2Token

        Raises:
            TokenRefreshError: If refresh fails
        """
        if not token.refresh_token:
            # No refresh token, acquire new token
            return await self.acquire_token()

        session = await self._ensure_session()

        request_data = {
            "grant_type": "refresh_token",
            "refresh_token": token.refresh_token,
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
        }

        try:
            async with session.post(
                self.config.token_url,
                data=request_data,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as response:
                if response.status != 200:
                    await response.text()
                    logger.warning(
                        f"Token refresh failed for '{self.provider_name}', "
                        f"will acquire new token: HTTP {response.status}"
                    )
                    # Fall back to acquiring new token
                    return await self.acquire_token()

                response_data = await response.json()

                logger.debug(f"Refreshed token for '{self.provider_name}'")

                return OAuth2Token.from_response(response_data)

        except Exception as e:
            logger.warning(
                f"Token refresh failed for '{self.provider_name}', will acquire new token: {e}"
            )
            # Fall back to acquiring new token
            return await self.acquire_token()

    async def close(self) -> None:
        """Close HTTP client session."""
        if self._session and not self._session.closed:
            await self._session.close()


__all__ = ["GenericOAuth2Provider"]
