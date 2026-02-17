"""Base OAuth2 provider interface."""

import logging
from abc import ABC, abstractmethod

from core.oauth2.models import OAuth2Token

logger = logging.getLogger(__name__)


class BaseOAuth2Provider(ABC):
    """
    Abstract base class for OAuth2 token providers.

    Implementations:
        - GenericOAuth2Provider: Standard OAuth2 client credentials flow (HTTP-based)

    Used by OAuth2TokenManager to manage tokens from multiple providers.
    """

    def __init__(self, provider_name: str):
        """
        Initialize provider.

        Args:
            provider_name: Unique identifier for this provider instance
        """
        self.provider_name = provider_name

    @abstractmethod
    async def acquire_token(self) -> OAuth2Token:
        """
        Acquire a new OAuth2 token.

        Returns:
            OAuth2Token with access token and expiration

        Raises:
            TokenAcquisitionError: If token acquisition fails
        """
        pass

    @abstractmethod
    async def refresh_token(self, token: OAuth2Token) -> OAuth2Token:
        """
        Refresh an existing token.

        Args:
            token: Current token to refresh

        Returns:
            New OAuth2Token

        Raises:
            TokenRefreshError: If refresh fails
        """
        pass


__all__ = ["BaseOAuth2Provider"]
