"""OAuth2 token manager with intelligent caching and refresh."""

import asyncio
import logging
import threading
from core.oauth2.exceptions import TokenAcquisitionError
from core.oauth2.models import OAuth2Token
from core.oauth2.provider import BaseOAuth2Provider

logger = logging.getLogger(__name__)

# Default token refresh buffer (5 minutes before expiry)
DEFAULT_REFRESH_BUFFER_SECONDS = 300


class OAuth2TokenManager:
    """
    Manages OAuth2 tokens with intelligent caching and automatic refresh.

    Provides thread-safe token management for multiple OAuth2 providers.
    Tokens are cached and automatically refreshed when they approach expiration.

    Usage:
        from core.oauth2 import GenericOAuth2Provider, OAuth2Config

        manager = OAuth2TokenManager()
        provider = GenericOAuth2Provider(OAuth2Config(
            provider_name="my_api",
            client_id="...",
            client_secret="...",
            token_url="https://auth.example.com/token",
        ))
        manager.add_provider(provider)

        token = await manager.get_token("my_api")
        headers = {"Authorization": f"Bearer {token}"}
    """

    def __init__(self, refresh_buffer_seconds: int = DEFAULT_REFRESH_BUFFER_SECONDS):
        """
        Initialize token manager.

        Args:
            refresh_buffer_seconds: Time before expiry to trigger refresh (default: 300s)
        """
        self._providers: dict[str, BaseOAuth2Provider] = {}
        self._tokens: dict[str, OAuth2Token] = {}
        self._lock = threading.Lock()
        self._refresh_locks: dict[str, asyncio.Lock] = {}
        self.refresh_buffer_seconds = refresh_buffer_seconds

        logger.debug(
            f"Initialized OAuth2TokenManager with {refresh_buffer_seconds}s refresh buffer"
        )

    def add_provider(self, provider: BaseOAuth2Provider) -> None:
        """
        Register an OAuth2 provider.

        Args:
            provider: OAuth2 provider instance

        Raises:
            ValueError: If provider with same name already exists
        """
        with self._lock:
            if provider.provider_name in self._providers:
                raise ValueError(f"Provider '{provider.provider_name}' already exists")

            self._providers[provider.provider_name] = provider
            self._refresh_locks[provider.provider_name] = asyncio.Lock()

            logger.info(
                f"Registered OAuth2 provider '{provider.provider_name}' "
                f"({provider.__class__.__name__})"
            )

    def get_provider(self, provider_name: str) -> BaseOAuth2Provider:
        """
        Get provider by name.

        Args:
            provider_name: Name of provider

        Returns:
            Provider instance

        Raises:
            KeyError: If provider not found
        """
        with self._lock:
            if provider_name not in self._providers:
                raise KeyError(
                    f"Provider '{provider_name}' not found. "
                    f"Available: {list(self._providers.keys())}"
                )
            return self._providers[provider_name]

    def _get_cached_token(self, provider_name: str) -> str | None:
        """Return cached access token if valid, or None if refresh is needed."""
        with self._lock:
            cached_token = self._tokens.get(provider_name)
            if cached_token and not cached_token.is_expired(self.refresh_buffer_seconds):
                return cached_token.access_token
        return None

    async def _acquire_or_refresh(
        self, provider: BaseOAuth2Provider, provider_name: str,
    ) -> str:
        """Acquire a new token or refresh an existing one."""
        with self._lock:
            current_token = self._tokens.get(provider_name)

        if current_token:
            logger.debug(f"Refreshing token for '{provider_name}'")
            new_token = await provider.refresh_token(current_token)
        else:
            logger.debug(f"Acquiring new token for '{provider_name}'")
            new_token = await provider.acquire_token()

        with self._lock:
            self._tokens[provider_name] = new_token

        logger.info(
            f"Token for '{provider_name}' valid until {new_token.expires_at.isoformat()}"
        )
        return new_token.access_token

    async def get_token(self, provider_name: str, force_refresh: bool = False) -> str:
        """
        Get access token for provider, with automatic caching and refresh.

        Thread-safe. If token is cached and valid, returns immediately.
        If token is expired or close to expiry, refreshes automatically.

        Args:
            provider_name: Name of provider to get token for
            force_refresh: Force token refresh even if cached token is valid

        Returns:
            Access token string

        Raises:
            KeyError: If provider not found
            TokenAcquisitionError: If token acquisition fails
        """
        provider = self.get_provider(provider_name)

        # Check if we have a valid cached token (thread-safe)
        if not force_refresh:
            cached = self._get_cached_token(provider_name)
            if cached is not None:
                logger.debug(f"Using cached token for '{provider_name}'")
                return cached

        # Need to refresh - use async lock to prevent concurrent refreshes
        refresh_lock = self._refresh_locks[provider_name]
        async with refresh_lock:
            # Double-check after acquiring lock (another coroutine may have refreshed)
            if not force_refresh:
                cached = self._get_cached_token(provider_name)
                if cached is not None:
                    logger.debug(f"Token was refreshed by another coroutine for '{provider_name}'")
                    return cached

            try:
                return await self._acquire_or_refresh(provider, provider_name)
            except Exception as e:
                logger.error(f"Failed to get token for '{provider_name}': {e}")
                raise TokenAcquisitionError(
                    f"Failed to get token for '{provider_name}': {e}"
                ) from e

    def clear_token(self, provider_name: str | None = None) -> None:
        """
        Clear cached token(s).

        Args:
            provider_name: Provider to clear token for. If None, clears all tokens.
        """
        with self._lock:
            if provider_name:
                self._tokens.pop(provider_name, None)
                logger.debug(f"Cleared token for '{provider_name}'")
            else:
                self._tokens.clear()
                logger.debug("Cleared all tokens")

    async def close(self) -> None:
        """Clean up resources (close provider sessions if needed)."""
        with self._lock:
            for provider in self._providers.values():
                # Close provider session if it has a close method
                if hasattr(provider, "close"):
                    try:
                        await provider.close()
                    except Exception as e:
                        logger.warning(f"Error closing provider '{provider.provider_name}': {e}")

            self._tokens.clear()
            logger.info("OAuth2TokenManager closed")



__all__ = [
    "OAuth2TokenManager",
    "DEFAULT_REFRESH_BUFFER_SECONDS",
]
