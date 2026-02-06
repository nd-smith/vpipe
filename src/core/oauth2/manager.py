"""OAuth2 token manager with intelligent caching and refresh."""

import asyncio
import logging
import threading
from typing import Any

from core.oauth2.exceptions import OAuth2Error, TokenAcquisitionError
from core.oauth2.models import OAuth2Token
from core.oauth2.providers.base import BaseOAuth2Provider

logger = logging.getLogger(__name__)

# Default token refresh buffer (5 minutes before expiry)
DEFAULT_REFRESH_BUFFER_SECONDS = 300


class OAuth2TokenManager:
    """
    Manages OAuth2 tokens with intelligent caching and automatic refresh.

    Provides thread-safe token management for multiple OAuth2 providers.
    Tokens are cached and automatically refreshed when they approach expiration.

    Usage:
        manager = OAuth2TokenManager()

        # Add provider
        azure_provider = AzureADProvider(
            provider_name="azure_storage",
            client_id="...",
            client_secret="...",
            tenant_id="...",
            scopes=["https://storage.azure.com/.default"]
        )
        manager.add_provider(azure_provider)

        # Get token (cached, auto-refreshes when needed)
        token = await manager.get_token("azure_storage")

        # Use token
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
                raise ValueError(
                    f"Provider '{provider.provider_name}' already exists"
                )

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
            with self._lock:
                cached_token = self._tokens.get(provider_name)
                if cached_token and not cached_token.is_expired(
                    self.refresh_buffer_seconds
                ):
                    logger.debug(
                        f"Using cached token for '{provider_name}' "
                        f"(expires in {cached_token.remaining_lifetime.total_seconds()}s)"
                    )
                    return cached_token.access_token

        # Need to refresh - use async lock to prevent concurrent refreshes
        refresh_lock = self._refresh_locks[provider_name]
        async with refresh_lock:
            # Double-check after acquiring lock (another coroutine may have refreshed)
            if not force_refresh:
                with self._lock:
                    cached_token = self._tokens.get(provider_name)
                    if cached_token and not cached_token.is_expired(
                        self.refresh_buffer_seconds
                    ):
                        logger.debug(
                            f"Token was refreshed by another coroutine for '{provider_name}'"
                        )
                        return cached_token.access_token

            # Acquire or refresh token
            try:
                with self._lock:
                    current_token = self._tokens.get(provider_name)

                if current_token:
                    logger.debug(f"Refreshing token for '{provider_name}'")
                    new_token = await provider.refresh_token(current_token)
                else:
                    logger.debug(f"Acquiring new token for '{provider_name}'")
                    new_token = await provider.acquire_token()

                # Cache the new token (thread-safe)
                with self._lock:
                    self._tokens[provider_name] = new_token

                logger.info(
                    f"Token for '{provider_name}' valid until "
                    f"{new_token.expires_at.isoformat()}"
                )

                return new_token.access_token

            except Exception as e:
                logger.error(f"Failed to get token for '{provider_name}': {e}")
                raise TokenAcquisitionError(
                    f"Failed to get token for '{provider_name}': {e}"
                )

    async def refresh_all(self) -> dict[str, bool]:
        """
        Force refresh all cached tokens.

        Returns:
            Dict mapping provider names to success status

        Raises:
            OAuth2Error: If any refresh fails
        """
        results = {}

        for provider_name in list(self._providers.keys()):
            try:
                await self.get_token(provider_name, force_refresh=True)
                results[provider_name] = True
            except Exception as e:
                logger.error(f"Failed to refresh token for '{provider_name}': {e}")
                results[provider_name] = False

        return results

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

    def get_cached_token_info(self, provider_name: str) -> dict[str, Any] | None:
        """
        Get information about cached token for diagnostics.

        Args:
            provider_name: Name of provider

        Returns:
            Dict with token info, or None if no token cached
        """
        with self._lock:
            token = self._tokens.get(provider_name)
            if not token:
                return None

            return {
                "provider_name": provider_name,
                "expires_at": token.expires_at.isoformat(),
                "remaining_seconds": token.remaining_lifetime.total_seconds(),
                "is_expired": token.is_expired(self.refresh_buffer_seconds),
                "token_type": token.token_type,
                "scope": token.scope,
            }

    def list_providers(self) -> list[str]:
        """Get list of registered provider names."""
        with self._lock:
            return list(self._providers.keys())

    async def close(self) -> None:
        """Clean up resources (close provider sessions if needed)."""
        with self._lock:
            for provider in self._providers.values():
                # Close provider session if it has a close method
                if hasattr(provider, "close"):
                    try:
                        await provider.close()
                    except Exception as e:
                        logger.warning(
                            f"Error closing provider '{provider.provider_name}': {e}"
                        )

            self._tokens.clear()
            logger.info("OAuth2TokenManager closed")


# Singleton instance for default usage
_default_manager: OAuth2TokenManager | None = None
_manager_lock = threading.Lock()


def get_default_manager() -> OAuth2TokenManager:
    """
    Get or create the default OAuth2TokenManager singleton.

    Returns:
        Default OAuth2TokenManager instance
    """
    global _default_manager

    if _default_manager is None:
        with _manager_lock:
            if _default_manager is None:
                _default_manager = OAuth2TokenManager()

    return _default_manager


__all__ = [
    "OAuth2TokenManager",
    "get_default_manager",
    "DEFAULT_REFRESH_BUFFER_SECONDS",
]
