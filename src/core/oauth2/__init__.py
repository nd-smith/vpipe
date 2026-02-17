"""
OAuth2 token management with intelligent caching and automatic refresh.

This module provides a complete OAuth2 token management solution for plugins
and services. It supports multiple OAuth2 providers with automatic token
caching, refresh, and thread-safe operations.

Usage:
    from core.oauth2 import OAuth2TokenManager, GenericOAuth2Provider, OAuth2Config

    config = OAuth2Config(
        provider_name="external_api",
        client_id=os.getenv("CLIENT_ID"),
        client_secret=os.getenv("CLIENT_SECRET"),
        token_url="https://auth.example.com/oauth/token",
        scope="read write"
    )
    provider = GenericOAuth2Provider(config)

    manager = OAuth2TokenManager()
    manager.add_provider(provider)

    token = await manager.get_token("external_api")
    headers = {"Authorization": f"Bearer {token}"}
"""

from core.oauth2.exceptions import (
    InvalidConfigurationError,
    OAuth2Error,
    TokenAcquisitionError,
    TokenRefreshError,
)
from core.oauth2.manager import (
    DEFAULT_REFRESH_BUFFER_SECONDS,
    OAuth2TokenManager,
)
from core.oauth2.models import OAuth2Config, OAuth2Token
from core.oauth2.providers import BaseOAuth2Provider, GenericOAuth2Provider

__all__ = [
    # Manager
    "OAuth2TokenManager",
    "DEFAULT_REFRESH_BUFFER_SECONDS",
    # Providers
    "BaseOAuth2Provider",
    "GenericOAuth2Provider",
    # Models
    "OAuth2Token",
    "OAuth2Config",
    # Exceptions
    "OAuth2Error",
    "TokenAcquisitionError",
    "TokenRefreshError",
    "InvalidConfigurationError",
]
