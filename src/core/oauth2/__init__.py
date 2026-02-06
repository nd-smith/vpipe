"""
OAuth2 token management with intelligent caching and automatic refresh.

This module provides a complete OAuth2 token management solution for plugins
and services. It supports multiple OAuth2 providers (Azure AD, generic OAuth2)
with automatic token caching, refresh, and thread-safe operations.

Basic Usage:
    from core.oauth2 import OAuth2TokenManager, AzureADProvider

    # Create and configure manager
    manager = OAuth2TokenManager()

    # Add Azure AD provider
    azure_provider = AzureADProvider(
        provider_name="my_azure_api",
        client_id=os.getenv("AZURE_CLIENT_ID"),
        client_secret=os.getenv("AZURE_CLIENT_SECRET"),
        tenant_id=os.getenv("AZURE_TENANT_ID"),
        scopes=["https://management.azure.com/.default"]
    )
    manager.add_provider(azure_provider)

    # Get token (automatically cached and refreshed)
    token = await manager.get_token("my_azure_api")

    # Use token in HTTP request
    headers = {"Authorization": f"Bearer {token}"}

Using Default Singleton:
    from core.oauth2 import get_default_manager, AzureADProvider

    manager = get_default_manager()
    manager.add_provider(azure_provider)
    token = await manager.get_token("my_azure_api")

Generic OAuth2:
    from core.oauth2 import GenericOAuth2Provider, OAuth2Config

    config = OAuth2Config(
        provider_name="external_api",
        client_id=os.getenv("CLIENT_ID"),
        client_secret=os.getenv("CLIENT_SECRET"),
        token_url="https://auth.example.com/oauth/token",
        scope="read write"
    )
    provider = GenericOAuth2Provider(config)
    manager.add_provider(provider)

    token = await manager.get_token("external_api")
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
    get_default_manager,
)
from core.oauth2.models import OAuth2Config, OAuth2Token
from core.oauth2.providers import AzureADProvider, BaseOAuth2Provider, GenericOAuth2Provider

__all__ = [
    # Manager
    "OAuth2TokenManager",
    "get_default_manager",
    "DEFAULT_REFRESH_BUFFER_SECONDS",
    # Providers
    "BaseOAuth2Provider",
    "AzureADProvider",
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
