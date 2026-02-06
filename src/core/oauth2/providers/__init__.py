"""OAuth2 provider implementations."""

from core.oauth2.providers.azure import AzureADProvider
from core.oauth2.providers.base import BaseOAuth2Provider
from core.oauth2.providers.generic import GenericOAuth2Provider

__all__ = ["BaseOAuth2Provider", "AzureADProvider", "GenericOAuth2Provider"]
