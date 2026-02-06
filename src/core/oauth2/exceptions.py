"""OAuth2-specific exceptions."""


class OAuth2Error(Exception):
    """Base exception for OAuth2 operations."""

    pass


class TokenAcquisitionError(OAuth2Error):
    """Token could not be acquired from the provider."""

    pass


class TokenRefreshError(OAuth2Error):
    """Token refresh failed."""

    pass


class InvalidConfigurationError(OAuth2Error):
    """OAuth2 provider configuration is invalid."""

    pass


__all__ = [
    "OAuth2Error",
    "TokenAcquisitionError",
    "TokenRefreshError",
    "InvalidConfigurationError",
]
