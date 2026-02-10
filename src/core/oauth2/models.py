"""OAuth2 data models and configuration."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


@dataclass
class OAuth2Token:
    """
    OAuth2 access token with expiration tracking.

    Attributes:
        access_token: The access token string
        token_type: Token type (typically "Bearer")
        expires_at: UTC timestamp when token expires
        scope: Space-separated scopes granted
        refresh_token: Optional refresh token for token renewal
    """

    access_token: str
    token_type: str
    expires_at: datetime
    scope: str | None = None
    refresh_token: str | None = None

    @classmethod
    def from_response(cls, response: dict, expires_in: int | None = None) -> "OAuth2Token":
        """
        Create token from OAuth2 token response.

        Args:
            response: OAuth2 token response dict
            expires_in: Optional override for expires_in (seconds)

        Returns:
            OAuth2Token instance
        """
        expires_in = expires_in or response.get("expires_in", 3600)
        expires_at = datetime.now(UTC) + timedelta(seconds=expires_in)

        return cls(
            access_token=response["access_token"],
            token_type=response.get("token_type", "Bearer"),
            expires_at=expires_at,
            scope=response.get("scope"),
            refresh_token=response.get("refresh_token"),
        )

    def is_expired(self, buffer_seconds: int = 300) -> bool:
        """
        Check if token is expired or close to expiry.

        Args:
            buffer_seconds: Safety buffer before actual expiry (default: 5 minutes)

        Returns:
            True if token should be refreshed
        """
        return datetime.now(UTC) >= self.expires_at - timedelta(seconds=buffer_seconds)

    @property
    def remaining_lifetime(self) -> timedelta:
        """Get remaining time before token expires."""
        return self.expires_at - datetime.now(UTC)


@dataclass
class OAuth2Config:
    """
    OAuth2 provider configuration.

    Attributes:
        provider_name: Unique identifier for this provider
        client_id: OAuth2 client ID
        client_secret: OAuth2 client secret
        token_url: Token endpoint URL
        scope: Space-separated or list of scopes to request
        additional_params: Additional parameters for token request
    """

    provider_name: str
    client_id: str
    client_secret: str
    token_url: str
    scope: str | list[str] | None = None
    additional_params: dict[str, str] | None = None

    def get_scope_string(self) -> str:
        """Get scope as space-separated string."""
        if not self.scope:
            return ""
        if isinstance(self.scope, list):
            return " ".join(self.scope)
        return self.scope


__all__ = ["OAuth2Token", "OAuth2Config"]
