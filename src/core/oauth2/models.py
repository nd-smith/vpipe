"""OAuth2 data models and configuration."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


@dataclass
class OAuth2Token:    
    access_token: str
    token_type: str
    expires_at: datetime
    scope: str | None = None
    refresh_token: str | None = None

    @classmethod
    def from_response(cls, response: dict, expires_in: int | None = None) -> "OAuth2Token":        
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
