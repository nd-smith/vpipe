"""
Thread-safe token cache with expiration tracking.

This module provides in-memory caching of authentication tokens with automatic
expiration handling. Tokens are cached per resource URL with configurable
expiration buffers to prevent using tokens close to expiry.

Local Development Support:
    For local development, tokens can be managed via external token refresher
    scripts that write to a JSON file. Services read fresh tokens from the file
    by calling auth methods that check the cache first, then fall back to
    reading the token file.

Thread Safety:
    All cache operations are protected by a lock to ensure thread-safe access
    in multi-threaded environments (e.g., async workers, concurrent requests).

Example:
    >>> cache = TokenCache()
    >>> cache.set("https://storage.azure.com/", "eyJ0eXAi...")
    >>> token = cache.get("https://storage.azure.com/")
    >>> if token:
    ...     # Use cached token
    >>> else:
    ...     # Token expired or not cached, fetch new one
"""

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional
import threading


# Token timing constants
TOKEN_REFRESH_MINS = 50  # Refresh before expiry (Azure tokens: 60 min lifetime)
TOKEN_EXPIRY_MINS = 60   # Azure token lifetime


@dataclass
class CachedToken:
    """
    Token with acquisition timestamp for expiration tracking.

    Attributes:
        value: The access token string
        acquired_at: UTC timestamp when token was cached
    """

    value: str
    acquired_at: datetime

    def is_valid(self, buffer_mins: int = TOKEN_REFRESH_MINS) -> bool:
        """
        Check if token is still valid with safety buffer.

        Tokens are considered invalid when they're within buffer_mins of expiry.
        This prevents using tokens that might expire mid-request.

        Args:
            buffer_mins: Minutes before expiry to consider token invalid.
                        Default is 50 min for 60 min Azure tokens.

        Returns:
            True if token age is less than buffer_mins, False otherwise.

        Example:
            >>> token = CachedToken("abc123", datetime.now(timezone.utc))
            >>> token.is_valid(buffer_mins=50)  # True if < 50 min old
        """
        age = datetime.now(timezone.utc) - self.acquired_at
        return age < timedelta(minutes=buffer_mins)


class TokenCache:
    """
    Thread-safe cache for authentication tokens.

    Maintains an in-memory cache of tokens keyed by resource URL. Each token
    tracks its acquisition time and is automatically considered invalid after
    a configurable buffer period.

    Thread Safety:
        All operations (get/set/clear) are protected by a threading.Lock to
        ensure safe concurrent access from multiple threads.

    Local Development:
        This cache is intended to work alongside file-based token management:
        1. token_refresher.py daemon writes fresh tokens to tokens.json
        2. Auth layer checks cache first (fast path)
        3. On cache miss, auth layer reads from tokens.json
        4. Fresh token is cached for subsequent requests
        5. Services re-read tokens.json after 50 min when cache expires

    Example:
        >>> cache = TokenCache()
        >>> # First request caches token
        >>> cache.set("https://storage.azure.com/", "eyJ0eXAi...")
        >>>
        >>> # Subsequent requests use cache (if < 50 min old)
        >>> cached_token = cache.get("https://storage.azure.com/")
        >>>
        >>> # After 50 min, cache returns None
        >>> # Auth layer re-reads tokens.json and caches fresh token
    """

    def __init__(self):
        """Initialize empty token cache with thread lock."""
        self._tokens: Dict[str, CachedToken] = {}
        self._lock = threading.Lock()

    def get(self, resource: str) -> Optional[str]:
        """
        Get cached token if still valid.

        Thread-safe operation that checks cache and validates token age.

        Args:
            resource: Resource URL to look up (e.g., "https://storage.azure.com/")

        Returns:
            Token string if cached and valid, None if expired or not found.

        Example:
            >>> cache.get("https://storage.azure.com/")
            'eyJ0eXAiOiJKV1...'  # or None if expired/missing
        """
        with self._lock:
            cached = self._tokens.get(resource)
            if cached and cached.is_valid():
                return cached.value
            return None

    def set(self, resource: str, token: str) -> None:
        """
        Cache a token with current timestamp.

        Thread-safe operation that stores token with UTC acquisition time.

        Args:
            resource: Resource URL to cache for (e.g., "https://storage.azure.com/")
            token: Access token string to cache

        Example:
            >>> cache.set("https://storage.azure.com/", "eyJ0eXAiOiJKV1...")
        """
        with self._lock:
            self._tokens[resource] = CachedToken(
                value=token, acquired_at=datetime.now(timezone.utc)
            )

    def clear(self, resource: Optional[str] = None) -> None:
        """
        Clear one or all cached tokens.

        Thread-safe operation to invalidate cache entries.

        Args:
            resource: Specific resource URL to clear. If None, clears all tokens.

        Example:
            >>> # Clear specific resource
            >>> cache.clear("https://storage.azure.com/")
            >>>
            >>> # Clear all tokens
            >>> cache.clear()
        """
        with self._lock:
            if resource:
                self._tokens.pop(resource, None)
            else:
                self._tokens.clear()

    def get_age(self, resource: str) -> Optional[timedelta]:
        """
        Get age of cached token for diagnostics.

        Thread-safe operation to check token freshness.

        Args:
            resource: Resource URL to check

        Returns:
            timedelta representing token age, or None if not cached.

        Example:
            >>> age = cache.get_age("https://storage.azure.com/")
            >>> if age and age.total_seconds() > 3000:
            ...     print("Token is over 50 minutes old")
        """
        with self._lock:
            cached = self._tokens.get(resource)
            if cached:
                return datetime.now(timezone.utc) - cached.acquired_at
            return None


__all__ = ["TokenCache", "CachedToken", "TOKEN_REFRESH_MINS", "TOKEN_EXPIRY_MINS"]
