"""Tests for OAuth2TokenManager."""

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from core.oauth2.exceptions import TokenAcquisitionError
from core.oauth2.manager import OAuth2TokenManager
from core.oauth2.models import OAuth2Token
from core.oauth2.provider import BaseOAuth2Provider


class MockOAuth2Provider(BaseOAuth2Provider):
    """Mock OAuth2 provider for testing."""

    def __init__(self, provider_name: str, token_lifetime_seconds: int = 3600):
        super().__init__(provider_name)
        self.acquire_count = 0
        self.refresh_count = 0
        self.token_lifetime_seconds = token_lifetime_seconds
        self.should_fail = False

    async def acquire_token(self) -> OAuth2Token:
        """Acquire a mock token."""
        if self.should_fail:
            raise TokenAcquisitionError("Mock acquisition failure")

        self.acquire_count += 1
        return OAuth2Token(
            access_token=f"token_{self.acquire_count}",
            token_type="Bearer",
            expires_at=datetime.now(UTC) + timedelta(seconds=self.token_lifetime_seconds),
            scope="read write",
        )

    async def refresh_token(self, token: OAuth2Token) -> OAuth2Token:
        """Refresh a mock token."""
        if self.should_fail:
            raise TokenAcquisitionError("Mock refresh failure")

        self.refresh_count += 1
        return OAuth2Token(
            access_token=f"refreshed_token_{self.refresh_count}",
            token_type="Bearer",
            expires_at=datetime.now(UTC) + timedelta(seconds=self.token_lifetime_seconds),
            scope="read write",
        )


@pytest.fixture
def manager():
    """Create fresh manager for each test."""
    return OAuth2TokenManager()


@pytest.fixture
def mock_provider():
    """Create mock provider."""
    return MockOAuth2Provider("test_provider")


class TestOAuth2TokenManagerBasics:
    """Tests for basic OAuth2TokenManager operations."""

    def test_init(self):
        """Should initialize with default settings."""
        manager = OAuth2TokenManager()
        assert manager.refresh_buffer_seconds == 300

    def test_custom_refresh_buffer(self):
        """Should accept custom refresh buffer."""
        manager = OAuth2TokenManager(refresh_buffer_seconds=600)
        assert manager.refresh_buffer_seconds == 600

    def test_add_provider(self, manager, mock_provider):
        """Should register provider."""
        manager.add_provider(mock_provider)

        assert manager.get_provider("test_provider") == mock_provider

    def test_add_duplicate_provider(self, manager, mock_provider):
        """Should reject duplicate provider names."""
        manager.add_provider(mock_provider)

        with pytest.raises(ValueError, match="already exists"):
            manager.add_provider(MockOAuth2Provider("test_provider"))

    def test_get_missing_provider(self, manager):
        """Should raise KeyError for missing provider."""
        with pytest.raises(KeyError, match="not found"):
            manager.get_provider("nonexistent")


class TestOAuth2TokenManagerTokenAcquisition:
    """Tests for token acquisition and caching."""

    @pytest.mark.asyncio
    async def test_get_token_first_time(self, manager, mock_provider):
        """Should acquire token on first request."""
        manager.add_provider(mock_provider)

        token = await manager.get_token("test_provider")

        assert token == "token_1"
        assert mock_provider.acquire_count == 1
        assert mock_provider.refresh_count == 0

    @pytest.mark.asyncio
    async def test_get_token_cached(self, manager, mock_provider):
        """Should return cached token on subsequent requests."""
        manager.add_provider(mock_provider)

        token1 = await manager.get_token("test_provider")
        token2 = await manager.get_token("test_provider")

        assert token1 == token2
        assert mock_provider.acquire_count == 1
        assert mock_provider.refresh_count == 0

    @pytest.mark.asyncio
    async def test_get_token_force_refresh(self, manager, mock_provider):
        """Should refresh token when forced."""
        manager.add_provider(mock_provider)

        token1 = await manager.get_token("test_provider")
        token2 = await manager.get_token("test_provider", force_refresh=True)

        assert token1 != token2
        assert mock_provider.acquire_count == 1
        assert mock_provider.refresh_count == 1

    @pytest.mark.asyncio
    async def test_get_token_expired(self, manager):
        """Should refresh expired token."""
        # Provider with very short token lifetime
        provider = MockOAuth2Provider("test_provider", token_lifetime_seconds=1)
        manager.add_provider(provider)

        token1 = await manager.get_token("test_provider")

        # Wait for token to expire
        await asyncio.sleep(1.5)

        token2 = await manager.get_token("test_provider")

        assert token1 != token2
        assert provider.acquire_count == 1
        assert provider.refresh_count == 1

    @pytest.mark.asyncio
    async def test_get_token_acquisition_failure(self, manager, mock_provider):
        """Should raise error on acquisition failure."""
        mock_provider.should_fail = True
        manager.add_provider(mock_provider)

        with pytest.raises(TokenAcquisitionError):
            await manager.get_token("test_provider")


class TestOAuth2TokenManagerCacheManagement:
    """Tests for cache management operations."""

    @pytest.mark.asyncio
    async def test_clear_specific_token(self, manager):
        """Should clear specific provider's token."""
        provider1 = MockOAuth2Provider("provider1")
        provider2 = MockOAuth2Provider("provider2")
        manager.add_provider(provider1)
        manager.add_provider(provider2)

        await manager.get_token("provider1")
        await manager.get_token("provider2")

        manager.clear_token("provider1")

        # provider1 should re-acquire (cache was cleared)
        token = await manager.get_token("provider1")
        assert provider1.acquire_count == 2

        # provider2 should still use cache
        token = await manager.get_token("provider2")
        assert provider2.acquire_count == 1

    @pytest.mark.asyncio
    async def test_clear_all_tokens(self, manager):
        """Should clear all cached tokens."""
        provider1 = MockOAuth2Provider("provider1")
        provider2 = MockOAuth2Provider("provider2")
        manager.add_provider(provider1)
        manager.add_provider(provider2)

        await manager.get_token("provider1")
        await manager.get_token("provider2")

        manager.clear_token()

        # Both should re-acquire
        await manager.get_token("provider1")
        await manager.get_token("provider2")
        assert provider1.acquire_count == 2
        assert provider2.acquire_count == 2

    @pytest.mark.asyncio
    async def test_clear_nonexistent_token(self, manager):
        """Clearing nonexistent token should not raise error."""
        manager.clear_token("nonexistent")  # Should not raise


class TestOAuth2TokenManagerConcurrency:
    """Tests for concurrent token operations."""

    @pytest.mark.asyncio
    async def test_concurrent_get_token(self, manager, mock_provider):
        """Concurrent requests should not duplicate token acquisition."""
        manager.add_provider(mock_provider)

        # Make 10 concurrent requests
        tasks = [manager.get_token("test_provider") for _ in range(10)]
        tokens = await asyncio.gather(*tasks)

        # All should get the same token
        assert all(t == tokens[0] for t in tokens)

        # Should only acquire once (not 10 times)
        assert mock_provider.acquire_count == 1

    @pytest.mark.asyncio
    async def test_concurrent_refresh(self):
        """Concurrent refresh requests should not duplicate refresh."""
        # Use manager with small refresh buffer for short-lived test tokens
        manager = OAuth2TokenManager(refresh_buffer_seconds=0)
        provider = MockOAuth2Provider("test_provider", token_lifetime_seconds=1)
        manager.add_provider(provider)

        # Get initial token
        await manager.get_token("test_provider")

        # Wait for expiry
        await asyncio.sleep(1.5)

        # Make 10 concurrent requests
        tasks = [manager.get_token("test_provider") for _ in range(10)]
        tokens = await asyncio.gather(*tasks)

        # All should get the same refreshed token
        assert all(t == tokens[0] for t in tokens)

        # Should only refresh once (not 10 times)
        assert provider.refresh_count == 1


class TestOAuth2TokenManagerCleanup:
    """Tests for cleanup operations."""

    @pytest.mark.asyncio
    async def test_close(self, manager):
        """Should close without errors."""
        provider = MockOAuth2Provider("test_provider")
        manager.add_provider(provider)
        await manager.get_token("test_provider")

        await manager.close()

        # Token cache should be cleared — next get_token will re-acquire
        token = await manager.get_token("test_provider")
        assert provider.acquire_count == 2

    @pytest.mark.asyncio
    async def test_close_with_provider_cleanup(self, manager):
        """Should call provider close method if available."""
        provider = MockOAuth2Provider("test_provider")
        provider.close = AsyncMock()

        manager.add_provider(provider)
        await manager.close()

        provider.close.assert_called_once()
