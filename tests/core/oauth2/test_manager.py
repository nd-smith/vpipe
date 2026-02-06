"""Tests for OAuth2TokenManager."""

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from core.oauth2.exceptions import TokenAcquisitionError
from core.oauth2.manager import OAuth2TokenManager, get_default_manager
from core.oauth2.models import OAuth2Token
from core.oauth2.providers.base import BaseOAuth2Provider


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
        assert manager.list_providers() == []

    def test_custom_refresh_buffer(self):
        """Should accept custom refresh buffer."""
        manager = OAuth2TokenManager(refresh_buffer_seconds=600)
        assert manager.refresh_buffer_seconds == 600

    def test_add_provider(self, manager, mock_provider):
        """Should register provider."""
        manager.add_provider(mock_provider)

        assert "test_provider" in manager.list_providers()
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

    def test_list_providers(self, manager):
        """Should list all registered providers."""
        manager.add_provider(MockOAuth2Provider("provider1"))
        manager.add_provider(MockOAuth2Provider("provider2"))
        manager.add_provider(MockOAuth2Provider("provider3"))

        providers = manager.list_providers()
        assert len(providers) == 3
        assert "provider1" in providers
        assert "provider2" in providers
        assert "provider3" in providers


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

    @pytest.mark.asyncio
    async def test_get_cached_token_info(self, manager, mock_provider):
        """Should provide token info for diagnostics."""
        manager.add_provider(mock_provider)
        await manager.get_token("test_provider")

        info = manager.get_cached_token_info("test_provider")

        assert info is not None
        assert info["provider_name"] == "test_provider"
        assert "expires_at" in info
        assert "remaining_seconds" in info
        assert info["token_type"] == "Bearer"
        assert not info["is_expired"]

    @pytest.mark.asyncio
    async def test_get_cached_token_info_missing(self, manager, mock_provider):
        """Should return None for uncached token."""
        manager.add_provider(mock_provider)

        info = manager.get_cached_token_info("test_provider")
        assert info is None


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

        assert manager.get_cached_token_info("provider1") is None
        assert manager.get_cached_token_info("provider2") is not None

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

        assert manager.get_cached_token_info("provider1") is None
        assert manager.get_cached_token_info("provider2") is None

    @pytest.mark.asyncio
    async def test_clear_nonexistent_token(self, manager):
        """Clearing nonexistent token should not raise error."""
        manager.clear_token("nonexistent")  # Should not raise

    @pytest.mark.asyncio
    async def test_refresh_all(self, manager):
        """Should refresh all cached tokens."""
        provider1 = MockOAuth2Provider("provider1")
        provider2 = MockOAuth2Provider("provider2")
        manager.add_provider(provider1)
        manager.add_provider(provider2)

        await manager.get_token("provider1")
        await manager.get_token("provider2")

        results = await manager.refresh_all()

        assert results["provider1"] is True
        assert results["provider2"] is True
        assert provider1.refresh_count == 1
        assert provider2.refresh_count == 1

    @pytest.mark.asyncio
    async def test_refresh_all_with_failure(self, manager):
        """Should handle failures during refresh_all."""
        provider1 = MockOAuth2Provider("provider1")
        provider2 = MockOAuth2Provider("provider2")
        provider2.should_fail = True

        manager.add_provider(provider1)
        manager.add_provider(provider2)

        await manager.get_token("provider1")

        # Get initial token for provider2 before it starts failing
        provider2.should_fail = False
        await manager.get_token("provider2")
        provider2.should_fail = True

        results = await manager.refresh_all()

        assert results["provider1"] is True
        assert results["provider2"] is False


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
    async def test_concurrent_refresh(self, manager):
        """Concurrent refresh requests should not duplicate refresh."""
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


class TestOAuth2TokenManagerSingleton:
    """Tests for singleton pattern."""

    def test_get_default_manager(self):
        """Should return singleton instance."""
        manager1 = get_default_manager()
        manager2 = get_default_manager()

        assert manager1 is manager2

    def test_default_manager_persistent(self):
        """Default manager should persist across calls."""
        manager = get_default_manager()
        provider = MockOAuth2Provider("test_provider")
        manager.add_provider(provider)

        manager2 = get_default_manager()
        assert "test_provider" in manager2.list_providers()


class TestOAuth2TokenManagerCleanup:
    """Tests for cleanup operations."""

    @pytest.mark.asyncio
    async def test_close(self, manager):
        """Should close without errors."""
        provider = MockOAuth2Provider("test_provider")
        manager.add_provider(provider)
        await manager.get_token("test_provider")

        await manager.close()

        # Tokens should be cleared
        assert manager.get_cached_token_info("test_provider") is None

    @pytest.mark.asyncio
    async def test_close_with_provider_cleanup(self, manager):
        """Should call provider close method if available."""
        provider = MockOAuth2Provider("test_provider")
        provider.close = AsyncMock()

        manager.add_provider(provider)
        await manager.close()

        provider.close.assert_called_once()
