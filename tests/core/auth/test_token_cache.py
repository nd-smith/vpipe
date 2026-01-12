"""
Tests for TokenCache - thread-safe token caching with expiration.

Test Coverage:
    - Basic cache operations (get/set/clear)
    - Token expiration validation
    - Thread safety with concurrent access
    - Timezone handling (UTC)
    - Diagnostics (get_age)
    - Edge cases
"""

import time
import threading
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

import pytest

from core.auth.token_cache import (
    TokenCache,
    CachedToken,
    TOKEN_REFRESH_MINS,
    TOKEN_EXPIRY_MINS,
)


class TestCachedToken:
    """Tests for CachedToken dataclass."""

    def test_is_valid_fresh_token(self):
        """Fresh token should be valid."""
        token = CachedToken("test_token", datetime.now(timezone.utc))
        assert token.is_valid(buffer_mins=50)

    def test_is_valid_expired_token(self):
        """Token older than buffer should be invalid."""
        old_time = datetime.now(timezone.utc) - timedelta(minutes=51)
        token = CachedToken("test_token", old_time)
        assert not token.is_valid(buffer_mins=50)

    def test_is_valid_edge_case(self):
        """Token exactly at buffer edge should be invalid."""
        edge_time = datetime.now(timezone.utc) - timedelta(minutes=50, seconds=1)
        token = CachedToken("test_token", edge_time)
        assert not token.is_valid(buffer_mins=50)

    def test_is_valid_custom_buffer(self):
        """Should respect custom buffer duration."""
        old_time = datetime.now(timezone.utc) - timedelta(minutes=31)
        token = CachedToken("test_token", old_time)
        assert not token.is_valid(buffer_mins=30)
        assert token.is_valid(buffer_mins=40)

    def test_uses_utc_timezone(self):
        """Should use UTC timezone for all comparisons."""
        token = CachedToken("test_token", datetime.now(timezone.utc))
        # If timezone-aware, this should work correctly
        assert token.acquired_at.tzinfo == timezone.utc


class TestTokenCache:
    """Tests for TokenCache class."""

    @pytest.fixture
    def cache(self):
        """Create fresh cache for each test."""
        return TokenCache()

    @pytest.fixture
    def storage_resource(self):
        """Standard storage resource URL."""
        return "https://storage.azure.com/"

    def test_init_empty_cache(self, cache):
        """New cache should be empty."""
        assert cache.get("any_resource") is None

    def test_set_and_get_token(self, cache, storage_resource):
        """Should cache and retrieve token."""
        token = "eyJ0eXAiOiJKV1QiLCJhbGc..."
        cache.set(storage_resource, token)
        assert cache.get(storage_resource) == token

    def test_get_missing_resource(self, cache):
        """Should return None for uncached resource."""
        assert cache.get("https://unknown.resource/") is None

    def test_multiple_resources(self, cache):
        """Should cache multiple resources independently."""
        storage_token = "storage_token_123"
        kusto_token = "kusto_token_456"

        cache.set("https://storage.azure.com/", storage_token)
        cache.set("https://help.kusto.windows.net", kusto_token)

        assert cache.get("https://storage.azure.com/") == storage_token
        assert cache.get("https://help.kusto.windows.net") == kusto_token

    def test_overwrite_existing_token(self, cache, storage_resource):
        """Setting token again should overwrite."""
        cache.set(storage_resource, "old_token")
        cache.set(storage_resource, "new_token")
        assert cache.get(storage_resource) == "new_token"

    def test_expired_token_returns_none(self, cache, storage_resource):
        """Expired token should return None."""
        # Mock an old acquisition time
        old_time = datetime.now(timezone.utc) - timedelta(minutes=51)
        with patch("core.auth.token_cache.datetime") as mock_dt:
            mock_dt.now.return_value = old_time
            cache.set(storage_resource, "old_token")

        # Now check with current time - should be expired
        assert cache.get(storage_resource) is None

    def test_clear_specific_resource(self, cache):
        """Should clear only specified resource."""
        cache.set("https://storage.azure.com/", "token1")
        cache.set("https://help.kusto.windows.net", "token2")

        cache.clear("https://storage.azure.com/")

        assert cache.get("https://storage.azure.com/") is None
        assert cache.get("https://help.kusto.windows.net") == "token2"

    def test_clear_all_resources(self, cache):
        """Should clear all cached tokens."""
        cache.set("https://storage.azure.com/", "token1")
        cache.set("https://help.kusto.windows.net", "token2")

        cache.clear()

        assert cache.get("https://storage.azure.com/") is None
        assert cache.get("https://help.kusto.windows.net") is None

    def test_clear_nonexistent_resource(self, cache):
        """Clearing nonexistent resource should not raise error."""
        cache.clear("https://nonexistent.com/")  # Should not raise

    def test_get_age_cached_token(self, cache, storage_resource):
        """Should return age of cached token."""
        cache.set(storage_resource, "test_token")
        time.sleep(0.1)  # Small delay to ensure measurable age

        age = cache.get_age(storage_resource)
        assert age is not None
        assert age.total_seconds() > 0
        assert age.total_seconds() < 1  # Should be very small

    def test_get_age_missing_token(self, cache):
        """Should return None for uncached resource."""
        assert cache.get_age("https://missing.com/") is None

    def test_get_age_precision(self, cache, storage_resource):
        """Age should be accurate to subsecond precision."""
        cache.set(storage_resource, "test_token")
        time.sleep(0.5)

        age = cache.get_age(storage_resource)
        assert age is not None
        assert 0.4 < age.total_seconds() < 0.6


class TestTokenCacheThreadSafety:
    """Tests for thread safety of TokenCache."""

    @pytest.fixture
    def cache(self):
        """Create fresh cache for each test."""
        return TokenCache()

    def test_concurrent_set_operations(self, cache):
        """Multiple threads setting tokens should be safe."""
        results = []
        errors = []

        def set_token(resource_id):
            try:
                cache.set(f"https://resource{resource_id}.com/", f"token_{resource_id}")
                results.append(resource_id)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=set_token, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(results) == 10
        # Verify all tokens cached correctly
        for i in range(10):
            assert cache.get(f"https://resource{i}.com/") == f"token_{i}"

    def test_concurrent_get_operations(self, cache):
        """Multiple threads getting tokens should be safe."""
        cache.set("https://storage.azure.com/", "test_token")
        results = []
        errors = []

        def get_token():
            try:
                token = cache.get("https://storage.azure.com/")
                results.append(token)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=get_token) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(results) == 10
        assert all(t == "test_token" for t in results)

    def test_concurrent_mixed_operations(self, cache):
        """Concurrent get/set/clear should be safe."""
        cache.set("https://storage.azure.com/", "initial_token")
        errors = []
        operation_counts = {"get": 0, "set": 0, "clear": 0}
        lock = threading.Lock()

        def mixed_operations(op_type):
            try:
                if op_type == "get":
                    cache.get("https://storage.azure.com/")
                elif op_type == "set":
                    cache.set("https://storage.azure.com/", f"token_{op_type}")
                elif op_type == "clear":
                    cache.clear("https://storage.azure.com/")

                with lock:
                    operation_counts[op_type] += 1
            except Exception as e:
                errors.append(e)

        ops = ["get"] * 10 + ["set"] * 5 + ["clear"] * 2
        threads = [threading.Thread(target=mixed_operations, args=(op,)) for op in ops]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert operation_counts["get"] == 10
        assert operation_counts["set"] == 5
        assert operation_counts["clear"] == 2

    def test_concurrent_get_age_operations(self, cache):
        """Multiple threads checking age should be safe."""
        cache.set("https://storage.azure.com/", "test_token")
        results = []
        errors = []

        def check_age():
            try:
                age = cache.get_age("https://storage.azure.com/")
                results.append(age)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=check_age) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(results) == 10
        assert all(age is not None for age in results)


class TestTokenCacheConstants:
    """Tests for token timing constants."""

    def test_refresh_buffer_correct(self):
        """Refresh buffer should be 50 min for 60 min tokens."""
        assert TOKEN_REFRESH_MINS == 50
        assert TOKEN_EXPIRY_MINS == 60
        # Buffer should be less than expiry
        assert TOKEN_REFRESH_MINS < TOKEN_EXPIRY_MINS

    def test_refresh_buffer_safe(self):
        """10-minute safety buffer should prevent expiry during requests."""
        safety_margin = TOKEN_EXPIRY_MINS - TOKEN_REFRESH_MINS
        assert safety_margin >= 10  # At least 10 min buffer


class TestLocalDevelopmentIntegration:
    """
    Tests for local development workflow integration.

    These tests verify the cache works correctly with the token_refresher.py
    pattern where tokens are read from a JSON file and cached.
    """

    @pytest.fixture
    def cache(self):
        """Create fresh cache for each test."""
        return TokenCache()

    def test_cache_miss_triggers_file_read(self, cache):
        """
        Simulate: Cache miss should trigger file read in auth layer.

        In practice:
        1. cache.get() returns None (cache miss)
        2. Auth layer reads tokens.json
        3. Auth layer calls cache.set() with fresh token
        4. Subsequent requests use cache
        """
        resource = "https://storage.azure.com/"

        # Initial cache miss
        assert cache.get(resource) is None

        # Simulate auth layer reading tokens.json and caching
        fresh_token = "eyJ0eXAiOiJKV1QiLCJhbGc..."
        cache.set(resource, fresh_token)

        # Subsequent requests hit cache
        assert cache.get(resource) == fresh_token

    def test_expired_cache_triggers_refresh(self, cache):
        """
        Simulate: Expired cache should trigger file re-read.

        In practice:
        1. Token cached for 50 min
        2. cache.get() returns None after expiry
        3. Auth layer re-reads tokens.json
        4. New token cached for next 50 min
        """
        resource = "https://storage.azure.com/"

        # Cache initial token
        old_time = datetime.now(timezone.utc) - timedelta(minutes=51)
        with patch("core.auth.token_cache.datetime") as mock_dt:
            mock_dt.now.return_value = old_time
            cache.set(resource, "old_token")

        # After 51 min, cache returns None
        assert cache.get(resource) is None

        # Auth layer re-reads tokens.json and caches fresh token
        cache.set(resource, "new_token")
        assert cache.get(resource) == "new_token"

    def test_multiple_services_share_cache(self, cache):
        """
        Simulate: Multiple services can share token cache instance.

        In practice:
        1. TokenCache instance shared across workers
        2. First worker caches token after reading tokens.json
        3. Other workers get cached token (no file read)
        4. All workers re-read after 50 min expiry
        """
        resource = "https://storage.azure.com/"
        token = "shared_token_123"

        # First worker reads and caches
        cache.set(resource, token)

        # Other workers hit cache
        assert cache.get(resource) == token
        assert cache.get(resource) == token

        # All workers benefit from shared cache
        age = cache.get_age(resource)
        assert age is not None
        assert age.total_seconds() < 1
