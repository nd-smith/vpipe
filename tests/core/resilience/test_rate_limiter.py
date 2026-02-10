"""
Tests for token bucket rate limiter.

Test Coverage:
    - RateLimiterConfig initialization
    - Token bucket algorithm (refill and consumption)
    - Blocking behavior when rate limit reached
    - Context manager (acquire_context)
    - Disabled rate limiter behavior
    - Registry pattern (get_rate_limiter)
    - Decorator usage (rate_limited)
    - Concurrent async calls
"""

import asyncio
import time

import pytest

from core.resilience.rate_limiter import (
    CLAIMX_API_RATE_CONFIG,
    RateLimiter,
    RateLimiterConfig,
    get_rate_limiter,
    rate_limited,
)


class TestRateLimiterConfig:
    """Tests for RateLimiterConfig dataclass."""

    def test_default_values(self):
        """RateLimiterConfig uses sensible defaults."""
        config = RateLimiterConfig()
        assert config.calls_per_second == 10.0
        assert config.burst_capacity is None
        assert config.enabled is True
        assert config.name == "rate_limiter"

    def test_custom_values(self):
        """Can override all config values."""
        config = RateLimiterConfig(
            calls_per_second=20.0,
            burst_capacity=40.0,
            enabled=False,
            name="custom",
        )
        assert config.calls_per_second == 20.0
        assert config.burst_capacity == 40.0
        assert config.enabled is False
        assert config.name == "custom"


class TestRateLimiterBasics:
    """Tests for basic rate limiter functionality."""

    @pytest.mark.asyncio
    async def test_init_with_config(self):
        """Can initialize with RateLimiterConfig."""
        config = RateLimiterConfig(calls_per_second=5.0, name="test")
        limiter = RateLimiter(config=config)
        assert limiter.config.calls_per_second == 5.0
        assert limiter.config.name == "test"

    @pytest.mark.asyncio
    async def test_init_with_shorthand(self):
        """Can initialize with shorthand parameters."""
        limiter = RateLimiter(calls_per_second=15.0, enabled=True)
        assert limiter.config.calls_per_second == 15.0
        assert limiter.config.enabled is True

    @pytest.mark.asyncio
    async def test_burst_capacity_defaults_to_rate(self):
        """Burst capacity defaults to calls_per_second."""
        limiter = RateLimiter(calls_per_second=10.0)
        assert limiter._burst_capacity == 10.0

    @pytest.mark.asyncio
    async def test_custom_burst_capacity(self):
        """Can set custom burst capacity."""
        config = RateLimiterConfig(calls_per_second=10.0, burst_capacity=20.0)
        limiter = RateLimiter(config=config)
        assert limiter._burst_capacity == 20.0

    @pytest.mark.asyncio
    async def test_starts_with_full_bucket(self):
        """Limiter starts with full token bucket."""
        limiter = RateLimiter(calls_per_second=10.0)
        assert limiter._tokens == limiter._burst_capacity


class TestRateLimiterAcquire:
    """Tests for acquire() method."""

    @pytest.mark.asyncio
    async def test_acquire_consumes_token(self):
        """acquire() consumes one token."""
        limiter = RateLimiter(calls_per_second=10.0)
        initial_tokens = limiter._tokens

        await limiter.acquire()

        assert limiter._tokens == initial_tokens - 1.0

    @pytest.mark.asyncio
    async def test_acquire_multiple_tokens(self):
        """acquire() can consume multiple tokens."""
        limiter = RateLimiter(calls_per_second=10.0)
        initial_tokens = limiter._tokens

        await limiter.acquire(tokens=3.0)

        assert limiter._tokens == initial_tokens - 3.0

    @pytest.mark.asyncio
    async def test_acquire_blocks_when_insufficient_tokens(self):
        """acquire() blocks when not enough tokens available."""
        limiter = RateLimiter(calls_per_second=10.0)  # 10 tokens/sec

        # Consume all tokens
        await limiter.acquire(tokens=10.0)
        assert limiter._tokens == 0.0

        # Next acquire should block
        start = time.monotonic()
        await limiter.acquire(tokens=1.0)
        elapsed = time.monotonic() - start

        # Should have waited ~0.1s (1 token / 10 tokens per sec)
        assert 0.08 <= elapsed <= 0.2

    @pytest.mark.asyncio
    async def test_acquire_refills_tokens_over_time(self):
        """Tokens refill based on elapsed time."""
        limiter = RateLimiter(calls_per_second=10.0)

        # Consume some tokens
        await limiter.acquire(tokens=5.0)
        assert limiter._tokens == 5.0

        # Wait for refill (0.5s = 5 tokens at 10/sec)
        await asyncio.sleep(0.5)

        # Next acquire should not block (tokens refilled)
        await limiter.acquire(tokens=1.0)
        # Should have refilled to full capacity
        assert limiter._tokens >= 9.0

    @pytest.mark.asyncio
    async def test_acquire_caps_tokens_at_burst_capacity(self):
        """Token refill caps at burst capacity."""
        limiter = RateLimiter(calls_per_second=10.0)  # burst_capacity=10.0

        # Wait longer than needed to refill
        await asyncio.sleep(2.0)  # 20 tokens worth, but capped at 10

        # Should have full burst capacity, not more
        assert limiter._tokens == limiter._burst_capacity

    @pytest.mark.asyncio
    async def test_acquire_exceeding_burst_raises_error(self):
        """Requesting more tokens than burst capacity raises ValueError."""
        limiter = RateLimiter(calls_per_second=10.0)  # burst_capacity=10.0

        with pytest.raises(ValueError, match="exceeds burst capacity"):
            await limiter.acquire(tokens=15.0)

    @pytest.mark.asyncio
    async def test_acquire_disabled_limiter_does_not_block(self):
        """acquire() returns immediately when rate limiting is disabled."""
        config = RateLimiterConfig(calls_per_second=1.0, enabled=False)
        limiter = RateLimiter(config=config)

        # Consume all tokens and more (should not block)
        start = time.monotonic()
        for _ in range(20):
            await limiter.acquire()
        elapsed = time.monotonic() - start

        # Should complete immediately
        assert elapsed < 0.1


class TestRateLimiterContextManager:
    """Tests for acquire_context context manager."""

    @pytest.mark.asyncio
    async def test_acquire_context_success(self):
        """acquire_context works as context manager."""
        limiter = RateLimiter(calls_per_second=10.0)

        async with limiter.acquire_context():
            pass  # Should not raise

        # Token should be consumed
        assert limiter._tokens < 10.0

    @pytest.mark.asyncio
    async def test_acquire_context_with_multiple_tokens(self):
        """acquire_context can consume multiple tokens."""
        limiter = RateLimiter(calls_per_second=10.0)
        initial_tokens = limiter._tokens

        async with limiter.acquire_context(tokens=3.0):
            pass

        assert limiter._tokens == initial_tokens - 3.0


class TestRateLimiterStats:
    """Tests for get_stats method."""

    def test_get_stats(self):
        """get_stats returns current state."""
        config = RateLimiterConfig(calls_per_second=5.0, enabled=True, name="test")
        limiter = RateLimiter(config=config)

        stats = limiter.get_stats()
        assert stats["name"] == "test"
        assert stats["enabled"] is True
        assert stats["calls_per_second"] == 5.0
        assert stats["burst_capacity"] == 5.0
        assert "tokens_available" in stats
        assert "last_update" in stats


class TestRateLimitedDecorator:
    """Tests for rate_limited decorator."""

    @pytest.mark.asyncio
    async def test_decorator_rate_limits_function(self):
        """Decorator applies rate limiting to function."""
        limiter = RateLimiter(calls_per_second=10.0)

        @rate_limited(limiter)
        async def func():
            return "success"

        # Consume all tokens
        await limiter.acquire(tokens=10.0)

        # Decorated call should block
        start = time.monotonic()
        result = await func()
        elapsed = time.monotonic() - start

        assert result == "success"
        assert elapsed >= 0.08  # Should have waited for token refill

    @pytest.mark.asyncio
    async def test_decorator_with_custom_token_count(self):
        """Decorator can consume multiple tokens per call."""
        limiter = RateLimiter(calls_per_second=10.0)
        initial_tokens = limiter._tokens

        @rate_limited(limiter, tokens=5.0)
        async def func():
            return "success"

        await func()
        assert limiter._tokens == initial_tokens - 5.0

    @pytest.mark.asyncio
    async def test_decorator_preserves_function_metadata(self):
        """Decorator preserves function name and docstring."""
        limiter = RateLimiter(calls_per_second=10.0)

        @rate_limited(limiter)
        async def my_function():
            """My docstring."""
            return "success"

        assert my_function.__name__ == "my_function"
        assert my_function.__doc__ == "My docstring."


class TestRateLimiterRegistry:
    """Tests for rate limiter registry."""

    def test_get_rate_limiter_creates_new(self):
        """get_rate_limiter creates new limiter on first call."""
        from core.resilience.rate_limiter import _rate_limiters

        _rate_limiters.clear()

        limiter = get_rate_limiter("test_registry")
        assert limiter.config.name == "test_registry"
        assert "test_registry" in _rate_limiters

    def test_get_rate_limiter_returns_existing(self):
        """get_rate_limiter returns existing limiter."""
        from core.resilience.rate_limiter import _rate_limiters

        _rate_limiters.clear()

        limiter1 = get_rate_limiter("test_registry")
        limiter2 = get_rate_limiter("test_registry")
        assert limiter1 is limiter2

    def test_get_rate_limiter_with_config(self):
        """get_rate_limiter uses provided config."""
        from core.resilience.rate_limiter import _rate_limiters

        _rate_limiters.clear()

        config = RateLimiterConfig(calls_per_second=20.0, name="custom")
        limiter = get_rate_limiter("custom", config)
        assert limiter.config.calls_per_second == 20.0


class TestRateLimiterConcurrency:
    """Tests for concurrent async calls."""

    @pytest.mark.asyncio
    async def test_concurrent_acquires_respect_rate(self):
        """Multiple concurrent acquires respect rate limit."""
        limiter = RateLimiter(calls_per_second=10.0)

        # Consume all tokens first
        await limiter.acquire(tokens=10.0)

        # Launch 5 concurrent acquires
        start = time.monotonic()
        await asyncio.gather(*[limiter.acquire() for _ in range(5)])
        elapsed = time.monotonic() - start

        # Should take at least 0.5s (5 tokens / 10 per sec)
        assert elapsed >= 0.4

    @pytest.mark.asyncio
    async def test_concurrent_decorated_calls(self):
        """Multiple concurrent decorated calls are rate limited."""
        limiter = RateLimiter(calls_per_second=10.0)
        call_count = 0

        @rate_limited(limiter)
        async def func():
            nonlocal call_count
            call_count += 1
            return "success"

        # Consume all tokens
        await limiter.acquire(tokens=10.0)

        # Launch concurrent calls
        results = await asyncio.gather(*[func() for _ in range(3)])

        assert len(results) == 3
        assert all(r == "success" for r in results)
        assert call_count == 3


class TestStandardConfigs:
    """Tests for standard rate limiter configurations."""

    def test_claimx_api_rate_config(self):
        """CLAIMX_API_RATE_CONFIG has expected structure."""
        # Config values come from env vars with defaults
        assert CLAIMX_API_RATE_CONFIG.name == "claimx_api"
        assert isinstance(CLAIMX_API_RATE_CONFIG.calls_per_second, float)
        assert isinstance(CLAIMX_API_RATE_CONFIG.enabled, bool)
