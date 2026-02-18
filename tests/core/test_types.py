"""Tests for core.types module."""

from core.types import ErrorCategory, TokenProvider


class TestErrorCategory:
    def test_values(self):
        assert ErrorCategory.TRANSIENT.value == "transient"
        assert ErrorCategory.AUTH.value == "auth"
        assert ErrorCategory.PERMANENT.value == "permanent"
        assert ErrorCategory.CIRCUIT_OPEN.value == "circuit_open"
        assert ErrorCategory.UNKNOWN.value == "unknown"

    def test_all_members(self):
        expected = {"TRANSIENT", "AUTH", "PERMANENT", "CIRCUIT_OPEN", "UNKNOWN"}
        assert set(ErrorCategory.__members__.keys()) == expected

    def test_from_value(self):
        assert ErrorCategory("transient") is ErrorCategory.TRANSIENT


class TestTokenProvider:
    def test_is_protocol(self):
        """TokenProvider is a Protocol — verify it has expected methods."""
        assert hasattr(TokenProvider, "get_token")
        assert hasattr(TokenProvider, "refresh_token")
