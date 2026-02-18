"""Tests for core.oauth2.exceptions module."""

from core.oauth2.exceptions import (
    InvalidConfigurationError,
    OAuth2Error,
    TokenAcquisitionError,
    TokenRefreshError,
)


class TestOAuth2Exceptions:
    def test_oauth2_error_is_exception(self):
        err = OAuth2Error("test")
        assert isinstance(err, Exception)
        assert str(err) == "test"

    def test_token_acquisition_error(self):
        err = TokenAcquisitionError("failed to acquire")
        assert isinstance(err, OAuth2Error)
        assert str(err) == "failed to acquire"

    def test_token_refresh_error(self):
        err = TokenRefreshError("failed to refresh")
        assert isinstance(err, OAuth2Error)

    def test_invalid_configuration_error(self):
        err = InvalidConfigurationError("bad config")
        assert isinstance(err, OAuth2Error)
