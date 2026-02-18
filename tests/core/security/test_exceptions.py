"""Tests for core.security.exceptions module."""

from core.security.exceptions import (
    FileValidationError,
    URLValidationError,
    ValidationError,
)


class TestSecurityExceptions:
    def test_validation_error_is_value_error(self):
        err = ValidationError("invalid")
        assert isinstance(err, ValueError)

    def test_url_validation_error(self):
        err = URLValidationError("bad url")
        assert isinstance(err, ValidationError)
        assert str(err) == "bad url"

    def test_file_validation_error(self):
        err = FileValidationError("bad file")
        assert isinstance(err, ValidationError)
        assert str(err) == "bad file"
