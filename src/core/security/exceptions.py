"""Security validation exceptions."""


class ValidationError(ValueError):
    """Base class for validation errors."""

    pass


class URLValidationError(ValidationError):
    """Raised when URL validation fails."""

    pass


class FileValidationError(ValidationError):
    """Raised when file type validation fails."""

    pass
