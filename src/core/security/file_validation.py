"""
File type validation for attachment downloads.

Implements FR-2.2.1: Validate files against allowed file type list.
Supports validation by file extension and Content-Type header.
"""

from urllib.parse import parse_qs, urlparse

from core.security.exceptions import FileValidationError

# Allowed file extensions (case-insensitive)
ALLOWED_EXTENSIONS: set[str] = {
    # Documents
    "pdf",
    "xml",
    "txt",
    "csv",
    "json",
    "html",
    "htm",
    "xls",
    "xlsx",
    "doc",
    "docx",
    "esx",
    # Images
    "jpg",
    "jpeg",
    "png",
    "gif",
    "bmp",
    "tiff",
    "tif",
    "webp",
    "mov",
    "mp4",
}

# Allowed MIME types (Content-Type header values)
ALLOWED_CONTENT_TYPES: set[str] = {
    # Documents
    "application/pdf",
    "application/xml",
    "text/xml",
    "text/plain",
    # Images
    "image/jpeg",
    "image/png",
    "image/gif",
    "image/bmp",
    "image/tiff",
    "image/webp",
    "video/quicktime",
    "video/mp4",
}

# Extension to primary MIME type mapping (for validation)
# All values are sets to allow multiple valid MIME types per extension
EXTENSION_TO_MIME: dict[str, set[str]] = {
    "pdf": {"application/pdf"},
    "xml": {"application/xml", "text/xml"},
    "txt": {"text/plain"},
    "jpg": {"image/jpeg"},
    "jpeg": {"image/jpeg"},
    "png": {"image/png"},
    "gif": {"image/gif"},
    "bmp": {"image/bmp"},
    "tiff": {"image/tiff"},
    "tif": {"image/tiff"},
    "webp": {"image/webp"},
    "mov": {"video/quicktime"},
    "mp4": {"video/mp4"},
}


def _extension_from_path(path: str) -> str | None:
    """Extract extension from a file path segment, stripping query/fragment."""
    if "." not in path:
        return None
    extension = path.rsplit(".", 1)[-1].lower()
    extension = extension.split("?")[0].split("#")[0]
    return extension or None


def _extension_from_query(query: str) -> str | None:
    """Extract extension from URL query params (filename, file, name)."""
    try:
        query_params = parse_qs(query)
        for param_name in ("filename", "file", "name"):
            if param_name in query_params and query_params[param_name]:
                result = _extension_from_path(query_params[param_name][0])
                if result:
                    return result
    except Exception:
        pass
    return None


def extract_extension(filename_or_url: str) -> str | None:
    """Extract file extension, checking URL path then 'filename' query param as fallback.

    Returns lowercase extension without dot, or None if not found.
    """
    if not filename_or_url:
        return None

    # Try to parse as URL first
    parsed = None
    try:
        parsed = urlparse(filename_or_url)
        path = parsed.path if parsed.path else filename_or_url
    except Exception:
        path = filename_or_url

    result = _extension_from_path(path)
    if result:
        return result

    # Fallback: check query parameters for filename
    if parsed and parsed.query:
        return _extension_from_query(parsed.query)

    return None


def normalize_content_type(content_type: str) -> str:
    """Normalize Content-Type to lowercase MIME type without parameters."""
    if not content_type:
        return ""

    # Split on semicolon to remove parameters
    mime_type = content_type.split(";")[0].strip().lower()
    return mime_type


def validate_file_type(
    filename_or_url: str,
    content_type: str | None = None,
    allowed_extensions: set[str] | None = None,
    allowed_content_types: set[str] | None = None,
) -> None:
    """
    Validate file type against allowed extensions and content types.

    Defense in depth: Validates both extension and Content-Type when provided.
    Also checks that extension and Content-Type are compatible to prevent spoofing.

    Args:
        filename_or_url: Filename or URL to validate
        content_type: Optional Content-Type header value
        allowed_extensions: Custom allowed extensions (None = use defaults)
        allowed_content_types: Custom allowed MIME types (None = use defaults)

    Raises:
        FileValidationError: If validation fails
    """
    if not filename_or_url:
        raise FileValidationError("Empty filename or URL")

    # Use default allowed lists if not provided
    if allowed_extensions is None:
        allowed_extensions = ALLOWED_EXTENSIONS
    if allowed_content_types is None:
        allowed_content_types = ALLOWED_CONTENT_TYPES

    # Normalize allowed sets to lowercase
    allowed_extensions = {ext.lower() for ext in allowed_extensions}
    allowed_content_types = {ct.lower() for ct in allowed_content_types}

    # Extract and validate extension
    extension = extract_extension(filename_or_url)
    if extension is None:
        raise FileValidationError("No file extension found")

    if extension not in allowed_extensions:
        raise FileValidationError(f"File extension '{extension}' not allowed")

    # Validate Content-Type if provided
    if content_type:
        _validate_content_type(content_type, extension, allowed_content_types)


def _validate_content_type(
    content_type: str, extension: str, allowed_content_types: set[str]
) -> None:
    """Validate Content-Type header against allowed types and extension compatibility."""
    normalized_ct = normalize_content_type(content_type)
    if not normalized_ct:
        raise FileValidationError("Invalid Content-Type header")

    if normalized_ct not in allowed_content_types:
        raise FileValidationError(f"Content-Type '{normalized_ct}' not allowed")

    # Defense against spoofing: extension and Content-Type should be compatible
    expected_mimes = EXTENSION_TO_MIME.get(extension)
    if expected_mimes and normalized_ct not in expected_mimes:
        raise FileValidationError(
            f"Content-Type '{normalized_ct}' doesn't match extension '{extension}'"
        )


def is_allowed_extension(extension: str) -> bool:
    """Check if extension (with or without leading dot) is in ALLOWED_EXTENSIONS."""
    if not extension:
        return False

    # Remove leading dot if present
    extension = extension.lstrip(".").lower()
    return extension in ALLOWED_EXTENSIONS


def is_allowed_content_type(content_type: str) -> bool:
    """Check if normalized Content-Type is in ALLOWED_CONTENT_TYPES."""
    if not content_type:
        return False

    normalized = normalize_content_type(content_type)
    return normalized in ALLOWED_CONTENT_TYPES
