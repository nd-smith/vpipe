"""
File type validation for attachment downloads.

Implements FR-2.2.1: Validate files against allowed file type list.
Supports validation by file extension and Content-Type header.
"""

from typing import Optional, Set, Tuple
from urllib.parse import urlparse, parse_qs


# Allowed file extensions (case-insensitive)
ALLOWED_EXTENSIONS: Set[str] = {
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
ALLOWED_CONTENT_TYPES: Set[str] = {
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
EXTENSION_TO_MIME = {
    "pdf": "application/pdf",
    "xml": {"application/xml", "text/xml"},
    "txt": "text/plain",
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "png": "image/png",
    "gif": "image/gif",
    "bmp": "image/bmp",
    "tiff": "image/tiff",
    "tif": "image/tiff",
    "webp": "image/webp",
    "mov": "video/quicktime",
    "mp4": "video/mp4",    
}


def extract_extension(filename_or_url: str) -> Optional[str]:
    """
    Extract file extension from filename or URL.

    Checks URL path first, then falls back to 'filename' query parameter
    for URLs where the filename is passed as a parameter (common pattern
    for file service APIs like ClaimX cxfileservice).

    Args:
        filename_or_url: Filename or URL to extract extension from

    Returns:
        Lowercase file extension without dot, or None if no extension found

    Examples:
        >>> extract_extension("document.pdf")
        "pdf"
        >>> extract_extension("https://example.com/file.XML")
        "xml"
        >>> extract_extension("https://example.com/get/uuid?filename=doc.pdf")
        "pdf"
        >>> extract_extension("no_extension")
        None
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

    # Extract extension from path
    if "." in path:
        extension = path.rsplit(".", 1)[-1].lower()
        # Remove query parameters if present
        extension = extension.split("?")[0].split("#")[0]
        if extension:
            return extension

    # Fallback: check query parameters for filename
    # Common pattern for file service APIs (e.g., ?filename=document.pdf)
    if parsed and parsed.query:
        try:
            query_params = parse_qs(parsed.query)
            # Check common filename parameter names
            for param_name in ("filename", "file", "name"):
                if param_name in query_params and query_params[param_name]:
                    filename = query_params[param_name][0]
                    if "." in filename:
                        extension = filename.rsplit(".", 1)[-1].lower()
                        if extension:
                            return extension
        except Exception:
            pass

    return None


def normalize_content_type(content_type: str) -> str:
    """
    Normalize Content-Type header value.

    Removes parameters like charset and converts to lowercase.

    Args:
        content_type: Content-Type header value (e.g., "image/jpeg; charset=utf-8")

    Returns:
        Normalized MIME type (e.g., "image/jpeg")

    Examples:
        >>> normalize_content_type("image/jpeg; charset=utf-8")
        "image/jpeg"
        >>> normalize_content_type("APPLICATION/PDF")
        "application/pdf"
    """
    if not content_type:
        return ""

    # Split on semicolon to remove parameters
    mime_type = content_type.split(";")[0].strip().lower()
    return mime_type


def validate_file_type(
    filename_or_url: str,
    content_type: Optional[str] = None,
    allowed_extensions: Optional[Set[str]] = None,
    allowed_content_types: Optional[Set[str]] = None,
) -> Tuple[bool, str]:
    """
    Validate file type against allowed extensions and content types.

    Implements FR-2.2.1: System SHALL validate files against allowed file type list.

    Validation strategy:
    - If only filename/URL provided: validate extension only
    - If both filename and Content-Type provided: both must be valid
    - Defense in depth: reject if either check fails

    Args:
        filename_or_url: Filename or URL to validate
        content_type: Optional Content-Type header value
        allowed_extensions: Optional custom set of allowed extensions (defaults to ALLOWED_EXTENSIONS)
        allowed_content_types: Optional custom set of allowed MIME types (defaults to ALLOWED_CONTENT_TYPES)

    Returns:
        (is_valid, error_message)
        - (True, "") if valid
        - (False, "error description") if invalid

    Examples:
        >>> validate_file_type("document.pdf")
        (True, "")

        >>> validate_file_type("document.exe")
        (False, "File extension 'exe' not allowed")

        >>> validate_file_type("document.pdf", "application/pdf")
        (True, "")

        >>> validate_file_type("document.pdf", "application/x-executable")
        (False, "Content-Type 'application/x-executable' not allowed")
    """
    if not filename_or_url:
        return False, "Empty filename or URL"

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
        return False, "No file extension found"

    if extension not in allowed_extensions:
        return False, f"File extension '{extension}' not allowed"

    # Validate Content-Type if provided
    if content_type:
        normalized_ct = normalize_content_type(content_type)
        if not normalized_ct:
            return False, "Invalid Content-Type header"

        if normalized_ct not in allowed_content_types:
            return False, f"Content-Type '{normalized_ct}' not allowed"

        # Additional check: extension and Content-Type should be compatible
        # (defense against spoofing)
        expected_mimes = EXTENSION_TO_MIME.get(extension)
        if expected_mimes:
            # Handle both single MIME type and set of MIME types
            if isinstance(expected_mimes, str):
                expected_mimes = {expected_mimes}

            if normalized_ct not in expected_mimes:
                return (
                    False,
                    f"Content-Type '{normalized_ct}' doesn't match extension '{extension}'",
                )

    # All checks passed
    return True, ""


def is_allowed_extension(extension: str) -> bool:
    """
    Check if file extension is allowed.

    Args:
        extension: File extension (with or without leading dot)

    Returns:
        True if extension is in ALLOWED_EXTENSIONS

    Examples:
        >>> is_allowed_extension("pdf")
        True
        >>> is_allowed_extension(".PDF")
        True
        >>> is_allowed_extension("exe")
        False
    """
    if not extension:
        return False

    # Remove leading dot if present
    extension = extension.lstrip(".").lower()
    return extension in ALLOWED_EXTENSIONS


def is_allowed_content_type(content_type: str) -> bool:
    """
    Check if Content-Type is allowed.

    Args:
        content_type: Content-Type header value

    Returns:
        True if MIME type is in ALLOWED_CONTENT_TYPES

    Examples:
        >>> is_allowed_content_type("application/pdf")
        True
        >>> is_allowed_content_type("IMAGE/JPEG; charset=utf-8")
        True
        >>> is_allowed_content_type("application/x-executable")
        False
    """
    if not content_type:
        return False

    normalized = normalize_content_type(content_type)
    return normalized in ALLOWED_CONTENT_TYPES
