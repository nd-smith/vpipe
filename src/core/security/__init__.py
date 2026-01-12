"""
Security validation module.

Provides input validation and sanitization for security-sensitive operations.

Components to extract and review from verisk_pipeline.common:
    - validate_download_url(): SSRF prevention with domain allowlist [DONE - WP-113]
    - validate_file_type(): File type allowlist validation [DONE - WP-114]
    - check_presigned_url(): AWS S3 and ClaimX URL parsing/expiration [DONE - WP-115]
    - sanitize_filename(): Path traversal prevention
    - sanitize_url(): Remove auth tokens from logged URLs
    - sanitize_error_message(): Remove sensitive data from logs

Review checklist:
    [x] Domain allowlist is complete and correct
    [x] URL validation handles edge cases (unicode, encoding)
    [x] File type validation enforces allowlist
    [x] Extension and Content-Type compatibility checked
    [x] Presigned URL expiration detection is accurate
    [ ] Path traversal prevention is robust
    [ ] No bypass vectors exist
"""

from core.security.file_validation import (
    ALLOWED_CONTENT_TYPES,
    ALLOWED_EXTENSIONS,
    extract_extension,
    is_allowed_content_type,
    is_allowed_extension,
    normalize_content_type,
    validate_file_type,
)
from core.security.presigned_urls import (
    PresignedUrlInfo,
    check_presigned_url,
    extract_expires_at_iso,
)
from core.security.url_validation import (
    ALLOWED_SCHEMES,
    BLOCKED_HOSTS,
    DEFAULT_ALLOWED_DOMAINS,
    PRIVATE_RANGES,
    get_allowed_domains,
    is_private_ip,
    validate_download_url,
)

__all__ = [
    # URL validation
    "validate_download_url",
    "get_allowed_domains",
    "is_private_ip",
    "ALLOWED_SCHEMES",
    "BLOCKED_HOSTS",
    "DEFAULT_ALLOWED_DOMAINS",
    "PRIVATE_RANGES",
    # Presigned URL handling
    "check_presigned_url",
    "extract_expires_at_iso",
    "PresignedUrlInfo",
    # File type validation
    "validate_file_type",
    "extract_extension",
    "normalize_content_type",
    "is_allowed_extension",
    "is_allowed_content_type",
    "ALLOWED_EXTENSIONS",
    "ALLOWED_CONTENT_TYPES",
]
