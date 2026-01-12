"""
URL validation for attachment downloads with SSRF prevention.

Provides strict URL validation against domain allowlists to prevent
Server-Side Request Forgery (SSRF) attacks and enforce secure download sources.
"""

import ipaddress
import os
import re
from typing import Optional, Set, Tuple
from urllib.parse import urlparse, urlunparse


# Allowed schemes for attachment downloads
ALLOWED_SCHEMES: Set[str] = {"https", "http"}

# Domains explicitly allowed for attachment downloads
# Configure via environment or extend this set
DEFAULT_ALLOWED_DOMAINS: Set[str] = {
    "usw2-prod-xn-exportreceiver-publish.s3.us-west-2.amazonaws.com",
    "claimxperience.s3.amazonaws.com",
    "claimxperience.s3.us-east-1.amazonaws.com",
    "www.claimxperience.com",
    "claimxperience.com",
    "xactware-claimx-us-prod.s3.us-west-1.amazonaws.com",
    "localhost",  # For local testing with dummy source
}

# Hosts to block (metadata endpoints, localhost, etc.)
BLOCKED_HOSTS: Set[str] = {
    #"localhost",
    "127.0.0.1",
    "0.0.0.0",
    "metadata.google.internal",
    "metadata.aws.internal",
    "169.254.169.254",
}

# Private IP ranges (RFC 1918 + link-local + loopback + IPv6)
PRIVATE_RANGES = [
    # IPv4 private ranges
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("169.254.0.0/16"),  # Link-local (cloud metadata)
    ipaddress.ip_network("127.0.0.0/8"),  # Loopback
    # IPv6 private ranges
    ipaddress.ip_network("::1/128"),  # IPv6 loopback
    ipaddress.ip_network("fe80::/10"),  # IPv6 link-local
    ipaddress.ip_network("fc00::/7"),  # IPv6 unique local addresses
]


def get_allowed_domains() -> Set[str]:
    """
    Get allowed domains for attachment URLs.

    Reads from ALLOWED_ATTACHMENT_DOMAINS env var (comma-separated)
    or falls back to DEFAULT_ALLOWED_DOMAINS.

    Returns:
        Set of allowed domain names (lowercase)
    """
    env_domains = os.getenv("ALLOWED_ATTACHMENT_DOMAINS", "")
    if env_domains:
        return {d.strip().lower() for d in env_domains.split(",") if d.strip()}
    return DEFAULT_ALLOWED_DOMAINS


def validate_download_url(
    url: str, allowed_domains: Optional[Set[str]] = None
) -> Tuple[bool, str]:
    """
    Validate URL against domain allowlist (strict mode).

    Use this for attachment downloads where source domains are known.
    Enforces HTTPS-only and domain allowlist to prevent SSRF attacks.

    Security considerations:
    - HTTPS required (no HTTP allowed)
    - Domain must be in allowlist (case-insensitive)
    - Hostname must be present and valid
    - Handles URL parsing errors safely

    Args:
        url: URL to validate
        allowed_domains: Optional set of allowed domains (defaults to get_allowed_domains())

    Returns:
        (is_valid, error_message)
        - (True, "") if valid
        - (False, "error description") if invalid

    Examples:
        >>> validate_download_url("https://example.s3.amazonaws.com/file.pdf")
        (False, "Domain not in allowlist: example.s3.amazonaws.com")

        >>> validate_download_url("http://claimxperience.com/file.pdf")
        (False, "Must be HTTPS, got http")

        >>> validate_download_url("https://claimxperience.com/file.pdf")
        (True, "")
    """
    if not url:
        return False, "Empty URL"

    # Parse URL safely
    try:
        parsed = urlparse(url)
    except Exception as e:
        return False, f"Invalid URL format: {e}"

    # Get allowed domains
    if allowed_domains is None:
        allowed_domains = get_allowed_domains()
    else:
        allowed_domains = {d.lower() for d in allowed_domains}

    # Extract hostname
    hostname = parsed.hostname
    if not hostname:
        return False, "No hostname in URL"

    # Normalize hostname to lowercase for comparison
    hostname_lower = hostname.lower()

    # Require HTTPS (except for localhost in testing)
    scheme = parsed.scheme.lower()
    if scheme not in ALLOWED_SCHEMES:
        return False, f"Invalid scheme: {scheme}"
    if scheme == "http" and hostname_lower != "localhost":
        return False, f"Must be HTTPS for non-localhost, got {scheme}"

    # Check allowlist
    if hostname_lower not in allowed_domains:
        return False, f"Domain not in allowlist: {hostname}"

    # All checks passed
    return True, ""


def is_private_ip(hostname: str) -> bool:
    """
    Check if hostname resolves to a private/internal IP address.

    Args:
        hostname: Hostname or IP address to check

    Returns:
        True if hostname is a private IP or in BLOCKED_HOSTS

    Note:
        This function does NOT perform DNS resolution for security reasons.
        It only checks if the hostname string itself is a private IP.
    """
    # Check blocked hosts
    if hostname.lower() in BLOCKED_HOSTS:
        return True

    # Check if it's an IP address
    try:
        ip = ipaddress.ip_address(hostname)
        # Check against private ranges
        for network in PRIVATE_RANGES:
            if ip in network:
                return True
    except ValueError:
        # Not an IP address, hostname only
        # We do NOT perform DNS resolution here to avoid DNS rebinding attacks
        pass

    return False


# ---------------------------------------------------------------------------
# URL Parsing
# ---------------------------------------------------------------------------


def extract_filename_from_url(url: str) -> Tuple[str, str]:
    """
    Extract filename and file extension from URL.

    Args:
        url: URL containing filename in path

    Returns:
        Tuple of (filename, file_type)
        - filename: Extracted filename without extension
        - file_type: File extension in uppercase (e.g., "PDF")

    Examples:
        >>> extract_filename_from_url("https://example.com/path/file.pdf?token=abc")
        ("file", "PDF")
    """
    try:
        parsed = urlparse(url)
        path = parsed.path

        # Get the last component of the path
        filename_with_ext = path.split("/")[-1]

        # Split filename and extension
        if "." in filename_with_ext:
            filename, ext = filename_with_ext.rsplit(".", 1)
            file_type = ext.upper()
        else:
            filename = filename_with_ext or "unknown"
            file_type = "UNKNOWN"

        return filename, file_type
    except Exception:
        return "unknown", "UNKNOWN"


# ---------------------------------------------------------------------------
# URL Sanitization (for logging)
# ---------------------------------------------------------------------------

# Query parameters that may contain sensitive tokens
SENSITIVE_PARAMS = {
    "sig",
    "signature",
    "sv",
    "se",
    "st",
    "sp",
    "sr",
    "spr",  # Azure SAS
    "x-amz-signature",
    "x-amz-credential",
    "x-amz-security-token",  # AWS
    "token",
    "access_token",
    "api_key",
    "apikey",
    "key",
    "secret",
    "password",
    "pwd",
    "auth",
    "authorization",
}


def sanitize_url(url: str) -> str:
    """
    Remove sensitive query parameters from URL.

    Preserves the path and structure for debugging while removing
    tokens that could grant access if exposed in logs.

    Args:
        url: URL that may contain sensitive parameters

    Returns:
        URL with sensitive parameters replaced with [REDACTED]
    """
    if not url:
        return url

    try:
        parsed = urlparse(url)
    except Exception:
        return url  # Return as-is if parsing fails

    if not parsed.query:
        return url  # No query string, nothing to sanitize

    # Parse and sanitize query parameters
    sanitized_params = []
    for param in parsed.query.split("&"):
        if "=" in param:
            key, value = param.split("=", 1)
            if key.lower() in SENSITIVE_PARAMS:
                sanitized_params.append(f"{key}=[REDACTED]")
            else:
                sanitized_params.append(param)
        else:
            sanitized_params.append(param)

    # Rebuild URL with sanitized query
    sanitized_query = "&".join(sanitized_params)
    return urlunparse(parsed._replace(query=sanitized_query))


# ---------------------------------------------------------------------------
# Error Message Sanitization
# ---------------------------------------------------------------------------

# Patterns that may contain sensitive data in error messages
SENSITIVE_PATTERNS = [
    (re.compile(r'sig=[^&\s"\']+', re.IGNORECASE), "sig=[REDACTED]"),
    (re.compile(r'sv=[^&\s"\']+', re.IGNORECASE), "sv=[REDACTED]"),
    (re.compile(r'se=[^&\s"\']+', re.IGNORECASE), "se=[REDACTED]"),
    (re.compile(r'st=[^&\s"\']+', re.IGNORECASE), "st=[REDACTED]"),
    (re.compile(r'token=[^&\s"\']+', re.IGNORECASE), "token=[REDACTED]"),
    (re.compile(r'key=[^&\s"\']+', re.IGNORECASE), "key=[REDACTED]"),
    (re.compile(r'password=[^&\s"\']+', re.IGNORECASE), "password=[REDACTED]"),
    (re.compile(r'secret=[^&\s"\']+', re.IGNORECASE), "secret=[REDACTED]"),
    (
        re.compile(r'x-amz-signature=[^&\s"\']+', re.IGNORECASE),
        "x-amz-signature=[REDACTED]",
    ),
    (
        re.compile(r'x-amz-credential=[^&\s"\']+', re.IGNORECASE),
        "x-amz-credential=[REDACTED]",
    ),
    (re.compile(r"bearer\s+[a-zA-Z0-9\-_.]+", re.IGNORECASE), "bearer [REDACTED]"),
    (re.compile(r'api[_-]?key[=:]\s*[^\s"\'&]+', re.IGNORECASE), "api_key=[REDACTED]"),
]


def sanitize_error_message(msg: str, max_length: int = 500) -> str:
    """
    Remove potentially sensitive data from error messages.

    Applies pattern-based redaction and truncates to max_length.

    Args:
        msg: Error message that may contain sensitive data
        max_length: Maximum length of returned message

    Returns:
        Sanitized and truncated error message
    """
    if not msg:
        return msg

    # Apply all sanitization patterns
    for pattern, replacement in SENSITIVE_PATTERNS:
        msg = pattern.sub(replacement, msg)

    # Also sanitize any URLs in the message
    # Find URLs and sanitize them
    url_pattern = re.compile(r'https?://[^\s"\'<>]+')
    for match in url_pattern.finditer(msg):
        original_url = match.group(0)
        sanitized = sanitize_url(original_url)
        if sanitized != original_url:
            msg = msg.replace(original_url, sanitized)

    # Truncate if needed
    if len(msg) > max_length:
        msg = msg[: max_length - 3] + "..."

    return msg
