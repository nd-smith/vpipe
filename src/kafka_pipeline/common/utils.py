"""
Common utility functions for kafka_pipeline.

Provides:
- URL expiration checking for presigned URLs
"""

from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse, parse_qs


def extract_expires_at_iso(url: str) -> Optional[str]:
    """
    Extract expiration time from presigned URL as ISO string.

    Supports:
    - AWS S3 presigned URLs (X-Amz-Date + X-Amz-Expires)
    - ClaimX service URLs (systemDate + expires)

    Args:
        url: Presigned URL

    Returns:
        ISO datetime string or None if not parseable
    """
    if not url:
        return None

    try:
        # Check for ClaimX URL
        if "claimxperience.com" in url:
            return _extract_claimx_expires(url)

        # Check for S3 presigned URL markers
        url_lower = url.lower()
        if "x-amz-expires" in url_lower or "x-amz-date" in url_lower:
            return _extract_s3_expires(url)

        return None

    except Exception:
        return None


def _extract_s3_expires(url: str) -> Optional[str]:
    """Extract expiration time from AWS S3 presigned URL."""
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)

        # Handle case-insensitive parameter names
        def get_param(name: str) -> Optional[str]:
            if name in params:
                return params[name][0]
            lower_params = {k.lower(): v for k, v in params.items()}
            if name.lower() in lower_params:
                return lower_params[name.lower()][0]
            return None

        amz_date_str = get_param("X-Amz-Date")
        expires_str = get_param("X-Amz-Expires")

        if not amz_date_str or not expires_str:
            return None

        # Parse timestamp - S3 dates are always UTC
        from datetime import timedelta

        signed_at = datetime.strptime(amz_date_str, "%Y%m%dT%H%M%SZ").replace(
            tzinfo=timezone.utc
        )
        ttl_seconds = int(expires_str)
        expires_at = signed_at + timedelta(seconds=ttl_seconds)

        return expires_at.isoformat()

    except Exception:
        return None


def _extract_claimx_expires(url: str) -> Optional[str]:
    """Extract expiration time from ClaimX service URL."""
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)

        system_date_str = params.get("systemDate", [None])[0]
        expires_str = params.get("expires", [None])[0]

        if not system_date_str or not expires_str:
            return None

        # Parse (convert ms to seconds)
        from datetime import timedelta

        signed_at = datetime.fromtimestamp(int(system_date_str) / 1000, tz=timezone.utc)
        ttl_ms = int(expires_str)
        expires_at = signed_at + timedelta(milliseconds=ttl_ms)

        return expires_at.isoformat()

    except Exception:
        return None
