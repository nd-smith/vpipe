"""
URL expiration checking for presigned S3 and ClaimX URLs.

Detects expired presigned URLs before download attempts to:
- Xact: Mark expired URLs as permanent failures (no refresh capability)
- ClaimX: Trigger URL refresh via API before download

Supported URL formats:
- AWS S3 presigned: X-Amz-Date + X-Amz-Expires parameters
- ClaimX service: systemDate + expires parameters (milliseconds)
"""

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional
from urllib.parse import urlparse, parse_qs


@dataclass
class PresignedUrlInfo:
    """Expiration info for a presigned URL."""

    url: str
    url_type: str  # "s3", "claimx", "unknown"
    is_presigned: bool
    is_expired: bool
    signed_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    ttl_seconds: Optional[int] = None
    parse_error: Optional[str] = None

    @property
    def time_remaining(self) -> Optional[timedelta]:
        """Time until expiration. Negative if expired."""
        if not self.expires_at:
            return None
        return self.expires_at - datetime.now(timezone.utc)

    @property
    def seconds_remaining(self) -> Optional[int]:
        """Seconds until expiration. Negative if expired."""
        remaining = self.time_remaining
        if remaining is None:
            return None
        return int(remaining.total_seconds())

    def expires_within(self, seconds: int) -> bool:
        """Check if URL expires within N seconds (for buffer logic)."""
        if not self.expires_at:
            return False
        return (
            datetime.now(timezone.utc) + timedelta(seconds=seconds) >= self.expires_at
        )


def check_presigned_url(url: str) -> PresignedUrlInfo:
    """
    Parse presigned URL and extract expiration info.

    Supports:
    - AWS S3 presigned URLs (X-Amz-Date + X-Amz-Expires)
    - ClaimX service URLs (systemDate + expires)
    - Unknown URLs pass through as non-presigned (assumed valid)

    Args:
        url: The URL to check

    Returns:
        PresignedUrlInfo with expiration details
    """
    if not url:
        return PresignedUrlInfo(
            url=url or "",
            url_type="unknown",
            is_presigned=False,
            is_expired=False,
            parse_error="Empty URL",
        )

    # Detect URL type and parse
    if "claimxperience.com" in url:
        return _parse_claimx_url(url)

    # Check for S3 presigned URL markers (either X-Amz-Date or X-Amz-Expires)
    url_lower = url.lower()
    if "x-amz-expires" in url_lower or "x-amz-date" in url_lower:
        return _parse_s3_url(url)

    # Unknown format - pass through as valid
    return PresignedUrlInfo(
        url=url,
        url_type="unknown",
        is_presigned=False,
        is_expired=False,
    )


def _parse_s3_url(url: str) -> PresignedUrlInfo:
    """Parse AWS S3 presigned URL."""
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)

        # Handle case-insensitive parameter names
        def get_param(name: str) -> Optional[str]:
            # Try exact case first
            if name in params:
                return params[name][0]
            # Try lowercase
            lower_params = {k.lower(): v for k, v in params.items()}
            if name.lower() in lower_params:
                return lower_params[name.lower()][0]
            return None

        # Extract X-Amz-Date (YYYYMMDDTHHMMSSZ)
        amz_date_str = get_param("X-Amz-Date")
        if not amz_date_str:
            return PresignedUrlInfo(
                url=url,
                url_type="s3",
                is_presigned=True,
                is_expired=False,
                parse_error="Missing X-Amz-Date",
            )

        # Extract X-Amz-Expires (seconds)
        expires_str = get_param("X-Amz-Expires")
        if not expires_str:
            return PresignedUrlInfo(
                url=url,
                url_type="s3",
                is_presigned=True,
                is_expired=False,
                parse_error="Missing X-Amz-Expires",
            )

        # Parse timestamp - S3 dates are always UTC, make timezone-aware
        signed_at = datetime.strptime(amz_date_str, "%Y%m%dT%H%M%SZ").replace(
            tzinfo=timezone.utc
        )
        ttl_seconds = int(expires_str)
        expires_at = signed_at + timedelta(seconds=ttl_seconds)

        return PresignedUrlInfo(
            url=url,
            url_type="s3",
            is_presigned=True,
            is_expired=datetime.now(timezone.utc) >= expires_at,
            signed_at=signed_at,
            expires_at=expires_at,
            ttl_seconds=ttl_seconds,
        )

    except (ValueError, KeyError, IndexError) as e:
        return PresignedUrlInfo(
            url=url,
            url_type="s3",
            is_presigned=True,
            is_expired=False,
            parse_error=f"Parse error: {e}",
        )


def _parse_claimx_url(url: str) -> PresignedUrlInfo:
    """Parse ClaimX service URL."""
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)

        # Extract systemDate (milliseconds since epoch)
        system_date_str = params.get("systemDate", [None])[0]
        if not system_date_str:
            return PresignedUrlInfo(
                url=url,
                url_type="claimx",
                is_presigned=True,
                is_expired=False,
                parse_error="Missing systemDate",
            )

        # Extract expires (milliseconds TTL)
        expires_str = params.get("expires", [None])[0]
        if not expires_str:
            return PresignedUrlInfo(
                url=url,
                url_type="claimx",
                is_presigned=True,
                is_expired=False,
                parse_error="Missing expires",
            )

        # Parse (convert ms to seconds) - use UTC-aware datetimes
        signed_at = datetime.fromtimestamp(int(system_date_str) / 1000, tz=timezone.utc)
        ttl_ms = int(expires_str)
        ttl_seconds = ttl_ms // 1000
        expires_at = signed_at + timedelta(milliseconds=ttl_ms)

        return PresignedUrlInfo(
            url=url,
            url_type="claimx",
            is_presigned=True,
            is_expired=datetime.now(timezone.utc) >= expires_at,
            signed_at=signed_at,
            expires_at=expires_at,
            ttl_seconds=ttl_seconds,
        )

    except (ValueError, KeyError, IndexError) as e:
        return PresignedUrlInfo(
            url=url,
            url_type="claimx",
            is_presigned=True,
            is_expired=False,
            parse_error=f"Parse error: {e}",
        )


def extract_expires_at_iso(url: str) -> Optional[str]:
    """
    Extract expiration time as ISO string for storage.

    Convenience function for adding expires_at column to tables.

    Args:
        url: Presigned URL

    Returns:
        ISO datetime string or None if not parseable
    """
    info = check_presigned_url(url)
    if info.expires_at:
        return info.expires_at.isoformat()
    return None
