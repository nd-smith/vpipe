"""
Tests for presigned URL parsing and expiration checking.

Covers:
- AWS S3 presigned URL parsing (X-Amz-Date, X-Amz-Expires)
- ClaimX URL parsing (systemDate, expires)
- Expiration detection
- Edge cases (missing params, invalid formats, empty URLs)
"""

import pytest
from datetime import datetime, timezone, timedelta
from core.security.presigned_urls import (
    check_presigned_url,
    extract_expires_at_iso,
    PresignedUrlInfo,
)


class TestS3PresignedUrls:
    """Test S3 presigned URL parsing."""

    def test_valid_s3_url_not_expired(self):
        """Valid S3 URL that hasn't expired."""
        # Create URL signed now with 1 hour expiry
        now = datetime.now(timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        url = f"https://bucket.s3.amazonaws.com/file.pdf?X-Amz-Date={amz_date}&X-Amz-Expires=3600"

        info = check_presigned_url(url)

        assert info.url_type == "s3"
        assert info.is_presigned is True
        assert info.is_expired is False
        assert info.signed_at is not None
        assert info.expires_at is not None
        assert info.ttl_seconds == 3600
        assert info.parse_error is None
        assert info.seconds_remaining is not None
        assert info.seconds_remaining > 0

    def test_valid_s3_url_expired(self):
        """Valid S3 URL that has expired."""
        # Create URL signed 2 hours ago with 1 hour expiry
        past = datetime.now(timezone.utc) - timedelta(hours=2)
        amz_date = past.strftime("%Y%m%dT%H%M%SZ")
        url = f"https://bucket.s3.amazonaws.com/file.pdf?X-Amz-Date={amz_date}&X-Amz-Expires=3600"

        info = check_presigned_url(url)

        assert info.url_type == "s3"
        assert info.is_presigned is True
        assert info.is_expired is True
        assert info.seconds_remaining is not None
        assert info.seconds_remaining < 0

    def test_s3_url_case_insensitive_params(self):
        """S3 URL params should be case-insensitive."""
        now = datetime.now(timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        # Use lowercase parameter names
        url = f"https://bucket.s3.amazonaws.com/file.pdf?x-amz-date={amz_date}&x-amz-expires=3600"

        info = check_presigned_url(url)

        assert info.url_type == "s3"
        assert info.is_presigned is True
        assert info.parse_error is None

    def test_s3_url_missing_amz_date(self):
        """S3 URL missing X-Amz-Date."""
        url = "https://bucket.s3.amazonaws.com/file.pdf?X-Amz-Expires=3600"

        info = check_presigned_url(url)

        assert info.url_type == "s3"
        assert info.is_presigned is True
        assert info.is_expired is False  # Can't determine expiry
        assert info.parse_error == "Missing X-Amz-Date"

    def test_s3_url_missing_expires(self):
        """S3 URL missing X-Amz-Expires."""
        now = datetime.now(timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        url = f"https://bucket.s3.amazonaws.com/file.pdf?X-Amz-Date={amz_date}"

        info = check_presigned_url(url)

        assert info.url_type == "s3"
        assert info.is_presigned is True
        assert info.is_expired is False
        assert info.parse_error == "Missing X-Amz-Expires"

    def test_s3_url_invalid_date_format(self):
        """S3 URL with invalid date format."""
        url = "https://bucket.s3.amazonaws.com/file.pdf?X-Amz-Date=INVALID&X-Amz-Expires=3600"

        info = check_presigned_url(url)

        assert info.url_type == "s3"
        assert info.is_presigned is True
        assert info.is_expired is False
        assert info.parse_error is not None
        assert "Parse error" in info.parse_error

    def test_s3_url_invalid_expires_format(self):
        """S3 URL with non-numeric expires."""
        now = datetime.now(timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        url = f"https://bucket.s3.amazonaws.com/file.pdf?X-Amz-Date={amz_date}&X-Amz-Expires=INVALID"

        info = check_presigned_url(url)

        assert info.url_type == "s3"
        assert info.is_presigned is True
        assert info.is_expired is False
        assert info.parse_error is not None

    def test_s3_url_expires_within_buffer(self):
        """Test expires_within for buffer logic."""
        # URL expires in 30 seconds
        now = datetime.now(timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        url = f"https://bucket.s3.amazonaws.com/file.pdf?X-Amz-Date={amz_date}&X-Amz-Expires=30"

        info = check_presigned_url(url)

        # Should expire within 60 seconds
        assert info.expires_within(60) is True
        # Should not expire within 10 seconds
        assert info.expires_within(10) is False


class TestClaimXUrls:
    """Test ClaimX URL parsing."""

    def test_valid_claimx_url_not_expired(self):
        """Valid ClaimX URL that hasn't expired."""
        # Create URL signed now with 1 hour expiry
        now = datetime.now(timezone.utc)
        system_date_ms = int(now.timestamp() * 1000)
        expires_ms = 3600 * 1000  # 1 hour in milliseconds
        url = f"https://api.claimxperience.com/file?systemDate={system_date_ms}&expires={expires_ms}"

        info = check_presigned_url(url)

        assert info.url_type == "claimx"
        assert info.is_presigned is True
        assert info.is_expired is False
        assert info.signed_at is not None
        assert info.expires_at is not None
        assert info.ttl_seconds == 3600
        assert info.parse_error is None
        assert info.seconds_remaining is not None
        assert info.seconds_remaining > 0

    def test_valid_claimx_url_expired(self):
        """Valid ClaimX URL that has expired."""
        # Create URL signed 2 hours ago with 1 hour expiry
        past = datetime.now(timezone.utc) - timedelta(hours=2)
        system_date_ms = int(past.timestamp() * 1000)
        expires_ms = 3600 * 1000
        url = f"https://api.claimxperience.com/file?systemDate={system_date_ms}&expires={expires_ms}"

        info = check_presigned_url(url)

        assert info.url_type == "claimx"
        assert info.is_presigned is True
        assert info.is_expired is True
        assert info.seconds_remaining is not None
        assert info.seconds_remaining < 0

    def test_claimx_url_missing_system_date(self):
        """ClaimX URL missing systemDate."""
        url = "https://api.claimxperience.com/file?expires=3600000"

        info = check_presigned_url(url)

        assert info.url_type == "claimx"
        assert info.is_presigned is True
        assert info.is_expired is False
        assert info.parse_error == "Missing systemDate"

    def test_claimx_url_missing_expires(self):
        """ClaimX URL missing expires."""
        now = datetime.now(timezone.utc)
        system_date_ms = int(now.timestamp() * 1000)
        url = f"https://api.claimxperience.com/file?systemDate={system_date_ms}"

        info = check_presigned_url(url)

        assert info.url_type == "claimx"
        assert info.is_presigned is True
        assert info.is_expired is False
        assert info.parse_error == "Missing expires"

    def test_claimx_url_invalid_system_date(self):
        """ClaimX URL with invalid systemDate."""
        url = "https://api.claimxperience.com/file?systemDate=INVALID&expires=3600000"

        info = check_presigned_url(url)

        assert info.url_type == "claimx"
        assert info.is_presigned is True
        assert info.is_expired is False
        assert info.parse_error is not None
        assert "Parse error" in info.parse_error

    def test_claimx_url_invalid_expires(self):
        """ClaimX URL with invalid expires."""
        now = datetime.now(timezone.utc)
        system_date_ms = int(now.timestamp() * 1000)
        url = f"https://api.claimxperience.com/file?systemDate={system_date_ms}&expires=INVALID"

        info = check_presigned_url(url)

        assert info.url_type == "claimx"
        assert info.is_presigned is True
        assert info.is_expired is False
        assert info.parse_error is not None


class TestUnknownUrls:
    """Test handling of non-presigned URLs."""

    def test_regular_http_url(self):
        """Regular HTTP URL without presigned markers."""
        url = "https://example.com/file.pdf"

        info = check_presigned_url(url)

        assert info.url_type == "unknown"
        assert info.is_presigned is False
        assert info.is_expired is False
        assert info.signed_at is None
        assert info.expires_at is None
        assert info.parse_error is None

    def test_empty_url(self):
        """Empty URL."""
        info = check_presigned_url("")

        assert info.url == ""
        assert info.url_type == "unknown"
        assert info.is_presigned is False
        assert info.is_expired is False
        assert info.parse_error == "Empty URL"

    def test_none_url(self):
        """None URL (edge case)."""
        info = check_presigned_url(None)

        assert info.url == ""
        assert info.url_type == "unknown"
        assert info.is_presigned is False
        assert info.is_expired is False
        assert info.parse_error == "Empty URL"

    def test_url_with_other_params(self):
        """URL with query params but not presigned."""
        url = "https://example.com/file.pdf?version=1&format=pdf"

        info = check_presigned_url(url)

        assert info.url_type == "unknown"
        assert info.is_presigned is False
        assert info.is_expired is False


class TestPresignedUrlInfoMethods:
    """Test PresignedUrlInfo helper methods."""

    def test_time_remaining_positive(self):
        """Test time_remaining for non-expired URL."""
        now = datetime.now(timezone.utc)
        expires = now + timedelta(hours=1)
        info = PresignedUrlInfo(
            url="test",
            url_type="s3",
            is_presigned=True,
            is_expired=False,
            expires_at=expires,
        )

        remaining = info.time_remaining
        assert remaining is not None
        assert remaining.total_seconds() > 0

    def test_time_remaining_negative(self):
        """Test time_remaining for expired URL."""
        now = datetime.now(timezone.utc)
        expires = now - timedelta(hours=1)
        info = PresignedUrlInfo(
            url="test",
            url_type="s3",
            is_presigned=True,
            is_expired=True,
            expires_at=expires,
        )

        remaining = info.time_remaining
        assert remaining is not None
        assert remaining.total_seconds() < 0

    def test_time_remaining_none(self):
        """Test time_remaining when expires_at is None."""
        info = PresignedUrlInfo(
            url="test",
            url_type="unknown",
            is_presigned=False,
            is_expired=False,
        )

        assert info.time_remaining is None
        assert info.seconds_remaining is None

    def test_expires_within_true(self):
        """Test expires_within when URL expires soon."""
        now = datetime.now(timezone.utc)
        expires = now + timedelta(seconds=30)
        info = PresignedUrlInfo(
            url="test",
            url_type="s3",
            is_presigned=True,
            is_expired=False,
            expires_at=expires,
        )

        assert info.expires_within(60) is True

    def test_expires_within_false(self):
        """Test expires_within when URL doesn't expire soon."""
        now = datetime.now(timezone.utc)
        expires = now + timedelta(hours=2)
        info = PresignedUrlInfo(
            url="test",
            url_type="s3",
            is_presigned=True,
            is_expired=False,
            expires_at=expires,
        )

        assert info.expires_within(60) is False

    def test_expires_within_no_expiry(self):
        """Test expires_within when no expiry time."""
        info = PresignedUrlInfo(
            url="test",
            url_type="unknown",
            is_presigned=False,
            is_expired=False,
        )

        assert info.expires_within(60) is False


class TestExtractExpiresAtIso:
    """Test ISO extraction helper."""

    def test_extract_valid_s3_url(self):
        """Extract ISO timestamp from valid S3 URL."""
        now = datetime.now(timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        url = f"https://bucket.s3.amazonaws.com/file.pdf?X-Amz-Date={amz_date}&X-Amz-Expires=3600"

        iso_str = extract_expires_at_iso(url)

        assert iso_str is not None
        assert "T" in iso_str
        # Verify it's a valid ISO datetime
        parsed = datetime.fromisoformat(iso_str)
        assert parsed.tzinfo is not None

    def test_extract_valid_claimx_url(self):
        """Extract ISO timestamp from valid ClaimX URL."""
        now = datetime.now(timezone.utc)
        system_date_ms = int(now.timestamp() * 1000)
        expires_ms = 3600 * 1000
        url = f"https://api.claimxperience.com/file?systemDate={system_date_ms}&expires={expires_ms}"

        iso_str = extract_expires_at_iso(url)

        assert iso_str is not None
        assert "T" in iso_str

    def test_extract_unknown_url(self):
        """Extract from unknown URL returns None."""
        url = "https://example.com/file.pdf"

        iso_str = extract_expires_at_iso(url)

        assert iso_str is None

    def test_extract_invalid_url(self):
        """Extract from invalid URL returns None."""
        url = "https://bucket.s3.amazonaws.com/file.pdf?X-Amz-Date=INVALID"

        iso_str = extract_expires_at_iso(url)

        # Should return None for parse errors
        assert iso_str is None
