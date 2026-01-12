"""
Tests for file type validation.
"""

import pytest

from core.security.file_validation import (
    ALLOWED_CONTENT_TYPES,
    ALLOWED_EXTENSIONS,
    extract_extension,
    is_allowed_content_type,
    is_allowed_extension,
    normalize_content_type,
    validate_file_type,
)


class TestExtractExtension:
    """Tests for extract_extension() function."""

    def test_simple_filename(self):
        """Should extract extension from simple filename."""
        assert extract_extension("document.pdf") == "pdf"
        assert extract_extension("file.xml") == "xml"
        assert extract_extension("image.jpg") == "jpg"

    def test_uppercase_extension(self):
        """Should convert extension to lowercase."""
        assert extract_extension("FILE.PDF") == "pdf"
        assert extract_extension("Document.XML") == "xml"

    def test_url_with_extension(self):
        """Should extract extension from URL."""
        url = "https://example.com/path/to/file.pdf"
        assert extract_extension(url) == "pdf"

    def test_url_with_query_params(self):
        """Should extract extension ignoring query parameters."""
        url = "https://example.com/file.pdf?key=value&foo=bar"
        assert extract_extension(url) == "pdf"

    def test_url_with_fragment(self):
        """Should extract extension ignoring fragment."""
        url = "https://example.com/file.xml#section"
        assert extract_extension(url) == "xml"

    def test_multiple_dots_in_filename(self):
        """Should extract last extension from filename with multiple dots."""
        assert extract_extension("my.document.pdf") == "pdf"
        assert extract_extension("archive.tar.gz") == "gz"

    def test_no_extension(self):
        """Should return None for filename without extension."""
        assert extract_extension("noextension") is None
        assert extract_extension("README") is None

    def test_empty_string(self):
        """Should return None for empty string."""
        assert extract_extension("") is None

    def test_extension_variations(self):
        """Should handle extension variations."""
        assert extract_extension("image.jpeg") == "jpeg"
        assert extract_extension("image.jpg") == "jpg"
        assert extract_extension("image.tiff") == "tiff"
        assert extract_extension("image.tif") == "tif"

    def test_filename_query_parameter(self):
        """Should extract extension from filename query parameter when path has no extension."""
        # ClaimX file service pattern - filename is in query param, not path
        url = "https://example.com/service/get/uuid-123?filename=document.pdf&download=true"
        assert extract_extension(url) == "pdf"

    def test_filename_query_parameter_various_params(self):
        """Should check multiple common filename parameter names."""
        assert extract_extension("https://example.com/get?file=test.xml") == "xml"
        assert extract_extension("https://example.com/get?name=image.png") == "png"

    def test_path_extension_takes_priority_over_query(self):
        """Path extension should be used when present, not query parameter."""
        url = "https://example.com/file.jpg?filename=document.pdf"
        assert extract_extension(url) == "jpg"

    def test_claimx_fileservice_url(self):
        """Should handle real ClaimX file service URL format."""
        url = (
            "https://www.claimxperience.com/service/cxfileservice/get/"
            "49ddecc8-3ca2-43eb-b51b-6a290421b0f7"
            "?sign=abc123&systemDate=123456&expires=86400000"
            "&filename=Property_Report.pdf&download=true"
        )
        assert extract_extension(url) == "pdf"


class TestNormalizeContentType:
    """Tests for normalize_content_type() function."""

    def test_simple_mime_type(self):
        """Should return MIME type as-is."""
        assert normalize_content_type("application/pdf") == "application/pdf"
        assert normalize_content_type("image/jpeg") == "image/jpeg"

    def test_mime_type_with_charset(self):
        """Should remove charset parameter."""
        ct = "application/pdf; charset=utf-8"
        assert normalize_content_type(ct) == "application/pdf"

    def test_mime_type_with_boundary(self):
        """Should remove boundary parameter."""
        ct = "multipart/form-data; boundary=something"
        assert normalize_content_type(ct) == "multipart/form-data"

    def test_uppercase_mime_type(self):
        """Should convert MIME type to lowercase."""
        assert normalize_content_type("APPLICATION/PDF") == "application/pdf"
        assert normalize_content_type("IMAGE/JPEG") == "image/jpeg"

    def test_mime_type_with_spaces(self):
        """Should strip whitespace."""
        assert normalize_content_type("  application/pdf  ") == "application/pdf"
        ct = "application/pdf ; charset=utf-8"
        assert normalize_content_type(ct) == "application/pdf"

    def test_empty_content_type(self):
        """Should return empty string for empty input."""
        assert normalize_content_type("") == ""
        assert normalize_content_type(None) == ""


class TestIsAllowedExtension:
    """Tests for is_allowed_extension() function."""

    def test_allowed_extensions(self):
        """Should accept allowed extensions."""
        assert is_allowed_extension("pdf") is True
        assert is_allowed_extension("xml") is True
        assert is_allowed_extension("txt") is True
        assert is_allowed_extension("jpg") is True
        assert is_allowed_extension("jpeg") is True
        assert is_allowed_extension("png") is True

    def test_disallowed_extensions(self):
        """Should reject disallowed extensions."""
        assert is_allowed_extension("exe") is False
        assert is_allowed_extension("sh") is False
        assert is_allowed_extension("bat") is False
        assert is_allowed_extension("zip") is False

    def test_case_insensitive(self):
        """Should be case-insensitive."""
        assert is_allowed_extension("PDF") is True
        assert is_allowed_extension("Xml") is True
        assert is_allowed_extension("JPG") is True

    def test_with_leading_dot(self):
        """Should handle extension with leading dot."""
        assert is_allowed_extension(".pdf") is True
        assert is_allowed_extension(".exe") is False

    def test_empty_extension(self):
        """Should reject empty extension."""
        assert is_allowed_extension("") is False
        assert is_allowed_extension(None) is False


class TestIsAllowedContentType:
    """Tests for is_allowed_content_type() function."""

    def test_allowed_content_types(self):
        """Should accept allowed content types."""
        assert is_allowed_content_type("application/pdf") is True
        assert is_allowed_content_type("application/xml") is True
        assert is_allowed_content_type("text/xml") is True
        assert is_allowed_content_type("text/plain") is True
        assert is_allowed_content_type("image/jpeg") is True
        assert is_allowed_content_type("image/png") is True

    def test_disallowed_content_types(self):
        """Should reject disallowed content types."""
        assert is_allowed_content_type("application/x-executable") is False
        assert is_allowed_content_type("application/zip") is False
        assert is_allowed_content_type("text/html") is False

    def test_case_insensitive(self):
        """Should be case-insensitive."""
        assert is_allowed_content_type("APPLICATION/PDF") is True
        assert is_allowed_content_type("Image/JPEG") is True

    def test_with_parameters(self):
        """Should handle content type with parameters."""
        assert is_allowed_content_type("application/pdf; charset=utf-8") is True
        assert is_allowed_content_type("image/jpeg; quality=high") is True

    def test_empty_content_type(self):
        """Should reject empty content type."""
        assert is_allowed_content_type("") is False
        assert is_allowed_content_type(None) is False


class TestValidateFileType:
    """Tests for validate_file_type() function."""

    # Valid file type tests
    def test_valid_pdf_extension_only(self):
        """Should accept PDF file."""
        is_valid, error = validate_file_type("document.pdf")
        assert is_valid is True
        assert error == ""

    def test_valid_xml_extension_only(self):
        """Should accept XML file."""
        is_valid, error = validate_file_type("data.xml")
        assert is_valid is True
        assert error == ""

    def test_valid_jpg_extension_only(self):
        """Should accept JPG image."""
        is_valid, error = validate_file_type("photo.jpg")
        assert is_valid is True
        assert error == ""

    def test_valid_png_extension_only(self):
        """Should accept PNG image."""
        is_valid, error = validate_file_type("image.png")
        assert is_valid is True
        assert error == ""

    def test_valid_pdf_with_content_type(self):
        """Should accept PDF with matching Content-Type."""
        is_valid, error = validate_file_type("document.pdf", "application/pdf")
        assert is_valid is True
        assert error == ""

    def test_valid_jpg_with_content_type(self):
        """Should accept JPG with matching Content-Type."""
        is_valid, error = validate_file_type("photo.jpg", "image/jpeg")
        assert is_valid is True
        assert error == ""

    def test_valid_url_with_extension(self):
        """Should accept URL with valid extension."""
        url = "https://example.com/path/document.pdf"
        is_valid, error = validate_file_type(url)
        assert is_valid is True
        assert error == ""

    def test_valid_url_with_query_params(self):
        """Should accept URL with query parameters."""
        url = "https://example.com/file.pdf?key=value"
        is_valid, error = validate_file_type(url)
        assert is_valid is True
        assert error == ""

    # Invalid file type tests
    def test_invalid_exe_extension(self):
        """Should reject executable file."""
        is_valid, error = validate_file_type("malware.exe")
        assert is_valid is False
        assert "exe" in error.lower()
        assert "not allowed" in error

    def test_invalid_zip_extension(self):
        """Should reject ZIP file."""
        is_valid, error = validate_file_type("archive.zip")
        assert is_valid is False
        assert "zip" in error.lower()

    def test_invalid_sh_extension(self):
        """Should reject shell script."""
        is_valid, error = validate_file_type("script.sh")
        assert is_valid is False
        assert "sh" in error.lower()

    def test_no_extension(self):
        """Should reject file without extension."""
        is_valid, error = validate_file_type("noextension")
        assert is_valid is False
        assert "extension" in error.lower()

    def test_empty_filename(self):
        """Should reject empty filename."""
        is_valid, error = validate_file_type("")
        assert is_valid is False
        assert "empty" in error.lower()

    # Content-Type mismatch tests
    def test_pdf_extension_with_wrong_content_type(self):
        """Should reject PDF extension with wrong Content-Type."""
        is_valid, error = validate_file_type("document.pdf", "image/jpeg")
        assert is_valid is False
        assert "doesn't match" in error.lower()

    def test_jpg_extension_with_wrong_content_type(self):
        """Should reject JPG extension with wrong Content-Type."""
        is_valid, error = validate_file_type("photo.jpg", "application/pdf")
        assert is_valid is False
        assert "doesn't match" in error.lower()

    def test_invalid_content_type(self):
        """Should reject file with invalid Content-Type."""
        is_valid, error = validate_file_type("document.pdf", "application/x-executable")
        assert is_valid is False
        assert "content-type" in error.lower()
        assert "not allowed" in error

    # Edge cases
    def test_case_insensitive_extension(self):
        """Should handle uppercase extension."""
        is_valid, error = validate_file_type("DOCUMENT.PDF")
        assert is_valid is True
        assert error == ""

    def test_case_insensitive_content_type(self):
        """Should handle uppercase Content-Type."""
        is_valid, error = validate_file_type("document.pdf", "APPLICATION/PDF")
        assert is_valid is True
        assert error == ""

    def test_content_type_with_charset(self):
        """Should handle Content-Type with charset parameter."""
        is_valid, error = validate_file_type(
            "document.pdf", "application/pdf; charset=utf-8"
        )
        assert is_valid is True
        assert error == ""

    def test_jpeg_vs_jpg_extension(self):
        """Should accept both jpeg and jpg extensions."""
        is_valid1, _ = validate_file_type("photo.jpeg", "image/jpeg")
        is_valid2, _ = validate_file_type("photo.jpg", "image/jpeg")
        assert is_valid1 is True
        assert is_valid2 is True

    def test_xml_with_text_xml_content_type(self):
        """Should accept text/xml for XML files."""
        is_valid, error = validate_file_type("data.xml", "text/xml")
        assert is_valid is True
        assert error == ""

    def test_xml_with_application_xml_content_type(self):
        """Should accept application/xml for XML files."""
        is_valid, error = validate_file_type("data.xml", "application/xml")
        assert is_valid is True
        assert error == ""

    def test_valid_txt_extension_only(self):
        """Should accept TXT file extension."""
        is_valid, error = validate_file_type("document.txt")
        assert is_valid is True
        assert error == ""

    def test_valid_txt_with_content_type(self):
        """Should accept TXT with matching Content-Type."""
        is_valid, error = validate_file_type("document.txt", "text/plain")
        assert is_valid is True
        assert error == ""

    def test_valid_txt_uppercase_extension(self):
        """Should accept uppercase TXT extension."""
        is_valid, error = validate_file_type("DOCUMENT.TXT")
        assert is_valid is True
        assert error == ""

    def test_s3_presigned_url_with_txt(self):
        """Should accept TXT file from S3 presigned URL with query parameters."""
        # This URL format matches Xact/Verisk S3 presigned URLs
        url = (
            "https://usw2-prod-xn-exportreceiver-publish.s3.us-west-2.amazonaws.com/"
            "Client-API/06NSLJQ/Reassigned-%20Reject%20Roof%20Measurement%20Update.TXT"
            "?X-Amz-Expires=259200&X-Amz-Algorithm=AWS4-HMAC-SHA256"
        )
        is_valid, error = validate_file_type(url)
        assert is_valid is True
        assert error == ""

    # Custom allowlists
    def test_custom_allowed_extensions(self):
        """Should use custom allowed extensions."""
        is_valid, error = validate_file_type(
            "document.pdf", allowed_extensions={"txt", "csv"}
        )
        assert is_valid is False
        assert "pdf" in error.lower()

    def test_custom_allowed_content_types(self):
        """Should use custom allowed content types."""
        is_valid, error = validate_file_type(
            "document.pdf",
            "application/pdf",
            allowed_content_types={"text/plain"},
        )
        assert is_valid is False
        assert "content-type" in error.lower()

    # Security tests
    def test_executable_masquerading_as_pdf(self):
        """Should reject executable with PDF Content-Type."""
        is_valid, error = validate_file_type("malware.exe", "application/pdf")
        assert is_valid is False
        assert "exe" in error.lower()

    def test_pdf_extension_with_executable_content_type(self):
        """Should reject PDF with executable Content-Type."""
        is_valid, error = validate_file_type(
            "document.pdf", "application/x-executable"
        )
        assert is_valid is False

    def test_path_traversal_attempt(self):
        """Should handle path traversal attempt."""
        is_valid, error = validate_file_type("../../etc/passwd.pdf")
        assert is_valid is True  # Extension is valid, path handling is elsewhere

    def test_url_encoded_extension(self):
        """Should handle URL-encoded characters in extension."""
        # This should fail as the extension would be weird
        is_valid, error = validate_file_type("file.pd%66")
        assert is_valid is False
