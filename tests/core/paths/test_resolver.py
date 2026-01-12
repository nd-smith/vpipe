"""
Tests for core.paths.resolver module.

Tests path generation for different event subtypes and OneLake path construction.
"""

import pytest

from core.paths.resolver import generate_blob_path, get_onelake_path_for_event


class TestGenerateBlobPath:
    """Tests for generate_blob_path function."""

    def test_documents_received(self):
        """Test path generation for documentsReceived events."""
        blob_path, file_type = generate_blob_path(
            status_subtype="documentsReceived",
            trace_id="trace-123",
            assignment_id="assignment-456",
            download_url="https://example.com/document.pdf",
        )

        assert blob_path == "documentsReceived/assignment-456/trace-123/assignment-456_document.pdf"
        assert file_type == "PDF"

    def test_fnol_received(self):
        """Test path generation for firstNoticeOfLossReceived events."""
        blob_path, file_type = generate_blob_path(
            status_subtype="firstNoticeOfLossReceived",
            trace_id="trace-123",
            assignment_id="assignment-456",
            download_url="https://example.com/fnol_report.pdf",
        )

        assert blob_path == "firstNoticeOfLossReceived/assignment-456/trace-123/assignment-456_FNOL_fnol_report.pdf"
        assert file_type == "PDF"

    def test_estimate_package_with_version(self):
        """Test path generation for estimatePackageReceived with version."""
        blob_path, file_type = generate_blob_path(
            status_subtype="estimatePackageReceived",
            trace_id="trace-123",
            assignment_id="assignment-456",
            download_url="https://example.com/estimate.pdf",
            estimate_version="v2.1",
        )

        assert blob_path == "estimatePackageReceived/assignment-456/trace-123/assignment-456_v2.1_estimate.pdf"
        assert file_type == "PDF"

    def test_estimate_package_without_version(self):
        """Test path generation for estimatePackageReceived without version."""
        blob_path, file_type = generate_blob_path(
            status_subtype="estimatePackageReceived",
            trace_id="trace-123",
            assignment_id="assignment-456",
            download_url="https://example.com/estimate.pdf",
        )

        assert blob_path == "estimatePackageReceived/assignment-456/trace-123/assignment-456_unknown_estimate.pdf"
        assert file_type == "PDF"

    def test_unknown_subtype_uses_default_pattern(self):
        """Test that unknown subtypes use the default pattern."""
        blob_path, file_type = generate_blob_path(
            status_subtype="unknownEventType",
            trace_id="trace-123",
            assignment_id="assignment-456",
            download_url="https://example.com/file.pdf",
        )

        # Should use default pattern: {assignment_id}_{filename}
        assert blob_path == "unknownEventType/assignment-456/trace-123/assignment-456_file.pdf"
        assert file_type == "PDF"

    def test_different_file_types(self):
        """Test path generation with different file extensions."""
        test_cases = [
            ("https://example.com/doc.jpg", "JPG"),
            ("https://example.com/doc.png", "PNG"),
            ("https://example.com/doc.docx", "DOCX"),
            ("https://example.com/doc.zip", "ZIP"),
        ]

        for url, expected_type in test_cases:
            _, file_type = generate_blob_path(
                status_subtype="documentsReceived",
                trace_id="trace-123",
                assignment_id="assignment-456",
                download_url=url,
            )
            assert file_type == expected_type

    def test_url_without_extension(self):
        """Test handling of URLs without file extensions."""
        blob_path, file_type = generate_blob_path(
            status_subtype="documentsReceived",
            trace_id="trace-123",
            assignment_id="assignment-456",
            download_url="https://example.com/document",
        )

        # Should handle gracefully (sanitize_filename will handle this)
        assert "documentsReceived/assignment-456/trace-123/" in blob_path
        assert file_type == "UNKNOWN"

    def test_complex_url_with_query_params(self):
        """Test URL with query parameters and special characters."""
        blob_path, file_type = generate_blob_path(
            status_subtype="documentsReceived",
            trace_id="trace-123",
            assignment_id="assignment-456",
            download_url="https://example.com/docs/report.pdf?token=abc&expires=123",
        )

        # Should extract just the filename, not query params
        assert "documentsReceived/assignment-456/trace-123/" in blob_path
        assert blob_path.endswith(".pdf")
        assert "?" not in blob_path
        assert "token" not in blob_path

    def test_special_characters_in_ids(self):
        """Test handling of special characters in trace_id and assignment_id."""
        blob_path, file_type = generate_blob_path(
            status_subtype="documentsReceived",
            trace_id="trace_123-456",
            assignment_id="assign_789-abc",
            download_url="https://example.com/file.pdf",
        )

        # IDs should be preserved as-is in the path
        assert "documentsReceived/assign_789-abc/trace_123-456/" in blob_path


class TestGetOneLakePathForEvent:
    """Tests for get_onelake_path_for_event function."""

    def test_basic_onelake_path(self):
        """Test basic OneLake path construction."""
        path = get_onelake_path_for_event(
            event_type="documentsReceived",
            trace_id="trace-123",
            assignment_id="assignment-456",
            download_url="https://example.com/document.pdf",
            base_path="workspace/lakehouse/Files/xact",
        )

        expected = "workspace/lakehouse/Files/xact/documentsReceived/assignment-456/trace-123/assignment-456_document.pdf"
        assert path == expected

    def test_base_path_with_trailing_slash(self):
        """Test that trailing slashes in base_path are handled correctly."""
        path = get_onelake_path_for_event(
            event_type="documentsReceived",
            trace_id="trace-123",
            assignment_id="assignment-456",
            download_url="https://example.com/document.pdf",
            base_path="workspace/lakehouse/Files/xact/",
        )

        # Should not have double slashes
        assert "//" not in path
        expected = "workspace/lakehouse/Files/xact/documentsReceived/assignment-456/trace-123/assignment-456_document.pdf"
        assert path == expected

    def test_onelake_path_with_fnol(self):
        """Test OneLake path for FNOL events."""
        path = get_onelake_path_for_event(
            event_type="firstNoticeOfLossReceived",
            trace_id="trace-123",
            assignment_id="assignment-456",
            download_url="https://example.com/fnol.pdf",
            base_path="workspace/lakehouse/Files/xact",
        )

        assert "firstNoticeOfLossReceived" in path
        assert "assignment-456_FNOL_fnol.pdf" in path

    def test_onelake_path_with_estimate_version(self):
        """Test OneLake path for estimate packages with version."""
        path = get_onelake_path_for_event(
            event_type="estimatePackageReceived",
            trace_id="trace-123",
            assignment_id="assignment-456",
            download_url="https://example.com/estimate.pdf",
            base_path="workspace/lakehouse/Files/xact",
            estimate_version="v3.0",
        )

        assert "estimatePackageReceived" in path
        assert "assignment-456_v3.0_estimate.pdf" in path

    def test_different_base_paths(self):
        """Test OneLake path construction with different base paths."""
        test_cases = [
            "workspace/lakehouse/Files/xact",
            "production/storage/files",
            "dev/test/files",
            "/absolute/path/to/storage",
        ]

        for base_path in test_cases:
            path = get_onelake_path_for_event(
                event_type="documentsReceived",
                trace_id="trace-123",
                assignment_id="assignment-456",
                download_url="https://example.com/doc.pdf",
                base_path=base_path,
            )

            # Should start with base path (stripped of trailing slash)
            expected_base = base_path.rstrip("/")
            assert path.startswith(expected_base)
            assert "//" not in path
