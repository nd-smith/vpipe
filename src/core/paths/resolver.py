"""
Path resolution logic for storage organization.

Provides path generation rules for different event status subtypes:
- documentsReceived
- firstNoticeOfLossReceived (FNOL)
- estimatePackageReceived

The path structure follows the pattern:
    {status_subtype}/{assignment_id}/{trace_id}/{filename}

Filenames are generated based on the event subtype and include sanitized
names extracted from the download URL.
"""

from typing import Optional, Tuple

from core.security.url_validation import extract_filename_from_url


def generate_blob_path(
    status_subtype: str,
    trace_id: str,
    assignment_id: str,
    download_url: str,
    estimate_version: Optional[str] = None,
) -> Tuple[str, str]:
    """
    Generate blob storage path based on event status subtype.

    Args:
        status_subtype: Event status subtype (documentsReceived, firstNoticeOfLossReceived, etc.)
        trace_id: Unique trace ID for the event
        assignment_id: Assignment identifier
        download_url: URL to extract filename from
        estimate_version: Version string for estimate packages (optional)

    Returns:
        Tuple of (blob_path, file_type)
        - blob_path: Complete path for storage (e.g., "documentsReceived/A123/T456/A123_file.pdf")
        - file_type: File extension in uppercase (e.g., "PDF")

    Examples:
        >>> generate_blob_path("documentsReceived", "T123", "A456", "https://example.com/file.pdf")
        ("documentsReceived/A456/T123/A456_file.pdf", "PDF")

        >>> generate_blob_path("firstNoticeOfLossReceived", "T123", "A456", "https://example.com/report.pdf")
        ("firstNoticeOfLossReceived/A456/T123/A456_FNOL_report.pdf", "PDF")

        >>> generate_blob_path("estimatePackageReceived", "T123", "A456", "https://example.com/est.pdf", "v2")
        ("estimatePackageReceived/A456/T123/A456_v2_est.pdf", "PDF")
    """
    url_filename, file_type = extract_filename_from_url(download_url)

    # Add extension back to filename
    if file_type and file_type != "UNKNOWN":
        url_filename_with_ext = f"{url_filename}.{file_type.lower()}"
    else:
        url_filename_with_ext = url_filename

    # Build filename based on bucket type
    if status_subtype == "documentsReceived":
        filename = f"{assignment_id}_{url_filename_with_ext}"

    elif status_subtype == "firstNoticeOfLossReceived":
        filename = f"{assignment_id}_FNOL_{url_filename_with_ext}"

    elif status_subtype == "estimatePackageReceived":
        version = estimate_version or "unknown"
        filename = f"{assignment_id}_{version}_{url_filename_with_ext}"

    else:
        # Default pattern for unknown subtypes
        filename = f"{assignment_id}_{url_filename_with_ext}"

    blob_path = f"{status_subtype}/{assignment_id}/{trace_id}/{filename}"
    return blob_path, file_type


def get_onelake_path_for_event(
    event_type: str,
    trace_id: str,
    assignment_id: str,
    download_url: str,
    base_path: str,
    estimate_version: Optional[str] = None,
) -> str:
    """
    Combine base path with domain-specific blob path.

    Args:
        event_type: Event status subtype
        trace_id: Unique trace ID for the event
        assignment_id: Assignment identifier
        download_url: URL to extract filename from
        base_path: Base storage path (e.g., "workspace/lakehouse/Files/xact")
        estimate_version: Version string for estimate packages (optional)

    Returns:
        Complete OneLake path combining base and generated blob path

    Examples:
        >>> get_onelake_path_for_event(
        ...     "documentsReceived", "T123", "A456",
        ...     "https://example.com/file.pdf",
        ...     "workspace/lakehouse/Files/xact"
        ... )
        "workspace/lakehouse/Files/xact/documentsReceived/A456/T123/A456_file.pdf"
    """
    blob_path, _ = generate_blob_path(
        event_type, trace_id, assignment_id, download_url, estimate_version
    )

    # Combine base path with domain-specific path
    base = base_path.rstrip("/")
    return f"{base}/{blob_path}"
