"""
Test data generators for Kafka message schemas.

Provides factory functions for creating valid test messages with sensible defaults.
All generators support customization via keyword arguments for test-specific scenarios.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from kafka_pipeline.xact.schemas.events import EventMessage
from kafka_pipeline.xact.schemas.results import (
    DownloadResultMessage,
    FailedDownloadMessage,
)
from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage

# ClaimX schemas
from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
from kafka_pipeline.claimx.schemas.tasks import ClaimXEnrichmentTask, ClaimXDownloadTask
from kafka_pipeline.claimx.schemas.cached import ClaimXCachedDownloadMessage
from kafka_pipeline.claimx.schemas.results import ClaimXUploadResultMessage


def create_event_message(
    trace_id: Optional[str] = None,
    event_type: str = "claim",
    event_subtype: str = "documentsReceived",
    source_system: str = "xn",
    attachments: Optional[List[str]] = None,
    timestamp: Optional[datetime] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> EventMessage:
    """
    Create test EventMessage with sensible defaults.

    Args:
        trace_id: Unique event identifier (auto-generated if None)
        event_type: High-level event category (default: "claim")
        event_subtype: Specific event action (default: "documentsReceived")
        source_system: Source system identifier (default: "xn")
        attachments: List of attachment URLs (default: single test PDF)
        timestamp: Event timestamp (default: current UTC time)
        payload: Event-specific data (default: minimal claim payload)

    Returns:
        EventMessage: Valid event message for testing

    Example:
        >>> event = create_event_message(trace_id="test-001")
        >>> event = create_event_message(
        ...     trace_id="test-002",
        ...     attachments=["https://example.com/file1.pdf", "https://example.com/file2.pdf"]
        ... )
    """
    import json

    # Auto-generate trace_id if not provided
    if trace_id is None:
        import uuid
        trace_id = f"test-evt-{uuid.uuid4().hex[:8]}"

    # Default attachments
    if attachments is None:
        attachments = ["https://example.com/attachment.pdf"]

    # Default timestamp
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)

    # Default payload - include attachments in data
    if payload is None:
        payload = {
            "assignmentId": f"A-{trace_id[-6:]}",
            "claim_id": f"C-{trace_id[-6:]}",
        }

    # Add attachments to data payload
    data_payload = {**payload, "attachments": attachments}

    # Build the event type string
    type_str = f"verisk.claims.property.{source_system}.{event_subtype}"

    return EventMessage(
        type=type_str,
        version=1,
        utcDateTime=timestamp.isoformat() if isinstance(timestamp, datetime) else timestamp,
        traceId=trace_id,
        data=json.dumps(data_payload),
    )


def create_download_task_message(
    trace_id: Optional[str] = None,
    attachment_url: str = "https://example.com/file.pdf",
    blob_path: Optional[str] = None,
    status_subtype: str = "documentsReceived",
    file_type: Optional[str] = None,
    assignment_id: str = "A12345",
    event_type: str = "xact",
    event_subtype: str = "documentsReceived",
    retry_count: int = 0,
    original_timestamp: Optional[datetime] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> DownloadTaskMessage:
    """
    Create test DownloadTaskMessage with sensible defaults.

    Args:
        trace_id: Event identifier (auto-generated if None)
        attachment_url: URL to download (default: example.com PDF)
        blob_path: OneLake blob path (auto-generated if None)
        status_subtype: Event status subtype (default: "documentsReceived")
        file_type: File type from URL (auto-detected if None)
        assignment_id: Assignment ID (default: "A12345")
        event_type: Event category (default: "xact")
        event_subtype: Event action (default: "documentsReceived")
        retry_count: Number of retry attempts (default: 0)
        original_timestamp: Original event timestamp (default: current UTC)
        metadata: Additional metadata dict (default: empty)

    Returns:
        DownloadTaskMessage: Valid download task for testing

    Example:
        >>> task = create_download_task_message(trace_id="test-001")
        >>> task = create_download_task_message(
        ...     trace_id="test-002",
        ...     attachment_url="https://example.com/large-file.pdf",
        ...     retry_count=2
        ... )
    """
    # Auto-generate trace_id if not provided
    if trace_id is None:
        import uuid
        trace_id = f"test-task-{uuid.uuid4().hex[:8]}"

    # Extract file_type from URL if not provided
    if file_type is None:
        filename = attachment_url.split("/")[-1]
        if "." in filename:
            file_type = filename.rsplit(".", 1)[-1].lower()
        else:
            file_type = "pdf"

    # Auto-generate blob_path if not provided
    if blob_path is None:
        filename = attachment_url.split("/")[-1]
        blob_path = f"{status_subtype}/{assignment_id}/{file_type}/{filename}"

    # Default timestamp
    if original_timestamp is None:
        original_timestamp = datetime.now(timezone.utc)

    # Default metadata
    if metadata is None:
        metadata = {}

    return DownloadTaskMessage(
        trace_id=trace_id,
        attachment_url=attachment_url,
        blob_path=blob_path,
        status_subtype=status_subtype,
        file_type=file_type,
        assignment_id=assignment_id,
        event_type=event_type,
        event_subtype=event_subtype,
        retry_count=retry_count,
        original_timestamp=original_timestamp,
        metadata=metadata,
    )


def create_download_result_message(
    trace_id: Optional[str] = None,
    attachment_url: str = "https://example.com/file.pdf",
    blob_path: Optional[str] = None,
    status_subtype: str = "documentsReceived",
    file_type: Optional[str] = None,
    assignment_id: str = "A12345",
    status: str = "completed",
    http_status: Optional[int] = 200,
    bytes_downloaded: int = 1024,
    retry_count: int = 0,
    created_at: Optional[datetime] = None,
    error_message: Optional[str] = None,
    expires_at: Optional[datetime] = None,
    expired_at_ingest: Optional[bool] = None,
) -> DownloadResultMessage:
    """
    Create test DownloadResultMessage with sensible defaults.

    Args:
        trace_id: Event identifier (auto-generated if None)
        attachment_url: Downloaded URL
        blob_path: OneLake path where file was uploaded
        status_subtype: Event status subtype (default: "documentsReceived")
        file_type: File extension (auto-detected from URL if None)
        assignment_id: Assignment ID (default: "A12345")
        status: Download status (default: "completed")
        http_status: HTTP response status code (default: 200)
        bytes_downloaded: Number of bytes downloaded (default: 1024, 0 for failures)
        retry_count: Number of retry attempts (default: 0)
        created_at: Creation timestamp (default: current UTC)
        error_message: Error description (for failures)
        expires_at: URL expiration timestamp (optional)
        expired_at_ingest: Whether URL was expired at ingest (optional)

    Returns:
        DownloadResultMessage: Valid result message for testing

    Example:
        >>> result = create_download_result_message(trace_id="test-001")
        >>> result = create_download_result_message(
        ...     trace_id="test-002",
        ...     status="failed",
        ...     error_message="Connection timeout",
        ...     bytes_downloaded=0
        ... )
    """
    # Auto-generate trace_id if not provided
    if trace_id is None:
        import uuid
        trace_id = f"test-result-{uuid.uuid4().hex[:8]}"

    # Extract file_type from URL if not provided
    if file_type is None:
        filename = attachment_url.split("/")[-1]
        if "." in filename:
            file_type = filename.rsplit(".", 1)[-1].lower()
        else:
            file_type = "pdf"

    # Auto-generate blob_path if not provided (only for successful downloads)
    if blob_path is None and status == "completed":
        filename = attachment_url.split("/")[-1]
        blob_path = f"{status_subtype}/{assignment_id}/{file_type}/{filename}"
    elif blob_path is None:
        # For failures, still need a blob_path
        filename = attachment_url.split("/")[-1]
        blob_path = f"{status_subtype}/{assignment_id}/{file_type}/{filename}"

    # Default creation timestamp
    if created_at is None:
        created_at = datetime.now(timezone.utc)

    # For failed downloads, bytes_downloaded should be 0
    if status != "completed":
        bytes_downloaded = 0
        http_status = None

    return DownloadResultMessage(
        trace_id=trace_id,
        attachment_url=attachment_url,
        blob_path=blob_path,
        status_subtype=status_subtype,
        file_type=file_type,
        assignment_id=assignment_id,
        status=status,
        http_status=http_status,
        bytes_downloaded=bytes_downloaded,
        retry_count=retry_count,
        error_message=error_message,
        created_at=created_at,
        expires_at=expires_at,
        expired_at_ingest=expired_at_ingest,
    )


def create_failed_download_message(
    trace_id: Optional[str] = None,
    attachment_url: str = "https://example.com/file.pdf",
    blob_path: Optional[str] = None,
    status_subtype: str = "documentsReceived",
    file_type: Optional[str] = None,
    assignment_id: str = "A12345",
    final_error: str = "Download failed after max retries",
    error_category: str = "PERMANENT",
    retry_count: int = 3,
    event_type: str = "xact",
    event_subtype: str = "documentsReceived",
    original_timestamp: Optional[datetime] = None,
    failed_at: Optional[datetime] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> FailedDownloadMessage:
    """
    Create test FailedDownloadMessage with sensible defaults.

    Args:
        trace_id: Event identifier (auto-generated if None)
        attachment_url: Failed download URL
        blob_path: OneLake blob path (auto-generated if None)
        status_subtype: Event status subtype (default: "documentsReceived")
        file_type: File type from URL (auto-detected if None)
        assignment_id: Assignment ID (default: "A12345")
        final_error: Error description
        error_category: Error classification (default: "PERMANENT")
        retry_count: Number of attempts made (default: 3)
        event_type: Event category (default: "xact")
        event_subtype: Event action (default: "documentsReceived")
        original_timestamp: Original event timestamp
        failed_at: Timestamp when task was sent to DLQ (default: current UTC)
        metadata: Additional error context

    Returns:
        FailedDownloadMessage: Valid DLQ message for testing

    Example:
        >>> failed = create_failed_download_message(trace_id="test-001")
        >>> failed = create_failed_download_message(
        ...     trace_id="test-002",
        ...     final_error="404 Not Found",
        ...     error_category="PERMANENT"
        ... )
    """
    # Auto-generate trace_id if not provided
    if trace_id is None:
        import uuid
        trace_id = f"test-failed-{uuid.uuid4().hex[:8]}"

    # Extract file_type from URL if not provided
    if file_type is None:
        filename = attachment_url.split("/")[-1]
        if "." in filename:
            file_type = filename.rsplit(".", 1)[-1].lower()
        else:
            file_type = "pdf"

    # Auto-generate blob_path if not provided
    if blob_path is None:
        filename = attachment_url.split("/")[-1]
        blob_path = f"{status_subtype}/{assignment_id}/{file_type}/{filename}"

    # Default timestamps
    if original_timestamp is None:
        original_timestamp = datetime.now(timezone.utc)

    if failed_at is None:
        failed_at = datetime.now(timezone.utc)

    # Default metadata with error context
    if metadata is None:
        metadata = {
            "error_history": [
                "Attempt 1: Connection timeout",
                "Attempt 2: Connection timeout",
                "Attempt 3: Connection timeout",
            ]
        }

    # Create original task for reference
    original_task = DownloadTaskMessage(
        trace_id=trace_id,
        attachment_url=attachment_url,
        blob_path=blob_path,
        status_subtype=status_subtype,
        file_type=file_type,
        assignment_id=assignment_id,
        event_type=event_type,
        event_subtype=event_subtype,
        retry_count=retry_count,
        original_timestamp=original_timestamp,
        metadata=metadata,
    )

    return FailedDownloadMessage(
        trace_id=trace_id,
        attachment_url=attachment_url,
        original_task=original_task,
        final_error=final_error,
        error_category=error_category,
        retry_count=retry_count,
        failed_at=failed_at,
    )


# ==========================================
# ClaimX Generators
# ==========================================


def create_claimx_event_message(
    event_id: Optional[str] = None,
    event_type: str = "PROJECT_FILE_ADDED",
    project_id: Optional[str] = None,
    ingested_at: Optional[datetime] = None,
    media_id: Optional[str] = None,
    task_assignment_id: Optional[str] = None,
    video_collaboration_id: Optional[str] = None,
    master_file_name: Optional[str] = None,
    raw_data: Optional[Dict[str, Any]] = None,
) -> ClaimXEventMessage:
    """
    Create test ClaimXEventMessage with sensible defaults.

    Args:
        event_id: Unique event identifier (auto-generated if None)
        event_type: Event type (default: PROJECT_FILE_ADDED)
        project_id: ClaimX project ID (auto-generated if None)
        ingested_at: Event ingestion timestamp (default: current UTC)
        media_id: Media file ID (default: auto-generated for file events)
        task_assignment_id: Task assignment ID (for task events)
        video_collaboration_id: Video collaboration ID (for video events)
        master_file_name: Master file name (for MFN events)
        raw_data: Raw event payload (default: minimal data)

    Returns:
        ClaimXEventMessage: Valid ClaimX event for testing

    Example:
        >>> event = create_claimx_event_message(event_id="evt_001")
        >>> event = create_claimx_event_message(
        ...     event_id="evt_002",
        ...     event_type="PROJECT_CREATED",
        ...     project_id="proj_12345"
        ... )
    """
    import uuid

    # Auto-generate IDs if not provided
    if event_id is None:
        event_id = f"evt_{uuid.uuid4().hex[:12]}"

    if project_id is None:
        project_id = f"proj_{uuid.uuid4().hex[:8]}"

    # Default ingestion timestamp
    if ingested_at is None:
        ingested_at = datetime.now(timezone.utc)

    # Auto-generate media_id for file-related events
    if media_id is None and event_type in ["PROJECT_FILE_ADDED"]:
        media_id = f"media_{uuid.uuid4().hex[:8]}"

    # Default raw_data
    if raw_data is None:
        raw_data = {
            "projectId": project_id,
            "eventType": event_type,
        }
        if media_id:
            raw_data["mediaId"] = media_id
            raw_data["fileName"] = "test_file.jpg"
            raw_data["fileSize"] = 1024

    return ClaimXEventMessage(
        event_id=event_id,
        event_type=event_type,
        project_id=project_id,
        ingested_at=ingested_at,
        media_id=media_id,
        task_assignment_id=task_assignment_id,
        video_collaboration_id=video_collaboration_id,
        master_file_name=master_file_name,
        raw_data=raw_data,
    )


def create_claimx_enrichment_task(
    event_id: Optional[str] = None,
    event_type: str = "PROJECT_FILE_ADDED",
    project_id: Optional[str] = None,
    retry_count: int = 0,
    created_at: Optional[datetime] = None,
    media_id: Optional[str] = None,
    task_assignment_id: Optional[str] = None,
    video_collaboration_id: Optional[str] = None,
    master_file_name: Optional[str] = None,
) -> ClaimXEnrichmentTask:
    """
    Create test ClaimXEnrichmentTask with sensible defaults.

    Args:
        event_id: Event identifier (auto-generated if None)
        event_type: Event type to process (default: PROJECT_FILE_ADDED)
        project_id: ClaimX project ID (auto-generated if None)
        retry_count: Retry count (default: 0)
        created_at: Task creation timestamp (default: current UTC)
        media_id: Media ID (for file events)
        task_assignment_id: Task assignment ID (for task events)
        video_collaboration_id: Video collaboration ID (for video events)
        master_file_name: Master file name (for MFN events)

    Returns:
        ClaimXEnrichmentTask: Valid enrichment task for testing

    Example:
        >>> task = create_claimx_enrichment_task(event_id="evt_001")
    """
    import uuid

    # Auto-generate IDs if not provided
    if event_id is None:
        event_id = f"evt_{uuid.uuid4().hex[:12]}"

    if project_id is None:
        project_id = f"proj_{uuid.uuid4().hex[:8]}"

    # Default creation timestamp
    if created_at is None:
        created_at = datetime.now(timezone.utc)

    # Auto-generate media_id for file-related events
    if media_id is None and event_type in ["PROJECT_FILE_ADDED"]:
        media_id = f"media_{uuid.uuid4().hex[:8]}"

    return ClaimXEnrichmentTask(
        event_id=event_id,
        event_type=event_type,
        project_id=project_id,
        retry_count=retry_count,
        created_at=created_at,
        media_id=media_id,
        task_assignment_id=task_assignment_id,
        video_collaboration_id=video_collaboration_id,
        master_file_name=master_file_name,
    )


def create_claimx_download_task(
    media_id: Optional[str] = None,
    project_id: Optional[str] = None,
    download_url: str = "https://claimxperience.s3.amazonaws.com/test.jpg",
    destination_path: Optional[str] = None,
    file_type: Optional[str] = None,
    file_name: Optional[str] = None,
    source_event_id: Optional[str] = None,
    retry_count: int = 0,
    created_at: Optional[datetime] = None,
) -> ClaimXDownloadTask:
    """
    Create test ClaimXDownloadTask with sensible defaults.

    Args:
        media_id: Media file ID (auto-generated if None)
        project_id: Project ID (auto-generated if None)
        download_url: S3 presigned URL (default: test S3 URL)
        destination_path: OneLake destination path (auto-generated if None)
        file_type: File extension (auto-detected if None)
        file_name: File name (auto-detected if None)
        source_event_id: Source event ID (auto-generated if None)
        retry_count: Retry count (default: 0)
        created_at: Task creation timestamp (default: current UTC)

    Returns:
        ClaimXDownloadTask: Valid download task for testing

    Example:
        >>> task = create_claimx_download_task(media_id="media_001")
    """
    import uuid

    # Auto-generate IDs if not provided
    if media_id is None:
        media_id = f"media_{uuid.uuid4().hex[:8]}"

    if project_id is None:
        project_id = f"proj_{uuid.uuid4().hex[:8]}"

    if source_event_id is None:
        source_event_id = f"evt_{uuid.uuid4().hex[:12]}"

    # Extract file info from URL
    if file_name is None:
        file_name = download_url.split("/")[-1].split("?")[0]  # Remove query params

    if file_type is None:
        if "." in file_name:
            file_type = file_name.rsplit(".", 1)[-1].lower()
        else:
            file_type = "jpg"

    # Auto-generate destination path
    if destination_path is None:
        destination_path = f"{project_id}/media/{file_name}"

    # Default creation timestamp
    if created_at is None:
        created_at = datetime.now(timezone.utc)

    return ClaimXDownloadTask(
        media_id=media_id,
        project_id=project_id,
        download_url=download_url,
        destination_path=destination_path,
        file_type=file_type,
        file_name=file_name,
        source_event_id=source_event_id,
        retry_count=retry_count,
        created_at=created_at,
    )


def create_claimx_cached_download_message(
    media_id: Optional[str] = None,
    project_id: Optional[str] = None,
    download_url: str = "https://claimxperience.s3.amazonaws.com/test.jpg",
    destination_path: Optional[str] = None,
    local_cache_path: Optional[str] = None,
    bytes_downloaded: int = 2048,
    content_type: Optional[str] = "image/jpeg",
    file_type: Optional[str] = None,
    file_name: Optional[str] = None,
    source_event_id: Optional[str] = None,
    downloaded_at: Optional[datetime] = None,
) -> ClaimXCachedDownloadMessage:
    """
    Create test ClaimXCachedDownloadMessage with sensible defaults.

    Args:
        media_id: Media file ID (auto-generated if None)
        project_id: Project ID (auto-generated if None)
        download_url: S3 presigned URL
        destination_path: OneLake destination path (auto-generated if None)
        local_cache_path: Local cache file path (auto-generated if None)
        bytes_downloaded: File size in bytes (default: 2048)
        content_type: MIME type (default: image/jpeg)
        file_type: File extension (auto-detected if None)
        file_name: File name (auto-detected if None)
        source_event_id: Source event ID (auto-generated if None)
        downloaded_at: Download timestamp (default: current UTC)

    Returns:
        ClaimXCachedDownloadMessage: Valid cached download message for testing

    Example:
        >>> cached = create_claimx_cached_download_message(media_id="media_001")
    """
    import uuid

    # Auto-generate IDs if not provided
    if media_id is None:
        media_id = f"media_{uuid.uuid4().hex[:8]}"

    if project_id is None:
        project_id = f"proj_{uuid.uuid4().hex[:8]}"

    if source_event_id is None:
        source_event_id = f"evt_{uuid.uuid4().hex[:12]}"

    # Extract file info from URL
    if file_name is None:
        file_name = download_url.split("/")[-1].split("?")[0]

    if file_type is None:
        if "." in file_name:
            file_type = file_name.rsplit(".", 1)[-1].lower()
        else:
            file_type = "jpg"

    # Auto-generate paths
    if destination_path is None:
        destination_path = f"{project_id}/media/{file_name}"

    if local_cache_path is None:
        local_cache_path = f"/tmp/kafka_pipeline_cache/{media_id}/{file_name}"

    # Default download timestamp
    if downloaded_at is None:
        downloaded_at = datetime.now(timezone.utc)

    return ClaimXCachedDownloadMessage(
        media_id=media_id,
        project_id=project_id,
        download_url=download_url,
        destination_path=destination_path,
        local_cache_path=local_cache_path,
        bytes_downloaded=bytes_downloaded,
        content_type=content_type,
        file_type=file_type,
        file_name=file_name,
        source_event_id=source_event_id,
        downloaded_at=downloaded_at,
    )


def create_claimx_upload_result_message(
    media_id: Optional[str] = None,
    project_id: Optional[str] = None,
    download_url: str = "https://claimxperience.s3.amazonaws.com/test.jpg",
    blob_path: Optional[str] = None,
    file_type: Optional[str] = None,
    file_name: Optional[str] = None,
    source_event_id: Optional[str] = None,
    status: str = "completed",
    bytes_uploaded: int = 2048,
    error_message: Optional[str] = None,
    created_at: Optional[datetime] = None,
) -> ClaimXUploadResultMessage:
    """
    Create test ClaimXUploadResultMessage with sensible defaults.

    Args:
        media_id: Media file ID (auto-generated if None)
        project_id: Project ID (auto-generated if None)
        download_url: S3 presigned URL
        blob_path: OneLake blob path (auto-generated if None)
        file_type: File extension (auto-detected if None)
        file_name: File name (auto-detected if None)
        source_event_id: Source event ID (auto-generated if None)
        status: Upload status (default: completed)
        bytes_uploaded: Bytes uploaded (default: 2048, 0 for failures)
        error_message: Error message (for failures)
        created_at: Result creation timestamp (default: current UTC)

    Returns:
        ClaimXUploadResultMessage: Valid upload result for testing

    Example:
        >>> result = create_claimx_upload_result_message(media_id="media_001")
        >>> failed_result = create_claimx_upload_result_message(
        ...     media_id="media_002",
        ...     status="failed",
        ...     error_message="Connection timeout",
        ...     bytes_uploaded=0
        ... )
    """
    import uuid

    # Auto-generate IDs if not provided
    if media_id is None:
        media_id = f"media_{uuid.uuid4().hex[:8]}"

    if project_id is None:
        project_id = f"proj_{uuid.uuid4().hex[:8]}"

    if source_event_id is None:
        source_event_id = f"evt_{uuid.uuid4().hex[:12]}"

    # Extract file info from URL
    if file_name is None:
        file_name = download_url.split("/")[-1].split("?")[0]

    if file_type is None:
        if "." in file_name:
            file_type = file_name.rsplit(".", 1)[-1].lower()
        else:
            file_type = "jpg"

    # Auto-generate blob path
    if blob_path is None:
        blob_path = f"{project_id}/media/{file_name}"

    # Default creation timestamp
    if created_at is None:
        created_at = datetime.now(timezone.utc)

    # For failures, bytes_uploaded should be 0
    if status != "completed":
        bytes_uploaded = 0

    return ClaimXUploadResultMessage(
        media_id=media_id,
        project_id=project_id,
        download_url=download_url,
        blob_path=blob_path,
        file_type=file_type,
        file_name=file_name,
        source_event_id=source_event_id,
        status=status,
        bytes_uploaded=bytes_uploaded,
        error_message=error_message,
        created_at=created_at,
    )
