"""
Test data generators for ClaimX integration tests.

Provides helper functions to create test ClaimX events, tasks, and entities.
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional

from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
from kafka_pipeline.claimx.schemas.tasks import ClaimXEnrichmentTask, ClaimXDownloadTask
from kafka_pipeline.claimx.schemas.cached import ClaimXCachedDownloadMessage
from kafka_pipeline.claimx.schemas.results import ClaimXUploadResultMessage


def create_claimx_event(
    event_id: int,
    event_type: str,
    project_id: int,
    user_id: Optional[int] = None,
    contact_id: Optional[int] = None,
    media_id: Optional[int] = None,
    task_id: Optional[int] = None,
    custom_task_template_id: Optional[int] = None,
    mfn: Optional[str] = None,
    **kwargs
) -> ClaimXEventMessage:
    """
    Create a ClaimX event message for testing.

    Args:
        event_id: Unique event ID
        event_type: Event type (PROJECT_CREATED, PROJECT_FILE_ADDED, etc.)
        project_id: Project ID
        user_id: User ID (optional)
        contact_id: Contact ID (optional)
        media_id: Media ID (optional)
        task_id: Task ID (optional)
        custom_task_template_id: Custom task template ID (optional)
        mfn: Master file number (optional)
        **kwargs: Additional fields

    Returns:
        ClaimXEventMessage: Test event message
    """
    event_data = {
        "event_id": str(event_id),  # Convert to string
        "event_type": event_type,
        "project_id": str(project_id),  # Convert to string
        "user_id": str(user_id) if user_id is not None else None,
        "contact_id": str(contact_id) if contact_id is not None else None,
        "media_id": str(media_id) if media_id is not None else None,
        "task_id": str(task_id) if task_id is not None else None,
        "custom_task_template_id": str(custom_task_template_id) if custom_task_template_id is not None else None,
        "mfn": mfn,
        "event_date": kwargs.get("event_date", datetime.now(timezone.utc)),
        "created_at": kwargs.get("created_at", datetime.now(timezone.utc)),
        "ingested_at": kwargs.get("ingested_at", datetime.now(timezone.utc)),
    }

    # Add any additional fields
    event_data.update(kwargs)

    return ClaimXEventMessage(**event_data)


def create_enrichment_task(
    event_id: int,
    event_type: str,
    project_id: int,
    **kwargs
) -> ClaimXEnrichmentTask:
    """
    Create a ClaimX enrichment task for testing.

    Args:
        event_id: Event ID
        event_type: Event type
        project_id: Project ID
        **kwargs: Additional fields

    Returns:
        ClaimXEnrichmentTask: Test enrichment task
    """
    # Extract kwargs and convert to strings where needed
    user_id = kwargs.get("user_id")
    contact_id = kwargs.get("contact_id")
    media_id = kwargs.get("media_id")
    task_id = kwargs.get("task_id")
    custom_task_template_id = kwargs.get("custom_task_template_id")

    task_data = {
        "event_id": str(event_id),  # Convert to string
        "event_type": event_type,
        "project_id": str(project_id),  # Convert to string
        "user_id": str(user_id) if user_id is not None else None,
        "contact_id": str(contact_id) if contact_id is not None else None,
        "media_id": str(media_id) if media_id is not None else None,
        "task_id": str(task_id) if task_id is not None else None,
        "custom_task_template_id": str(custom_task_template_id) if custom_task_template_id is not None else None,
        "mfn": kwargs.get("mfn"),
        "created_at": kwargs.get("created_at", datetime.now(timezone.utc)),
        "retry_count": kwargs.get("retry_count", 0),
    }

    return ClaimXEnrichmentTask(**task_data)


def create_download_task(
    media_id: int,
    project_id: int,
    download_url: str,
    **kwargs
) -> ClaimXDownloadTask:
    """
    Create a ClaimX download task for testing.

    Args:
        media_id: Media ID
        project_id: Project ID
        download_url: Presigned download URL
        **kwargs: Additional fields

    Returns:
        ClaimXDownloadTask: Test download task
    """
    # Default blob_path if not provided
    # Note: path is relative to OneLake domain base path (which includes 'claimx')
    default_blob_path = f"project_{project_id}/media_{media_id}.jpg"

    task_data = {
        "media_id": str(media_id),  # Convert to string
        "project_id": str(project_id),  # Convert to string
        "download_url": download_url,
        "blob_path": kwargs.get("blob_path", default_blob_path),
        "file_type": kwargs.get("file_type", "jpg"),
        "file_name": kwargs.get("file_name", f"media_{media_id}.jpg"),
        "source_event_id": kwargs.get("source_event_id", f"evt_{media_id}"),
        "retry_count": kwargs.get("retry_count", 0),
        "expires_at": kwargs.get("expires_at"),
        "refresh_count": kwargs.get("refresh_count", 0),
    }

    return ClaimXDownloadTask(**task_data)


def create_cached_download_message(
    media_id: int,
    project_id: int,
    local_path: str,
    download_url: str,
    **kwargs
) -> ClaimXCachedDownloadMessage:
    """
    Create a ClaimX cached download message for testing.

    Args:
        media_id: Media ID
        project_id: Project ID
        local_path: Local file path
        download_url: Original download URL
        **kwargs: Additional fields

    Returns:
        ClaimXCachedDownloadMessage: Test cached download message
    """
    msg_data = {
        "media_id": str(media_id),  # Convert to string
        "project_id": str(project_id),  # Convert to string
        "local_path": local_path,
        "download_url": download_url,
        "file_size_bytes": kwargs.get("file_size_bytes", 1024),
        "content_type": kwargs.get("content_type", "image/jpeg"),
        "cached_at": kwargs.get("cached_at", datetime.now(timezone.utc)),
    }

    return ClaimXCachedDownloadMessage(**msg_data)


def create_upload_result_message(
    media_id: int,
    project_id: int,
    status: str,
    **kwargs
) -> ClaimXUploadResultMessage:
    """
    Create a ClaimX upload result message for testing.

    Args:
        media_id: Media ID
        project_id: Project ID
        status: Upload status (completed/failed/failed_permanent)
        **kwargs: Additional fields

    Returns:
        ClaimXUploadResultMessage: Test upload result message
    """
    result_data = {
        "media_id": str(media_id),  # Convert to string
        "project_id": str(project_id),  # Convert to string
        "status": status,
        "download_url": kwargs.get("download_url", f"https://s3.amazonaws.com/claimx/media_{media_id}.jpg"),
        "blob_path": kwargs.get("blob_path", f"project_{project_id}/media_{media_id}.jpg"),
        "file_type": kwargs.get("file_type", "jpg"),
        "file_name": kwargs.get("file_name", f"media_{media_id}.jpg"),
        "source_event_id": kwargs.get("source_event_id", ""),
        "bytes_uploaded": kwargs.get("bytes_uploaded", 1024 if status == "completed" else 0),
        "error_message": kwargs.get("error_message"),
        "created_at": kwargs.get("created_at", datetime.now(timezone.utc)),
    }

    return ClaimXUploadResultMessage(**result_data)


# Mock API response generators


def create_mock_project_response(project_id: int, mfn: Optional[str] = None) -> Dict:
    """Create mock project API response."""
    return {
        "id": project_id,
        "mfn": mfn or f"MFN-{project_id}",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "status": "active",
        "name": f"Test Project {project_id}",
        "contacts": []
    }


def create_mock_contact_response(contact_id: int, project_id: int) -> Dict:
    """Create mock contact API response."""
    return {
        "id": contact_id,
        "project_id": project_id,
        "name": f"Test Contact {contact_id}",
        "email": f"contact{contact_id}@example.com",
        "role": "policyholder",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def create_mock_media_response(
    media_id: int,
    project_id: int,
    download_url: str = "https://example.com/media/test.jpg"
) -> Dict:
    """Create mock media API response."""
    return {
        "id": media_id,
        "project_id": project_id,
        "filename": f"media_{media_id}.jpg",
        "content_type": "image/jpeg",
        "file_size": 2048,
        "download_url": download_url,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def create_mock_task_response(task_id: int, project_id: int) -> Dict:
    """Create mock task API response."""
    return {
        "id": task_id,
        "project_id": project_id,
        "name": f"Test Task {task_id}",
        "status": "assigned",
        "assignee_id": 123,
        "template_id": 456,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "due_date": None,
    }


def create_mock_video_collab_response(project_id: int) -> Dict:
    """Create mock video collaboration API response."""
    return {
        "project_id": project_id,
        "session_id": f"session_{project_id}",
        "status": "completed",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": 1800,
        "participants": [
            {"id": 1, "name": "Adjuster"},
            {"id": 2, "name": "Policyholder"}
        ]
    }
