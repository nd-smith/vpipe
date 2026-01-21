"""
Download task factory for ClaimX enrichment pipeline.

Converts media metadata rows into ClaimXDownloadTask objects for the download worker.
Handles URL extraction, blob path generation, and task creation.
"""

import logging
from typing import Any, Dict, List

from core.logging import get_logger
from kafka_pipeline.claimx.schemas.tasks import ClaimXDownloadTask

logger = get_logger(__name__)


class DownloadTaskFactory:
    """
    Factory for creating download tasks from media metadata rows.

    Responsibilities:
    - Extract download URLs from media rows
    - Generate OneLake blob storage paths
    - Create ClaimXDownloadTask objects
    """

    @staticmethod
    def create_download_tasks_from_media(
        media_rows: List[Dict[str, Any]],
    ) -> List[ClaimXDownloadTask]:
        """
        Convert media metadata rows into download tasks.

        Args:
            media_rows: List of media row dictionaries from handlers

        Returns:
            List of ClaimXDownloadTask objects ready for Kafka
        """
        download_tasks = []

        for media_row in media_rows:
            download_url = media_row.get("full_download_link")
            if not download_url:
                logger.debug(
                    "Skipping media row without download URL",
                    extra={
                        "media_id": media_row.get("media_id"),
                        "project_id": media_row.get("project_id"),
                    },
                )
                continue

            task = ClaimXDownloadTask(
                media_id=str(media_row.get("media_id", "")),
                project_id=str(media_row.get("project_id", "")),
                download_url=download_url,
                blob_path=DownloadTaskFactory._generate_blob_path(media_row),
                file_type=media_row.get("file_type", ""),
                file_name=media_row.get("file_name", ""),
                source_event_id=media_row.get("event_id", ""),
                retry_count=0,
                expires_at=media_row.get("expires_at"),
                refresh_count=0,
            )
            download_tasks.append(task)

        logger.debug(
            "Created download tasks from media rows",
            extra={
                "media_rows": len(media_rows),
                "download_tasks": len(download_tasks),
            },
        )

        return download_tasks

    @staticmethod
    def _generate_blob_path(media_row: Dict[str, Any]) -> str:
        """
        Generate blob storage path for media file.

        Path is relative to OneLake domain base path (which includes 'claimx' prefix).

        Args:
            media_row: Media metadata row dictionary

        Returns:
            Blob path string in format: {project_id}/media/{file_name}
        """
        project_id = media_row.get("project_id", "unknown")
        media_id = media_row.get("media_id", "unknown")
        file_name = media_row.get("file_name", f"media_{media_id}")
        return f"{project_id}/media/{file_name}"
