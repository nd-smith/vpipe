"""
Media downloader for iTel Cabinet API plugin.

Downloads media files from ClaimX and uploads to OneLake storage.
Integrated into enrichment pipeline for completed tasks.
"""

import asyncio
import logging
import mimetypes
from pathlib import Path
from typing import List, Optional, Tuple

import aiohttp

from kafka_pipeline.common.storage.onelake_client import OneLakeClient
from kafka_pipeline.plugins.shared.connections import ConnectionManager

from .models import CabinetAttachment

logger = logging.getLogger(__name__)


class MediaDownloader:
    """
    Downloads media files for iTel Cabinet attachments.

    Downloads files from ClaimX API and uploads to OneLake storage.
    Handles individual downloads with error resilience - continues on partial failures.

    Usage:
        downloader = MediaDownloader(
            connection_manager=connections,
            onelake_base_path="abfss://...",
        )
        async with downloader:
            attachments = await downloader.download_and_upload(
                attachments,
                project_id=123,
                assignment_id=456,
            )
    """

    def __init__(
        self,
        connection_manager: ConnectionManager,
        onelake_base_path: str,
        claimx_connection: str = "claimx_api",
        download_timeout: int = 60,
    ):
        """
        Initialize media downloader.

        Args:
            connection_manager: For ClaimX API access
            onelake_base_path: Base OneLake path (from ITEL_ATTACHMENTS_PATH env)
            claimx_connection: Name of ClaimX connection in connection manager
            download_timeout: Timeout for individual file downloads (seconds)
        """
        self.connections = connection_manager
        self.onelake_base_path = onelake_base_path
        self.claimx_connection = claimx_connection
        self.download_timeout = download_timeout

        self.onelake_client: Optional[OneLakeClient] = None
        self._session: Optional[aiohttp.ClientSession] = None

        logger.info(
            "MediaDownloader initialized",
            extra={
                "onelake_base_path": onelake_base_path,
                "download_timeout": download_timeout,
            }
        )

    async def __aenter__(self):
        """Initialize clients on context entry."""
        # Create OneLake client
        self.onelake_client = OneLakeClient(self.onelake_base_path)
        await self.onelake_client.__aenter__()

        # Create HTTP session for direct file downloads
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.download_timeout)
        )

        logger.debug("MediaDownloader clients initialized")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close clients on context exit."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

        if self.onelake_client:
            await self.onelake_client.close()
            self.onelake_client = None

        logger.debug("MediaDownloader clients closed")
        return False

    async def download_and_upload(
        self,
        attachments: List[CabinetAttachment],
        project_id: int,
        assignment_id: int,
    ) -> List[CabinetAttachment]:
        """
        Download media files and upload to OneLake.

        Downloads each attachment's media file from ClaimX API and uploads
        to OneLake. Updates blob_path for successful uploads.

        Continues on partial failures - logs errors but doesn't fail entire batch.

        Args:
            attachments: List of attachments to download
            project_id: Project ID for path construction
            assignment_id: Assignment ID for path construction

        Returns:
            List of attachments with blob_path updated for successful uploads
        """
        if not attachments:
            logger.debug("No attachments to download")
            return []

        logger.info(
            "Starting media download and upload",
            extra={
                "project_id": project_id,
                "assignment_id": assignment_id,
                "attachment_count": len(attachments),
            }
        )

        # Process downloads concurrently
        tasks = [
            self._download_and_upload_single(att, project_id, assignment_id)
            for att in attachments
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Count successes/failures
        success_count = 0
        failure_count = 0

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(
                    f"Unexpected error downloading media: {result}",
                    extra={
                        "media_id": attachments[i].media_id,
                        "project_id": project_id,
                        "assignment_id": assignment_id,
                    },
                    exc_info=True,
                )
                failure_count += 1
            elif result is None:
                # Failed but logged internally
                failure_count += 1
            else:
                # Success - update attachment with blob_path
                attachments[i] = result
                success_count += 1

        logger.info(
            "Media download and upload complete",
            extra={
                "project_id": project_id,
                "assignment_id": assignment_id,
                "total": len(attachments),
                "succeeded": success_count,
                "failed": failure_count,
            }
        )

        return attachments

    async def _download_and_upload_single(
        self,
        attachment: CabinetAttachment,
        project_id: int,
        assignment_id: int,
    ) -> Optional[CabinetAttachment]:
        """
        Download and upload a single media file.

        Args:
            attachment: Attachment to download
            project_id: Project ID
            assignment_id: Assignment ID

        Returns:
            Updated attachment with blob_path, or None on failure
        """
        try:
            # Get media metadata from ClaimX API to get download URL
            media_metadata = await self._fetch_media_metadata(
                project_id, attachment.media_id
            )

            if not media_metadata:
                logger.warning(
                    "Media metadata not found",
                    extra={
                        "media_id": attachment.media_id,
                        "project_id": project_id,
                    }
                )
                return None

            download_url = media_metadata.get("fullDownloadLink")
            if not download_url:
                logger.warning(
                    "No download URL in media metadata",
                    extra={
                        "media_id": attachment.media_id,
                        "project_id": project_id,
                    }
                )
                return None

            # Detect file extension from media metadata
            file_extension = self._detect_file_extension(media_metadata)

            # Download file content
            file_content = await self._download_file(download_url)

            # Construct blob path: {project_id}/{assignment_id}/media/{media_id}.{ext}
            relative_path = f"{project_id}/{assignment_id}/media/{attachment.media_id}.{file_extension}"

            # Upload to OneLake
            assert self.onelake_client is not None
            full_blob_path = await self.onelake_client.upload_bytes(
                relative_path=relative_path,
                data=file_content,
                overwrite=True,
            )

            logger.info(
                "Media uploaded successfully",
                extra={
                    "media_id": attachment.media_id,
                    "project_id": project_id,
                    "assignment_id": assignment_id,
                    "blob_path": full_blob_path,
                    "bytes": len(file_content),
                }
            )

            # Extract relative path from full abfss:// path
            # Full: abfss://...@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files/...
            # Store: Files/...
            relative_blob_path = full_blob_path
            if "/" in full_blob_path:
                # Find the lakehouse ID (GUID after the host) and extract path after it
                parts = full_blob_path.split("/")
                # Find index of "Files" and keep everything from there
                for i, part in enumerate(parts):
                    if part == "Files":
                        relative_blob_path = "/".join(parts[i:])
                        break

            # Update attachment with blob_path, media_type, and file_extension
            attachment.blob_path = relative_blob_path
            attachment.media_type = self._detect_mime_type(file_extension)
            attachment.file_extension = file_extension

            return attachment

        except Exception as e:
            logger.error(
                f"Failed to download and upload media: {e}",
                extra={
                    "media_id": attachment.media_id,
                    "project_id": project_id,
                    "assignment_id": assignment_id,
                },
                exc_info=True,
            )
            return None

    async def _fetch_media_metadata(
        self, project_id: int, media_id: int
    ) -> Optional[dict]:
        """
        Fetch media metadata from ClaimX API.

        Args:
            project_id: Project ID
            media_id: Media ID

        Returns:
            Media metadata dict with fullDownloadLink, or None if not found
        """
        endpoint = f"/export/project/{project_id}/media"
        params = {"mediaIds": str(media_id)}

        status, response = await self.connections.request_json(
            connection_name=self.claimx_connection,
            method="GET",
            path=endpoint,
            params=params,
        )

        if status < 200 or status >= 300:
            logger.warning(
                f"Failed to fetch media metadata: HTTP {status}",
                extra={
                    "project_id": project_id,
                    "media_id": media_id,
                    "status": status,
                }
            )
            return None

        # Normalize response to list
        if isinstance(response, list):
            media_list = response
        elif isinstance(response, dict):
            if "data" in response:
                media_list = response["data"]
            elif "media" in response:
                media_list = response["media"]
            else:
                media_list = [response]
        else:
            media_list = []

        # Find our media_id in the response
        for media in media_list:
            if media.get("mediaID") == media_id:
                return media

        return None

    async def _download_file(self, url: str) -> bytes:
        """
        Download file content from URL.

        Args:
            url: Full download URL

        Returns:
            File content as bytes

        Raises:
            Exception: On download failure
        """
        assert self._session is not None

        async with self._session.get(url) as response:
            if response.status != 200:
                raise Exception(
                    f"Failed to download file: HTTP {response.status} from {url}"
                )

            content = await response.read()
            logger.debug(
                "Downloaded file",
                extra={
                    "url": url[:100],  # Truncate long URLs
                    "bytes": len(content),
                }
            )

            return content

    def _detect_file_extension(self, media_metadata: dict) -> str:
        """
        Detect file extension from media metadata.

        Checks mediaType field and falls back to mediaName if needed.

        Args:
            media_metadata: Media metadata dict from API

        Returns:
            File extension (e.g., "jpg", "png", "pdf")
        """
        # Try mediaType first (e.g., "jpg", "png", "pdf")
        media_type = media_metadata.get("mediaType", "").lower()
        if media_type:
            # Remove leading dot if present
            return media_type.lstrip(".")

        # Fall back to parsing from file name
        media_name = media_metadata.get("mediaName", "")
        if media_name and "." in media_name:
            return media_name.rsplit(".", 1)[1].lower()

        # Default to jpg if unknown
        logger.warning(
            "Could not detect file extension, defaulting to jpg",
            extra={"media_metadata": media_metadata}
        )
        return "jpg"

    def _detect_mime_type(self, file_extension: str) -> str:
        """
        Detect MIME type from file extension.

        Args:
            file_extension: File extension (e.g., "jpg", "png", "pdf")

        Returns:
            MIME type (e.g., "image/jpeg", "application/pdf")
        """
        mime_type, _ = mimetypes.guess_type(f"file.{file_extension}")
        return mime_type or "application/octet-stream"
